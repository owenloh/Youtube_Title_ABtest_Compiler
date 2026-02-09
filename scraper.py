"""Fetch videos from channel using YouTube Data API and scrape titles from watch pages."""
import re
import time
from datetime import datetime
from typing import List, Optional, Tuple
import requests

from youtube_comment import get_credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

WATCH_URL = "https://www.youtube.com/watch?v={video_id}"
SHORTS_URL = "https://www.youtube.com/shorts/{video_id}"

# Browser-like so we might get the same A/B variant as users
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_videos_from_channel(channel_id: str, max_videos: int = 50) -> List[Tuple[str, datetime]]:
    """
    Get videos from channel using YouTube Data API.
    Returns: [(video_id, published_at), ...] ordered newest first.
    
    Uses playlistItems.list on the channel's uploads playlist.
    Cost: ~3 quota units per call (1 for channels.list + 1-2 for playlistItems.list)
    """
    creds = get_credentials()
    if not creds:
        print(f"No credentials available for API call")
        return []
    
    youtube = build("youtube", "v3", credentials=creds)
    
    try:
        # Get the uploads playlist ID (it's "UU" + channel_id without "UC" prefix)
        # e.g., UCHnyfMqiRRG1u-2MsSQLbXA -> UUHnyfMqiRRG1u-2MsSQLbXA
        uploads_playlist_id = "UU" + channel_id[2:]
        
        # Get videos from uploads playlist
        videos = []
        next_page_token = None
        
        while len(videos) < max_videos:
            request = youtube.playlistItems().list(
                part="snippet",
                playlistId=uploads_playlist_id,
                maxResults=min(50, max_videos - len(videos)),
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                video_id = snippet.get("resourceId", {}).get("videoId")
                published_str = snippet.get("publishedAt")
                
                if video_id and published_str:
                    try:
                        published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                        videos.append((video_id, published_at))
                    except (ValueError, AttributeError):
                        continue
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        
        return videos
        
    except HttpError as e:
        print(f"YouTube API error for channel {channel_id}: {e}")
        return []
    except Exception as e:
        print(f"Error fetching videos for channel {channel_id}: {e}")
        return []


# Keep old function name as alias for compatibility
get_videos_from_rss = get_videos_from_channel


def is_short(video_id: str) -> bool:
    """
    Check if a video is a YouTube Short (efficient: tries lightest methods first).
    RSS feed doesn't tell us, so we need to check the video itself.
    """
    try:
        # Method 1: Try HEAD request to shorts URL (lightest check)
        # If video is accessible via /shorts/ URL, it's a Short
        shorts_url = SHORTS_URL.format(video_id=video_id)
        r_head = requests.head(shorts_url, headers=HEADERS, timeout=5, allow_redirects=True)
        if r_head.ok:
            # Check if final URL contains /shorts/ (even after redirects)
            if '/shorts/' in r_head.url.lower():
                return True
            # If HEAD returns 200 and URL is still /shorts/, it's a Short
            if '/shorts/' in shorts_url.lower() and r_head.status_code == 200:
                return True
        
        # Method 2: Check og:url meta tag from watch page (partial fetch, often cached)
        # We only need to check the <head> section, but requests.get gets full page
        # Still more efficient than checking full HTML content
        url = WATCH_URL.format(video_id=video_id)
        r = requests.get(url, headers=HEADERS, timeout=10, stream=True)
        if r.ok:
            # Read first 50KB to get <head> section with og:url
            content = b""
            for chunk in r.iter_content(chunk_size=8192):
                content += chunk
                if len(content) > 50000:  # Stop after 50KB (enough for <head>)
                    break
            
            html = content.decode('utf-8', errors='ignore').lower()
            
            # Check og:url meta tag first (most reliable indicator)
            og_url_match = re.search(
                r'<meta\s+property=["\']og:url["\']\s+content=["\']([^"\']+)["\']',
                html,
                re.IGNORECASE
            )
            if og_url_match and '/shorts/' in og_url_match.group(1).lower():
                return True
            
            # Check for shorts indicators in the partial HTML
            if '"isShorts":true' in html or '"isShort":true' in html:
                return True
        
    except Exception:
        pass
    
    return False


def _parse_title_from_html(html: str) -> Optional[str]:
    """Try several patterns to get title from watch page HTML."""
    import html as html_lib
    # 1. og:title (double-quoted)
    m = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', html)
    if m:
        return html_lib.unescape(m.group(1).strip())
    # 2. og:title (content first, or single quotes)
    m = re.search(r'<meta\s+content="([^"]+)"\s+property="og:title"', html)
    if m:
        return html_lib.unescape(m.group(1).strip())
    m = re.search(r"<meta\s+property=['\"]og:title['\"]\s+content=['\"]([^'\"]+)['\"]", html)
    if m:
        return html_lib.unescape(m.group(1).strip())
    # 3. <title>... - YouTube</title>
    m = re.search(r"<title>([^<]+)\s*-\s*YouTube</title>", html, re.IGNORECASE | re.DOTALL)
    if m:
        return html_lib.unescape(m.group(1).strip())
    # 4. Embedded JSON: "runs":[{"text":"Title here"}] (videoPrimaryInfoRenderer)
    m = re.search(r'"runs":\s*\[\s*\{\s*"text":\s*"((?:[^"\\]|\\.)*)"', html)
    if m:
        return html_lib.unescape(m.group(1).strip())
    # 5. "simpleText":"Title"
    m = re.search(r'"simpleText":\s*"((?:[^"\\]|\\.)*)"', html)
    if m:
        return html_lib.unescape(m.group(1).strip())
    return None


def get_video_title(video_id: str) -> Optional[str]:
    """Fetch watch page and parse title (og:title / <title> / JSON). Fallback: noembed."""
    url = WATCH_URL.format(video_id=video_id)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        title = _parse_title_from_html(r.text)
        if title:
            return title
    except Exception:
        pass
    # Fallback: noembed (third-party; may not reflect A/B variant but gets a title)
    try:
        noembed_url = f"https://noembed.com/embed?url=https://www.youtube.com/watch?v={video_id}"
        r = requests.get(noembed_url, headers=HEADERS, timeout=10)
        if r.ok:
            data = r.json()
            if isinstance(data.get("title"), str):
                return data["title"].strip()
    except Exception:
        pass
    return None


def sample_titles(video_id: str, count: int) -> List[str]:
    """Fetch title `count` times (with small delay) to collect A/B samples."""
    seen = []
    for _ in range(count):
        title = get_video_title(video_id)
        if title:
            seen.append(title)
        time.sleep(1.5)  # be nice to YouTube
    return seen
