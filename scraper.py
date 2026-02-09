"""Fetch videos from channel using RSS (primary) with YouTube Data API fallback."""
import re
import time
from datetime import datetime
from typing import List, Optional, Tuple
import xml.etree.ElementTree as ET
import requests

from youtube_comment import get_credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
WATCH_URL = "https://www.youtube.com/watch?v={video_id}"
SHORTS_URL = "https://www.youtube.com/shorts/{video_id}"

# Browser-like so we might get the same A/B variant as users
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _get_videos_from_rss(channel_id: str, max_videos: int = 50) -> List[Tuple[str, datetime]]:
    """Try RSS feed first (free, no quota)."""
    url = RSS_URL.format(channel_id=channel_id)
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    
    ns = {"yt": "http://www.youtube.com/xml/schemas/2015", "atom": "http://www.w3.org/2005/Atom"}
    entries = root.findall(".//atom:entry", namespaces=ns)
    if not entries:
        entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    
    videos = []
    for entry in entries[:max_videos]:
        video_id_elem = entry.find(".//yt:videoId", namespaces=ns)
        if video_id_elem is None:
            video_id_elem = entry.find(".//{http://www.youtube.com/xml/schemas/2015}videoId")
        if video_id_elem is None:
            continue
        
        video_id = (video_id_elem.text or "").strip()
        if not video_id:
            continue
        
        published_elem = entry.find(".//atom:published", namespaces=ns)
        if published_elem is None:
            published_elem = entry.find(".//{http://www.w3.org/2005/Atom}published")
        
        if published_elem is not None and published_elem.text:
            try:
                published_str = published_elem.text.strip()
                published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                videos.append((video_id, published_at))
            except (ValueError, AttributeError):
                continue
    
    return videos


def _get_videos_from_api(channel_id: str, max_videos: int = 50) -> List[Tuple[str, datetime]]:
    """Fallback to YouTube Data API (costs quota)."""
    creds = get_credentials()
    if not creds:
        return []
    
    youtube = build("youtube", "v3", credentials=creds)
    
    # Uploads playlist ID = "UU" + channel_id without "UC" prefix
    uploads_playlist_id = "UU" + channel_id[2:]
    
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


def get_videos_from_rss(channel_id: str, max_videos: int = 50) -> List[Tuple[str, datetime]]:
    """
    Get videos from channel. Tries RSS first (free), falls back to API if RSS fails.
    Returns: [(video_id, published_at), ...] ordered newest first.
    """
    # Try RSS first
    try:
        videos = _get_videos_from_rss(channel_id, max_videos)
        if videos:
            return videos
    except Exception as e:
        print(f"RSS failed for {channel_id}: {e}, trying API...")
    
    # Fallback to API
    try:
        videos = _get_videos_from_api(channel_id, max_videos)
        if videos:
            print(f"API fallback succeeded for {channel_id}")
            return videos
    except HttpError as e:
        print(f"API fallback failed for {channel_id}: {e}")
    except Exception as e:
        print(f"API fallback error for {channel_id}: {e}")
    
    return []


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


def sample_titles(video_id: str, count: int, delay: float = 1.5) -> List[str]:
    """Fetch title `count` times (with small delay) to collect A/B samples."""
    seen = []
    for i in range(count):
        title = get_video_title(video_id)
        if title:
            seen.append(title)
        if i < count - 1:  # Don't sleep after last sample
            time.sleep(delay)
    return seen
