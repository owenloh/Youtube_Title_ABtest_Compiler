"""
Main scheduler: checks for new videos every minute, checks active videos every hour.
Processes videos immediately and tracks title history.
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import List, Optional

from config import (
    ACTIVE_VIDEO_CHECK_INTERVAL,
    CHANNELS,
    COMMENT_DESCRIPTION,
    CUTOFF_DATE,
    INACTIVE_DAYS_THRESHOLD,
    MIN_SAMPLES_TO_POST,
    NEW_VIDEO_CHECK_INTERVAL,
    SAMPLES_PER_RUN,
    SKIP_COMMENT,
)
from scraper import get_videos_from_rss, is_short, sample_titles
from storage import (
    add_channel,
    add_title_sample,
    add_video,
    get_active_videos,
    get_comment_id,
    get_known_video_ids_for_channel,
    get_title_history_by_date,
    get_title_stats,
    get_total_samples,
    get_unique_titles_for_date,
    init_db,
    is_video_active,
    mark_video_deleted,
    mark_video_ignored,
    set_comment_id,
    update_comment_edited,
    update_last_checked,
    update_title_history,
)
from youtube_comment import post_comment, update_comment


def build_comment_text(video_id: str, is_finalized: bool = False) -> str:
    """Build comment text with historical title changes."""
    history = get_title_history_by_date(video_id)
    
    lines = [COMMENT_DESCRIPTION, ""]
    
    if is_finalized:
        lines.append(f"Probable finalized titles (since {CUTOFF_DATE.strftime('%d %b %Y')}):")
    else:
        lines.append(f"Latest video titles ({date.today().strftime('%d %b %Y')}):")
    
    lines.append("")
    
    # If no history, get current titles from stats
    if not history:
        stats = get_title_stats(video_id)
        if stats:
            for i, (title, _) in enumerate(stats, 1):
                short = (title[:80] + "...") if len(title) > 80 else title
                lines.append(f"{i}. {short}")
        else:
            lines.append("(No data yet)")
    else:
        # Group by date
        for hist_date, titles in history:
            date_str = hist_date.strftime("%d %b %Y")
            lines.append(f"Video titles as of {date_str}:")
            for i, title in enumerate(titles, 1):
                short = (title[:80] + "...") if len(title) > 80 else title
                lines.append(f"{i}. {short}")
            lines.append("")
    
    return "\n".join(lines).strip()


def process_video(video_id: str, channel_id: str, channel_name: str, published_at: datetime):
    """Process a single video: sample titles, update stats, post/update comment."""
    print(f"[{channel_name}] Processing video {video_id} (published {published_at.date()})")
    
    # Sample titles
    titles = sample_titles(video_id, SAMPLES_PER_RUN)
    if not titles:
        print(f"[{channel_name}] No titles found for {video_id}")
        return
    
    # Add samples
    for title in titles:
        add_title_sample(video_id, title)
    
    total = get_total_samples(video_id)
    print(f"[{channel_name}] Video {video_id}: {total} total samples, {len(set(titles))} unique titles")
    
    # Update title history for today
    today = date.today()
    unique_titles_today = set(titles)
    update_title_history(video_id, list(unique_titles_today), today)
    
    if SKIP_COMMENT:
        print(f"[{channel_name}] SKIP_COMMENT=1: skipping comment")
        return
    
    if total < MIN_SAMPLES_TO_POST:
        print(f"[{channel_name}] Skipping comment (need {MIN_SAMPLES_TO_POST}+ samples)")
        return
    
    # Check if video is finalized (non-active)
    finalized = not is_video_active(video_id, INACTIVE_DAYS_THRESHOLD)
    
    # Build comment
    comment_text = build_comment_text(video_id, finalized)
    
    # Post or update comment
    comment_id = get_comment_id(video_id)
    if comment_id:
        try:
            if update_comment(comment_id, comment_text):
                update_comment_edited(video_id)
                print(f"[{channel_name}] Updated comment for {video_id}")
            else:
                print(f"[{channel_name}] Failed to update comment for {video_id}")
        except Exception:
            # Comment was deleted (404/403)
            print(f"[{channel_name}] Comment deleted for {video_id} - marking as ignored")
            mark_video_ignored(video_id)
    else:
        new_id = post_comment(video_id, comment_text)
        if new_id:
            set_comment_id(video_id, new_id)
            print(f"[{channel_name}] Posted new comment for {video_id}")
        else:
            print(f"[{channel_name}] Failed to post comment for {video_id}")


def check_new_videos():
    """
    Check all channels for new videos.
    Find the first known video that appears in RSS (latest, then 2nd latest, etc.) and slice
    everything before it as new. No duplicates: add_video uses ON CONFLICT DO NOTHING.
    """
    print(f"\n=== Checking for new videos at {datetime.now()} ===")
    
    for channel_id, channel_name in CHANNELS:
        print(f"\n[{channel_name}] Checking channel {channel_id}")
        
        try:
            # Get videos from RSS (newest first)
            rss_videos = get_videos_from_rss(channel_id, max_videos=50)
            if not rss_videos:
                print(f"[{channel_name}] No videos found in RSS")
                continue
            
            add_channel(channel_id, channel_name)
            
            # Known video IDs for this channel (newest first) - use as anchors for slicing
            known_ids = set(get_known_video_ids_for_channel(channel_id, limit=50))
            rss_ids = {video_id for video_id, _ in rss_videos}
            
            if known_ids:
                # Find first RSS position that matches any known video (latest, 2nd latest, etc.)
                anchor_index = None
                anchor_video_id = None
                for i, (video_id, _) in enumerate(rss_videos):
                    if video_id in known_ids:
                        anchor_index = i
                        anchor_video_id = video_id
                        break
                
                if anchor_index is not None:
                    # Slice: everything before the anchor is new
                    new_videos_raw = rss_videos[:anchor_index]
                    print(f"[{channel_name}] Anchor {anchor_video_id} at position {anchor_index}, {len(new_videos_raw)} new videos")
                else:
                    # No known video in RSS: either deleted or 50+ new videos pushed them out.
                    # Don't mark as deleted (ambiguous). Use all RSS as candidates; DB prevents duplicates.
                    new_videos_raw = rss_videos
                    print(f"[{channel_name}] No known video in RSS; treating {len(rss_videos)} as candidates (duplicates skipped by DB)")
            else:
                # No videos in DB yet - all RSS videos are candidates
                new_videos_raw = rss_videos
                print(f"[{channel_name}] No previous videos in DB, {len(rss_videos)} candidates")
            
            # Filter: date cutoff, shorts; add to DB (ON CONFLICT DO NOTHING prevents duplicates)
            new_videos = []
            for video_id, published_at in new_videos_raw:
                if published_at.date() < CUTOFF_DATE:
                    continue
                if is_short(video_id):
                    add_video(video_id, channel_id, published_at, is_short=True)
                    continue
                # Only count as "new" if we actually inserted (not already in DB)
                if add_video(video_id, channel_id, published_at, is_short=False):
                    new_videos.append((video_id, published_at))
                    print(f"[{channel_name}] New video: {video_id} (published {published_at.date()})")
            
            # Process only newly added long-form videos (no duplicates)
            if new_videos:
                print(f"[{channel_name}] Processing {len(new_videos)} new long-form videos")
                for video_id, published_at in new_videos:
                    try:
                        process_video(video_id, channel_id, channel_name, published_at)
                    except Exception as e:
                        print(f"[{channel_name}] Error processing {video_id}: {e}", file=sys.stderr)
                        import traceback
                        traceback.print_exc()
            else:
                print(f"[{channel_name}] No new long-form videos to process")
        
        except Exception as e:
            print(f"[{channel_name}] Error checking channel: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()


def check_active_videos():
    """Check active videos for title changes (hourly task)."""
    print(f"\n=== Checking active videos at {datetime.now()} ===")
    
    active_videos = get_active_videos()
    print(f"Found {len(active_videos)} active videos to check")
    
    for video_info in active_videos:
        video_id = video_info["video_id"]
        channel_id = video_info["channel_id"]
        
        try:
            update_last_checked(video_id)
            
            # Get current unique titles
            today = date.today()
            current_titles = get_unique_titles_for_date(video_id, today)
            
            if not current_titles:
                # Sample titles to get current state
                titles = sample_titles(video_id, SAMPLES_PER_RUN)
                if titles:
                    for title in titles:
                        add_title_sample(video_id, title)
                    current_titles = set(titles)
            
            if not current_titles:
                continue
            
            # Get previous title set from history
            history = get_title_history_by_date(video_id)
            previous_titles = set()
            if history and len(history) > 0:
                # Get titles from most recent history entry before today
                for hist_date, titles_list in history:
                    if hist_date < today:
                        previous_titles = set(titles_list)
                        break
                # If no previous date found, use the most recent entry if it's not today
                if not previous_titles and history[0][0] < today:
                    previous_titles = set(history[0][1])
            
            # Check if titles changed (by name, not percentage)
            if current_titles != previous_titles:
                print(f"Title change detected for {video_id}: {previous_titles} -> {current_titles}")
                
                # Update history
                update_title_history(video_id, list(current_titles), today)
                
                # Update comment
                if not SKIP_COMMENT:
                    finalized = not is_video_active(video_id, INACTIVE_DAYS_THRESHOLD)
                    comment_text = build_comment_text(video_id, finalized)
                    comment_id = get_comment_id(video_id)
                    
                    if comment_id:
                        try:
                            if update_comment(comment_id, comment_text):
                                update_comment_edited(video_id)
                                print(f"Updated comment for {video_id}")
                            else:
                                print(f"Failed to update comment for {video_id}")
                        except Exception:
                            # Comment was deleted
                            print(f"Comment deleted for {video_id} - marking as ignored")
                            mark_video_ignored(video_id)
        
        except Exception as e:
            print(f"Error checking video {video_id}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()


def run_scheduler():
    """Run the main scheduler loop."""
    print("Initializing database...")
    init_db()
    
    print(f"Starting scheduler:")
    print(f"  - New video check: every {NEW_VIDEO_CHECK_INTERVAL}s")
    print(f"  - Active video check: every {ACTIVE_VIDEO_CHECK_INTERVAL}s")
    print(f"  - Channels: {len(CHANNELS)}")
    print(f"  - Cutoff date: {CUTOFF_DATE}")
    print(f"  - Inactive threshold: {INACTIVE_DAYS_THRESHOLD} days")
    
    last_new_check = 0
    last_active_check = 0
    
    try:
        while True:
            now = time.time()
            
            # Check for new videos
            if now - last_new_check >= NEW_VIDEO_CHECK_INTERVAL:
                check_new_videos()
                last_new_check = now
            
            # Check active videos
            if now - last_active_check >= ACTIVE_VIDEO_CHECK_INTERVAL:
                check_active_videos()
                last_active_check = now
            
            # Sleep for a short time to avoid busy loop
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("\nShutting down scheduler...")
        sys.exit(0)


if __name__ == "__main__":
    run_scheduler()
