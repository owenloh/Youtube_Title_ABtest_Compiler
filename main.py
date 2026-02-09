"""
Main scheduler: checks for new videos every minute, checks active videos every hour.
Processes videos immediately and tracks title history.
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from threading import Thread
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

# Thread pool for background processing
executor = ThreadPoolExecutor(max_workers=10)


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


def process_video(video_id: str, channel_id: str, channel_name: str, published_at: datetime, fast_first: bool = True):
    """Process a single video: sample titles, update stats, post/update comment.
    
    fast_first: If True and no comment exists yet, post quickly with fewer samples first,
                then continue sampling in background.
    """
    print(f"[{channel_name}] Processing video {video_id} (published {published_at.date()})")
    
    # Check if this is a new video (no comment yet)
    existing_comment = get_comment_id(video_id)
    
    if fast_first and not existing_comment and not SKIP_COMMENT:
        # FAST PATH: Get 5 samples in parallel, post comment ASAP
        print(f"[{channel_name}] Fast-posting comment for {video_id}...")
        quick_titles = sample_titles(video_id, 5, parallel=True)  # ~1-2s total
        
        if quick_titles:
            for title in quick_titles:
                add_title_sample(video_id, title)
            
            today = date.today()
            update_title_history(video_id, list(set(quick_titles)), today)
            
            comment_text = build_comment_text(video_id, is_finalized=False)
            new_id = post_comment(video_id, comment_text)
            if new_id:
                set_comment_id(video_id, new_id)
                print(f"[{channel_name}] FAST COMMENT POSTED for {video_id}")
            else:
                print(f"[{channel_name}] Failed to post fast comment for {video_id}")
        
        # Continue with more samples in background (sequential to be gentle)
        remaining_samples = SAMPLES_PER_RUN - 5
        if remaining_samples > 0:
            titles = sample_titles(video_id, remaining_samples)
            if titles:
                for title in titles:
                    add_title_sample(video_id, title)
                
                # Update history with all titles
                all_titles = set(quick_titles) | set(titles)
                update_title_history(video_id, list(all_titles), today)
                
                # Update comment with full data
                comment_id = get_comment_id(video_id)
                if comment_id and len(all_titles) > len(set(quick_titles)):
                    # Only update if we found new titles
                    comment_text = build_comment_text(video_id, is_finalized=False)
                    if update_comment(comment_id, comment_text):
                        update_comment_edited(video_id)
                        print(f"[{channel_name}] Updated comment with {len(all_titles)} unique titles for {video_id}")
        
        total = get_total_samples(video_id)
        print(f"[{channel_name}] Video {video_id}: {total} total samples, {len(set(quick_titles + (titles if 'titles' in dir() else [])))} unique titles")
        return
    
    # NORMAL PATH: Full sampling (for updates or if fast_first disabled)
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
    Check all channels for new videos IN PARALLEL.
    When new video found, spawn background task to process it immediately.
    """
    print(f"\n=== Checking for new videos at {datetime.now()} ===")
    
    def check_channel(channel_id: str, channel_name: str) -> List[tuple]:
        """Check single channel, return list of new videos to process."""
        new_videos = []
        try:
            rss_videos = get_videos_from_rss(channel_id, max_videos=50)
            if not rss_videos:
                return []
            
            add_channel(channel_id, channel_name)
            known_ids = set(get_known_video_ids_for_channel(channel_id, limit=50))
            
            if known_ids:
                anchor_index = None
                for i, (video_id, _) in enumerate(rss_videos):
                    if video_id in known_ids:
                        anchor_index = i
                        break
                new_videos_raw = rss_videos[:anchor_index] if anchor_index is not None else rss_videos
            else:
                new_videos_raw = rss_videos
            
            for video_id, published_at in new_videos_raw:
                if published_at.date() < CUTOFF_DATE:
                    continue
                if is_short(video_id):
                    add_video(video_id, channel_id, published_at, is_short=True)
                    continue
                if add_video(video_id, channel_id, published_at, is_short=False):
                    new_videos.append((video_id, channel_id, channel_name, published_at))
                    print(f"[{channel_name}] NEW VIDEO FOUND: {video_id}")
        
        except Exception as e:
            print(f"[{channel_name}] Error checking channel: {e}", file=sys.stderr)
        
        return new_videos
    
    # Check all channels in parallel
    futures = {
        executor.submit(check_channel, ch_id, ch_name): (ch_id, ch_name)
        for ch_id, ch_name in CHANNELS
    }
    
    # Collect new videos and spawn processing tasks
    for future in as_completed(futures):
        new_videos = future.result()
        for video_id, channel_id, channel_name, published_at in new_videos:
            # Process in background - don't block other channels
            executor.submit(process_video, video_id, channel_id, channel_name, published_at)
            print(f"[{channel_name}] Spawned background task for {video_id}")


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
