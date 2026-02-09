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
    mark_video_inactive,
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
    
    # If no history, get current titles from stats
    if not history:
        stats = get_title_stats(video_id)
        if stats:
            lines.append("Titles observed:")
            for i, (title, _) in enumerate(stats, 1):
                short = (title[:80] + "...") if len(title) > 80 else title
                lines.append(f"{i}. {short}")
        else:
            lines.append("(No data yet)")
    elif len(history) == 1:
        # Single date - simple format
        hist_date, titles = history[0]
        if len(titles) == 1:
            lines.append(f"Title: {titles[0]}")
        else:
            lines.append("Titles observed:")
            for i, title in enumerate(titles, 1):
                short = (title[:80] + "...") if len(title) > 80 else title
                lines.append(f"{i}. {short}")
    else:
        # Multiple dates - show history
        if is_finalized:
            lines.append("Title history:")
        else:
            lines.append("Title changes detected:")
        lines.append("")
        for hist_date, titles in history:
            date_str = hist_date.strftime("%d %b")
            if len(titles) == 1:
                lines.append(f"{date_str}: {titles[0]}")
            else:
                lines.append(f"{date_str}:")
                for i, title in enumerate(titles, 1):
                    short = (title[:80] + "...") if len(title) > 80 else title
                    lines.append(f"  {i}. {short}")
    
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
    
    def check_channel(channel_slug: str, channel_name: str) -> List[tuple]:
        """Check single channel, return list of new videos to process."""
        new_videos = []
        try:
            print(f"[{channel_name}] Checking channel {channel_slug}")
            rss_videos = get_videos_from_rss(channel_slug, expected_name=channel_name, max_videos=50)
            if not rss_videos:
                print(f"[{channel_name}] No videos found")
                return []
            
            # Check if we have dates (RSS) or not (HTTP fallback)
            has_dates = rss_videos[0][1] is not None
            
            add_channel(channel_slug, channel_name)
            known_ids = set(get_known_video_ids_for_channel(channel_slug, limit=50))
            
            print(f"[{channel_name}] Found {len(rss_videos)} videos ({'RSS' if has_dates else 'HTTP'}), {len(known_ids)} known")
            
            if has_dates:
                # RSS MODE: We have publish dates
                # Only store/process videos AFTER cutoff date
                # Always ensure at least 1 video stored (newest after cutoff) for HTTP fallback reference
                
                processed_count = 0
                for video_id, published_at in rss_videos:
                    # Skip if before cutoff - don't store at all
                    if published_at.date() < CUTOFF_DATE:
                        continue
                    
                    # Skip if already known
                    if video_id in known_ids:
                        continue
                    
                    # Check if it's a short
                    if is_short(video_id):
                        add_video(video_id, channel_slug, published_at, is_short=True)
                        print(f"[{channel_name}] Skipping {video_id} (Short)")
                        continue
                    
                    # Store and process
                    if add_video(video_id, channel_slug, published_at, is_short=False):
                        new_videos.append((video_id, channel_slug, channel_name, published_at))
                        processed_count += 1
                        print(f"[{channel_name}] NEW VIDEO: {video_id} (published {published_at.date()})")
                
                # Ensure at least 1 video exists for this channel (for HTTP fallback anchor)
                if not known_ids and processed_count == 0:
                    # No videos after cutoff and no known videos - store the newest one as reference
                    newest_id, newest_date = rss_videos[0]
                    if not is_short(newest_id):
                        add_video(newest_id, channel_slug, newest_date, is_short=False)
                        print(f"[{channel_name}] Stored {newest_id} as anchor (no videos after cutoff)")
            
            else:
                # HTTP MODE: No dates, use anchor-based detection
                # Find newest known video in the list
                anchor_index = None
                for i, (video_id, _) in enumerate(rss_videos):
                    if video_id in known_ids:
                        anchor_index = i
                        break
                
                if anchor_index is not None:
                    # Only process videos newer than anchor (before it in list)
                    candidates = rss_videos[:anchor_index]
                else:
                    # No anchor - first run via HTTP, only take first 3
                    candidates = rss_videos[:3]
                
                for video_id, _ in candidates:
                    if video_id in known_ids:
                        continue
                    
                    if is_short(video_id):
                        add_video(video_id, channel_slug, datetime.now(), is_short=True)
                        print(f"[{channel_name}] Skipping {video_id} (Short)")
                        continue
                    
                    if add_video(video_id, channel_slug, datetime.now(), is_short=False):
                        new_videos.append((video_id, channel_slug, channel_name, datetime.now()))
                        print(f"[{channel_name}] NEW VIDEO: {video_id} (HTTP, no date)")
        
        except Exception as e:
            print(f"[{channel_name}] Error checking channel: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
        
        return new_videos
    
    # Check all channels in parallel
    futures = {
        executor.submit(check_channel, ch_slug, ch_name): (ch_slug, ch_name)
        for ch_slug, ch_name in CHANNELS
    }
    
    print(f"Submitted {len(futures)} channel checks...")
    
    # Collect new videos and spawn processing tasks
    for future in as_completed(futures):
        ch_slug, ch_name = futures[future]
        try:
            new_videos = future.result()
            if new_videos:
                for video_id, channel_slug, channel_name, published_at in new_videos:
                    # Process in background - don't block other channels
                    executor.submit(process_video, video_id, channel_slug, channel_name, published_at)
                    print(f"[{channel_name}] Spawned background task for {video_id}")
        except Exception as e:
            print(f"[{ch_name}] Channel check failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()


def check_active_videos():
    """Check active videos for title changes (hourly task).
    
    Videos are already filtered by is_active = TRUE in the database.
    If a video has stagnated (same single title for N days), mark it inactive permanently.
    """
    print(f"\n=== Checking active videos at {datetime.now()} ===")
    
    active_videos = get_active_videos()
    print(f"Found {len(active_videos)} active videos to check")
    
    for video_info in active_videos:
        video_id = video_info["video_id"]
        channel_id = video_info["channel_id"]
        
        try:
            # Check if video has stagnated
            if not is_video_active(video_id, INACTIVE_DAYS_THRESHOLD):
                print(f"Video {video_id} has stagnated (same title for {INACTIVE_DAYS_THRESHOLD}+ days) - marking inactive")
                mark_video_inactive(video_id)
                
                # Update comment to show finalized
                if not SKIP_COMMENT:
                    comment_text = build_comment_text(video_id, is_finalized=True)
                    comment_id = get_comment_id(video_id)
                    if comment_id:
                        try:
                            if update_comment(comment_id, comment_text):
                                update_comment_edited(video_id)
                                print(f"Updated comment for {video_id} (finalized)")
                        except Exception:
                            pass
                continue
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
