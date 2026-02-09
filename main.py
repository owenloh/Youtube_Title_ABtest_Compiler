"""
Main scheduler: checks for new videos every 3 minutes, checks active videos every hour.
Processes videos immediately and tracks title history.
"""
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import List

from config import (
    ACTIVE_VIDEO_CHECK_INTERVAL,
    CHANNELS,
    COMMENT_INTROS,
    CUTOFF_DATE,
    FAST_SAMPLES,
    INACTIVE_DAYS_THRESHOLD,
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
    get_videos_without_comments,
    init_db,
    is_video_active,
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


def reprocess_videos_without_comments():
    """Find and reprocess any active videos that don't have comments yet."""
    videos = get_videos_without_comments()
    if not videos:
        print("No videos without comments to reprocess")
        return
    
    print(f"Found {len(videos)} videos without comments - reprocessing...")
    for video in videos:
        video_id = video["video_id"]
        channel_id = video["channel_id"]
        channel_name = video["channel_name"]
        published_at = video["published_at"]
        
        executor.submit(process_video, video_id, channel_id, channel_name, published_at)
        print(f"[{channel_name}] Spawned reprocess task for {video_id}")


def build_comment_text(video_id: str, is_finalized: bool = False) -> str:
    """Build comment text with historical title changes."""
    history = get_title_history_by_date(video_id)
    
    # Random intro line to avoid spam detection
    intro = random.choice(COMMENT_INTROS)
    
    if not history:
        # No history yet, get from stats
        stats = get_title_stats(video_id)
        if stats:
            titles = [t for t, _ in stats]
            if len(titles) == 1:
                return f"{intro}\n\nCurrent title: {titles[0][:100]}"
            else:
                title_str = " | ".join(t[:60] for t in titles[:4])
                if len(titles) > 4:
                    title_str += f" (+{len(titles)-4})"
                return f"{intro}\n\nTitles seen: {title_str}"
        return intro
    
    # Build date-based format: "Feb 7: Title A | Title B"
    lines = [intro, ""]
    for hist_date, titles in history:
        date_str = hist_date.strftime("%b %d")
        if len(titles) == 1:
            lines.append(f"{date_str}: {titles[0][:80]}")
        else:
            title_str = " | ".join(t[:50] for t in titles[:4])
            if len(titles) > 4:
                title_str += f" (+{len(titles)-4})"
            lines.append(f"{date_str}: {title_str}")
    
    return "\n".join(lines)


def process_video(video_id: str, channel_id: str, channel_name: str, published_at: datetime, fast_first: bool = True):
    """Process a single video: sample titles, update stats, post/update comment.
    
    fast_first: If True and no comment exists yet, post quickly with fewer samples first,
                then continue sampling in background.
    """
    print(f"[{channel_name}] Processing video {video_id} (published {published_at.date()})")
    
    # Check if this is a new video (no comment yet)
    existing_comment = get_comment_id(video_id)
    
    if fast_first and not existing_comment and not SKIP_COMMENT:
        # FAST PATH: Get quick samples in parallel, post comment ASAP
        try:
            quick_titles = sample_titles(video_id, FAST_SAMPLES, parallel=True)
        except Exception as e:
            print(f"[{channel_name}] ERROR sampling titles for {video_id}: {e}", flush=True)
            quick_titles = []
        
        if quick_titles:
            try:
                for title in quick_titles:
                    add_title_sample(video_id, title)
                
                today = date.today()
                update_title_history(video_id, list(set(quick_titles)), today)
                
                comment_text = build_comment_text(video_id, is_finalized=False)
                new_id, status = post_comment(video_id, comment_text)
                if new_id:
                    set_comment_id(video_id, new_id, status)
                    print(f"[{channel_name}] Comment posted for {video_id} (status: {status})", flush=True)
                elif status == "quota_exceeded":
                    print(f"[{channel_name}] Quota exceeded - skipping comment for {video_id}", flush=True)
                else:
                    print(f"[{channel_name}] Failed to post comment for {video_id}", flush=True)
            except Exception as e:
                print(f"[{channel_name}] ERROR posting comment for {video_id}: {e}", flush=True)
        
        # Continue with more samples in background (sequential to be gentle)
        remaining_samples = SAMPLES_PER_RUN - FAST_SAMPLES
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
            rss_videos = get_videos_from_rss(channel_slug, expected_name=channel_name, max_videos=50)
            if not rss_videos:
                return []
            
            # Check if we have dates (RSS) or not (HTTP fallback)
            has_dates = rss_videos[0][1] is not None
            
            add_channel(channel_slug, channel_name)
            known_ids = set(get_known_video_ids_for_channel(channel_slug, limit=50))
            
            if has_dates:
                # RSS MODE: We have publish dates
                # RSS includes shorts, so filter them out first before anchor detection
                # NEVER store shorts - they can't be anchors for HTTP fallback
                
                # Filter to long-form videos only
                long_form_videos = []
                for video_id, published_at in rss_videos:
                    if not is_short(video_id):
                        long_form_videos.append((video_id, published_at))
                
                if not long_form_videos:
                    print(f"[{channel_name}] No long-form videos found")
                    return new_videos
                
                # Find anchor (first known video) in long-form list
                anchor_index = None
                for i, (video_id, _) in enumerate(long_form_videos):
                    if video_id in known_ids:
                        anchor_index = i
                        break
                
                # Only consider videos before anchor (newer)
                candidates = long_form_videos[:anchor_index] if anchor_index is not None else long_form_videos
                
                processed_count = 0
                for video_id, published_at in candidates:
                    # Skip if before cutoff - don't store at all
                    if published_at.date() < CUTOFF_DATE:
                        continue
                    
                    # Skip if already known
                    if video_id in known_ids:
                        continue
                    
                    # Store and process long-form video
                    if add_video(video_id, channel_slug, published_at):
                        new_videos.append((video_id, channel_slug, channel_name, published_at))
                        processed_count += 1
                        print(f"[{channel_name}] NEW VIDEO: {video_id} (published {published_at.date()})")
                
                # First run: ensure at least 1 long-form video exists for HTTP fallback anchor
                if not known_ids and processed_count == 0:
                    # No videos after cutoff - store newest long-form as anchor (inactive)
                    vid_id, vid_date = long_form_videos[0]
                    add_video(vid_id, channel_slug, vid_date, is_active=False)
            
            else:
                # HTTP MODE: No dates, already filtered to long-form only
                # Find newest known video in the list
                anchor_index = None
                for i, (video_id, _) in enumerate(rss_videos):
                    if video_id in known_ids:
                        anchor_index = i
                        break
                
                if anchor_index is not None:
                    # Only process videos newer than anchor (before it in list)
                    candidates = rss_videos[:anchor_index]
                    
                    for video_id, _ in candidates:
                        if video_id in known_ids:
                            continue
                        
                        if add_video(video_id, channel_slug, datetime.now()):
                            new_videos.append((video_id, channel_slug, channel_name, datetime.now()))
                            print(f"[{channel_name}] NEW VIDEO: {video_id} (HTTP, no date)")
                else:
                    # No anchor - first run via HTTP
                    # Store first video as anchor (inactive)
                    vid_id, _ = rss_videos[0]
                    add_video(vid_id, channel_slug, datetime.now(), is_active=False)
        
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
    
    # Reprocess any videos that have no comments (from failed earlier runs)
    reprocess_videos_without_comments()
    
    print(f"Starting scheduler:")
    print(f"  - New video check: every {NEW_VIDEO_CHECK_INTERVAL}s")
    print(f"  - Active video check: every {ACTIVE_VIDEO_CHECK_INTERVAL}s")
    print(f"  - Channels: {len(CHANNELS)}")
    print(f"  - Cutoff date: {CUTOFF_DATE}")
    print(f"  - Inactive threshold: {INACTIVE_DAYS_THRESHOLD} days")
    
    last_new_check = 0
    last_active_check = time.time()  # Don't run hourly check immediately on startup
    
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
