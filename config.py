"""Config from environment. Set these in Railway (or .env locally)."""
import os
from datetime import datetime, date
from typing import List

from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection (Railway provides DATABASE_URL)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Multi-channel support: comma-separated channel IDs or names
# Format: "channel_id:display_name,channel_id:display_name" or just channel IDs
CHANNELS_STR = os.environ.get("YOUTUBE_CHANNELS", "UCHnyfMqiRRG1u-2MsSQLbXA:Veritasium")
CHANNELS: List[tuple[str, str]] = []
for ch in CHANNELS_STR.split(","):
    ch = ch.strip()
    if ":" in ch:
        ch_id, ch_name = ch.split(":", 1)
        CHANNELS.append((ch_id.strip(), ch_name.strip()))
    else:
        CHANNELS.append((ch.strip(), ch.strip()))

# OAuth for posting/editing comments
YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID", "")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET", "")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN", "")

# Date cutoff: only process videos from this date onwards (checked on every call)
CUTOFF_DATE_STR = os.environ.get("CUTOFF_DATE", "2026-02-08")
try:
    CUTOFF_DATE = datetime.strptime(CUTOFF_DATE_STR, "%Y-%m-%d").date()
except ValueError:
    CUTOFF_DATE = date(2026, 2, 8)  # Default: Feb 8, 2026

# Polling intervals (in seconds)
NEW_VIDEO_CHECK_INTERVAL = int(os.environ.get("NEW_VIDEO_CHECK_INTERVAL", "60"))  # 1 minute
ACTIVE_VIDEO_CHECK_INTERVAL = int(os.environ.get("ACTIVE_VIDEO_CHECK_INTERVAL", "3600"))  # 1 hour

# Title sampling
SAMPLES_PER_RUN = int(os.environ.get("SAMPLES_PER_RUN", "20"))
MIN_SAMPLES_TO_POST = int(os.environ.get("MIN_SAMPLES_TO_POST", "3"))

# Active/non-active logic: non-active if N days straight same single title
INACTIVE_DAYS_THRESHOLD = int(os.environ.get("INACTIVE_DAYS_THRESHOLD", "5"))

# Comment description (human, engaging, no emojis)
COMMENT_DESCRIPTION = os.environ.get(
    "COMMENT_DESCRIPTION",
    "I compile video titles to show what YouTubers A/B test with. I have too much time and this is fun."
)

# Set to 1 to run without posting/updating YouTube comment
SKIP_COMMENT = os.environ.get("SKIP_COMMENT", "0").strip().lower() in ("1", "true", "yes")
