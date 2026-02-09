# YouTube Title A/B Test Compiler

Bot that **scrapes** video titles from multiple YouTube channels, tracks A/B test variants over time, and **posts/updates comments** with historical title changes. Only processes long-form videos (no Shorts).

- **Multi-channel support**: Track multiple channels simultaneously
- **Smart detection**: Only checks new videos (cascades until finding existing video)
- **Date filtering**: Only processes videos from a configurable cutoff date
- **Title history**: Tracks title changes by date, shows historical progression in comments
- **Active/non-active logic**: Automatically detects when titles are finalized
- **PostgreSQL**: Uses PostgreSQL for persistence (Railway provides DATABASE_URL)
- **Dashboard**: Web dashboard for health and status tracking

## Features

- **New video check**: Every minute, checks all channels for new videos
- **Active video check**: Every hour, checks active videos for title changes
- **Immediate processing**: New videos are processed and commented immediately
- **Historical tracking**: Comments show title changes by date
- **Error handling**: If a comment is deleted, the video is marked as ignored
- **Shorts filtering**: Automatically skips YouTube Shorts

## Quick Start

### 1. Google Cloud OAuth (one-time)

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials
2. Create **OAuth 2.0 Client ID** (Desktop app recommended)
3. Enable **YouTube Data API v3** for the project
4. Add your email as a test user in OAuth consent screen

### 2. Get refresh token (one-time, on your machine)

1. Put `YOUTUBE_CLIENT_ID` and `YOUTUBE_CLIENT_SECRET` in `.env`
2. Run: `python get_refresh_token.py`
3. Log in and copy the printed `YOUTUBE_REFRESH_TOKEN` to `.env`

### 3. Railway Setup

1. **Add PostgreSQL**: In Railway, add a PostgreSQL service and note the `DATABASE_URL`
2. **Deploy**: Connect your repo to Railway
3. **Environment Variables**:
   ```
   DATABASE_URL=<from PostgreSQL service>
   YOUTUBE_CLIENT_ID=<from step 1>
   YOUTUBE_CLIENT_SECRET=<from step 1>
   YOUTUBE_REFRESH_TOKEN=<from step 2>
   YOUTUBE_CHANNELS=UCHnyfMqiRRG1u-2MsSQLbXA:Veritasium,UCsXVk37bltHxD1rDPwtNM8Q:Kurzgesagt
   CUTOFF_DATE=2026-02-08
   ```
4. **Deploy**: The scheduler runs continuously

### 4. Dashboard (Optional)

The dashboard API can be run separately:

```bash
python dashboard_api.py
```

Then open `dashboard.html` in a browser (update `API_URL` in the HTML to point to your Railway URL).

For Netlify deployment:
1. Deploy `dashboard.html` as a static site
2. Update `API_URL` in `dashboard.html` to your Railway dashboard API URL
3. Enable CORS on your Railway service (already enabled in `dashboard_api.py`)

## Configuration

All settings are via environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (from Railway) |
| `YOUTUBE_CLIENT_ID` | Yes | — | OAuth client ID |
| `YOUTUBE_CLIENT_SECRET` | Yes | — | OAuth client secret |
| `YOUTUBE_REFRESH_TOKEN` | Yes | — | From `get_refresh_token.py` |
| `YOUTUBE_CHANNELS` | No | Veritasium | Comma-separated: `channel_id:name,channel_id:name` |
| `CUTOFF_DATE` | No | 2026-02-08 | Only process videos from this date (YYYY-MM-DD) |
| `NEW_VIDEO_CHECK_INTERVAL` | No | 60 | Seconds between new video checks |
| `ACTIVE_VIDEO_CHECK_INTERVAL` | No | 3600 | Seconds between active video checks |
| `SAMPLES_PER_RUN` | No | 20 | Title samples per video per check |
| `MIN_SAMPLES_TO_POST` | No | 3 | Min samples before posting comment |
| `INACTIVE_DAYS_THRESHOLD` | No | 5 | Days of single title = non-active |
| `COMMENT_DESCRIPTION` | No | (see config.py) | Human description text for comments |
| `SKIP_COMMENT` | No | 0 | Set to 1 to disable commenting |

## How It Works

### New Video Detection

1. Every minute, checks RSS feeds for all configured channels
2. Gets videos from RSS (newest first)
3. For each video:
   - If published before `CUTOFF_DATE`, stop (don't check older videos)
   - If video exists in DB, stop (found existing video)
   - If it's a Short, skip (but add to DB so we don't check again)
   - Otherwise, add to DB and process immediately

### Processing

1. Samples titles `SAMPLES_PER_RUN` times (with 1.5s delay between)
2. Records samples in database
3. Updates title history for today's date
4. If enough samples, posts/updates comment with historical title changes

### Active Video Checks

1. Every hour, checks all active videos (not ignored, not shorts, has comment)
2. Samples titles to get current state
3. Compares with previous title set (by name, not percentage)
4. If changed, updates comment with new historical entry

### Active vs Non-Active

- **Active**: Video is still being A/B tested (multiple titles or recent changes)
- **Non-active**: Same single title for `INACTIVE_DAYS_THRESHOLD` days straight
- Non-active videos show "Probable finalized titles" in comments

### Comment Format

```
I compile video titles to show what YouTubers A/B test with. I have too much time and this is fun.

Latest video titles (8 Feb 2026):

Video titles as of 8 Feb 2026:
1. Title Variant A
2. Title Variant B

Video titles as of 7 Feb 2026:
1. Title Variant A
2. Title Variant C
```

For finalized videos:
```
Probable finalized titles (since 8 Feb 2026):

Video titles as of 8 Feb 2026:
1. Final Title
```

## Database Schema

- `channels`: Channel IDs and display names
- `videos`: Video metadata, comment IDs, timestamps
- `title_samples`: Individual title samples with timestamps
- `title_history`: Unique titles per date (for historical display)

## Error Handling

- If a comment update fails with 404/403, the video is marked as `is_ignored=True`
- Ignored videos are never checked again
- All errors are logged to stdout (visible in Railway logs)

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set up .env
DATABASE_URL=postgresql://user:pass@localhost/dbname
YOUTUBE_CLIENT_ID=...
YOUTUBE_CLIENT_SECRET=...
YOUTUBE_REFRESH_TOKEN=...
YOUTUBE_CHANNELS=UCHnyfMqiRRG1u-2MsSQLbXA:Veritasium
CUTOFF_DATE=2026-02-08

# Run scheduler
python main.py

# Run dashboard API (separate terminal)
python dashboard_api.py
```

## Cost

- **Scraping**: No API key; no quota
- **Comment API**: ~50 units per post/update; free quota 10,000/day
- **Railway**: Pay for usage; scheduler runs continuously
- **PostgreSQL**: Railway provides free tier, scales as needed
