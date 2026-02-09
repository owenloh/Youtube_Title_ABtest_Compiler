# YouTube Title A/B Test Compiler

Tracks YouTube title A/B tests across multiple channels. Scrapes titles, detects changes, and posts comments with historical title data.

## Features

- Monitors 18+ YouTube channels for new videos
- Samples titles 21 times to catch A/B test variants
- Posts timestamped comments showing title history
- Dashboard to view all tracked videos
- Auto-detects when titles stabilize (marks inactive after 5 days)
- Skips Shorts automatically

## File Structure

```
app.py              # Entry point (Railway)
main.py             # Scheduler + video processing
storage.py          # PostgreSQL database operations
scraper.py          # Title sampling via web scraping
youtube_comment.py  # YouTube API for comments
config.py           # Environment settings
dashboard_api.py    # Flask API endpoints
dashboard.html      # Web dashboard UI
get_refresh_token.py # OAuth setup helper
```

## Setup

### 1. Google OAuth (one-time)

1. [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials
2. Create OAuth 2.0 Client ID (Desktop app)
3. Enable YouTube Data API v3
4. Add your email as test user in OAuth consent screen

### 2. Get Refresh Token (local)

```bash
# .env
YOUTUBE_CLIENT_ID=your_client_id
YOUTUBE_CLIENT_SECRET=your_client_secret

python get_refresh_token.py
# Copy the printed YOUTUBE_REFRESH_TOKEN
```

### 3. Deploy to Railway

Add PostgreSQL service, connect repo, set env vars:

```env
DATABASE_URL=<from Railway PostgreSQL>
YOUTUBE_CLIENT_ID=<from step 1>
YOUTUBE_CLIENT_SECRET=<from step 1>
YOUTUBE_REFRESH_TOKEN=<from step 2>
YOUTUBE_CHANNELS=@veritasium:Veritasium,@kurzgesagt:Kurzgesagt,@MrBeast:MrBeast
CUTOFF_DATE=2026-02-07
```

Dashboard available at your Railway public URL.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `YOUTUBE_CLIENT_ID` | Yes | — | OAuth client ID |
| `YOUTUBE_CLIENT_SECRET` | Yes | — | OAuth client secret |
| `YOUTUBE_REFRESH_TOKEN` | Yes | — | From `get_refresh_token.py` |
| `YOUTUBE_CHANNELS` | No | Veritasium | `@handle:name,@handle:name` format |
| `CUTOFF_DATE` | No | 2026-02-08 | Only process videos after this date |
| `NEW_VIDEO_CHECK_INTERVAL` | No | 180 | Seconds between new video checks |
| `ACTIVE_VIDEO_CHECK_INTERVAL` | No | 3600 | Seconds between active video checks |
| `SAMPLES_PER_RUN` | No | 21 | Title samples per check |
| `INACTIVE_DAYS_THRESHOLD` | No | 5 | Days of same title = finalized |
| `SKIP_COMMENT` | No | 0 | Set to 1 to disable commenting |

## Comment Format

Comments show title history by date:

```
I noticed YouTube is testing different titles on this video

Feb 07: Original Title
Feb 08: Original Title | New Test Title
Feb 09: New Test Title | Another Variant | Third Option
```

## API Endpoints

- `GET /` - Dashboard
- `GET /api/videos` - All videos with stats
- `GET /api/stats` - Summary counts
- `POST /api/reset` - Clear database (use with caution)

## How It Works

1. Checks RSS feeds every 3 minutes for new videos
2. New videos get 5 quick samples, comment posted immediately
3. Then 16 more samples collected in background
4. Hourly checks detect title changes on active videos
5. Comments updated when new titles detected
6. Videos marked inactive after 5 days of same title

## Local Dev

```bash
pip install -r requirements.txt
python app.py  # Runs scheduler + dashboard on port 8080
```
