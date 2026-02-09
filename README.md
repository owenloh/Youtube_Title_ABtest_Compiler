# YouTube Title A/B Test Compiler

Tracks YouTube title A/B tests across multiple channels. Scrapes titles, detects changes, and posts comments with historical title data.

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
YOUTUBE_CHANNELS=UCHnyfMqiRRG1u-2MsSQLbXA:Veritasium,UCsXVk37bltHxD1rDPwtNM8Q:Kurzgesagt
```

Dashboard available at your Railway public URL.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `YOUTUBE_CLIENT_ID` | Yes | — | OAuth client ID |
| `YOUTUBE_CLIENT_SECRET` | Yes | — | OAuth client secret |
| `YOUTUBE_REFRESH_TOKEN` | Yes | — | From `get_refresh_token.py` |
| `YOUTUBE_CHANNELS` | No | Veritasium | `channel_id:name,channel_id:name` |
| `CUTOFF_DATE` | No | 2026-02-08 | Only process videos after this date |
| `NEW_VIDEO_CHECK_INTERVAL` | No | 60 | Seconds between new video checks |
| `ACTIVE_VIDEO_CHECK_INTERVAL` | No | 3600 | Seconds between active video checks |
| `SAMPLES_PER_RUN` | No | 20 | Title samples per check |
| `MIN_SAMPLES_TO_POST` | No | 3 | Min samples before posting comment |
| `INACTIVE_DAYS_THRESHOLD` | No | 5 | Days of same title = finalized |
| `SKIP_COMMENT` | No | 0 | Set to 1 to disable commenting |

## How It Works

- Checks RSS feeds every minute for new videos
- Samples titles multiple times to catch A/B variants
- Posts comment with title history, updates when titles change
- Marks videos as finalized after N days of same title
- Skips Shorts automatically

## Local Dev

```bash
pip install -r requirements.txt
python app.py  # Runs scheduler + dashboard on port 5000
```
