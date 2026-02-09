"""Post and update a top-level comment on a video using YouTube Data API v3 (OAuth)."""
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


def get_credentials():
    """Build credentials from refresh token (set in env for Railway)."""
    if not YOUTUBE_CLIENT_ID or not YOUTUBE_CLIENT_SECRET or not YOUTUBE_REFRESH_TOKEN:
        return None
    creds = Credentials(
        token=None,
        refresh_token=YOUTUBE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YOUTUBE_CLIENT_ID,
        client_secret=YOUTUBE_CLIENT_SECRET,
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds


def post_comment(video_id: str, text: str) -> str | None:
    """Post a top-level comment; returns the comment ID."""
    creds = get_credentials()
    if not creds:
        return None
    youtube = build("youtube", "v3", credentials=creds)
    body = {
        "snippet": {
            "videoId": video_id,
            "topLevelComment": {
                "snippet": {
                    "textOriginal": text[:10000],  # API limit
                }
            },
        }
    }
    try:
        response = youtube.commentThreads().insert(part="snippet", body=body).execute()
        return response["snippet"]["topLevelComment"]["id"]
    except HttpError as e:
        print(f"API error posting comment: {e}")
        return None


def update_comment(comment_id: str, text: str) -> bool:
    """
    Update an existing comment's text.
    Returns True on success, False on error.
    Raises exception if comment was deleted (404/403) so caller can mark video as ignored.
    """
    creds = get_credentials()
    if not creds:
        return False
    youtube = build("youtube", "v3", credentials=creds)
    body = {
        "id": comment_id,
        "snippet": {
            "textOriginal": text[:10000],
        },
    }
    try:
        youtube.comments().update(part="snippet", body=body).execute()
        return True
    except HttpError as e:
        # 404 or 403 usually means comment was deleted
        if e.resp.status in (404, 403):
            print(f"Comment {comment_id} not found (deleted?) - status {e.resp.status}")
            raise  # Re-raise so caller can mark video as ignored
        print(f"API error updating comment: {e}")
        return False
