"""Post and update a top-level comment on a video using YouTube Data API v3 (OAuth)."""
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# The real YouTube moderationStatus values. When the API omits the field on a
# successful own-comment insert, the comment is effectively public, so we infer
# "published" (using isPublic when present) instead of leaving it "unknown".
_REAL_STATUSES = ("published", "heldForReview", "likelySpam", "rejected")


def _normalize_status(moderation_status, is_public):
    if moderation_status in _REAL_STATUSES:
        return moderation_status
    if is_public is False:
        return "heldForReview"
    return "published"


def _is_quota_or_rate_error(exc) -> bool:
    """True if an API error is a quota/rate-limit condition.

    These surface as HTTP 403 (same status as a genuinely deleted/forbidden
    comment), so callers MUST check this before treating a 403 as "deleted" --
    otherwise hitting the daily quota would wrongly mark videos as ignored and
    drop them from tracking permanently (the quota resets at midnight PT).
    """
    s = str(exc).lower()
    return (
        "quota" in s
        or "ratelimit" in s
        or "rate limit" in s
        or "userratelimitexceeded" in s
    )


def get_credentials():
    """Build credentials from refresh token (set in env for Railway)."""
    if not YOUTUBE_CLIENT_ID or not YOUTUBE_CLIENT_SECRET or not YOUTUBE_REFRESH_TOKEN:
        print("Missing YouTube credentials (CLIENT_ID, CLIENT_SECRET, or REFRESH_TOKEN)")
        return None
    try:
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
    except Exception as e:
        print(f"Failed to refresh credentials: {e}")
        return None


def post_comment(video_id: str, text: str) -> tuple[str | None, str | None]:
    """Post a top-level comment; returns (comment_id, moderation_status)."""
    creds = get_credentials()
    if not creds:
        print(f"No credentials available for posting comment on {video_id}")
        return None, None
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
        comment_id = response["snippet"]["topLevelComment"]["id"]

        raw = response["snippet"]["topLevelComment"]["snippet"].get("moderationStatus")
        is_public = response["snippet"].get("isPublic", None)
        status = _normalize_status(raw, is_public)
        print(f"Comment {comment_id} on {video_id}: {status.upper()}")
        return comment_id, status
    except HttpError as e:
        if _is_quota_or_rate_error(e):
            print(f"QUOTA/RATE LIMIT - cannot post comment on {video_id}")
            return None, "quota_exceeded"
        print(f"API error posting comment on {video_id}: {e}")
        return None, "error"
    except Exception as e:
        print(f"Unexpected error posting comment on {video_id}: {e}")
        return None, "error"


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
        # Quota/rate-limit errors ALSO come back as 403 -- check them FIRST and
        # treat as transient (retry later), never as a deletion. Otherwise every
        # edit attempt after the daily quota runs out would be misread as "comment
        # deleted" and permanently drop the video from tracking.
        if _is_quota_or_rate_error(e):
            print(f"Quota/rate limit updating comment {comment_id} - will retry later")
            return False
        # A genuine 404 (and, after ruling out quota, a 403) means the comment is
        # gone / no longer editable -- re-raise so the caller marks it ignored.
        if e.resp.status in (404, 403):
            print(f"Comment {comment_id} not found (deleted?) - status {e.resp.status}")
            raise
        print(f"API error updating comment: {e}")
        return False


def fetch_comment_meta(comment_id: str):
    """Re-read a comment's status + engagement in one call (1 quota unit).

    Returns {'status', 'likes', 'replies'} or None. Lets a 'heldForReview'
    comment flip to 'published' once approved, and tracks likes/replies as a
    virality signal.
    """
    creds = get_credentials()
    if not creds:
        return None
    youtube = build("youtube", "v3", credentials=creds)
    try:
        resp = youtube.commentThreads().list(part="snippet", id=comment_id).execute()
        items = resp.get("items", [])
        if not items:
            return None
        snippet = items[0]["snippet"]
        top = snippet["topLevelComment"]["snippet"]
        return {
            "status": _normalize_status(top.get("moderationStatus"), snippet.get("isPublic")),
            "likes": int(top.get("likeCount", 0) or 0),
            "replies": int(snippet.get("totalReplyCount", 0) or 0),
        }
    except Exception:
        return None
