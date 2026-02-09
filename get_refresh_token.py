"""
One-time run (on your machine) to get a refresh token for the bot account.
1. Create OAuth 2.0 credentials in Google Cloud Console (Desktop app or Web application).
   If Web application: add redirect URI http://localhost:8080/
2. Put YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET in .env (or export them).
3. Run: python get_refresh_token.py
4. Log in with the YouTube account that should post comments.
5. Copy the printed YOUTUBE_REFRESH_TOKEN into .env and Railway.
"""
import os

from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
LOCAL_PORT = 8080

# OAuth "installed" app config format (redirect_uri must match Google Cloud Console).
REDIRECT_URI = f"http://localhost:{LOCAL_PORT}/"


def main():
    client_id = os.environ.get("YOUTUBE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        print("Missing YOUTUBE_CLIENT_ID or YOUTUBE_CLIENT_SECRET in .env")
        return

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uris": [REDIRECT_URI],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES, redirect_uri=REDIRECT_URI)
    creds = flow.run_local_server(port=LOCAL_PORT)
    print("\n--- Add to .env and Railway ---")
    print("YOUTUBE_REFRESH_TOKEN=" + creds.refresh_token)
    print("---")

if __name__ == "__main__":
    main()
