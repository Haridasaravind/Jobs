import os
import base64
import pickle
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# =========================
# CONFIG
# =========================
SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "credentials.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.json")

# =========================
# AUTHENTICATION
# =========================
def get_gmail_service():
    creds = None

    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE,
                SCOPES
            )
            creds = flow.run_local_server(
                port=0,
                prompt="consent",
                authorization_prompt_message=""
            )

        with open(TOKEN_FILE, "wb") as token:
            pickle.dump(creds, token)

    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    print("📧 Authenticated Gmail:", profile["emailAddress"])

    return service

# =========================
# CREATE HTML DRAFT (TO + CC SUPPORT)
# =========================
def create_html_draft(service, to, subject, html_body, cc=None):
    """
    Create a Gmail HTML draft.
    - 'to' is optional
    - 'cc' is optional
    """

    message = MIMEMultipart("alternative")

    # ✅ ONLY SET HEADERS IF PROVIDED
    if to:
        message["To"] = to

    if cc:
        message["Cc"] = cc

    message["Subject"] = subject

    mime_text = MIMEText(html_body, "html")
    message.attach(mime_text)

    raw_message = base64.urlsafe_b64encode(
        message.as_bytes()
    ).decode()

    draft_body = {
        "message": {
            "raw": raw_message
        }
    }

    service.users().drafts().create(
        userId="me",
        body=draft_body
    ).execute()

    if to and cc:
        print(f"✅ Draft created → To: {to}, CC: {cc}")
    elif to:
        print(f"✅ Draft created → To: {to}")
    else:
        print("✅ Draft created (no recipient)")
