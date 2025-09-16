import os, time
from typing import Dict, Iterator, Optional, List
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

SCOPES = os.getenv("GOOGLE_SCOPES","https://www.googleapis.com/auth/drive").split()

def get_creds(data_dir: str) -> Credentials:
    token_path = os.path.join(data_dir, "token.json")
    creds_path = os.getenv("GOOGLE_CLIENT_SECRETS", os.path.join(data_dir, "credentials.json"))

    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Web Server Flow: /auth inicia, /oauth2callback conclui
            raise RuntimeError("Missing or invalid token. Run OAuth at /auth first.")
    return creds

def save_token(token_path: str, creds: Credentials):
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(creds.to_json())

def build_flow_for_web(creds_path: str, redirect_uri: str) -> Flow:
    flow = Flow.from_client_secrets_file(
        creds_path,
        scopes=SCOPES,
        redirect_uri=redirect_uri
    )
    return flow

def drive_service(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_children(drive, folder_id: str) -> Iterator[Dict]:
    q = f"'{folder_id}' in parents and trashed = false"
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            fields="nextPageToken, files(id,name,mimeType,md5Checksum,size,modifiedTime,shortcutDetails,parents)",
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        for it in resp.get("files", []):
            yield it
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

def ensure_folder(drive, parent_id: str, name: str) -> str:
    safe_name = name.replace("'", "\\'")
    q = f"'{parent_id}' in parents and name = '{safe_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed=false"
    resp = drive.files().list(q=q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    created = drive.files().create(body=body, fields="id", supportsAllDrives=True).execute()
    return created["id"]

def copy_file_server_side(drive, src_id: str, dest_parent_id: str, name: Optional[str] = None) -> str:
    body = {"parents": [dest_parent_id]}
    if name: body["name"] = name
    file = drive.files().copy(fileId=src_id, supportsAllDrives=True, body=body, fields="id").execute()
    return file["id"]

def trash_if_exists(drive, file_id: str):
    try:
        drive.files().update(fileId=file_id, body={"trashed": True}, supportsAllDrives=True).execute()
    except HttpError:
        pass
