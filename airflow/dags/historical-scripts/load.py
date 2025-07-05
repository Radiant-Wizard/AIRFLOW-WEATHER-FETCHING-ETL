import os
import mimetypes
import json
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()
# Configuration
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# I encoded my credentials to base64 so i need to decode it
GOOGLE_SERVICE_ACCOUNT_JSON = base64.b64decode(str(GOOGLE_SERVICE_ACCOUNT_JSON))
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
MIMETYPE_FOLDER = "application/vnd.google-apps.folder"

# creating the googleApiService
def get_service(account_service_info : str):
    info_json = json.loads(account_service_info)
    credentials = service_account.Credentials.from_service_account_info(
        info_json, scopes=SCOPES
    )

    return build("drive", version="v3", credentials=credentials)


def get_or_create_folder(service, name, parent_id, cache):
    key = (parent_id, name)
    if key in cache:
        return cache[key]
    
    query = (
        f"'{parent_id}' in parents and name = '{name}'"
        f"and mimeType = '{MIMETYPE_FOLDER}' and trashed = false"
    )
    
    results = service.files().list(q=query, fields="files(id)", pageSize=1).execute()
    files = results.get("files", [])
    if files:
        folder_id = files[0]["id"]
    else:
        metadata = {
            "name": name,
            "mimeType": MIMETYPE_FOLDER,
            "parents": [parent_id]
        }   
        folder = service.files().create(
            body=metadata,
            fields="id"
        ).execute()
        folder_id = folder["id"]
        
    cache[key] = folder_id
    return folder_id
    
def find_file_id(service, name, parent_id):
    query = (
        f"'{parent_id}' in parents and name = '{name}' "
        f"and mimeType != '{MIMETYPE_FOLDER}' and trashed = false"
    )
    response = service.files().list(q=query, fields="files(id)", pageSize=1).execute()
    return response["files"][0]["id"] if response.get("files") else None


def upload_or_update_file(service, filepath : Path, parent_id : str, mode : str):
    file_id = find_file_id(service, filepath.name, parent_id)
    mimetype, _ = mimetypes.guess_type(filepath)
    media = MediaFileUpload(filepath, mimetype=mimetype, resumable=True)
    
    if file_id and mode == "update":
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"file {file_id} has been updated")
    elif file_id and mode == "create":
        print(f"file {file_id} already exit, skipped")
    else:
        metadata = {
                "name": filepath.name,
                "parents": [parent_id]
            }

        file_id = service.files().create(
            body=metadata,
            media_body=media,
            fields="id"
        ).execute()["id"]
        print(f"file {file_id} has been uploaded")

def sync_directory(service, local_base : Path, drive_base_id : str, mode : str):
    folder_cache = {}
    for dirpath, _, filenames in os.walk(local_base):
        relative_part = Path(dirpath).relative_to(local_base).parts
        parent_id = drive_base_id
        
        for part in relative_part:
            parent_id = get_or_create_folder(service, part, parent_id, folder_cache)
            
        for file_name in filenames:
            upload_or_update_file(service, Path(dirpath) / file_name, parent_id, mode)
            
def main(account_service_info : str, drive_folder_id : str):
    base_dir = Path(os.getenv('AIRFLOW_HOME', Path(__file__).parent.parent))

    if not base_dir.is_dir():
        raise SystemExit(f"Source directory '{base_dir}' not found.")
    
    service = get_service(account_service_info)
    sync_directory(service, base_dir / "historical-data", drive_folder_id, "update")
    
if __name__ == "__main__":
    main(GOOGLE_SERVICE_ACCOUNT_JSON, str(DRIVE_FOLDER_ID)) # type: ignore