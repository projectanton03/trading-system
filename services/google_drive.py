"""
Google Drive Integration Service
Functions for reading/writing Excel files to/from Google Drive
"""

import os
import io
import logging
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive']

def get_credentials():
    """Get and refresh service account credentials"""
    import config
    credentials = service_account.Credentials.from_service_account_file(
        config.GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    # Refresh credentials to get access token
    credentials.refresh(Request())
    return credentials

def get_drive_service():
    """
    Create and return Google Drive service object
    """
    try:
        credentials = get_credentials()
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Google Drive service created successfully")
        return service
    except Exception as e:
        logger.error(f"Failed to create Google Drive service: {e}")
        raise

def test_drive_connection():
    """Test Google Drive connection"""
    try:
        service = get_drive_service()
        results = service.files().list(
            pageSize=1,
            fields="files(id, name)"
        ).execute()
        logger.info("Google Drive connection test successful")
        return True, "Google Drive connection successful"
    except Exception as e:
        error_msg = f"Google Drive connection failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def list_files_in_folder(folder_id, file_type=None):
    """List all files in a Google Drive folder"""
    try:
        service = get_drive_service()
        query = f"'{folder_id}' in parents and trashed=false"
        if file_type:
            query += f" and mimeType='{file_type}'"
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, modifiedTime, size)",
            orderBy="name"
        ).execute()
        files = results.get('files', [])
        logger.info(f"Found {len(files)} files in folder {folder_id}")
        return files
    except HttpError as e:
        logger.error(f"Error listing folder {folder_id}: {e}")
        raise

def download_file_as_bytes(file_id):
    """
    Download file from Google Drive using requests library
    (More reliable SSL handling than googleapiclient)
    """
    try:
        # Get fresh credentials with access token
        credentials = get_credentials()
        access_token = credentials.token

        # Use requests library for download (better SSL support)
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(
            url,
            headers=headers,
            timeout=60,
            stream=True
        )
        response.raise_for_status()

        content = response.content
        logger.info(f"File {file_id} downloaded successfully ({len(content)} bytes)")
        return content

    except Exception as e:
        logger.error(f"Error downloading file {file_id}: {e}")
        raise

def download_file(file_id, local_path):
    """
    Download file from Google Drive to local path
    """
    try:
        content = download_file_as_bytes(file_id)

        # Ensure directory exists
        dir_path = os.path.dirname(local_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        with open(local_path, 'wb') as f:
            f.write(content)

        logger.info(f"File {file_id} saved to {local_path}")
        return local_path

    except Exception as e:
        logger.error(f"Error saving file {file_id}: {e}")
        raise

def upload_file(local_path, file_id=None, folder_id=None, file_name=None):
    """
    Upload file to Google Drive using requests library
    """
    try:
        credentials = get_credentials()
        access_token = credentials.token

        if not file_name:
            file_name = os.path.basename(local_path)

        mime_type = 'application/octet-stream'
        if local_path.endswith('.xlsx') or local_path.endswith('.xlsm'):
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        with open(local_path, 'rb') as f:
            file_content = f.read()

        headers = {"Authorization": f"Bearer {access_token}"}

        if file_id:
            # Update existing file
            url = f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media"
            response = requests.patch(
                url,
                headers={**headers, "Content-Type": mime_type},
                data=file_content,
                timeout=120
            )
            response.raise_for_status()
            logger.info(f"File {file_id} updated successfully")
            return response.json()
        else:
            # Create new file - metadata first
            import json
            metadata = {"name": file_name}
            if folder_id:
                metadata["parents"] = [folder_id]

            # Multipart upload
            boundary = "boundary_trading_system"
            body = (
                f"--{boundary}\r\n"
                f"Content-Type: application/json\r\n\r\n"
                f"{json.dumps(metadata)}\r\n"
                f"--{boundary}\r\n"
                f"Content-Type: {mime_type}\r\n\r\n"
            ).encode() + file_content + f"\r\n--{boundary}--".encode()

            url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
            response = requests.post(
                url,
                headers={
                    **headers,
                    "Content-Type": f"multipart/related; boundary={boundary}"
                },
                data=body,
                timeout=120
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"File created: {result.get('id')}")
            return result

    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise

def find_file_by_name(file_name, folder_id=None):
    """Find file ID by name"""
    try:
        service = get_drive_service()
        query = f"name='{file_name}' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=1
        ).execute()
        files = results.get('files', [])
        if files:
            return files[0]['id']
        return None
    except HttpError as e:
        logger.error(f"Error finding file: {e}")
        raise

def get_file_metadata(file_id):
    """Get metadata for a file"""
    try:
        service = get_drive_service()
        return service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size, modifiedTime, createdTime, parents"
        ).execute()
    except HttpError as e:
        logger.error(f"Error getting file metadata: {e}")
        raise

def create_folder(folder_name, parent_folder_id=None):
    """Create a new folder in Google Drive"""
    try:
        service = get_drive_service()
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        folder = service.files().create(
            body=file_metadata,
            fields='id, name'
        ).execute()
        logger.info(f"Folder created: {folder['name']} ({folder['id']})")
        return folder['id']
    except HttpError as e:
        logger.error(f"Error creating folder: {e}")
        raise
