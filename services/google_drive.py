"""
Google Drive Integration Service
Functions for reading/writing Excel files to/from Google Drive
"""

import os
import io
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Scopes for Google Drive access
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_drive_service():
    """
    Create and return Google Drive service object
    
    Returns:
        Google Drive API service object
    """
    try:
        # Import config here to avoid circular imports
        import config
        
        credentials = service_account.Credentials.from_service_account_file(
            config.GOOGLE_SERVICE_ACCOUNT_FILE,
            scopes=SCOPES
        )
        
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Google Drive service created successfully")
        return service
        
    except Exception as e:
        logger.error(f"Failed to create Google Drive service: {e}")
        raise

def test_drive_connection():
    """
    Test Google Drive connection
    
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        service = get_drive_service()
        # Try to list files (limit 1)
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
    """
    List all files in a Google Drive folder
    
    Args:
        folder_id: Google Drive folder ID
        file_type: Optional MIME type filter (e.g., 'application/vnd.google-apps.spreadsheet')
    
    Returns:
        list: List of file dictionaries with id, name, mimeType
    """
    try:
        service = get_drive_service()
        
        # Build query
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

def download_file(file_id, local_path):
    """
    Download file from Google Drive to local path
    
    Args:
        file_id: Google Drive file ID
        local_path: Local path to save file
    
    Returns:
        str: Path to downloaded file
    """
    try:
        service = get_drive_service()
        
        request = service.files().get_media(fileId=file_id)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug(f"Download progress: {int(status.progress() * 100)}%")
        
        logger.info(f"File {file_id} downloaded to {local_path}")
        return local_path
        
    except HttpError as e:
        logger.error(f"Error downloading file {file_id}: {e}")
        raise

def upload_file(local_path, file_id=None, folder_id=None, file_name=None):
    """
    Upload file to Google Drive (update existing or create new)
    
    Args:
        local_path: Path to local file
        file_id: Optional - Google Drive file ID to update
        folder_id: Optional - Folder ID for new files
        file_name: Optional - Name for file (defaults to basename)
    
    Returns:
        dict: Uploaded file metadata
    """
    try:
        service = get_drive_service()
        
        if not file_name:
            file_name = os.path.basename(local_path)
        
        # Determine MIME type based on extension
        mime_type = None
        if local_path.endswith('.xlsx') or local_path.endswith('.xlsm'):
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        
        media = MediaFileUpload(
            local_path,
            mimetype=mime_type,
            resumable=True
        )
        
        if file_id:
            # Update existing file
            updated_file = service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id, name, modifiedTime'
            ).execute()
            logger.info(f"File {file_id} updated successfully")
            return updated_file
        else:
            # Create new file
            file_metadata = {'name': file_name}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            created_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, modifiedTime'
            ).execute()
            logger.info(f"File created: {created_file['id']}")
            return created_file
            
    except HttpError as e:
        logger.error(f"Error uploading file: {e}")
        raise

def find_file_by_name(file_name, folder_id=None):
    """
    Find file ID by name (optionally in specific folder)
    
    Args:
        file_name: Name of file to find
        folder_id: Optional folder ID to search within
    
    Returns:
        str or None: File ID if found, None otherwise
    """
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
            logger.info(f"Found file '{file_name}': {files[0]['id']}")
            return files[0]['id']
        else:
            logger.warning(f"File '{file_name}' not found")
            return None
            
    except HttpError as e:
        logger.error(f"Error finding file: {e}")
        raise

def get_file_metadata(file_id):
    """
    Get metadata for a file
    
    Args:
        file_id: Google Drive file ID
    
    Returns:
        dict: File metadata
    """
    try:
        service = get_drive_service()
        
        file_meta = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size, modifiedTime, createdTime, parents"
        ).execute()
        
        return file_meta
        
    except HttpError as e:
        logger.error(f"Error getting file metadata: {e}")
        raise

def create_folder(folder_name, parent_folder_id=None):
    """
    Create a new folder in Google Drive
    
    Args:
        folder_name: Name for the new folder
        parent_folder_id: Optional parent folder ID
    
    Returns:
        str: New folder ID
    """
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
def download_file_as_bytes(file_id):
    """
    Download file from Google Drive and return raw bytes
    """
    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=file_id)

        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        file_stream.seek(0)
        return file_stream.read()

    except HttpError as e:
        logger.error(f"Error downloading file {file_id}: {e}")
        raise
