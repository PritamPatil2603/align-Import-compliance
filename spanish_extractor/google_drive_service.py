import os
import pickle
import logging
from typing import List, Dict, Optional

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

class SpanishDriveService:
    """Dedicated Google Drive service for Spanish invoice extraction"""
    
    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self.drive_service = None
        self.token_path = "credentials/spanish_token.pickle"
        
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Drive API"""
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        
        creds = None
        
        # Load existing token
        if os.path.exists(self.token_path):
            try:
                with open(self.token_path, 'rb') as token:
                    creds = pickle.load(token)
            except Exception as e:
                logger.warning(f"Error loading token: {e}")
                creds = None
        
        # If no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.warning(f"Error refreshing token: {e}")
                    creds = None
            
            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    raise ValueError(f"Authentication failed: {e}")
            
            # Save credentials
            try:
                os.makedirs(os.path.dirname(self.token_path), exist_ok=True)
                with open(self.token_path, 'wb') as token:
                    pickle.dump(creds, token)
            except Exception as e:
                logger.warning(f"Could not save token: {e}")
        
        # Build service
        try:
            self.drive_service = build('drive', 'v3', credentials=creds)
            logger.info("✅ Spanish Google Drive service authenticated successfully")
        except Exception as e:
            raise ValueError(f"Failed to build Drive service: {e}")
    
    def get_esn_folders(self, root_folder_id: str) -> List[Dict[str, str]]:
        """Get all ESN folders from root directory"""
        try:
            logger.info(f"🔍 Searching for ESN folders in root: {root_folder_id}")
            
            # Search for folders that look like ESNs
            query = (
                f"'{root_folder_id}' in parents and "
                f"mimeType='application/vnd.google-apps.folder' and "
                f"name contains 'AE'"
            )
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, modifiedTime)",
                pageSize=1000
            ).execute()
            
            folders = results.get('files', [])
            logger.info(f"Found {len(folders)} potential ESN folders")
            
            # Filter to actual ESN folders (AE + digits, length 10+)
            esn_folders = []
            for folder in folders:
                name = folder['name']
                if name.startswith('AE') and len(name) >= 10:
                    # Check if it's mostly digits after AE
                    digits_part = name[2:]
                    if digits_part.isdigit() or self._is_esn_like(digits_part):
                        esn_folders.append({
                            'id': folder['id'],
                            'name': folder['name'],
                            'modified_time': folder.get('modifiedTime', '')
                        })
            
            logger.info(f"✅ Found {len(esn_folders)} valid ESN folders")
            return esn_folders
            
        except Exception as e:
            logger.error(f"❌ Error getting ESN folders: {e}")
            return []
    
    def _is_esn_like(self, text: str) -> bool:
        """Check if text looks like an ESN (mostly digits)"""
        if not text:
            return False
        digit_count = sum(1 for c in text if c.isdigit())
        return (digit_count / len(text)) >= 0.8  # At least 80% digits
    
    def get_commercial_invoices_folder(self, esn_folder_id: str) -> Optional[str]:
        """Get COMMERCIAL INVOICES subfolder ID"""
        try:
            logger.debug(f"Looking for COMMERCIAL INVOICES folder in {esn_folder_id}")
            
            query = (
                f"'{esn_folder_id}' in parents and "
                f"mimeType='application/vnd.google-apps.folder'"
            )
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            
            folders = results.get('files', [])
            
            # Look for folder with "COMMERCIAL" and "INVOICES" in name
            for folder in folders:
                name = folder['name'].upper()
                if 'COMMERCIAL' in name and 'INVOICE' in name:
                    logger.debug(f"✅ Found commercial invoices folder: {folder['name']}")
                    return folder['id']
            
            logger.warning(f"❌ No COMMERCIAL INVOICES folder found in ESN folder")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting COMMERCIAL INVOICES folder: {e}")
            return None
    
    def get_pdf_files(self, folder_id: str) -> List[Dict[str, str]]:
        """Get all PDF files from folder"""
        try:
            logger.debug(f"Getting PDF files from folder {folder_id}")
            
            query = (
                f"'{folder_id}' in parents and "
                f"mimeType='application/pdf'"
            )
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, size, modifiedTime)",
                pageSize=100
            ).execute()
            
            pdf_files = results.get('files', [])
            logger.debug(f"Found {len(pdf_files)} PDF files")
            
            return pdf_files
            
        except Exception as e:
            logger.error(f"❌ Error getting PDF files: {e}")
            return []
    
    def download_file(self, file_id: str, local_path: str) -> bool:
        """Download file from Google Drive"""
        try:
            logger.debug(f"Downloading file {file_id} to {local_path}")
            
            # Get file metadata
            try:
                file_metadata = self.drive_service.files().get(fileId=file_id, fields="name,size").execute()
                file_name = file_metadata.get('name', 'unknown')
                file_size = int(file_metadata.get('size', 0))
            except Exception as e:
                logger.warning(f"Could not get file metadata: {e}")
                file_name = "unknown"
                file_size = 0
            
            # Request file content
            request = self.drive_service.files().get_media(fileId=file_id)
            
            # Create directory
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download with progress tracking
            with open(local_path, 'wb') as local_file:
                downloader = MediaIoBaseDownload(local_file, request)
                done = False
                downloaded_bytes = 0
                
                while done is False:
                    status, done = downloader.next_chunk()
                    if status:
                        downloaded_bytes = int(status.resumable_progress)
                        if file_size > 0:
                            progress = (downloaded_bytes / file_size) * 100
                            logger.debug(f"Download progress: {progress:.1f}%")
            
            # Verify download
            if os.path.exists(local_path):
                actual_size = os.path.getsize(local_path)
                if actual_size > 0:
                    logger.debug(f"✅ Successfully downloaded {file_name} ({actual_size:,} bytes)")
                    return True
                else:
                    logger.error(f"❌ Downloaded file is empty: {local_path}")
                    return False
            else:
                logger.error(f"❌ Download failed: {local_path} not created")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error downloading file {file_id}: {e}")
            return False
    
    def get_folder_info(self, folder_id: str) -> Optional[Dict[str, str]]:
        """Get folder information"""
        try:
            result = self.drive_service.files().get(
                fileId=folder_id,
                fields="id, name, parents, modifiedTime, createdTime"
            ).execute()
            
            return {
                'id': result.get('id'),
                'name': result.get('name'),
                'parents': result.get('parents', []),
                'modified_time': result.get('modifiedTime'),
                'created_time': result.get('createdTime')
            }
            
        except Exception as e:
            logger.error(f"Error getting folder info: {e}")
            return None
    
    def list_folder_contents(self, folder_id: str) -> List[Dict[str, str]]:
        """List all contents of a folder (for debugging)"""
        try:
            query = f"'{folder_id}' in parents"
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, mimeType, size, modifiedTime)",
                pageSize=1000
            ).execute()
            
            return results.get('files', [])
            
        except Exception as e:
            logger.error(f"Error listing folder contents: {e}")
            return []