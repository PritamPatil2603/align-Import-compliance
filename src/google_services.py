import os
import logging
from typing import List, Dict, Optional
from decimal import Decimal
import pandas as pd
import pickle

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

class GoogleServicesManager:
    """Manages Google Drive and Sheets operations for production use"""
    
    def __init__(self, credentials_path: str, sheets_id: str):
        self.credentials_path = credentials_path
        self.sheets_id = sheets_id
        self.drive_service = None
        self.sheets_service = None
        self.token_path = "credentials/token.pickle"
        
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google APIs using OAuth2"""
        SCOPES = [
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly'
        ]
        
        creds = None
        
        # Load existing token
        if os.path.exists(self.token_path):
            with open(self.token_path, 'rb') as token:
                creds = pickle.load(token)
        
        # If there are no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            os.makedirs(os.path.dirname(self.token_path), exist_ok=True)
            with open(self.token_path, 'wb') as token:
                pickle.dump(creds, token)
        
        # Build services
        self.drive_service = build('drive', 'v3', credentials=creds)
        self.sheets_service = build('sheets', 'v4', credentials=creds)
        
        logger.info("✅ Google APIs authenticated successfully")
    
    def get_esn_declared_amount(self, esn: str) -> Optional[Decimal]:
        """Get declared amount for ESN from Google Sheets - PRODUCTION VERSION"""
        try:
            # Read the entire spreadsheet
            range_name = "Sheet1!A:Z"  # Adjust if your data is in different sheet
            
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.sheets_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                logger.warning("No data found in Google Sheets")
                return None
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(values[1:], columns=values[0])
            logger.info(f"Loaded spreadsheet with {len(df)} rows")
            
            # Clean column names (remove extra spaces)
            df.columns = df.columns.str.strip()
            
            # Find ESN and amount columns - based on your screenshot
            esn_column = None
            amount_column = None
            
            # Look for ESN column (Entry Summary Number)
            for col in df.columns:
                if 'entry' in col.lower() and 'summary' in col.lower() and 'number' in col.lower():
                    esn_column = col
                    break
            
            # Look for amount column (Line Tariff Goods Value Amount)
            for col in df.columns:
                if ('line' in col.lower() and 'tariff' in col.lower() and 
                    'goods' in col.lower() and 'value' in col.lower() and 'amount' in col.lower()):
                    amount_column = col
                    break
            
            if not esn_column:
                logger.error(f"Could not find ESN column. Available columns: {list(df.columns)}")
                # Fallback - try to find column with 'Entry' or 'ESN'
                for col in df.columns:
                    if 'entry' in col.lower() or 'esn' in col.lower():
                        esn_column = col
                        break
                
                if not esn_column:
                    logger.error("No suitable ESN column found")
                    return None
            
            if not amount_column:
                logger.error(f"Could not find amount column. Available columns: {list(df.columns)}")
                # Fallback - try to find column with 'amount' or 'value'
                for col in df.columns:
                    if 'amount' in col.lower() or 'value' in col.lower():
                        amount_column = col
                        break
                
                if not amount_column:
                    logger.error("No suitable amount column found")
                    return None
            
            logger.info(f"Using ESN column: '{esn_column}', Amount column: '{amount_column}'")
            
            # Find the specific ESN
            esn_row = df[df[esn_column].astype(str).str.strip() == esn.strip()]
            
            if esn_row.empty:
                logger.warning(f"ESN {esn} not found in column '{esn_column}'")
                return None
            
            # Extract and clean amount
            amount_str = str(esn_row[amount_column].iloc[0])
            
            # Clean amount: remove $, commas, spaces
            clean_amount = amount_str.replace('$', '').replace(',', '').replace(' ', '').strip()
            
            try:
                amount = Decimal(clean_amount)
                logger.info(f"Found declared amount for {esn}: ${amount}")
                return amount
            except:
                logger.error(f"Could not parse amount '{amount_str}' for ESN {esn}")
                return None
                
        except Exception as e:
            logger.error(f"Error reading Google Sheets for ESN {esn}: {e}")
            return None
    
    def get_all_esn_folders(self) -> List[Dict[str, str]]:
        """Get all ESN folders from Google Drive - PRODUCTION VERSION"""
        try:
            logger.info("🔍 Searching for ESN folders in Google Drive...")
            
            # Search for folders that match ESN pattern (AE followed by numbers)
            query = "mimeType='application/vnd.google-apps.folder' and name contains 'AE'"
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, parents)",
                pageSize=1000  # Get up to 1000 folders
            ).execute()
            
            folders = results.get('files', [])
            logger.info(f"Found {len(folders)} potential ESN folders")
            
            # Filter to valid ESN format (AE + 9 digits) like AE900683929
            esn_folders = []
            for folder in folders:
                folder_name = folder['name'].strip()
                if (folder_name.startswith('AE') and 
                    len(folder_name) >= 11 and 
                    folder_name[2:11].isdigit()):  # Check if next 9 chars are digits
                    
                    esn_folders.append({
                        'esn': folder_name,
                        'folder_id': folder['id'],
                        'folder_name': folder_name
                    })
                    logger.debug(f"Valid ESN folder: {folder_name}")
            
            logger.info(f"✅ Found {len(esn_folders)} valid ESN folders")
            return esn_folders
            
        except Exception as e:
            logger.error(f"Error getting ESN folders: {e}")
            return []
    
    def get_commercial_invoices_files(self, esn_folder_id: str) -> List[Dict[str, str]]:
        """Get PDF files from COMMERCIAL INVOICES subfolder - PRODUCTION VERSION"""
        try:
            # Search for COMMERCIAL INVOICES subfolder (exact match)
            commercial_query = (
                f"'{esn_folder_id}' in parents and "
                f"name='COMMERCIAL INVOICES' and "
                f"mimeType='application/vnd.google-apps.folder'"
            )
            
            commercial_results = self.drive_service.files().list(
                q=commercial_query,
                fields="files(id, name)"
            ).execute()
            
            commercial_folders = commercial_results.get('files', [])
            
            if not commercial_folders:
                logger.warning(f"No 'COMMERCIAL INVOICES' folder found in ESN folder {esn_folder_id}")
                return []
            
            commercial_folder_id = commercial_folders[0]['id']
            logger.debug(f"Found COMMERCIAL INVOICES folder: {commercial_folder_id}")
            
            # Get PDF files from commercial invoices folder
            pdf_query = (
                f"'{commercial_folder_id}' in parents and "
                f"mimeType='application/pdf'"
            )
            
            pdf_results = self.drive_service.files().list(
                q=pdf_query,
                fields="files(id, name, size, modifiedTime)"
            ).execute()
            
            pdf_files = pdf_results.get('files', [])
            logger.info(f"Found {len(pdf_files)} PDF files in COMMERCIAL INVOICES")
            
            return pdf_files
            
        except Exception as e:
            logger.error(f"Error getting commercial invoice files: {e}")
            return []
    
    def download_file(self, file_id: str, local_path: str) -> bool:
        """Download file from Google Drive - PRODUCTION VERSION"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as file:
                downloader = MediaIoBaseDownload(file, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download progress: {int(status.progress() * 100)}%")
            
            # Verify file was downloaded
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                logger.debug(f"✅ Downloaded file to: {local_path}")
                return True
            else:
                logger.error(f"❌ File download failed or empty: {local_path}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error downloading file {file_id}: {e}")
            return False