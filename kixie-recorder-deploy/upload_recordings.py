"""
Kixie Call Recording Downloader and Google Drive Uploader
Downloads call recordings from CSV file and uploads them to Google Drive
"""

import csv
import os
import requests
import time
from pathlib import Path
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('recording_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file']

class RecordingUploader:
    def __init__(self, csv_file_path, temp_download_dir='temp_recordings', drive_folder_name='Kixie Recordings'):
        self.csv_file_path = csv_file_path
        self.temp_download_dir = temp_download_dir
        self.drive_folder_name = drive_folder_name
        self.drive_service = None
        self.drive_folder_id = None
        self.stats = {
            'total': 0,
            'downloaded': 0,
            'uploaded': 0,
            'skipped': 0,
            'errors': 0
        }
        
        # Create temp directory
        Path(self.temp_download_dir).mkdir(exist_ok=True)
        
    def cleanup_temp_files(self):
        """Clean up temporary recording files to free disk space"""
        try:
            temp_dir = Path("temp_recordings")
            if temp_dir.exists():
                # Remove all files in temp_recordings directory
                for file_path in temp_dir.glob("*"):
                    if file_path.is_file():
                        file_path.unlink()
                logger.info("Cleaned up temporary recording files")
            else:
                logger.debug("Temp recordings directory does not exist")
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")
            raise

    def authenticate_google_drive(self):
        """Authenticate with Google Drive API"""
        creds = None
        token_path = 'credentials/token.json'
        
        # Get credentials path from config or use default
        try:
            config = load_config()
            credentials_path = config.get('credentials_file', 'credentials/credentials.json')
        except:
            credentials_path = 'credentials/credentials.json'
        
        # Check if token.json exists
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        
        # If no valid credentials, let user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(credentials_path):
                    logger.error(f"credentials.json not found at {credentials_path}")
                    logger.error("Please place your credentials.json file in the credentials/ folder")
                    raise FileNotFoundError(f"credentials.json not found at {credentials_path}")
                
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
        
        self.drive_service = build('drive', 'v3', credentials=creds)
        logger.info("Successfully authenticated with Google Drive")
        
    def create_drive_folder(self):
        """Create or get the Google Drive folder"""
        try:
            # Search for existing folder
            query = f"name='{self.drive_folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = results.get('files', [])
            
            if files:
                self.drive_folder_id = files[0]['id']
                logger.info(f"Using existing folder: {self.drive_folder_name} (ID: {self.drive_folder_id})")
            else:
                # Create new folder
                file_metadata = {
                    'name': self.drive_folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                folder = self.drive_service.files().create(body=file_metadata, fields='id').execute()
                self.drive_folder_id = folder.get('id')
                logger.info(f"Created new folder: {self.drive_folder_name} (ID: {self.drive_folder_id})")
                
        except HttpError as error:
            logger.error(f"Error creating/finding folder: {error}")
            raise
            
    def download_recording(self, url, file_path, max_retries=3):
        """Download a recording from URL with retry logic"""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, stream=True, timeout=(10, 300))  # 10s connect, 300s read
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                return True
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to download {url} after {max_retries} attempts")
                    return False
        
        return False
        
    def upload_to_drive(self, file_path, file_name, max_retries=3):
        """Upload a file to Google Drive with retry logic"""
        # Get file size
        file_size = os.path.getsize(file_path)
        
        for attempt in range(max_retries):
            try:
                file_metadata = {
                    'name': file_name,
                    'parents': [self.drive_folder_id]
                }
                
                # Use non-resumable upload for small files (< 5MB)
                if file_size < 5 * 1024 * 1024:
                    media = MediaFileUpload(
                        file_path,
                        mimetype='audio/mpeg',
                        resumable=False
                    )
                else:
                    media = MediaFileUpload(
                        file_path,
                        mimetype='audio/mpeg',
                        resumable=True,
                        chunksize=256*1024  # 256KB chunks for better compatibility
                    )
                
                file = self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, webViewLink'
                ).execute()
                
                # Success - return the file info
                file_id = file.get('id')
                web_link = file.get('webViewLink')
                return file_id, web_link
                
            except Exception as error:
                error_msg = str(error)
                logger.warning(f"Upload attempt {attempt + 1}/{max_retries} failed for {file_name}: {error_msg}")
                
                # If SSL error, wait longer before retry
                if 'SSL' in error_msg or 'ssl' in error_msg.lower():
                    time.sleep(5 * (attempt + 1))
                elif attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to upload {file_name} after {max_retries} attempts")
                    return None, None
        
        return None, None
        
    def process_recording(self, row):
        """Process a single recording (download and upload)"""
        try:
            # Get recording URLs (try both Call Recording and Voicemail Recording)
            recording_url = row.get('Call Recording', '').strip()
            voicemail_url = row.get('Voicemail Recording', '').strip()
            
            url = recording_url if recording_url else voicemail_url
            
            if not url or not url.startswith('http'):
                self.stats['skipped'] += 1
                return None
            
            # Generate unique filename from URL
            # Extract filename from URL (e.g., cfe9fa49-fce0-48d3-8865-6a39b52328b6.mp3)
            url_parts = url.split('/')
            url_filename = url_parts[-1] if url_parts else 'unknown.mp3'
            
            # If URL doesn't have .mp3 extension, add it
            if not url_filename.endswith('.mp3'):
                url_filename += '.mp3'
            
            file_name = url_filename
            
            temp_file_path = os.path.join(self.temp_download_dir, file_name)
            
            # Download recording
            if self.download_recording(url, temp_file_path):
                self.stats['downloaded'] += 1
                file_size = os.path.getsize(temp_file_path) / 1024
                logger.info(f"Downloaded: {file_name} ({file_size:.1f} KB)")
                
                # Upload to Drive
                file_id, web_link = self.upload_to_drive(temp_file_path, file_name)
                
                if file_id:
                    self.stats['uploaded'] += 1
                    logger.info(f"Uploaded: {file_name} -> {web_link}")
                    
                    # Clean up temp file
                    try:
                        os.remove(temp_file_path)
                    except Exception as e:
                        logger.warning(f"Could not delete temp file {temp_file_path}: {e}")
                    
                    return {
                        'file_name': file_name,
                        'drive_id': file_id,
                        'drive_link': web_link,
                        'original_url': url
                    }
                else:
                    self.stats['errors'] += 1
            else:
                self.stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"Error processing recording: {str(e)}")
            self.stats['errors'] += 1
            
        return None
        
    def process_csv(self, batch_size=10, max_workers=5, resume_from_row=0, cleanup_interval=1000):
        """Process the CSV file and upload recordings"""
        logger.info(f"Starting to process CSV file: {self.csv_file_path}")
        if resume_from_row > 0:
            logger.info(f"Resuming from row: {resume_from_row}")
        
        # Authenticate and create folder
        self.authenticate_google_drive()
        self.create_drive_folder()
        
        # Read CSV
        with open(self.csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        self.stats['total'] = len(rows)
        logger.info(f"Found {self.stats['total']} rows in CSV")
        
        # Skip rows before resume point
        if resume_from_row > 0:
            rows = rows[resume_from_row:]
            logger.info(f"Processing {len(rows)} rows starting from row {resume_from_row}")
        
        # Process results storage
        results = []
        processed_count = resume_from_row
        
        # Process recordings with thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_row = {executor.submit(self.process_recording, row): row for row in rows}
            
            # Process completed tasks with progress bar
            try:
                with tqdm(total=len(rows), desc="Processing recordings", initial=0) as pbar:
                    for future in as_completed(future_to_row):
                        result = future.result()
                        if result:
                            results.append(result)
                        pbar.update(1)
                        processed_count += 1
                        
                        # Update resume point every 10 items
                        if processed_count % 10 == 0:
                            try:
                                save_config_value('resume_from_row', str(processed_count))
                            except Exception as e:
                                logger.warning(f"Could not save resume point: {e}")
                            
                            logger.info(f"Progress: {processed_count}/{self.stats['total']} - "
                                      f"Downloaded: {self.stats['downloaded']}, "
                                      f"Uploaded: {self.stats['uploaded']}, "
                                      f"Errors: {self.stats['errors']}, "
                                      f"Skipped: {self.stats['skipped']}")
                        
                        # Clean up temp files periodically
                        if processed_count % cleanup_interval == 0:
                            try:
                                self.cleanup_temp_files()
                                logger.info(f"Cleaned up temp files at {processed_count} recordings")
                            except Exception as e:
                                logger.warning(f"Could not cleanup temp files: {e}")
                                
            except Exception as e:
                logger.warning(f"Progress bar error: {e}, continuing without progress bar")
                # Fallback without progress bar
                for future in as_completed(future_to_row):
                    result = future.result()
                    if result:
                        results.append(result)
                    
                    processed_count += 1
                    
                    # Update resume point every 10 items
                    if processed_count % 10 == 0:
                        try:
                            save_config_value('resume_from_row', str(processed_count))
                        except Exception as e:
                            logger.warning(f"Could not save resume point: {e}")
                        
                        logger.info(f"Progress: {processed_count}/{self.stats['total']} - "
                                  f"Downloaded: {self.stats['downloaded']}, "
                                  f"Uploaded: {self.stats['uploaded']}, "
                                  f"Errors: {self.stats['errors']}, "
                                  f"Skipped: {self.stats['skipped']}")
                    
                    # Clean up temp files periodically
                    if processed_count % cleanup_interval == 0:
                        try:
                            self.cleanup_temp_files()
                            logger.info(f"Cleaned up temp files at {processed_count} recordings")
                        except Exception as e:
                            logger.warning(f"Could not cleanup temp files: {e}")
        
        # Final cleanup
        try:
            self.cleanup_temp_files()
        except Exception as e:
            logger.warning(f"Could not perform final cleanup: {e}")
        
        # Reset resume point on successful completion
        try:
            save_config_value('resume_from_row', '0')
        except Exception as e:
            logger.warning(f"Could not reset resume point: {e}")
        
        # Save results to CSV
        if results:
            output_file = f"upload_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['file_name', 'drive_id', 'drive_link', 'original_url']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            logger.info(f"Saved results to {output_file}")
        
        # Print final statistics
        logger.info("=" * 60)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total rows: {self.stats['total']}")
        logger.info(f"Downloaded: {self.stats['downloaded']}")
        logger.info(f"Uploaded: {self.stats['uploaded']}")
        logger.info(f"Skipped (no URL): {self.stats['skipped']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Success rate: {(self.stats['uploaded'] / self.stats['total'] * 100):.2f}%")
        logger.info("=" * 60)
        
        return results


def load_config():
    """Load configuration from config.ini"""
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['UPLOADER']

def save_config_value(key, value):
    """Save a single config value"""
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')
    if not config.has_section('UPLOADER'):
        config.add_section('UPLOADER')
    config.set('UPLOADER', key, str(value))
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

def main():
    """Main function"""
    # Load configuration from config.ini
    try:
        config = load_config()
        CSV_FILE = config.get('csv_file', 'samplerecordingdownload.csv')
        TEMP_DIR = config.get('temp_dir', 'temp_recordings')
        DRIVE_FOLDER = config.get('drive_folder', 'Kixie Recordings')
        MAX_WORKERS = int(config.get('max_workers', '1'))
        LOG_FILE = config.get('log_file', 'logs/recording_upload.log')
        RESUME_FROM_ROW = int(config.get('resume_from_row', '0'))
        CLEANUP_INTERVAL = int(config.get('cleanup_interval', '1000'))
        
        # Update logging to use config file path
        logging.getLogger().handlers[0].baseFilename = LOG_FILE
        
    except Exception as e:
        print(f"Warning: Could not load config.ini, using defaults: {e}")
        # Fallback to defaults
        CSV_FILE = 'samplerecordingdownload.csv'
        TEMP_DIR = 'temp_recordings'
        DRIVE_FOLDER = 'Kixie Recordings'
        MAX_WORKERS = 1
        LOG_FILE = 'recording_upload.log'
        RESUME_FROM_ROW = 0
        CLEANUP_INTERVAL = 1000
    
    print("=" * 60)
    print("Kixie Recording Downloader & Google Drive Uploader")
    print("=" * 60)
    print(f"CSV File: {CSV_FILE}")
    print(f"Drive Folder: {DRIVE_FOLDER}")
    print(f"Max Parallel Workers: {MAX_WORKERS}")
    print("=" * 60)
    print()
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
        return
    
    # Initialize uploader
    uploader = RecordingUploader(
        csv_file_path=CSV_FILE,
        temp_download_dir=TEMP_DIR,
        drive_folder_name=DRIVE_FOLDER
    )
    
    # Process recordings
    try:
        results = uploader.process_csv(max_workers=MAX_WORKERS, resume_from_row=RESUME_FROM_ROW, cleanup_interval=CLEANUP_INTERVAL)
        print(f"\n✅ Processing complete! Uploaded {len(results)} recordings to Google Drive")
        print(f"📊 Check 'recording_upload.log' for detailed logs")
        print(f"📁 Google Drive Folder: {DRIVE_FOLDER}")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"\n❌ Error occurred: {str(e)}")
        print("Check 'recording_upload.log' for details")


if __name__ == "__main__":
    main()
