import asyncio
import logging
import pandas as pd
import httpx
import os
import re
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fetch_notes.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

INPUT_CSV_FILE = "Downloaddatanotes.csv"
OUTPUT_CSV_FILE = "Downloaddatanotes_with_notes.csv"

class NoteFetcher:
    def __init__(self):
        self.base_url = "https://services.leadconnectorhq.com"
        self.subaccounts_map = {str(s['id']): s for s in settings.subaccounts_list}
        # Use configured concurrency limit
        self.semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def get_notes(self, client: httpx.AsyncClient, contact_id: str) -> list:
        try:
            response = await client.get(f"{self.base_url}/contacts/{contact_id}/notes")
            if response.status_code == 200:
                data = response.json()
                return data.get('notes', [])
            elif response.status_code == 404:
                logger.warning(f"Contact {contact_id} not found (404)")
                return []
            else:
                logger.error(f"Failed to get notes for contact {contact_id}: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception getting notes for contact {contact_id}: {str(e)}")
            return []

    async def process_row(self, row_data: dict) -> dict:
        async with self.semaphore:
            # Normalize keys to handle potential case differences or whitespace
            # Assuming standard keys based on previous files, but being robust
            contact_id = str(row_data.get('Contact ID', '')).strip()
            account_id = str(row_data.get('Account Id', '')).strip()
            
            # If keys are missing, try to find them case-insensitively
            if not contact_id:
                for k in row_data.keys():
                    if k.lower().replace('_', '').replace(' ', '') == 'contactid':
                        contact_id = str(row_data[k]).strip()
                        break
            
            if not account_id:
                for k in row_data.keys():
                    if k.lower().replace('_', '').replace(' ', '') == 'accountid':
                        account_id = str(row_data[k]).strip()
                        break

            if not contact_id or not account_id:
                logger.warning(f"Skipping row with missing ID: Contact={contact_id}, Account={account_id}")
                return row_data

            subaccount = self.subaccounts_map.get(account_id)
            if not subaccount:
                logger.warning(f"Subaccount {account_id} not found in configuration")
                return row_data

            access_token = subaccount.get('access_token')
            if not access_token:
                logger.warning(f"No access token for subaccount {account_id}")
                return row_data

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Version": "2021-07-28",
                "Accept": "application/json"
            }

            async with httpx.AsyncClient(headers=headers, timeout=30) as client:
                logger.info(f"Fetching notes for contact {contact_id} (Account {account_id})...")
                notes = await self.get_notes(client, contact_id)
                
                if notes:
                    logger.info(f"Found {len(notes)} notes for contact {contact_id}")
                    
                    # Filter out WAVV Call notes and clean patterns
                    valid_notes = []
                    for note in notes:
                        note_body = note.get('body', '')
                        if note_body:
                            # Remove [ WAVV: ... ] pattern
                            note_body = re.sub(r'\[\s*WAVV:\s*[a-f0-9-]+\s*\]', '', note_body).strip()
                            
                            # Skip if empty after cleaning or starts with "WAVV Call"
                            if note_body and not note_body.startswith("WAVV Call"):
                                valid_notes.append(note_body)
                    
                    for i, note_body in enumerate(valid_notes):
                        # Add to row data
                        row_data[f'note{i+1}'] = note_body
                else:
                    logger.info(f"No notes found for contact {contact_id}")
            
            return row_data

    async def run(self):
        # Check current directory first
        file_path = INPUT_CSV_FILE
        if not os.path.exists(file_path):
            # Check Downloads folder as fallback
            downloads_path = os.path.expanduser("~/Downloads")
            fallback_path = os.path.join(downloads_path, INPUT_CSV_FILE)
            if os.path.exists(fallback_path):
                logger.info(f"Input file found in Downloads: {fallback_path}")
                file_path = fallback_path
            else:
                logger.error(f"Input file '{INPUT_CSV_FILE}' not found in current directory or Downloads folder.")
                return

        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path}")
            
            # Convert to list of dicts for processing
            rows = df.to_dict('records')
            
            tasks = []
            for row in rows:
                tasks.append(self.process_row(row))
            
            # Run all tasks
            updated_rows = await asyncio.gather(*tasks)
            
            # Create new DataFrame
            result_df = pd.DataFrame(updated_rows)
            
            # Save to new CSV
            result_df.to_csv(OUTPUT_CSV_FILE, index=False)
            logger.info(f"Processing complete. Saved to {OUTPUT_CSV_FILE}")
            
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    fetcher = NoteFetcher()
    asyncio.run(fetcher.run())
