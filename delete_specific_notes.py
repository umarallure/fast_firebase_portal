import asyncio
import logging
import pandas as pd
import httpx
import json
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("note_deletion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Target note text to delete
TARGET_NOTE_TEXT = "This prospect has caused multiple chargebacks in our agency, so we will not be accepting additional applications for this individual"

CSV_FILE_PATH = "Leads Update New TEST - Notes to delete.csv"

class NoteDeleter:
    def __init__(self):
        self.base_url = "https://services.leadconnectorhq.com"
        self.subaccounts_map = {str(s['id']): s for s in settings.subaccounts_list}
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent requests

    async def get_notes(self, client: httpx.AsyncClient, contact_id: str) -> list:
        try:
            response = await client.get(f"{self.base_url}/contacts/{contact_id}/notes")
            if response.status_code == 200:
                data = response.json()
                return data.get('notes', [])
            else:
                logger.error(f"Failed to get notes for contact {contact_id}: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception getting notes for contact {contact_id}: {str(e)}")
            return []

    async def delete_note(self, client: httpx.AsyncClient, contact_id: str, note_id: str):
        try:
            response = await client.delete(f"{self.base_url}/contacts/{contact_id}/notes/{note_id}")
            if response.status_code == 200:
                logger.info(f"Successfully deleted note {note_id} for contact {contact_id}")
                return True
            else:
                logger.error(f"Failed to delete note {note_id} for contact {contact_id}: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception deleting note {note_id} for contact {contact_id}: {str(e)}")
            return False

    async def process_row(self, row):
        async with self.semaphore:
            contact_id = str(row.get('Contact ID', '')).strip()
            account_id = str(row.get('Account Id', '')).strip()
            contact_name = str(row.get('Contact Name', 'Unknown'))

            if not contact_id or not account_id:
                logger.warning(f"Skipping row with missing ID: Contact={contact_id}, Account={account_id}")
                return

            logger.info(f"STARTING: Contact '{contact_name}' ({contact_id}) | Account {account_id}")

            subaccount = self.subaccounts_map.get(account_id)
            if not subaccount:
                logger.warning(f"Subaccount {account_id} not found in configuration")
                return

            access_token = subaccount.get('access_token')
            if not access_token:
                logger.warning(f"No access token for subaccount {account_id}")
                return

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Version": "2021-07-28",
                "Accept": "application/json"
            }

            async with httpx.AsyncClient(headers=headers, timeout=30) as client:
                # 1. Get all notes
                logger.info(f"Fetching notes for contact {contact_id}...")
                notes = await self.get_notes(client, contact_id)
                logger.info(f"Found {len(notes)} notes for contact {contact_id}")
                
                # 2. Find all target notes
                target_note_ids = []
                for note in notes:
                    note_body = note.get('body', '').strip()
                    # Check for exact match or if the target text is contained (to be safe against whitespace/formatting)
                    if note_body == TARGET_NOTE_TEXT or TARGET_NOTE_TEXT in note_body:
                        note_id = note.get('id')
                        target_note_ids.append(note_id)
                        logger.info(f"MATCH FOUND: Note {note_id} contains target text.")
                
                if target_note_ids:
                    logger.info(f"Found {len(target_note_ids)} matching notes to delete for {contact_name} ({contact_id})")
                    # 3. Delete notes
                    for note_id in target_note_ids:
                        logger.info(f"DELETING note {note_id}...")
                        success = await self.delete_note(client, contact_id, note_id)
                        if success:
                            logger.info(f"SUCCESS: Deleted note {note_id} for {contact_name} ({contact_id})")
                        else:
                            logger.error(f"FAILURE: Could not delete note {note_id} for {contact_name} ({contact_id})")
                else:
                    logger.info(f"NO MATCH: Target note not found for contact {contact_name} ({contact_id})")

    async def run(self):
        try:
            df = pd.read_csv(CSV_FILE_PATH)
            logger.info(f"Loaded {len(df)} rows from {CSV_FILE_PATH}")
            
            tasks = []
            for _, row in df.iterrows():
                tasks.append(self.process_row(row))
            
            await asyncio.gather(*tasks)
            logger.info("Processing complete")
            
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    deleter = NoteDeleter()
    asyncio.run(deleter.run())
