import os
import pandas as pd
import requests
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# CONFIGURATION
API_BASE = os.getenv('LC_API_BASE', 'https://services.leadconnectorhq.com')
API_VERSION = os.getenv('LC_API_VERSION', '2021-07-28')
CENTERS_JSON = os.getenv('CENTERS_JSON', '{}')
USER_ID = os.getenv('GHL_USER_ID', '')  # Set your GHL userId here

# INPUT CSV
INPUT_CSV = 'ghl_opportunities_export (5).csv'  # Change if needed

# OUTPUT LOG
LOG_CSV = 'notes_creation_log.csv'

# Load subaccount tokens
import json
try:
    CENTERS = json.loads(CENTERS_JSON)
except Exception:
    CENTERS = {}

def get_token_for_account(account_id):
    for center in CENTERS.values():
        if center.get('locationId') == account_id:
            return center.get('token')
    return None

def create_note(contact_id, note, token, user_id):
    url = f"{API_BASE}/contacts/{contact_id}/notes"
    headers = {
        'Authorization': f'Bearer {token}',
        'Version': API_VERSION,
        'Content-Type': 'application/json'
    }
    payload = {
        'userId': user_id,
        'body': note
    }
    for attempt in range(4):
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 201:
            return True, None
        elif resp.status_code in [429, 500]:
            time.sleep(2 ** attempt)
            continue
        else:
            return False, f"HTTP {resp.status_code}: {resp.text}"
    return False, f"Failed after retries: {resp.text}"

def main():
    df = pd.read_csv(INPUT_CSV, dtype=str)
    results = []
    total = len(df)
    success = 0
    failed = 0
    for idx, row in df.iterrows():
        contact_id = row.get('Contact ID') or row.get('contactId')
        note = row.get('newnote') or row.get('note1')  # Use 'newnote' if present, else 'note1' for demo
        account_id = row.get('Account Id') or row.get('accountId')
        if not contact_id or not note or not account_id:
            results.append({'row': idx+2, 'contactId': contact_id, 'reason': 'Missing contactId, note, or accountId'})
            failed += 1
            continue
        token = get_token_for_account(account_id)
        if not token:
            results.append({'row': idx+2, 'contactId': contact_id, 'reason': 'No token for Account Id'})
            failed += 1
            continue
        ok, err = create_note(contact_id, note, token, USER_ID)
        if ok:
            success += 1
        else:
            results.append({'row': idx+2, 'contactId': contact_id, 'reason': err})
            failed += 1
    # Write log
    log_df = pd.DataFrame(results)
    log_df.to_csv(LOG_CSV, index=False)
    # Print summary
    print({
        'totalRows': total,
        'notesCreated': success,
        'failed': failed,
        'failedDetails': results
    })

if __name__ == '__main__':
    main()
