import os
import pandas as pd
import requests
import time
import tempfile
import json
from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

API_BASE = os.getenv('LC_API_BASE', 'https://services.leadconnectorhq.com')
API_VERSION = os.getenv('LC_API_VERSION', '2021-07-28')
CENTERS_JSON = os.getenv('CENTERS_JSON', '{}')
USER_ID = os.getenv('GHL_USER_ID', '')

try:
    CENTERS = json.loads(CENTERS_JSON)
except Exception:
    CENTERS = {}

def get_token_for_account(account_id):
    for center in CENTERS.values():
        if center.get('locationId') == account_id:
            return center.get('token')
    return None

def create_note(contact_id, note, token, user_id, dry_run=False):
    if dry_run:
        return True, None
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

@router.post('/bulk-create-notes')
def bulk_create_notes(file: UploadFile = File(...), dry_run: bool = Form(True)):
    """
    Bulk create notes for contacts from uploaded CSV/Excel file.
    """
    # Save uploaded file to temp
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name
    # Read file
    if file.filename.endswith('.csv'):
        df = pd.read_csv(tmp_path, dtype=str)
    else:
        df = pd.read_excel(tmp_path, dtype=str)
    os.remove(tmp_path)
    results = []
    total = len(df)
    success = 0
    failed = 0
    for idx, row in df.iterrows():
        contact_id = row.get('contactId') or row.get('Contact ID')
        note = row.get('newnote') or row.get('note1')
        account_id = row.get('accountId') or row.get('Account Id')
        if not contact_id or not note or not account_id:
            results.append({'row': idx+2, 'contactId': contact_id, 'reason': 'Missing contactId, note, or accountId'})
            failed += 1
            continue
        token = get_token_for_account(account_id)
        if not token:
            results.append({'row': idx+2, 'contactId': contact_id, 'reason': 'No token for Account Id'})
            failed += 1
            continue
        ok, err = create_note(contact_id, note, token, USER_ID, dry_run)
        if ok:
            success += 1
        else:
            results.append({'row': idx+2, 'contactId': contact_id, 'reason': err})
            failed += 1
    # Write log
    log_path = None
    if results:
        log_df = pd.DataFrame(results)
        log_path = os.path.join(tempfile.gettempdir(), 'notes_creation_log.csv')
        log_df.to_csv(log_path, index=False)
    summary = {
        'totalRows': total,
        'notesCreated': success,
        'failed': failed,
        'failedDetails': results,
        'logFile': log_path
    }
    return JSONResponse(summary)

@router.get('/bulk-create-notes/log')
def download_log():
    log_path = os.path.join(tempfile.gettempdir(), 'notes_creation_log.csv')
    if os.path.exists(log_path):
        return FileResponse(log_path, media_type='text/csv', filename='notes_creation_log.csv')
    return JSONResponse({'error': 'No log file found'}, status_code=404)
