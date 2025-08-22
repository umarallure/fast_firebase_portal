import os
import pandas as pd
import httpx
import time
import tempfile
import json
from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse
from dotenv import load_dotenv
from difflib import get_close_matches

load_dotenv()
router = APIRouter()


API_BASE = os.getenv('LC_API_BASE', 'https://services.leadconnectorhq.com')
API_VERSION = os.getenv('LC_API_VERSION', '2021-07-28')
SUBACCOUNTS = json.loads(os.getenv('SUBACCOUNTS', '[]'))

# Helper: get access_token and location_id for account
def get_account_auth(account_id):
    for sub in SUBACCOUNTS:
        # Support both string and int id
        if str(sub.get('id')) == str(account_id):
            return sub.get('access_token'), sub.get('location_id')
    return None, None

# Helper: normalize name
def normalize(name):
    if not name:
        return ''
    return ' '.join(name.lower().replace('-', ' ').replace('_', ' ').split())

# Helper: fuzzy match
def fuzzy_match(name, choices):
    norm_name = normalize(name)
    norm_choices = [normalize(c) for c in choices]
    matches = get_close_matches(norm_name, norm_choices, n=1, cutoff=0.8)
    if matches:
        idx = norm_choices.index(matches[0])
        return choices[idx]
    # Levenshtein fallback
    for c in choices:
        if norm_name.startswith(normalize(c)) or abs(len(norm_name) - len(normalize(c))) <= 2:
            return c
    return None

async def fetch_pipelines(account_id, token):
    url = f"{API_BASE}/opportunities/pipelines?locationId={account_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Version': API_VERSION
    }
    async with httpx.AsyncClient() as client:
        for attempt in range(4):
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.json().get('pipelines', [])
            elif resp.status_code in [429, 500]:
                time.sleep(2 ** attempt)
                continue
            else:
                break
    return []

async def update_opportunity(opp_id, pipeline_id, stage_id, token, dry_run=False):
    if dry_run:
        return True, None
    url = f"{API_BASE}/opportunities/{opp_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Version': API_VERSION,
        'Content-Type': 'application/json'
    }
    payload = {
        'pipelineId': pipeline_id,
        'pipelineStageId': stage_id
    }
    async with httpx.AsyncClient() as client:
        for attempt in range(4):
            resp = await client.put(url, headers=headers, json=payload)
            if resp.status_code == 200:
                return True, None
            elif resp.status_code in [429, 500]:
                time.sleep(2 ** attempt)
                continue
            else:
                return False, f"HTTP {resp.status_code}: {resp.text}"
    return False, f"Failed after retries"

# Helper: create note for contact
async def create_note(contact_id, note, token, dry_run=False):
    if dry_run:
        return True, None
    url = f"{API_BASE}/contacts/{contact_id}/notes"
    headers = {
        'Authorization': f'Bearer {token}',
        'Version': API_VERSION,
        'Content-Type': 'application/json'
    }
    user_id = 'sYUZNa30qZ9uH5V7hgXe'
    payload = {
        'userId': user_id,
        'body': note
    }
    async with httpx.AsyncClient() as client:
        for attempt in range(4):
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code == 201:
                return True, None
            elif resp.status_code in [429, 500]:
                time.sleep(2 ** attempt)
                continue
            else:
                return False, f"HTTP {resp.status_code}: {resp.text}"
    return False, f"Failed after retries"

@router.post('/bulk-update-opportunity-stage')
async def bulk_update_opportunity_stage(file: UploadFile = File(...), dry_run: bool = Form(True)):
    """
    Bulk update opportunity pipeline/stage from uploaded CSV/Excel file.
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name
    if file.filename.endswith('.csv'):
        df = pd.read_csv(tmp_path, dtype=str)
    else:
        df = pd.read_excel(tmp_path, dtype=str)
    os.remove(tmp_path)
    required_cols = ['Opportunity ID', 'pipeline', 'stage', 'Account Id']
    for col in required_cols:
        if not any([col.lower() == c.lower() for c in df.columns]):
            return JSONResponse({'error': f'Missing required column: {col}'}, status_code=400)
    results = []
    total = len(df)
    updated = 0
    skipped = 0
    # Cache pipelines per account
    pipeline_cache = {}
    for idx, row in df.iterrows():
        opp_id = row.get('Opportunity ID') or row.get('opportunityId')
        pipeline_name = row.get('pipeline')
        stage_name = row.get('stage')
        account_id = row.get('Account Id') or row.get('accountId')
        contact_id = row.get('Contact ID') or row.get('contactId')
        newnote = row.get('newnote')
        if not opp_id or not pipeline_name or not stage_name or not account_id:
            results.append({'row': idx+2, 'reason': 'Missing required fields'})
            skipped += 1
            continue
        access_token, location_id = get_account_auth(account_id)
        if not access_token or not location_id:
            results.append({'row': idx+2, 'reason': 'No access_token/location_id for Account Id'})
            skipped += 1
            continue
        # Fetch pipelines for account (cache)
        if location_id not in pipeline_cache:
            pipeline_cache[location_id] = await fetch_pipelines(location_id, access_token)
        pipelines = pipeline_cache[location_id]
        # Match pipeline
        pipeline_id = None
        stage_id = None
        pipeline_choices = [p['name'] for p in pipelines]
        matched_pipeline = fuzzy_match(pipeline_name, pipeline_choices)
        if not matched_pipeline:
            results.append({'row': idx+2, 'reason': 'Pipeline not found'})
            skipped += 1
            continue
        pipeline_obj = next((p for p in pipelines if p['name'] == matched_pipeline), None)
        pipeline_id = pipeline_obj['id'] if pipeline_obj else None
        # Match stage
        stage_choices = [s['name'] for s in pipeline_obj.get('stages', [])]
        matched_stage = fuzzy_match(stage_name, stage_choices)
        if not matched_stage:
            results.append({'row': idx+2, 'reason': 'Stage not found'})
            skipped += 1
            continue
        stage_obj = next((s for s in pipeline_obj.get('stages', []) if s['name'] == matched_stage), None)
        stage_id = stage_obj['id'] if stage_obj else None
        # Update opportunity
        ok, err = await update_opportunity(opp_id, pipeline_id, stage_id, access_token, dry_run)
        if ok:
            updated += 1
        else:
            results.append({'row': idx+2, 'reason': err})
            skipped += 1
        # Bulk notes: if newnote exists, create note for contact
        if contact_id and newnote:
            note_ok, note_err = await create_note(contact_id, newnote, access_token, dry_run)
            if not note_ok:
                results.append({'row': idx+2, 'contactId': contact_id, 'reason': f'Note error: {note_err}'})
    # Write log
    log_path = None
    if results:
        log_df = pd.DataFrame(results)
        log_path = os.path.join(tempfile.gettempdir(), 'opportunity_update_log.csv')
        log_df.to_csv(log_path, index=False)
    summary = {
        'totalRows': total,
        'updated': updated,
        'skipped': skipped,
        'skippedDetails': results,
        'logFile': log_path
    }
    return JSONResponse(summary)

@router.get('/bulk-update-opportunity-stage/log')
def download_log():
    log_path = os.path.join(tempfile.gettempdir(), 'opportunity_update_log.csv')
    if os.path.exists(log_path):
        return FileResponse(log_path, media_type='text/csv', filename='opportunity_update_log.csv')
    return JSONResponse({'error': 'No log file found'}, status_code=404)
