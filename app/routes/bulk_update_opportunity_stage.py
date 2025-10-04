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

async def update_opportunity(opp_id, pipeline_id, stage_id, token, lead_value=None, dry_run=False):
    if dry_run:
        print(f"[DRY RUN] Would update opportunity {opp_id}: pipeline={pipeline_id}, stage={stage_id}, value={lead_value}")
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
    
    # Add lead value if provided
    if lead_value is not None:
        try:
            # Convert to float and then to string to ensure valid number format
            payload['monetaryValue'] = float(lead_value)
            print(f"[DEBUG] Adding monetaryValue: {payload['monetaryValue']} for opportunity {opp_id}")
        except (ValueError, TypeError) as e:
            print(f"[WARNING] Invalid lead value '{lead_value}' for opportunity {opp_id}: {e}")
    
    print(f"[DEBUG] Updating opportunity {opp_id} with payload: {payload}")
    
    async with httpx.AsyncClient() as client:
        for attempt in range(4):
            resp = await client.put(url, headers=headers, json=payload)
            print(f"[DEBUG] Attempt {attempt + 1} for {opp_id}: Status {resp.status_code}")
            
            if resp.status_code == 200:
                print(f"[SUCCESS] Updated opportunity {opp_id}")
                return True, None
            elif resp.status_code in [429, 500]:
                print(f"[RETRY] Rate limit/server error for {opp_id}, retrying...")
                time.sleep(2 ** attempt)
                continue
            else:
                error_msg = f"HTTP {resp.status_code}: {resp.text}"
                print(f"[ERROR] Failed to update {opp_id}: {error_msg}")
                return False, error_msg
    
    error_msg = f"Failed after retries"
    print(f"[ERROR] {error_msg} for opportunity {opp_id}")
    return False, error_msg

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
    
    print(f"[DEBUG] Processing {len(df)} rows from uploaded file")
    print(f"[DEBUG] Available columns: {list(df.columns)}")
    
    results = []
    total = len(df)
    updated = 0
    skipped = 0
    value_updates = 0
    
    # Cache pipelines per account
    pipeline_cache = {}
    
    for idx, row in df.iterrows():
        print(f"\n[DEBUG] Processing row {idx + 2}:")
        
        opp_id = row.get('Opportunity ID') or row.get('opportunityId')
        pipeline_name = row.get('pipeline')
        stage_name = row.get('stage')
        account_id = row.get('Account Id') or row.get('accountId')
        contact_id = row.get('Contact ID') or row.get('contactId')
        newnote = row.get('newnote')
        lead_value = row.get('Lead Value') or row.get('leadValue') or row.get('Lead value')
        
        print(f"[DEBUG] Row data - OppID: {opp_id}, Pipeline: {pipeline_name}, Stage: {stage_name}, Account: {account_id}, LeadValue: {lead_value}")
        
        if not opp_id or not pipeline_name or not stage_name or not account_id:
            reason = f'Missing required fields - OppID: {bool(opp_id)}, Pipeline: {bool(pipeline_name)}, Stage: {bool(stage_name)}, Account: {bool(account_id)}'
            print(f"[SKIP] {reason}")
            results.append({'row': idx+2, 'reason': reason})
            skipped += 1
            continue
        access_token, location_id = get_account_auth(account_id)
        if not access_token or not location_id:
            reason = f'No access_token/location_id for Account Id {account_id}'
            print(f"[SKIP] {reason}")
            results.append({'row': idx+2, 'reason': reason})
            skipped += 1
            continue
        
        print(f"[DEBUG] Using location_id: {location_id} for account: {account_id}")
        
        # Fetch pipelines for account (cache)
        if location_id not in pipeline_cache:
            print(f"[DEBUG] Fetching pipelines for location {location_id}")
            pipeline_cache[location_id] = await fetch_pipelines(location_id, access_token)
            print(f"[DEBUG] Found {len(pipeline_cache[location_id])} pipelines")
        
        pipelines = pipeline_cache[location_id]
        
        # Match pipeline
        pipeline_id = None
        stage_id = None
        pipeline_choices = [p['name'] for p in pipelines]
        print(f"[DEBUG] Available pipelines: {pipeline_choices}")
        
        matched_pipeline = fuzzy_match(pipeline_name, pipeline_choices)
        if not matched_pipeline:
            reason = f'Pipeline "{pipeline_name}" not found in {pipeline_choices}'
            print(f"[SKIP] {reason}")
            results.append({'row': idx+2, 'reason': reason})
            skipped += 1
            continue
        
        print(f"[DEBUG] Matched pipeline: '{pipeline_name}' -> '{matched_pipeline}'")
        
        pipeline_obj = next((p for p in pipelines if p['name'] == matched_pipeline), None)
        pipeline_id = pipeline_obj['id'] if pipeline_obj else None
        
        # Match stage
        stage_choices = [s['name'] for s in pipeline_obj.get('stages', [])]
        print(f"[DEBUG] Available stages for pipeline '{matched_pipeline}': {stage_choices}")
        
        matched_stage = fuzzy_match(stage_name, stage_choices)
        if not matched_stage:
            reason = f'Stage "{stage_name}" not found in {stage_choices}'
            print(f"[SKIP] {reason}")
            results.append({'row': idx+2, 'reason': reason})
            skipped += 1
            continue
        
        print(f"[DEBUG] Matched stage: '{stage_name}' -> '{matched_stage}'")
        
        stage_obj = next((s for s in pipeline_obj.get('stages', []) if s['name'] == matched_stage), None)
        stage_id = stage_obj['id'] if stage_obj else None
        
        # Update opportunity
        ok, err = await update_opportunity(opp_id, pipeline_id, stage_id, access_token, lead_value, dry_run)
        if ok:
            updated += 1
            if lead_value:
                value_updates += 1
                print(f"[SUCCESS] Updated opportunity {opp_id} with value {lead_value}")
        else:
            print(f"[ERROR] Failed to update opportunity {opp_id}: {err}")
            results.append({'row': idx+2, 'reason': err, 'opp_id': opp_id})
            skipped += 1
        # Bulk notes: if newnote exists, create note for contact
        if contact_id and newnote:
            print(f"[DEBUG] Adding note for contact {contact_id}")
            note_ok, note_err = await create_note(contact_id, newnote, access_token, dry_run)
            if not note_ok:
                print(f"[ERROR] Failed to add note for contact {contact_id}: {note_err}")
                results.append({'row': idx+2, 'contactId': contact_id, 'reason': f'Note error: {note_err}'})
    
    print(f"\n[SUMMARY] Total: {total}, Updated: {updated}, Skipped: {skipped}, Value Updates: {value_updates}")
    
    # Write log
    log_path = None
    if results:
        log_df = pd.DataFrame(results)
        log_path = os.path.join(tempfile.gettempdir(), 'opportunity_update_log.csv')
        log_df.to_csv(log_path, index=False)
        print(f"[DEBUG] Log file written to: {log_path}")
    
    summary = {
        'totalRows': total,
        'updated': updated,
        'skipped': skipped,
        'valueUpdates': value_updates,
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
