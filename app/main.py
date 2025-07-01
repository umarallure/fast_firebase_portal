from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.api.automation import router as automation_router
from fastapi.responses import JSONResponse, FileResponse
from app.config import settings
from app.auth.firebase import get_current_user
import httpx
import csv
import csv as pycsv
import io
import uuid
from datetime import datetime
from typing import Optional
from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
import logging
import asyncio
import tempfile
import os
from starlette.background import BackgroundTask
import uuid
import csv as pycsv
import json

logger = logging.getLogger(__name__)

def normalize_phone_number(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    # Remove non-digit characters
    normalized_phone = "".join(filter(str.isdigit, phone))
    # Optionally add a default country code if it seems to be missing and is a local number
    if len(normalized_phone) == 10: # Assuming 10-digit US numbers
        normalized_phone = "1" + normalized_phone
    if not normalized_phone.startswith('+'):
        normalized_phone = '+' + normalized_phone
    return normalized_phone

app = FastAPI()

# Configure templates and static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers (API routes only are protected)
app.include_router(
    automation_router,
    prefix="/api/v1"
)

FAILED_CSV_DIR = os.path.join(os.path.dirname(__file__), "static", "failed_csvs")
os.makedirs(FAILED_CSV_DIR, exist_ok=True)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/")
async def root(request: Request):
    # Render the new index page with automation cards, login check will be done in JS
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/dashboard")
async def dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/api/subaccounts")
def get_subaccounts():
    return JSONResponse(content=settings.subaccounts_list)

@app.get("/api/subaccounts/{sub_id}/pipelines")
async def get_pipelines_for_subaccount(sub_id: str):
    subaccounts = settings.subaccounts_list
    sub = next((s for s in subaccounts if str(s.get("id")) == str(sub_id)), None)
    if not sub or not sub.get("api_key"):
        return []
    api_key = sub["api_key"]
    url = "https://rest.gohighlevel.com/v1/pipelines"
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(timeout=settings.ghl_api_timeout) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            return data.get("pipelines", [])
        except Exception as e:
            # Log the error and return an empty list instead of raising
            logging.error(f"Failed to fetch pipelines for subaccount {sub_id}: {e}")
            return []

@app.get("/bulk-update-notes")
async def bulk_update_notes_page(request: Request):
    return templates.TemplateResponse("bulk_update_notes.html", {"request": request})

@app.get("/bulk-update-opportunity")
async def bulk_update_opportunity_page(request: Request):
    return templates.TemplateResponse("bulk_update_opportunity.html", {"request": request})

@app.post("/api/bulk-update-notes")
async def bulk_update_notes_api(csvFile: UploadFile = File(...)):
    try:
        content = await csvFile.read()
        decoded = content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))
        rows = list(reader)
        if not rows:
            return {"success": False, "message": "CSV is empty or invalid."}
        # Prepare subaccount API keys
        subaccounts = settings.subaccounts_list
        account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}
        errors = []
        success_count = 0
        async with httpx.AsyncClient(timeout=30) as client:
            for row in rows:
                contact_id = row.get('Contact ID')
                notes = row.get('Notes')
                account_id = str(row.get('Account Id'))
                api_key = account_api_keys.get(account_id)
                # Skip if contact_id or api_key is missing; skip if notes is empty or None
                if not contact_id or not api_key:
                    errors.append(f"Missing data for contact {contact_id}")
                    continue
                if not notes or not notes.strip():
                    # Skip contacts with empty notes, but do not log as error
                    continue
                url = f"https://rest.gohighlevel.com/v1/contacts/{contact_id}/notes/"
                payload = {"body": notes}
                headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
                try:
                    resp = await client.post(url, json=payload, headers=headers)
                    if resp.status_code == 200:
                        success_count += 1
                    else:
                        errors.append(f"Failed for {contact_id}: {resp.text}")
                except Exception as e:
                    errors.append(f"Exception for {contact_id}: {str(e)}")
        msg = f"Successfully updated notes for {success_count} contacts."
        if errors:
            msg += f" Errors: {'; '.join(errors[:5])}{'...' if len(errors)>5 else ''}"
        return {"success": True, "message": msg}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.get("/static/failed_csvs/{filename}")
async def download_failed_csv(filename: str):
    file_path = os.path.join(FAILED_CSV_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename, media_type="text/csv")
    return JSONResponse({"error": "File not found"}, status_code=404)

@app.post("/api/bulk-update-opportunity")
async def bulk_update_opportunity_api(csvFile: UploadFile = File(...)):
    expected_columns = [
        "Found in Carrier?", "Updated in GHL?", "Client Phone Number", "Lead Vender", "Date",
        "INSURED NAME", "Buffer Agent", "Agent", "Status", "Carrier", "Product Type",
        "Draft Date", "From Callback?", "Notes", "Policy Number", "Carrier Audit",
        "ProductTypeCarrier", "Level Or GI"
    ]
    try:
        content = await csvFile.read()
        decoded = content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))
        
        # Check if the sheet has the proper structure
        if not all(col in reader.fieldnames for col in expected_columns):
            missing_columns = [col for col in expected_columns if col not in reader.fieldnames]
            return {"success": False, "message": f"CSV is missing required columns: {', '.join(missing_columns)}"}

        rows = list(reader)
        if not rows:
            return {"success": False, "message": "CSV is empty or invalid."}

        cleaned_rows = []
        for row in rows:
            # Clean the sheet: remove rows with no info for 'Lead Vender'
            if row.get('Lead Vender', '').strip():
                cleaned_rows.append(row)
        
        if not cleaned_rows:
            return {"success": False, "message": "No valid rows found after cleaning (missing 'Lead Vender')."}

        # Prepare subaccount API keys based on Lead Vender
        subaccounts = settings.subaccounts_list
        vender_api_keys = {s['name']: s['api_key'] for s in subaccounts if s.get('name') and s.get('api_key')}

        processed_rows_with_api_key = []
        api_key_fetch_status = {}

        for row in cleaned_rows:
            lead_vender = row.get('Lead Vender', '').strip()
            if lead_vender:
                api_key = vender_api_keys.get(lead_vender)
                if api_key:
                    row['api_key'] = api_key
                    processed_rows_with_api_key.append(row)
                    api_key_fetch_status[lead_vender] = "Fetched"
                else:
                    api_key_fetch_status[lead_vender] = "Not Found"
            else:
                api_key_fetch_status["Unknown Vender"] = "Skipped (No Lead Vender)"

        opportunity_update_results = {}
        
        # Group cleaned rows by API key
        rows_by_api_key = {}
        for row in processed_rows_with_api_key:
            api_key = row['api_key']
            if api_key not in rows_by_api_key:
                rows_by_api_key[api_key] = []
            rows_by_api_key[api_key].append(row)

        failed_rows = []
        for api_key, rows_to_update in rows_by_api_key.items():
            updater = GHLOpportunityUpdater(api_key)
            account_id = next((s['id'] for s in subaccounts if s.get('api_key') == api_key), None)
            if not account_id:
                opportunity_update_results[api_key] = {"status": "Failed", "message": "Account ID not found for API key."}
                continue

            all_ghl_opportunities = await updater.get_all_opportunities_for_account(account_id)
            ghl_opportunity_lookup = {}
            for opp in all_ghl_opportunities:
                phone = normalize_phone_number(opp.get("contact_phone"))
                name = opp.get("contact_name")
                created_at_str = opp.get("Created on") # Assuming this is the key from ghl_opportunity_updater
                created_date = None
                if created_at_str:
                    try:
                        # Assuming 'Created on' from ghl_opportunity_updater is already a datetime object or can be parsed
                        # If it's a datetime object, format it. If it's a string, parse then format.
                        if isinstance(created_at_str, datetime):
                            created_date = created_at_str.strftime('%Y-%m-%d')
                        else: # Assume string format like 'YYYY-MM-DDTHH:MM:SS.fZ'
                            created_date = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"Could not parse Created on date: {created_at_str}")
                
                lookup_key = None
                if phone and name and created_date:
                    lookup_key = (phone, name, created_date)
                elif name and created_date: # Fallback to just name and date if phone is missing
                    lookup_key = (None, name, created_date)
                elif phone and name: # Original fallback if date is missing/invalid
                    lookup_key = (phone, name, None)
                elif name: # Original fallback if phone and date are missing/invalid
                    lookup_key = (None, name, None)

                if lookup_key:
                    ghl_opportunity_lookup[lookup_key] = opp
                    logger.info(f"GHL Lookup: Added key {lookup_key} for opp ID {opp.get('opportunity_id')}")
                else:
                    logger.info(f"GHL Lookup: Skipped opp ID {opp.get('opportunity_id')} due to missing key data (phone: {phone}, name: {name}, date: {created_date})")

            account_update_success_count = 0
            account_update_errors = []
            notes_update_results = []
            stage_update_results = []  # <-- Track stage update results

            # Fetch all pipelines and stages for this account (API key)
            async with httpx.AsyncClient(timeout=30) as client:
                pipelines_resp = await client.get(
                    "https://rest.gohighlevel.com/v1/pipelines/",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    }
                )
                pipelines_data = pipelines_resp.json() if pipelines_resp.status_code == 200 else {}
                pipelines_list = pipelines_data.get("pipelines", [])

            # Build a mapping: {pipeline_id: {stage_name_lower: stage_id}}
            pipeline_stage_map = {}
            for pipeline in pipelines_list:
                pid = pipeline.get("id")
                stages = pipeline.get("stages", [])
                stage_map = {stage.get("name", "").strip().lower(): stage.get("id") for stage in stages}
                pipeline_stage_map[pid] = stage_map

            for row in rows_to_update:
                error_msg = None  # <-- Initialize at the start of each row
                client_phone = normalize_phone_number(row.get('Client Phone Number', '').strip())
                insured_name = row.get('INSURED NAME', '').strip()
                status_from_csv = row.get('Status', '').strip()
                csv_date_str = row.get('Date', '').strip() # Assuming 'Date' is the column name for Created Date in CSV
                
                csv_created_date = None
                if csv_date_str:
                    try:
                        # Assuming CSV date format is 'M/D/YY' or 'MM/DD/YYYY'
                        # Try multiple formats
                        for fmt in ('%m/%d/%y', '%m/%d/%Y'):
                            try:
                                csv_created_date = datetime.strptime(csv_date_str, fmt).strftime('%Y-%m-%d')
                                break
                            except ValueError:
                                continue
                        if not csv_created_date:
                            logger.warning(f"Could not parse CSV date: {csv_date_str}")
                    except Exception as e:
                        logger.warning(f"Error parsing CSV date '{csv_date_str}': {e}")

                logger.info(f"CSV Row: Phone='{client_phone}', Name='{insured_name}', Date='{csv_created_date}'")

                matched_opportunity = None
                lookup_key_with_date = None
                lookup_key_no_date = None

                # --- Flexible matching logic ---
                attempted_keys = []

                def is_transfer_portal(opp):
                    return opp.get("pipeline_name", "").strip().lower() == "transfer portal"

                # 1. Try full phone, name, date
                if client_phone and insured_name and csv_created_date:
                    lookup_key_with_date = (client_phone, insured_name, csv_created_date)
                    attempted_keys.append(lookup_key_with_date)
                    opp = ghl_opportunity_lookup.get(lookup_key_with_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp

                # 2. Try last 7 or 10 digits of phone, name, date
                if not matched_opportunity and client_phone and insured_name and csv_created_date:
                    phone_digits = ''.join(filter(str.isdigit, client_phone))
                    for digits in [7, 10]:
                        short_phone = '+' + phone_digits[-digits:] if len(phone_digits) >= digits else None
                        if short_phone:
                            key = (short_phone, insured_name, csv_created_date)
                            attempted_keys.append(key)
                            opp = ghl_opportunity_lookup.get(key)
                            if opp and is_transfer_portal(opp):
                                matched_opportunity = opp
                                break

                # 3. Try just name and date
                if not matched_opportunity and insured_name and csv_created_date:
                    lookup_key_with_date = (None, insured_name, csv_created_date)
                    attempted_keys.append(lookup_key_with_date)
                    opp = ghl_opportunity_lookup.get(lookup_key_with_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp

                # 4. Try full phone and name (no date)
                if not matched_opportunity and client_phone and insured_name:
                    lookup_key_no_date = (client_phone, insured_name, None)
                    attempted_keys.append(lookup_key_no_date)
                    opp = ghl_opportunity_lookup.get(lookup_key_no_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp

                # 5. Try last 7 or 10 digits of phone and name (no date)
                if not matched_opportunity and client_phone and insured_name:
                    phone_digits = ''.join(filter(str.isdigit, client_phone))
                    for digits in [7, 10]:
                        short_phone = '+' + phone_digits[-digits:] if len(phone_digits) >= digits else None
                        if short_phone:
                            key = (short_phone, insured_name, None)
                            attempted_keys.append(key)
                            opp = ghl_opportunity_lookup.get(key)
                            if opp and is_transfer_portal(opp):
                                matched_opportunity = opp
                                break

                # 6. Try just name (no date, no phone)
                if not matched_opportunity and insured_name:
                    lookup_key_no_date = (None, insured_name, None)
                    attempted_keys.append(lookup_key_no_date)
                    opp = ghl_opportunity_lookup.get(lookup_key_no_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp

                # 7. Try matching by opportunity_name (fuzzy, contains insured_name and phone digits)
                if not matched_opportunity:
                    phone_digits = ''.join(filter(str.isdigit, client_phone)) if client_phone else ""
                    for opp in all_ghl_opportunities:
                        opp_name = opp.get("opportunity_name", "")
                        if (
                            insured_name.lower() in opp_name.lower()
                            and (phone_digits[-7:] in opp_name or phone_digits[-10:] in opp_name)
                            and is_transfer_portal(opp)
                        ):
                            matched_opportunity = opp
                            attempted_keys.append(("fuzzy", insured_name, phone_digits))
                            break

                if matched_opportunity:
                    logger.info(f"Match Found for CSV row: {client_phone}, {insured_name}, {csv_created_date}. Matched GHL Opp ID: {matched_opportunity.get('opportunity_id')}")
                    pipeline_id = matched_opportunity['pipeline_id']
                    opportunity_id = matched_opportunity['opportunity_id']
                    
                    # --- Agent mapping ---
                    agent_id_map = {
                        "Benjamin": "uO52LEhmrtCqg9eYdiIZ",
                        "Lydia": "XgpXx6hOyuj3KjGzjxUO",
                        "Claudia": "Y4DkBuz0jORYFvkMQzlF"
                    }
                    agent_name = row.get("Agent", "").strip()
                    assigned_to_id = agent_id_map.get(agent_name, "")

                    payload = {
                        "title": f"{row.get('INSURED NAME', '')} - {row.get('Client Phone Number', '')}"
                    }
                    # Always include status to satisfy API requirements
                    payload["status"] = "open"
                    if assigned_to_id:
                        payload["assignedTo"] = assigned_to_id

                    update_success = await updater.update_opportunity(pipeline_id, opportunity_id, payload)
                    if update_success:
                        account_update_success_count += 1
                    else:
                        error_msg = f"Failed to update opportunity for {insured_name} (ID: {opportunity_id})."
                        account_update_errors.append(error_msg)

                    # --- Update Notes ---
                    notes_from_csv = row.get('Notes', '').strip()
                    if notes_from_csv and matched_opportunity.get('contact_id'):
                        contact_id = matched_opportunity['contact_id']
                        logger.info(f"Attempting to update notes for contact {contact_id} with notes: {notes_from_csv}")
                        notes_result = await updater.update_contact_notes(contact_id, notes_from_csv)
                        
                        notes_update_results.append({
                            "insured_name": insured_name,
                            "contact_id": contact_id,
                            "notes_status": notes_result.get("status", "unknown"),
                            "notes_error": notes_result.get("error", ""),
                            "notes": notes_from_csv
                        })
                        
                        if notes_result.get("status") == "success":
                            logger.info(f"Successfully updated notes for {insured_name} (Contact ID: {contact_id})")
                        else:
                            logger.error(f"Failed to update notes for {insured_name} (Contact ID: {contact_id}): {notes_result.get('error', 'Unknown error')}")
                    elif notes_from_csv and not matched_opportunity.get('contact_id'):
                        logger.warning(f"Notes found for {insured_name} but no contact_id available in matched opportunity")
                        notes_update_results.append({
                            "insured_name": insured_name,
                            "contact_id": "N/A",
                            "notes_status": "skipped",
                            "notes_error": "No contact_id found in matched opportunity",
                            "notes": notes_from_csv
                        })
                    elif not notes_from_csv:
                        logger.info(f"No notes to update for {insured_name}")
                    
                    # --- Stage Update Logic (if needed) ---
                    stage_name_csv = row.get('Status', '').strip()
                    if stage_name_csv and pipeline_id and opportunity_id:
                        logger.info(f"Desired stage from CSV: '{stage_name_csv}'")
                        # Resolve stage ID from stage name
                        new_stage_id = await updater.get_stage_id_from_name(pipeline_id, stage_name_csv)
                        if new_stage_id:
                            # Use the dedicated status endpoint to set stage
                            logger.info(f"Updating status and stage for opportunity {opportunity_id} to '{stage_name_csv}' (ID: {new_stage_id})")
                            # Assuming CSV 'Status' value is also the desired status; use 'open' if missing
                            desired_status = 'open'
                            stage_success = await updater.update_opportunity_status(pipeline_id, opportunity_id, desired_status, new_stage_id)
                            stage_update_results.append({
                                "insured_name": insured_name,
                                "opportunity_id": opportunity_id,
                                "stage_name": stage_name_csv,
                                "stage_id": new_stage_id,
                                "stage_status": "success" if stage_success else "error"
                            })
                            if stage_success:
                                logger.info(f"Successfully updated stage to '{stage_name_csv}' for {insured_name}")
                            else:
                                logger.error(f"Failed to update stage for {insured_name} (ID: {opportunity_id})")
                        else:
                            logger.warning(f"Stage name '{stage_name_csv}' not found for pipeline {pipeline_id}")
                            stage_update_results.append({
                                "insured_name": insured_name,
                                "opportunity_id": opportunity_id,
                                "stage_name": stage_name_csv,
                                "stage_id": None,
                                "stage_status": "skipped",
                                "stage_error": f"Stage '{stage_name_csv}' not found"
                            })
                    else:
                        logger.info(f"No stage update for {insured_name}")
                else:
                    error_msg = f"No matching opportunity found for {insured_name} (Phone: {client_phone}, Date: {csv_created_date})."
                    account_update_errors.append(error_msg)
                if error_msg:
                    failed_row = row.copy()
                    failed_row["Error"] = error_msg
                    failed_rows.append(failed_row)

            opportunity_update_results[api_key] = {
                "status": "Completed",
                "success_count": account_update_success_count,
                "errors": account_update_errors,
                "notes_update_results": notes_update_results,
                "stage_update_results": stage_update_results  # <-- Add stage results here
            }

        failed_csv_url = None
        if failed_rows:
            failed_filename = f"failed_{uuid.uuid4().hex}.csv"
            failed_path = os.path.join(FAILED_CSV_DIR, failed_filename)
            with open(failed_path, "w", newline='', encoding="utf-8") as f:
                writer = pycsv.DictWriter(f, fieldnames=list(failed_rows[0].keys()))
                writer.writeheader()
                writer.writerows(failed_rows)
            failed_csv_url = f"/static/failed_csvs/{failed_filename}"

        return {
            "success": True,
            "message": f"CSV processed. Found {len(rows)} rows, {len(cleaned_rows)} valid rows after cleaning. ",
            "total_rows": len(rows),
            "cleaned_rows": len(cleaned_rows),
            "api_key_status": api_key_fetch_status,
            "opportunity_update_results": opportunity_update_results,
            "failed_csv_url": failed_csv_url,
            "failed_rows": failed_rows,  # <-- Add this line
        }
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/audit-opportunity-matches")
async def audit_opportunity_matches_api(csvFile: UploadFile = File(...)):
    expected_columns = [
        "Found in Carrier?", "Updated in GHL?", "Client Phone Number", "Lead Vender", "Date",
        "INSURED NAME", "Buffer Agent", "Agent", "Status", "Carrier", "Product Type",
        "Draft Date", "From Callback?", "Notes", "Policy Number", "Carrier Audit",
        "ProductTypeCarrier", "Level Or GI"
    ]
    try:
        content = await csvFile.read()
        decoded = content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))
        if not all(col in reader.fieldnames for col in expected_columns):
            missing_columns = [col for col in expected_columns if col not in reader.fieldnames]
            return {"success": False, "message": f"CSV is missing required columns: {', '.join(missing_columns)}"}
        rows = list(reader)
        if not rows:
            return {"success": False, "message": "CSV is empty or invalid."}
        cleaned_rows = [row for row in rows if row.get('Lead Vender', '').strip()]
        if not cleaned_rows:
            return {"success": False, "message": "No valid rows found after cleaning (missing 'Lead Vender')."}
        subaccounts = settings.subaccounts_list
        vender_api_keys = {s['name']: s['api_key'] for s in subaccounts if s.get('name') and s.get('api_key')}
        processed_rows_with_api_key = []
        api_key_fetch_status = {}
        for row in cleaned_rows:
            lead_vender = row.get('Lead Vender', '').strip()
            if lead_vender:
                api_key = vender_api_keys.get(lead_vender)
                if api_key:
                    row['api_key'] = api_key
                    processed_rows_with_api_key.append(row)
                    api_key_fetch_status[lead_vender] = "Fetched"
                else:
                    api_key_fetch_status[lead_vender] = "Not Found"
            else:
                api_key_fetch_status["Unknown Vender"] = "Skipped (No Lead Vender)"
        # Group cleaned rows by API key
        rows_by_api_key = {}
        for row in processed_rows_with_api_key:
            api_key = row['api_key']
            if api_key not in rows_by_api_key:
                rows_by_api_key[api_key] = []
            rows_by_api_key[api_key].append(row)
        audit_results = []
        matched_opportunity_ids = {}
        for api_key, rows_to_audit in rows_by_api_key.items():
            updater = GHLOpportunityUpdater(api_key)
            account_id = next((s['id'] for s in subaccounts if s.get('api_key') == api_key), None)
            if not account_id:
                for row in rows_to_audit:
                    audit_results.append({"csv_row": row, "matched_opportunity": None, "status": "not matched", "reason": "Account ID not found for API key.", "fallback": "No Lead Vender/API Key"})
                continue
            all_ghl_opportunities = await updater.get_all_opportunities_for_account(account_id)
            ghl_opportunity_lookup = {}
            for opp in all_ghl_opportunities:
                phone = normalize_phone_number(opp.get("contact_phone"))
                name = opp.get("contact_name")
                created_at_str = opp.get("Created on")
                created_date = None
                if created_at_str:
                    try:
                        if isinstance(created_at_str, datetime):
                            created_date = created_at_str.strftime('%Y-%m-%d')
                        else:
                            created_date = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    except Exception:
                        pass
                lookup_key = None
                if phone and name and created_date:
                    lookup_key = (phone, name, created_date)
                elif name and created_date:
                    lookup_key = (None, name, created_date)
                elif phone and name:
                    lookup_key = (phone, name, None)
                elif name:
                    lookup_key = (None, name, None)
                if lookup_key:
                    ghl_opportunity_lookup[lookup_key] = opp
            for row in rows_to_audit:
                client_phone = normalize_phone_number(row.get('Client Phone Number', '').strip())
                insured_name = row.get('INSURED NAME', '').strip()
                csv_date_str = row.get('Date', '').strip()
                csv_created_date = None
                if csv_date_str:
                    for fmt in ('%m/%d/%y', '%m/%d/%Y'):
                        try:
                            csv_created_date = datetime.strptime(csv_date_str, fmt).strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                matched_opportunity = None
                fallback = None
                attempted_keys = []
                def is_transfer_portal(opp):
                    return opp.get("pipeline_name", "").strip().lower() == "transfer portal"
                # 1. Try full phone, name, date
                if client_phone and insured_name and csv_created_date:
                    lookup_key_with_date = (client_phone, insured_name, csv_created_date)
                    attempted_keys.append(("full phone, name, date", lookup_key_with_date))
                    opp = ghl_opportunity_lookup.get(lookup_key_with_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp
                        fallback = "full phone, name, date"
                # 2. Try last 7 or 10 digits of phone, name, date
                if not matched_opportunity and client_phone and insured_name and csv_created_date:
                    phone_digits = ''.join(filter(str.isdigit, client_phone))
                    for digits in [7, 10]:
                        short_phone = '+' + phone_digits[-digits:] if len(phone_digits) >= digits else None
                        if short_phone:
                            key = (short_phone, insured_name, csv_created_date)
                            attempted_keys.append((f"last {digits} digits phone, name, date", key))
                            opp = ghl_opportunity_lookup.get(key)
                            if opp and is_transfer_portal(opp):
                                matched_opportunity = opp
                                fallback = f"last {digits} digits phone, name, date"
                                break
                # 3. Try just name and date
                if not matched_opportunity and insured_name and csv_created_date:
                    lookup_key_with_date = (None, insured_name, csv_created_date)
                    attempted_keys.append(("name, date", lookup_key_with_date))
                    opp = ghl_opportunity_lookup.get(lookup_key_with_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp
                        fallback = "name, date"
                # 4. Try full phone and name (no date)
                if not matched_opportunity and client_phone and insured_name:
                    lookup_key_no_date = (client_phone, insured_name, None)
                    attempted_keys.append(("full phone, name", lookup_key_no_date))
                    opp = ghl_opportunity_lookup.get(lookup_key_no_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp
                        fallback = "full phone, name"
                # 5. Try last 7 or 10 digits of phone and name (no date)
                if not matched_opportunity and client_phone and insured_name:
                    phone_digits = ''.join(filter(str.isdigit, client_phone))
                    for digits in [7, 10]:
                        short_phone = '+' + phone_digits[-digits:] if len(phone_digits) >= digits else None
                        if short_phone:
                            key = (short_phone, insured_name, None)
                            attempted_keys.append((f"last {digits} digits phone, name", key))
                            opp = ghl_opportunity_lookup.get(key)
                            if opp and is_transfer_portal(opp):
                                matched_opportunity = opp
                                fallback = f"last {digits} digits phone, name"
                                break
                # 6. Try just name (no date, no phone)
                if not matched_opportunity and insured_name:
                    lookup_key_no_date = (None, insured_name, None)
                    attempted_keys.append(("name only", lookup_key_no_date))
                    opp = ghl_opportunity_lookup.get(lookup_key_no_date)
                    if opp and is_transfer_portal(opp):
                        matched_opportunity = opp
                        fallback = "name only"
                # 7. Try matching by opportunity_name (fuzzy, contains insured_name and phone digits)
                if not matched_opportunity:
                    phone_digits = ''.join(filter(str.isdigit, client_phone)) if client_phone else ""
                    for opp in all_ghl_opportunities:
                        opp_name = opp.get("opportunity_name", "")
                        if (
                            insured_name.lower() in opp_name.lower()
                            and (phone_digits[-7:] in opp_name or phone_digits[-10:] in opp_name)
                            and is_transfer_portal(opp)
                        ):
                            matched_opportunity = opp
                            fallback = "fuzzy match by name and phone digits"
                            break
                status = "matched" if matched_opportunity else "not matched"
                reason = "" if matched_opportunity else f"No match after {fallback or 'all attempts'}"
                audit_results.append({
                    "csv_row": row,
                    "matched_opportunity": matched_opportunity,
                    "status": status,
                    "reason": reason,
                    "fallback": fallback,
                })
                # Track matched opportunity IDs for duplicate detection
                if matched_opportunity:
                    opp_id = matched_opportunity.get("opportunity_id")
                    if opp_id:
                        matched_opportunity_ids.setdefault(opp_id, []).append(row)
        # Mark duplicates
        duplicate_opp_ids = {opp_id for opp_id, rows in matched_opportunity_ids.items() if len(rows) > 1}
        for result in audit_results:
            if result["status"] == "matched" and result["matched_opportunity"]:
                opp_id = result["matched_opportunity"].get("opportunity_id")
                if opp_id in duplicate_opp_ids:
                    result["status"] = "duplicate"
                    result["reason"] = "Matched to the same opportunity as another row (duplicate)"
        # Write downloadable CSV
        audit_csv_url = None
        if audit_results:
            audit_filename = f"audit_{uuid.uuid4().hex}.csv"
            audit_path = os.path.join(FAILED_CSV_DIR, audit_filename)
            csv_headers = list(audit_results[0]["csv_row"].keys()) + ["status", "reason", "fallback"]
            with open(audit_path, "w", newline='', encoding="utf-8") as f:
                writer = pycsv.DictWriter(f, fieldnames=csv_headers)
                writer.writeheader()
                for result in audit_results:
                    row = result["csv_row"].copy()
                    row["status"] = result["status"]
                    row["reason"] = result["reason"]
                    row["fallback"] = result["fallback"]
                    writer.writerow(row)
            audit_csv_url = f"/static/failed_csvs/{audit_filename}"
        return {
            "success": True,
            "message": f"Audit completed. Found {len(rows)} rows, {len(cleaned_rows)} valid rows after cleaning.",
            "total_rows": len(rows),
            "cleaned_rows": len(cleaned_rows),
            "api_key_status": api_key_fetch_status,
            "audit_results": audit_results,
            "audit_csv_url": audit_csv_url
        }
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.get("/audit-opportunity-matches")
async def audit_opportunity_matches_page(request: Request):
    return templates.TemplateResponse("audit_opportunity_matches.html", {"request": request})

@app.get("/subaccounts")
async def subaccounts_page(request: Request):
    """Render the subaccounts management form"""
    return templates.TemplateResponse("subaccounts.html", {"request": request})

@app.post("/api/subaccounts")
async def add_subaccount(data: dict = Body(...)):
    """Add a new subaccount to the .env SUBACCOUNTS list"""
    # Path to the .env file (two levels up from this file)
    env_path = Path(__file__).parent.parent / ".env"
    try:
        text = env_path.read_text(encoding='utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read .env file: {e}")

    lines = text.splitlines()
    updated = False
    new_lines = []
    for line in lines:
        if line.startswith("SUBACCOUNTS="):
            # Extract existing JSON array
            key, val = line.split("=", 1)
            array_str = val.strip().strip("'").strip('"')
            try:
                subs = json.loads(array_str)
            except json.JSONDecodeError:
                subs = []
            # Append new subaccount
            subs.append(data)
            # Re-dump and format
            new_array_str = json.dumps(subs)
            new_line = f"SUBACCOUNTS='{new_array_str}'"
            new_lines.append(new_line)
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        # Add new SUBACCOUNTS entry if missing
        subs = [data]
        new_array_str = json.dumps(subs)
        new_lines.append(f"SUBACCOUNTS='{new_array_str}'")
    # Write back
    try:
        env_path.write_text("\n".join(new_lines), encoding='utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not write .env file: {e}")

    return JSONResponse(content={"success": True, "message": "Subaccount added to .env"})