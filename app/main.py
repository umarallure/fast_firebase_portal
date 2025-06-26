from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.api.automation import router as automation_router
from fastapi.responses import JSONResponse
from app.config import settings
from app.auth.firebase import get_current_user
import httpx
import csv
import io
from datetime import datetime
from typing import Optional
from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
import logging

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

        for api_key, rows_to_update in rows_by_api_key.items():
            updater = GHLOpportunityUpdater(api_key)
            account_id = next((s['id'] for s in subaccounts if s.get('api_key') == api_key), None)
            if not account_id:
                opportunity_update_results[api_key] = {"status": "Failed", "message": "Account ID not found for API key."}
                continue

            all_ghl_opportunities = await updater.get_all_opportunities_for_account(account_id)
            
            # Create a lookup for existing GHL opportunities
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

            for row in rows_to_update:
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

                if client_phone and insured_name and csv_created_date:
                    lookup_key_with_date = (client_phone, insured_name, csv_created_date)
                    matched_opportunity = ghl_opportunity_lookup.get(lookup_key_with_date)
                
                if not matched_opportunity and insured_name and csv_created_date: # Fallback to just name and date if phone is missing
                    lookup_key_with_date = (None, insured_name, csv_created_date)
                    matched_opportunity = ghl_opportunity_lookup.get(lookup_key_with_date)

                if not matched_opportunity and client_phone and insured_name: # Original fallback if date is missing/invalid
                    lookup_key_no_date = (client_phone, insured_name, None)
                    matched_opportunity = ghl_opportunity_lookup.get(lookup_key_no_date)
                
                if not matched_opportunity and insured_name: # Original fallback if phone and date are missing/invalid
                    lookup_key_no_date = (None, insured_name, None)
                    matched_opportunity = ghl_opportunity_lookup.get(lookup_key_no_date)

                if matched_opportunity:
                    logger.info(f"Match Found for CSV row: {client_phone}, {insured_name}, {csv_created_date}. Matched GHL Opp ID: {matched_opportunity.get('opportunity_id')}")
                    pipeline_id = matched_opportunity['pipeline_id']
                    opportunity_id = matched_opportunity['opportunity_id']
                    
                    # Get stage ID from stage name
                    # stage_id = await updater.get_stage_id_from_name(pipeline_id, status_from_csv)
                    
                    # if not stage_id:
                    #     account_update_errors.append(f"Could not find stage ID for status '{status_from_csv}' in pipeline {pipeline_id} for {insured_name}.")
                    #     continue

                    payload = {
                        "title": f"{row.get('Lead Vender', '')} - {row.get('Client Phone Number', '')}", # Concat Lead Vender and Phone Number
                        "status": "open" # GHL requires status field for update
                    }
                    # The user only wants to update the name and status for now.
                    # stageId, monetaryValue, assignedTo, contactId, email, name, phone, tags, companyName
                    # will not be updated at this stage.
                    
                    # Add other optional fields if available in CSV and relevant
                    # if row.get('Lead Value'):
                    #     try:
                    #         payload['monetaryValue'] = float(row['Lead Value'])
                    #     except ValueError:
                    #         pass # Ignore if not a valid number
                    # if row.get('Agent'):
                    #     # This would require mapping agent name to GHL user ID, which is not in scope yet.
                    #     # For now, we'll skip or assume 'assignedTo' is not updated via CSV.
                    #     pass 
                    # if row.get('Contact ID'): # If CSV has Contact ID, use it
                    #     payload['contactId'] = row['Contact ID']
                    # elif matched_opportunity.get('contact_id'): # Otherwise, use fetched Contact ID
                    #     payload['contactId'] = matched_opportunity['contact_id']
                    
                    # # Example of adding tags if available in CSV
                    # if row.get('tags'):
                    #     payload['tags'] = [tag.strip() for tag in row['tags'].split(',') if tag.strip()]

                    update_success = await updater.update_opportunity(pipeline_id, opportunity_id, payload)
                    if update_success:
                        account_update_success_count += 1
                    else:
                        account_update_errors.append(f"Failed to update opportunity for {insured_name} (ID: {opportunity_id}).")
                else:
                    logger.info(f"No Match Found for CSV row: {client_phone}, {insured_name}, {csv_created_date}. Looked for keys: {lookup_key_with_date}, {lookup_key_no_date}")
                    account_update_errors.append(f"No matching opportunity found for {insured_name} (Phone: {client_phone}, Date: {csv_created_date}).")
            
            opportunity_update_results[api_key] = {
                "status": "Completed",
                "success_count": account_update_success_count,
                "errors": account_update_errors
            }

        return {
            "success": True,
            "message": f"CSV processed. Found {len(rows)} rows, {len(cleaned_rows)} valid rows after cleaning. "
                       f"API Key Status: {api_key_fetch_status}. Opportunity Update Results: {opportunity_update_results}",
            "total_rows": len(rows),
            "cleaned_rows": len(cleaned_rows),
            "api_key_status": api_key_fetch_status,
            "opportunity_update_results": opportunity_update_results,
            "debug_ghl_opportunities": all_ghl_opportunities, # For debugging
            "debug_cleaned_rows": cleaned_rows # For debugging
        }

    except Exception as e:
        return {"success": False, "message": str(e)}