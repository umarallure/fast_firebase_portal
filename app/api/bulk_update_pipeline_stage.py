from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import asyncio
import logging
from typing import Dict, List, Any
from app.services.ghl_opportunity_updater_v2 import GHLOpportunityUpdaterV2
from app.config import settings
import tempfile
import os

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)

@router.get("/api/account/{account_id}/config")
async def get_account_config(account_id: str):
    """Get the configuration for a specific account ID"""
    try:
        for subaccount in settings.subaccounts_list:
            if str(subaccount.get('id')) == str(account_id):
                return JSONResponse({
                    "success": True,
                    "account": {
                        "id": subaccount.get('id'),
                        "name": subaccount.get('name'),
                        "location_id": subaccount.get('location_id'),
                        "has_access_token": bool(subaccount.get('access_token')),
                        "has_api_key": bool(subaccount.get('api_key'))
                    }
                })
        
        return JSONResponse({
            "success": False,
            "message": f"Account {account_id} not found in configuration"
        })
        
    except Exception as e:
        logger.error(f"Error getting account config: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error getting account configuration: {str(e)}"
        })

@router.post("/api/test-v2-connection")
async def test_v2_connection(data: dict):
    """Test V2 API connection with provided access token and location ID"""
    try:
        access_token = data.get('access_token')
        location_id = data.get('location_id')
        
        if not access_token:
            return JSONResponse({
                "success": False,
                "message": "Access token is required"
            })
        
        if not location_id:
            return JSONResponse({
                "success": False,
                "message": "Location ID is required"
            })
        
        # Test the connection
        updater = GHLOpportunityUpdaterV2(access_token, location_id)
        
        try:
            # Test connection
            connection_result = await updater.test_connection()
            
            # Test pipeline fetch
            pipelines = await updater.get_pipelines()
            
            return JSONResponse({
                "success": True,
                "connection_test": connection_result,
                "pipelines_found": len(pipelines),
                "pipelines": [{"id": p.get("id"), "name": p.get("name")} for p in pipelines[:5]]  # First 5 only
            })
            
        finally:
            await updater.close()
            
    except Exception as e:
        logger.error(f"Error testing V2 connection: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error testing connection: {str(e)}"
        })

@router.get("/bulk-update-pipeline-stage", response_class=HTMLResponse)
async def bulk_update_pipeline_stage_page(request: Request):
    """Render the bulk update pipeline & stage page"""
    return templates.TemplateResponse("bulk_update_pipeline_stage.html", {"request": request})

@router.post("/api/bulk-update-pipeline-stage")
async def bulk_update_pipeline_stage_api(csvFile: UploadFile = File(...)):
    """API endpoint to process CSV and update opportunities using v2 API"""
    try:
        # Read and validate CSV
        content = await csvFile.read()
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
            tmp_file.write(content)
            tmp_file_path = tmp_file.name
        
        try:
            # Read CSV with pandas
            df = pd.read_csv(tmp_file_path)
            logger.info(f"Loaded CSV with {len(df)} rows")
            
            # Validate required columns
            required_columns = ['Opportunity ID', 'pipeline', 'stage', 'Lead Value', 'Notes', 'Account Id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return JSONResponse({
                    "success": False,
                    "message": f"Missing required columns: {', '.join(missing_columns)}"
                })
            
            # Clean and prepare data
            df = df.dropna(subset=['Opportunity ID', 'Account Id'])
            df['Lead Value'] = pd.to_numeric(df['Lead Value'], errors='coerce').fillna(0)
            df['Notes'] = df['Notes'].fillna('').astype(str)
            
            logger.info(f"After cleaning: {len(df)} rows")
            
            # Group by Account Id for processing
            results_by_account = {}
            failed_entries = []
            
            # Process each account
            for account_id in df['Account Id'].unique():
                account_df = df[df['Account Id'] == account_id]
                logger.info(f"Processing {len(account_df)} opportunities for account {account_id}")
                
                # Get access token and location ID for this account
                access_token = None
                location_id = None
                account_name = None
                for subaccount in settings.subaccounts_list:
                    if str(subaccount.get('id')) == str(account_id):
                        access_token = subaccount.get('access_token')  # V2 access token
                        location_id = subaccount.get('location_id')    # GHL location ID
                        account_name = subaccount.get('name', f'Account {account_id}')
                        break
                
                if not access_token:
                    logger.warning(f"No access token found for account {account_id}")
                    failed_entries.extend(account_df.to_dict('records'))
                    results_by_account[account_name or f'Account {account_id}'] = {
                        "status": "error",
                        "error": "No access token found for this account",
                        "success_count": 0,
                        "failed_count": len(account_df)
                    }
                    continue
                
                if not location_id:
                    logger.warning(f"No location ID found for account {account_id}")
                    failed_entries.extend(account_df.to_dict('records'))
                    results_by_account[account_name or f'Account {account_id}'] = {
                        "status": "error",
                        "error": "No location ID configured for this account",
                        "success_count": 0,
                        "failed_count": len(account_df)
                    }
                    continue
                
                # Initialize GHL updater with access token and location ID
                updater = GHLOpportunityUpdaterV2(access_token, location_id)
                
                try:
                    # Process each row for this account
                    account_results = await process_account_opportunities_v2(
                        updater, account_df, account_id, account_name
                    )
                    
                    results_by_account[account_name] = account_results
                    
                except Exception as e:
                    logger.error(f"Error processing account {account_id}: {str(e)}")
                    failed_entries.extend(account_df.to_dict('records'))
                    results_by_account[account_name] = {
                        "status": "error",
                        "error": str(e),
                        "success_count": 0,
                        "failed_count": len(account_df)
                    }
                finally:
                    await updater.close()
            
            # Generate failed CSV if there are failures
            failed_csv_url = None
            if failed_entries:
                failed_df = pd.DataFrame(failed_entries)
                os.makedirs("app/static/temp", exist_ok=True)
                failed_csv_path = f"app/static/temp/failed_pipeline_updates_v2_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
                failed_df.to_csv(failed_csv_path, index=False)
                failed_csv_url = f"/static/temp/failed_pipeline_updates_v2_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            return JSONResponse({
                "success": True,
                "message": "Bulk update completed",
                "total_rows": len(df),
                "results_by_account": results_by_account,
                "failed_csv_url": failed_csv_url
            })
            
        finally:
            # Clean up temp file
            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
                
    except Exception as e:
        logger.error(f"Error in bulk update pipeline stage v2: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error processing file: {str(e)}"
        })

async def process_account_opportunities_v2(
    updater: GHLOpportunityUpdaterV2, 
    account_df: pd.DataFrame, 
    account_id: str,
    account_name: str
) -> Dict[str, Any]:
    """Process opportunities for a specific account using v2 API"""
    
    success_count = 0
    failed_count = 0
    update_results = []
    notes_results = []
    errors = []
    
    for _, row in account_df.iterrows():
        try:
            opportunity_id = str(row['Opportunity ID']).strip()
            opportunity_name = str(row['Opportunity Name']).strip() if pd.notna(row['Opportunity Name']) else f"Opportunity {opportunity_id}"
            pipeline_name = str(row['pipeline']).strip()
            stage_name = str(row['stage']).strip()
            lead_value = float(row['Lead Value']) if pd.notna(row['Lead Value']) else 0
            notes = str(row['Notes']).strip() if pd.notna(row['Notes']) else ""
            contact_id = str(row['Contact ID']).strip() if pd.notna(row['Contact ID']) else ""
            
            logger.info(f"Processing opportunity: {opportunity_id} - {opportunity_name}")
            logger.info(f"Moving to pipeline: {pipeline_name}, stage: {stage_name}")
            
            # Get pipeline ID from name
            pipeline_id = await updater.get_pipeline_id_by_name(pipeline_name)
            if not pipeline_id:
                errors.append(f"Pipeline '{pipeline_name}' not found for opportunity {opportunity_id}")
                failed_count += 1
                continue
            
            # Get stage ID from name
            stage_id = await updater.get_stage_id_by_name(pipeline_id, stage_name)
            if not stage_id:
                errors.append(f"Stage '{stage_name}' not found in pipeline '{pipeline_name}' for opportunity {opportunity_id}")
                failed_count += 1
                continue
            
            # Prepare update payload with required name field
            update_payload = {
                "name": opportunity_name,
                "pipelineId": pipeline_id,
                "pipelineStageId": stage_id,
                "status": "open",
                "monetaryValue": lead_value
            }
            
            logger.info(f"Update payload: {update_payload}")
            
            # Update opportunity
            result = await updater.update_opportunity_v2(opportunity_id, update_payload)
            
            if result["status"] == "success":
                success_count += 1
                update_results.append({
                    "opportunity_id": opportunity_id,
                    "pipeline": pipeline_name,
                    "stage": stage_name,
                    "lead_value": lead_value,
                    "status": "success"
                })
                
                # Add notes if provided and Contact ID is available
                if notes and notes.upper() not in ['NAN', 'N/A', 'NULL', ''] and contact_id:
                    logger.info(f"Adding notes for opportunity {opportunity_id}, contact {contact_id}: '{notes}'")
                    notes_result = await updater.update_contact_notes(contact_id, notes)
                    notes_results.append({
                        "opportunity_id": opportunity_id,
                        "contact_id": contact_id,
                        "notes": notes,
                        "status": notes_result["status"],
                        "message": notes_result.get("error", "Notes added successfully")
                    })
                elif notes and not contact_id:
                    logger.warning(f"Notes provided for opportunity {opportunity_id} but no Contact ID available")
                    notes_results.append({
                        "opportunity_id": opportunity_id,
                        "contact_id": "N/A",
                        "notes": notes,
                        "status": "skipped",
                        "message": "No Contact ID provided"
                    })
                    
            else:
                failed_count += 1
                errors.append(f"Failed to update opportunity {opportunity_id}: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error processing opportunity {row.get('Opportunity ID', 'Unknown')}: {str(e)}")
            errors.append(f"Error processing opportunity {row.get('Opportunity ID', 'Unknown')}: {str(e)}")
            failed_count += 1
    
    return {
        "status": "completed",
        "account_name": account_name,
        "success_count": success_count,
        "failed_count": failed_count,
        "update_results": update_results,
        "notes_results": notes_results,
        "errors": errors[:10]  # Limit errors to first 10
    }
