from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from app.services.custom_fields_ssn_export_service import CustomFieldsSSNExportService
from app.config import settings
import io
import csv
from datetime import datetime

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)

@router.get("/custom-fields-ssn-export", response_class=HTMLResponse)
async def custom_fields_ssn_export_page(request: Request):
    """Render the custom fields SSN export page"""
    return templates.TemplateResponse("custom_fields_ssn_export.html", {"request": request})

@router.get("/api/custom-fields-ssn-export/accounts")
async def get_available_accounts():
    """Get list of all available accounts for SSN export"""
    try:
        accounts = []
        for subaccount in settings.subaccounts_list:
            # Include all subaccounts
            accounts.append({
                "id": subaccount.get('id'),
                "name": subaccount.get('name'),
                "location_id": subaccount.get('location_id'),
                "has_access_token": bool(subaccount.get('access_token')),
                "has_api_key": bool(subaccount.get('api_key'))
            })
        
        logger.info(f"Retrieved {len(accounts)} available accounts for SSN export")
        
        return JSONResponse({
            "success": True,
            "accounts": accounts,
            "total_accounts": len(accounts)
        })
        
    except Exception as e:
        logger.error(f"Error getting available accounts: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error retrieving accounts: {str(e)}"
        })

@router.post("/api/custom-fields-ssn-export/export")
async def export_custom_fields_ssn(
    selected_accounts: str = Form(...),
    include_contacts: bool = Form(False),
    include_opportunities: bool = Form(False),
    export_format: str = Form("csv")
):
    """Export opportunities data from selected accounts - now acts exactly like dashboard export"""
    try:
        # Parse selected accounts
        account_ids = [acc.strip() for acc in selected_accounts.split(',') if acc.strip()]
        
        if not account_ids:
            return JSONResponse({
                "success": False,
                "message": "No accounts selected for export"
            })
        
        # Initialize the service
        export_service = CustomFieldsSSNExportService()
        
        # Perform the export
        export_result = await export_service.export_ssn_data(
            account_ids=account_ids,
            include_contacts=include_contacts,
            include_opportunities=include_opportunities,
            export_format=export_format
        )
        
        if export_result["success"]:
            # Generate CSV/Excel content exactly like dashboard export
            export_data = export_result["data"]
            
            if export_format.lower() == "excel":
                # Create Excel response like dashboard
                df = pd.DataFrame(export_data)
                output = io.BytesIO()
                df.to_excel(output, index=False)
                output.seek(0)
                
                filename = f"opportunities_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                return StreamingResponse(
                    io.BytesIO(output.getvalue()),
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
            else:
                # Create CSV response
                output = io.StringIO()
                if export_data:
                    df = pd.DataFrame(export_data)
                    df.to_csv(output, index=False)
                    output.seek(0)
                    
                    filename = f"opportunities_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    
                    return StreamingResponse(
                        io.BytesIO(output.getvalue().encode('utf-8')),
                        media_type="application/octet-stream",
                        headers={"Content-Disposition": f"attachment; filename={filename}"}
                    )
                else:
                    return JSONResponse({
                        "success": False,
                        "message": "No opportunities found for export"
                    })
        else:
            return JSONResponse({
                "success": False,
                "message": export_result.get("message", "Export failed")
            })
            
    except Exception as e:
        logger.error(f"Error in custom fields SSN export: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error processing export: {str(e)}"
        })

@router.post("/api/custom-fields-ssn-export/preview")
async def preview_custom_fields_ssn(
    selected_accounts: str = Form(...),
    include_contacts: bool = Form(False),
    include_opportunities: bool = Form(False)
):
    """Preview opportunities data before export"""
    try:
        # Parse selected accounts
        account_ids = [acc.strip() for acc in selected_accounts.split(',') if acc.strip()]
        
        if not account_ids:
            return JSONResponse({
                "success": False,
                "message": "No accounts selected for preview"
            })
        
        # Initialize the service
        export_service = CustomFieldsSSNExportService()
        
        # Get preview data (limited to first 10 records)
        preview_result = await export_service.preview_ssn_data(
            account_ids=account_ids,
            include_contacts=include_contacts,
            include_opportunities=include_opportunities,
            limit=10
        )
        
        return JSONResponse(preview_result)
            
    except Exception as e:
        logger.error(f"Error in custom fields SSN preview: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error generating preview: {str(e)}"
        })

@router.get("/api/custom-fields-ssn-export/test-connection/{account_id}")
async def test_account_connection(account_id: str):
    """Test connection to a specific account"""
    try:
        # Find account configuration
        account_config = None
        for subaccount in settings.subaccounts_list:
            if str(subaccount.get('id')) == str(account_id):
                account_config = subaccount
                break
        
        if not account_config:
            return JSONResponse({
                "success": False,
                "message": f"Account {account_id} not found in configuration"
            })
        
        # Initialize the service
        export_service = CustomFieldsSSNExportService()
        
        # Test connection
        test_result = await export_service.test_account_connection(account_config)
        
        return JSONResponse(test_result)
            
    except Exception as e:
        logger.error(f"Error testing account connection: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error testing connection: {str(e)}"
        })

@router.get("/api/custom-fields-ssn-export/custom-fields/{account_id}")
async def get_custom_fields(account_id: str):
    """Get custom field definitions for a specific account"""
    try:
        # Find account configuration
        account_config = None
        for subaccount in settings.subaccounts_list:
            if str(subaccount.get('id')) == str(account_id):
                account_config = subaccount
                break
        
        if not account_config:
            return JSONResponse({
                "success": False,
                "message": f"Account {account_id} not found in configuration"
            })
        
        # Initialize the service
        export_service = CustomFieldsSSNExportService()
        
        # Get custom field definitions
        field_result = await export_service.get_custom_field_definitions(account_config)
        
        return JSONResponse(field_result)
            
    except Exception as e:
        logger.error(f"Error getting custom fields: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error getting custom fields: {str(e)}"
        })

@router.get("/api/custom-fields-ssn-export/pipelines/{account_id}")
async def get_pipelines(account_id: str):
    """Get pipeline and stage mappings for a specific account"""
    try:
        # Find account configuration
        account_config = None
        for subaccount in settings.subaccounts_list:
            if str(subaccount.get('id')) == str(account_id):
                account_config = subaccount
                break
        
        if not account_config:
            return JSONResponse({
                "success": False,
                "message": f"Account {account_id} not found in configuration"
            })
        
        # Initialize the service
        export_service = CustomFieldsSSNExportService()
        
        # Get pipeline mappings
        pipeline_result = await export_service.get_pipeline_stage_mapping(account_config)
        
        return JSONResponse(pipeline_result)
            
    except Exception as e:
        logger.error(f"Error getting pipelines: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error getting pipelines: {str(e)}"
        })

@router.get("/api/custom-fields-ssn-export/debug-test/{account_id}")
async def debug_test_opportunities_with_ssn(account_id: str):
    """DEBUG: Test the opportunities export method directly"""
    try:
        # Find account configuration
        account_config = None
        for subaccount in settings.subaccounts_list:
            if str(subaccount.get('id')) == str(account_id):
                account_config = subaccount
                break
        
        if not account_config:
            return JSONResponse({
                "success": False,
                "message": f"Account {account_id} not found in configuration"
            })
        
        # Initialize the service
        export_service = CustomFieldsSSNExportService()
        
        # Call the method directly
        logger.info(f"*** DEBUG TEST: About to call debug_test_opportunities_with_ssn for account {account_id}")
        opportunities_data = await export_service.debug_test_opportunities_with_ssn(account_config)
        logger.info(f"*** DEBUG TEST: debug_test_opportunities_with_ssn returned {len(opportunities_data)} records")
        
        return JSONResponse({
            "success": True,
            "message": f"Direct method test completed",
            "records_returned": len(opportunities_data),
            "sample_data": opportunities_data[:1] if opportunities_data else [],
            "account_id": account_id,
            "account_name": account_config.get('name')
        })
            
    except Exception as e:
        logger.error(f"Error in debug test: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Error in debug test: {str(e)}"
        })
        