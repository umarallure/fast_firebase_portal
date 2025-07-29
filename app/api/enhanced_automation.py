from fastapi import APIRouter, status, HTTPException, Response
from fastapi.responses import StreamingResponse, JSONResponse
from app.models.schemas import ExportRequest
from app.services.ghl_enhanced_export import process_enhanced_export_request, EnhancedGHLClient
from app.config import settings
import logging
import requests
from typing import List

router = APIRouter()
logger = logging.getLogger(__name__)

# Helper to get subaccounts from settings
def get_subaccounts_from_settings():
    return settings.subaccounts_list

@router.get("/enhanced-subaccounts")
async def get_enhanced_subaccounts():
    return get_subaccounts_from_settings()

@router.get("/enhanced-pipelines/{subaccount_id}")
async def get_enhanced_pipelines(subaccount_id: str):
    subaccounts = get_subaccounts_from_settings()
    sub = next((s for s in subaccounts if s["id"] == subaccount_id), None)
    if not sub:
        return JSONResponse(status_code=404, content={"detail": "Subaccount not found"})
    api_key = sub["api_key"]
    base_url = "https://rest.gohighlevel.com/v1"
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{base_url}/pipelines"
    try:
        response = requests.get(url, headers=headers)
        pipelines = response.json().get("pipelines", [])
        return pipelines
    except Exception as e:
        logger.error(f"Pipeline fetch error: {e}")
        return []

@router.get("/debug-custom-fields/{subaccount_id}")
async def debug_custom_fields(subaccount_id: str, limit: int = 10):
    """
    Debug endpoint to check custom fields for contacts in a specific subaccount
    """
    subaccounts = get_subaccounts_from_settings()
    sub = next((s for s in subaccounts if s["id"] == subaccount_id), None)
    if not sub:
        return JSONResponse(status_code=404, content={"detail": "Subaccount not found"})
    
    api_key = sub["api_key"]
    
    try:
        client = EnhancedGHLClient(api_key)
        
        # Get custom field definitions
        custom_field_definitions = await client.get_custom_fields()
        
        # Get sample contacts
        base_url = "https://rest.gohighlevel.com/v1"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        async with client.client as http_client:
            response = await http_client.get(f"{base_url}/contacts", params={"limit": limit})
            response.raise_for_status()
            contacts_data = response.json()
            contacts = contacts_data.get("contacts", [])
        
        # Target fields we're looking for
        target_fields = [
            'Date of Submission', 'Birth State', 'Age', 'Social Security Number',
            'Height', 'Weight', 'Doctors Name', 'Tobacco User?', 'Health Conditions', 'Medications'
        ]
        
        results = {
            "custom_field_definitions": {
                "total_count": len(custom_field_definitions),
                "fields": [{"id": k, "name": v} for k, v in custom_field_definitions.items()]
            },
            "target_field_matches": [],
            "contacts_checked": [],
            "contacts_with_target_fields": []
        }
        
        # Find target field IDs in definitions
        for field_id, field_name in custom_field_definitions.items():
            for target in target_fields:
                if target.lower() in field_name.lower() or field_name.lower() in target.lower():
                    results["target_field_matches"].append({
                        "id": field_id,
                        "name": field_name,
                        "target": target
                    })
        
        # Check each contact
        for contact in contacts[:limit]:
            contact_id = contact.get("id")
            contact_name = contact.get("name", "Unknown")
            
            contact_details = await client.get_contact_details(contact_id)
            custom_fields = contact_details.get("custom_fields", {})
            
            contact_info = {
                "id": contact_id,
                "name": contact_name,
                "custom_fields_count": len(custom_fields),
                "custom_fields": custom_fields,
                "has_target_fields": False,
                "target_fields_found": []
            }
            
            # Check if any target fields are present
            for target in target_fields:
                for cf_name, cf_value in custom_fields.items():
                    if target.lower() in cf_name.lower() or cf_name.lower() in target.lower():
                        contact_info["has_target_fields"] = True
                        contact_info["target_fields_found"].append({
                            "field_name": cf_name,
                            "target_match": target,
                            "value": cf_value
                        })
            
            results["contacts_checked"].append(contact_info)
            
            if contact_info["has_target_fields"]:
                results["contacts_with_target_fields"].append(contact_info)
        
        results["summary"] = {
            "total_custom_field_definitions": len(custom_field_definitions),
            "target_field_matches_in_definitions": len(results["target_field_matches"]),
            "contacts_checked": len(results["contacts_checked"]),
            "contacts_with_target_fields": len(results["contacts_with_target_fields"])
        }
        
        return results
        
    except Exception as e:
        logger.error(f"Debug custom fields error: {e}")
        return JSONResponse(status_code=500, content={"detail": f"Error: {str(e)}"})

@router.post(
    "/enhanced-export",
    status_code=status.HTTP_200_OK,
    summary="Trigger Enhanced GHL data export with contact details and custom fields",
    response_class=StreamingResponse
)
async def trigger_enhanced_export(export_request: ExportRequest):
    """
    Enhanced export that includes:
    1. All original opportunity data
    2. Enhanced contact information (first name, last name, company, address, etc.)
    3. All custom fields for each contact
    """
    try:
        excel_bytes = await process_enhanced_export_request(export_request)
        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=Enhanced_GHL_Opportunities_Export.xlsx"}
        )
    except Exception as e:
        logger.error(f"Enhanced export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
