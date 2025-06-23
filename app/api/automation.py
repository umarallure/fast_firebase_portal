from fastapi import APIRouter, status, HTTPException, Response
from fastapi.responses import StreamingResponse, JSONResponse
from app.models.schemas import ExportRequest
from app.services.excel_writer import generate_excel
from app.services.ghl_export import process_export_request
import logging
import os
import json
import requests
from typing import List

router = APIRouter()
logger = logging.getLogger(__name__)

# Helper to get subaccounts from env

def get_subaccounts_from_env():
    subaccounts_json = os.getenv("SUBACCOUNTS", "[]")
    try:
        subaccounts = json.loads(subaccounts_json)
    except Exception:
        subaccounts = []
    return subaccounts

@router.get("/subaccounts")
async def get_subaccounts():
    return get_subaccounts_from_env()

@router.get("/pipelines/{subaccount_id}")
async def get_pipelines(subaccount_id: str):
    subaccounts = get_subaccounts_from_env()
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

@router.post(
    "/automation/export",
    status_code=status.HTTP_200_OK,
    summary="Trigger GHL data export",
    response_class=StreamingResponse
)
async def trigger_export(export_request: ExportRequest):
    excel_bytes = await process_export_request(export_request)
    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=GHL_Opportunities_Export.xlsx"}
    )