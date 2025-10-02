from fastapi import APIRouter, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, JSONResponse
import os
import json
import pandas as pd
import httpx
from pathlib import Path
import asyncio
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

API_BASE = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"

@router.get("/pipelines-stages")
async def pipelines_stages_page(request: Request):
    """Render the pipelines and stages page"""
    return templates.TemplateResponse("pipelines_stages.html", {"request": request})

@router.post("/api/export-pipelines-stages")
async def export_pipelines_stages():
    """Export all pipelines and stages data from GHL subaccounts"""
    try:
        # Get subaccounts from environment
        subaccounts_json = os.getenv('SUBACCOUNTS', '[]')
        subaccounts = json.loads(subaccounts_json)
        
        if not subaccounts:
            return JSONResponse({
                "success": False,
                "error": "No subaccounts found in configuration"
            }, status_code=400)
        
        rows = []
        processed_accounts = 0
        failed_accounts = []
        
        for sub in subaccounts:
            account_id = sub.get('id')
            account_name = sub.get('name')
            location_id = sub.get('location_id')
            access_token = sub.get('access_token')
            
            if not location_id or not access_token:
                logger.warning(f"Skipping {account_name} ({account_id}): missing location_id or access_token")
                failed_accounts.append({
                    "account_name": account_name,
                    "account_id": account_id,
                    "error": "Missing location_id or access_token"
                })
                continue

            url = f"{API_BASE}/opportunities/pipelines?locationId={location_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Version": API_VERSION,
                "Accept": "application/json"
            }
            
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.get(url, headers=headers)
                    
                if resp.status_code != 200:
                    logger.error(f"Failed for {account_name} ({account_id}): {resp.status_code} {resp.text}")
                    failed_accounts.append({
                        "account_name": account_name,
                        "account_id": account_id,
                        "error": f"HTTP {resp.status_code}: {resp.text[:100]}"
                    })
                    continue
                
                data = resp.json()
                pipelines = data.get("pipelines", [])
                
                for pipeline in pipelines:
                    pipeline_id = pipeline.get("id")
                    pipeline_name = pipeline.get("name")
                    
                    for stage in pipeline.get("stages", []):
                        stage_id = stage.get("id")
                        stage_name = stage.get("name")
                        
                        rows.append({
                            "Account Id": account_id,
                            "Account Name": account_name,
                            "Pipeline Id": pipeline_id,
                            "Pipeline Name": pipeline_name,
                            "Stage Id": stage_id,
                            "Stage Name": stage_name
                        })
                
                processed_accounts += 1
                logger.info(f"Successfully processed {account_name} ({account_id})")
                
            except Exception as e:
                logger.error(f"Error for {account_name} ({account_id}): {str(e)}")
                failed_accounts.append({
                    "account_name": account_name,
                    "account_id": account_id,
                    "error": str(e)
                })
        
        if not rows:
            return JSONResponse({
                "success": False,
                "error": "No pipeline data found",
                "failed_accounts": failed_accounts
            }, status_code=400)
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"all_pipelines_and_stages_{timestamp}.csv"
        filepath = Path(filename)
        
        df.to_csv(filepath, index=False)
        
        return JSONResponse({
            "success": True,
            "message": f"Successfully exported {len(rows)} pipeline/stage records from {processed_accounts} accounts",
            "filename": filename,
            "total_records": len(rows),
            "processed_accounts": processed_accounts,
            "failed_accounts": failed_accounts,
            "download_url": f"/api/download-pipelines-stages/{filename}"
        })
        
    except Exception as e:
        logger.error(f"Export pipelines/stages error: {str(e)}")
        return JSONResponse({
            "success": False,
            "error": str(e)
        }, status_code=500)

@router.get("/api/download-pipelines-stages/{filename}")
async def download_pipelines_stages(filename: str):
    """Download the generated pipelines and stages CSV file"""
    try:
        filepath = Path(filename)
        
        if not filepath.exists():
            raise HTTPException(status_code=404, detail="File not found")
        
        return FileResponse(
            path=filepath,
            filename=filename,
            media_type="text/csv"
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))