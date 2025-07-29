from fastapi import APIRouter, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import os

from app.services.bulk_opportunity_owner_update import bulk_opportunity_owner_service

router = APIRouter()

@router.post("/api/bulk-update-opportunity-owner/upload")
async def upload_opportunity_owner_csv(file: UploadFile = File(...)):
    """Upload and parse CSV file for opportunity owner updates"""
    
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Read and parse CSV
        csv_content = await file.read()
        csv_text = csv_content.decode('utf-8')
        
        result = bulk_opportunity_owner_service.parse_csv(csv_text)
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result['error'])
        
        return JSONResponse(content={
            'success': True,
            'message': f'CSV parsed successfully. Found {len(result["opportunities"])} opportunities to update.',
            'data': result
        })
        
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Invalid file encoding. Please use UTF-8 encoded CSV files.")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@router.post("/api/bulk-update-opportunity-owner/validate-users")
async def validate_user_ids(
    opportunities_data: str = Form(...),
    account_id: str = Form(...)
):
    """Validate user IDs before processing"""
    
    try:
        import json
        opportunities = json.loads(opportunities_data)
        
        # Get API key for this account  
        from app.config import settings
        subaccounts = settings.subaccounts_list
        account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}
        
        api_key = account_api_keys.get(account_id)
        if not api_key:
            raise HTTPException(status_code=400, detail=f"No API key found for account {account_id}")
        
        result = await bulk_opportunity_owner_service.validate_user_ids(opportunities, api_key)
        
        return JSONResponse(content={
            'success': True,
            'validation': result
        })
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid opportunities data format")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error validating users: {str(e)}")

@router.post("/api/bulk-update-opportunity-owner/process")
async def process_opportunity_owner_updates(
    opportunities_data: str = Form(...),
    dry_run: bool = Form(default=False),
    batch_size: int = Form(default=10)
):
    """Process opportunity owner updates"""
    
    try:
        import json
        opportunities = json.loads(opportunities_data)
        
        # Validate batch size
        if batch_size < 1 or batch_size > 50:
            raise HTTPException(status_code=400, detail="Batch size must be between 1 and 50")
        
        result = await bulk_opportunity_owner_service.process_opportunity_owner_updates(
            opportunities, dry_run=dry_run, batch_size=batch_size
        )
        
        return JSONResponse(content=result)
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid opportunities data format")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing updates: {str(e)}")

@router.get("/api/bulk-update-opportunity-owner/progress/{processing_id}")
async def get_opportunity_owner_update_progress(processing_id: str):
    """Get progress of opportunity owner update operation"""
    
    result = bulk_opportunity_owner_service.get_progress(processing_id)
    return JSONResponse(content=result)

@router.get("/api/bulk-update-opportunity-owner/sample-csv")
async def download_sample_csv():
    """Download sample CSV template for opportunity owner updates"""
    
    try:
        csv_content = bulk_opportunity_owner_service.generate_sample_csv()
        
        from fastapi.responses import Response
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sample_opportunity_owner_update.csv"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating sample CSV: {str(e)}")
