from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import Response
from typing import Optional
import logging
from app.services.master_child_opportunity_update import master_child_opportunity_service

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/upload")
async def upload_csv_files(
    master_file: UploadFile = File(...),
    child_file: UploadFile = File(...)
):
    """Upload and parse master and child CSV files"""
    
    try:
        # Validate file types
        if not master_file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Master file must be a CSV file")
        
        if not child_file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Child file must be a CSV file")
        
        # Read file contents
        master_content = await master_file.read()
        child_content = await child_file.read()
        
        # Parse CSV files
        result = master_child_opportunity_service.parse_csv_files(
            master_content.decode('utf-8'),
            child_content.decode('utf-8')
        )
        
        if result['success']:
            return {
                'success': True,
                'message': 'CSV files parsed successfully',
                'data': result
            }
        else:
            raise HTTPException(status_code=400, detail=result['error'])
    
    except Exception as e:
        logger.error(f"Error uploading CSV files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File processing failed: {str(e)}")

@router.post("/match")
async def start_matching(
    master_opportunities: str = Form(...),
    child_opportunities: str = Form(...),
    match_threshold: float = Form(0.7),
    high_confidence_threshold: float = Form(0.9)
):
    """Start the opportunity matching process"""
    
    try:
        import json
        
        # Parse JSON data
        master_data = json.loads(master_opportunities)
        child_data = json.loads(child_opportunities)
        
        # Start matching process
        result = await master_child_opportunity_service.match_opportunities(
            master_data,
            child_data,
            match_threshold,
            high_confidence_threshold
        )
        
        return result
    
    except Exception as e:
        logger.error(f"Error starting matching: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Matching process failed: {str(e)}")

@router.get("/match-progress/{matching_id}")
async def get_matching_progress(matching_id: str):
    """Get progress of opportunity matching process"""
    
    try:
        result = master_child_opportunity_service.get_matching_progress(matching_id)
        return result
    
    except Exception as e:
        logger.error(f"Error getting matching progress: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Progress check failed: {str(e)}")

@router.post("/process")
async def start_processing(
    matches_data: str = Form(...),
    dry_run: bool = Form(False),
    batch_size: int = Form(10),
    process_exact_only: bool = Form(False)
):
    """Start the opportunity update process"""
    
    try:
        import json
        
        # Parse matches data
        matches = json.loads(matches_data)
        
        # Start processing
        result = await master_child_opportunity_service.process_opportunity_updates(
            matches,
            dry_run,
            batch_size,
            process_exact_only
        )
        
        return result
    
    except Exception as e:
        logger.error(f"Error starting processing: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@router.get("/progress/{processing_id}")
async def get_progress(processing_id: str):
    """Get progress of opportunity update process"""
    
    try:
        result = master_child_opportunity_service.get_progress(processing_id)
        return result
    
    except Exception as e:
        logger.error(f"Error getting progress: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Progress check failed: {str(e)}")

@router.get("/sample-master-csv")
async def download_sample_master_csv():
    """Download sample master CSV template"""
    
    try:
        master_csv, _ = master_child_opportunity_service.generate_sample_csvs()
        
        return Response(
            content=master_csv,
            media_type='text/csv',
            headers={'Content-Disposition': 'attachment; filename="sample_master_opportunities.csv"'}
        )
    
    except Exception as e:
        logger.error(f"Error generating sample master CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Sample generation failed: {str(e)}")

@router.get("/sample-child-csv")
async def download_sample_child_csv():
    """Download sample child CSV template"""
    
    try:
        _, child_csv = master_child_opportunity_service.generate_sample_csvs()
        
        return Response(
            content=child_csv,
            media_type='text/csv',
            headers={'Content-Disposition': 'attachment; filename="sample_child_opportunities.csv"'}
        )
    
    except Exception as e:
        logger.error(f"Error generating sample child CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Sample generation failed: {str(e)}")
