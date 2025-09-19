from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import Response
from typing import Optional
import logging
import os
from datetime import datetime
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

@router.get("/unmatched-csv/{matching_id}")
async def download_unmatched_csv(matching_id: str):
    """Download CSV of unmatched child opportunities"""

    try:
        # Get matching progress to check if completed
        progress = master_child_opportunity_service.get_matching_progress(matching_id)

        if not progress['success']:
            raise HTTPException(status_code=404, detail="Matching ID not found")

        if progress['progress']['status'] != 'completed':
            raise HTTPException(status_code=400, detail="Matching process not completed yet")

        # Get the matches data and filter for unmatched records
        matches = progress['progress'].get('matches', [])
        unmatched_matches = [match for match in matches if match.get('match_type') == 'no_match']

        if not unmatched_matches:
            raise HTTPException(status_code=404, detail="No unmatched opportunities found")

        # Generate CSV content
        import io
        import pandas as pd

        csv_data = []
        for match in unmatched_matches:
            child_opp = match.get('child_opportunity', {})
            csv_data.append({
                'contact_name': child_opp.get('contact_name', ''),
                'phone': child_opp.get('phone', ''),
                'email': child_opp.get('email', ''),
                'opportunity_name': child_opp.get('opportunity_name', ''),
                'pipeline': child_opp.get('pipeline', ''),
                'stage': child_opp.get('stage', ''),
                'status': child_opp.get('status', ''),
                'value': child_opp.get('value', ''),
                'account_id': child_opp.get('account_id', ''),
                'opportunity_id': child_opp.get('opportunity_id', ''),
                'match_score': match.get('match_score', 0.0),
                'match_type': match.get('match_type', ''),
                'confidence': match.get('confidence', ''),
                'skip_reason': match.get('skip_reason', ''),
                'matching_id': matching_id
            })

        df = pd.DataFrame(csv_data)
        csv_output = io.StringIO()
        df.to_csv(csv_output, index=False)

        return Response(
            content=csv_output.getvalue(),
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="unmatched_opportunities_{matching_id}.csv"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading unmatched CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@router.get("/export-unmatched-failed/{processing_id}")
async def export_unmatched_and_failed_csv(processing_id: str):
    """Export unmatched records and failed updates to CSV"""

    try:
        # Export the data
        result = await master_child_opportunity_service.export_unmatched_and_failed_to_csv(processing_id)

        if result.startswith("Error:") or result.startswith("No unmatched records"):
            raise HTTPException(status_code=404, detail=result)

        # Extract filename from result message
        if "to " in result:
            filename_part = result.split("to ")[-1]
        else:
            filename_part = f"unmatched_and_failed_updates_{processing_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Use the service's results directory
        filepath = os.path.join(master_child_opportunity_service.results_dir, filename_part)

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                csv_content = f.read()
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="CSV file not found")

        return Response(
            content=csv_content,
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{filename_part}"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting unmatched and failed CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
