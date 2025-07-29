from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
import os
import tempfile
import io
from datetime import datetime
import json
from typing import Optional
import logging

from ..services.transfer_portal_comparison import TransferPortalComparison

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/process")
async def process_transfer_portal_comparison(
    master_file: UploadFile = File(..., description="Master CSV file (transferportalmaster format)"),
    child_file: UploadFile = File(..., description="Child CSV file (transferportalchild format)"),
    return_format: str = Form("csv", description="Return format: csv or json")
):
    """
    Process transfer portal comparison between master and child CSV files.
    Returns new entries from child that are not found in master.
    """
    
    if not master_file.filename.endswith('.csv') or not child_file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Both files must be CSV format")
    
    try:
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save uploaded files
            master_path = os.path.join(temp_dir, "master.csv")
            child_path = os.path.join(temp_dir, "child.csv")
            
            # Write uploaded files to temporary location
            with open(master_path, "wb") as f:
                content = await master_file.read()
                f.write(content)
            
            with open(child_path, "wb") as f:
                content = await child_file.read()
                f.write(content)
            
            # Process the comparison
            comparison = TransferPortalComparison()
            processed_entries, stats = comparison.process_comparison(
                master_path, child_path, temp_dir
            )
            
            # Generate summary report
            summary = comparison.generate_summary_report(processed_entries, stats)
            
            if return_format.lower() == "json":
                # Return JSON response with data and summary
                return JSONResponse({
                    "success": True,
                    "stats": stats,
                    "summary": summary,
                    "new_entries": processed_entries.to_dict('records') if not processed_entries.empty else [],
                    "total_new_entries": len(processed_entries)
                })
            
            else:
                # Return CSV file
                if processed_entries.empty:
                    # Return empty CSV with headers
                    empty_df = pd.DataFrame(columns=['Customer Phone Number', 'Name', 'Policy Status', 'GHL Pipeline Stage', 'CALL CENTER'])
                    output = io.StringIO()
                    empty_df.to_csv(output, index=False)
                    output.seek(0)
                    
                    return StreamingResponse(
                        io.BytesIO(output.getvalue().encode()),
                        media_type="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=transfer_portal_new_entries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
                    )
                
                # Convert DataFrame to CSV
                output = io.StringIO()
                processed_entries.to_csv(output, index=False)
                output.seek(0)
                
                return StreamingResponse(
                    io.BytesIO(output.getvalue().encode()),
                    media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=transfer_portal_new_entries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
                )
                
    except Exception as e:
        logger.error(f"Error processing transfer portal comparison: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@router.get("/info")
async def get_transfer_portal_info():
    """
    Get information about the transfer portal comparison automation
    """
    return {
        "automation_name": "Transfer Portal Comparison",
        "description": "Compare child and master CSV files to find new entries not present in master",
        "input_files": {
            "master_file": {
                "format": "CSV",
                "required_columns": ["Customer Phone Number", "Name", "Policy Status", "GHL Pipeline Stage", "CALL CENTER"],
                "description": "Master database containing existing leads"
            },
            "child_file": {
                "format": "CSV", 
                "required_columns": ["Contact Name", "phone", "pipeline", "stage", "Account Id"],
                "description": "Child database with leads to check against master"
            }
        },
        "output": {
            "csv_format": "New entries with master CSV structure",
            "json_format": "Processing statistics and new entries data"
        },
        "features": [
            "Phone number normalization for accurate matching",
            "Account ID to name mapping from environment variables",
            "Processing statistics and summary reports",
            "Flexible output formats (CSV/JSON)"
        ]
    }


@router.post("/preview")
async def preview_transfer_portal_files(
    master_file: Optional[UploadFile] = File(None),
    child_file: Optional[UploadFile] = File(None)
):
    """
    Preview the structure and first few rows of uploaded CSV files
    """
    try:
        result = {}
        
        if master_file:
            content = await master_file.read()
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(io.StringIO(content.decode(encoding)))
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                df = pd.read_csv(io.StringIO(content.decode('utf-8', errors='ignore')))
                
            result["master_preview"] = {
                "filename": master_file.filename,
                "columns": list(df.columns),
                "row_count": len(df),
                "sample_data": df.head(5).to_dict('records')
            }
        
        if child_file:
            content = await child_file.read()
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(io.StringIO(content.decode(encoding)))
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                df = pd.read_csv(io.StringIO(content.decode('utf-8', errors='ignore')))
                
            result["child_preview"] = {
                "filename": child_file.filename,
                "columns": list(df.columns),
                "row_count": len(df),
                "sample_data": df.head(5).to_dict('records')
            }
        
        return JSONResponse(result)
        
    except Exception as e:
        logger.error(f"Error previewing files: {e}")
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")
