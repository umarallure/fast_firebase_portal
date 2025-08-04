"""
Lead Search API Endpoints
Provides API for searching leads with fuzzy name matching
"""

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional
import logging
from app.services.lead_search import lead_search_service

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/upload-database")
async def upload_lead_database(
    database_file: UploadFile = File(..., description="CSV file containing lead database")
):
    """Upload and load lead database CSV file"""
    try:
        # Validate file type
        if not database_file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Read file content
        content = await database_file.read()
        
        # Try different encodings
        csv_content = None
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                csv_content = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if csv_content is None:
            raise HTTPException(status_code=400, detail="Unable to decode CSV file. Please ensure it uses a standard encoding (UTF-8, Latin-1, etc.)")
        
        # Load data into the search service
        success = lead_search_service.load_csv_data(csv_content)
        
        if not success:
            raise HTTPException(status_code=400, detail="Failed to load CSV data")
        
        # Get database statistics
        stats = lead_search_service.get_database_stats()
        
        return {
            "success": True,
            "message": f"Database loaded successfully with {stats['total_leads']} leads",
            "stats": stats
        }
        
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Unable to decode CSV file. Please ensure it uses a standard encoding.")
    except Exception as e:
        logger.error(f"Error uploading database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload database: {str(e)}")

@router.get("/search")
async def search_leads_by_name(
    name: str = Query(..., description="Name to search for"),
    min_score: int = Query(60, description="Minimum similarity score (0-100)", ge=0, le=100),
    max_results: int = Query(10, description="Maximum number of results", ge=1, le=50)
):
    """Search for leads by name using fuzzy matching"""
    try:
        if not name or len(name.strip()) < 2:
            raise HTTPException(status_code=400, detail="Search name must be at least 2 characters long")
        
        results = lead_search_service.search_leads(name, min_score, max_results)
        
        return {
            "success": True,
            "search_term": name,
            "min_score": min_score,
            "results_count": len(results),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error searching leads: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@router.get("/database-status")
async def get_database_status():
    """Get current database status and statistics"""
    try:
        stats = lead_search_service.get_database_stats()
        
        return {
            "success": True,
            "database_loaded": stats['total_leads'] > 0,
            "stats": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting database status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get database status: {str(e)}")

@router.post("/batch-search")
async def batch_search_leads(
    names: List[str],
    min_score: int = Query(60, description="Minimum similarity score (0-100)", ge=0, le=100),
    max_results_per_name: int = Query(5, description="Maximum results per name", ge=1, le=20)
):
    """Search for multiple leads at once"""
    try:
        if not names or len(names) == 0:
            raise HTTPException(status_code=400, detail="At least one name is required")
        
        if len(names) > 50:
            raise HTTPException(status_code=400, detail="Maximum 50 names allowed per batch")
        
        batch_results = {}
        
        for name in names:
            if name and len(name.strip()) >= 2:
                results = lead_search_service.search_leads(name.strip(), min_score, max_results_per_name)
                batch_results[name] = results
            else:
                batch_results[name] = []
        
        return {
            "success": True,
            "batch_size": len(names),
            "min_score": min_score,
            "results": batch_results
        }
        
    except Exception as e:
        logger.error(f"Error in batch search: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch search failed: {str(e)}")

@router.get("/info")
async def get_lead_search_info():
    """Get information about the lead search system"""
    return {
        "name": "Lead Search System",
        "description": "Search for leads using fuzzy name matching algorithms",
        "features": [
            "Fuzzy name matching with multiple algorithms",
            "Handles name variations and typos",
            "Supports first/last name swaps",
            "Configurable similarity thresholds",
            "Batch search capabilities"
        ],
        "required_fields": [
            "full_name (required for search)",
            "Draft_Date or draft_date",
            "Monthly Premium or monthly_premium", 
            "Coverage Amount or coverage_amount",
            "Carrier or carrier",
            "center (Call Center Name)"
        ],
        "search_algorithms": [
            "Direct fuzzy matching",
            "Token sort matching (handles word order)",
            "Partial matching (handles partial names)",
            "Token set matching (handles different word sets)",
            "Name swap detection (first/last name reversal)"
        ]
    }
