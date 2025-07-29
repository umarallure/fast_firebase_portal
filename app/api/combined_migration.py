"""
Combined Migration API Endpoints
Efficiently handles both Contact and Opportunity migration
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, Any, Optional
import asyncio
import logging
from app.config import settings
from app.services.ghl_combined_migration import GHLCombinedMigrationService

router = APIRouter()
logger = logging.getLogger(__name__)

# Global state for tracking migration progress
migration_progress = {
    "active": False,
    "stage": "",
    "current": 0,
    "total": 0,
    "message": "",
    "phase": 0,
    "phase_name": "",
    "overall_progress": 0,
    "results": None,
    "error": None
}

def update_migration_progress(progress_data):
    """Update global migration progress"""
    global migration_progress
    migration_progress.update(progress_data)
    migration_progress["active"] = True

def complete_migration(results=None, error=None):
    """Mark migration as complete"""
    global migration_progress
    migration_progress["active"] = False
    migration_progress["results"] = results
    migration_progress["error"] = error

@router.get("/config")
async def get_combined_migration_config():
    """Get combined migration configuration"""
    try:
        return {
            "success": True,
            "config": {
                "child_location_id": settings.ghl_child_location_id,
                "master_location_id": settings.ghl_master_location_id,
                "has_child_api_key": bool(settings.ghl_child_location_api_key),
                "has_master_api_key": bool(settings.ghl_master_location_api_key),
                "migration_batch_size": settings.migration_batch_size,
                "rate_limit_delay": settings.migration_rate_limit_delay,
                "contact_settings": {
                    "test_mode": settings.contact_migration_test_limit > 0,
                    "test_limit": settings.contact_migration_test_limit if settings.contact_migration_test_limit > 0 else None,
                    "migration_tag": settings.contact_migration_tag or "migrated-contact"
                },
                "opportunity_settings": {
                    "test_mode": settings.opportunity_migration_test_limit > 0,
                    "test_limit": settings.opportunity_migration_test_limit if settings.opportunity_migration_test_limit > 0 else None,
                    "migration_tag": settings.opportunity_migration_tag or "migrated-opportunity"
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting combined migration config: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/preview")
async def get_combined_migration_preview():
    """Get preview of both contact and opportunity migration"""
    try:
        if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="API keys not configured")
        
        migration_service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            preview = await migration_service.get_migration_preview()
            return preview
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting combined migration preview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mapping-analysis")
async def get_mapping_analysis():
    """Get comprehensive field and pipeline mapping analysis"""
    try:
        if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="API keys not configured")
        
        migration_service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            mapping = await migration_service.get_pipeline_and_field_mapping()
            return mapping
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting mapping analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/migrate/full")
async def start_full_migration(background_tasks: BackgroundTasks):
    """Start complete migration (both contacts and opportunities)"""
    global migration_progress
    
    if migration_progress["active"]:
        raise HTTPException(status_code=400, detail="Migration already in progress")
    
    if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
        raise HTTPException(status_code=400, detail="API keys not configured")
    
    # Reset progress
    migration_progress = {
        "active": True,
        "stage": "Initializing",
        "current": 0,
        "total": 0,
        "message": "Starting combined migration...",
        "phase": 0,
        "phase_name": "Initialization",
        "overall_progress": 0,
        "results": None,
        "error": None
    }
    
    # Start migration in background
    background_tasks.add_task(run_full_migration)
    
    return {
        "success": True,
        "message": "Combined migration started (Contacts + Opportunities)",
        "migration_id": "combined_migration_full"
    }

@router.post("/migrate/contacts-only")
async def start_contact_migration(background_tasks: BackgroundTasks):
    """Start contact migration only (Phase 1)"""
    global migration_progress
    
    if migration_progress["active"]:
        raise HTTPException(status_code=400, detail="Migration already in progress")
    
    if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
        raise HTTPException(status_code=400, detail="API keys not configured")
    
    # Reset progress
    migration_progress = {
        "active": True,
        "stage": "Phase 1 - Initializing",
        "current": 0,
        "total": 0,
        "message": "Starting contact migration...",
        "phase": 1,
        "phase_name": "Contact Migration",
        "overall_progress": 0,
        "results": None,
        "error": None
    }
    
    # Start migration in background
    background_tasks.add_task(run_contact_migration)
    
    return {
        "success": True,
        "message": "Contact migration started (Phase 1 only)",
        "migration_id": "combined_migration_contacts"
    }

@router.post("/migrate/opportunities-only")
async def start_opportunity_migration(background_tasks: BackgroundTasks):
    """Start opportunity migration only (Phase 2) - requires existing contact mapping"""
    global migration_progress
    
    if migration_progress["active"]:
        raise HTTPException(status_code=400, detail="Migration already in progress")
    
    if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
        raise HTTPException(status_code=400, detail="API keys not configured")
    
    # Reset progress
    migration_progress = {
        "active": True,
        "stage": "Phase 2 - Initializing",
        "current": 0,
        "total": 0,
        "message": "Starting opportunity migration...",
        "phase": 2,
        "phase_name": "Opportunity Migration",
        "overall_progress": 0,
        "results": None,
        "error": None
    }
    
    # Start migration in background
    background_tasks.add_task(run_opportunity_migration)
    
    return {
        "success": True,
        "message": "Opportunity migration started (Phase 2 only)",
        "migration_id": "combined_migration_opportunities"
    }

@router.post("/migrate/sequential")
async def run_sequential_migration():
    """
    Run sequential contact-opportunity migration
    Process one contact and their opportunities at a time for better relationship mapping
    """
    try:
        if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="API keys not configured")
        
        service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key
        )
        
        result = await service.run_sequential_migration()
        return result
    except Exception as e:
        logger.error(f"Sequential migration failed: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

async def run_full_migration():
    """Run the complete migration in background"""
    try:
        migration_service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key,
            progress_callback=update_migration_progress
        )
        
        try:
            results = await migration_service.run_full_migration()
            complete_migration(results=results)
            logger.info("Combined migration completed successfully")
        finally:
            await migration_service.close()
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Combined migration failed: {error_msg}")
        complete_migration(error=error_msg)

async def run_contact_migration():
    """Run contact migration only in background"""
    try:
        migration_service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key,
            progress_callback=update_migration_progress
        )
        
        try:
            results = await migration_service.run_contact_migration_only()
            complete_migration(results=results)
            logger.info("Contact migration completed successfully")
        finally:
            await migration_service.close()
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Contact migration failed: {error_msg}")
        complete_migration(error=error_msg)

async def run_opportunity_migration():
    """Run opportunity migration only in background"""
    try:
        migration_service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key,
            progress_callback=update_migration_progress
        )
        
        try:
            results = await migration_service.run_opportunity_migration_only()
            complete_migration(results=results)
            logger.info("Opportunity migration completed successfully")
        finally:
            await migration_service.close()
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Opportunity migration failed: {error_msg}")
        complete_migration(error=error_msg)

@router.get("/status")
async def get_migration_status():
    """Get current migration status"""
    return {
        "success": True,
        "status": migration_progress
    }

@router.get("/health")
async def get_migration_health():
    """Get migration service health and capabilities"""
    try:
        if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
            return {
                "success": False,
                "healthy": False,
                "error": "API keys not configured"
            }
        
        migration_service = GHLCombinedMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            status = await migration_service.get_migration_status()
            return {
                "success": True,
                "healthy": True,
                "status": status,
                "api_keys_configured": True,
                "services_available": status.get("services_available", {})
            }
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error checking migration health: {str(e)}")
        return {
            "success": False,
            "healthy": False,
            "error": str(e)
        }

@router.delete("/reset")
async def reset_migration_state():
    """Reset migration state (emergency stop)"""
    global migration_progress
    
    migration_progress = {
        "active": False,
        "stage": "",
        "current": 0,
        "total": 0,
        "message": "",
        "phase": 0,
        "phase_name": "",
        "overall_progress": 0,
        "results": None,
        "error": None
    }
    
    return {
        "success": True,
        "message": "Migration state reset successfully"
    }
