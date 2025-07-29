"""
Opportunity Migration API Endpoints
Phase 2: Smart Opportunity Migration
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, Any, Optional
import asyncio
import logging
from app.config import settings
from app.services.ghl_opportunity_migration import GHLOpportunityMigrationService

router = APIRouter()
logger = logging.getLogger(__name__)

# Global state for tracking migration progress
migration_progress = {
    "active": False,
    "stage": "",
    "current": 0,
    "total": 0,
    "message": "",
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
async def get_opportunity_migration_config():
    """Get opportunity migration configuration"""
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
                "opportunity_migration_tag": settings.opportunity_migration_tag,
                "test_settings": {
                    "test_mode": settings.opportunity_migration_test_limit > 0,
                    "test_limit": settings.opportunity_migration_test_limit if settings.opportunity_migration_test_limit > 0 else None,
                    "migration_tag": settings.opportunity_migration_tag or "migrated-opportunity"
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting opportunity migration config: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipeline-mapping")
async def get_pipeline_mapping():
    """Get pipeline and stage mapping between child and master locations"""
    try:
        if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="API keys not configured")
        
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            mapping_result = await migration_service.create_pipeline_mapping()
            return mapping_result
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting pipeline mapping: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/preview")
async def get_migration_preview():
    """Get a preview of opportunities that will be migrated"""
    try:
        if not settings.ghl_child_location_api_key or not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="API keys not configured")
        
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            preview = await migration_service.get_migration_preview()
            return preview
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting migration preview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/migrate")
async def start_opportunity_migration(background_tasks: BackgroundTasks):
    """Start the opportunity migration process"""
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
        "message": "Starting opportunity migration...",
        "results": None,
        "error": None
    }
    
    # Start migration in background
    background_tasks.add_task(run_opportunity_migration)
    
    return {
        "success": True,
        "message": "Opportunity migration started",
        "migration_id": "opportunity_migration_1"
    }

async def run_opportunity_migration():
    """Run the opportunity migration in background"""
    try:
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key,
            progress_callback=update_migration_progress
        )
        
        try:
            results = await migration_service.migrate_opportunities()
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

@router.get("/pipelines/child")
async def get_child_pipelines():
    """Get all pipelines from child location"""
    try:
        if not settings.ghl_child_location_api_key:
            raise HTTPException(status_code=400, detail="Child API key not configured")
        
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key,
            master_api_key=settings.ghl_master_location_api_key or "dummy"
        )
        
        try:
            pipelines = await migration_service.fetch_pipelines(migration_service.child_client)
            return {
                "success": True,
                "pipelines": pipelines,
                "count": len(pipelines)
            }
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting child pipelines: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipelines/master")
async def get_master_pipelines():
    """Get all pipelines from master location"""
    try:
        if not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="Master API key not configured")
        
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key or "dummy",
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            pipelines = await migration_service.fetch_pipelines(migration_service.master_client)
            return {
                "success": True,
                "pipelines": pipelines,
                "count": len(pipelines)
            }
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting master pipelines: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipelines/{pipeline_id}/opportunities")
async def get_pipeline_opportunities(pipeline_id: str, location: str = "child"):
    """Get opportunities from a specific pipeline"""
    try:
        if location == "child":
            api_key = settings.ghl_child_location_api_key
        else:
            api_key = settings.ghl_master_location_api_key
            
        if not api_key:
            raise HTTPException(status_code=400, detail=f"{location.title()} API key not configured")
        
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key or "dummy",
            master_api_key=settings.ghl_master_location_api_key or "dummy"
        )
        
        try:
            client = migration_service.child_client if location == "child" else migration_service.master_client
            opportunities = await migration_service.fetch_opportunities_from_pipeline(client, pipeline_id)
            
            return {
                "success": True,
                "pipeline_id": pipeline_id,
                "location": location,
                "opportunities": opportunities,
                "count": len(opportunities)
            }
        finally:
            await migration_service.close()
            
    except Exception as e:
        logger.error(f"Error getting pipeline opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/test-create-opportunity")
async def test_create_opportunity(opportunity_data: Dict[str, Any]):
    """Test creating a single opportunity in master location"""
    try:
        if not settings.ghl_master_location_api_key:
            raise HTTPException(status_code=400, detail="Master API key not configured")
        
        # Validate required fields
        required_fields = ["title", "stageId", "contactId"]
        for field in required_fields:
            if field not in opportunity_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        migration_service = GHLOpportunityMigrationService(
            child_api_key=settings.ghl_child_location_api_key or "dummy",
            master_api_key=settings.ghl_master_location_api_key
        )
        
        try:
            # Extract pipeline ID from stage or use default logic
            pipeline_id = opportunity_data.get("pipelineId")
            if not pipeline_id:
                # Find pipeline that contains the stage
                master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client)
                stage_id = opportunity_data["stageId"]
                
                for pipeline in master_pipelines:
                    for stage in pipeline.get("stages", []):
                        if stage["id"] == stage_id:
                            pipeline_id = pipeline["id"]
                            break
                    if pipeline_id:
                        break
                
                if not pipeline_id:
                    raise HTTPException(status_code=400, detail="Could not find pipeline for the specified stage")
            
            created_opportunity = await migration_service.create_opportunity(opportunity_data, pipeline_id)
            
            if created_opportunity:
                return {
                    "success": True,
                    "opportunity": created_opportunity,
                    "message": "Opportunity created successfully"
                }
            else:
                raise HTTPException(status_code=500, detail="Failed to create opportunity")
                
        finally:
            await migration_service.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing opportunity creation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
