"""
Migration API endpoints for GoHighLevel V1 API migration
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, status
from fastapi.responses import JSONResponse
from app.models.schemas import MigrationRequest, MigrationStatus
from app.services.ghl_migration import GHLMigrationService
from app.services.ghl_api_docs import get_api_docs_for_endpoint
from app.config import settings
import asyncio
import uuid
from datetime import datetime
import logging
import os
import json
from typing import Dict, Any

router = APIRouter()
logger = logging.getLogger(__name__)

# In-memory storage for migration status (in production, use Redis or database)
migration_status_store: Dict[str, Dict[str, Any]] = {}

def get_api_keys_from_env():
    """Get API keys from environment variables"""
    child_api_key = settings.ghl_child_location_api_key
    master_api_key = settings.ghl_master_location_api_key
    child_location_id = settings.ghl_child_location_id
    master_location_id = settings.ghl_master_location_id
    
    if not all([child_api_key, master_api_key, child_location_id, master_location_id]):
        missing_vars = []
        if not child_api_key:
            missing_vars.append("GHL_CHILD_LOCATION_API_KEY")
        if not master_api_key:
            missing_vars.append("GHL_MASTER_LOCATION_API_KEY")
        if not child_location_id:
            missing_vars.append("GHL_CHILD_LOCATION_ID")
        if not master_location_id:
            missing_vars.append("GHL_MASTER_LOCATION_ID")
        
        raise HTTPException(
            status_code=400, 
            detail=f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    return {
        "child_api_key": child_api_key,
        "master_api_key": master_api_key,
        "child_location_id": child_location_id,
        "master_location_id": master_location_id
    }

async def run_migration_background(migration_id: str, child_api_key: str, master_api_key: str):
    """Background task to run the migration with progress tracking"""
    
    def progress_callback(progress_data):
        """Update progress in migration status store"""
        migration_status_store[migration_id]["current_stage"] = progress_data["stage"]
        migration_status_store[migration_id]["progress"] = progress_data
        migration_status_store[migration_id]["last_update"] = datetime.now().isoformat()
    
    try:
        # Update status to running
        migration_status_store[migration_id]["status"] = "running"
        migration_status_store[migration_id]["start_time"] = datetime.now().isoformat()
        migration_status_store[migration_id]["progress"] = {
            "stage": "initializing",
            "current": 0,
            "total": 0,
            "percentage": 0,
            "message": "Starting migration..."
        }
        
        # Create migration service with progress callback
        migration_service = GHLMigrationService(child_api_key, master_api_key, progress_callback)
        results = await migration_service.run_full_migration()
        
        # Update status with results
        migration_status_store[migration_id].update(results)
        migration_status_store[migration_id]["status"] = "completed"
        migration_status_store[migration_id]["progress"]["percentage"] = 100
        migration_status_store[migration_id]["progress"]["message"] = "Migration completed successfully"
        
        logger.info(f"Migration {migration_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Migration {migration_id} failed: {str(e)}")
        migration_status_store[migration_id]["status"] = "failed"
        migration_status_store[migration_id]["errors"] = [str(e)]
        migration_status_store[migration_id]["end_time"] = datetime.now().isoformat()
        migration_status_store[migration_id]["progress"]["message"] = f"Migration failed: {str(e)}"

@router.post("/migration/start", response_model=Dict[str, str])
async def start_migration(
    migration_request: MigrationRequest,
    background_tasks: BackgroundTasks
):
    """Start a new migration process"""
    try:
        # Generate unique migration ID
        migration_id = str(uuid.uuid4())
        
        # Initialize migration status
        migration_status_store[migration_id] = {
            "migration_id": migration_id,
            "status": "pending",
            "start_time": None,
            "end_time": None,
            "duration_minutes": None,
            "custom_fields": {"status": "pending", "mapped_count": 0},
            "pipelines": {"status": "pending", "mapped_count": 0},
            "contacts": {"status": "pending", "mapped_count": 0},
            "opportunities": {"status": "pending", "created_count": 0},
            "errors": []
        }
        
        # Add background task
        background_tasks.add_task(
            run_migration_background,
            migration_id,
            migration_request.child_api_key,
            migration_request.master_api_key
        )
        
        logger.info(f"Started migration {migration_id}")
        
        return {
            "migration_id": migration_id,
            "status": "started",
            "message": "Migration started successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to start migration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start migration: {str(e)}"
        )

@router.post("/migration/start-env", response_model=Dict[str, str])
async def start_migration_from_env(background_tasks: BackgroundTasks):
    """Start migration using API keys from environment variables"""
    try:
        api_keys = get_api_keys_from_env()
        
        if not all(api_keys.values()):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required environment variables for migration"
            )
        
        # Create migration request from env vars
        migration_request = MigrationRequest(
            child_location_id=api_keys["child_location_id"],
            master_location_id=api_keys["master_location_id"],
            child_api_key=api_keys["child_api_key"],
            master_api_key=api_keys["master_api_key"]
        )
        
        return await start_migration(migration_request, background_tasks)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start migration from env: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start migration: {str(e)}"
        )

@router.get("/migration/status/{migration_id}", response_model=MigrationStatus)
async def get_migration_status(migration_id: str):
    """Get the status of a migration"""
    if migration_id not in migration_status_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Migration not found"
        )
    
    return MigrationStatus(**migration_status_store[migration_id])

@router.get("/migration/list", response_model=Dict[str, Any])
async def list_migrations():
    """List all migrations and their status"""
    return {
        "migrations": list(migration_status_store.values()),
        "total_count": len(migration_status_store)
    }

@router.delete("/migration/{migration_id}")
async def delete_migration(migration_id: str):
    """Delete a migration record"""
    if migration_id not in migration_status_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Migration not found"
        )
    
    del migration_status_store[migration_id]
    return {"message": "Migration record deleted successfully"}

@router.get("/migration/test-connection")
async def test_api_connections():
    """Test API connections to both child and master accounts"""
    try:
        api_keys = get_api_keys_from_env()
        
        if not all(api_keys.values()):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required environment variables"
            )
        
        # Test both connections
        migration_service = GHLMigrationService(
            api_keys["child_api_key"],
            api_keys["master_api_key"]
        )
        
        # Test child connection
        child_pipelines = await migration_service.fetch_pipelines(migration_service.child_client, "child")
        child_status = "connected" if child_pipelines else "failed"
        
        # Test master connection
        master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client, "master")
        master_status = "connected" if master_pipelines else "failed"
        
        await migration_service.close()
        
        return {
            "child_account": {
                "status": child_status,
                "location_id": api_keys["child_location_id"],
                "pipelines_count": len(child_pipelines) if child_pipelines else 0
            },
            "master_account": {
                "status": master_status,
                "location_id": api_keys["master_location_id"],
                "pipelines_count": len(master_pipelines) if master_pipelines else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Connection test failed: {str(e)}"
        )

@router.get("/migration/preview")
async def preview_migration_data():
    """Preview what data would be migrated without actually migrating"""
    try:
        api_keys = get_api_keys_from_env()
        
        if not all(api_keys.values()):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required environment variables"
            )
        
        migration_service = GHLMigrationService(
            api_keys["child_api_key"],
            api_keys["master_api_key"]
        )
        
        # Fetch preview data
        child_custom_fields = await migration_service.fetch_custom_fields(migration_service.child_client, "child")
        child_pipelines = await migration_service.fetch_pipelines(migration_service.child_client, "child")
        child_contacts = await migration_service.fetch_contacts(migration_service.child_client, "child")
        child_opportunities = await migration_service.fetch_opportunities(migration_service.child_client, "child")
        
        # Fetch master data for comparison
        master_custom_fields = await migration_service.fetch_custom_fields(migration_service.master_client, "master")
        master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client, "master")
        
        await migration_service.close()
        
        # Calculate what would be created/mapped
        child_field_names = {field["name"] for field in child_custom_fields}
        master_field_names = {field["name"] for field in master_custom_fields}
        fields_to_create = child_field_names - master_field_names
        
        child_pipeline_names = {pipeline["name"] for pipeline in child_pipelines}
        master_pipeline_names = {pipeline["name"] for pipeline in master_pipelines}
        pipelines_missing = child_pipeline_names - master_pipeline_names
        
        return {
            "child_account_data": {
                "custom_fields": len(child_custom_fields),
                "pipelines": len(child_pipelines),
                "contacts": len(child_contacts),
                "opportunities": len(child_opportunities)
            },
            "master_account_data": {
                "custom_fields": len(master_custom_fields),
                "pipelines": len(master_pipelines)
            },
            "migration_plan": {
                "custom_fields_to_create": len(fields_to_create),
                "custom_fields_to_map": len(child_field_names & master_field_names),
                "pipelines_missing_in_master": list(pipelines_missing),
                "contacts_to_migrate": len(child_contacts),
                "opportunities_to_migrate": len(child_opportunities)
            }
        }
        
    except Exception as e:
        logger.error(f"Preview failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Preview failed: {str(e)}"
        )

@router.get("/migration/docs/{endpoint_name}")
async def get_api_documentation(endpoint_name: str):
    """Get API documentation for specific GHL endpoints"""
    docs = get_api_docs_for_endpoint(endpoint_name)
    if "error" in docs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Documentation not found for endpoint: {endpoint_name}"
        )
    return docs

@router.get("/migration/docs")
async def get_all_api_documentation():
    """Get complete API documentation for GHL migration"""
    return get_api_docs_for_endpoint("all")

@router.get("/migration/analyze")
async def analyze_migration_compatibility():
    """Analyze migration compatibility and generate smart mapping report"""
    try:
        api_keys = get_api_keys_from_env()
        
        if not all(api_keys.values()):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required environment variables"
            )
        
        migration_service = GHLMigrationService(
            api_keys["child_api_key"],
            api_keys["master_api_key"]
        )
        
        # Fetch data from both accounts
        child_custom_fields = await migration_service.fetch_custom_fields(migration_service.child_client, "child")
        child_pipelines = await migration_service.fetch_pipelines(migration_service.child_client, "child")
        
        master_custom_fields = await migration_service.fetch_custom_fields(migration_service.master_client, "master")
        master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client, "master")
        
        # Generate smart mapping strategies
        field_strategy = migration_service.smart_mapper.create_custom_field_mapping_strategy(
            child_custom_fields, master_custom_fields
        )
        pipeline_strategy = migration_service.smart_mapper.create_pipeline_mapping_strategy(
            child_pipelines, master_pipelines
        )
        
        # Generate comprehensive report
        mapping_report = migration_service.smart_mapper.generate_migration_report(
            pipeline_strategy, field_strategy
        )
        
        await migration_service.close()
        
        return {
            "analysis_timestamp": datetime.now().isoformat(),
            "child_account_summary": {
                "custom_fields": len(child_custom_fields),
                "pipelines": len(child_pipelines),
                "total_stages": sum(len(p.get("stages", [])) for p in child_pipelines)
            },
            "master_account_summary": {
                "custom_fields": len(master_custom_fields),
                "pipelines": len(master_pipelines),
                "total_stages": sum(len(p.get("stages", [])) for p in master_pipelines)
            },
            "smart_mapping_analysis": mapping_report
        }
        
    except Exception as e:
        logger.error(f"Migration analysis failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Migration analysis failed: {str(e)}"
        )

@router.get("/migration/debug/env")
async def debug_environment_variables():
    """Debug endpoint to check environment variables"""
    return {
        "child_api_key_exists": bool(settings.ghl_child_location_api_key or os.getenv("GHL_CHILD_LOCATION_API_KEY")),
        "master_api_key_exists": bool(settings.ghl_master_location_api_key or os.getenv("GHL_MASTER_LOCATION_API_KEY")),
        "child_location_id": settings.ghl_child_location_id or os.getenv("GHL_CHILD_LOCATION_ID") or "NOT_SET",
        "master_location_id": settings.ghl_master_location_id or os.getenv("GHL_MASTER_LOCATION_ID") or "NOT_SET",
        "env_file_loaded": os.path.exists(".env"),
        "direct_env_check": {
            "GHL_CHILD_LOCATION_API_KEY": bool(os.getenv("GHL_CHILD_LOCATION_API_KEY")),
            "GHL_MASTER_LOCATION_API_KEY": bool(os.getenv("GHL_MASTER_LOCATION_API_KEY")),
            "GHL_CHILD_LOCATION_ID": os.getenv("GHL_CHILD_LOCATION_ID") or "NOT_SET",
            "GHL_MASTER_LOCATION_ID": os.getenv("GHL_MASTER_LOCATION_ID") or "NOT_SET"
        }
    }

@router.get("/migration/summary")
async def get_migration_summary():
    """Get a summary of all migrations with enhanced statistics"""
    try:
        # Get basic connection test first
        api_keys = get_api_keys_from_env()
        migration_service = GHLMigrationService(api_keys["child_api_key"], api_keys["master_api_key"])
        
        # Fetch preview data - same as preview endpoint
        child_custom_fields = await migration_service.fetch_custom_fields(migration_service.child_client, "child")
        child_pipelines = await migration_service.fetch_pipelines(migration_service.child_client, "child")
        child_contacts = await migration_service.fetch_contacts(migration_service.child_client, "child")
        child_opportunities = await migration_service.fetch_opportunities(migration_service.child_client, "child")
        
        # Fetch master data for comparison
        master_custom_fields = await migration_service.fetch_custom_fields(migration_service.master_client, "master")
        master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client, "master")
        
        await migration_service.close()
        
        # Calculate what would be created/mapped
        child_field_names = {field["name"] for field in child_custom_fields}
        master_field_names = {field["name"] for field in master_custom_fields}
        fields_to_create = child_field_names - master_field_names
        
        child_pipeline_names = {pipeline["name"] for pipeline in child_pipelines}
        master_pipeline_names = {pipeline["name"] for pipeline in master_pipelines}
        pipelines_missing = child_pipeline_names - master_pipeline_names
        
        preview_data = {
            "child_account_data": {
                "custom_fields": len(child_custom_fields),
                "pipelines": len(child_pipelines),
                "contacts": len(child_contacts),
                "opportunities": len(child_opportunities)
            },
            "master_account_data": {
                "custom_fields": len(master_custom_fields),
                "pipelines": len(master_pipelines)
            },
            "migration_plan": {
                "custom_fields_to_create": len(fields_to_create),
                "custom_fields_to_map": len(child_field_names & master_field_names),
                "pipelines_missing_in_master": list(pipelines_missing),
                "contacts_to_migrate": len(child_contacts),
                "opportunities_to_migrate": len(child_opportunities)
            }
        }
        
        # Count completed migrations
        completed_migrations = [m for m in migration_status_store.values() if m.get("status") == "completed"]
        failed_migrations = [m for m in migration_status_store.values() if m.get("status") == "failed"]
        running_migrations = [m for m in migration_status_store.values() if m.get("status") == "running"]
        
        # Calculate totals
        total_contacts_migrated = sum(
            m.get("contacts", {}).get("mapped_count", 0) for m in completed_migrations
        )
        total_opportunities_migrated = sum(
            m.get("opportunities", {}).get("created_count", 0) for m in completed_migrations
        )
        
        return {
            "account_status": {
                "child_account": preview_data["child_account_data"],
                "master_account": preview_data["master_account_data"]
            },
            "migration_statistics": {
                "total_migrations": len(migration_status_store),
                "completed_migrations": len(completed_migrations),
                "failed_migrations": len(failed_migrations),
                "running_migrations": len(running_migrations),
                "total_contacts_migrated": total_contacts_migrated,
                "total_opportunities_migrated": total_opportunities_migrated
            },
            "migration_plan": preview_data["migration_plan"],
            "rate_limiting_info": {
                "current_batch_size": int(os.getenv('MIGRATION_BATCH_SIZE', '20')),
                "rate_limit_delay": float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.2')),
                "recommendations": [
                    "Smaller batches reduce rate limiting",
                    "Increase delay if you encounter frequent 429 errors",
                    "Monitor logs for optimal timing"
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting migration summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get migration summary: {str(e)}")

@router.get("/migration/config")
async def get_migration_config():
    """Get current migration configuration"""
    return {
        "batch_size": int(os.getenv('MIGRATION_BATCH_SIZE', '20')),
        "rate_limit_delay": float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.2')),
        "max_retries": 3,
        "exponential_backoff_enabled": True,
        "concurrent_processing": False,  # Now disabled to avoid rate limits
        "recommendations": {
            "for_small_accounts": {
                "batch_size": 10,
                "rate_limit_delay": 0.1
            },
            "for_large_accounts": {
                "batch_size": 20,
                "rate_limit_delay": 0.3
            },
            "for_rate_limited_accounts": {
                "batch_size": 5,
                "rate_limit_delay": 0.5
            }
        }
    }

@router.post("/migration/config/update")
async def update_migration_config(config: dict):
    """Update migration configuration (note: requires app restart)"""
    try:
        env_file_path = ".env"
        
        # Read current .env file
        env_lines = []
        if os.path.exists(env_file_path):
            with open(env_file_path, 'r') as f:
                env_lines = f.readlines()
        
        # Update or add the configuration values
        updated_lines = []
        batch_size_updated = False
        delay_updated = False
        
        for line in env_lines:
            if line.startswith("MIGRATION_BATCH_SIZE="):
                updated_lines.append(f"MIGRATION_BATCH_SIZE={config.get('batch_size', 20)}\n")
                batch_size_updated = True
            elif line.startswith("MIGRATION_RATE_LIMIT_DELAY="):
                updated_lines.append(f"MIGRATION_RATE_LIMIT_DELAY={config.get('rate_limit_delay', 0.2)}\n")
                delay_updated = True
            else:
                updated_lines.append(line)
        
        # Add missing config if not found
        if not batch_size_updated:
            updated_lines.append(f"MIGRATION_BATCH_SIZE={config.get('batch_size', 20)}\n")
        if not delay_updated:
            updated_lines.append(f"MIGRATION_RATE_LIMIT_DELAY={config.get('rate_limit_delay', 0.2)}\n")
        
        # Write back to .env file
        with open(env_file_path, 'w') as f:
            f.writelines(updated_lines)
        
        return {
            "status": "success",
            "message": "Configuration updated. Note: App restart required for changes to take effect.",
            "updated_config": {
                "batch_size": config.get('batch_size', 20),
                "rate_limit_delay": config.get('rate_limit_delay', 0.2)
            }
        }
        
    except Exception as e:
        logger.error(f"Error updating migration config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update config: {str(e)}")
