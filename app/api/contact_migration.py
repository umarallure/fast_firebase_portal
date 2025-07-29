"""
Contact-Only Migration API Endpoints
Simplified endpoints focused on contact migration with custom field mapping
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, status
from typing import Dict, Any
import uuid
import logging
import os
from datetime import datetime
from app.services.ghl_contact_migration import GHLContactMigrationService
from app.config import settings

router = APIRouter(prefix="/api/v1/contact-migration", tags=["Contact Migration"])
logger = logging.getLogger(__name__)

# In-memory storage for contact migration status
contact_migration_status_store: Dict[str, Dict[str, Any]] = {}

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

async def run_contact_migration_background(migration_id: str, child_api_key: str, master_api_key: str):
    """Background task to run contact-only migration with progress tracking"""
    
    def progress_callback(progress_data):
        """Update progress in migration status store"""
        contact_migration_status_store[migration_id]["current_stage"] = progress_data["stage"]
        contact_migration_status_store[migration_id]["progress"] = progress_data
        contact_migration_status_store[migration_id]["last_update"] = datetime.now().isoformat()
    
    try:
        # Update status to running
        contact_migration_status_store[migration_id]["status"] = "running"
        contact_migration_status_store[migration_id]["start_time"] = datetime.now().isoformat()
        contact_migration_status_store[migration_id]["progress"] = {
            "stage": "initializing",
            "current": 0,
            "total": 0,
            "percentage": 0,
            "message": "Starting contact migration..."
        }
        
        # Create contact migration service with progress callback
        migration_service = GHLContactMigrationService(child_api_key, master_api_key, progress_callback)
        results = await migration_service.run_contact_migration()
        
        # Update status with results
        contact_migration_status_store[migration_id].update(results)
        contact_migration_status_store[migration_id]["status"] = "completed"
        contact_migration_status_store[migration_id]["progress"]["percentage"] = 100
        contact_migration_status_store[migration_id]["progress"]["message"] = "Contact migration completed successfully"
        
        logger.info(f"Contact migration {migration_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Contact migration {migration_id} failed: {str(e)}")
        contact_migration_status_store[migration_id]["status"] = "failed"
        contact_migration_status_store[migration_id]["errors"] = [str(e)]
        contact_migration_status_store[migration_id]["end_time"] = datetime.now().isoformat()
        contact_migration_status_store[migration_id]["progress"]["message"] = f"Contact migration failed: {str(e)}"

@router.get("/test-connection")
async def test_connection():
    """Test connection to both child and master GHL accounts"""
    try:
        api_keys = get_api_keys_from_env()
        migration_service = GHLContactMigrationService(api_keys["child_api_key"], api_keys["master_api_key"])
        
        # Test child account
        child_fields = await migration_service.fetch_custom_fields(migration_service.child_client, "child")
        child_status = {
            "status": "connected",
            "location_id": api_keys["child_location_id"],
            "custom_fields_count": len(child_fields)
        }
        
        # Test master account
        master_fields = await migration_service.fetch_custom_fields(migration_service.master_client, "master")
        master_status = {
            "status": "connected", 
            "location_id": api_keys["master_location_id"],
            "custom_fields_count": len(master_fields)
        }
        
        await migration_service.close()
        
        return {
            "child_account": child_status,
            "master_account": master_status
        }
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")

@router.get("/preview")
async def preview_contact_migration():
    """Preview what contacts and custom fields would be migrated"""
    try:
        api_keys = get_api_keys_from_env()
        migration_service = GHLContactMigrationService(api_keys["child_api_key"], api_keys["master_api_key"])
        
        preview_data = await migration_service.preview_contact_migration()
        
        return preview_data
        
    except Exception as e:
        logger.error(f"Preview failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Preview failed: {str(e)}"
        )

@router.post("/start")
async def start_contact_migration(background_tasks: BackgroundTasks):
    """Start contact-only migration using environment settings"""
    try:
        api_keys = get_api_keys_from_env()
        
        # Generate migration ID
        migration_id = str(uuid.uuid4())
        
        # Initialize migration status
        contact_migration_status_store[migration_id] = {
            "id": migration_id,
            "status": "queued",
            "created_time": datetime.now().isoformat(),
            "child_location_id": api_keys["child_location_id"],
            "master_location_id": api_keys["master_location_id"],
            "migration_type": "contact_only"
        }
        
        # Start background migration
        background_tasks.add_task(
            run_contact_migration_background,
            migration_id,
            api_keys["child_api_key"],
            api_keys["master_api_key"]
        )
        
        return {
            "migration_id": migration_id,
            "status": "queued",
            "message": "Contact migration started in background"
        }
        
    except Exception as e:
        logger.error(f"Failed to start contact migration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start contact migration: {str(e)}"
        )

@router.get("/status/{migration_id}")
async def get_contact_migration_status(migration_id: str):
    """Get the status of a contact migration"""
    if migration_id not in contact_migration_status_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Contact migration not found"
        )
    
    return contact_migration_status_store[migration_id]

@router.get("/list")
async def list_contact_migrations():
    """List all contact migrations"""
    migrations = []
    for migration_id, migration_data in contact_migration_status_store.items():
        migrations.append({
            "id": migration_id,
            "status": migration_data.get("status"),
            "created_time": migration_data.get("created_time"),
            "contacts_migrated": migration_data.get("contacts", {}).get("mapped_count", 0),
            "custom_fields_mapped": migration_data.get("custom_fields", {}).get("mapped_count", 0),
            "duration_minutes": migration_data.get("duration_minutes"),
            "migration_type": "contact_only"
        })
    
    return {"migrations": migrations}

@router.get("/config")
async def get_contact_migration_config():
    """Get current contact migration configuration"""
    return {
        "batch_size": int(os.getenv('MIGRATION_BATCH_SIZE', '15')),
        "rate_limit_delay": float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.3')),
        "max_retries": 3,
        "exponential_backoff_enabled": True,
        "migration_type": "contact_only",
        "test_settings": {
            "test_limit": settings.contact_migration_test_limit,
            "test_mode_enabled": settings.contact_migration_test_limit > 0,
            "migration_tag": settings.contact_migration_tag if settings.contact_migration_tag else "None"
        },
        "features": {
            "custom_field_mapping": True,
            "contact_deduplication": True,
            "progress_tracking": True,
            "opportunities_migration": False,
            "contact_tagging": bool(settings.contact_migration_tag)
        },
        "recommendations": {
            "for_small_datasets": {
                "batch_size": 10,
                "rate_limit_delay": 0.2
            },
            "for_large_datasets": {
                "batch_size": 15,
                "rate_limit_delay": 0.4
            },
            "for_rate_limited_accounts": {
                "batch_size": 5,
                "rate_limit_delay": 0.6
            }
        }
    }

@router.get("/summary")
async def get_contact_migration_summary():
    """Get a summary of all contact migrations"""
    try:
        # Get basic connection info
        api_keys = get_api_keys_from_env()
        migration_service = GHLContactMigrationService(api_keys["child_api_key"], api_keys["master_api_key"])
        
        preview_data = await migration_service.preview_contact_migration()
        
        # Count migrations by status
        completed_migrations = [m for m in contact_migration_status_store.values() if m.get("status") == "completed"]
        failed_migrations = [m for m in contact_migration_status_store.values() if m.get("status") == "failed"]
        running_migrations = [m for m in contact_migration_status_store.values() if m.get("status") == "running"]
        
        # Calculate totals
        total_contacts_migrated = sum(
            m.get("contacts", {}).get("mapped_count", 0) for m in completed_migrations
        )
        total_fields_mapped = sum(
            m.get("custom_fields", {}).get("mapped_count", 0) for m in completed_migrations
        )
        
        return {
            "account_status": {
                "child_account": preview_data["child_account_summary"],
                "master_account": preview_data["master_account_summary"]
            },
            "migration_statistics": {
                "total_migrations": len(contact_migration_status_store),
                "completed_migrations": len(completed_migrations),
                "failed_migrations": len(failed_migrations),
                "running_migrations": len(running_migrations),
                "total_contacts_migrated": total_contacts_migrated,
                "total_fields_mapped": total_fields_mapped
            },
            "migration_plan": preview_data["migration_plan"],
            "field_mapping_info": preview_data["field_mapping_preview"],
            "migration_type": "contact_only",
            "rate_limiting_info": {
                "current_batch_size": int(os.getenv('MIGRATION_BATCH_SIZE', '15')),
                "rate_limit_delay": float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.3')),
                "optimized_for": "contact_migration_with_custom_fields"
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting contact migration summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get contact migration summary: {str(e)}")

@router.get("/verify-master-account")
async def verify_master_account():
    """Verify the current state of the master account"""
    try:
        api_keys = get_api_keys_from_env()
        migration_service = GHLContactMigrationService(api_keys["child_api_key"], api_keys["master_api_key"])
        
        verification = await migration_service.verify_master_account_state()
        await migration_service.close()
        
        return {
            "master_account_state": verification,
            "is_empty": verification["total_contacts"] == 0,
            "recommendation": "safe_to_migrate" if verification["total_contacts"] == 0 else "review_existing_contacts"
        }
        
    except Exception as e:
        logger.error(f"Master account verification failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

@router.delete("/clear-history")
async def clear_contact_migration_history():
    """Clear all contact migration history"""
    global contact_migration_status_store
    contact_migration_status_store.clear()
    return {"message": "Contact migration history cleared"}

@router.get("/debug/search-contact")
async def debug_search_contact(email: str = None, phone: str = None):
    """Debug endpoint to test contact search functionality"""
    try:
        api_keys = get_api_keys_from_env()
        migration_service = GHLContactMigrationService(api_keys["child_api_key"], api_keys["master_api_key"])
        
        results = {}
        
        if email:
            # Search by email
            email_result = await migration_service._find_contact_by_email(email)
            results["email_search"] = {
                "query": email,
                "found_contact_id": email_result,
                "found": email_result is not None
            }
            
        if phone:
            # Search by phone
            phone_result = await migration_service._find_contact_by_phone(phone)
            results["phone_search"] = {
                "query": phone,
                "found_contact_id": phone_result,
                "found": phone_result is not None
            }
            
        await migration_service.close()
        return results
        
    except Exception as e:
        logger.error(f"Debug search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Debug search failed: {str(e)}")
