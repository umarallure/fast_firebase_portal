"""
Test script for GHL Migration System
Run this to test the migration system without doing a full migration
"""

import asyncio
import os
import sys
import logging
from pathlib import Path

# Add the app directory to Python path
sys.path.append(str(Path(__file__).parent))

from app.services.ghl_migration import GHLMigrationService
from app.services.smart_mapping import SmartMappingStrategy
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

async def test_api_connections():
    """Test API connections to both accounts"""
    logger.info("Testing API connections...")
    
    child_api_key = os.getenv("GHL_CHILD_LOCATION_API_KEY")
    master_api_key = os.getenv("GHL_MASTER_LOCATION_API_KEY")
    
    if not child_api_key or not master_api_key:
        logger.error("Missing API keys in environment variables")
        return False
    
    migration_service = GHLMigrationService(child_api_key, master_api_key)
    
    try:
        # Test child connection
        child_pipelines = await migration_service.fetch_pipelines(migration_service.child_client, "child")
        logger.info(f"Child account: {len(child_pipelines)} pipelines found")
        
        # Test master connection
        master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client, "master")
        logger.info(f"Master account: {len(master_pipelines)} pipelines found")
        
        if child_pipelines and master_pipelines:
            logger.info("✅ Both API connections successful")
            return True
        else:
            logger.error("❌ One or both API connections failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Connection test failed: {str(e)}")
        return False
    finally:
        await migration_service.close()

async def test_smart_mapping():
    """Test smart mapping functionality"""
    logger.info("Testing smart mapping system...")
    
    child_api_key = os.getenv("GHL_CHILD_LOCATION_API_KEY")
    master_api_key = os.getenv("GHL_MASTER_LOCATION_API_KEY")
    
    migration_service = GHLMigrationService(child_api_key, master_api_key)
    
    try:
        # Fetch data for mapping analysis
        child_fields = await migration_service.fetch_custom_fields(migration_service.child_client, "child")
        master_fields = await migration_service.fetch_custom_fields(migration_service.master_client, "master")
        
        child_pipelines = await migration_service.fetch_pipelines(migration_service.child_client, "child")
        master_pipelines = await migration_service.fetch_pipelines(migration_service.master_client, "master")
        
        # Test smart mapping strategies
        field_strategy = migration_service.smart_mapper.create_custom_field_mapping_strategy(
            child_fields, master_fields
        )
        
        pipeline_strategy = migration_service.smart_mapper.create_pipeline_mapping_strategy(
            child_pipelines, master_pipelines
        )
        
        # Generate mapping report
        mapping_report = migration_service.smart_mapper.generate_migration_report(
            pipeline_strategy, field_strategy
        )
        
        logger.info("Smart Mapping Results:")
        logger.info(f"  Fields - Auto-mapped: {len(field_strategy['field_mappings'])}, To create: {len(field_strategy['unmapped_fields'])}")
        logger.info(f"  Pipelines - Auto-mapped: {len(pipeline_strategy['pipeline_mappings'])}, Missing: {len(pipeline_strategy['unmapped_pipelines'])}")
        logger.info(f"  Migration readiness: {mapping_report['recommendations']['migration_readiness']['overall_readiness_percent']:.1f}%")
        
        if mapping_report['recommendations']['migration_readiness']['can_proceed']:
            logger.info("✅ Migration readiness check passed")
        else:
            logger.warning("⚠️ Migration readiness check failed")
            for warning in mapping_report['recommendations']['migration_readiness']['warnings']:
                logger.warning(f"  - {warning}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Smart mapping test failed: {str(e)}")
        return False
    finally:
        await migration_service.close()

async def test_data_preview():
    """Test data preview functionality"""
    logger.info("Testing data preview...")
    
    child_api_key = os.getenv("GHL_CHILD_LOCATION_API_KEY")
    master_api_key = os.getenv("GHL_MASTER_LOCATION_API_KEY")
    
    migration_service = GHLMigrationService(child_api_key, master_api_key)
    
    try:
        # Fetch sample data
        child_contacts = await migration_service.fetch_contacts(migration_service.child_client, "child")
        child_opportunities = await migration_service.fetch_opportunities(migration_service.child_client, "child")
        
        logger.info(f"Data preview:")
        logger.info(f"  Contacts to migrate: {len(child_contacts)}")
        logger.info(f"  Opportunities to migrate: {len(child_opportunities)}")
        
        # Show sample contact data structure
        if child_contacts:
            sample_contact = child_contacts[0]
            logger.info(f"  Sample contact fields: {list(sample_contact.keys())}")
            if sample_contact.get('customFields'):
                logger.info(f"  Sample contact has {len(sample_contact['customFields'])} custom fields")
        
        # Show sample opportunity data structure
        if child_opportunities:
            sample_opp = child_opportunities[0]
            logger.info(f"  Sample opportunity fields: {list(sample_opp.keys())}")
            if sample_opp.get('customFields'):
                logger.info(f"  Sample opportunity has {len(sample_opp['customFields'])} custom fields")
        
        logger.info("✅ Data preview completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data preview failed: {str(e)}")
        return False
    finally:
        await migration_service.close()

async def main():
    """Run all tests"""
    logger.info("🚀 Starting GHL Migration System Tests")
    
    # Check environment variables
    required_vars = [
        "GHL_CHILD_LOCATION_API_KEY",
        "GHL_MASTER_LOCATION_API_KEY",
        "GHL_CHILD_LOCATION_ID",
        "GHL_MASTER_LOCATION_ID"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing environment variables: {missing_vars}")
        return
    
    # Run tests
    tests = [
        ("API Connections", test_api_connections),
        ("Smart Mapping", test_smart_mapping),
        ("Data Preview", test_data_preview)
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name} test...")
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"❌ {test_name} test failed with exception: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    logger.info(f"\n📊 Test Results Summary:")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"  {test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Migration system is ready to use.")
    else:
        logger.warning("⚠️ Some tests failed. Please check the logs and fix issues before running migrations.")

if __name__ == "__main__":
    asyncio.run(main())
