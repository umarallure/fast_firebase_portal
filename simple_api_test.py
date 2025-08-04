"""
Simple GHL API Test for Daily Deal Flow CSV
==========================================
This script tests if the problematic stages exist in your current GHL subaccount.
"""

import asyncio
import os
import sys

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.ghl_opportunity_updater import GHLOpportunityUpdater

async def test_ghl_api_simple():
    """Simple test to check if API is working and stages exist"""
    
    print("🔧 Simple GHL API Test")
    print("=" * 40)
    
    # Try to get API key from environment variables
    api_key = os.environ.get('GHL_API_KEY') or os.environ.get('ghl_child_location_api_key') or os.environ.get('ghl_master_location_api_key')
    
    if not api_key:
        print("❌ No API key found!")
        print("Please set one of these environment variables:")
        print("   - GHL_API_KEY")
        print("   - ghl_child_location_api_key") 
        print("   - ghl_master_location_api_key")
        print("\nExample: set GHL_API_KEY=your_api_key_here")
        return
    
    print(f"✓ Found API key: {api_key[:20]}...")
    
    try:
        # Initialize the updater
        updater = GHLOpportunityUpdater(api_key)
        
        # Test API connection by getting pipelines
        print("\n🔍 Testing API connection...")
        pipelines = await updater.get_pipelines()
        
        print(f"✓ API connection successful!")
        print(f"✓ Found {len(pipelines)} pipelines")
        
        # Test the problematic stages from our CSV
        problematic_stages = [
            'Needs to be Fixed',
            'Needs Carrier Application', 
            'Incomplete transfer',
            'Returned to Center'
        ]
        
        print(f"\n🎯 Testing problematic stages...")
        
        for pipeline in pipelines:
            pipeline_id = pipeline.get('id')
            pipeline_name = pipeline.get('name', 'Unknown')
            
            print(f"\n📋 Pipeline: {pipeline_name}")
            print(f"   ID: {pipeline_id}")
            
            # Get all stages for this pipeline
            stages = await updater.get_pipeline_stages(pipeline_id)
            stage_names = [stage.get('name', '') for stage in stages]
            
            print(f"   Available stages ({len(stage_names)}):")
            for stage_name in stage_names:
                print(f"     - {stage_name}")
            
            # Test each problematic stage
            print(f"\n   Testing problematic stages:")
            for stage_name in problematic_stages:
                stage_id = await updater.get_stage_id_from_name(stage_name, pipeline_id)
                if stage_id:
                    print(f"     ✓ '{stage_name}' -> {stage_id}")
                else:
                    print(f"     ✗ '{stage_name}' -> NOT FOUND")
        
        print(f"\n🎉 API test completed successfully!")
        
    except Exception as e:
        print(f"❌ API test failed: {e}")
        print("Please check:")
        print("   1. API key is valid")
        print("   2. API key has proper permissions")
        print("   3. Network connection is working")

if __name__ == "__main__":
    asyncio.run(test_ghl_api_simple())
