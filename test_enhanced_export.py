#!/usr/bin/env python3
"""
Test script for Enhanced GHL Export functionality
This script tests the enhanced export feature that includes contact details and custom fields.
"""

import asyncio
import os
import sys
import json
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.services.ghl_enhanced_export import process_enhanced_export_request, EnhancedGHLClient
from app.models.schemas import ExportRequest, SelectionSchema


async def test_enhanced_client():
    """Test the enhanced GHL client functionality"""
    print("🧪 Testing Enhanced GHL Client...")
    
    # You'll need to replace this with a real API key for testing
    test_api_key = "test_api_key_here"
    
    if test_api_key == "test_api_key_here":
        print("⚠️  Please set a real API key in the test script to run the test")
        return False
    
    client = EnhancedGHLClient(test_api_key)
    
    try:
        # Test pipeline fetching
        pipelines = await client.get_pipelines()
        print(f"✅ Successfully fetched {len(pipelines)} pipelines")
        
        # Test contact details fetching (if we have a test contact ID)
        test_contact_id = "test_contact_id_here"
        if test_contact_id != "test_contact_id_here":
            contact_details = await client.get_contact_details(test_contact_id)
            print(f"✅ Successfully fetched contact details: {len(contact_details.get('custom_fields', {}))} custom fields")
        
        return True
    except Exception as e:
        print(f"❌ Error testing enhanced client: {e}")
        return False


async def test_enhanced_export_structure():
    """Test the enhanced export request structure"""
    print("🧪 Testing Enhanced Export Request Structure...")
    
    try:
        # Create a test export request
        test_request = ExportRequest(
            selections=[
                SelectionSchema(
                    account_id="test_account_1",
                    api_key="test_api_key_1",
                    pipelines=["pipeline_1", "pipeline_2"]
                ),
                SelectionSchema(
                    account_id="test_account_2", 
                    api_key="test_api_key_2",
                    pipelines=["pipeline_3"]
                )
            ]
        )
        
        print(f"✅ Successfully created test export request with {len(test_request.selections)} selections")
        return True
    except Exception as e:
        print(f"❌ Error creating test export request: {e}")
        return False


def test_environment_setup():
    """Test that all required dependencies are available"""
    print("🧪 Testing Environment Setup...")
    
    try:
        import httpx
        import pandas as pd
        import asyncio
        from datetime import datetime
        print("✅ All required packages are available")
        return True
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        return False


def print_enhanced_export_info():
    """Print information about the enhanced export functionality"""
    print("\n" + "="*60)
    print("📊 ENHANCED GHL EXPORT - FEATURE OVERVIEW")
    print("="*60)
    print()
    print("🚀 NEW FEATURES:")
    print("  • Fetches detailed contact information for each opportunity")
    print("  • Includes all custom fields for contacts")
    print("  • Enhanced contact details: first name, last name, company, address")
    print("  • Timezone, DND status, contact type, and source information")
    print("  • Maintains all original opportunity data")
    print()
    print("📈 ENHANCED DATA COLUMNS:")
    print("  Original Data:")
    print("    - Opportunity Name, Contact Name, Phone, Email")
    print("    - Pipeline, Stage, Lead Value, Source, Assigned")
    print("    - Created/Updated dates, Status, IDs")
    print()
    print("  Enhanced Contact Data:")
    print("    - Enhanced Contact Name, Phone, Email")
    print("    - Contact First Name, Last Name, Company")
    print("    - Full Address (Street, City, State, ZIP, Country)")
    print("    - Website, Timezone, DND Status")
    print("    - Contact Type, Source, Date Added/Updated")
    print()
    print("  Custom Fields:")
    print("    - All custom fields prefixed with 'Custom: '")
    print("    - Dynamic columns based on available custom fields")
    print()
    print("🔗 API ENDPOINTS:")
    print("  • GET /enhanced-dashboard - Enhanced export dashboard")
    print("  • GET /api/v1/enhanced/enhanced-subaccounts - Get subaccounts")
    print("  • GET /api/v1/enhanced/enhanced-pipelines/{id} - Get pipelines")
    print("  • POST /api/v1/enhanced/enhanced-export - Process enhanced export")
    print()
    print("⚙️  RATE LIMITING:")
    print("  • 0.3s delay between opportunity requests")
    print("  • 0.1s delay between contact detail requests")
    print("  • Efficient batching by API key")
    print()
    print("📋 USAGE:")
    print("  1. Navigate to /enhanced-dashboard")
    print("  2. Select subaccounts and pipelines")
    print("  3. Click 'Export Selected with Enhanced Details'")
    print("  4. Wait for processing (may take several minutes)")
    print("  5. Download the enhanced Excel file")
    print("="*60)


async def main():
    """Main test function"""
    print("🧪 ENHANCED GHL EXPORT - TEST SUITE")
    print("="*50)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Enhanced Export Structure", test_enhanced_export_structure),
        ("Enhanced Client", test_enhanced_client),
    ]
    
    results = []
    for test_name, test_func in tests:
        if asyncio.iscoroutinefunction(test_func):
            result = await test_func()
        else:
            result = test_func()
        results.append((test_name, result))
    
    print("\n" + "="*50)
    print("📊 TEST RESULTS:")
    print("="*50)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Enhanced export is ready to use.")
    else:
        print("⚠️  Some tests failed. Please check the configuration.")
    
    print_enhanced_export_info()


if __name__ == "__main__":
    asyncio.run(main())
