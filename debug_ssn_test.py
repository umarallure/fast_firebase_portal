#!/usr/bin/env python3
"""
Debug script to test the SSN export service directly without the web server
"""

import asyncio
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.custom_fields_ssn_export_service import CustomFieldsSSNExportService
from app.config import settings

async def test_opportunities_with_ssn():
    """Test the get_all_opportunities_with_ssn method directly"""
    
    print("*** DIRECT TEST OF get_all_opportunities_with_ssn METHOD ***")
    
    # Find account 46 specifically
    test_account = None
    for account in settings.subaccounts_list:
        if str(account.get('id')) == '6':
            test_account = account
            break
    
    if not test_account:
        print("ERROR: Account 46 not found")
        return
    
    print(f"Testing with account: {test_account.get('id')} - {test_account.get('name')}")
    print(f"Has API key: {bool(test_account.get('api_key'))}")
    print(f"Has access token: {bool(test_account.get('access_token'))}")
    
    # Initialize service
    service = CustomFieldsSSNExportService()
    
    try:
        print("\n*** CALLING get_all_opportunities_with_ssn ***")
        
        # First test the pipeline mapping
        print("Testing pipeline mapping...")
        pipeline_result = await service.get_pipeline_stage_mapping(test_account)
        print(f"Pipeline mapping success: {pipeline_result.get('success')}")
        if pipeline_result.get('success'):
            pipeline_map = pipeline_result.get('pipeline_map', {})
            print(f"Found {len(pipeline_map)} pipelines: {list(pipeline_map.values())}")
        else:
            print(f"Pipeline mapping failed: {pipeline_result.get('message')}")
        
        # Test custom field definitions
        print("\nTesting custom field definitions...")
        field_result = await service.get_custom_field_definitions(test_account)
        print(f"Field definitions success: {field_result.get('success')}")
        if field_result.get('success'):
            ssn_field = field_result.get('ssn_field')
            if ssn_field:
                print(f"Found SSN field: {ssn_field.get('name')} (ID: {ssn_field.get('id')})")
            else:
                print("No SSN field found")
        else:
            print(f"Field definitions failed: {field_result.get('message')}")
        
        result = await service.get_all_opportunities_with_ssn(test_account, limit=2)
        
        print(f"Result type: {type(result)}")
        print(f"Number of records returned: {len(result)}")
        
        if result:
            print("\n*** FIRST RECORD ***")
            first_record = result[0]
            print(f"Record type: {first_record.get('record_type')}")
            print(f"Keys in record: {list(first_record.keys())}")
            print(f"Full record: {first_record}")
        else:
            print("No records returned!")
            
        # Also test the main export method
        print("\n*** TESTING MAIN EXPORT METHOD ***")
        export_result = await service.export_ssn_data([str(test_account.get('id'))], include_contacts=True)
        
        print(f"Export success: {export_result.get('success')}")
        if export_result.get('success'):
            data = export_result.get('data', [])
            print(f"Export returned {len(data)} records")
            if data:
                first_export_record = data[0]
                print(f"Export record type: {first_export_record.get('record_type')}")
                print(f"Export first record: {first_export_record}")
        else:
            print(f"Export failed: {export_result.get('message')}")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_opportunities_with_ssn())