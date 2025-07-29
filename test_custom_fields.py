#!/usr/bin/env python3
"""
Simple test script to verify custom field extraction from GHL Contact API
"""

import asyncio
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.services.ghl_enhanced_export import EnhancedGHLClient
from app.config import settings

async def test_custom_field_extraction():
    """Test custom field extraction with the first available subaccount"""
    
    subaccounts = settings.subaccounts_list
    if not subaccounts:
        print("❌ No subaccounts found in configuration")
        return
    
    test_account = subaccounts[0]
    api_key = test_account["api_key"]
    account_name = test_account["name"]
    
    print(f"🧪 Testing Custom Field Extraction")
    print(f"📍 Account: {account_name}")
    print(f"🔑 API Key: {api_key[:20]}...")
    print("=" * 60)
    
    client = EnhancedGHLClient(api_key)
    
    try:
        # Step 1: Test custom field definitions fetch
        print("\n🔍 Step 1: Fetching custom field definitions...")
        custom_field_defs = await client.get_custom_fields()
        print(f"✅ Found {len(custom_field_defs)} custom field definitions")
        
        if custom_field_defs:
            print("📋 Custom Field Definitions:")
            for field_id, field_name in list(custom_field_defs.items())[:5]:  # Show first 5
                print(f"   • {field_id}: {field_name}")
            if len(custom_field_defs) > 5:
                print(f"   ... and {len(custom_field_defs) - 5} more")
        
        # Step 2: Get some opportunities to find contacts
        print("\n🔍 Step 2: Finding opportunities with contacts...")
        pipelines = await client.get_pipelines()
        
        if not pipelines:
            print("❌ No pipelines found")
            return
        
        pipeline = pipelines[0]
        opportunities = await client.get_opportunities(
            pipeline["id"], 
            pipeline["name"], 
            {stage["id"]: stage["name"] for stage in pipeline.get("stages", [])},
            test_account["id"]
        )
        
        if not opportunities:
            print("❌ No opportunities found")
            return
        
        print(f"✅ Found {len(opportunities)} opportunities")
        
        # Step 3: Test contact details extraction for first few contacts
        print("\n🔍 Step 3: Testing contact details extraction...")
        
        tested_contacts = 0
        for i, opp in enumerate(opportunities[:3]):  # Test first 3 contacts
            contact_id = opp.get("Contact ID")
            if not contact_id:
                continue
            
            tested_contacts += 1
            opp_name = opp.get("Opportunity Name", "Unknown")
            print(f"\n📧 Contact {tested_contacts}: {opp_name}")
            print(f"   ID: {contact_id}")
            
            # Fetch enhanced contact details
            contact_details = await client.get_contact_details(contact_id)
            contact_data = contact_details.get("contact_data", {})
            custom_fields = contact_details.get("custom_fields", {})
            raw_response = contact_details.get("raw_response", {})
            
            # Display basic contact info
            print(f"   Name: {contact_data.get('name', 'N/A')}")
            print(f"   Email: {contact_data.get('email', 'N/A')}")
            print(f"   Phone: {contact_data.get('phone', 'N/A')}")
            
            # Show raw response structure for debugging
            print(f"   📊 Raw Response Keys: {list(raw_response.keys())}")
            if 'contact' in raw_response:
                contact_obj = raw_response['contact']
                print(f"   📊 Contact Object Keys: {list(contact_obj.keys())}")
                
                # Check for customField array specifically
                if 'customField' in contact_obj:
                    cf_array = contact_obj['customField']
                    print(f"   🔍 customField Array: {len(cf_array)} items")
                    for idx, cf in enumerate(cf_array[:3]):  # Show first 3
                        print(f"      [{idx}] ID: {cf.get('id')}, Value: {cf.get('value')}")
                else:
                    print(f"   ❌ No 'customField' array found in contact object")
            
            # Display custom fields
            if custom_fields:
                print(f"   ✅ Custom Fields Found: {len(custom_fields)}")
                for field_name, field_value in custom_fields.items():
                    if field_value:  # Only show fields with values
                        print(f"      • {field_name}: {field_value}")
            else:
                print("   ❌ No custom fields found")
            
            # Check for the specific fields mentioned by the user
            specific_fields = ['Date of Submission', 'Birth State', 'Age', 'Social Security Number', 
                             'Height', 'Weight', 'Doctors Name', 'Tobacco User?', 'Health Conditions', 'Medications']
            
            found_specific = []
            for field_name in specific_fields:
                if field_name in custom_fields or field_name.lower().replace(' ', '_').replace('?', '') in custom_fields:
                    found_specific.append(field_name)
            
            if found_specific:
                print(f"   🎯 Found specific fields: {', '.join(found_specific)}")
            
            await asyncio.sleep(0.5)  # Rate limiting
        
        if tested_contacts == 0:
            print("❌ No contacts with IDs found in opportunities")
        else:
            print(f"\n✅ Successfully tested {tested_contacts} contacts")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_custom_field_extraction())
