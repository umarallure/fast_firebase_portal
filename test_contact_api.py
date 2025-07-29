#!/usr/bin/env python3
"""
Test script to directly test GHL Contact API and see the actual response structure
"""

import asyncio
import httpx
import json
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.config import settings

async def test_contact_api():
    """Test the GHL Contact API directly to see the response structure"""
    
    # Use the first subaccount for testing
    subaccounts = settings.subaccounts_list
    if not subaccounts:
        print("❌ No subaccounts found in configuration")
        return
    
    test_account = subaccounts[0]
    api_key = test_account["api_key"]
    account_name = test_account["name"]
    
    print(f"🧪 Testing Contact API with account: {account_name}")
    print(f"📍 API Key: {api_key[:20]}...")
    
    base_url = "https://rest.gohighlevel.com/v1"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        try:
            # First, get some opportunities to find contact IDs
            print("🔍 Fetching pipelines to find opportunities...")
            pipelines_response = await client.get(f"{base_url}/pipelines")
            pipelines_response.raise_for_status()
            pipelines = pipelines_response.json().get("pipelines", [])
            
            if not pipelines:
                print("❌ No pipelines found")
                return
            
            pipeline = pipelines[0]
            pipeline_id = pipeline["id"]
            pipeline_name = pipeline["name"]
            
            print(f"📋 Using pipeline: {pipeline_name} ({pipeline_id})")
            
            # Get opportunities from this pipeline
            print("🔍 Fetching opportunities...")
            opportunities_response = await client.get(
                f"{base_url}/pipelines/{pipeline_id}/opportunities",
                params={"limit": 5}
            )
            opportunities_response.raise_for_status()
            opportunities = opportunities_response.json().get("opportunities", [])
            
            if not opportunities:
                print("❌ No opportunities found in this pipeline")
                return
            
            print(f"✅ Found {len(opportunities)} opportunities")
            
            # Test contact details for the first few opportunities
            for i, opp in enumerate(opportunities[:3]):
                contact_id = opp.get("contact", {}).get("id")
                if not contact_id:
                    print(f"⚠️  Opportunity {i+1}: No contact ID found")
                    continue
                
                opp_name = opp.get("name", "Unknown")
                print(f"\n🎯 Testing Contact {i+1}: {opp_name}")
                print(f"📧 Contact ID: {contact_id}")
                
                try:
                    # Fetch contact details
                    contact_response = await client.get(f"{base_url}/contacts/{contact_id}")
                    contact_response.raise_for_status()
                    
                    contact_data = contact_response.json()
                    
                    print(f"📊 Contact API Response Structure:")
                    print(f"   • Root keys: {list(contact_data.keys())}")
                    
                    if "contact" in contact_data:
                        contact_obj = contact_data["contact"]
                        print(f"   • Contact object keys: {list(contact_obj.keys())}")
                        
                        # Check for custom fields
                        if "customFields" in contact_obj:
                            custom_fields = contact_obj["customFields"]
                            print(f"   • Found customFields array with {len(custom_fields)} items")
                            
                            for idx, field in enumerate(custom_fields):
                                field_name = field.get("name", "No name")
                                field_value = field.get("value", "No value")
                                field_id = field.get("id", "No ID")
                                print(f"     [{idx+1}] {field_name}: {field_value} (ID: {field_id})")
                        else:
                            print("   • No 'customFields' array found")
                        
                        # Check for the specific fields you mentioned
                        custom_field_names = [
                            'date_of_submission', 'birth_state', 'age', 'social_security_number',
                            'height', 'weight', 'doctors_name', 'tobacco_user', 'health_conditions', 'medications'
                        ]
                        
                        print(f"   • Checking for specific custom fields:")
                        found_custom_fields = []
                        for field_name in custom_field_names:
                            if field_name in contact_obj:
                                found_custom_fields.append(f"{field_name}: {contact_obj[field_name]}")
                                
                        if found_custom_fields:
                            print("     ✅ Found direct custom fields:")
                            for field in found_custom_fields:
                                print(f"       • {field}")
                        else:
                            print("     ❌ No direct custom fields found")
                        
                        # Show all non-standard fields
                        standard_fields = {
                            'id', 'name', 'firstName', 'lastName', 'email', 'phone', 'address1', 'address2',
                            'city', 'state', 'postalCode', 'country', 'companyName', 'website', 'timezone',
                            'dnd', 'type', 'source', 'dateAdded', 'dateUpdated', 'tags', 'customFields',
                            'locationId', 'contactType', 'assignedTo', 'lastActivity'
                        }
                        
                        non_standard_fields = {k: v for k, v in contact_obj.items() if k not in standard_fields}
                        if non_standard_fields:
                            print(f"     🔍 Non-standard fields found:")
                            for key, value in non_standard_fields.items():
                                print(f"       • {key}: {value}")
                    
                    # Save the raw response for detailed inspection
                    with open(f"contact_{i+1}_response.json", "w") as f:
                        json.dump(contact_data, f, indent=2)
                    print(f"💾 Saved raw response to: contact_{i+1}_response.json")
                    
                except Exception as e:
                    print(f"❌ Error fetching contact {contact_id}: {e}")
                
                await asyncio.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 GHL CONTACT API STRUCTURE TEST")
    print("=" * 50)
    asyncio.run(test_contact_api())
