#!/usr/bin/env python3
"""
Test script to find contacts with specific custom fields
"""
import asyncio
import httpx
import json
from app.config import settings

# Add your actual API key and account ID here
TEST_API_KEY = "your_api_key_here"  # Replace with actual API key
TEST_ACCOUNT_ID = "your_account_id_here"  # Replace with actual account ID

async def test_contact_search():
    """Search for contacts that might have custom fields populated"""
    
    # Fields we're looking for
    target_fields = [
        'date_of_submission', 'birth_state', 'age', 'social_security_number',
        'height', 'weight', 'doctors_name', 'tobacco_user', 'health_conditions', 'medications'
    ]
    
    base_url = "https://rest.gohighlevel.com/v1"
    headers = {"Authorization": f"Bearer {TEST_API_KEY}"}
    
    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        try:
            # First get custom field definitions
            print("🔍 Fetching custom field definitions...")
            response = await client.get(f"{base_url}/custom-fields/")
            response.raise_for_status()
            custom_fields_data = response.json()
            custom_fields = custom_fields_data.get("customFields", [])
            
            print(f"📋 Found {len(custom_fields)} custom field definitions:")
            field_mapping = {}
            for field in custom_fields:
                field_id = field.get("id")
                field_name = field.get("name", "")
                field_type = field.get("dataType", "")
                print(f"   • {field_name} ({field_type}) - ID: {field_id}")
                field_mapping[field_id] = field_name
            
            # Look for our target fields in the definitions
            print("\n🎯 Looking for target custom fields:")
            target_field_ids = []
            for field in custom_fields:
                field_name = field.get("name", "").lower()
                field_id = field.get("id")
                for target in target_fields:
                    target_clean = target.replace('_', ' ').lower()
                    if target_clean in field_name or field_name in target_clean:
                        print(f"   ✅ Found match: '{field.get('name')}' (ID: {field_id}) matches '{target}'")
                        target_field_ids.append(field_id)
                        break
            
            # Get a sample of contacts
            print(f"\n📞 Fetching sample contacts...")
            response = await client.get(f"{base_url}/contacts", params={"limit": 50})
            response.raise_for_status()
            contacts_data = response.json()
            contacts = contacts_data.get("contacts", [])
            
            print(f"📞 Found {len(contacts)} contacts to check")
            
            # Check each contact for custom fields
            contacts_with_custom_fields = []
            for i, contact in enumerate(contacts[:10]):  # Check first 10 contacts
                contact_id = contact.get("id")
                contact_name = contact.get("name", "Unknown")
                
                print(f"\n🔍 Checking contact {i+1}: {contact_name} (ID: {contact_id})")
                
                # Get detailed contact info
                detail_response = await client.get(f"{base_url}/contacts/{contact_id}")
                detail_response.raise_for_status()
                contact_detail = detail_response.json().get("contact", {})
                
                # Check for customField array
                custom_field_array = contact_detail.get("customField", [])
                print(f"   📊 Found {len(custom_field_array)} custom fields in customField array")
                
                found_target_fields = []
                for cf in custom_field_array:
                    cf_id = cf.get("id")
                    cf_value = cf.get("value", "")
                    cf_name = field_mapping.get(cf_id, f"Unknown Field {cf_id}")
                    
                    print(f"      • {cf_name} (ID: {cf_id}) = '{cf_value}'")
                    
                    # Check if this is one of our target fields
                    if cf_id in target_field_ids:
                        found_target_fields.append({
                            "id": cf_id,
                            "name": cf_name,
                            "value": cf_value
                        })
                
                if found_target_fields:
                    contacts_with_custom_fields.append({
                        "contact_id": contact_id,
                        "contact_name": contact_name,
                        "target_fields": found_target_fields
                    })
                    print(f"   ✅ Found {len(found_target_fields)} target custom fields!")
                
                # Also check for direct fields in contact data
                direct_fields_found = []
                for target in target_fields:
                    if target in contact_detail:
                        direct_fields_found.append(f"{target} = {contact_detail[target]}")
                        print(f"   📍 Found direct field: {target} = {contact_detail[target]}")
                
                await asyncio.sleep(0.1)  # Rate limiting
            
            print(f"\n📈 Summary:")
            print(f"   • Total custom field definitions: {len(custom_fields)}")
            print(f"   • Target field IDs found: {len(target_field_ids)}")
            print(f"   • Contacts with target custom fields: {len(contacts_with_custom_fields)}")
            
            if contacts_with_custom_fields:
                print("\n🎯 Contacts with target custom fields:")
                for contact in contacts_with_custom_fields:
                    print(f"   📞 {contact['contact_name']} (ID: {contact['contact_id']})")
                    for field in contact['target_fields']:
                        print(f"      • {field['name']}: {field['value']}")
            else:
                print("\n⚠️  No contacts found with the target custom fields populated")
                print("   This could mean:")
                print("   • The custom fields exist but are not populated for these contacts")
                print("   • The custom fields are in a different account/location")
                print("   • The field names don't exactly match what we're looking for")
        
        except Exception as e:
            print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    print("🚀 Testing Custom Field Detection")
    print("=" * 50)
    
    if TEST_API_KEY == "your_api_key_here":
        print("❌ Please update TEST_API_KEY in the script with your actual API key")
    else:
        asyncio.run(test_contact_search())
