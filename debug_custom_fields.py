"""
Debug Custom Fields Mapping
===========================

This script will help us understand why custom fields aren't being populated properly.
It will show:
1. What custom fields are available in GoHighLevel
2. What custom field values exist for our test contacts
3. How the mapping logic is working
"""

import asyncio
import httpx
import pandas as pd
import json
from app.config import settings
from webhook_payload_enhancer import get_api_key_for_source, WebhookPayloadEnhancer

async def debug_custom_fields():
    """Debug custom fields for the first contact"""
    
    # Read CSV and get first contact
    df = pd.read_csv("DATABASEGHL.csv")
    first_contact = df.iloc[0]
    
    contact_id = first_contact['Contact ID']
    source_id = str(first_contact['Account Id'])
    
    print("🔍 CUSTOM FIELDS DEBUG")
    print("=" * 60)
    print(f"Contact: {first_contact['full_name']}")
    print(f"Contact ID: {contact_id}")
    print(f"Source: {source_id}")
    
    # Get API key
    api_key = get_api_key_for_source(source_id)
    if not api_key:
        print("❌ No API key found")
        return
    
    # Create enhancer
    enhancer = WebhookPayloadEnhancer(api_key)
    
    try:
        # 1. Check custom field definitions
        print(f"\n📋 STEP 1: CUSTOM FIELD DEFINITIONS")
        print("-" * 40)
        
        custom_field_definitions = await enhancer.get_custom_fields()
        print(f"Total custom fields available: {len(custom_field_definitions)}")
        
        print(f"\nCustom field definitions:")
        for field_id, field_info in custom_field_definitions.items():
            print(f"  ID: {field_id}")
            print(f"  Name: {field_info['name']}")
            print(f"  Key: {field_info['key']}")
            print(f"  Type: {field_info['type']}")
            print()
        
        # 2. Check contact details with custom fields
        print(f"\n📋 STEP 2: CONTACT DETAILS")
        print("-" * 40)
        
        response = await enhancer.client.get(f"{enhancer.base_url}/contacts/{contact_id}")
        response.raise_for_status()
        raw_response = response.json()
        
        contact = raw_response.get("contact", {})
        
        print(f"Contact basic info:")
        print(f"  Name: {contact.get('firstName', '')} {contact.get('lastName', '')}")
        print(f"  Email: {contact.get('email', 'N/A')}")
        print(f"  Phone: {contact.get('phone', 'N/A')}")
        print(f"  Address: {contact.get('address1', 'N/A')}")
        print(f"  City: {contact.get('city', 'N/A')}")
        print(f"  State: {contact.get('state', 'N/A')}")
        
        # 3. Check custom field values
        print(f"\n📋 STEP 3: CUSTOM FIELD VALUES")
        print("-" * 40)
        
        custom_values = contact.get('customFields', [])
        print(f"Custom field values found: {len(custom_values)}")
        
        if custom_values:
            for custom_field in custom_values:
                field_id = custom_field.get('id')
                field_value = custom_field.get('value', '')
                
                if field_id in custom_field_definitions:
                    field_info = custom_field_definitions[field_id]
                    field_name = field_info['name']
                    field_key = field_info['key']
                    
                    print(f"\n  Custom Field:")
                    print(f"    ID: {field_id}")
                    print(f"    Name: {field_name}")
                    print(f"    Key: {field_key}")
                    print(f"    Value: {field_value}")
                    
                    # Test mapping
                    mapped_field = enhancer.map_custom_field_to_webhook(field_name, field_key)
                    print(f"    Maps to: {mapped_field}")
                else:
                    print(f"\n  Unknown Custom Field:")
                    print(f"    ID: {field_id}")
                    print(f"    Value: {field_value}")
        else:
            print("❌ No custom field values found for this contact")
        
        # 4. Show full contact JSON for reference
        print(f"\n📋 STEP 4: FULL CONTACT DATA")
        print("-" * 40)
        print("Full contact JSON (first 1000 chars):")
        print(json.dumps(contact, indent=2)[:1000] + "...")
        
        # 5. Test the actual enhancement process
        print(f"\n📋 STEP 5: ENHANCEMENT PROCESS TEST")
        print("-" * 40)
        
        webhook_data = await enhancer.get_contact_details(contact_id)
        if webhook_data:
            print("✅ Enhancement successful")
            
            # Show populated fields
            populated_fields = {k: v for k, v in webhook_data.items() if v and str(v).strip()}
            print(f"\nPopulated fields ({len(populated_fields)}):")
            for field, value in populated_fields.items():
                if len(str(value)) > 50:
                    value = str(value)[:50] + "..."
                print(f"  {field}: {value}")
            
            # Show empty fields that should have data
            key_empty_fields = []
            for field in ['date_of_birth', 'age', 'social_security_number', 'monthly_premium', 'coverage_amount']:
                if not webhook_data.get(field):
                    key_empty_fields.append(field)
            
            if key_empty_fields:
                print(f"\n❌ Key empty fields: {', '.join(key_empty_fields)}")
            
        else:
            print("❌ Enhancement failed")
            
    finally:
        await enhancer.close()

async def debug_multiple_contacts():
    """Debug custom fields for multiple contacts to see patterns"""
    
    print(f"\n🔍 MULTIPLE CONTACTS CUSTOM FIELDS DEBUG")
    print("=" * 60)
    
    df = pd.read_csv("DATABASEGHL.csv")
    test_contacts = df.head(3)  # Test first 3 contacts
    
    for index, row in test_contacts.iterrows():
        contact_id = row['Contact ID']
        source_id = str(row['Account Id'])
        name = row['full_name']
        
        print(f"\n📋 Contact {index + 1}: {name}")
        print("-" * 40)
        
        api_key = get_api_key_for_source(source_id)
        if not api_key:
            print("❌ No API key")
            continue
            
        enhancer = WebhookPayloadEnhancer(api_key)
        
        try:
            response = await enhancer.client.get(f"{enhancer.base_url}/contacts/{contact_id}")
            response.raise_for_status()
            contact = response.json().get("contact", {})
            
            custom_values = contact.get('customFields', [])
            print(f"Custom fields: {len(custom_values)}")
            
            if custom_values:
                for cf in custom_values[:3]:  # Show first 3 custom fields
                    print(f"  - {cf.get('id', 'N/A')}: {cf.get('value', 'N/A')}")
            else:
                print("  No custom fields found")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
        finally:
            await enhancer.close()

def main():
    """Run custom fields debugging"""
    try:
        print("Starting custom fields debug...")
        asyncio.run(debug_custom_fields())
        asyncio.run(debug_multiple_contacts())
    except Exception as e:
        print(f"Debug failed: {str(e)}")

if __name__ == "__main__":
    main()
