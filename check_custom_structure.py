"""
Quick Custom Field Structure Check
==================================

Check the exact structure of custom fields in the API response.
"""

import asyncio
import httpx
import json
from app.config import settings
from webhook_payload_enhancer import get_api_key_for_source

async def check_custom_field_structure():
    """Check the exact structure of custom fields"""
    
    # Get first contact
    contact_id = "pBg2gwuL7tsvPs0qj5Y0"  # Sharon Howman
    source_id = "2"
    
    api_key = get_api_key_for_source(source_id)
    
    client = httpx.AsyncClient(headers={"Authorization": f"Bearer {api_key}"}, timeout=60)
    
    try:
        response = await client.get(f"https://rest.gohighlevel.com/v1/contacts/{contact_id}")
        response.raise_for_status()
        contact = response.json().get("contact", {})
        
        print("🔍 CUSTOM FIELD STRUCTURE CHECK")
        print("=" * 50)
        
        # Check for different possible field names
        field_names = ['customFields', 'customField', 'custom_fields', 'custom_field']
        
        for field_name in field_names:
            if field_name in contact:
                custom_data = contact[field_name]
                print(f"✅ Found: {field_name}")
                print(f"   Type: {type(custom_data)}")
                print(f"   Length: {len(custom_data) if isinstance(custom_data, (list, dict)) else 'N/A'}")
                
                if isinstance(custom_data, list) and len(custom_data) > 0:
                    print(f"   First item: {json.dumps(custom_data[0], indent=2)}")
                elif isinstance(custom_data, dict):
                    print(f"   Content: {json.dumps(custom_data, indent=2)[:200]}...")
                
                print()
            else:
                print(f"❌ Not found: {field_name}")
        
        # Check if dateOfBirth is there (we saw it in the JSON)
        if 'dateOfBirth' in contact:
            print(f"✅ Found dateOfBirth: {contact['dateOfBirth']}")
        
        # Show all available fields
        print(f"\n📋 ALL AVAILABLE FIELDS:")
        for key in sorted(contact.keys()):
            value = contact[key]
            if isinstance(value, (str, int, float)):
                print(f"  {key}: {value}")
            else:
                print(f"  {key}: {type(value)} (length: {len(value) if hasattr(value, '__len__') else 'N/A'})")
        
    finally:
        await client.aclose()

if __name__ == "__main__":
    asyncio.run(check_custom_field_structure())
