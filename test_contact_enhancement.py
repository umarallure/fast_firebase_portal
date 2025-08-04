"""
Contact Data Enhancement Script
==============================

This script reads the database.csv file, takes the first 5 contacts,
and fetches additional data from GoHighLevel API to create an enhanced CSV
with all fields required for the webhook.

Required Environment Variables:
- Set up subaccounts in your environment with API keys

Usage:
    python test_contact_enhancement.py
"""

import pandas as pd
import asyncio
import httpx
import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from app.config import settings

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContactDataEnhancer:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=30)
        self._custom_field_cache = {}
        
    async def get_custom_fields(self) -> Dict[str, str]:
        """Fetch all custom field definitions and return a mapping of ID to name"""
        if self._custom_field_cache:
            return self._custom_field_cache
            
        try:
            response = await self.client.get(f"{self.base_url}/custom-fields/")
            response.raise_for_status()
            data = response.json()
            custom_fields = data.get("customFields", [])
            
            # Create mapping of ID to field name
            field_mapping = {}
            for field in custom_fields:
                field_id = field.get("id")
                field_name = field.get("name", f"Custom Field {field_id}")
                if field_id:
                    field_mapping[field_id] = field_name
            
            self._custom_field_cache = field_mapping
            logger.info(f"Cached {len(field_mapping)} custom field definitions")
            return field_mapping
            
        except httpx.HTTPError as e:
            logger.error(f"Custom fields fetch failed: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching custom fields: {str(e)}")
            return {}
    
    async def get_contact_details(self, contact_id: str) -> Dict[str, Any]:
        """Fetch detailed contact information including custom fields"""
        try:
            # Ensure we have custom field definitions
            custom_field_definitions = await self.get_custom_fields()
            
            # Fetch contact details
            response = await self.client.get(f"{self.base_url}/contacts/{contact_id}")
            response.raise_for_status()
            raw_response = response.json()
            
            contact_data = raw_response.get("contact", {})
            
            # Process custom fields
            enhanced_data = {
                # Basic contact info
                "id": contact_data.get("id", ""),
                "locationId": contact_data.get("locationId", ""),
                "email": contact_data.get("email", ""),
                "emailLowerCase": contact_data.get("emailLowerCase", ""),
                "fingerprint": contact_data.get("fingerprint", ""),
                "timezone": contact_data.get("timezone", ""),
                "country": contact_data.get("country", ""),
                "source": contact_data.get("source", ""),
                "dateAdded": contact_data.get("dateAdded", ""),
                "tags": contact_data.get("tags", []),
                
                # Additional fields that might be present
                "firstName": contact_data.get("firstName", ""),
                "lastName": contact_data.get("lastName", ""),
                "name": contact_data.get("name", ""),
                "phone": contact_data.get("phone", ""),
                "address1": contact_data.get("address1", ""),
                "city": contact_data.get("city", ""),
                "state": contact_data.get("state", ""),
                "postalCode": contact_data.get("postalCode", ""),
                "website": contact_data.get("website", ""),
                "companyName": contact_data.get("companyName", ""),
                "dnd": contact_data.get("dnd", False),
                "dndSettings": contact_data.get("dndSettings", {}),
                "type": contact_data.get("type", ""),
                "assignedTo": contact_data.get("assignedTo", ""),
                "businessId": contact_data.get("businessId", ""),
                "attributions": contact_data.get("attributions", []),
            }
            
            # Process custom fields with human-readable names
            custom_fields = contact_data.get("customField", [])
            for cf in custom_fields:
                field_id = cf.get("id", "")
                field_value = cf.get("value", "")
                
                # Use human-readable name if available, otherwise use ID
                field_name = custom_field_definitions.get(field_id, f"custom_{field_id}")
                enhanced_data[f"custom_field_{field_name}"] = field_value
            
            return enhanced_data
            
        except httpx.HTTPError as e:
            logger.error(f"Contact fetch failed for {contact_id}: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching contact {contact_id}: {str(e)}")
            return {}
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

def get_api_key_for_source(source_id: str) -> Optional[str]:
    """Get API key for a specific source/subaccount ID from settings"""
    subaccounts = settings.subaccounts_list
    
    # Find subaccount by source ID
    for sub in subaccounts:
        if str(sub.get("id")) == str(source_id):
            api_key = sub.get("api_key")
            subaccount_name = sub.get("name", f"Subaccount {source_id}")
            logger.info(f"Using API key for source {source_id} ({subaccount_name})")
            return api_key
    
    logger.warning(f"No API key found for source ID: {source_id}")
    
    # Fallback to first available subaccount
    if subaccounts:
        fallback_sub = subaccounts[0]
        logger.info(f"Falling back to first subaccount: {fallback_sub.get('name', 'Unknown')}")
        return fallback_sub.get("api_key")
    
    # Final fallback to config API keys
    if hasattr(settings, 'ghl_child_location_api_key') and settings.ghl_child_location_api_key:
        logger.info("Using fallback child location API key")
        return settings.ghl_child_location_api_key
    elif hasattr(settings, 'ghl_master_location_api_key') and settings.ghl_master_location_api_key:
        logger.info("Using fallback master location API key")
        return settings.ghl_master_location_api_key
    
    return None

async def enhance_contact_data():
    """Main function to enhance contact data from CSV"""
    
    # Read the CSV file
    csv_file = "database.csv"
    if not os.path.exists(csv_file):
        logger.error(f"CSV file {csv_file} not found!")
        return
    
    df = pd.read_csv(csv_file)
    logger.info(f"Loaded {len(df)} contacts from {csv_file}")
    
    # Take first 5 rows for testing
    test_df = df.head(5)
    logger.info(f"Processing first {len(test_df)} contacts for testing")
    
    enhanced_contacts = []
    
    # Process each contact
    for index, row in test_df.iterrows():
        contact_id = row['contact_id']
        source_id = str(row['source'])
        logger.info(f"Processing contact {index + 1}/5: {contact_id} (Source: {source_id})")
        
        # Get API key for this specific source
        api_key = get_api_key_for_source(source_id)
        
        if not api_key:
            logger.error(f"No API key found for source {source_id}! Skipping contact {contact_id}")
            continue
        
        enhancer = ContactDataEnhancer(api_key)
        
        try:
            # Fetch enhanced contact data
            enhanced_data = await enhancer.get_contact_details(contact_id)
            
            if enhanced_data:
                # Combine original CSV data with enhanced data
                combined_data = {
                    # Original CSV fields
                    "csv_opportunity_name": row['opportunity_name'],
                    "csv_full_name": row['full_name'],
                    "csv_phone": row['phone'],
                    "csv_pipeline_id": row['pipeline_id'],
                    "csv_current_stage": row['current_stage'],
                    "csv_contact_id": row['contact_id'],
                    "csv_source": row['source'],
                    "csv_opportunity_status": row['opportunity_status'],
                    "csv_center": row['center'],
                    "csv_ghl_id": row['ghl_id'],
                    
                    # Enhanced API data
                    **enhanced_data,
                    
                    # Add source info for reference
                    "source_id": source_id
                }
                enhanced_contacts.append(combined_data)
                logger.info(f"Successfully enhanced contact {contact_id}")
            else:
                logger.warning(f"Failed to enhance contact {contact_id}")
                
        except Exception as e:
            logger.error(f"Error processing contact {contact_id}: {str(e)}")
        
        finally:
            await enhancer.close()
    
    # Create enhanced CSV
    if enhanced_contacts:
        enhanced_df = pd.DataFrame(enhanced_contacts)
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"enhanced_contacts_test_{timestamp}.csv"
        
        enhanced_df.to_csv(output_file, index=False)
        logger.info(f"Enhanced contact data saved to {output_file}")
        
        # Print summary
        print(f"\n=== ENHANCEMENT SUMMARY ===")
        print(f"Original fields: {len(df.columns)}")
        print(f"Enhanced fields: {len(enhanced_df.columns)}")
        print(f"Contacts processed: {len(enhanced_contacts)}")
        print(f"Output file: {output_file}")
        
        # Show sample of enhanced fields
        print(f"\n=== NEW FIELDS ADDED ===")
        original_columns = set(df.columns)
        new_columns = [col for col in enhanced_df.columns if col not in original_columns and not col.startswith('csv_')]
        for col in sorted(new_columns)[:20]:  # Show first 20 new fields
            print(f"- {col}")
        if len(new_columns) > 20:
            print(f"... and {len(new_columns) - 20} more fields")
    else:
        logger.error("No contacts were successfully enhanced!")

def main():
    """Run the contact enhancement process"""
    try:
        asyncio.run(enhance_contact_data())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()
