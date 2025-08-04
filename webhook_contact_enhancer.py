"""
Webhook-Ready Contact Enhancement Script
=======================================

This script creates a CSV with all fields required for the webhook function you provided.
It maps GoHighLevel contact data to match the expected webhook payload structure.
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

class WebhookContactEnhancer:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=30)
        self._custom_field_cache = {}
        
    async def get_custom_fields(self) -> Dict[str, Dict]:
        """Fetch all custom field definitions"""
        if self._custom_field_cache:
            return self._custom_field_cache
            
        try:
            response = await self.client.get(f"{self.base_url}/custom-fields/")
            response.raise_for_status()
            data = response.json()
            custom_fields = data.get("customFields", [])
            
            # Create mapping of ID to field info
            field_mapping = {}
            for field in custom_fields:
                field_id = field.get("id")
                if field_id:
                    field_mapping[field_id] = field
            
            self._custom_field_cache = field_mapping
            logger.info(f"Cached {len(field_mapping)} custom field definitions")
            return field_mapping
            
        except Exception as e:
            logger.error(f"Custom fields fetch failed: {str(e)}")
            return {}
    
    async def get_contact_details(self, contact_id: str) -> Dict[str, Any]:
        """Fetch contact details and map to webhook format"""
        try:
            # Ensure we have custom field definitions
            custom_field_definitions = await self.get_custom_fields()
            
            # Fetch contact details
            response = await self.client.get(f"{self.base_url}/contacts/{contact_id}")
            response.raise_for_status()
            raw_response = response.json()
            
            contact = raw_response.get("contact", {})
            
            # Initialize webhook payload structure
            webhook_data = {
                # Required fields from your webhook
                "full_name": "",
                "email": "",
                "phone": "",
                "center": "",
                "pipeline_id": "",
                "to_stage": "",
                "from_stage": "",
                "ghl_id": "",
                
                # Optional fields for webhook
                "source": "",
                "address": "",
                "city": "",
                "state": "",
                "postal_code": "",
                "date_of_birth": "",
                "birth_state": "",
                "age": "",
                "social_security_number": "",
                "height": "",
                "weight": "",
                "doctors_name": "",
                "tobacco_user": "",
                "health_conditions": "",
                "medications": "",
                "monthly_premium": "",
                "coverage_amount": "",
                "carrier": "",
                "draft_date": "",
                "beneficiary_information": "",
                "bank_name": "",
                "routing_number": "",
                "account_number": "",
                "future_draft_date": "",
                "additional_information": "",
                "driver_license_number": "",
                "existing_coverage_last_2_years": "",
                "previous_applications_last_2_years": "",
                "date_of_submission": "",
                "timestamp": "",
                
                # Additional GHL fields
                "contact_id": contact.get("id", ""),
                "location_id": contact.get("locationId", ""),
                "fingerprint": contact.get("fingerprint", ""),
                "timezone": contact.get("timezone", ""),
                "country": contact.get("country", ""),
                "date_added": contact.get("dateAdded", ""),
                "tags": json.dumps(contact.get("tags", [])),
                "company_name": contact.get("companyName", ""),
                "website": contact.get("website", ""),
                "type": contact.get("type", ""),
                "assigned_to": contact.get("assignedTo", ""),
                "dnd": contact.get("dnd", False),
            }
            
            # Map basic contact fields
            webhook_data["full_name"] = contact.get("name", "") or f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
            webhook_data["email"] = contact.get("email", "")
            webhook_data["phone"] = contact.get("phone", "")
            webhook_data["address"] = contact.get("address1", "")
            webhook_data["city"] = contact.get("city", "")
            webhook_data["state"] = contact.get("state", "")
            webhook_data["postal_code"] = contact.get("postalCode", "")
            webhook_data["source"] = contact.get("source", "")
            webhook_data["contact_id"] = contact.get("id", "")
            
            # Process custom fields and try to map them to webhook fields
            custom_fields = contact.get("customField", [])
            custom_field_data = {}
            
            for cf in custom_fields:
                field_id = cf.get("id", "")
                field_value = cf.get("value", "")
                
                if field_id in custom_field_definitions:
                    field_info = custom_field_definitions[field_id]
                    field_name = field_info.get("name", "").lower()
                    field_key = field_info.get("fieldKey", "").lower()
                    
                    # Store all custom fields
                    custom_field_data[f"custom_{field_info.get('name', field_id)}"] = field_value
                    
                    # Try to map to webhook fields based on field name/key
                    if any(keyword in field_name for keyword in ["birth", "dob", "date_of_birth"]):
                        webhook_data["date_of_birth"] = field_value
                    elif any(keyword in field_name for keyword in ["age"]):
                        webhook_data["age"] = field_value
                    elif any(keyword in field_name for keyword in ["ssn", "social", "security"]):
                        webhook_data["social_security_number"] = field_value
                    elif any(keyword in field_name for keyword in ["height"]):
                        webhook_data["height"] = field_value
                    elif any(keyword in field_name for keyword in ["weight"]):
                        webhook_data["weight"] = field_value
                    elif any(keyword in field_name for keyword in ["doctor", "physician"]):
                        webhook_data["doctors_name"] = field_value
                    elif any(keyword in field_name for keyword in ["tobacco", "smoke", "smoking"]):
                        webhook_data["tobacco_user"] = field_value
                    elif any(keyword in field_name for keyword in ["health", "condition", "medical"]):
                        webhook_data["health_conditions"] = field_value
                    elif any(keyword in field_name for keyword in ["medication", "medicine", "drug"]):
                        webhook_data["medications"] = field_value
                    elif any(keyword in field_name for keyword in ["premium", "monthly"]):
                        webhook_data["monthly_premium"] = field_value
                    elif any(keyword in field_name for keyword in ["coverage", "amount", "benefit"]):
                        webhook_data["coverage_amount"] = field_value
                    elif any(keyword in field_name for keyword in ["carrier", "insurance", "company"]):
                        webhook_data["carrier"] = field_value
                    elif any(keyword in field_name for keyword in ["draft", "payment"]):
                        webhook_data["draft_date"] = field_value
                    elif any(keyword in field_name for keyword in ["beneficiary"]):
                        webhook_data["beneficiary_information"] = field_value
                    elif any(keyword in field_name for keyword in ["bank"]):
                        webhook_data["bank_name"] = field_value
                    elif any(keyword in field_name for keyword in ["routing"]):
                        webhook_data["routing_number"] = field_value
                    elif any(keyword in field_name for keyword in ["account"]):
                        webhook_data["account_number"] = field_value
                    elif any(keyword in field_name for keyword in ["license", "driver"]):
                        webhook_data["driver_license_number"] = field_value
                    elif any(keyword in field_name for keyword in ["birth_state", "birth state"]):
                        webhook_data["birth_state"] = field_value
            
            # Add all custom fields to the response
            webhook_data.update(custom_field_data)
            
            # Set timestamp
            webhook_data["timestamp"] = datetime.now().isoformat()
            
            return webhook_data
            
        except Exception as e:
            logger.error(f"Error fetching contact {contact_id}: {str(e)}")
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

async def create_webhook_ready_csv():
    """Main function to create webhook-ready CSV"""
    
    # Read the CSV file
    csv_file = "database.csv"
    if not os.path.exists(csv_file):
        logger.error(f"CSV file {csv_file} not found!")
        return
    
    df = pd.read_csv(csv_file)
    logger.info(f"Loaded {len(df)} contacts from {csv_file}")
    
    # Take all contacts for full processing (change this line to limit if needed)
    # test_df = df.head(5)  # Only first 5 for testing
    test_df = df  # Process ALL contacts
    logger.info(f"Processing ALL {len(test_df)} contacts from database")
    
    webhook_ready_data = []
    
    # Process each contact with its specific source API key
    for index, row in test_df.iterrows():
        contact_id = row['contact_id']
        source_id = str(row['source'])
        logger.info(f"Processing contact {index + 1}/5: {contact_id} (Source: {source_id})")
        
        # Get API key for this specific source
        api_key = get_api_key_for_source(source_id)
        
        if not api_key:
            logger.error(f"No API key found for source {source_id}! Skipping contact {contact_id}")
            continue
        
        enhancer = WebhookContactEnhancer(api_key)
        
        try:
            # Fetch enhanced contact data
            enhanced_data = await enhancer.get_contact_details(contact_id)
            
            if enhanced_data:
                # Add CSV data to enhanced data
                enhanced_data.update({
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
                })
                
                # Map CSV data to webhook fields if not already populated
                if not enhanced_data["center"]:
                    enhanced_data["center"] = row['center']
                if not enhanced_data["pipeline_id"]:
                    enhanced_data["pipeline_id"] = row['pipeline_id']
                enhanced_data["to_stage"] = row['current_stage']
                enhanced_data["ghl_id"] = row['ghl_id']
                enhanced_data["source_id"] = source_id  # Add source ID for reference
                
                webhook_ready_data.append(enhanced_data)
                logger.info(f"Successfully processed contact {contact_id}")
            else:
                logger.warning(f"Failed to enhance contact {contact_id}")
                
        except Exception as e:
            logger.error(f"Error processing contact {contact_id}: {str(e)}")
        
        finally:
            await enhancer.close()
    
    # Create webhook-ready CSV
    if webhook_ready_data:
        df_webhook = pd.DataFrame(webhook_ready_data)
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"webhook_ready_contacts_{timestamp}.csv"
        
        df_webhook.to_csv(output_file, index=False)
        logger.info(f"Webhook-ready contact data saved to {output_file}")
        
        # Print summary
        print(f"\n=== WEBHOOK CSV CREATION SUMMARY ===")
        print(f"Contacts processed: {len(webhook_ready_data)}")
        print(f"Total fields: {len(df_webhook.columns)}")
        print(f"Output file: {output_file}")
        
        # Show webhook fields status
        webhook_fields = [
            "full_name", "email", "phone", "center", "pipeline_id", 
            "to_stage", "ghl_id", "source", "address", "city", "state",
            "date_of_birth", "age", "social_security_number", "monthly_premium",
            "coverage_amount", "carrier"
        ]
        
        print(f"\n=== WEBHOOK FIELD STATUS ===")
        for field in webhook_fields:
            if field in df_webhook.columns:
                non_empty = df_webhook[field].notna().sum()
                print(f"✓ {field}: {non_empty}/{len(df_webhook)} populated")
            else:
                print(f"✗ {field}: missing")
                
        # Show custom fields found
        custom_fields = [col for col in df_webhook.columns if col.startswith('custom_')]
        if custom_fields:
            print(f"\n=== CUSTOM FIELDS FOUND ({len(custom_fields)}) ===")
            for field in custom_fields[:10]:  # Show first 10
                print(f"- {field}")
            if len(custom_fields) > 10:
                print(f"... and {len(custom_fields) - 10} more custom fields")
    else:
        logger.error("No contacts were successfully processed!")

def main():
    """Run the webhook CSV creation process"""
    try:
        asyncio.run(create_webhook_ready_csv())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()
