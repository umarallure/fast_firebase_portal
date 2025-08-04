"""
Full Database Contact Enhancement Script
=======================================

This script processes ALL contacts from the database.csv file and creates
a webhook-ready CSV with enhanced data from GoHighLevel API.

Features:
- Processes all contacts in the database
- Uses correct API key based on source field
- Implements rate limiting to avoid API throttling
- Provides progress tracking
- Creates batched output files for large datasets
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
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FullDatabaseContactEnhancer:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=60)  # Increased timeout
        self._custom_field_cache = {}
        self.request_count = 0
        self.rate_limit_delay = 0.5  # 0.5 seconds between requests to avoid rate limiting
        
    async def get_custom_fields(self) -> Dict[str, Dict]:
        """Fetch all custom field definitions"""
        if self._custom_field_cache:
            return self._custom_field_cache
            
        try:
            await self._rate_limit()
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
    
    async def _rate_limit(self):
        """Implement rate limiting between API calls"""
        if self.request_count > 0:
            await asyncio.sleep(self.rate_limit_delay)
        self.request_count += 1
    
    async def get_contact_details(self, contact_id: str) -> Dict[str, Any]:
        """Fetch contact details and map to webhook format"""
        try:
            # Ensure we have custom field definitions
            custom_field_definitions = await self.get_custom_fields()
            
            # Rate limit before API call
            await self._rate_limit()
            
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
                    
                    # Store all custom fields
                    custom_field_data[f"custom_{field_info.get('name', field_id)}"] = field_value
                    
                    # Try to map to webhook fields based on field name
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
            return api_key
    
    logger.warning(f"No API key found for source ID: {source_id}")
    
    # Fallback to first available subaccount
    if subaccounts:
        fallback_sub = subaccounts[0]
        return fallback_sub.get("api_key")
    
    return None

async def process_full_database():
    """Main function to process the entire database"""
    
    # Read the CSV file
    csv_file = "database.csv"
    if not os.path.exists(csv_file):
        logger.error(f"CSV file {csv_file} not found!")
        return
    
    df = pd.read_csv(csv_file)
    total_contacts = len(df)
    logger.info(f"Loading {total_contacts} contacts from {csv_file}")
    
    # Group contacts by source to optimize API key usage
    source_groups = df.groupby('source')
    
    all_enhanced_data = []
    total_processed = 0
    total_successful = 0
    total_failed = 0
    
    start_time = datetime.now()
    
    print(f"\n🚀 STARTING FULL DATABASE ENHANCEMENT")
    print(f"📊 Total contacts to process: {total_contacts}")
    print(f"📂 Source groups found: {list(source_groups.groups.keys())}")
    print(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Process each source group
    for source_id, group_df in source_groups:
        source_id_str = str(source_id)
        group_size = len(group_df)
        
        print(f"\n📋 Processing Source {source_id_str}: {group_size} contacts")
        
        # Get API key for this source
        api_key = get_api_key_for_source(source_id_str)
        
        if not api_key:
            logger.error(f"No API key found for source {source_id_str}! Skipping {group_size} contacts")
            total_failed += group_size
            continue
        
        # Get subaccount name for logging
        subaccounts = settings.subaccounts_list
        subaccount_name = "Unknown"
        for sub in subaccounts:
            if str(sub.get("id")) == source_id_str:
                subaccount_name = sub.get("name", "Unknown")
                break
        
        print(f"🔑 Using API key for: {subaccount_name}")
        
        enhancer = FullDatabaseContactEnhancer(api_key)
        
        group_successful = 0
        group_failed = 0
        
        try:
            # Process each contact in this source group
            for index, row in group_df.iterrows():
                contact_id = row['contact_id']
                total_processed += 1
                
                # Progress update every 50 contacts
                if total_processed % 50 == 0:
                    elapsed = datetime.now() - start_time
                    rate = total_processed / elapsed.total_seconds() * 60  # contacts per minute
                    eta_minutes = (total_contacts - total_processed) / (rate / 60) if rate > 0 else 0
                    
                    print(f"⏳ Progress: {total_processed}/{total_contacts} ({total_processed/total_contacts*100:.1f}%) | "
                          f"Rate: {rate:.1f}/min | ETA: {eta_minutes:.0f}min")
                
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
                        enhanced_data["source_id"] = source_id_str
                        
                        all_enhanced_data.append(enhanced_data)
                        total_successful += 1
                        group_successful += 1
                    else:
                        total_failed += 1
                        group_failed += 1
                        logger.warning(f"Failed to enhance contact {contact_id}")
                        
                except Exception as e:
                    total_failed += 1
                    group_failed += 1
                    logger.error(f"Error processing contact {contact_id}: {str(e)}")
        
        finally:
            await enhancer.close()
        
        print(f"✅ Source {source_id_str} complete: {group_successful} successful, {group_failed} failed")
        
        # Save intermediate results every 500 contacts to prevent data loss
        if len(all_enhanced_data) >= 500 and len(all_enhanced_data) % 500 == 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            intermediate_file = f"webhook_contacts_intermediate_{len(all_enhanced_data)}_{timestamp}.csv"
            
            temp_df = pd.DataFrame(all_enhanced_data)
            temp_df.to_csv(intermediate_file, index=False)
            print(f"💾 Intermediate save: {intermediate_file} ({len(all_enhanced_data)} contacts)")
    
    # Create final enhanced CSV
    if all_enhanced_data:
        enhanced_df = pd.DataFrame(all_enhanced_data)
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"webhook_ready_contacts_FULL_{timestamp}.csv"
        
        enhanced_df.to_csv(output_file, index=False)
        
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print(f"\n🎉 FULL DATABASE ENHANCEMENT COMPLETE!")
        print("=" * 80)
        print(f"📁 Output file: {output_file}")
        print(f"📊 Total contacts processed: {total_processed}")
        print(f"✅ Successful enhancements: {total_successful}")
        print(f"❌ Failed enhancements: {total_failed}")
        print(f"📈 Success rate: {total_successful/total_processed*100:.1f}%")
        print(f"🕐 Total time: {total_time}")
        print(f"⚡ Average rate: {total_processed/total_time.total_seconds()*60:.1f} contacts/minute")
        print(f"🎯 Total fields per contact: {len(enhanced_df.columns)}")
        
        # Show field statistics
        webhook_fields = [
            "full_name", "email", "phone", "center", "pipeline_id", 
            "to_stage", "ghl_id", "source", "address", "city", "state",
            "date_of_birth", "age", "social_security_number", "monthly_premium",
            "coverage_amount", "carrier"
        ]
        
        print(f"\n📋 WEBHOOK FIELD POPULATION SUMMARY:")
        for field in webhook_fields:
            if field in enhanced_df.columns:
                non_empty = enhanced_df[field].notna().sum()
                percentage = non_empty/len(enhanced_df)*100
                print(f"  {field}: {non_empty}/{len(enhanced_df)} ({percentage:.1f}%)")
        
        # Show custom fields found
        custom_fields = [col for col in enhanced_df.columns if col.startswith('custom_')]
        if custom_fields:
            print(f"\n🔧 Custom fields found: {len(custom_fields)}")
        
    else:
        logger.error("No contacts were successfully processed!")

def main():
    """Run the full database enhancement process"""
    try:
        asyncio.run(process_full_database())
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        print("Any intermediate files saved will be preserved.")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()
