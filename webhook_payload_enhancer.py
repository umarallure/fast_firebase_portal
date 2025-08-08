"""
Updated Webhook Contact Enhancement Script
=========================================

This script processes the new DATABASEGHL.csv format and creates a webhook-ready CSV
with all fields required for your Supabase webhook payload.

Key Changes:
- Handles new CSV column names: "Contact ID" (with space), "Account Id"
- Maps "pipeline" to "pipeline_id" and "current_stage" to "to_stage"
- Fetches all custom fields from GoHighLevel API
- Creates output with ONLY the webhook payload columns
"""

import asyncio
import httpx
import pandas as pd
import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from app.config import settings

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebhookPayloadEnhancer:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=60)
        self._custom_field_cache = {}
        self.request_count = 0
        self.rate_limit_delay = 0.5  # 0.5 seconds between requests
        
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
                field_name = field.get("fieldKey", "").lower()
                field_mapping[field_id] = {
                    "name": field.get("name", ""),
                    "key": field_name,
                    "type": field.get("dataType", "")
                }
            
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
    
    def map_custom_field_to_webhook(self, field_name: str, field_key: str) -> str:
        """Map GoHighLevel custom field to webhook payload field"""
        field_lower = f"{field_name} {field_key}".lower()
        
        # Mapping logic based on field names/keys
        if any(keyword in field_lower for keyword in ['birth', 'dob', 'date_of_birth']):
            return 'date_of_birth'
        elif 'birth_state' in field_lower or 'state_of_birth' in field_lower:
            return 'birth_state'
        elif 'age' in field_lower and 'coverage' not in field_lower:
            return 'age'
        elif any(keyword in field_lower for keyword in ['ssn', 'social_security', 'social security']):
            return 'social_security_number'
        elif 'height' in field_lower:
            return 'height'
        elif 'weight' in field_lower:
            return 'weight'
        elif any(keyword in field_lower for keyword in ['doctor', 'physician']):
            return 'doctors_name'
        elif any(keyword in field_lower for keyword in ['tobacco', 'smoke', 'smoking']):
            return 'tobacco_user'
        elif any(keyword in field_lower for keyword in ['health', 'condition', 'medical']):
            return 'health_conditions'
        elif any(keyword in field_lower for keyword in ['medication', 'medicine', 'drug']):
            return 'medications'
        elif any(keyword in field_lower for keyword in ['premium', 'monthly_premium']):
            return 'monthly_premium'
        elif any(keyword in field_lower for keyword in ['coverage', 'face_amount', 'face amount']):
            return 'coverage_amount'
        elif 'carrier' in field_lower or 'insurance' in field_lower:
            return 'carrier'
        elif 'draft_date' in field_lower or 'draft date' in field_lower:
            return 'draft_date'
        elif any(keyword in field_lower for keyword in ['beneficiary', 'beneficiaries']):
            return 'beneficiary_information'
        elif 'bank' in field_lower and 'name' in field_lower:
            return 'bank_name'
        elif any(keyword in field_lower for keyword in ['routing', 'aba']):
            return 'routing_number'
        elif 'account' in field_lower and 'number' in field_lower:
            return 'account_number'
        elif 'future_draft' in field_lower or 'next_draft' in field_lower:
            return 'future_draft_date'
        elif any(keyword in field_lower for keyword in ['driver', 'license', 'dl']):
            return 'driver_license_number'
        elif 'existing_coverage' in field_lower or 'previous_coverage' in field_lower:
            return 'existing_coverage_last_2_years'
        elif 'previous_application' in field_lower or 'prior_application' in field_lower:
            return 'previous_applications_last_2_years'
        elif any(keyword in field_lower for keyword in ['submission', 'submit']):
            return 'date_of_submission'
        elif any(keyword in field_lower for keyword in ['additional', 'notes', 'comments']):
            return 'additional_information'
        elif any(keyword in field_lower for keyword in ['address', 'street']):
            return 'address'
        elif 'city' in field_lower:
            return 'city'
        elif 'state' in field_lower and 'birth' not in field_lower:
            return 'state'
        elif any(keyword in field_lower for keyword in ['zip', 'postal']):
            return 'postal_code'
        else:
            return f'custom_{field_key}' if field_key else f'custom_{field_name.lower().replace(" ", "_")}'
    
    async def get_contact_details(self, contact_id: str) -> Dict[str, Any]:
        """Fetch contact details and map to webhook payload format"""
        try:
            # Ensure we have custom field definitions
            custom_field_definitions = await self.get_custom_fields()
            
            # Fetch contact details
            await self._rate_limit()
            response = await self.client.get(f"{self.base_url}/contacts/{contact_id}")
            response.raise_for_status()
            raw_response = response.json()
            
            contact = raw_response.get("contact", {})
            
            # Initialize with all possible webhook payload fields
            webhook_data = {
                # Required fields
                'full_name': contact.get('firstName', '') + ' ' + contact.get('lastName', ''),
                'email': contact.get('email', ''),
                'phone': contact.get('phone', ''),
                'center': '',  # Will be set from CSV
                'pipeline_id': '',  # Will be set from CSV  
                'to_stage': '',  # Will be set from CSV
                'ghl_id': contact_id,  # Will be updated with subaccount prefix later
                
                # Optional contact info fields
                'source': contact.get('source', ''),
                'address': contact.get('address1', ''),
                'city': contact.get('city', ''),
                'state': contact.get('state', ''),
                'postal_code': contact.get('postalCode', ''),
                
                # Personal information fields
                'date_of_birth': '',
                'birth_state': '',
                'age': '',
                'social_security_number': '',
                'height': '',
                'weight': '',
                'doctors_name': '',
                'tobacco_user': '',
                'health_conditions': '',
                'medications': '',
                
                # Insurance fields
                'monthly_premium': '',
                'coverage_amount': '',
                'carrier': '',
                'draft_date': '',
                'beneficiary_information': '',
                
                # Banking fields
                'bank_name': '',
                'routing_number': '',
                'account_number': '',
                'future_draft_date': '',
                
                # Additional fields
                'additional_information': '',
                'driver_license_number': '',
                'existing_coverage_last_2_years': '',
                'previous_applications_last_2_years': '',
                'date_of_submission': '',
                'timestamp': contact.get('dateAdded', ''),
                'from_stage': '',  # Will be populated if needed
            }
            
            # Clean up full_name
            webhook_data['full_name'] = webhook_data['full_name'].strip()
            if not webhook_data['full_name']:
                webhook_data['full_name'] = contact.get('name', '')
            
            # Get date of birth from direct field
            if contact.get('dateOfBirth'):
                webhook_data['date_of_birth'] = contact.get('dateOfBirth')
            
            # Process custom fields - IMPORTANT: it's 'customField' not 'customFields'
            custom_values = contact.get('customField', [])  # Changed from 'customFields'
            if custom_values:
                for custom_field in custom_values:
                    field_id = custom_field.get('id')
                    field_value = custom_field.get('value', '')
                    
                    if field_id in custom_field_definitions:
                        field_info = custom_field_definitions[field_id]
                        field_name = field_info['name']
                        field_key = field_info['key']
                        
                        # Map to webhook field
                        webhook_field = self.map_custom_field_to_webhook(field_name, field_key)
                        
                        if webhook_field in webhook_data:
                            webhook_data[webhook_field] = field_value
                        else:
                            # Store unmapped custom fields
                            webhook_data[webhook_field] = field_value
            
            logger.info(f"Successfully processed contact {contact_id}")
            return webhook_data
            
        except Exception as e:
            logger.error(f"Failed to fetch contact {contact_id}: {str(e)}")
            return None
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

def get_api_key_for_source(source_id: str) -> Optional[str]:
    """Get API key for a specific source/subaccount ID from settings"""
    subaccounts = settings.subaccounts_list
    
    # Find subaccount by source ID
    for sub in subaccounts:
        if str(sub.get("id")) == str(source_id):
            return sub.get("api_key")
    
    logger.warning(f"No API key found for source ID: {source_id}")
    
    # Fallback to first available subaccount
    if subaccounts:
        fallback_sub = subaccounts[0]
        logger.info(f"Falling back to first subaccount: {fallback_sub.get('name', 'Unknown')}")
        return fallback_sub.get("api_key")
    
    return None

def get_subaccount_name_for_source(source_id: str) -> str:
    """Get subaccount name for a specific source/subaccount ID from settings"""
    subaccounts = settings.subaccounts_list
    
    # Find subaccount by source ID
    for sub in subaccounts:
        if str(sub.get("id")) == str(source_id):
            name = sub.get("name", "Unknown")
            # Return first 3 letters in lowercase
            return name[:3].lower()
    
    # Fallback to first available subaccount
    if subaccounts:
        fallback_sub = subaccounts[0]
        name = fallback_sub.get('name', 'Unknown')
        return name[:3].lower()
    
    return "unk"  # Default unknown

async def process_new_csv_format():
    """Main function to process the new DATABASEGHL.csv format"""
    
    # Read the new CSV file
    csv_file = "DATABASEGHL.csv"
    if not os.path.exists(csv_file):
        logger.error(f"CSV file {csv_file} not found!")
        return
    
    df = pd.read_csv(csv_file)
    total_contacts = len(df)
    logger.info(f"Loading {total_contacts} contacts from {csv_file}")
    
    # Show the columns we found
    print(f"\n📋 CSV COLUMNS FOUND:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    # Map the CSV column names to our expected names
    column_mapping = {
        'Contact ID': 'contact_id',  # Handle space in column name
        'Account Id': 'source',      # Map Account Id to source
        'pipeline': 'pipeline_id',   # Map pipeline to pipeline_id
        'current_stage': 'to_stage'  # Map current_stage to to_stage
    }
    
    # Rename columns
    df_renamed = df.rename(columns=column_mapping)
    
    # Add center column (required for webhook) - you may need to map this differently
    if 'center' not in df_renamed.columns:
        df_renamed['center'] = 'Default Center'  # Set default or map from another field
    
    print(f"\n📊 PROCESSING SUMMARY:")
    print(f"Total contacts to process: {total_contacts}")
    print(f"Contact ID column: {'✓' if 'contact_id' in df_renamed.columns else '✗'}")
    print(f"Source column: {'✓' if 'source' in df_renamed.columns else '✗'}")
    print(f"Pipeline ID column: {'✓' if 'pipeline_id' in df_renamed.columns else '✗'}")
    print(f"Center column: {'✓' if 'center' in df_renamed.columns else '✗'}")
    
    # Process in smaller batches for testing
    # test_df = df_renamed.head(5)  # Test with first 5
    test_df = df_renamed  # Process all contacts
    
    # Group contacts by source to optimize API key usage
    source_groups = test_df.groupby('source')
    
    all_webhook_data = []
    total_processed = 0
    total_successful = 0
    total_failed = 0
    
    start_time = datetime.now()
    
    print(f"\n🚀 STARTING WEBHOOK PAYLOAD ENHANCEMENT")
    print(f"📊 Total contacts to process: {len(test_df)}")
    print(f"📂 Source groups found: {len(source_groups)}")
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
            logger.error(f"No API key found for source {source_id_str}, skipping {group_size} contacts")
            total_failed += group_size
            continue
        
        enhancer = WebhookPayloadEnhancer(api_key)
        
        group_successful = 0
        group_failed = 0
        
        try:
            for index, row in group_df.iterrows():
                contact_id = row['contact_id']
                total_processed += 1
                
                try:
                    # Fetch enhanced contact data
                    webhook_data = await enhancer.get_contact_details(contact_id)
                    
                    if webhook_data:
                        # Get subaccount name for ghl_id formatting
                        subaccount_prefix = get_subaccount_name_for_source(source_id_str)
                        
                        # Add CSV data to webhook payload
                        webhook_data.update({
                            'center': row.get('center', 'Default Center'),
                            'pipeline_id': row.get('pipeline_id', ''),
                            'to_stage': row.get('to_stage', ''),
                            'source': source_id_str,
                            'ghl_id': f"{contact_id}-{subaccount_prefix}"  # New format: ContactID-first3letters
                        })
                        
                        # Add CSV fields that might not be in GHL
                        csv_fields = ['Opportunity Name', 'full_name', 'phone', 'email']
                        for field in csv_fields:
                            if field in row and pd.notna(row[field]):
                                csv_key = f'csv_{field.lower().replace(" ", "_")}'
                                webhook_data[csv_key] = row[field]
                        
                        all_webhook_data.append(webhook_data)
                        group_successful += 1
                        total_successful += 1
                        
                        if total_processed % 50 == 0:
                            print(f"⏳ Progress: {total_processed}/{len(test_df)} ({total_processed/len(test_df)*100:.1f}%)")
                    else:
                        group_failed += 1
                        total_failed += 1
                        
                except Exception as e:
                    logger.error(f"Error processing contact {contact_id}: {str(e)}")
                    group_failed += 1
                    total_failed += 1
        
        finally:
            await enhancer.close()
        
        print(f"✅ Source {source_id_str} complete: {group_successful} successful, {group_failed} failed")
    
    # Create final webhook-ready CSV with ONLY webhook payload columns
    if all_webhook_data:
        webhook_df = pd.DataFrame(all_webhook_data)
        
        # Define the exact columns needed for webhook payload (in order)
        webhook_columns = [
            # Required fields
            'full_name', 'email', 'phone', 'center', 'pipeline_id', 'to_stage', 'ghl_id',
            
            # Optional fields (exactly as in webhook function)
            'source', 'address', 'city', 'state', 'postal_code',
            'date_of_birth', 'birth_state', 'age', 'social_security_number',
            'height', 'weight', 'doctors_name', 'tobacco_user',
            'health_conditions', 'medications', 'monthly_premium', 'coverage_amount', 'carrier',
            'draft_date', 'beneficiary_information', 'bank_name', 'routing_number', 'account_number',
            'future_draft_date', 'additional_information', 'driver_license_number',
            'existing_coverage_last_2_years', 'previous_applications_last_2_years',
            'date_of_submission', 'timestamp', 'from_stage'
        ]
        
        # Create final dataframe with only webhook columns
        final_df = pd.DataFrame()
        for col in webhook_columns:
            if col in webhook_df.columns:
                final_df[col] = webhook_df[col]
            else:
                final_df[col] = ''  # Empty string for missing columns
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"webhook_payload_ready_{timestamp}.csv"
        
        final_df.to_csv(output_file, index=False)
        
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print(f"\n🎉 WEBHOOK PAYLOAD ENHANCEMENT COMPLETE!")
        print("=" * 80)
        print(f"📁 Output file: {output_file}")
        print(f"📊 Total contacts processed: {total_processed}")
        print(f"✅ Successful enhancements: {total_successful}")
        print(f"❌ Failed enhancements: {total_failed}")
        print(f"📈 Success rate: {total_successful/total_processed*100:.1f}%")
        print(f"🕐 Total time: {total_time}")
        print(f"⚡ Average rate: {total_processed/total_time.total_seconds()*60:.1f} contacts/minute")
        print(f"🎯 Webhook payload columns: {len(final_df.columns)}")
        
        # Show field population statistics
        print(f"\n📋 WEBHOOK FIELD POPULATION SUMMARY:")
        for col in webhook_columns[:15]:  # Show first 15 fields
            non_empty = final_df[col].notna().sum() if col in final_df.columns else 0
            filled_count = (final_df[col] != '').sum() if col in final_df.columns else 0
            percentage = (filled_count / len(final_df) * 100) if len(final_df) > 0 else 0
            print(f"  {col}: {filled_count}/{len(final_df)} ({percentage:.1f}%)")
        
        if len(webhook_columns) > 15:
            print(f"  ... and {len(webhook_columns) - 15} more fields")
        
        # Show sample data
        print(f"\n📄 SAMPLE WEBHOOK PAYLOAD DATA:")
        if len(final_df) > 0:
            sample_row = final_df.iloc[0]
            required_fields = ['full_name', 'center', 'pipeline_id', 'to_stage']
            for field in required_fields:
                value = sample_row.get(field, '')
                print(f"  {field}: {value}")
        
    else:
        logger.error("No contacts were successfully processed!")

def main():
    """Run the webhook payload enhancement process"""
    try:
        asyncio.run(process_new_csv_format())
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        print("Any processed data has been preserved.")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()
