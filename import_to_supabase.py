#!/usr/bin/env python3
"""
Supabase Lead/Opportunity Import Script
Imports data from finaltransfercheckerimport.csv to Supabase database
via the create-lead-opportunity webhook endpoint
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('supabase_import.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class SupabaseImporter:
    def __init__(self):
        self.webhook_url = "https://akdryqadcxhzqcqhssok.supabase.co/functions/v1/create-lead-opportunity"
        self.auth_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFrZHJ5cWFkY3hoenFjcWhzc29rIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3Mjg5MDQsImV4cCI6MjA2OTMwNDkwNH0.36poCyc_PGl2EnGM3283Hj5_yxRYQU2IetYl8aUA3r4"
        self.headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }
        self.success_count = 0
        self.error_count = 0
        self.errors = []

    def clean_value(self, value: Any) -> Optional[str]:
        """Clean and normalize CSV values"""
        if pd.isna(value) or value == '' or str(value).strip() in ['', 'NA', 'N/A', 'na', 'n/a']:
            return None
        
        # Handle numeric values that should be strings (like phone numbers)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        
        return str(value).strip()

    def format_phone_number(self, value: Any) -> Optional[str]:
        """Format phone number to display as (XXX) XXX-XXXX"""
        if pd.isna(value) or value == '' or str(value).strip() in ['', 'NA', 'N/A', 'na', 'n/a']:
            return None
        
        # Handle numeric values (remove .0 suffix)
        if isinstance(value, float) and value.is_integer():
            phone_str = str(int(value))
        else:
            phone_str = str(value).strip()
        
        # Remove any non-digit characters
        import re
        cleaned = re.sub(r'[^\d]', '', phone_str)
        
        if not cleaned:
            return None
        
        # Format based on length
        if len(cleaned) == 10:
            # US phone number: (XXX) XXX-XXXX
            return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        elif len(cleaned) == 11 and cleaned.startswith('1'):
            # US phone number with country code: (XXX) XXX-XXXX
            return f"({cleaned[1:4]}) {cleaned[4:7]}-{cleaned[7:]}"
        else:
            # Return as-is if not standard US format
            return cleaned

    def format_numeric_string(self, value: Any) -> Optional[str]:
        """Format numeric values that should be strings (like account numbers, SSN, etc.)"""
        if pd.isna(value) or value == '' or str(value).strip() in ['', 'NA', 'N/A', 'na', 'n/a']:
            return None
        
        # Handle numeric values (remove .0 suffix)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        
        return str(value).strip()

    def parse_boolean(self, value: Any) -> Optional[bool]:
        """Parse boolean values from various string formats"""
        if value is None:
            return None
        
        clean_val = self.clean_value(value)
        if clean_val is None:
            return None
            
        clean_val = clean_val.lower()
        if clean_val in ['yes', 'true', '1', 'y']:
            return True
        elif clean_val in ['no', 'false', '0', 'n']:
            return False
        return None

    def parse_date(self, value: Any) -> Optional[str]:
        """Parse date values and convert to ISO format"""
        clean_val = self.clean_value(value)
        if clean_val is None:
            return None
            
        try:
            # Try different date formats
            for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y']:
                try:
                    parsed_date = datetime.strptime(clean_val, fmt)
                    return parsed_date.isoformat()
                except ValueError:
                    continue
            
            # If standard formats fail, try pandas parsing
            parsed_date = pd.to_datetime(clean_val, errors='coerce')
            if not pd.isna(parsed_date):
                return parsed_date.isoformat()
                
        except Exception as e:
            logger.warning(f"Could not parse date '{clean_val}': {e}")
            
        return None

    def parse_number(self, value: Any) -> Optional[float]:
        """Parse numeric values"""
        clean_val = self.clean_value(value)
        if clean_val is None:
            return None
            
        try:
            # Remove any non-numeric characters except decimal point and minus
            import re
            numeric_str = re.sub(r'[^\d.-]', '', clean_val)
            if numeric_str:
                return float(numeric_str)
        except (ValueError, TypeError):
            pass
            
        return None

    def map_csv_to_payload(self, row: pd.Series) -> Dict[str, Any]:
        """Map CSV row to webhook payload format"""
        payload = {
            # Required fields
            'full_name': self.clean_value(row.get('full_name')),
            'center': self.clean_value(row.get('center')),
            'pipeline_id': self.clean_value(row.get('pipeline_id')),
            'to_stage': self.clean_value(row.get('to_stage')),
            
            # Contact information
            'email': self.clean_value(row.get('email')),
            'phone': self.format_phone_number(row.get('phone')),
            'ghl_id': self.clean_value(row.get('ghl_id')),
            
            # Address information
            'address': self.clean_value(row.get('address')),
            'city': self.clean_value(row.get('city')),
            'state': self.clean_value(row.get('state')),
            'postal_code': self.clean_value(row.get('postal_code')),
            
            # Personal information
            'birth_state': self.clean_value(row.get('Birth State')),
            'age': self.parse_number(row.get('Age')),
            'social_security_number': self.format_numeric_string(row.get('Social Security Number')),
            'height': self.clean_value(row.get('Height')),
            'weight': self.parse_number(row.get('Weight')),
            'doctors_name': self.clean_value(row.get('Doctors_Name')),
            
            # Health information
            'tobacco_user': self.parse_boolean(row.get('custom_Tobacco User?')),
            'health_conditions': self.clean_value(row.get('Health_Conditions')),
            'medications': self.clean_value(row.get('Medications')),
            
            # Insurance information
            'monthly_premium': self.parse_number(row.get('Monthly Premium')),
            'coverage_amount': self.parse_number(row.get('Coverage Amount')),
            'carrier': self.clean_value(row.get('Carrier')),
            'draft_date': self.clean_value(row.get('Draft_Date')),
            'beneficiary_information': self.clean_value(row.get('Beneficiary_Information')),
            
            # Banking information
            'routing_number': self.format_numeric_string(row.get('Routing #')),
            'account_number': self.format_numeric_string(row.get('Account #')),
            'future_draft_date': self.clean_value(row.get('Future Draft Date')),
            
            # Additional information
            'additional_information': self.clean_value(row.get('custom_Additional Information')),
            'driver_license_number': self.format_numeric_string(row.get('custom_Driver license Number:')),
            'existing_coverage_last_2_years': self.parse_boolean(row.get('custom_Any existing / previous coverage in last 2 years?')),
            
            # Dates
            'date_of_submission': self.parse_date(row.get('Date of Submission')),
            
            # Pipeline information
            'from_stage': self.clean_value(row.get('from_stage')),
            
            # Source
            'source': 'CSV Import',
            'timestamp': datetime.now().isoformat()
        }
        
        # Remove None values to avoid sending unnecessary data
        return {k: v for k, v in payload.items() if v is not None}

    def send_to_webhook(self, payload: Dict[str, Any], row_index: int) -> bool:
        """Send payload to Supabase webhook"""
        try:
            response = requests.post(
                self.webhook_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    self.success_count += 1
                    logger.info(f"Row {row_index}: Successfully imported {payload.get('full_name', 'N/A')}")
                    return True
                else:
                    error_msg = result.get('error', 'Unknown error')
                    self.error_count += 1
                    self.errors.append(f"Row {row_index}: {error_msg}")
                    logger.error(f"Row {row_index}: API error - {error_msg}")
                    return False
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.error_count += 1
                self.errors.append(f"Row {row_index}: {error_msg}")
                logger.error(f"Row {row_index}: {error_msg}")
                return False
                
        except requests.exceptions.Timeout:
            error_msg = "Request timeout"
            self.error_count += 1
            self.errors.append(f"Row {row_index}: {error_msg}")
            logger.error(f"Row {row_index}: {error_msg}")
            return False
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {str(e)}"
            self.error_count += 1
            self.errors.append(f"Row {row_index}: {error_msg}")
            logger.error(f"Row {row_index}: {error_msg}")
            return False
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.error_count += 1
            self.errors.append(f"Row {row_index}: {error_msg}")
            logger.error(f"Row {row_index}: {error_msg}")
            return False

    def validate_required_fields(self, payload: Dict[str, Any]) -> tuple[bool, str]:
        """Validate that required fields are present"""
        required_fields = ['full_name', 'center', 'pipeline_id', 'to_stage']
        missing_fields = []
        
        for field in required_fields:
            if not payload.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
        
        return True, ""

    def import_csv(self, csv_file_path: str, batch_size: int = 10, delay_between_requests: float = 0.5):
        """Import CSV data to Supabase"""
        logger.info(f"Starting import from {csv_file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            logger.info(f"Loaded {len(df)} rows from CSV")
            
        except UnicodeDecodeError:
            # Try with different encoding
            df = pd.read_csv(csv_file_path, encoding='latin-1')
            logger.info(f"Loaded {len(df)} rows from CSV (latin-1 encoding)")
        
        # Filter out rows with empty names
        df = df.dropna(subset=['full_name'])
        df = df[df['full_name'].str.strip() != '']
        logger.info(f"Processing {len(df)} rows with valid names")
        
        start_time = datetime.now()
        processed_count = 0
        
        for index, row in df.iterrows():
            processed_count += 1
            row_number = index + 1
            
            # Map CSV row to payload
            payload = self.map_csv_to_payload(row)
            
            # Validate required fields
            is_valid, validation_error = self.validate_required_fields(payload)
            if not is_valid:
                self.error_count += 1
                self.errors.append(f"Row {row_number}: {validation_error}")
                logger.error(f"Row {row_number}: Validation failed - {validation_error}")
                continue
            
            # Send to webhook
            self.send_to_webhook(payload, row_number)
            
            # Add delay between requests to avoid rate limiting
            if delay_between_requests > 0:
                time.sleep(delay_between_requests)
            
            # Progress update
            if processed_count % batch_size == 0:
                elapsed_time = datetime.now() - start_time
                rate = processed_count / elapsed_time.total_seconds() * 60  # per minute
                logger.info(f"Processed {processed_count}/{len(df)} rows. Success: {self.success_count}, Errors: {self.error_count}. Rate: {rate:.1f} rows/min")
        
        # Final summary
        elapsed_time = datetime.now() - start_time
        logger.info(f"Import completed in {elapsed_time}")
        logger.info(f"Total processed: {processed_count}")
        logger.info(f"Successful imports: {self.success_count}")
        logger.info(f"Failed imports: {self.error_count}")
        
        if self.errors:
            logger.info(f"Errors encountered:")
            for error in self.errors[:10]:  # Show first 10 errors
                logger.error(f"  {error}")
            if len(self.errors) > 10:
                logger.info(f"  ... and {len(self.errors) - 10} more errors")
        
        # Save detailed error report
        if self.errors:
            error_file = f"import_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(error_file, 'w') as f:
                f.write(f"Import Error Report - {datetime.now()}\n")
                f.write(f"Total Errors: {len(self.errors)}\n\n")
                for error in self.errors:
                    f.write(f"{error}\n")
            logger.info(f"Detailed error report saved to {error_file}")

def main():
    """Main function to run the import"""
    csv_file = "finaltransfercheckerimport.csv"
    
    print("Supabase Lead/Opportunity Import Script")
    print("=" * 50)
    print(f"CSV File: {csv_file}")
    print(f"Webhook URL: https://akdryqadcxhzqcqhssok.supabase.co/functions/v1/create-lead-opportunity")
    print()
    
    # Confirm before proceeding
    response = input("Do you want to proceed with the import? (y/N): ").strip().lower()
    if response != 'y':
        print("Import cancelled.")
        return
    
    # Configuration
    batch_size = int(input("Enter batch size for progress updates (default 10): ").strip() or "10")
    delay = float(input("Enter delay between requests in seconds (default 0.5): ").strip() or "0.5")
    
    print(f"\nStarting import with batch size {batch_size} and {delay}s delay...")
    
    # Create importer and run
    importer = SupabaseImporter()
    importer.import_csv(csv_file, batch_size=batch_size, delay_between_requests=delay)
    
    print("\nImport completed!")
    print(f"Successful imports: {importer.success_count}")
    print(f"Failed imports: {importer.error_count}")
    
    if importer.error_count > 0:
        print("\nSome imports failed. Check the log files for details.")

if __name__ == "__main__":
    main()
