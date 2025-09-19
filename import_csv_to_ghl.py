import csv
import requests
import json
import time
import logging
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ghl_csv_import.log'),
        logging.StreamHandler()
    ]
)

class GHLImporter:
    def __init__(self, api_token: str, location_id: str):
        """
        Initialize GHL Importer
        
        Args:
            api_token: GHL API token (Bearer token)
            location_id: GHL location ID
        """
        self.api_token = api_token
        self.location_id = location_id
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        self.pipelines_cache = {}
        self.stages_cache = {}
        
    def get_pipelines(self) -> Dict:
        """
        Fetch all pipelines and their stages
        """
        try:
            url = f"{self.base_url}/pipelines/"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                pipelines_data = response.json()
                logging.info(f"Successfully fetched {len(pipelines_data.get('pipelines', []))} pipelines")
                
                # Cache pipelines and stages for quick lookup
                for pipeline in pipelines_data.get('pipelines', []):
                    pipeline_name = pipeline['name']
                    pipeline_id = pipeline['id']
                    self.pipelines_cache[pipeline_name] = pipeline_id
                    
                    # Cache stages for this pipeline
                    for stage in pipeline.get('stages', []):
                        stage_key = f"{pipeline_name}|{stage['name']}"
                        self.stages_cache[stage_key] = {
                            'stage_id': stage['id'],
                            'pipeline_id': pipeline_id
                        }
                
                return pipelines_data
            else:
                logging.error(f"Failed to fetch pipelines: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            logging.error(f"Error fetching pipelines: {str(e)}")
            return {}
    
    def get_stage_id(self, pipeline_name: str, stage_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get stage ID and pipeline ID for given pipeline and stage names
        
        Returns:
            Tuple of (stage_id, pipeline_id) or (None, None) if not found
        """
        stage_key = f"{pipeline_name}|{stage_name}"
        stage_info = self.stages_cache.get(stage_key)
        
        if stage_info:
            return stage_info['stage_id'], stage_info['pipeline_id']
        
        logging.warning(f"Stage not found: {pipeline_name} -> {stage_name}")
        return None, None
    
    def parse_name(self, full_name: str) -> Tuple[str, str]:
        """
        Parse full name into first and last name
        """
        if not full_name:
            return "", ""
        
        name_parts = full_name.strip().split()
        if len(name_parts) == 1:
            return name_parts[0], ""
        elif len(name_parts) == 2:
            return name_parts[0], name_parts[1]
        else:
            # If more than 2 parts, first is firstName, rest is lastName
            return name_parts[0], " ".join(name_parts[1:])
    
    def parse_tags(self, tags_str: str) -> List[str]:
        """
        Parse tags string into list
        """
        if not tags_str:
            return []
        
        # Split by comma and clean up
        tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
        return tags
    
    def create_contact(self, row: Dict) -> Optional[str]:
        """
        Create a contact in GHL
        
        Returns:
            Contact ID if successful, None otherwise
        """
        try:
            # Parse name
            first_name, last_name = self.parse_name(row.get('Customer Name', ''))
            
            # Parse tags
            tags = self.parse_tags(row.get('tags', ''))
            
            # Prepare contact data
            contact_data = {
                "firstName": first_name,
                "lastName": last_name,
                "name": row.get('Customer Name', ''),
                "email": row.get('email', ''),
                "phone": row.get('phone', ''),
                "source": row.get('source', 'CSV Import'),
                "tags": tags
            }
            
            # Remove empty fields
            contact_data = {k: v for k, v in contact_data.items() if v}
            
            # Validate required fields (email or phone)
            if not contact_data.get('email') and not contact_data.get('phone'):
                logging.error(f"Contact {row.get('Customer Name', '')} has no email or phone")
                return None
            
            url = f"{self.base_url}/contacts/"
            response = requests.post(url, headers=self.headers, json=contact_data)
            
            if response.status_code in [200, 201]:
                contact_response = response.json()
                contact_id = contact_response.get('contact', {}).get('id')
                logging.info(f"Successfully created contact: {row.get('Customer Name', '')} (ID: {contact_id})")
                return contact_id
            else:
                logging.error(f"Failed to create contact {row.get('Customer Name', '')}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"Error creating contact {row.get('Customer Name', '')}: {str(e)}")
            return None
    
    def create_opportunity(self, row: Dict, contact_id: str) -> bool:
        """
        Create an opportunity in GHL
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get pipeline and stage IDs
            pipeline_name = row.get('pipeline', '')
            stage_name = row.get('stage', '')
            
            stage_id, pipeline_id = self.get_stage_id(pipeline_name, stage_name)
            
            if not stage_id or not pipeline_id:
                logging.error(f"Could not find stage ID for pipeline '{pipeline_name}' stage '{stage_name}'")
                return False
            
            # Parse tags
            tags = self.parse_tags(row.get('tags', ''))
            
            # Parse monetary value
            lead_value = 0
            try:
                if row.get('Lead Value'):
                    lead_value = float(row.get('Lead Value', 0))
            except (ValueError, TypeError):
                lead_value = 0
            
            # Prepare opportunity data
            opportunity_data = {
                "title": row.get('Opportunity Name', ''),
                "status": row.get('status', 'open'),
                "stageId": stage_id,
                "contactId": contact_id,
                "monetaryValue": lead_value,
                "source": row.get('source', 'CSV Import'),
                "name": row.get('Customer Name', ''),
                "tags": tags,
                "assignedTo": "GABS47CZATpMX2dGWOFH"  # Hardcoded assignment
            }
            
            # Add notes if available
            if row.get('Notes'):
                # Note: GHL API might not directly support notes in opportunity creation
                # You might need to create a note separately after opportunity creation
                pass
            
            # Remove empty fields
            opportunity_data = {k: v for k, v in opportunity_data.items() if v}
            
            url = f"{self.base_url}/pipelines/{pipeline_id}/opportunities/"
            response = requests.post(url, headers=self.headers, json=opportunity_data)
            
            if response.status_code in [200, 201]:
                opportunity_response = response.json()
                opportunity_id = opportunity_response.get('opportunity', {}).get('id', 'Unknown')
                logging.info(f"Successfully created opportunity: {row.get('Opportunity Name', '')} (ID: {opportunity_id})")
                return True
            else:
                logging.error(f"Failed to create opportunity {row.get('Opportunity Name', '')}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"Error creating opportunity {row.get('Opportunity Name', '')}: {str(e)}")
            return False
    
    def import_csv(self, csv_file_path: str, delay_seconds: float = 1.0) -> Dict[str, int]:
        """
        Import CSV data to GHL
        
        Args:
            csv_file_path: Path to the CSV file
            delay_seconds: Delay between API calls to avoid rate limiting
            
        Returns:
            Dictionary with import statistics
        """
        stats = {
            'total_rows': 0,
            'contacts_created': 0,
            'contacts_failed': 0,
            'opportunities_created': 0,
            'opportunities_failed': 0
        }
        
        # First, fetch all pipelines and stages
        logging.info("Fetching pipelines and stages...")
        self.get_pipelines()
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row_num, row in enumerate(csv_reader, 1):
                    stats['total_rows'] += 1
                    logging.info(f"Processing row {row_num}: {row.get('Customer Name', 'Unknown')}")
                    
                    # Create contact first
                    contact_id = self.create_contact(row)
                    
                    if contact_id:
                        stats['contacts_created'] += 1
                        
                        # Add delay to avoid rate limiting
                        time.sleep(delay_seconds)
                        
                        # Create opportunity
                        if self.create_opportunity(row, contact_id):
                            stats['opportunities_created'] += 1
                        else:
                            stats['opportunities_failed'] += 1
                    else:
                        stats['contacts_failed'] += 1
                        stats['opportunities_failed'] += 1
                    
                    # Add delay between rows
                    time.sleep(delay_seconds)
                    
        except FileNotFoundError:
            logging.error(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            logging.error(f"Error reading CSV file: {str(e)}")
        
        return stats

def main():
    """
    Main function to run the import
    """
    # Configuration - UPDATE THESE VALUES
    API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsb2NhdGlvbl9pZCI6IlNUWXc0RnBvVnViejBCbHlBT3EyIiwidmVyc2lvbiI6MSwiaWF0IjoxNzU2OTA1OTE0ODc1LCJzdWIiOiJEakNNRlVDbVdISjFORTNaUDRITCJ9.8HcmXyBxrwyWgGvLhsAfmU-U84eIUTl49NdzX4Wpxt8"  # Your actual API token
    LOCATION_ID = "STYw4FpoVubz0BlyAOq2"  # Your actual location ID
    CSV_FILE_PATH = r"c:\Users\Dell\Downloads\original_opportunities.csv"
    
    # Initialize importer
    importer = GHLImporter(API_TOKEN, LOCATION_ID)
    
    # Run import
    logging.info("Starting CSV import to GHL...")
    stats = importer.import_csv(CSV_FILE_PATH, delay_seconds=1.5)  # 1.5 second delay to be safe with rate limits
    
    # Print final statistics
    logging.info("Import completed!")
    logging.info(f"Statistics:")
    logging.info(f"  Total rows processed: {stats['total_rows']}")
    logging.info(f"  Contacts created: {stats['contacts_created']}")
    logging.info(f"  Contacts failed: {stats['contacts_failed']}")
    logging.info(f"  Opportunities created: {stats['opportunities_created']}")
    logging.info(f"  Opportunities failed: {stats['opportunities_failed']}")
    
    success_rate = (stats['contacts_created'] / stats['total_rows'] * 100) if stats['total_rows'] > 0 else 0
    logging.info(f"  Success rate: {success_rate:.1f}%")

if __name__ == "__main__":
    main()
