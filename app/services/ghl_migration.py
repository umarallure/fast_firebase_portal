"""
GoHighLevel V1 API Migration Service
Migrates contacts, custom fields, and opportunities from child account to master account
"""

import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from app.config import settings
from app.services.smart_mapping import SmartMappingStrategy
import os
from datetime import datetime
import json

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ghl_migration.log', mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class GHLMigrationService:
    def __init__(self, child_api_key: str, master_api_key: str, progress_callback=None):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.child_client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {child_api_key}", "Content-Type": "application/json"},
            timeout=settings.ghl_api_timeout
        )
        self.master_client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {master_api_key}", "Content-Type": "application/json"},
            timeout=settings.ghl_api_timeout
        )
        
        # Smart mapping strategy
        self.smart_mapper = SmartMappingStrategy()
        
        # Progress tracking callback
        self.progress_callback = progress_callback
        
        # Mapping dictionaries to track IDs between accounts
        self.custom_field_mapping: Dict[str, str] = {}  # {child_field_id: master_field_id}
        self.pipeline_mapping: Dict[str, str] = {}      # {child_pipeline_id: master_pipeline_id}
        self.stage_mapping: Dict[str, str] = {}         # {child_stage_id: master_stage_id}
        self.contact_mapping: Dict[str, str] = {}       # {child_contact_id: master_contact_id}
        
        # Migration strategy reports
        self.pipeline_strategy: Dict[str, Any] = {}
        self.field_strategy: Dict[str, Any] = {}
        
        # Batch settings - Use smaller batches for better rate limiting
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '20'))
        self.rate_limit_delay = float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.2'))
        
    def _update_progress(self, stage: str, current: int, total: int, message: str = ""):
        """Update progress if callback is provided"""
        if self.progress_callback:
            self.progress_callback({
                "stage": stage,
                "current": current,
                "total": total,
                "percentage": (current / total * 100) if total > 0 else 0,
                "message": message
            })

    async def close(self):
        """Close HTTP clients"""
        await self.child_client.aclose()
        await self.master_client.aclose()

    async def _make_request(self, client: httpx.AsyncClient, method: str, endpoint: str, 
                          data: Optional[Dict] = None, params: Optional[Dict] = None,
                          max_retries: int = 3) -> Optional[Dict]:
        """Make an HTTP request with error handling and exponential backoff for rate limiting"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(max_retries + 1):
            try:
                if method.upper() == "GET":
                    response = await client.get(url, params=params)
                elif method.upper() == "POST":
                    response = await client.post(url, json=data, params=params)
                elif method.upper() == "PUT":
                    response = await client.put(url, json=data, params=params)
                elif method.upper() == "PATCH":
                    response = await client.patch(url, json=data, params=params)
                elif method.upper() == "DELETE":
                    response = await client.delete(url, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    if attempt < max_retries:
                        # Exponential backoff: 1s, 2s, 4s
                        backoff_delay = (2 ** attempt) * 1.0
                        logger.warning(f"Rate limited (429). Retrying in {backoff_delay}s... (attempt {attempt + 1}/{max_retries + 1})")
                        await asyncio.sleep(backoff_delay)
                        continue
                    else:
                        logger.error(f"Rate limited (429) after {max_retries} retries. Giving up on {method} {endpoint}")
                        return None
                
                response.raise_for_status()
                await asyncio.sleep(self.rate_limit_delay)  # Normal rate limiting
                
                return response.json() if response.content else {}
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Already handled above
                    continue
                logger.error(f"{method} {endpoint} failed with status {e.response.status_code}: {e.response.text}")
                return None
            except Exception as e:
                logger.error(f"{method} {endpoint} failed: {str(e)}")
                if attempt < max_retries:
                    await asyncio.sleep(1.0)  # Brief delay before retry
                    continue
                return None
        
        return None

    async def fetch_custom_fields(self, client: httpx.AsyncClient, account_type: str) -> List[Dict[str, Any]]:
        """Fetch custom fields from an account"""
        logger.info(f"Fetching custom fields from {account_type} account...")
        
        # Try contact custom fields first
        contact_fields = await self._make_request(client, "GET", "/custom-fields") or {}
        fields = contact_fields.get("customFields", [])
        
        # Also try opportunity custom fields if available
        opp_fields = await self._make_request(client, "GET", "/custom-fields/opportunity") or {}
        opp_fields_list = opp_fields.get("customFields", [])
        
        # Mark opportunity fields
        for field in opp_fields_list:
            field["isOpportunityField"] = True
            
        fields.extend(opp_fields_list)
        
        logger.info(f"Found {len(fields)} custom fields in {account_type} account")
        return fields

    async def create_custom_field(self, field_data: Dict[str, Any]) -> Optional[str]:
        """Create a custom field in master account"""
        create_data = {
            "name": field_data["name"],
            "type": field_data.get("type", "TEXT")
        }
        
        # Add options for select fields
        if field_data.get("options"):
            create_data["options"] = field_data["options"]
            
        # Determine endpoint based on field type
        endpoint = "/custom-fields"
        if field_data.get("isOpportunityField"):
            endpoint = "/custom-fields/opportunity"
            
        result = await self._make_request(self.master_client, "POST", endpoint, create_data)
        
        if result and "customField" in result:
            field_id = result["customField"]["id"]
            logger.info(f"Created custom field '{field_data['name']}' with ID: {field_id}")
            return field_id
        
        logger.error(f"Failed to create custom field: {field_data['name']}")
        return None

    async def sync_custom_fields(self) -> bool:
        """Sync custom fields from child to master account with smart mapping"""
        logger.info("Starting custom fields synchronization with smart mapping...")
        
        # Fetch fields from both accounts
        child_fields = await self.fetch_custom_fields(self.child_client, "child")
        master_fields = await self.fetch_custom_fields(self.master_client, "master")
        
        # Create smart mapping strategy
        self.field_strategy = self.smart_mapper.create_custom_field_mapping_strategy(child_fields, master_fields)
        
        # Apply existing mappings first
        for child_field_id, master_field_id in self.field_strategy["field_mappings"].items():
            self.custom_field_mapping[child_field_id] = master_field_id
            
        # Create missing fields
        for unmapped_field in self.field_strategy["unmapped_fields"]:
            master_field_id = await self.create_custom_field(unmapped_field)
            if master_field_id:
                self.custom_field_mapping[unmapped_field["id"]] = master_field_id
                logger.info(f"Created and mapped field '{unmapped_field['name']}'")
            else:
                logger.error(f"Failed to create field '{unmapped_field['name']}'")
                
        logger.info(f"Custom fields sync completed. Mapped {len(self.custom_field_mapping)} fields.")
        logger.info(f"Smart mapping report: {len(self.field_strategy['field_mappings'])} auto-mapped, {len(self.field_strategy['unmapped_fields'])} created")
        return True

    async def fetch_pipelines(self, client: httpx.AsyncClient, account_type: str) -> List[Dict[str, Any]]:
        """Fetch pipelines from an account"""
        logger.info(f"Fetching pipelines from {account_type} account...")
        
        result = await self._make_request(client, "GET", "/pipelines")
        pipelines = result.get("pipelines", []) if result else []
        
        logger.info(f"Found {len(pipelines)} pipelines in {account_type} account")
        return pipelines

    async def create_pipeline_stage(self, pipeline_id: str, stage_data: Dict[str, Any]) -> Optional[str]:
        """Create a stage in a pipeline"""
        create_data = {
            "name": stage_data["name"],
            "position": stage_data.get("position", 0)
        }
        
        result = await self._make_request(
            self.master_client, 
            "POST", 
            f"/pipelines/{pipeline_id}/stages", 
            create_data
        )
        
        if result and "stage" in result:
            stage_id = result["stage"]["id"]
            logger.info(f"Created stage '{stage_data['name']}' with ID: {stage_id}")
            return stage_id
        
        logger.error(f"Failed to create stage: {stage_data['name']}")
        return None

    async def sync_pipelines_and_stages(self) -> bool:
        """Sync pipelines and stages from child to master account with smart mapping"""
        logger.info("Starting pipelines and stages synchronization with smart mapping...")
        
        # Fetch pipelines from both accounts
        child_pipelines = await self.fetch_pipelines(self.child_client, "child")
        master_pipelines = await self.fetch_pipelines(self.master_client, "master")
        
        # Create smart mapping strategy
        self.pipeline_strategy = self.smart_mapper.create_pipeline_mapping_strategy(child_pipelines, master_pipelines)
        
        # Apply pipeline mappings
        for child_pipeline_id, master_pipeline_id in self.pipeline_strategy["pipeline_mappings"].items():
            self.pipeline_mapping[child_pipeline_id] = master_pipeline_id
            
        # Apply stage mappings
        for child_stage_id, master_stage_id in self.pipeline_strategy["stage_mappings"].items():
            self.stage_mapping[child_stage_id] = master_stage_id
            
        # Create missing stages for mapped pipelines
        for unmapped_stage in self.pipeline_strategy["unmapped_stages"]:
            master_pipeline_id = unmapped_stage["master_pipeline_id"]
            stage_data = unmapped_stage["child_stage"]
            
            master_stage_id = await self.create_pipeline_stage(master_pipeline_id, stage_data)
            if master_stage_id:
                self.stage_mapping[stage_data["id"]] = master_stage_id
                logger.info(f"Created and mapped stage '{stage_data['name']}'")
            else:
                logger.error(f"Failed to create stage '{stage_data['name']}'")
                
        logger.info(f"Pipelines sync completed. Mapped {len(self.pipeline_mapping)} pipelines, {len(self.stage_mapping)} stages.")
        logger.info(f"Smart mapping report: {len(self.pipeline_strategy['pipeline_mappings'])} pipelines mapped, {len(self.pipeline_strategy['unmapped_pipelines'])} unmapped")
        
        # Log unmapped pipelines as warnings
        for unmapped_pipeline in self.pipeline_strategy["unmapped_pipelines"]:
            logger.warning(f"Pipeline '{unmapped_pipeline['name']}' could not be mapped - opportunities in this pipeline may fail to migrate")
            
        return True

    async def fetch_contacts(self, client: httpx.AsyncClient, account_type: str) -> List[Dict[str, Any]]:
        """Fetch all contacts from an account with pagination"""
        logger.info(f"Fetching contacts from {account_type} account...")
        
        contacts = []
        page = 1
        limit = 100
        
        while True:
            params = {"page": page, "limit": limit}
            result = await self._make_request(client, "GET", "/contacts", params=params)
            
            if not result or "contacts" not in result:
                break
                
            batch = result["contacts"]
            if not batch:
                break
                
            contacts.extend(batch)
            logger.info(f"Fetched page {page}: {len(batch)} contacts")
            
            # Check if there are more pages
            if len(batch) < limit:
                break
                
            page += 1
            
        logger.info(f"Total contacts fetched from {account_type}: {len(contacts)}")
        return contacts

    def map_custom_field_values(self, child_custom_fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map custom field values from child to master field IDs"""
        mapped_fields = []
        
        for field in child_custom_fields:
            child_field_id = field.get("fieldId")
            field_value = field.get("value")
            
            if child_field_id in self.custom_field_mapping:
                master_field_id = self.custom_field_mapping[child_field_id]
                mapped_fields.append({
                    "fieldId": master_field_id,
                    "value": field_value
                })
            else:
                logger.warning(f"Custom field ID {child_field_id} not found in mapping")
                
        return mapped_fields

    async def create_contact(self, contact_data: Dict[str, Any]) -> Optional[str]:
        """Create a contact in master account"""
        # Prepare contact data
        create_data = {
            "firstName": contact_data.get("firstName", ""),
            "lastName": contact_data.get("lastName", ""),
            "email": contact_data.get("email"),
            "phone": contact_data.get("phone"),
            "address1": contact_data.get("address1"),
            "city": contact_data.get("city"),
            "state": contact_data.get("state"),
            "postalCode": contact_data.get("postalCode"),
            "country": contact_data.get("country", "US"),
        }
        
        # Remove None values
        create_data = {k: v for k, v in create_data.items() if v is not None}
        
        # Map custom fields
        if contact_data.get("customFields"):
            create_data["customFields"] = self.map_custom_field_values(contact_data["customFields"])
            
        result = await self._make_request(self.master_client, "POST", "/contacts", create_data)
        
        if result and "contact" in result:
            contact_id = result["contact"]["id"]
            logger.info(f"Created contact '{contact_data.get('firstName', '')} {contact_data.get('lastName', '')}' with ID: {contact_id}")
            return contact_id
        
        logger.error(f"Failed to create contact: {contact_data.get('email', 'No email')}")
        return None

    async def sync_contacts(self) -> bool:
        """Sync contacts from child to master account with optimized processing"""
        logger.info("Starting contacts synchronization with smart processing...")
        
        # Fetch contacts from child account
        child_contacts = await self.fetch_contacts(self.child_client, "child")
        
        # Optimize contact processing order
        optimized_contacts = self.smart_mapper.optimize_contact_processing_order(child_contacts)
        logger.info(f"Optimized processing order for {len(optimized_contacts)} contacts")
        
        # Process contacts in batches with improved rate limiting
        total_contacts = len(optimized_contacts)
        processed_count = 0
        
        for i in range(0, len(optimized_contacts), self.batch_size):
            batch = optimized_contacts[i:i + self.batch_size]
            batch_num = i//self.batch_size + 1
            total_batches = (total_contacts + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Processing contacts batch {batch_num}/{total_batches}: {len(batch)} contacts")
            self._update_progress("contacts", processed_count, total_contacts, 
                                f"Processing batch {batch_num}/{total_batches}")
            
            # Process contacts sequentially within batch to avoid rate limits
            for j, contact in enumerate(batch):
                try:
                    result = await self._process_single_contact(contact)
                    if result:
                        child_contact_id = contact["id"]
                        self.contact_mapping[child_contact_id] = result
                    
                    processed_count += 1
                    # Update progress more frequently
                    if processed_count % 5 == 0 or processed_count == total_contacts:
                        self._update_progress("contacts", processed_count, total_contacts,
                                            f"Processed {processed_count}/{total_contacts} contacts")
                        
                    # Small delay between each contact
                    await asyncio.sleep(self.rate_limit_delay)
                except Exception as e:
                    logger.error(f"Error processing contact {contact.get('id')}: {e}")
                    processed_count += 1
                    
            # Longer delay between batches to respect rate limits
            await asyncio.sleep(self.rate_limit_delay * 5)
            
        logger.info(f"Contacts sync completed. Mapped {len(self.contact_mapping)} contacts.")
        return True

    async def _process_single_contact(self, contact_data: Dict[str, Any]) -> Optional[str]:
        """Process a single contact - check if exists or create new"""
        email = contact_data.get("email")
        phone = contact_data.get("phone")
        
        # Try to find existing contact by email or phone
        if email:
            existing = await self._find_contact_by_email(email)
            if existing:
                logger.info(f"Contact with email {email} already exists with ID: {existing}")
                return existing
                
        if phone:
            existing = await self._find_contact_by_phone(phone)
            if existing:
                logger.info(f"Contact with phone {phone} already exists with ID: {existing}")
                return existing
        
        # Create new contact
        return await self.create_contact(contact_data)

    async def _find_contact_by_email(self, email: str) -> Optional[str]:
        """Find contact by email in master account"""
        params = {"email": email}
        result = await self._make_request(self.master_client, "GET", "/contacts", params=params)
        
        if result and "contacts" in result and result["contacts"]:
            return result["contacts"][0]["id"]
        return None

    async def _find_contact_by_phone(self, phone: str) -> Optional[str]:
        """Find contact by phone in master account"""
        params = {"phone": phone}
        result = await self._make_request(self.master_client, "GET", "/contacts", params=params)
        
        if result and "contacts" in result and result["contacts"]:
            return result["contacts"][0]["id"]
        return None

    async def fetch_opportunities(self, client: httpx.AsyncClient, account_type: str) -> List[Dict[str, Any]]:
        """Fetch all opportunities from an account"""
        logger.info(f"Fetching opportunities from {account_type} account...")
        
        opportunities = []
        page = 1
        limit = 100
        
        while True:
            params = {"page": page, "limit": limit}
            result = await self._make_request(client, "GET", "/opportunities", params=params)
            
            if not result or "opportunities" not in result:
                break
                
            batch = result["opportunities"]
            if not batch:
                break
                
            opportunities.extend(batch)
            logger.info(f"Fetched page {page}: {len(batch)} opportunities")
            
            # Check if there are more pages
            if len(batch) < limit:
                break
                
            page += 1
            
        logger.info(f"Total opportunities fetched from {account_type}: {len(opportunities)}")
        return opportunities

    async def create_opportunity(self, opportunity_data: Dict[str, Any]) -> Optional[str]:
        """Create an opportunity in master account"""
        child_contact_id = opportunity_data.get("contactId")
        child_pipeline_id = opportunity_data.get("pipelineId")
        child_stage_id = opportunity_data.get("pipelineStageId")
        
        # Map IDs to master account
        master_contact_id = self.contact_mapping.get(child_contact_id)
        master_pipeline_id = self.pipeline_mapping.get(child_pipeline_id)
        master_stage_id = self.stage_mapping.get(child_stage_id)
        
        if not master_contact_id:
            logger.error(f"Contact ID {child_contact_id} not found in mapping")
            return None
            
        if not master_pipeline_id:
            logger.error(f"Pipeline ID {child_pipeline_id} not found in mapping")
            return None
            
        if not master_stage_id:
            logger.error(f"Stage ID {child_stage_id} not found in mapping")
            return None
        
        # Prepare opportunity data
        create_data = {
            "name": opportunity_data.get("name", "Migrated Opportunity"),
            "pipelineId": master_pipeline_id,
            "stageId": master_stage_id,
            "status": opportunity_data.get("status", "open"),
            "value": opportunity_data.get("value", 0),
            "contactId": master_contact_id,
        }
        
        # Map custom fields if present
        if opportunity_data.get("customFields"):
            create_data["customFields"] = self.map_custom_field_values(opportunity_data["customFields"])
            
        result = await self._make_request(self.master_client, "POST", "/opportunities", create_data)
        
        if result and "opportunity" in result:
            opportunity_id = result["opportunity"]["id"]
            logger.info(f"Created opportunity '{opportunity_data.get('name', '')}' with ID: {opportunity_id}")
            return opportunity_id
        
        logger.error(f"Failed to create opportunity: {opportunity_data.get('name', 'No name')}")
        return None

    async def sync_opportunities(self) -> bool:
        """Sync opportunities from child to master account"""
        logger.info("Starting opportunities synchronization...")
        
        # Fetch opportunities from child account
        child_opportunities = await self.fetch_opportunities(self.child_client, "child")
        
        # Process opportunities in batches
        created_count = 0
        for i in range(0, len(child_opportunities), self.batch_size):
            batch = child_opportunities[i:i + self.batch_size]
            logger.info(f"Processing opportunities batch {i//self.batch_size + 1}: {len(batch)} opportunities")
            
            # Process each opportunity in the batch
            tasks = []
            for opportunity in batch:
                tasks.append(self.create_opportunity(opportunity))
                
            # Execute batch concurrently but with limited concurrency
            semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
            
            async def limited_task(task):
                async with semaphore:
                    return await task
                    
            results = await asyncio.gather(*[limited_task(task) for task in tasks], return_exceptions=True)
            
            # Count successful creations
            for result in results:
                if isinstance(result, str):  # Success returns opportunity ID
                    created_count += 1
                elif isinstance(result, Exception):
                    logger.error(f"Error creating opportunity: {result}")
                    
            # Small delay between batches
            await asyncio.sleep(self.rate_limit_delay * 2)
            
        logger.info(f"Opportunities sync completed. Created {created_count} opportunities.")
        return True

    async def run_full_migration(self) -> Dict[str, Any]:
        """Run the complete migration process"""
        logger.info("=== Starting Full GHL Account Migration ===")
        start_time = datetime.now()
        
        migration_results = {
            "start_time": start_time.isoformat(),
            "custom_fields": {"status": "pending", "mapped_count": 0},
            "pipelines": {"status": "pending", "mapped_count": 0},
            "contacts": {"status": "pending", "mapped_count": 0},
            "opportunities": {"status": "pending", "created_count": 0},
            "smart_mapping_report": {},
            "errors": []
        }
        
        try:
            # Step 1: Sync Custom Fields
            logger.info("Step 1: Syncing custom fields...")
            if await self.sync_custom_fields():
                migration_results["custom_fields"]["status"] = "completed"
                migration_results["custom_fields"]["mapped_count"] = len(self.custom_field_mapping)
            else:
                migration_results["custom_fields"]["status"] = "failed"
                
            # Step 2: Sync Pipelines and Stages
            logger.info("Step 2: Syncing pipelines and stages...")
            if await self.sync_pipelines_and_stages():
                migration_results["pipelines"]["status"] = "completed"
                migration_results["pipelines"]["mapped_count"] = len(self.pipeline_mapping)
            else:
                migration_results["pipelines"]["status"] = "failed"
                
            # Step 3: Sync Contacts
            logger.info("Step 3: Syncing contacts...")
            if await self.sync_contacts():
                migration_results["contacts"]["status"] = "completed"
                migration_results["contacts"]["mapped_count"] = len(self.contact_mapping)
            else:
                migration_results["contacts"]["status"] = "failed"
                
            # Step 4: Sync Opportunities
            logger.info("Step 4: Syncing opportunities...")
            if await self.sync_opportunities():
                migration_results["opportunities"]["status"] = "completed"
            else:
                migration_results["opportunities"]["status"] = "failed"
                
        except Exception as e:
            logger.error(f"Migration failed with error: {str(e)}")
            migration_results["errors"].append(str(e))
            
        finally:
            await self.close()
            
        # Generate smart mapping report
        migration_results["smart_mapping_report"] = self.smart_mapper.generate_migration_report(
            self.pipeline_strategy, self.field_strategy
        )
            
        end_time = datetime.now()
        migration_results["end_time"] = end_time.isoformat()
        migration_results["duration_minutes"] = (end_time - start_time).total_seconds() / 60
        
        logger.info("=== Migration Complete ===")
        logger.info(f"Duration: {migration_results['duration_minutes']:.2f} minutes")
        logger.info(f"Results: {json.dumps(migration_results, indent=2)}")
        
        return migration_results
