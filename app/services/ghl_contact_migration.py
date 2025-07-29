"""
GoHighLevel V1 API Contact-Only Migration Service
Simplified version that focuses only on contact migration with custom field mapping
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
        logging.FileHandler('ghl_contact_migration.log', mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class GHLContactMigrationService:
    """Simplified migration service focused only on contacts with custom field mapping"""
    
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
        
        # Mapping dictionaries for contact migration
        self.custom_field_mapping: Dict[str, str] = {}  # {child_field_id: master_field_id}
        self.contact_mapping: Dict[str, str] = {}       # {child_contact_id: master_contact_id}
        
        # Migration strategy reports
        self.field_strategy: Dict[str, Any] = {}
        
        # Batch settings - Optimized for contact processing
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '15'))
        self.rate_limit_delay = float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.3'))
        
        # Testing and tagging settings
        self.migration_tag = settings.contact_migration_tag
        self.test_limit = settings.contact_migration_test_limit
        
        logger.info(f"Contact migration initialized with test limit: {self.test_limit}, tag: '{self.migration_tag}'")
        
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
        
        logger.info(f"Found {len(fields)} custom fields in {account_type} account")
        return fields

    async def fetch_contacts(self, client: httpx.AsyncClient, account_type: str) -> List[Dict[str, Any]]:
        """Fetch all contacts from an account using proper GHL V1 API pagination"""
        logger.info(f"Fetching contacts from {account_type} account...")
        
        all_contacts = []
        limit = 100  # Max allowed by API
        start_after_id = None
        start_after = None
        
        while True:
            # Build params for pagination
            params = {"limit": limit}
            
            # Add pagination parameters if we have them
            if start_after_id and start_after:
                params["startAfterId"] = start_after_id
                params["startAfter"] = start_after
            
            logger.info(f"Fetching contacts with params: {params}")
            response = await self._make_request(client, "GET", "/contacts/", params=params)
            
            if not response or "contacts" not in response:
                logger.warning(f"No response or contacts field missing: {response}")
                break
                
            contacts = response["contacts"]
            if not contacts:
                logger.info("No more contacts found, ending pagination")
                break
                
            all_contacts.extend(contacts)
            logger.info(f"Fetched {len(contacts)} contacts (total so far: {len(all_contacts)})")
            
            # Check if we have more pages using meta information
            meta = response.get("meta", {})
            logger.debug(f"Response meta: {meta}")
            
            # Get pagination info for next request
            next_start_after_id = meta.get("startAfterId")
            next_start_after = meta.get("startAfter")
            
            # If we don't have pagination info or got less than limit, we're done
            if not next_start_after_id or not next_start_after or len(contacts) < limit:
                logger.info("Reached end of contacts pagination")
                break
                
            # Update pagination parameters for next request
            start_after_id = next_start_after_id
            start_after = next_start_after
            
            # Small delay between requests to respect rate limits
            await asyncio.sleep(0.5)
            
        logger.info(f"Total contacts fetched from {account_type}: {len(all_contacts)}")
        return all_contacts

    async def sync_custom_fields(self) -> bool:
        """Phase 1: Sync custom fields between accounts"""
        logger.info("=== Phase 1: Custom Fields Mapping ===")
        
        # Fetch custom fields from both accounts
        child_fields = await self.fetch_custom_fields(self.child_client, "child")
        master_fields = await self.fetch_custom_fields(self.master_client, "master")
        
        if not child_fields:
            logger.warning("No custom fields found in child account")
            return True
            
        # Use smart mapping to map fields
        self.field_strategy = self.smart_mapper.create_custom_field_mapping_strategy(child_fields, master_fields)
        
        # Store the mapping for contact processing
        for child_field in child_fields:
            child_field_id = child_field["id"]
            mapped_master_field = self.field_strategy["field_mappings"].get(child_field_id)
            if mapped_master_field:
                self.custom_field_mapping[child_field_id] = mapped_master_field
                logger.info(f"Mapped field '{child_field['name']}' -> master field")
        
        logger.info(f"Custom fields mapping completed. Mapped {len(self.custom_field_mapping)} fields.")
        return True

    async def _find_contact_by_email(self, email: str) -> Optional[str]:
        """Find contact in master account by email using query parameter"""
        if not email or not email.strip():
            return None
            
        search_email = email.strip()
        params = {"query": search_email, "limit": 10}  # Use query param to search
        response = await self._make_request(self.master_client, "GET", "/contacts/", params=params)
        
        logger.debug(f"Email search for '{email}' returned: {response}")
        
        if response and "contacts" in response and response["contacts"]:
            # Check each returned contact for exact email match
            for contact in response["contacts"]:
                contact_email = contact.get("email", "").strip().lower()
                search_email_lower = search_email.lower()
                
                if contact_email == search_email_lower:
                    logger.info(f"Found exact email match for {email} with ID: {contact['id']}")
                    return contact["id"]
            
            # If no exact match found, log this
            logger.debug(f"Email search returned {len(response['contacts'])} contacts but none matched exactly: {email}")
            
        logger.debug(f"No existing contact found for email: {email}")
        return None

    async def _find_contact_by_phone(self, phone: str) -> Optional[str]:
        """Find contact in master account by phone using query parameter"""
        if not phone or not phone.strip():
            return None
            
        # Clean phone number
        clean_phone = phone.strip()
        params = {"query": clean_phone, "limit": 10}  # Use query param to search
        response = await self._make_request(self.master_client, "GET", "/contacts/", params=params)
        
        logger.debug(f"Phone search for '{phone}' returned: {response}")
        
        if response and "contacts" in response and response["contacts"]:
            # Check each returned contact for exact phone match
            for contact in response["contacts"]:
                contact_phone = contact.get("phone", "").strip()
                
                if contact_phone == clean_phone:
                    logger.info(f"Found exact phone match for {phone} with ID: {contact['id']}")
                    return contact["id"]
                    
            # If no exact match found, log this
            logger.debug(f"Phone search returned {len(response['contacts'])} contacts but none matched exactly: {phone}")
            
        logger.debug(f"No existing contact found for phone: {phone}")
        return None

    def _map_contact_custom_fields(self, contact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map custom fields from child to master account using field mapping"""
        mapped_contact = contact_data.copy()
        
        # Map custom fields if they exist
        if "customFields" in contact_data and contact_data["customFields"]:
            mapped_custom_fields = []
            
            for field_data in contact_data["customFields"]:
                child_field_id = field_data.get("id")
                if child_field_id and child_field_id in self.custom_field_mapping:
                    # Map to master field ID
                    mapped_field = field_data.copy()
                    mapped_field["id"] = self.custom_field_mapping[child_field_id]
                    mapped_custom_fields.append(mapped_field)
                    logger.debug(f"Mapped custom field {child_field_id} -> {self.custom_field_mapping[child_field_id]}")
        
            mapped_contact["customFields"] = mapped_custom_fields
            
        return mapped_contact

    async def _create_contact(self, contact_data: Dict[str, Any]) -> Optional[str]:
        """Create a new contact in master account with mapped custom fields and custom tag"""
        
        # Map custom fields before creating
        mapped_contact = self._map_contact_custom_fields(contact_data)
        
        # Clean up lastName field - remove phone number if concatenated
        if "lastName" in mapped_contact:
            last_name = mapped_contact["lastName"]
            if " - " in last_name:
                # Split and take only the name part before " - "
                mapped_contact["lastName"] = last_name.split(" - ")[0].strip()
        
        # Remove fields that shouldn't be copied
        fields_to_remove = ["id", "dateAdded", "dateUpdated", "locationId"]
        for field in fields_to_remove:
            mapped_contact.pop(field, None)
        
        # Add custom migration tag if specified
        if self.migration_tag:
            existing_tags = mapped_contact.get("tags", [])
            if isinstance(existing_tags, list):
                # Add migration tag if not already present
                if self.migration_tag not in existing_tags:
                    existing_tags.append(self.migration_tag)
                    mapped_contact["tags"] = existing_tags
            else:
                # If tags is not a list, create new list with migration tag
                mapped_contact["tags"] = [self.migration_tag]
            
            logger.info(f"Added migration tag '{self.migration_tag}' to contact")
        
        response = await self._make_request(self.master_client, "POST", "/contacts", data=mapped_contact)
        
        if response and "contact" in response:
            contact_id = response["contact"]["id"]
            contact_name = mapped_contact.get("firstName", "") + " " + mapped_contact.get("lastName", "")
            tag_info = f" with tag '{self.migration_tag}'" if self.migration_tag else ""
            logger.info(f"Created contact '{contact_name.strip()}' with ID: {contact_id}{tag_info}")
            return contact_id
        else:
            logger.error(f"Failed to create contact: {response}")
            return None

    async def _process_single_contact(self, contact_data: Dict[str, Any]) -> Optional[str]:
        """Process a single contact - check if exists or create new with proper field mapping"""
        email = contact_data.get("email")
        phone = contact_data.get("phone")
        
        # Try to find existing contact by email or phone
        if email:
            existing = await self._find_contact_by_email(email)
            if existing:
                return existing
                
        if phone:
            existing = await self._find_contact_by_phone(phone)
            if existing:
                return existing
                
        # Create new contact with mapped custom fields
        return await self._create_contact(contact_data)

    async def sync_contacts(self) -> Dict[str, Any]:
        """Phase 2: Sync contacts with custom field mapping"""
        logger.info("=== Phase 2: Contact Migration with Custom Field Mapping ===")
        
        start_time = datetime.now()
        
        # Results tracking
        results = {
            "success": True,
            "total_processed": 0,
            "mapped_contacts": 0,
            "skipped_contacts": 0,
            "failed_contacts": 0,
            "errors": [],
            "start_time": start_time.isoformat()
        }
        
        try:
            # First, verify master account state
            master_verification = await self.verify_master_account_state()
            logger.info(f"Master account has {master_verification['total_contacts']} existing contacts")
            
            if master_verification['total_contacts'] > 0:
                logger.warning(f"Master account is not empty! Found {master_verification['total_contacts']} existing contacts:")
                for contact in master_verification['contacts_found']:
                    logger.warning(f"  - {contact}")
            
            # Fetch contacts from child account
            child_contacts = await self.fetch_contacts(self.child_client, "child")
            
            if not child_contacts:
                logger.info("No contacts found in child account to migrate")
                results["total_processed"] = 0
                results["end_time"] = datetime.now().isoformat()
                return results
            
            # Apply test limit if set
            if self.test_limit > 0:
                original_count = len(child_contacts)
                child_contacts = child_contacts[:self.test_limit]
                logger.info(f"TEST MODE: Limited contacts from {original_count} to {len(child_contacts)} contacts")
            
            logger.info(f"Starting migration of {len(child_contacts)} contacts...")
            if self.migration_tag:
                logger.info(f"All contacts will be tagged with: '{self.migration_tag}'")
            
            self._update_progress("contacts", 0, len(child_contacts), "Starting contact migration")
            
            # Process contacts in batches with improved rate limiting
            total_contacts = len(child_contacts)
            processed_count = 0
            
            for i in range(0, len(child_contacts), self.batch_size):
                batch = child_contacts[i:i + self.batch_size]
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
                            results["mapped_contacts"] += 1
                        else:
                            results["skipped_contacts"] += 1
                        
                        processed_count += 1
                        results["total_processed"] = processed_count
                        
                        # Update progress more frequently
                        if processed_count % 5 == 0 or processed_count == total_contacts:
                            self._update_progress("contacts", processed_count, total_contacts,
                                                f"Processed {processed_count}/{total_contacts} contacts")
                            
                        # Small delay between each contact
                        await asyncio.sleep(self.rate_limit_delay)
                    except Exception as e:
                        logger.error(f"Error processing contact {contact.get('id')}: {e}")
                        results["failed_contacts"] += 1
                        results["errors"].append(f"Contact {contact.get('id')}: {str(e)}")
                        processed_count += 1
                        results["total_processed"] = processed_count
                        
                # Longer delay between batches to respect rate limits
                await asyncio.sleep(self.rate_limit_delay * 3)
                
            end_time = datetime.now()
            results["end_time"] = end_time.isoformat()
            results["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info(f"Contacts sync completed. Mapped {results['mapped_contacts']} contacts.")
            return results
            
        except Exception as e:
            logger.error(f"Contact migration failed: {str(e)}")
            results["success"] = False
            results["error"] = str(e)
            results["end_time"] = datetime.now().isoformat()
            return results

    async def run_contact_migration(self) -> Dict[str, Any]:
        """Run the simplified contact-only migration process"""
        logger.info("=== Starting Contact-Only Migration ===")
        start_time = datetime.now()
        
        migration_results = {
            "start_time": start_time.isoformat(),
            "custom_fields": {"status": "pending", "mapped_count": 0},
            "contacts": {"status": "pending", "mapped_count": 0},
            "smart_mapping_report": {},
            "errors": []
        }
        
        try:
            # Phase 1: Map Custom Fields
            logger.info("Phase 1: Analyzing and mapping custom fields...")
            self._update_progress("custom_fields", 0, 1, "Mapping custom fields...")
            
            if await self.sync_custom_fields():
                migration_results["custom_fields"]["status"] = "completed"
                migration_results["custom_fields"]["mapped_count"] = len(self.custom_field_mapping)
                self._update_progress("custom_fields", 1, 1, f"Mapped {len(self.custom_field_mapping)} custom fields")
            else:
                migration_results["custom_fields"]["status"] = "failed"
                
            # Phase 2: Migrate Contacts
            logger.info("Phase 2: Migrating contacts with custom field mapping...")
            if await self.sync_contacts():
                migration_results["contacts"]["status"] = "completed"
                migration_results["contacts"]["mapped_count"] = len(self.contact_mapping)
            else:
                migration_results["contacts"]["status"] = "failed"
                
        except Exception as e:
            logger.error(f"Contact migration failed with error: {str(e)}")
            migration_results["errors"].append(str(e))
            
        finally:
            await self.close()
            
        # Generate smart mapping report for custom fields only
        migration_results["smart_mapping_report"] = {
            "field_details": self.field_strategy,
            "summary": {
                "fields_mapped": len(self.custom_field_mapping),
                "fields_unmapped": len([f for f in self.field_strategy.get("unmapped_fields", [])]),
                "contacts_migrated": len(self.contact_mapping)
            }
        }
            
        end_time = datetime.now()
        migration_results["end_time"] = end_time.isoformat()
        migration_results["duration_minutes"] = (end_time - start_time).total_seconds() / 60
        
        logger.info("=== Contact Migration Complete ===")
        logger.info(f"Duration: {migration_results['duration_minutes']:.2f} minutes")
        logger.info(f"Results: {json.dumps(migration_results, indent=2)}")
        
        return migration_results

    def _generate_sample_mappings(self, field_strategy: Dict[str, Any], 
                                 child_fields: List[Dict[str, Any]], 
                                 master_fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate sample mappings for preview display"""
        sample_mappings = []
        field_mappings = field_strategy.get("field_mappings", {})
        
        # Show up to 5 sample mappings
        count = 0
        for child_field_id, master_field_id in field_mappings.items():
            if count >= 5:
                break
                
            child_field = next((f for f in child_fields if f["id"] == child_field_id), None)
            master_field = next((f for f in master_fields if f["id"] == master_field_id), None)
            
            if child_field and master_field:
                # Calculate confidence based on similarity
                confidence = self.smart_mapper.calculate_similarity(child_field["name"], master_field["name"]) * 100
                
                sample_mappings.append({
                    "child_field": child_field["name"],
                    "master_field": master_field["name"],
                    "confidence": round(confidence)
                })
                count += 1
        
        return sample_mappings

    async def preview_contact_migration(self) -> Dict[str, Any]:
        """Preview what contacts and custom fields would be migrated"""
        logger.info("Generating contact migration preview...")
        
        # Fetch data for preview
        child_custom_fields = await self.fetch_custom_fields(self.child_client, "child")
        master_custom_fields = await self.fetch_custom_fields(self.master_client, "master")
        child_contacts = await self.fetch_contacts(self.child_client, "child")
        
        await self.close()
        
        # Apply test limit for preview
        total_contacts = len(child_contacts)
        contacts_to_process = total_contacts
        if self.test_limit > 0 and total_contacts > self.test_limit:
            contacts_to_process = self.test_limit
        
        # Analyze field mapping potential
        field_strategy = self.smart_mapper.create_custom_field_mapping_strategy(child_custom_fields, master_custom_fields)
        
        # Calculate mapping stats
        child_field_names = {field["name"] for field in child_custom_fields}
        master_field_names = {field["name"] for field in master_custom_fields}
        fields_to_map = len(field_strategy.get("field_mappings", {}))
        fields_unmapped = len(field_strategy.get("unmapped_fields", []))
        
        return {
            "child_account_summary": {
                "custom_fields": len(child_custom_fields),
                "contacts": total_contacts
            },
            "master_account_summary": {
                "custom_fields": len(master_custom_fields)
            },
            "migration_plan": {
                "total_contacts_available": total_contacts,
                "contacts_to_migrate": contacts_to_process,
                "test_mode": self.test_limit > 0,
                "test_limit": self.test_limit if self.test_limit > 0 else None,
                "migration_tag": self.migration_tag if self.migration_tag else "None",
                "custom_fields_to_map": fields_to_map,
                "custom_fields_unmapped": fields_unmapped,
                "estimated_duration_minutes": (contacts_to_process * 0.5) / 60  # Rough estimate
            },
            "field_mapping_preview": {
                "custom_fields_to_map": fields_to_map,
                "mapped_fields": len(field_strategy.get("field_mappings", {})),
                "unmapped_fields": field_strategy.get("unmapped_fields", []),
                "mapping_confidence": "high" if fields_to_map > fields_unmapped else "medium",
                "mapping_accuracy_percentage": round((fields_to_map / max(len(child_custom_fields), 1)) * 100),
                "sample_mappings": self._generate_sample_mappings(field_strategy, child_custom_fields, master_custom_fields)
            }
        }

    async def verify_master_account_state(self) -> Dict[str, Any]:
        """Verify the current state of the master account"""
        logger.info("Verifying master account state...")
        
        # Fetch all contacts from master account using the new method
        master_contacts = await self.fetch_contacts(self.master_client, "master")
        
        verification = {
            "total_contacts": len(master_contacts),
            "contacts_found": []
        }
        
        # Show first 10 contacts if any exist
        for contact in master_contacts[:10]:
            verification["contacts_found"].append({
                "id": contact.get("id"),
                "email": contact.get("email"),
                "phone": contact.get("phone"),
                "firstName": contact.get("firstName"),
                "lastName": contact.get("lastName"),
                "dateAdded": contact.get("dateAdded")
            })
            
        logger.info(f"Master account verification: {verification['total_contacts']} contacts found")
        return verification

    async def get_migration_preview(self) -> Dict[str, Any]:
        """Get a preview of contact migration without actually migrating"""
        logger.info("Generating contact migration preview...")
        
        try:
            # Analyze custom fields
            field_analysis = await self.analyze_custom_fields()
            
            # Get child contacts count
            child_contacts = await self.fetch_contacts(self.child_client, "child")
            
            # Get master contacts count for comparison
            master_contacts = await self.fetch_contacts(self.master_client, "master")
            
            preview_data = {
                "field_mapping": field_analysis,
                "total_contacts": len(child_contacts),
                "master_contacts_existing": len(master_contacts),
                "contacts_preview": child_contacts[:5] if child_contacts else [],  # Show first 5 as preview
                "test_mode": self.test_limit > 0,
                "test_limit": self.test_limit if self.test_limit > 0 else None,
                "migration_tag": self.migration_tag
            }
            
            # Apply test limit to preview
            if self.test_limit > 0:
                preview_data["total_contacts"] = min(preview_data["total_contacts"], self.test_limit)
            
            return {
                "success": True,
                "preview": preview_data
            }
            
        except Exception as e:
            logger.error(f"Error generating contact migration preview: {str(e)}")
            return {"success": False, "error": str(e)}

    async def analyze_custom_fields(self) -> Dict[str, Any]:
        """Analyze custom fields between child and master accounts for mapping"""
        logger.info("Analyzing custom fields for mapping...")
        
        try:
            # Fetch custom fields from both accounts
            child_fields = await self.fetch_custom_fields(self.child_client, "child")
            master_fields = await self.fetch_custom_fields(self.master_client, "master")
            
            logger.info(f"Found {len(child_fields)} custom fields in child account")
            logger.info(f"Found {len(master_fields)} custom fields in master account")
            
            # Create mapping analysis
            field_analysis = {
                "child_fields": child_fields,
                "master_fields": master_fields,
                "mapped_fields": [],
                "unmapped_child_fields": [],
                "unmapped_master_fields": master_fields.copy(),
                "mapping_suggestions": []
            }
            
            # Map fields using smart mapping
            for child_field in child_fields:
                best_match = None
                best_similarity = 0.0
                
                for master_field in master_fields:
                    similarity = self.smart_mapper.calculate_similarity(
                        child_field.get("name", ""),
                        master_field.get("name", "")
                    )
                    
                    if similarity > best_similarity and similarity >= 0.8:  # 80% similarity threshold
                        best_similarity = similarity
                        best_match = master_field
                
                if best_match:
                    # Found a good match
                    field_analysis["mapped_fields"].append({
                        "child_field": child_field,
                        "master_field": best_match,
                        "similarity": best_similarity
                    })
                    
                    # Add to mapping dictionary
                    self.custom_field_mapping[child_field["id"]] = best_match["id"]
                    
                    # Remove from unmapped master fields
                    field_analysis["unmapped_master_fields"] = [
                        f for f in field_analysis["unmapped_master_fields"] 
                        if f["id"] != best_match["id"]
                    ]
                    
                    logger.info(f"Mapped field '{child_field.get('name')}' -> '{best_match.get('name')}' (similarity: {best_similarity:.2f})")
                else:
                    # No good match found
                    field_analysis["unmapped_child_fields"].append(child_field)
                    
                    # Suggest creating new field or best partial matches
                    suggestions = []
                    for master_field in master_fields:
                        similarity = self.smart_mapper.calculate_similarity(
                            child_field.get("name", ""),
                            master_field.get("name", "")
                        )
                        if similarity > 0.3:  # Show suggestions above 30% similarity
                            suggestions.append({
                                "master_field": master_field,
                                "similarity": similarity
                            })
                    
                    suggestions.sort(key=lambda x: x["similarity"], reverse=True)
                    field_analysis["mapping_suggestions"].append({
                        "child_field": child_field,
                        "suggestions": suggestions[:3]  # Top 3 suggestions
                    })
            
            logger.info(f"Field mapping analysis complete: {len(field_analysis['mapped_fields'])} mapped, {len(field_analysis['unmapped_child_fields'])} unmapped")
            
            return {
                "success": True,
                "analysis": field_analysis,
                "total_child_fields": len(child_fields),
                "total_master_fields": len(master_fields),
                "mapped_fields_count": len(field_analysis["mapped_fields"]),
                "unmapped_fields_count": len(field_analysis["unmapped_child_fields"])
            }
            
        except Exception as e:
            logger.error(f"Error analyzing custom fields: {str(e)}")
            return {
                "success": False,
                "error": f"Failed to analyze custom fields: {str(e)}"
            }

    async def close(self):
        """Close HTTP clients"""
        await self.child_client.aclose()
        await self.master_client.aclose()
