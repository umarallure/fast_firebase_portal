"""
GoHighLevel V1 API Opportunity Migration Service
Phase 2: Smart Opportunity Migration with Pipeline Mapping
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
from difflib import SequenceMatcher

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ghl_opportunity_migration.log', mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class GHLOpportunityMigrationService:
    """Smart opportunity migration service with pipeline and stage mapping"""
    
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
        
        # Mapping dictionaries
        self.pipeline_mapping: Dict[str, str] = {}      # {child_pipeline_id: master_pipeline_id}
        self.stage_mapping: Dict[str, str] = {}         # {child_stage_id: master_stage_id}
        self.contact_mapping: Dict[str, str] = {}       # {child_contact_id: master_contact_id}
        self.opportunity_mapping: Dict[str, str] = {}   # {child_opportunity_id: master_opportunity_id}
        
        # Cache for pipelines and stages
        self.child_pipelines: List[Dict[str, Any]] = []
        self.master_pipelines: List[Dict[str, Any]] = []
        
        # Test mode settings
        self.test_limit = getattr(settings, 'opportunity_migration_test_limit', 5)
        self.migration_tag = getattr(settings, 'opportunity_migration_tag', 'migrated-opportunity')

    def _update_progress(self, stage: str, current: int, total: int, message: str = ""):
        """Update progress through callback"""
        if self.progress_callback:
            self.progress_callback({
                "stage": stage,
                "current": current,
                "total": total,
                "message": message
            })

    async def _make_request(self, client: httpx.AsyncClient, method: str, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic and rate limiting"""
        max_retries = 3
        base_delay = 1.0
        
        # Clear previous error response
        self._last_error_response = None
        
        for attempt in range(max_retries):
            try:
                # Rate limiting
                await asyncio.sleep(settings.migration_rate_limit_delay)
                
                response = await client.request(method, url, **kwargs)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {delay}s before retry {attempt + 1}")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    # Store error response for validation error checking
                    try:
                        self._last_error_response = response.json()
                    except:
                        self._last_error_response = {"error": response.text}
                    return None
                    
            except Exception as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    return None
        
        return None

    async def fetch_pipelines(self, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        """Fetch all pipelines from a location"""
        url = f"{self.base_url}/pipelines/"
        
        logger.info(f"Fetching pipelines from {url}")
        response = await self._make_request(client, "GET", url)
        
        if response and "pipelines" in response:
            pipelines = response["pipelines"]
            logger.info(f"Found {len(pipelines)} pipelines")
            return pipelines
        
        logger.warning("No pipelines found or request failed")
        return []

    async def fetch_opportunities_from_pipeline(self, client: httpx.AsyncClient, pipeline_id: str) -> List[Dict[str, Any]]:
        """Fetch all opportunities from a specific pipeline with proper pagination"""
        all_opportunities = []
        start_after_id = None
        start_after = None
        limit = 100
        page_count = 0
        max_pages = 100  # Safety limit to prevent infinite loops
        
        # Apply safety limit for maximum opportunities
        max_opportunities = settings.opportunity_migration_max_limit
        
        logger.info(f"Fetching opportunities from pipeline {pipeline_id} (max {max_opportunities} opportunities)")
        
        while page_count < max_pages:
            page_count += 1
            
            # Build URL with pagination parameters
            url = f"{self.base_url}/pipelines/{pipeline_id}/opportunities"
            params = {"limit": limit}
            
            if start_after_id:
                params["startAfterId"] = start_after_id
            if start_after:
                params["startAfter"] = start_after
            
            logger.debug(f"Page {page_count}: Fetching opportunities with params: {params}")
            response = await self._make_request(client, "GET", url, params=params)
            
            if not response or "opportunities" not in response:
                logger.warning(f"No opportunities found in pipeline {pipeline_id}")
                break
            
            opportunities = response["opportunities"]
            if not opportunities:
                logger.info(f"No more opportunities to fetch from pipeline {pipeline_id}")
                break
            
            # Check for duplicate IDs to prevent infinite loops
            new_opportunity_ids = {opp.get("id") for opp in opportunities}
            existing_opportunity_ids = {opp.get("id") for opp in all_opportunities}
            
            if new_opportunity_ids.intersection(existing_opportunity_ids):
                logger.warning(f"Detected duplicate opportunities in response, stopping pagination")
                break
            
            all_opportunities.extend(opportunities)
            logger.info(f"Page {page_count}: Fetched {len(opportunities)} opportunities, total: {len(all_opportunities)}")
            
            # Check if we've hit the safety limit
            if len(all_opportunities) >= max_opportunities:
                logger.warning(f"Reached maximum opportunity limit ({max_opportunities}), stopping fetch")
                all_opportunities = all_opportunities[:max_opportunities]
                break
            
            # Set pagination for next request
            if opportunities:
                last_opportunity = opportunities[-1]
                new_start_after_id = last_opportunity.get("id")
                new_start_after = last_opportunity.get("dateAdded")
                
                # Prevent using the same pagination parameters
                if new_start_after_id == start_after_id and new_start_after == start_after:
                    logger.warning(f"Pagination parameters unchanged, stopping to prevent infinite loop")
                    break
                
                start_after_id = new_start_after_id
                start_after = new_start_after
                
                logger.debug(f"Next pagination: startAfterId={start_after_id}, startAfter={start_after}")
            
            # Break if we got fewer than limit (last page)
            if len(opportunities) < limit:
                logger.info(f"Received {len(opportunities)} < {limit}, reached last page")
                break
            
            # Small delay between requests
            await asyncio.sleep(0.1)
        
        if page_count >= max_pages:
            logger.warning(f"Reached maximum page limit ({max_pages}), stopping pagination")
        
        logger.info(f"Total opportunities fetched from pipeline {pipeline_id}: {len(all_opportunities)} (in {page_count} pages)")
        return all_opportunities

    async def fetch_opportunities_sample(self, client: httpx.AsyncClient, pipeline_id: str, sample_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch a sample of opportunities from a pipeline for preview purposes"""
        logger.info(f"Fetching sample of {sample_size} opportunities from pipeline {pipeline_id}")
        
        url = f"{self.base_url}/pipelines/{pipeline_id}/opportunities"
        params = {"limit": min(sample_size, 100)}  # API limit is usually 100
        
        response = await self._make_request(client, "GET", url, params=params)
        
        if not response or "opportunities" not in response:
            logger.warning(f"No opportunities found in pipeline {pipeline_id}")
            return []
        
        opportunities = response["opportunities"]
        logger.info(f"Fetched {len(opportunities)} sample opportunities from pipeline {pipeline_id}")
        return opportunities[:sample_size]  # Ensure we don't exceed sample_size

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts"""
        if not text1 or not text2:
            return 0.0
        return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()

    async def create_pipeline_mapping(self) -> Dict[str, Any]:
        """Create smart mapping between child and master pipelines"""
        logger.info("Creating pipeline mapping...")
        
        # Fetch pipelines from both locations
        self.child_pipelines = await self.fetch_pipelines(self.child_client)
        self.master_pipelines = await self.fetch_pipelines(self.master_client)
        
        if not self.child_pipelines or not self.master_pipelines:
            logger.error("Could not fetch pipelines from both locations")
            return {"success": False, "error": "Failed to fetch pipelines"}
        
        pipeline_matches = []
        stage_matches = []
        
        for child_pipeline in self.child_pipelines:
            child_pipeline_name = child_pipeline.get("name", "")
            best_match = None
            best_similarity = 0.0
            
            # Find best matching master pipeline by name similarity
            for master_pipeline in self.master_pipelines:
                master_pipeline_name = master_pipeline.get("name", "")
                similarity = self.calculate_similarity(child_pipeline_name, master_pipeline_name)
                
                if similarity > best_similarity and similarity >= 0.6:  # 60% similarity threshold
                    best_similarity = similarity
                    best_match = master_pipeline
            
            if best_match:
                self.pipeline_mapping[child_pipeline["id"]] = best_match["id"]
                pipeline_matches.append({
                    "child_id": child_pipeline["id"],
                    "child_name": child_pipeline_name,
                    "master_id": best_match["id"],
                    "master_name": best_match.get("name", ""),
                    "similarity": best_similarity
                })
                
                # Map stages within the matched pipelines
                child_stages = child_pipeline.get("stages", [])
                master_stages = best_match.get("stages", [])
                
                for child_stage in child_stages:
                    child_stage_name = child_stage.get("name", "")
                    stage_best_match = None
                    stage_best_similarity = 0.0
                    
                    for master_stage in master_stages:
                        master_stage_name = master_stage.get("name", "")
                        stage_similarity = self.calculate_similarity(child_stage_name, master_stage_name)
                        
                        if stage_similarity > stage_best_similarity and stage_similarity >= 0.6:
                            stage_best_similarity = stage_similarity
                            stage_best_match = master_stage
                    
                    if stage_best_match:
                        self.stage_mapping[child_stage["id"]] = stage_best_match["id"]
                        stage_matches.append({
                            "child_stage_id": child_stage["id"],
                            "child_stage_name": child_stage_name,
                            "master_stage_id": stage_best_match["id"],
                            "master_stage_name": stage_best_match.get("name", ""),
                            "similarity": stage_best_similarity,
                            "pipeline": child_pipeline_name
                        })
                    else:
                        logger.warning(f"No matching stage found for '{child_stage_name}' in pipeline '{child_pipeline_name}'")
            else:
                logger.warning(f"No matching pipeline found for '{child_pipeline_name}'")
        
        logger.info(f"Pipeline mapping complete: {len(pipeline_matches)} pipelines, {len(stage_matches)} stages mapped")
        
        return {
            "success": True,
            "pipeline_matches": pipeline_matches,
            "stage_matches": stage_matches,
            "total_pipelines_mapped": len(pipeline_matches),
            "total_stages_mapped": len(stage_matches)
        }

    async def load_contact_mapping(self) -> Dict[str, str]:
        """Load contact mapping from previous migration or contact migration service"""
        # If contact mapping is already loaded (e.g., from combined migration), use it
        if hasattr(self, 'contact_mapping') and self.contact_mapping:
            logger.info(f"Using pre-loaded contact mapping: {len(self.contact_mapping)} contacts")
            return self.contact_mapping
        
        # Try to load from contact migration log file
        contact_mapping_file = "contact_mapping.json"
        
        if os.path.exists(contact_mapping_file):
            try:
                with open(contact_mapping_file, 'r') as f:
                    self.contact_mapping = json.load(f)
                logger.info(f"Loaded {len(self.contact_mapping)} contact mappings from file")
                return self.contact_mapping
            except Exception as e:
                logger.error(f"Failed to load contact mapping from file: {e}")
        
        logger.warning("No contact mapping file found. You may need to run contact migration first.")
        return {}

    async def create_opportunity(self, opportunity_data: Dict[str, Any], master_pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Create opportunity in master location"""
        url = f"{self.base_url}/pipelines/{master_pipeline_id}/opportunities/"
        
        # Add migration tag if configured
        if self.migration_tag:
            tags = opportunity_data.get("tags", [])
            if self.migration_tag not in tags:
                tags.append(self.migration_tag)
                opportunity_data["tags"] = tags
        
        logger.debug(f"Creating opportunity: {opportunity_data.get('title', 'Unknown')}")
        logger.debug(f"Opportunity data: {opportunity_data}")
        logger.debug(f"POST URL: {url}")
        
        response = await self._make_request(self.master_client, "POST", url, json=opportunity_data)
        
        logger.debug(f"Create opportunity response: {response}")
        
        if response and "opportunity" in response:
            return response["opportunity"]
        elif response:
            # Log the actual response structure
            logger.error(f"Unexpected response structure: {response}")
            logger.error(f"Response keys: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
            
            # Check if the response itself is the opportunity (some APIs return the object directly)
            if isinstance(response, dict) and "id" in response:
                logger.info(f"Response appears to be the opportunity object directly: {response}")
                return response
        
        # Check for specific validation errors
        if hasattr(self, '_last_error_response') and self._last_error_response:
            error_data = self._last_error_response
            if isinstance(error_data, dict) and "contactId" in error_data:
                contact_error = error_data.get("contactId", {})
                if isinstance(contact_error, dict) and contact_error.get("message"):
                    error_message = contact_error["message"]
                    if "already opportunity exists" in error_message:
                        logger.warning(f"Contact already has opportunity in this pipeline: {opportunity_data.get('title', 'Unknown')}")
                        return {"skip_reason": "duplicate_opportunity", "message": error_message}
        
        logger.error(f"Failed to create opportunity: {opportunity_data.get('title', 'Unknown')}")
        return None

    async def migrate_opportunities(self) -> Dict[str, Any]:
        """Migrate opportunities from child to master location with smart mapping"""
        logger.info("Starting opportunity migration...")
        
        # Load contact mapping
        await self.load_contact_mapping()
        if not self.contact_mapping:
            return {
                "success": False,
                "error": "No contact mapping available. Please run contact migration first."
            }
        
        # Create pipeline mapping
        mapping_result = await self.create_pipeline_mapping()
        if not mapping_result["success"]:
            return mapping_result
        
        total_opportunities = 0
        migrated_opportunities = 0
        failed_opportunities = 0
        skipped_opportunities = 0
        
        migration_results = []
        
        self._update_progress("Counting opportunities", 0, 1, "Counting total opportunities to migrate...")
        
        # Count total opportunities across all mapped pipelines
        for child_pipeline in self.child_pipelines:
            if child_pipeline["id"] in self.pipeline_mapping:
                opportunities = await self.fetch_opportunities_from_pipeline(self.child_client, child_pipeline["id"])
                total_opportunities += len(opportunities)
        
        logger.info(f"Total opportunities to migrate: {total_opportunities}")
        
        # Apply test limit if configured
        if self.test_limit > 0:
            logger.info(f"Test mode: Processing only {self.test_limit} opportunities")
            total_opportunities = min(total_opportunities, self.test_limit)
        
        processed = 0
        
        # Migrate opportunities from each mapped pipeline
        for child_pipeline in self.child_pipelines:
            child_pipeline_id = child_pipeline["id"]
            child_pipeline_name = child_pipeline.get("name", "Unknown")
            
            if child_pipeline_id not in self.pipeline_mapping:
                logger.warning(f"Skipping pipeline '{child_pipeline_name}' - no mapping found")
                continue
            
            master_pipeline_id = self.pipeline_mapping[child_pipeline_id]
            
            self._update_progress("Fetching opportunities", processed, total_opportunities, 
                                f"Fetching opportunities from pipeline: {child_pipeline_name}")
            
            opportunities = await self.fetch_opportunities_from_pipeline(self.child_client, child_pipeline_id)
            
            for opportunity in opportunities:
                if self.test_limit > 0 and processed >= self.test_limit:
                    logger.info(f"Test limit reached: {self.test_limit} opportunities")
                    break
                
                processed += 1
                
                self._update_progress("Migrating opportunities", processed, total_opportunities,
                                    f"Migrating: {opportunity.get('title', 'Unknown')}")
                
                try:
                    # Debug: Log opportunity structure to understand available fields
                    logger.info(f"Processing opportunity: {opportunity.get('id', 'unknown')}")
                    logger.debug(f"Opportunity keys: {list(opportunity.keys())}")
                    logger.debug(f"Full opportunity data: {opportunity}")
                    
                    # Get child contact ID and find master contact ID
                    child_contact_id = opportunity.get("contactId")
                    
                    # Try alternative contact ID fields if primary is missing
                    if not child_contact_id:
                        child_contact_id = opportunity.get("contact", {}).get("id") if isinstance(opportunity.get("contact"), dict) else None
                    if not child_contact_id:
                        child_contact_id = opportunity.get("contactID")  # Alternative spelling
                    if not child_contact_id:
                        child_contact_id = opportunity.get("contact_id")  # Snake case
                    
                    if not child_contact_id:
                        logger.warning(f"Opportunity '{opportunity.get('title', 'Unknown')}' (ID: {opportunity.get('id')}) has no contact ID - available fields: {list(opportunity.keys())}")
                        skipped_opportunities += 1
                        continue
                    
                    master_contact_id = self.contact_mapping.get(child_contact_id)
                    if not master_contact_id:
                        logger.warning(f"No master contact found for child contact {child_contact_id}")
                        skipped_opportunities += 1
                        continue
                    
                    # Map stage ID
                    child_stage_id = opportunity.get("stageId")
                    master_stage_id = self.stage_mapping.get(child_stage_id) if child_stage_id else None
                    
                    if not master_stage_id:
                        # Use first stage of master pipeline as fallback
                        master_pipeline = next((p for p in self.master_pipelines if p["id"] == master_pipeline_id), None)
                        if master_pipeline and master_pipeline.get("stages"):
                            master_stage_id = master_pipeline["stages"][0]["id"]
                            logger.warning(f"Using fallback stage for opportunity '{opportunity.get('title')}'")
                        else:
                            logger.error(f"No stage mapping or fallback for opportunity '{opportunity.get('title')}'")
                            failed_opportunities += 1
                            continue
                    
                    # Prepare opportunity data for master location
                    opportunity_data = {
                        "title": opportunity.get("title", "Migrated Opportunity"),
                        "status": opportunity.get("status", "open"),
                        "stageId": master_stage_id,
                        "contactId": master_contact_id,
                        "monetaryValue": opportunity.get("monetaryValue", 0),
                        "source": "migration_api",
                        "tags": opportunity.get("tags", [])
                    }
                    
                    # Add optional fields if they exist
                    if opportunity.get("assignedTo"):
                        opportunity_data["assignedTo"] = opportunity.get("assignedTo")
                    if opportunity.get("name"):
                        opportunity_data["name"] = opportunity.get("name")
                    if opportunity.get("companyName"):
                        opportunity_data["companyName"] = opportunity.get("companyName")
                    
                    # Create opportunity in master location
                    created_opportunity = await self.create_opportunity(opportunity_data, master_pipeline_id)
                    
                    if created_opportunity:
                        self.opportunity_mapping[opportunity["id"]] = created_opportunity["id"]
                        migrated_opportunities += 1
                        
                        migration_results.append({
                            "child_opportunity_id": opportunity["id"],
                            "master_opportunity_id": created_opportunity["id"],
                            "title": opportunity.get("title"),
                            "status": "success"
                        })
                        
                        logger.info(f"Migrated opportunity: {opportunity.get('title')} -> {created_opportunity['id']}")
                    else:
                        failed_opportunities += 1
                        migration_results.append({
                            "child_opportunity_id": opportunity["id"],
                            "title": opportunity.get("title"),
                            "status": "failed",
                            "error": "Failed to create opportunity"
                        })
                
                except Exception as e:
                    failed_opportunities += 1
                    logger.error(f"Error migrating opportunity {opportunity.get('id', 'unknown')}: {str(e)}")
                    migration_results.append({
                        "child_opportunity_id": opportunity.get("id", "unknown"),
                        "title": opportunity.get("title", "Unknown"),
                        "status": "failed",
                        "error": str(e)
                    })
                
                # Rate limiting
                await asyncio.sleep(settings.migration_rate_limit_delay)
            
            if self.test_limit > 0 and processed >= self.test_limit:
                break
        
        # Save opportunity mapping
        opportunity_mapping_file = "opportunity_mapping.json"
        try:
            with open(opportunity_mapping_file, 'w') as f:
                json.dump(self.opportunity_mapping, f, indent=2)
            logger.info(f"Saved opportunity mapping to {opportunity_mapping_file}")
        except Exception as e:
            logger.error(f"Failed to save opportunity mapping: {e}")
        
        self._update_progress("Complete", processed, total_opportunities, "Opportunity migration completed")
        
        logger.info(f"Opportunity migration completed:")
        logger.info(f"  Total processed: {processed}")
        logger.info(f"  Successfully migrated: {migrated_opportunities}")
        logger.info(f"  Failed: {failed_opportunities}")
        logger.info(f"  Skipped: {skipped_opportunities}")
        
        return {
            "success": True,
            "total_processed": processed,
            "migrated": migrated_opportunities,
            "failed": failed_opportunities,
            "skipped": skipped_opportunities,
            "pipeline_mapping": mapping_result,
            "migration_results": migration_results
        }

    async def get_migration_preview(self) -> Dict[str, Any]:
        """Get a preview of what will be migrated without actually migrating"""
        logger.info("Generating migration preview...")
        
        # Load contact mapping
        await self.load_contact_mapping()
        
        # Create pipeline mapping
        mapping_result = await self.create_pipeline_mapping()
        if not mapping_result["success"]:
            return mapping_result
        
        preview_data = {
            "pipeline_mapping": mapping_result,
            "contact_mapping_count": len(self.contact_mapping),
            "opportunities_by_pipeline": [],
            "total_opportunities": 0,
            "test_mode": self.test_limit > 0,
            "test_limit": self.test_limit if self.test_limit > 0 else None
        }
        
        # Count opportunities in each mapped pipeline (using efficient sampling)
        for child_pipeline in self.child_pipelines:
            if child_pipeline["id"] in self.pipeline_mapping:
                # For preview, get a sample instead of all opportunities
                sample_opportunities = await self.fetch_opportunities_sample(self.child_client, child_pipeline["id"], 10)
                
                # Get actual count by making a single request to see total
                url = f"{self.base_url}/pipelines/{child_pipeline['id']}/opportunities"
                response = await self._make_request(self.child_client, "GET", url, params={"limit": 1})
                
                # Estimate total count (this is a rough estimate since GHL V1 doesn't provide total count)
                estimated_total = 0
                if response and "opportunities" in response and response["opportunities"]:
                    # If we get results, make a few sample requests to estimate
                    sample_response = await self._make_request(self.child_client, "GET", url, params={"limit": 100})
                    if sample_response and "opportunities" in sample_response:
                        sample_size = len(sample_response["opportunities"])
                        if sample_size == 100:
                            # Likely more than 100, estimate based on sample requests
                            estimated_total = sample_size * 50  # Conservative estimate
                        else:
                            estimated_total = sample_size
                else:
                    estimated_total = 0
                
                pipeline_info = {
                    "child_pipeline_id": child_pipeline["id"],
                    "child_pipeline_name": child_pipeline.get("name", "Unknown"),
                    "master_pipeline_id": self.pipeline_mapping[child_pipeline["id"]],
                    "opportunity_count": estimated_total,
                    "opportunities_preview": sample_opportunities[:3] if sample_opportunities else []  # Show first 3 as preview
                }
                
                preview_data["opportunities_by_pipeline"].append(pipeline_info)
                preview_data["total_opportunities"] += estimated_total
        
        if self.test_limit > 0:
            preview_data["total_opportunities"] = min(preview_data["total_opportunities"], self.test_limit)
        
        return {
            "success": True,
            "preview": preview_data
        }

    async def close(self):
        """Close HTTP clients"""
        await self.child_client.aclose()
        await self.master_client.aclose()
