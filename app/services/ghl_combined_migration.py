"""
Combined GHL Migration Service
Efficiently handles both Contact and Opportunity migration in coordinated phases
"""

import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from app.config import settings
from app.services.smart_mapping import SmartMappingStrategy
from app.services.ghl_contact_migration import GHLContactMigrationService
from app.services.ghl_opportunity_migration import GHLOpportunityMigrationService
import os
from datetime import datetime
import json

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ghl_combined_migration.log', mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class GHLCombinedMigrationService:
    """Combined migration service that handles both contacts and opportunities efficiently"""
    
    def __init__(self, child_api_key: str, master_api_key: str, progress_callback=None):
        self.base_url = "https://rest.gohighlevel.com/v1"
        
        # Initialize individual migration services
        self.contact_service = GHLContactMigrationService(
            child_api_key=child_api_key,
            master_api_key=master_api_key,
            progress_callback=self._contact_progress_callback
        )
        
        self.opportunity_service = GHLOpportunityMigrationService(
            child_api_key=child_api_key,
            master_api_key=master_api_key,
            progress_callback=self._opportunity_progress_callback
        )
        
        # Main progress callback
        self.progress_callback = progress_callback
        
        # Migration state
        self.migration_state = {
            "phase": "initializing",
            "contact_migration": {"completed": False, "results": None},
            "opportunity_migration": {"completed": False, "results": None},
            "total_phases": 2,
            "current_phase": 0,
            "overall_progress": 0
        }
        
        # Test mode settings
        self.contact_test_limit = getattr(settings, 'contact_migration_test_limit', 5)
        self.opportunity_test_limit = getattr(settings, 'opportunity_migration_test_limit', 5)
        self.contact_tag = getattr(settings, 'contact_migration_tag', 'migrated-contact')
        self.opportunity_tag = getattr(settings, 'opportunity_migration_tag', 'migrated-opportunity')

    def _contact_progress_callback(self, progress_data):
        """Handle contact migration progress updates"""
        # Update overall progress (contacts are 60% of total work)
        contact_progress = (progress_data.get("current", 0) / max(progress_data.get("total", 1), 1)) * 60
        self.migration_state["overall_progress"] = contact_progress
        
        if self.progress_callback:
            self.progress_callback({
                "stage": f"Phase 1 - {progress_data.get('stage', 'Contact Migration')}",
                "current": progress_data.get("current", 0),
                "total": progress_data.get("total", 0),
                "message": progress_data.get("message", ""),
                "phase": 1,
                "overall_progress": contact_progress,
                "phase_name": "Contact Migration"
            })

    def _opportunity_progress_callback(self, progress_data):
        """Handle opportunity migration progress updates"""
        # Update overall progress (opportunities are 40% of total work, starting from 60%)
        opportunity_progress = 60 + (progress_data.get("current", 0) / max(progress_data.get("total", 1), 1)) * 40
        self.migration_state["overall_progress"] = opportunity_progress
        
        if self.progress_callback:
            self.progress_callback({
                "stage": f"Phase 2 - {progress_data.get('stage', 'Opportunity Migration')}",
                "current": progress_data.get("current", 0),
                "total": progress_data.get("total", 0),
                "message": progress_data.get("message", ""),
                "phase": 2,
                "overall_progress": opportunity_progress,
                "phase_name": "Opportunity Migration"
            })

    def _update_progress(self, stage: str, message: str = "", phase: int = 0, overall_progress: float = 0):
        """Update main progress"""
        if self.progress_callback:
            self.progress_callback({
                "stage": stage,
                "current": 0,
                "total": 0,
                "message": message,
                "phase": phase,
                "overall_progress": overall_progress,
                "phase_name": "Combined Migration"
            })

    async def get_migration_preview(self) -> Dict[str, Any]:
        """Get comprehensive preview of both contact and opportunity migration"""
        logger.info("Generating combined migration preview...")
        
        self._update_progress("Generating Preview", "Analyzing contacts and opportunities...", 0, 5)
        
        try:
            # Get contact preview
            contact_preview = await self.contact_service.get_migration_preview()
            
            self._update_progress("Generating Preview", "Analyzing pipeline mappings...", 0, 25)
            
            # Get opportunity preview
            opportunity_preview = await self.opportunity_service.get_migration_preview()
            
            self._update_progress("Preview Complete", "Analysis complete", 0, 100)
            
            # Combine previews
            combined_preview = {
                "success": True,
                "preview": {
                    "contact_migration": contact_preview.get("preview", {}),
                    "opportunity_migration": opportunity_preview.get("preview", {}),
                    "summary": {
                        "total_contacts": contact_preview.get("preview", {}).get("total_contacts", 0),
                        "total_opportunities": opportunity_preview.get("preview", {}).get("total_opportunities", 0),
                        "mapped_pipelines": len(opportunity_preview.get("preview", {}).get("opportunities_by_pipeline", [])),
                        "custom_fields_mapped": len(contact_preview.get("preview", {}).get("field_mapping", {}).get("mapped_fields", [])),
                        "test_mode": {
                            "contact_test_limit": self.contact_test_limit if self.contact_test_limit > 0 else None,
                            "opportunity_test_limit": self.opportunity_test_limit if self.opportunity_test_limit > 0 else None,
                            "contact_tag": self.contact_tag,
                            "opportunity_tag": self.opportunity_tag
                        }
                    }
                }
            }
            
            return combined_preview
            
        except Exception as e:
            logger.error(f"Error generating combined preview: {str(e)}")
            return {"success": False, "error": str(e)}

    async def run_full_migration(self) -> Dict[str, Any]:
        """Run complete migration: Phase 1 (Contacts) followed by Phase 2 (Opportunities)"""
        logger.info("Starting combined migration (Contacts + Opportunities)...")
        
        migration_results = {
            "success": False,
            "contact_results": None,
            "opportunity_results": None,
            "total_time": 0,
            "phases_completed": 0
        }
        
        start_time = datetime.now()
        
        try:
            # Phase 1: Contact Migration
            self.migration_state["phase"] = "contact_migration"
            self.migration_state["current_phase"] = 1
            
            self._update_progress("Phase 1 Starting", "Initializing contact migration...", 1, 0)
            
            logger.info("Phase 1: Starting contact migration...")
            contact_results = await self.contact_service.sync_contacts()
            
            if not contact_results.get("success", False):
                logger.error("Phase 1 failed: Contact migration unsuccessful")
                migration_results["error"] = f"Contact migration failed: {contact_results.get('error', 'Unknown error')}"
                return migration_results
            
            self.migration_state["contact_migration"]["completed"] = True
            self.migration_state["contact_migration"]["results"] = contact_results
            migration_results["contact_results"] = contact_results
            migration_results["phases_completed"] = 1
            
            logger.info(f"Phase 1 completed: {contact_results.get('mapped_contacts', 0)} contacts migrated")
            
            # Save contact mapping for Phase 2
            contact_mapping_file = "contact_mapping.json"
            try:
                with open(contact_mapping_file, 'w') as f:
                    json.dump(self.contact_service.contact_mapping, f, indent=2)
                logger.info(f"Contact mapping saved to {contact_mapping_file}")
            except Exception as e:
                logger.warning(f"Failed to save contact mapping: {e}")
            
            # Brief pause between phases
            await asyncio.sleep(2)
            
            # Phase 2: Opportunity Migration
            self.migration_state["phase"] = "opportunity_migration"
            self.migration_state["current_phase"] = 2
            
            self._update_progress("Phase 2 Starting", "Initializing opportunity migration...", 2, 60)
            
            logger.info("Phase 2: Starting opportunity migration...")
            
            # Load contact mapping into opportunity service
            self.opportunity_service.contact_mapping = self.contact_service.contact_mapping.copy()
            
            opportunity_results = await self.opportunity_service.migrate_opportunities()
            
            if not opportunity_results.get("success", False):
                logger.warning("Phase 2 had issues but Phase 1 was successful")
                migration_results["opportunity_results"] = opportunity_results
                migration_results["warning"] = f"Opportunity migration had issues: {opportunity_results.get('error', 'Unknown error')}"
            else:
                self.migration_state["opportunity_migration"]["completed"] = True
                self.migration_state["opportunity_migration"]["results"] = opportunity_results
                migration_results["opportunity_results"] = opportunity_results
                migration_results["phases_completed"] = 2
                
                logger.info(f"Phase 2 completed: {opportunity_results.get('migrated', 0)} opportunities migrated")
            
            # Calculate total time
            end_time = datetime.now()
            migration_results["total_time"] = (end_time - start_time).total_seconds()
            
            # Determine overall success
            migration_results["success"] = (
                migration_results["phases_completed"] >= 1 and 
                contact_results.get("success", False)
            )
            
            self._update_progress("Migration Complete", "All phases completed", 2, 100)
            
            logger.info(f"Combined migration completed in {migration_results['total_time']:.2f} seconds")
            logger.info(f"Phases completed: {migration_results['phases_completed']}/2")
            
            return migration_results
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Combined migration failed: {error_msg}")
            migration_results["error"] = error_msg
            
            # Calculate partial time
            end_time = datetime.now()
            migration_results["total_time"] = (end_time - start_time).total_seconds()
            
            return migration_results

    async def run_contact_migration_only(self) -> Dict[str, Any]:
        """Run only contact migration (Phase 1)"""
        logger.info("Starting contact-only migration...")
        
        self._update_progress("Contact Migration", "Starting Phase 1 only...", 1, 0)
        
        try:
            results = await self.contact_service.sync_contacts()
            
            # Save contact mapping
            if results.get("success", False):
                contact_mapping_file = "contact_mapping.json"
                try:
                    with open(contact_mapping_file, 'w') as f:
                        json.dump(self.contact_service.contact_mapping, f, indent=2)
                    logger.info(f"Contact mapping saved to {contact_mapping_file}")
                    results["contact_mapping_saved"] = True
                except Exception as e:
                    logger.warning(f"Failed to save contact mapping: {e}")
                    results["contact_mapping_saved"] = False
            
            self._update_progress("Contact Migration Complete", "Phase 1 completed", 1, 100)
            return results
            
        except Exception as e:
            logger.error(f"Contact migration failed: {str(e)}")
            return {"success": False, "error": str(e)}

    async def run_opportunity_migration_only(self) -> Dict[str, Any]:
        """Run only opportunity migration (Phase 2) - requires existing contact mapping"""
        logger.info("Starting opportunity-only migration...")
        
        self._update_progress("Opportunity Migration", "Starting Phase 2 only...", 2, 0)
        
        try:
            # Try to load existing contact mapping
            contact_mapping_file = "contact_mapping.json"
            if os.path.exists(contact_mapping_file):
                try:
                    with open(contact_mapping_file, 'r') as f:
                        contact_mapping = json.load(f)
                    self.opportunity_service.contact_mapping = contact_mapping
                    logger.info(f"Loaded {len(contact_mapping)} contact mappings from file")
                except Exception as e:
                    logger.error(f"Failed to load contact mapping: {e}")
                    return {
                        "success": False, 
                        "error": "Failed to load contact mapping. Please run contact migration first."
                    }
            else:
                return {
                    "success": False,
                    "error": "No contact mapping found. Please run contact migration (Phase 1) first."
                }
            
            results = await self.opportunity_service.migrate_opportunities()
            self._update_progress("Opportunity Migration Complete", "Phase 2 completed", 2, 100)
            return results
            
        except Exception as e:
            logger.error(f"Opportunity migration failed: {str(e)}")
            return {"success": False, "error": str(e)}

    async def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status and progress"""
        return {
            "success": True,
            "status": self.migration_state.copy(),
            "services_available": {
                "contact_migration": True,
                "opportunity_migration": True,
                "combined_migration": True
            },
            "test_settings": {
                "contact_test_limit": self.contact_test_limit if self.contact_test_limit > 0 else None,
                "opportunity_test_limit": self.opportunity_test_limit if self.opportunity_test_limit > 0 else None,
                "contact_tag": self.contact_tag,
                "opportunity_tag": self.opportunity_tag
            }
        }

    async def get_pipeline_and_field_mapping(self) -> Dict[str, Any]:
        """Get both custom field mapping and pipeline mapping"""
        logger.info("Generating comprehensive mapping analysis...")
        
        try:
            # Get field mapping analysis
            field_mapping = await self.contact_service.analyze_custom_fields()
            
            # Get pipeline mapping analysis
            pipeline_mapping = await self.opportunity_service.create_pipeline_mapping()
            
            return {
                "success": True,
                "field_mapping": field_mapping,
                "pipeline_mapping": pipeline_mapping,
                "summary": {
                    "custom_fields_analyzed": len(field_mapping.get("field_analysis", [])),
                    "pipelines_mapped": pipeline_mapping.get("total_pipelines_mapped", 0),
                    "stages_mapped": pipeline_mapping.get("total_stages_mapped", 0)
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating mapping analysis: {str(e)}")
            return {"success": False, "error": str(e)}

    async def close(self):
        """Close all HTTP clients"""
        await self.contact_service.close()
        await self.opportunity_service.close()

    async def run_sequential_migration(self) -> Dict[str, Any]:
        """
        Sequential migration: Process one contact and their opportunities at a time
        This ensures proper contact-opportunity relationships
        """
        logger.info("Starting sequential contact-opportunity migration...")
        
        start_time = datetime.now()
        migration_results = {
            "start_time": start_time.isoformat(),
            "phase": "sequential_migration",
            "contact_results": {"mapped_contacts": 0, "failed_contacts": 0},
            "opportunity_results": {"migrated": 0, "failed": 0, "skipped": 0},
            "total_processed": 0,
            "success": True,
            "errors": []
        }
        
        try:
            # Initialize both services
            self._update_progress("Initialization", "Setting up services...", 1, 0)
            
            # Verify master account is clean
            master_verification = await self.contact_service.verify_master_account_state()
            if master_verification['total_contacts'] > 0:
                logger.warning(f"Master account has {master_verification['total_contacts']} existing contacts")
            
            # Create pipeline mapping once
            self._update_progress("Pipeline Mapping", "Creating pipeline mappings...", 1, 5)
            pipeline_result = await self.opportunity_service.create_pipeline_mapping()
            if not pipeline_result["success"]:
                raise Exception(f"Pipeline mapping failed: {pipeline_result.get('error')}")
            
            # Fetch all child contacts
            self._update_progress("Fetching Contacts", "Loading contacts from child account...", 1, 10)
            child_contacts = await self.contact_service.fetch_contacts(self.contact_service.child_client, "child")
            
            # Store child contacts for opportunity naming
            self.child_contacts = child_contacts
            
            if not child_contacts:
                logger.info("No contacts found in child account")
                migration_results["end_time"] = datetime.now().isoformat()
                return migration_results
            
            # Apply test limit if configured
            if self.contact_service.test_limit > 0:
                original_count = len(child_contacts)
                child_contacts = child_contacts[:self.contact_service.test_limit]
                logger.info(f"TEST MODE: Limited contacts from {original_count} to {len(child_contacts)}")
            
            total_contacts = len(child_contacts)
            logger.info(f"Processing {total_contacts} contacts sequentially with their opportunities")
            
            # Process each contact and their opportunities sequentially
            for contact_index, child_contact in enumerate(child_contacts):
                contact_progress = int(15 + (contact_index / total_contacts) * 80)
                contact_name = f"{child_contact.get('firstName', '')} {child_contact.get('lastName', '')}".strip()
                
                self._update_progress(
                    f"Processing Contact {contact_index + 1}/{total_contacts}",
                    f"Processing {contact_name}...",
                    1,
                    contact_progress
                )
                
                try:
                    # Step 1: Process the contact
                    logger.info(f"Processing contact {contact_index + 1}/{total_contacts}: {contact_name} (ID: {child_contact.get('id')})")
                    
                    master_contact_id = await self.contact_service._process_single_contact(child_contact)
                    
                    if master_contact_id:
                        # Store contact mapping
                        child_contact_id = child_contact["id"]
                        self.contact_service.contact_mapping[child_contact_id] = master_contact_id
                        migration_results["contact_results"]["mapped_contacts"] += 1
                        
                        logger.info(f"✅ Contact migrated: {contact_name} → {master_contact_id}")
                        
                        # Step 2: Find and migrate opportunities for this contact
                        await self._migrate_opportunities_for_contact(
                            child_contact_id, 
                            master_contact_id, 
                            contact_name,
                            migration_results
                        )
                        
                    else:
                        migration_results["contact_results"]["failed_contacts"] += 1
                        logger.warning(f"❌ Failed to migrate contact: {contact_name}")
                    
                    migration_results["total_processed"] += 1
                    
                    # Small delay between contacts to respect rate limits
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error processing contact {contact_name}: {str(e)}")
                    migration_results["contact_results"]["failed_contacts"] += 1
                    migration_results["errors"].append(f"Contact {contact_name}: {str(e)}")
            
            # Save mappings
            self._update_progress("Saving Results", "Saving contact and opportunity mappings...", 1, 95)
            
            # Save contact mapping
            try:
                with open("contact_mapping.json", 'w') as f:
                    json.dump(self.contact_service.contact_mapping, f, indent=2)
                logger.info("Contact mapping saved to contact_mapping.json")
            except Exception as e:
                logger.warning(f"Failed to save contact mapping: {e}")
            
            # Save opportunity mapping
            try:
                with open("opportunity_mapping.json", 'w') as f:
                    json.dump(self.opportunity_service.opportunity_mapping, f, indent=2)
                logger.info("Opportunity mapping saved to opportunity_mapping.json")
            except Exception as e:
                logger.warning(f"Failed to save opportunity mapping: {e}")
            
            # Calculate results
            end_time = datetime.now()
            migration_results["end_time"] = end_time.isoformat()
            migration_results["duration_seconds"] = (end_time - start_time).total_seconds()
            
            self._update_progress("Complete", "Sequential migration completed!", 1, 100)
            
            logger.info("Sequential migration completed:")
            logger.info(f"  Contacts migrated: {migration_results['contact_results']['mapped_contacts']}")
            logger.info(f"  Opportunities migrated: {migration_results['opportunity_results']['migrated']}")
            logger.info(f"  Duration: {migration_results['duration_seconds']:.2f} seconds")
            
            return migration_results
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Sequential migration failed: {error_msg}")
            migration_results["success"] = False
            migration_results["error"] = error_msg
            migration_results["end_time"] = datetime.now().isoformat()
            return migration_results

    async def _migrate_opportunities_for_contact(
        self, 
        child_contact_id: str, 
        master_contact_id: str, 
        contact_name: str,
        migration_results: Dict[str, Any]
    ):
        """Find and migrate all opportunities for a specific contact"""
        try:
            logger.info(f"  🔍 Finding opportunities for contact: {contact_name} ({child_contact_id})")
            
            # Search for opportunities associated with this contact across all pipelines
            contact_opportunities = []
            
            for child_pipeline in self.opportunity_service.child_pipelines:
                pipeline_id = child_pipeline["id"]
                pipeline_name = child_pipeline.get("name", "Unknown")
                
                # Fetch opportunities from this pipeline
                opportunities = await self.opportunity_service.fetch_opportunities_from_pipeline(
                    self.opportunity_service.child_client, 
                    pipeline_id
                )
                
                # Filter opportunities for this specific contact
                for opportunity in opportunities:
                    opportunity_contact_id = opportunity.get("contactId")
                    
                    # Try alternative contact ID fields
                    if not opportunity_contact_id:
                        opportunity_contact_id = opportunity.get("contact", {}).get("id") if isinstance(opportunity.get("contact"), dict) else None
                    if not opportunity_contact_id:
                        opportunity_contact_id = opportunity.get("contactID")
                    if not opportunity_contact_id:
                        opportunity_contact_id = opportunity.get("contact_id")
                    
                    if opportunity_contact_id == child_contact_id:
                        opportunity["source_pipeline_id"] = pipeline_id
                        opportunity["source_pipeline_name"] = pipeline_name
                        contact_opportunities.append(opportunity)
            
            if not contact_opportunities:
                logger.info(f"  ℹ️  No opportunities found for contact: {contact_name}")
                return
            
            logger.info(f"  📋 Found {len(contact_opportunities)} opportunities for contact: {contact_name}")
            
            # Migrate each opportunity
            for opp_index, opportunity in enumerate(contact_opportunities):
                try:
                    # Get contact information for opportunity title
                    child_contact = next((c for c in self.child_contacts if c.get("id") == child_contact_id), None)
                    source_pipeline_id = opportunity["source_pipeline_id"]
                    
                    if child_contact:
                        # Extract clean first and last names
                        first_name = child_contact.get("firstName", "").strip()
                        last_name = child_contact.get("lastName", "").strip()
                        
                        # Clean up last name if it has phone concatenated
                        if " - " in last_name:
                            last_name = last_name.split(" - ")[0].strip()
                        
                        # Get phone for opportunity title
                        phone = child_contact.get("phone", "").strip()
                        
                        # Create full name
                        full_name = f"{first_name} {last_name}".strip()
                        
                        # Create opportunity title in format: "FirstName LastName - Phone"
                        if phone and full_name:
                            opportunity_title = f"{full_name} - {phone}"
                        elif full_name:
                            opportunity_title = full_name
                        else:
                            opportunity_title = opportunity.get("title", "Untitled")
                    else:
                        opportunity_title = opportunity.get("title", "Untitled")
                    
                    logger.info(f"    🔄 Migrating opportunity {opp_index + 1}/{len(contact_opportunities)}: {opportunity_title}")
                    
                    # Get master pipeline mapping
                    master_pipeline_id = self.opportunity_service.pipeline_mapping.get(source_pipeline_id)
                    if not master_pipeline_id:
                        logger.warning(f"    ❌ No master pipeline mapping for source pipeline {source_pipeline_id}")
                        migration_results["opportunity_results"]["skipped"] += 1
                        continue
                    
                    # Get stage mapping
                    child_stage_id = opportunity.get("stageId")
                    master_stage_id = self.opportunity_service.stage_mapping.get(child_stage_id) if child_stage_id else None
                    
                    if not master_stage_id:
                        # Use first stage of master pipeline as fallback
                        master_pipeline = next((p for p in self.opportunity_service.master_pipelines if p["id"] == master_pipeline_id), None)
                        if master_pipeline and master_pipeline.get("stages"):
                            master_stage_id = master_pipeline["stages"][0]["id"]
                            logger.info(f"    ⚠️  Using fallback stage for opportunity: {opportunity_title}")
                        else:
                            logger.error(f"    ❌ No stage mapping for opportunity: {opportunity_title}")
                            migration_results["opportunity_results"]["failed"] += 1
                            continue
                    
                    # Prepare opportunity data
                    opportunity_data = {
                        "title": opportunity_title,
                        "status": opportunity.get("status", "open"),
                        "stageId": master_stage_id,
                        "contactId": master_contact_id,  # Use the newly created master contact ID
                        "monetaryValue": opportunity.get("monetaryValue", 0),
                        "source": "sequential_migration"
                    }
                    
                    # Add optional fields
                    if opportunity.get("assignedTo"):
                        opportunity_data["assignedTo"] = opportunity.get("assignedTo")
                    if opportunity.get("name"):
                        opportunity_data["name"] = opportunity.get("name")
                    if opportunity.get("companyName"):
                        opportunity_data["companyName"] = opportunity.get("companyName")
                    
                    # Create opportunity in master account
                    created_opportunity = await self.opportunity_service.create_opportunity(
                        opportunity_data, 
                        master_pipeline_id
                    )
                    
                    if created_opportunity:
                        # Check if this was a skip due to duplicate
                        if isinstance(created_opportunity, dict) and created_opportunity.get("skip_reason") == "duplicate_opportunity":
                            migration_results["opportunity_results"]["skipped"] += 1
                            logger.info(f"    ⚠️  Opportunity skipped (already exists): {opportunity_title}")
                        else:
                            # Store opportunity mapping
                            self.opportunity_service.opportunity_mapping[opportunity["id"]] = created_opportunity["id"]
                            migration_results["opportunity_results"]["migrated"] += 1
                            logger.info(f"    ✅ Opportunity migrated: {opportunity_title} → {created_opportunity['id']}")
                    else:
                        migration_results["opportunity_results"]["failed"] += 1
                        logger.error(f"    ❌ Failed to create opportunity: {opportunity_title}")
                    
                    # Small delay between opportunities
                    await asyncio.sleep(0.2)
                    
                except Exception as e:
                    logger.error(f"    ❌ Error migrating opportunity {opportunity.get('title', 'Unknown')}: {str(e)}")
                    migration_results["opportunity_results"]["failed"] += 1
                    migration_results["errors"].append(f"Opportunity {opportunity.get('title', 'Unknown')}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error finding opportunities for contact {contact_name}: {str(e)}")
            migration_results["errors"].append(f"Opportunity search for {contact_name}: {str(e)}")
