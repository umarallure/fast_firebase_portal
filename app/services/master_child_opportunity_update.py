import asyncio
import csv
import io
import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
import pandas as pd
import httpx
from fuzzywuzzy import fuzz
import re
from app.config import settings

logger = logging.getLogger(__name__)

class MasterChildOpportunityUpdateService:
    def __init__(self):
        self.base_dir = os.path.join(os.getcwd(), "master_child_opportunity_updates")
        self.results_dir = os.path.join(self.base_dir, "results")
        self.logs_dir = os.path.join(self.base_dir, "logs")
        
        # Ensure directories exist
        for directory in [self.base_dir, self.results_dir, self.logs_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # In-memory progress tracking
        self.progress_tracker = {}
        self.matching_tracker = {}
        
        # Pipeline and stage mapping cache
        self.pipeline_cache = {}

    async def get_pipeline_mapping(self, account_id: str, api_key: str) -> Dict[str, Any]:
        """Get pipeline and stage mapping for an account"""
        
        # Always refresh pipeline mapping to ensure we have the latest data
        # if account_id in self.pipeline_cache:
        #     return self.pipeline_cache[account_id]
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = "https://rest.gohighlevel.com/v1/pipelines/"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                logger.info(f"Fetching pipeline data for account {account_id}")
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                
                pipelines_data = response.json()
                logger.info(f"Raw pipeline response: {json.dumps(pipelines_data, indent=2)}")
                
                # Create mapping of pipeline names to IDs and stage names to IDs
                pipeline_mapping = {
                    'pipelines': {},
                    'stages': {},
                    'pipeline_stages': {}
                }
                
                total_stages = 0
                for pipeline in pipelines_data.get('pipelines', []):
                    pipeline_id = pipeline['id']
                    pipeline_name = pipeline['name'].strip().lower()
                    
                    logger.info(f"Processing pipeline: {pipeline['name']} (ID: {pipeline_id})")
                    
                    # Map pipeline name to ID
                    pipeline_mapping['pipelines'][pipeline_name] = pipeline_id
                    
                    # Map stages for this pipeline
                    pipeline_mapping['pipeline_stages'][pipeline_id] = {}
                    
                    for stage in pipeline.get('stages', []):
                        stage_id = stage['id']
                        stage_name = stage['name'].strip()
                        stage_name_lower = stage_name.lower()
                        
                        logger.info(f"  Stage: '{stage_name}' (ID: {stage_id})")
                        
                        # Map stage name to ID globally (using lowercase for matching)
                        pipeline_mapping['stages'][stage_name_lower] = stage_id
                        
                        # Map stage name to ID within this pipeline (using lowercase for matching)
                        pipeline_mapping['pipeline_stages'][pipeline_id][stage_name_lower] = stage_id
                        
                        total_stages += 1
                
                # Cache the mapping
                self.pipeline_cache[account_id] = pipeline_mapping
                
                logger.info(f"Pipeline mapping cached for account {account_id}: {len(pipelines_data.get('pipelines', []))} pipelines, {total_stages} total stages")
                logger.info(f"All available stages: {list(pipeline_mapping['stages'].keys())}")
                return pipeline_mapping
                
        except Exception as e:
            logger.error(f"Error fetching pipeline mapping for account {account_id}: {str(e)}")
            return {
                'pipelines': {},
                'stages': {},
                'pipeline_stages': {}
            }

    def find_stage_id(self, stage_name: str, pipeline_id: str, pipeline_mapping: Dict) -> Optional[str]:
        """Find stage ID based on stage name and pipeline"""
        
        if not stage_name:
            return None
            
        stage_name_normalized = stage_name.strip().lower()
        
        # Debug logging
        logger.info(f"Looking for stage '{stage_name}' (normalized: '{stage_name_normalized}') in pipeline {pipeline_id}")
        
        # First try to find stage within the specific pipeline
        if pipeline_id in pipeline_mapping.get('pipeline_stages', {}):
            pipeline_stages = pipeline_mapping['pipeline_stages'][pipeline_id]
            logger.info(f"Available stages in pipeline {pipeline_id}: {list(pipeline_stages.keys())}")
            if stage_name_normalized in pipeline_stages:
                stage_id = pipeline_stages[stage_name_normalized]
                logger.info(f"Found exact match for stage '{stage_name}' in pipeline: {stage_id}")
                return stage_id
        
        # Fallback to global stage mapping, but ONLY if the stage actually exists in this pipeline
        if stage_name_normalized in pipeline_mapping.get('stages', {}):
            global_stage_id = pipeline_mapping['stages'][stage_name_normalized]
            
            # Verify this stage ID actually exists in the target pipeline
            if pipeline_id in pipeline_mapping.get('pipeline_stages', {}):
                pipeline_stages = pipeline_mapping['pipeline_stages'][pipeline_id]
                # Check if any stage in this pipeline has the same ID as the global one
                for existing_stage_name, existing_stage_id in pipeline_stages.items():
                    if existing_stage_id == global_stage_id:
                        logger.info(f"Found stage '{stage_name}' in global mapping and verified it exists in pipeline: {global_stage_id}")
                        return global_stage_id
                
                # If we get here, the global stage ID doesn't exist in this pipeline
                logger.warning(f"Stage '{stage_name}' found in global mapping (ID: {global_stage_id}) but not available in pipeline {pipeline_id}")
                logger.warning(f"Available stages in pipeline: {list(pipeline_stages.keys())}")
                return None
            else:
                # If we don't have pipeline-specific data, fall back to global (less safe but better than nothing)
                logger.warning(f"Using global stage mapping for '{stage_name}' (ID: {global_stage_id}) - pipeline-specific data not available")
                return global_stage_id
        
        # Try fuzzy matching for stage names within the specific pipeline first
        if pipeline_id in pipeline_mapping.get('pipeline_stages', {}):
            pipeline_stages = pipeline_mapping['pipeline_stages'][pipeline_id]
            for existing_stage_name, stage_id in pipeline_stages.items():
                similarity = fuzz.ratio(stage_name_normalized, existing_stage_name)
                if similarity > 85:
                    logger.info(f"Fuzzy matched stage '{stage_name}' to '{existing_stage_name}' (ID: {stage_id}, similarity: {similarity}%)")
                    return stage_id
        
        # Try fuzzy matching for stage names globally, but verify they exist in the target pipeline
        all_stages = pipeline_mapping.get('stages', {})
        best_match = None
        best_similarity = 0
        
        for existing_stage_name, stage_id in all_stages.items():
            similarity = fuzz.ratio(stage_name_normalized, existing_stage_name)
            if similarity > best_similarity and similarity > 85:
                # Verify this stage ID exists in the target pipeline
                if pipeline_id in pipeline_mapping.get('pipeline_stages', {}):
                    pipeline_stages = pipeline_mapping['pipeline_stages'][pipeline_id]
                    for pipe_stage_name, pipe_stage_id in pipeline_stages.items():
                        if pipe_stage_id == stage_id:
                            best_match = (existing_stage_name, stage_id, similarity)
                            best_similarity = similarity
                            break
                else:
                    # If no pipeline-specific data, accept the fuzzy match
                    best_match = (existing_stage_name, stage_id, similarity)
                    best_similarity = similarity
        
        if best_match:
            existing_stage_name, stage_id, similarity = best_match
            logger.info(f"Fuzzy matched stage '{stage_name}' to '{existing_stage_name}' (ID: {stage_id}, similarity: {similarity}%) - verified for pipeline")
            return stage_id
        
        # Log all available stages for debugging
        logger.warning(f"Could not find stage ID for '{stage_name}' in pipeline {pipeline_id}")
        logger.warning(f"All available stages: {list(pipeline_mapping.get('stages', {}).keys())}")
        
        # Fallback: Known stage IDs for Customer Pipeline (temporary fix)
        if pipeline_id == "OYXsfalmHRurVTGchofz":  # Customer Pipeline
            known_stages = {
                "chargeback fix form": "7e7d888c-35f9-4ef4-ae60-8beb170fdfb4",
                "approved customer - not paid": "c3525ee4-5d03-41bb-b1c8-4ea946c64d06",
                "first draft payment failure": "993da8ed-ccd9-4c57-a9f4-7fa749ced916",
                "active placed - paid as earned": "616cedb1-542c-42a4-bd83-701eea8fd6ee",
                "active placed - paid as advanced": "441e0dd2-277f-40b8-837d-69ed87ab4204",
                "pending lapse": "40d37746-094d-4cdd-8376-d6f58c9b33bb",
                "charge-back / payment failure": "b5eded38-9784-4720-94dd-811bf48b2026",
                "charged-back / canceled policy": "b752e7ff-8a76-46d9-912b-ebbf88a054d3",
                "active - 3 months +": "b844925a-a050-4728-8f50-b5fe4cf13c26",
                "active - 6 months +": "d300118d-d20a-4548-9a36-452fc3bb64a7",
                "active - past charge-back period": "e19c1f3e-7d68-483d-b2bb-344ea6d9a1a4"
            }
            
            if stage_name_normalized in known_stages:
                stage_id = known_stages[stage_name_normalized]
                logger.info(f"Using fallback stage mapping for '{stage_name}': {stage_id}")
                return stage_id
        
        return None

    def parse_csv_files(self, master_csv_content: str, child_csv_content: str) -> Dict[str, Any]:
        """Parse and validate both master and child CSV files"""
        
        try:
            # Parse master CSV
            master_df = pd.read_csv(io.StringIO(master_csv_content))
            child_df = pd.read_csv(io.StringIO(child_csv_content))
            
            # Required columns for opportunity matching and updating (matching your CSV structure)
            required_columns = ['Contact Name', 'phone', 'stage', 'Opportunity ID', 'Pipeline ID', 'Account Id']
            child_required_columns = required_columns + ['assigned']  # Child must have assigned column
            
            # Check master columns
            missing_master_columns = [col for col in required_columns if col not in master_df.columns]
            missing_child_columns = [col for col in child_required_columns if col not in child_df.columns]
            
            if missing_master_columns:
                return {
                    'success': False,
                    'error': f'Missing required columns in master file: {missing_master_columns}',
                    'master_opportunities': [],
                    'child_opportunities': [],
                    'summary': {}
                }
            
            if missing_child_columns:
                return {
                    'success': False,
                    'error': f'Missing required columns in child file: {missing_child_columns}',
                    'master_opportunities': [],
                    'child_opportunities': [],
                    'summary': {}
                }
            
            # Fill NaN values
            master_df = master_df.fillna('')
            child_df = child_df.fillna('')
            
            # Extract master opportunities data
            master_opportunities = []
            master_account_ids = set()
            
            for idx, row in master_df.iterrows():
                opportunity_id = str(row.get('Opportunity ID', '')).strip()
                pipeline_id = str(row.get('Pipeline ID', '')).strip()
                account_id = str(row.get('Account Id', '')).strip()  # Note: 'Account Id' with space
                contact_name = str(row.get('Contact Name', '')).strip()
                phone = str(row.get('phone', '')).strip()
                stage = str(row.get('stage', '')).strip()
                assigned_to = str(row.get('assigned', '')).strip()  # Current assignment
                
                # Skip rows with missing critical data
                if not all([opportunity_id, pipeline_id, account_id, contact_name]):
                    logger.warning(f"Skipping master row {idx + 1}: Missing critical data")
                    continue
                
                master_opportunities.append({
                    'opportunity_id': opportunity_id,
                    'pipeline_id': pipeline_id,
                    'account_id': account_id,
                    'contact_name': contact_name,
                    'phone': self._normalize_phone(phone),
                    'stage': stage,
                    'current_assigned_to': assigned_to,
                    'opportunity_name': str(row.get('Opportunity Name', '')).strip(),
                    'pipeline': str(row.get('pipeline', '')).strip(),
                    'status': str(row.get('status', 'open')).strip(),
                    'pipeline_stage_id': str(row.get('Pipeline Stage ID', '')).strip(),
                    'lead_value': str(row.get('Lead Value', '')).strip(),
                    'source': str(row.get('source', '')).strip(),
                    'Created on': str(row.get('Created on', '')).strip(),
                    'row_number': idx + 1,
                    'has_assignment': bool(assigned_to),
                    'source_type': 'master'
                })
                
                master_account_ids.add(account_id)
            
            # Extract child opportunities data
            child_opportunities = []
            child_account_ids = set()
            
            for idx, row in child_df.iterrows():
                opportunity_id = str(row.get('Opportunity ID', '')).strip()
                pipeline_id = str(row.get('Pipeline ID', '')).strip()
                account_id = str(row.get('Account Id', '')).strip()  # Note: 'Account Id' with space
                contact_name = str(row.get('Contact Name', '')).strip()
                phone = str(row.get('phone', '')).strip()
                stage = str(row.get('stage', '')).strip()
                assigned_to = str(row.get('assigned', '')).strip()  # Source assignment
                
                # Skip rows with missing critical data (assigned is optional for child records)
                if not all([opportunity_id, pipeline_id, account_id, contact_name]):
                    logger.warning(f"Skipping child row {idx + 1}: Missing critical data")
                    continue
                
                child_opportunities.append({
                    'opportunity_id': opportunity_id,
                    'pipeline_id': pipeline_id,
                    'account_id': account_id,
                    'contact_name': contact_name,
                    'phone': self._normalize_phone(phone),
                    'stage': stage,
                    'assigned_to': assigned_to,
                    'opportunity_name': str(row.get('Opportunity Name', '')).strip(),
                    'pipeline': str(row.get('pipeline', '')).strip(),
                    'status': str(row.get('status', 'open')).strip(),
                    'pipeline_stage_id': str(row.get('Pipeline Stage ID', '')).strip(),
                    'lead_value': str(row.get('Lead Value', '')).strip(),
                    'source': str(row.get('source', '')).strip(),
                    'Created on': str(row.get('Created on', '')).strip(),
                    'row_number': idx + 1,
                    'source_type': 'child'
                })
                
                child_account_ids.add(account_id)
            
            summary = {
                'master_opportunities': len(master_opportunities),
                'child_opportunities': len(child_opportunities),
                'master_accounts': len(master_account_ids),
                'child_accounts': len(child_account_ids),
                'master_account_ids': list(master_account_ids),
                'child_account_ids': list(child_account_ids),
                'master_with_assignment': sum(1 for opp in master_opportunities if opp['has_assignment']),
                'master_without_assignment': sum(1 for opp in master_opportunities if not opp['has_assignment'])
            }
            
            logger.info(f"Parsed files: {len(master_opportunities)} master opportunities, {len(child_opportunities)} child opportunities")
            
            return {
                'success': True,
                'master_opportunities': master_opportunities,
                'child_opportunities': child_opportunities,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"CSV parsing error: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'master_opportunities': [],
                'child_opportunities': [],
                'summary': {}
            }

    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number for comparison"""
        if not phone:
            return ""
        
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', phone)
        
        # If it starts with 1 and has 11 digits, remove the 1
        if len(digits_only) == 11 and digits_only.startswith('1'):
            digits_only = digits_only[1:]
        
        # If it's already 10 digits, keep as is
        # This handles cases like +17075675820 -> 7075675820
        return digits_only

    def _calculate_match_score(self, master_opp: Dict, child_opp: Dict) -> float:
        """Calculate match score between master and child opportunities using 5 criteria"""
        
        # Phone match (30% weight)
        phone_score = 0.0
        if master_opp['phone'] and child_opp['phone']:
            if master_opp['phone'] == child_opp['phone']:
                phone_score = 1.0
        
        # Contact Name match (25% weight) - fuzzy matching
        contact_name_score = 0.0
        if master_opp['contact_name'] and child_opp['contact_name']:
            contact_name_score = fuzz.ratio(
                master_opp['contact_name'].lower().strip(),
                child_opp['contact_name'].lower().strip()
            ) / 100.0
        
        # Opportunity Name match (25% weight) - fuzzy matching
        opportunity_name_score = 0.0
        if master_opp.get('opportunity_name') and child_opp.get('opportunity_name'):
            opportunity_name_score = fuzz.ratio(
                master_opp['opportunity_name'].lower().strip(),
                child_opp['opportunity_name'].lower().strip()
            ) / 100.0
        
        # Pipeline match (10% weight) - exact match
        pipeline_score = 0.0
        if master_opp.get('pipeline') and child_opp.get('pipeline'):
            if master_opp['pipeline'].lower().strip() == child_opp['pipeline'].lower().strip():
                pipeline_score = 1.0
        
        # Stage match (10% weight) - exact match
        stage_score = 0.0
        if master_opp['stage'] and child_opp['stage']:
            if master_opp['stage'].lower().strip() == child_opp['stage'].lower().strip():
                stage_score = 1.0
        
        # Calculate weighted score
        total_score = (
            (phone_score * 0.30) + 
            (contact_name_score * 0.25) + 
            (opportunity_name_score * 0.25) + 
            (pipeline_score * 0.10) + 
            (stage_score * 0.10)
        )
        
        return total_score

    async def match_opportunities(
        self, 
        master_opportunities: List[Dict], 
        child_opportunities: List[Dict],
        match_threshold: float = 0.7,
        high_confidence_threshold: float = 0.9
    ) -> Dict[str, Any]:
        """Match master and child opportunities using fuzzy matching algorithm"""
        
        matching_id = str(uuid.uuid4())
        operation_start = datetime.now()
        
        # Initialize matching progress tracking
        self.matching_tracker[matching_id] = {
            'status': 'initializing',
            'total_master': len(master_opportunities),
            'total_child': len(child_opportunities),
            'processed': 0,
            'matches_found': 0,
            'exact_matches': 0,
            'fuzzy_matches': 0,
            'no_matches': 0,
            'start_time': operation_start.isoformat(),
            'matches': []
        }
        
        try:
            logger.info(f"Starting opportunity matching: {len(master_opportunities)} master vs {len(child_opportunities)} child opportunities")

            # Perform matching synchronously for now (can be made async later if needed)
            await self._perform_matching(
                matching_id, master_opportunities, child_opportunities,
                match_threshold, high_confidence_threshold
            )

            # Get the results from the tracker
            tracker = self.matching_tracker[matching_id]
            
            return {
                'success': True,
                'message': f'Opportunity matching completed. Processing {len(master_opportunities)} master opportunities.',
                'matching_id': matching_id,
                'results': {
                    'total_master': tracker['total_master'],
                    'total_child': tracker['total_child'],
                    'matches_found': tracker['matches_found'],
                    'exact_matches': tracker['exact_matches'],
                    'fuzzy_matches': tracker['fuzzy_matches'],
                    'no_matches': tracker['no_matches'],
                    'matches': tracker['matches'],
                    'unmatched_summary': tracker.get('unmatched_summary', {})
                }
            }
            
        except Exception as e:
            self.matching_tracker[matching_id]['status'] = 'failed'
            logger.error(f"Opportunity matching process failed: {str(e)}")
            return {
                'success': False,
                'message': f'Matching process failed: {str(e)}',
                'matching_id': matching_id
            }

    def _calculate_match_score(self, master_opp: Dict, child_opp: Dict) -> float:
        """
        Calculate matching score between master and child opportunities
        Returns score from 0.0 to 1.0
        """
        score = 0.0
        total_weight = 0.0

        # Phone number matching (highest weight: 40%)
        master_phone = self._normalize_phone(master_opp.get('phone', ''))
        child_phone = self._normalize_phone(child_opp.get('phone', ''))

        if master_phone and child_phone:
            total_weight += 0.4
            if master_phone == child_phone:
                score += 0.4  # Exact phone match
            elif self._phones_similar(master_phone, child_phone):
                score += 0.3  # Similar phone numbers
            elif master_phone[-7:] == child_phone[-7:] or master_phone[-10:] == child_phone[-10:]:
                score += 0.2  # Last 7 or 10 digits match

        # Name matching using fuzzy logic (weight: 35%)
        master_name = master_opp.get('contact_name', '').strip().lower()
        child_name = child_opp.get('contact_name', '').strip().lower()

        if master_name and child_name:
            total_weight += 0.35
            name_similarity = fuzz.ratio(master_name, child_name) / 100.0

            if name_similarity >= 0.95:  # Near exact match
                score += 0.35
            elif name_similarity >= 0.85:  # Good match
                score += 0.25
            elif name_similarity >= 0.70:  # Fair match
                score += 0.15

        # Date matching (weight: 25%)
        master_date = self._extract_date(master_opp)
        child_date = self._extract_date(child_opp)

        if master_date and child_date:
            total_weight += 0.25
            date_diff = abs((master_date - child_date).days)

            if date_diff == 0:  # Same date
                score += 0.25
            elif date_diff <= 7:  # Within a week
                score += 0.20
            elif date_diff <= 30:  # Within a month
                score += 0.15
            elif date_diff <= 90:  # Within 3 months
                score += 0.10

        # Normalize score by total weight
        if total_weight > 0:
            final_score = score / total_weight
        else:
            final_score = 0.0

        return min(final_score, 1.0)  # Cap at 1.0

    def _phones_similar(self, phone1: str, phone2: str) -> bool:
        """Check if two phone numbers are similar"""
        if not phone1 or not phone2:
            return False

        # Remove country codes for comparison
        p1_digits = phone1.replace('+1', '').replace('+', '')
        p2_digits = phone2.replace('+1', '').replace('+', '')

        # Check if they share the last 7 digits (local number)
        return p1_digits[-7:] == p2_digits[-7:]

    def _extract_date(self, opportunity: Dict) -> Optional[datetime]:
        """Extract and parse date from opportunity data"""
        date_fields = ['Created on', 'created_date', 'date']

        for field in date_fields:
            date_str = opportunity.get(field, '')
            if date_str:
                try:
                    # Try different date formats
                    for fmt in ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y']:
                        try:
                            return datetime.strptime(date_str, fmt)
                        except ValueError:
                            continue
                except Exception:
                    continue

        return None

    async def _perform_matching(
        self,
        matching_id: str,
        master_opportunities: List[Dict],
        child_opportunities: List[Dict],
        match_threshold: float,
        high_confidence_threshold: float
    ):
        """Perform the actual matching logic - Child-to-Master matching"""

        tracker = self.matching_tracker[matching_id]
        tracker['status'] = 'processing'

        matches = []
        matched_master_ids = set()  # Track which masters have already been matched

        for child_idx, child_opp in enumerate(child_opportunities):
            try:
                tracker['processed'] = child_idx + 1
                tracker['status'] = f'processing_child_{child_idx + 1}/{len(child_opportunities)}'

                best_match = None
                best_score = 0.0
                best_master_opp = None

                # Compare this child with all available master opportunities
                for master_opp in master_opportunities:
                    # Skip if this master has already been matched
                    if master_opp['opportunity_id'] in matched_master_ids:
                        continue

                    score = self._calculate_match_score(master_opp, child_opp)

                    if score >= match_threshold and score > best_score:
                        best_score = score
                        best_master_opp = master_opp

                # Record match result
                if best_master_opp:
                    match_type = 'exact' if best_score >= high_confidence_threshold else 'fuzzy'

                    # Mark this master as matched
                    matched_master_ids.add(best_master_opp['opportunity_id'])

                    # Only skip if the matched child has the same stage as master (meaning no change needed)
                    same_stage = (best_master_opp['stage'].lower().strip() == child_opp['stage'].lower().strip())
                    can_update = not same_stage  # Update unless stages are the same
                    skip_reason = 'Same stage - no change needed' if same_stage else None

                    match_record = {
                        'master_opportunity': best_master_opp,
                        'child_opportunity': child_opp,
                        'match_score': best_score,
                        'match_type': match_type,
                        'confidence': 'high' if best_score >= high_confidence_threshold else 'medium',
                        'can_update': can_update,
                        'skip_reason': skip_reason,
                        # Enhanced data for complete opportunity sync
                        'sync_data': {
                            'assigned_to': child_opp['assigned_to'],
                            'status': child_opp['status'],
                            'stage': child_opp['stage'],
                            'pipeline_stage_id': child_opp.get('pipeline_stage_id', ''),
                            'source_pipeline_id': child_opp['pipeline_id'],
                            'target_pipeline_id': best_master_opp['pipeline_id']
                        }
                    }

                    matches.append(match_record)
                    tracker['matches_found'] += 1

                    if match_type == 'exact':
                        tracker['exact_matches'] += 1
                    else:
                        tracker['fuzzy_matches'] += 1

                    logger.info(f"Match found: {child_opp['contact_name']} -> {best_master_opp['contact_name']} (score: {best_score:.2f})")
                else:
                    # No match found for this child - create a no-match record
                    no_match_record = {
                        'master_opportunity': None,
                        'child_opportunity': child_opp,
                        'match_score': 0.0,
                        'match_type': 'no_match',
                        'confidence': 'none',
                        'can_update': False,
                        'skip_reason': 'No matching master opportunity found',
                        'sync_data': None
                    }

                    matches.append(no_match_record)
                    tracker['no_matches'] += 1
                    logger.info(f"No match found for child: {child_opp['contact_name']} (Phone: {child_opp['phone']})")

            except Exception as e:
                logger.error(f"Error matching child opportunity {child_idx + 1}: {str(e)}")

        # Store matches and mark as completed
        tracker['matches'] = matches
        tracker['status'] = 'completed'
        tracker['completion_time'] = datetime.now().isoformat()

        # Generate unmatched summary
        matched_children = [m for m in matches if m['match_type'] != 'no_match']
        unmatched_children = [m for m in matches if m['match_type'] == 'no_match']

        unmatched_summary = {
            'total_child_opportunities': len(child_opportunities),
            'matched_opportunities': len(matched_children),
            'unmatched_opportunities': len(unmatched_children),
            'note': 'Each child opportunity matches to at most one master opportunity'
        }

        tracker['unmatched_summary'] = unmatched_summary

        logger.info(f"Matching completed: {len(matched_children)} matches found ({tracker['exact_matches']} exact, {tracker['fuzzy_matches']} fuzzy), {len(unmatched_children)} unmatched children")

    def get_matching_progress(self, matching_id: str) -> Dict[str, Any]:
        """Get progress for a matching operation"""
        
        if matching_id not in self.matching_tracker:
            return {'success': False, 'message': 'Matching ID not found'}
        
        return {
            'success': True,
            'progress': self.matching_tracker[matching_id]
        }

    async def process_opportunity_updates(
        self, 
        matches: List[Dict], 
        dry_run: bool = False,
        batch_size: int = 10,
        process_exact_only: bool = False
    ) -> Dict[str, Any]:
        """Process opportunity owner updates based on matches"""
        
        processing_id = str(uuid.uuid4())
        operation_start = datetime.now()
        
        # Filter matches based on criteria
        filtered_matches = []
        for match in matches:
            # Only filter by match type if requested
            if process_exact_only and match['match_type'] != 'exact':
                continue
            
            # Process all opportunities - no skipping based on assignment status
            filtered_matches.append(match)
        
        # Initialize progress tracking
        self.progress_tracker[processing_id] = {
            'status': 'initializing',
            'total': len(filtered_matches),
            'completed': 0,
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0,
            'current_batch': 0,
            'recent_errors': [],
            'start_time': operation_start.isoformat(),
            'eta': None,
            'rate': None,
            'dry_run': dry_run,
            'process_exact_only': process_exact_only
        }
        
        try:
            # Prepare subaccount API keys
            subaccounts = settings.subaccounts_list
            account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}
            
            logger.info(f"Starting opportunity updates: {len(filtered_matches)} matches to process, batch size: {batch_size}, dry_run: {dry_run}")
            
            # Start background processing
            asyncio.create_task(self._process_update_batches(
                processing_id, filtered_matches, account_api_keys, batch_size, dry_run
            ))
            
            return {
                'success': True,
                'message': f'Opportunity update process started. Processing {len(filtered_matches)} matches.',
                'processing_id': processing_id,
                'dry_run': dry_run,
                'total_matches': len(filtered_matches)
            }
            
        except Exception as e:
            self.progress_tracker[processing_id]['status'] = 'failed'
            logger.error(f"Opportunity update process failed: {str(e)}")
            return {
                'success': False,
                'message': f'Process failed: {str(e)}',
                'processing_id': processing_id
            }

    async def _process_update_batches(
        self, 
        processing_id: str, 
        matches: List[Dict], 
        account_api_keys: Dict[str, str], 
        batch_size: int,
        dry_run: bool
    ):
        """Process opportunity updates in batches with complete data sync"""
        
        progress = self.progress_tracker[processing_id]
        total_matches = len(matches)
        processed_matches = 0
        
        logger.info(f"Processing {total_matches} opportunity updates with complete data sync")
        
        # Pre-load pipeline mappings for all accounts
        pipeline_mappings = {}
        # Filter out no_match records before extracting account IDs
        valid_matches = [match for match in matches if match.get('master_opportunity') is not None]
        unique_accounts = set(match['master_opportunity']['account_id'] for match in valid_matches)
        
        for account_id in unique_accounts:
            api_key = account_api_keys.get(account_id)
            if api_key:
                pipeline_mappings[account_id] = await self.get_pipeline_mapping(account_id, api_key)
                logger.info(f"Loaded pipeline mapping for account {account_id}")
        
        async with httpx.AsyncClient(timeout=60) as client:
            for batch_start in range(0, total_matches, batch_size):
                batch_end = min(batch_start + batch_size, total_matches)
                batch_matches = matches[batch_start:batch_end]
                current_batch_num = batch_start // batch_size + 1
                
                progress['current_batch'] = current_batch_num
                progress['status'] = f'processing_batch_{current_batch_num}'
                
                logger.info(f"Processing batch {current_batch_num}: matches {batch_start + 1}-{batch_end}")
                
                # Process each match in the batch
                for match_index, match in enumerate(batch_matches):
                    match_number = batch_start + match_index + 1
                    
                    try:
                        # Skip no_match records where master_opportunity is None
                        if match.get('master_opportunity') is None:
                            logger.info(f"[{match_number}/{total_matches}] Skipping no_match record")
                            continue
                            
                        master_opp = match['master_opportunity']
                        child_opp = match['child_opportunity']
                        sync_data = match['sync_data']
                        
                        opportunity_id = master_opp['opportunity_id']
                        pipeline_id = master_opp['pipeline_id']
                        account_id = master_opp['account_id']
                        
                        # Update progress status
                        progress['status'] = f'processing_match_{match_number}/{total_matches}'
                        
                        # Get API key for this account
                        api_key = account_api_keys.get(account_id)
                        if not api_key:
                            error_msg = f"[{match_number}/{total_matches}] No API key found for account {account_id}"
                            progress['recent_errors'].append(error_msg)
                            progress['error_count'] += 1
                            logger.error(error_msg)
                            continue
                        
                        # Get pipeline mapping for this account
                        pipeline_mapping = pipeline_mappings.get(account_id, {})
                        
                        logger.info(f"[{match_number}/{total_matches}] Updating opportunity {opportunity_id} with selective fields (Score: {match['match_score']:.2f})")
                        logger.info(f"  → Pipeline Stage: {child_opp.get('stage', 'N/A')}")
                        logger.info(f"  → Opportunity Value: {child_opp.get('value', child_opp.get('Lead Value', 'N/A'))}")

                        # Update opportunity with selective fields only (pipeline stage and value)
                        if not dry_run:
                            success = await self._update_master_opportunity_selective(
                                client, pipeline_id, opportunity_id, child_opp, master_opp, api_key, pipeline_mapping
                            )

                            if success:
                                progress['success_count'] += 1
                                logger.info(f"[{match_number}/{total_matches}] Successfully updated opportunity {opportunity_id}")
                            else:
                                progress['error_count'] += 1
                                logger.error(f"[{match_number}/{total_matches}] Failed to update opportunity {opportunity_id}")
                        else:
                            progress['success_count'] += 1
                            logger.info(f"[{match_number}/{total_matches}] [DRY RUN] Would update opportunity {opportunity_id}")
                            logger.info(f"  → Would update stage to: {child_opp.get('stage', 'N/A')}")
                            logger.info(f"  → Would update value to: {child_opp.get('value', child_opp.get('Lead Value', 'N/A'))}")
                        
                        processed_matches += 1
                        
                    except Exception as e:
                        error_msg = f"[{match_number}/{total_matches}] Exception updating match: {str(e)}"
                        progress['recent_errors'].append(error_msg)
                        progress['error_count'] += 1
                        logger.error(error_msg)
                    
                    finally:
                        progress['completed'] += 1
                        
                        # Update ETA and rate calculations
                        elapsed = (datetime.now() - datetime.fromisoformat(progress['start_time'])).total_seconds()
                        if elapsed > 0 and progress['completed'] > 0:
                            rate = progress['completed'] / elapsed * 60  # items per minute
                            progress['rate'] = f"{rate:.1f}"
                            
                            if progress['completed'] < total_matches:
                                remaining = total_matches - progress['completed']
                                eta_seconds = remaining / (progress['completed'] / elapsed)
                                if eta_seconds < 60:
                                    progress['eta'] = f"{eta_seconds:.0f} seconds"
                                else:
                                    progress['eta'] = f"{eta_seconds/60:.1f} minutes"
                            else:
                                progress['eta'] = "Complete"
                        
                        # Keep only recent errors (last 10)
                        if len(progress['recent_errors']) > 10:
                            progress['recent_errors'] = progress['recent_errors'][-10:]
                        
                        # Log progress every 5 matches
                        if progress['completed'] % 5 == 0 or progress['completed'] == total_matches:
                            logger.info(f"Progress: {progress['completed']}/{total_matches} matches processed, {progress['success_count']} successful, {progress['error_count']} errors")
                
                # Log batch completion
                logger.info(f"Completed batch {current_batch_num}: {len(batch_matches)} matches processed")
                
                # Small delay between batches to prevent rate limiting
                if batch_end < total_matches:
                    await asyncio.sleep(1.0)
        
        # Mark as completed
        progress['status'] = 'completed'
        logger.info(f"Opportunity updates completed: {processed_matches} matches processed, {progress['success_count']} successful, {progress['error_count']} errors")
        
        # Save operation results
        await self._save_operation_results(processing_id, matches, progress)
        
        # Return results directly for API response
        return {
            'success': True,
            'message': f'Opportunity update process completed. Processed {processed_matches} matches.',
            'processing_id': processing_id,
            'results': {
                'total_matches': total_matches,
                'processed': processed_matches,
                'successful': progress['success_count'],
                'failed': progress['error_count'],
                'skipped': progress['skipped_count'],
                'dry_run': progress['dry_run'],
                'process_exact_only': progress['process_exact_only'],
                'updates': []  # Would contain detailed update results
            }
        }

    async def _update_master_opportunity_complete(
        self, 
        client: httpx.AsyncClient, 
        pipeline_id: str, 
        opportunity_id: str, 
        sync_data: Dict,
        api_key: str,
        master_opportunity: Dict,
        pipeline_mapping: Dict
    ) -> bool:
        """Update master opportunity with complete data sync (owner, status, stage)"""
        
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            # Validate required sync data
            if not sync_data.get('assigned_to'):
                logger.info(f"No assigned_to value for opportunity {opportunity_id}, will sync without owner assignment")
                # Don't return False - continue with status/stage sync only
                
            if not sync_data.get('status'):
                logger.warning(f"No status value for opportunity {opportunity_id}, using 'open' as default")
                sync_data['status'] = 'open'
            
            # Step 1: Try to update opportunity owner and basic data
            # Note: User IDs from child accounts may not be valid in master account
            owner_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities/{opportunity_id}"
            owner_payload = {
                "status": sync_data['status']  # Required field for GHL API
            }
            
            # Only add assignedTo if we have a valid non-empty value
            if sync_data.get('assigned_to') and sync_data['assigned_to'].strip():
                owner_payload["assignedTo"] = sync_data['assigned_to']
            
            # Add other fields to maintain data integrity
            if master_opportunity.get('opportunity_name'):
                owner_payload['title'] = master_opportunity['opportunity_name']
            
            # Try to update with owner assignment first
            try:
                owner_response = await client.put(owner_url, headers=headers, json=owner_payload)
                owner_response.raise_for_status()
                
                if "assignedTo" in owner_payload:
                    logger.info(f"Successfully updated opportunity {opportunity_id} owner to {sync_data['assigned_to']}")
                    owner_updated = True
                else:
                    logger.info(f"Successfully updated opportunity {opportunity_id} (no owner assignment)")
                    owner_updated = False
                    
            except httpx.HTTPStatusError as owner_error:
                if owner_error.response.status_code == 422 and "assignedTo" in owner_error.response.text:
                    logger.warning(f"Invalid user ID '{sync_data['assigned_to']}' for opportunity {opportunity_id}, updating without owner assignment")
                    # Retry without assignedTo field
                    fallback_payload = {k: v for k, v in owner_payload.items() if k != "assignedTo"}
                    owner_response = await client.put(owner_url, headers=headers, json=fallback_payload)
                    owner_response.raise_for_status()
                    owner_updated = False
                else:
                    raise  # Re-raise if it's a different error
            
            # Step 2: Update opportunity status and stage using the new API
            status_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities/{opportunity_id}/status"
            
            # Find the correct stage ID for the target pipeline
            target_stage_id = None
            logger.info(f"Attempting to find stage ID for stage '{sync_data.get('stage')}' in pipeline {pipeline_id}")
            
            # Log pipeline mapping info
            if pipeline_mapping:
                logger.info(f"Pipeline mapping available: {len(pipeline_mapping.get('stages', {}))} total stages")
                logger.info(f"Available stages: {list(pipeline_mapping.get('stages', {}).keys())}")
            else:
                logger.error(f"No pipeline mapping available for account!")
            
            # Always prioritize stage name lookup over CSV stage ID to ensure correct pipeline mapping
            if sync_data.get('stage'):
                # Find stage ID based on stage name first
                target_stage_id = self.find_stage_id(
                    sync_data['stage'], 
                    pipeline_id, 
                    pipeline_mapping
                )
                if target_stage_id:
                    logger.info(f"Found stage ID via name lookup: {target_stage_id}")
                else:
                    logger.error(f"Could not find stage ID for '{sync_data['stage']}' in pipeline {pipeline_id}")
            
            # Only fall back to CSV stage ID if name lookup failed and we're confident it's valid
            if not target_stage_id and sync_data.get('pipeline_stage_id'):
                logger.warning(f"Falling back to CSV pipeline_stage_id: {sync_data['pipeline_stage_id']} (stage name lookup failed)")
                target_stage_id = sync_data['pipeline_stage_id']
            
            if target_stage_id:
                status_payload = {
                    "status": sync_data['status'],
                    "stageId": target_stage_id
                }
                
                status_response = await client.put(status_url, headers=headers, json=status_payload)
                status_response.raise_for_status()
                
                logger.info(f"Successfully updated opportunity {opportunity_id} status to '{sync_data['status']}' and stage to '{sync_data['stage']}' (ID: {target_stage_id})")
            else:
                # If we can't find the stage ID, just update the status
                status_payload = {
                    "status": sync_data['status']
                }
                
                status_response = await client.put(status_url, headers=headers, json=status_payload)
                status_response.raise_for_status()
                
                logger.warning(f"Updated opportunity {opportunity_id} status to '{sync_data['status']}' but could not find stage ID for '{sync_data['stage']}'")
            
            # Log final sync summary
            sync_summary = []
            if owner_updated:
                sync_summary.append(f"owner: {sync_data['assigned_to']}")
            sync_summary.append(f"status: {sync_data['status']}")
            if target_stage_id:
                sync_summary.append(f"stage: {sync_data['stage']}")
            
            logger.info(f"Complete data sync for opportunity {opportunity_id}: {', '.join(sync_summary)}")
            return True
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.error(f"Opportunity {opportunity_id} or pipeline {pipeline_id} not found")
            elif e.response.status_code == 403:
                logger.error(f"Access denied for opportunity {opportunity_id} - check API key permissions")
            elif e.response.status_code == 422:
                error_details = e.response.text
                logger.error(f"Validation error updating opportunity {opportunity_id}: {error_details}")
                if "assignedTo" in error_details:
                    logger.error(f"Invalid user ID '{sync_data['assigned_to']}' - user may not exist in target account")
                if "status" in error_details:
                    logger.error(f"Invalid status '{sync_data['status']}' - check status values")
            else:
                logger.error(f"HTTP error updating opportunity {opportunity_id}: {e.response.status_code} - {e.response.text}")
            return False
            
        except Exception as e:
            logger.error(f"Error updating opportunity {opportunity_id} complete data: {str(e)}")
            return False

    async def _update_master_opportunity_selective(
        self,
        client: httpx.AsyncClient,
        pipeline_id: str,
        opportunity_id: str,
        child_opp: Dict,
        master_opp: Dict,
        api_key: str,
        pipeline_mapping: Dict
    ) -> bool:
        """
        Update master opportunity with only pipeline stage and opportunity value as requested
        """
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }

            update_data = {}

            # 1. Update pipeline stage if different
            child_stage = child_opp.get('stage', '').strip()
            master_stage = master_opp.get('stage', '').strip()

            if child_stage and child_stage.lower() != master_stage.lower():
                # Find the correct stage ID for the target pipeline
                target_stage_id = self.find_stage_id(child_stage, pipeline_id, pipeline_mapping)
                if target_stage_id:
                    update_data['stageId'] = target_stage_id
                    logger.info(f"Will update stage from '{master_stage}' to '{child_stage}' (ID: {target_stage_id})")
                else:
                    logger.warning(f"Could not find stage ID for '{child_stage}' in pipeline {pipeline_id}")

            # 2. Update opportunity value if different
            child_value = child_opp.get('value') or child_opp.get('Lead Value', 0)
            master_value = master_opp.get('value') or master_opp.get('Lead Value', 0)

            # Convert to float for comparison
            try:
                child_value_float = float(child_value) if child_value else 0.0
                master_value_float = float(master_value) if master_value else 0.0

                if child_value_float != master_value_float:
                    update_data['value'] = child_value_float
                    logger.info(f"Will update value from {master_value_float} to {child_value_float}")
            except (ValueError, TypeError):
                logger.warning(f"Could not parse opportunity values: child={child_value}, master={master_value}")

            # Only proceed if there are changes to make
            if not update_data:
                logger.info(f"No changes needed for opportunity {opportunity_id} - values already match")
                return True

            # Use the status update endpoint to change stage
            if 'stageId' in update_data:
                status_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities/{opportunity_id}/status"
                status_payload = {
                    "stageId": update_data['stageId']
                }

                # Include status if available
                if master_opp.get('status'):
                    status_payload['status'] = master_opp['status']

                status_response = await client.put(status_url, headers=headers, json=status_payload)
                status_response.raise_for_status()
                logger.info(f"Successfully updated opportunity {opportunity_id} stage")

            # Update opportunity value using main opportunity endpoint
            if 'value' in update_data:
                opp_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities/{opportunity_id}"
                value_payload = {
                    "value": update_data['value']
                }

                # Include required fields to maintain data integrity
                if master_opp.get('opportunity_name'):
                    value_payload['title'] = master_opp['opportunity_name']
                if master_opp.get('status'):
                    value_payload['status'] = master_opp['status']

                value_response = await client.put(opp_url, headers=headers, json=value_payload)
                value_response.raise_for_status()
                logger.info(f"Successfully updated opportunity {opportunity_id} value to {update_data['value']}")

            logger.info(f"Successfully updated opportunity {opportunity_id} with fields: {list(update_data.keys())}")
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.error(f"Opportunity {opportunity_id} or pipeline {pipeline_id} not found")
            elif e.response.status_code == 403:
                logger.error(f"Access denied for opportunity {opportunity_id} - check API key permissions")
            elif e.response.status_code == 422:
                logger.error(f"Validation error updating opportunity {opportunity_id}: {e.response.text}")
            else:
                logger.error(f"HTTP error updating opportunity {opportunity_id}: {e.response.status_code} - {e.response.text}")
            return False

        except Exception as e:
            logger.error(f"Error updating opportunity {opportunity_id}: {str(e)}")
            return False

    def generate_unmatched_csv(self, unmatched_opportunities: List[Dict], matching_id: str) -> str:
        """
        Generate CSV file for unmatched child opportunities
        """
        if not unmatched_opportunities:
            return ""

        # Prepare data for CSV
        csv_data = []
        for opp in unmatched_opportunities:
            csv_row = {
                'Opportunity Name': opp.get('opportunity_name', opp.get('Opportunity Name', '')),
                'Contact Name': opp.get('contact_name', opp.get('Contact Name', '')),
                'Phone': opp.get('phone', ''),
                'Email': opp.get('email', ''),
                'Pipeline': opp.get('pipeline', ''),
                'Stage': opp.get('stage', ''),
                'Lead Value': opp.get('value', opp.get('Lead Value', '0')),
                'Source': opp.get('source', ''),
                'Assigned': opp.get('assigned', ''),
                'Created on': opp.get('Created on', opp.get('created_date', '')),
                'Updated on': opp.get('Updated on', ''),
                'Status': opp.get('status', ''),
                'Opportunity ID': opp.get('opportunity_id', opp.get('Opportunity ID', '')),
                'Contact ID': opp.get('contact_id', opp.get('Contact ID', '')),
                'Pipeline Stage ID': opp.get('pipeline_stage_id', opp.get('Pipeline Stage ID', '')),
                'Pipeline ID': opp.get('pipeline_id', opp.get('Pipeline ID', '')),
                'Account Id': opp.get('account_id', opp.get('Account Id', '')),
                'Match Reason': 'No matching opportunity found in master account'
            }
            csv_data.append(csv_row)

        # Convert to CSV
        output = io.StringIO()
        if csv_data:
            df = pd.DataFrame(csv_data)
            df.to_csv(output, index=False)

        csv_content = output.getvalue()

        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"unmatched_opportunities_{matching_id}_{timestamp}.csv"
        filepath = os.path.join(self.results_dir, filename)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            f.write(csv_content)

        logger.info(f"Generated unmatched opportunities CSV: {filepath} ({len(unmatched_opportunities)} opportunities)")

        return csv_content

    async def _save_operation_results(self, processing_id: str, matches: List[Dict], progress: Dict):
        """Save operation results to file"""
        
        operation_record = {
            'processing_id': processing_id,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_matches': progress['total'],
                'successful_updates': progress['success_count'],
                'failed_updates': progress['error_count'],
                'dry_run': progress['dry_run']
            },
            'matches_processed': len(matches),
            'final_errors': progress['recent_errors']
        }
        
        results_file = os.path.join(self.results_dir, f"master_child_opportunity_update_{processing_id}.json")
        with open(results_file, 'w') as f:
            json.dump(operation_record, f, indent=2)

    def get_progress(self, processing_id: str) -> Dict[str, Any]:
        """Get progress for a processing operation"""
        
        if processing_id not in self.progress_tracker:
            return {'success': False, 'message': 'Processing ID not found'}
        
        return {
            'success': True,
            'progress': self.progress_tracker[processing_id]
        }

    def generate_sample_csvs(self) -> Tuple[str, str]:
        """Generate sample CSV templates for master and child files"""
        
        # Master opportunities sample (matching your structure)
        master_sample_data = [
            {
                'Opportunity Name': 'Jay Kelly - (707) 567-5820',
                'Contact Name': 'Jay Kelly',
                'phone': '+17075675820',
                'email': '',
                'pipeline': 'Customer Pipeline',
                'stage': 'Approved Customer - Not Paid',
                'Lead Value': '0',
                'source': 'Jotform-call center',
                'assigned': '',  # Empty - will be updated from child
                'Created on': '2025-07-15 22:13:01',
                'Updated on': '2025-07-17 17:32:26',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': '',
                'Notes': '',
                'tags': 'call center, master',
                'Engagement Score': '',
                'status': 'open',
                'Opportunity ID': 'xomsoy7d3OO69Z5q0W8m',  # Required
                'Contact ID': 'zd9L5eEQMqIF4rZ1Mv2X',
                'Pipeline Stage ID': '3a7d8a5b-e46b-4be1-b2e6-a72218bc5fb8',
                'Pipeline ID': '0BkkFdKbxY7fChcZ3g8M',  # Required
                'Days Since Last Stage Change': '2',
                'Days Since Last Status Change': '2',
                'Days Since Last Updated': '0',
                'Account Id': '2'  # Required (note the space)
            },
            {
                'Opportunity Name': 'Sarah Johnson - (555) 123-4567',
                'Contact Name': 'Sarah Johnson',
                'phone': '+15551234567',
                'email': '',
                'pipeline': 'Customer Pipeline',
                'stage': 'Approved Customer - Not Paid',
                'Lead Value': '150',
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',  # Already assigned - will be skipped
                'Created on': '2025-07-16 10:30:15',
                'Updated on': '2025-07-17 14:20:10',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': '',
                'Notes': '',
                'tags': 'call center, master',
                'Engagement Score': '',
                'status': 'open',
                'Opportunity ID': 'MasterOpp123456789ABC',  # Required
                'Contact ID': 'ContactMaster123456789',
                'Pipeline Stage ID': '3a7d8a5b-e46b-4be1-b2e6-a72218bc5fb8',
                'Pipeline ID': '0BkkFdKbxY7fChcZ3g8M',  # Required
                'Days Since Last Stage Change': '1',
                'Days Since Last Status Change': '1',
                'Days Since Last Updated': '0',
                'Account Id': '2'  # Required (note the space)
            }
        ]
        
        # Child opportunities sample (source of assignments)
        child_sample_data = [
            {
                'Opportunity Name': 'Jay Kelly - (707) 567-5820 - Child',
                'Contact Name': 'Jay Kelly',
                'phone': '+17075675820',
                'email': '',
                'pipeline': 'Customer Pipeline',
                'stage': 'Approved Customer - Not Paid',
                'Lead Value': '0',
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',  # Required - Source assignment
                'Created on': '2025-07-15 22:13:01',
                'Updated on': '2025-07-17 17:32:26',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': '',
                'Notes': '',
                'tags': 'call center, child',
                'Engagement Score': '',
                'status': 'open',
                'Opportunity ID': 'ChildOpp123456789ABC',  # Required
                'Contact ID': 'zd9L5eEQMqIF4rZ1Mv2X',
                'Pipeline Stage ID': '3a7d8a5b-e46b-4be1-b2e6-a72218bc5fb8',
                'Pipeline ID': '0BkkFdKbxY7fChcZ3g8M',  # Required
                'Days Since Last Stage Change': '2',
                'Days Since Last Status Change': '2',
                'Days Since Last Updated': '0',
                'Account Id': '2'  # Required (note the space)
            },
            {
                'Opportunity Name': 'Sarah Johnson - (555) 123-4567 - Child',
                'Contact Name': 'Sarah Johnson',
                'phone': '+15551234567',
                'email': '',
                'pipeline': 'Customer Pipeline',
                'stage': 'Approved Customer - Not Paid',
                'Lead Value': '150',
                'source': 'Jotform-call center',
                'assigned': 'GkBzcbgHkCNaoSKKdIkZ',  # Required - Source assignment
                'Created on': '2025-07-16 10:30:15',
                'Updated on': '2025-07-17 14:20:10',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': '',
                'Notes': '',
                'tags': 'call center, child',
                'Engagement Score': '',
                'status': 'open',
                'Opportunity ID': 'ChildOpp987654321XYZ',  # Required
                'Contact ID': 'ContactMaster123456789',
                'Pipeline Stage ID': '3a7d8a5b-e46b-4be1-b2e6-a72218bc5fb8',
                'Pipeline ID': '0BkkFdKbxY7fChcZ3g8M',  # Required
                'Days Since Last Stage Change': '1',
                'Days Since Last Status Change': '1',
                'Days Since Last Updated': '0',
                'Account Id': '2'  # Required (note the space)
            }
        ]
        
        # Convert to CSV
        master_output = io.StringIO()
        child_output = io.StringIO()
        
        if master_sample_data:
            master_df = pd.DataFrame(master_sample_data)
            master_df.to_csv(master_output, index=False)
        
        if child_sample_data:
            child_df = pd.DataFrame(child_sample_data)
            child_df.to_csv(child_output, index=False)
        
        return master_output.getvalue(), child_output.getvalue()

    async def export_unmatched_and_failed_to_csv(self, processing_id: str) -> str:
        """Export unmatched records and failed updates to CSV"""
        try:
            # Get the progress data
            progress = self.progress_tracker.get(processing_id)
            if not progress:
                # Try to find any recent processing data
                if self.progress_tracker:
                    # Get the most recent processing ID
                    recent_ids = list(self.progress_tracker.keys())
                    if recent_ids:
                        processing_id = recent_ids[-1]  # Use the most recent
                        progress = self.progress_tracker.get(processing_id)
                        logger.info(f"Using most recent processing ID: {processing_id}")

            if not progress:
                return "Error: No processing data found. Please complete the sync process first."

            # Get the matching tracker data - try to find it by looking for recent matching data
            tracker = self.matching_tracker.get(processing_id)
            if not tracker and self.matching_tracker:
                # Try to find matching data from recent operations
                recent_matching_ids = list(self.matching_tracker.keys())
                if recent_matching_ids:
                    recent_matching_id = recent_matching_ids[-1]
                    tracker = self.matching_tracker.get(recent_matching_id)
                    logger.info(f"Using recent matching data from ID: {recent_matching_id}")

            if not tracker:
                # If no matching data, just export failed updates
                logger.warning(f"No matching data found for processing {processing_id}, exporting failed updates only")

            # Prepare CSV data
            csv_data = []
            timestamp = datetime.now().isoformat()

            # Add unmatched records if we have matching data
            if tracker:
                matches = tracker.get('matches', [])
                for match in matches:
                    if match.get('match_type') == 'no_match':
                        child_opp = match.get('child_opportunity', {})
                        csv_data.append({
                            'type': 'unmatched',
                            'contact_name': child_opp.get('contact_name', ''),
                            'phone': child_opp.get('phone', ''),
                            'email': child_opp.get('email', ''),
                            'opportunity_name': child_opp.get('opportunity_name', ''),
                            'pipeline': child_opp.get('pipeline', ''),
                            'stage': child_opp.get('stage', ''),
                            'status': child_opp.get('status', ''),
                            'value': child_opp.get('value', ''),
                            'account_id': child_opp.get('account_id', ''),
                            'opportunity_id': child_opp.get('opportunity_id', ''),
                            'match_score': match.get('match_score', 0.0),
                            'match_type': match.get('match_type', ''),
                            'confidence': match.get('confidence', ''),
                            'skip_reason': match.get('skip_reason', ''),
                            'error_message': '',
                            'timestamp': timestamp,
                            'processing_id': processing_id
                        })

            # Add failed updates from recent_errors
            recent_errors = progress.get('recent_errors', [])
            for error in recent_errors:
                csv_data.append({
                    'type': 'failed_update',
                    'contact_name': '',
                    'phone': '',
                    'email': '',
                    'opportunity_name': '',
                    'pipeline': '',
                    'stage': '',
                    'status': '',
                    'value': '',
                    'account_id': '',
                    'opportunity_id': '',
                    'match_score': 0.0,
                    'match_type': '',
                    'confidence': '',
                    'skip_reason': '',
                    'error_message': error,
                    'timestamp': timestamp,
                    'processing_id': processing_id
                })

            # If no data at all, return informative message
            if not csv_data:
                return "No unmatched records or failed updates found to export."

            df = pd.DataFrame(csv_data)

            # Create filename with timestamp
            filename = f"unmatched_and_failed_updates_{processing_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(self.results_dir, filename)

            # Save to file
            df.to_csv(filepath, index=False)

            logger.info(f"Exported {len(csv_data)} records to {filepath}")

            return f"Successfully exported {len(csv_data)} records to {filename}"

        except Exception as e:
            logger.error(f"Error exporting unmatched and failed records: {str(e)}")
            return f"Error exporting data: {str(e)}"

# Create global instance
master_child_opportunity_service = MasterChildOpportunityUpdateService()
