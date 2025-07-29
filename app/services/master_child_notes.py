import asyncio
import csv
import io
import json
import math
import os
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
import pandas as pd
import httpx
from app.config import settings
from fuzzywuzzy import fuzz
import re

logger = logging.getLogger(__name__)

class MasterChildNotesService:
    def __init__(self):
        self.base_dir = os.path.join(os.getcwd(), "master_child_data")
        self.results_dir = os.path.join(self.base_dir, "results")
        self.logs_dir = os.path.join(self.base_dir, "logs")
        
        # Ensure directories exist
        for directory in [self.base_dir, self.results_dir, self.logs_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # In-memory progress tracking
        self.progress_tracker = {}

    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number for comparison"""
        if not phone:
            return ""
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str(phone))
        # Take last 10 digits for US numbers
        if len(digits_only) >= 10:
            return digits_only[-10:]
        return digits_only

    def normalize_name(self, name: str) -> str:
        """Normalize name for comparison"""
        if not name:
            return ""
        # Convert to lowercase, remove extra spaces, and special characters
        normalized = re.sub(r'[^\w\s]', '', str(name).lower())
        return ' '.join(normalized.split())

    def calculate_similarity_score(self, master_contact: Dict, child_contact: Dict) -> float:
        """Calculate similarity score between master and child contacts"""
        
        # Normalize fields
        master_name = self.normalize_name(master_contact.get('Contact Name', ''))
        child_name = self.normalize_name(child_contact.get('Contact Name', ''))
        
        master_phone = self.normalize_phone(master_contact.get('phone', ''))
        child_phone = self.normalize_phone(child_contact.get('phone', ''))
        
        master_stage = str(master_contact.get('stage', '')).lower().strip()
        child_stage = str(child_contact.get('stage', '')).lower().strip()
        
        # Calculate individual scores with NaN handling
        try:
            name_score = fuzz.ratio(master_name, child_name) / 100.0 if master_name and child_name else 0.0
            # Handle potential NaN from fuzz.ratio
            if math.isnan(name_score) or math.isinf(name_score):
                name_score = 0.0
        except (ValueError, ZeroDivisionError):
            name_score = 0.0
            
        phone_score = 1.0 if master_phone and child_phone and master_phone == child_phone else 0.0
        stage_score = 1.0 if master_stage and child_stage and master_stage == child_stage else 0.0
        
        # Weighted scoring - phone is most important, then name, then stage
        total_score = (phone_score * 0.6) + (name_score * 0.3) + (stage_score * 0.1)
        
        # Ensure score is valid and within bounds
        if math.isnan(total_score) or math.isinf(total_score):
            total_score = 0.0
        
        return max(0.0, min(1.0, total_score))

    async def match_contacts(self, master_csv_content: str, child_csv_content: str) -> Dict[str, Any]:
        """Match contacts between master and child CSV files"""
        
        try:
            # Parse CSV files with error handling
            try:
                master_df = pd.read_csv(io.StringIO(master_csv_content))
                child_df = pd.read_csv(io.StringIO(child_csv_content))
            except Exception as e:
                logger.error(f"CSV parsing error: {str(e)}")
                return {
                    'success': False,
                    'error': f'CSV parsing failed: {str(e)}',
                    'matches': [],
                    'unmatched_master': [],
                    'summary': {}
                }
            
            # Validate CSV structure
            required_columns = ['Contact Name', 'phone', 'stage', 'Contact ID', 'Account Id']
            missing_master = [col for col in required_columns if col not in master_df.columns]
            missing_child = [col for col in required_columns if col not in child_df.columns]
            
            if missing_master or missing_child:
                error_msg = f"Missing columns - Master: {missing_master}, Child: {missing_child}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg,
                    'matches': [],
                    'unmatched_master': [],
                    'summary': {}
                }
            
            # Fill NaN values to prevent issues
            master_df = master_df.fillna('')
            child_df = child_df.fillna('')
            
            logger.info(f"Master CSV: {len(master_df)} contacts")
            logger.info(f"Child CSV: {len(child_df)} contacts")
            
            matches = []
            unmatched_master = []
            match_summary = {
                'total_master_contacts': len(master_df),
                'total_child_contacts': len(child_df),
                'exact_matches': 0,
                'fuzzy_matches': 0,
                'no_matches': 0,
                'multiple_matches': 0
            }
            
            # For each master contact, find best matching child contact
            for master_idx, master_contact in master_df.iterrows():
                best_match = None
                best_score = 0.0
                potential_matches = []
                
                # Compare with all child contacts
                for child_idx, child_contact in child_df.iterrows():
                    score = self.calculate_similarity_score(master_contact.to_dict(), child_contact.to_dict())
                    
                    # Ensure score is valid
                    if math.isnan(score) or math.isinf(score):
                        score = 0.0
                    score = max(0.0, min(1.0, score))
                    
                    if score > 0.7:  # Threshold for potential match
                        potential_matches.append({
                            'child_contact': child_contact.to_dict(),
                            'child_index': child_idx,
                            'score': score
                        })
                        
                        if score > best_score:
                            best_score = score
                            best_match = {
                                'child_contact': child_contact.to_dict(),
                                'child_index': child_idx,
                                'score': score
                            }
                
                # Categorize the match
                if best_match:
                    # Ensure best_score is valid
                    if math.isnan(best_score) or math.isinf(best_score):
                        best_score = 0.0
                    best_score = max(0.0, min(1.0, best_score))
                    
                    match_type = 'exact' if best_score >= 0.9 else 'fuzzy'
                    if match_type == 'exact':
                        match_summary['exact_matches'] += 1
                    else:
                        match_summary['fuzzy_matches'] += 1
                    
                    # Check for multiple high-confidence matches (with NaN safety)
                    high_confidence_matches = [m for m in potential_matches if not math.isnan(m.get('score', 0)) and m.get('score', 0) >= 0.8]
                    if len(high_confidence_matches) > 1:
                        match_summary['multiple_matches'] += 1
                    
                    matches.append({
                        'master_contact': master_contact.to_dict(),
                        'master_index': master_idx,
                        'child_contact': best_match['child_contact'],
                        'child_index': best_match['child_index'],
                        'match_score': best_score,
                        'match_type': match_type,
                        'potential_matches_count': len(potential_matches),
                        'master_contact_id': master_contact.get('Contact ID'),
                        'child_contact_id': best_match['child_contact'].get('Contact ID'),
                        'master_account_id': master_contact.get('Account Id'),
                        'child_account_id': best_match['child_contact'].get('Account Id')
                    })
                else:
                    match_summary['no_matches'] += 1
                    unmatched_master.append({
                        'master_contact': master_contact.to_dict(),
                        'master_index': master_idx,
                        'master_contact_id': master_contact.get('Contact ID'),
                        'master_account_id': master_contact.get('Account Id')
                    })
            
            return {
                'success': True,
                'matches': matches,
                'unmatched_master': unmatched_master,
                'summary': match_summary
            }
            
        except Exception as e:
            logger.error(f"Contact matching failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'matches': [],
                'unmatched_master': [],
                'summary': {}
            }

    async def process_notes_transfer(
        self, 
        matches: List[Dict], 
        dry_run: bool = False,
        batch_size: int = 10
    ) -> Dict[str, Any]:
        """Process notes transfer from child contacts to master contacts"""
        
        processing_id = str(uuid.uuid4())
        operation_start = datetime.now()
        
        # Analyze match types
        exact_matches = [m for m in matches if m.get('match_type') == 'exact']
        fuzzy_matches = [m for m in matches if m.get('match_type') == 'fuzzy']
        
        logger.info(f"Processing notes transfer: {len(exact_matches)} exact matches, {len(fuzzy_matches)} fuzzy matches")
        
        # Initialize progress tracking
        self.progress_tracker[processing_id] = {
            'status': 'initializing',
            'total': len(matches),
            'completed': 0,
            'success_count': 0,
            'error_count': 0,
            'notes_transferred': 0,
            'current_batch': 0,
            'recent_errors': [],
            'start_time': operation_start,
            'eta': None,
            'rate': None,
            'dry_run': dry_run,
            'exact_matches_count': len(exact_matches),
            'fuzzy_matches_count': len(fuzzy_matches),
            'processing_summary': f"Processing {len(exact_matches)} exact + {len(fuzzy_matches)} fuzzy matches"
        }
        
        try:
            # Prepare subaccount API keys
            subaccounts = settings.subaccounts_list
            account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}
            
            # Start background processing
            asyncio.create_task(self._process_notes_batches(
                processing_id, matches, account_api_keys, batch_size, dry_run
            ))
            
            return {
                'success': True,
                'message': f'Notes transfer process started. Processing {len(matches)} contact pairs.',
                'processing_id': processing_id,
                'dry_run': dry_run
            }
            
        except Exception as e:
            self.progress_tracker[processing_id]['status'] = 'failed'
            logger.error(f"Notes transfer process failed: {str(e)}")
            return {
                'success': False,
                'message': f'Process failed: {str(e)}',
                'processing_id': processing_id
            }

    async def _process_notes_batches(
        self, 
        processing_id: str, 
        matches: List[Dict], 
        account_api_keys: Dict[str, str], 
        batch_size: int,
        dry_run: bool
    ):
        """Process notes transfer in batches"""
        
        progress = self.progress_tracker[processing_id]
        total_matches = len(matches)
        processed_contacts = 0
        
        logger.info(f"Starting notes transfer: {total_matches} contact pairs, batch size: {batch_size}, dry_run: {dry_run}")
        
        async with httpx.AsyncClient(timeout=60) as client:
            for batch_start in range(0, total_matches, batch_size):
                batch_end = min(batch_start + batch_size, total_matches)
                batch_matches = matches[batch_start:batch_end]
                current_batch_num = batch_start // batch_size + 1
                
                progress['current_batch'] = current_batch_num
                progress['status'] = f'processing_batch_{current_batch_num}'
                
                logger.info(f"Processing batch {current_batch_num}: contacts {batch_start + 1}-{batch_end}")
                
                # Process each contact pair in the batch
                for match_index, match in enumerate(batch_matches):
                    contact_pair_number = batch_start + match_index + 1
                    
                    try:
                        child_contact_id = match.get('child_contact_id')
                        master_contact_id = match.get('master_contact_id')
                        child_account_id = str(match.get('child_account_id', ''))
                        master_account_id = str(match.get('master_account_id', ''))
                        match_type = match.get('match_type', 'unknown')
                        
                        child_api_key = account_api_keys.get(child_account_id)
                        master_api_key = account_api_keys.get(master_account_id)
                        
                        # Update progress status with current contact
                        progress['status'] = f'processing_contact_{contact_pair_number}/{total_matches}'
                        
                        if not all([child_contact_id, master_contact_id, child_api_key, master_api_key]):
                            error_msg = f"Missing required data for contact pair {contact_pair_number}: {child_contact_id} -> {master_contact_id}"
                            progress['recent_errors'].append(error_msg)
                            progress['error_count'] += 1
                            logger.error(error_msg)
                            continue
                        
                        logger.info(f"[{contact_pair_number}/{total_matches}] Processing {match_type} match: {child_contact_id} -> {master_contact_id}")
                        
                        # Step 1: Get notes from child contact
                        child_notes = await self._get_contact_notes(
                            client, child_contact_id, child_api_key
                        )
                        
                        if not child_notes:
                            logger.info(f"[{contact_pair_number}/{total_matches}] No notes found for child contact {child_contact_id}")
                            progress['success_count'] += 1
                            processed_contacts += 1
                            continue
                        
                        # Step 2: Transfer notes to master contact
                        if not dry_run:
                            transferred_count = await self._transfer_notes_to_master(
                                client, master_contact_id, master_api_key, child_notes
                            )
                            progress['notes_transferred'] += transferred_count
                            logger.info(f"[{contact_pair_number}/{total_matches}] Successfully transferred {transferred_count}/{len(child_notes)} notes from {child_contact_id} to {master_contact_id}")
                        else:
                            progress['notes_transferred'] += len(child_notes)
                            logger.info(f"[{contact_pair_number}/{total_matches}] [DRY RUN] Would transfer {len(child_notes)} notes from {child_contact_id} to {master_contact_id}")
                        
                        progress['success_count'] += 1
                        processed_contacts += 1
                        
                    except Exception as e:
                        error_msg = f"[{contact_pair_number}/{total_matches}] Exception processing contact pair {match.get('child_contact_id')} -> {match.get('master_contact_id')}: {str(e)}"
                        progress['recent_errors'].append(error_msg)
                        progress['error_count'] += 1
                        logger.error(error_msg)
                    
                    finally:
                        progress['completed'] += 1
                        
                        # Update ETA and rate calculations
                        elapsed = (datetime.now() - progress['start_time']).total_seconds()
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
                        
                        # Log progress every 5 contacts
                        if progress['completed'] % 5 == 0 or progress['completed'] == total_matches:
                            logger.info(f"Progress: {progress['completed']}/{total_matches} contacts processed, {progress['success_count']} successful, {progress['notes_transferred']} notes transferred")
                
                # Log batch completion
                logger.info(f"Completed batch {current_batch_num}: {len(batch_matches)} contacts processed")
                
                # Small delay between batches to prevent rate limiting
                if batch_end < total_matches:  # Don't delay after the last batch
                    await asyncio.sleep(1.0)
        
        # Mark as completed
        progress['status'] = 'completed'
        logger.info(f"Notes transfer completed: {processed_contacts} contacts processed, {progress['success_count']} successful, {progress['error_count']} errors, {progress['notes_transferred']} notes transferred")
        
        # Save operation results
        await self._save_operation_results(processing_id, matches, progress)

    async def _get_contact_notes(self, client: httpx.AsyncClient, contact_id: str, api_key: str) -> List[Dict]:
        """Get all notes for a contact"""
        
        try:
            url = f"https://rest.gohighlevel.com/v1/contacts/{contact_id}/notes/"
            headers = {"Authorization": f"Bearer {api_key}"}
            
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            notes = data.get('notes', [])
            
            logger.info(f"Retrieved {len(notes)} notes for contact {contact_id}")
            return notes
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Contact {contact_id} not found")
                return []
            else:
                logger.error(f"HTTP error getting notes for {contact_id}: {e.response.status_code}")
                raise
        except Exception as e:
            logger.error(f"Error getting notes for contact {contact_id}: {str(e)}")
            raise

    async def _transfer_notes_to_master(
        self, 
        client: httpx.AsyncClient, 
        master_contact_id: str, 
        master_api_key: str, 
        notes: List[Dict]
    ) -> int:
        """Transfer notes to master contact"""
        
        transferred_count = 0
        
        for note in notes:
            try:
                # Prepare note body with source information
                original_body = note.get('body', '')
                created_by = note.get('createdBy', 'Unknown')
                created_at = note.get('createdAt', '')
                
                # Add source information to the note
                enhanced_body = f"[Transferred from child contact]\n{original_body}\n\n--- Original note created by: {created_by} at {created_at} ---"
                
                url = f"https://rest.gohighlevel.com/v1/contacts/{master_contact_id}/notes/"
                headers = {
                    "Authorization": f"Bearer {master_api_key}",
                    "Content-Type": "application/json"
                }
                payload = {"body": enhanced_body}
                
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                
                transferred_count += 1
                logger.info(f"Successfully transferred note to master contact {master_contact_id}")
                
                # Small delay between individual note transfers
                await asyncio.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error transferring note to master contact {master_contact_id}: {str(e)}")
                # Continue with other notes even if one fails
                continue
        
        return transferred_count

    async def _save_operation_results(self, processing_id: str, matches: List[Dict], progress: Dict):
        """Save operation results to file"""
        
        operation_record = {
            'processing_id': processing_id,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_matches': progress['total'],
                'successful_transfers': progress['success_count'],
                'failed_transfers': progress['error_count'],
                'total_notes_transferred': progress['notes_transferred'],
                'dry_run': progress['dry_run']
            },
            'matches_processed': len(matches),
            'final_errors': progress['recent_errors']
        }
        
        results_file = os.path.join(self.results_dir, f"notes_transfer_{processing_id}.json")
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

    def export_matches_to_csv(self, matches: List[Dict], unmatched: List[Dict]) -> str:
        """Export matches to CSV format"""
        
        # Prepare matched contacts data
        matched_data = []
        for match in matches:
            master = match['master_contact']
            child = match['child_contact']
            
            matched_data.append({
                'Match_Type': match['match_type'],
                'Match_Score': round(match['match_score'], 3),
                'Master_Contact_Name': master.get('Contact Name', ''),
                'Master_Phone': master.get('phone', ''),
                'Master_Stage': master.get('stage', ''),
                'Master_Contact_ID': master.get('Contact ID', ''),
                'Master_Account_ID': master.get('Account Id', ''),
                'Child_Contact_Name': child.get('Contact Name', ''),
                'Child_Phone': child.get('phone', ''),
                'Child_Stage': child.get('stage', ''),
                'Child_Contact_ID': child.get('Contact ID', ''),
                'Child_Account_ID': child.get('Account Id', ''),
                'Potential_Matches_Count': match['potential_matches_count']
            })
        
        # Prepare unmatched data
        unmatched_data = []
        for unmatched_contact in unmatched:
            master = unmatched_contact['master_contact']
            unmatched_data.append({
                'Master_Contact_Name': master.get('Contact Name', ''),
                'Master_Phone': master.get('phone', ''),
                'Master_Stage': master.get('stage', ''),
                'Master_Contact_ID': master.get('Contact ID', ''),
                'Master_Account_ID': master.get('Account Id', ''),
                'Reason': 'No matching child contact found'
            })
        
        # Create CSV content
        output = io.StringIO()
        
        # Write matched contacts
        if matched_data:
            matched_df = pd.DataFrame(matched_data)
            output.write("=== MATCHED CONTACTS ===\n")
            matched_df.to_csv(output, index=False)
            output.write("\n\n")
        
        # Write unmatched contacts
        if unmatched_data:
            unmatched_df = pd.DataFrame(unmatched_data)
            output.write("=== UNMATCHED MASTER CONTACTS ===\n")
            unmatched_df.to_csv(output, index=False)
        
        return output.getvalue()

# Create global instance
master_child_service = MasterChildNotesService()
