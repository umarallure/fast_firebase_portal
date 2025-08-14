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
        # Convert to string and remove decimal if present
        phone_str = str(phone)
        if phone_str.endswith('.0'):
            phone_str = phone_str[:-2]
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', phone_str)
        # Remove leading country code (1) if present
        if digits_only.startswith('1') and len(digits_only) == 11:
            digits_only = digits_only[1:]
        # Always return last 10 digits
        if len(digits_only) >= 10:
            return digits_only[-10:]
        # Pad with leading zeros if less than 10 digits
        return digits_only.zfill(10)
        # (Removed unreachable and mis-indented code. Logic is already handled above.)

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
        print(f"Comparing phones: master='{master_phone}' child='{child_phone}'")
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

            # Fill NaN values to prevent issues
            master_df = master_df.fillna('')
            child_df = child_df.fillna('')

            logger.info(f"Master CSV: {len(master_df)} contacts")
            logger.info(f"Child CSV: {len(child_df)} contacts")

            # Build phone lookup for child contacts
            child_phone_lookup = {}
            for child_idx, child_contact in child_df.iterrows():
                norm_phone = self.normalize_phone(child_contact.get('phone', ''))
                if norm_phone:
                    child_phone_lookup.setdefault(norm_phone, []).append((child_idx, child_contact))

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
                # Parse CSV files
                try:
                    master_df = pd.read_csv(io.StringIO(master_csv_content)).fillna('')
                    child_df = pd.read_csv(io.StringIO(child_csv_content)).fillna('')
                except Exception as e:
                    logger.error(f"CSV parsing error: {e}")
                    return {'success': False, 'error': str(e), 'matches': [], 'unmatched_master': [], 'summary': {}}

                print("Sample master contact:", master_df.iloc[0].to_dict() if len(master_df) > 0 else "None")
                print("Sample child contact:", child_df.iloc[0].to_dict() if len(child_df) > 0 else "None")
                print("--- Normalized master contacts (first 3) ---")
                for i in range(min(3, len(master_df))):
                    mc = master_df.iloc[i].to_dict()
                    print(f"Master #{i+1}: Name='{self.normalize_name(mc.get('Contact Name', ''))}', Phone='{self.normalize_phone(mc.get('phone', ''))}'")
                print("--- Normalized child contacts (first 3) ---")
                for i in range(min(3, len(child_df))):
                    cc = child_df.iloc[i].to_dict()
                    print(f"Child #{i+1}: Name='{self.normalize_name(cc.get('Contact Name', ''))}', Phone='{self.normalize_phone(cc.get('phone', ''))}'")

                # Build phone lookup for child contacts
                child_phone_lookup = {}
                for child_idx, child_contact in child_df.iterrows():
                    norm_phone = self.normalize_phone(child_contact.get('phone', ''))
                    if norm_phone:
                        child_phone_lookup.setdefault(norm_phone, []).append(child_contact)

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

                # For each master contact, check all child contacts for best match
                print("\n--- MATCHING DEBUG OUTPUT ---")
                for master_idx, master_contact_row in master_df.iterrows():
                    master_contact = master_contact_row.to_dict()
                    norm_master_phone = self.normalize_phone(str(master_contact.get('phone', '')))
                    norm_master_name = self.normalize_name(master_contact.get('Contact Name', ''))
                    norm_master_opp_name = self.normalize_name(master_contact.get('Opportunity Name', ''))

                    best_match = None
                    best_score = 0.0
                    match_type = 'none'
                    potential_matches_count = 0
                    best_candidate_info = None

                    for child_idx, child_contact_row in child_df.iterrows():
                        child_contact = child_contact_row.to_dict()
                        norm_child_phone = self.normalize_phone(str(child_contact.get('phone', '')))
                        norm_child_name = self.normalize_name(child_contact.get('Contact Name', ''))
                        norm_child_opp_name = self.normalize_name(child_contact.get('Opportunity Name', ''))

                        # Phone match (40% weight)
                        phone_score = 0.0
                        if norm_master_phone and norm_child_phone and norm_master_phone == norm_child_phone:
                            phone_score = 1.0

                        # Contact Name match (30% weight) - fuzzy matching
                        contact_name_score = fuzz.ratio(norm_master_name, norm_child_name) / 100.0 if norm_master_name and norm_child_name else 0.0

                        # Opportunity Name match (30% weight) - fuzzy matching
                        opportunity_name_score = fuzz.ratio(norm_master_opp_name, norm_child_opp_name) / 100.0 if norm_master_opp_name and norm_child_opp_name else 0.0

                        # Weighted score
                        total_score = (phone_score * 0.4) + (contact_name_score * 0.3) + (opportunity_name_score * 0.3)

                        if total_score > best_score:
                            best_score = total_score
                            best_match = child_contact
                            potential_matches_count = 1
                            best_candidate_info = {
                                'child_name': child_contact.get('Contact Name', ''),
                                'child_phone': child_contact.get('phone', ''),
                                'child_opp_name': child_contact.get('Opportunity Name', ''),
                                'score': total_score,
                                'phone_score': phone_score,
                                'contact_name_score': contact_name_score,
                                'opportunity_name_score': opportunity_name_score
                            }

                    print(f"Master #{master_idx+1}: {master_contact.get('Contact Name', '')} | Best Score: {best_score:.3f}")
                    if best_candidate_info:
                        print(f"  Best Candidate: Name='{best_candidate_info['child_name']}', Phone='{best_candidate_info['child_phone']}', OppName='{best_candidate_info['child_opp_name']}'")
                        print(f"    Score: {best_candidate_info['score']:.3f} (Phone: {best_candidate_info['phone_score']:.3f}, Name: {best_candidate_info['contact_name_score']:.3f}, OppName: {best_candidate_info['opportunity_name_score']:.3f})")
                    else:
                        print("  No candidate found.")
                    if best_match and best_score >= 0.85:
                        match_type = 'exact'
                        match_summary['exact_matches'] += 1
                    elif best_match and best_score >= 0.7:
                        match_type = 'fuzzy'
                        match_summary['fuzzy_matches'] += 1
                    else:
                        match_type = 'none'
                        match_summary['no_matches'] += 1

                    if best_match and match_type != 'none':
                        matches.append({
                            'master_contact': master_contact,
                            'child_contact': best_match,
                            'match_score': best_score,
                            'match_type': match_type,
                            'potential_matches_count': potential_matches_count,
                            'master_contact_id': master_contact.get('Contact ID', ''),
                            'child_contact_id': best_match.get('Contact ID', '')
                        })
                    else:
                        unmatched_master.append({
                            'master_contact': master_contact,
                            'master_contact_id': master_contact.get('Contact ID', '')
                        })

                return {
                    'success': True,
                    'matches': matches,
                    'unmatched_master': unmatched_master,
                    'summary': match_summary
                }

            return {
                'success': True,
                'matches': matches,
                'unmatched_master': unmatched_master,
                'summary': match_summary
            }
            
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

    async def process_notes_transfer(self, matches, dry_run=False, batch_size=10):
                    # Debug output for API key selection
                    print(f"  Child Account Id: {child_account_id}")
                    print(f"  Child API Key: {child_api_key}")
                    print(f"  Master Account Id: {master_account_id}")
                    print(f"  Master API Key: {master_api_key}")
    async def process_notes_transfer(self, matches, dry_run=False, batch_size=10):
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
                'dry_run': False,
            'exact_matches_count': len(exact_matches),
            'fuzzy_matches_count': len(fuzzy_matches),
            'processing_summary': f"Processing {len(exact_matches)} exact + {len(fuzzy_matches)} fuzzy matches"
        }

        try:
            # Prepare subaccount API keys
            import os, json
            subaccounts = json.loads(os.environ.get('SUBACCOUNTS', '[]'))
            account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}

            # Production notes transfer: call actual API for each contact pair
            import httpx
            total = len(matches)
            notes_transferred = 0
            errors = 0
            async with httpx.AsyncClient() as client:
                print(f"Available account_api_keys: {list(account_api_keys.keys())}")
                for idx, match in enumerate(matches):
                    child_contact_id = match.get('child_contact_id')
                    master_contact_id = match.get('master_contact_id')
                    # Get Account Ids from CSV contact dicts, ensure string and strip whitespace
                    child_account_id = str(match['child_contact'].get('Account Id', '')).strip()
                    master_account_id = str(match['master_contact'].get('Account Id', '')).strip()
                    match_type = match.get('match_type', 'unknown')
                    child_api_key = account_api_keys.get(child_account_id)
                    master_api_key = account_api_keys.get(master_account_id)
                    print(f"  Child Account Id: '{child_account_id}'")
                    print(f"  Child API Key: {child_api_key}")
                    print(f"  Master Account Id: '{master_account_id}'")
                    print(f"  Master API Key: {master_api_key}")
                    print(f"[{idx+1}/{total}] Processing {match_type} match: {child_contact_id} -> {master_contact_id}")
                    try:
                        # Get notes from child contact
                        child_notes = await self._get_contact_notes(client, child_contact_id, child_api_key)
                        if not child_notes:
                            print(f"No notes found for child contact {child_contact_id}")
                            continue
                        # Transfer notes to master contact
                        transferred_count = await self._transfer_notes_to_master(client, master_contact_id, master_api_key, child_notes)
                        notes_transferred += transferred_count
                        print(f"Transferred {transferred_count} notes from child {child_contact_id} to master {master_contact_id}")
                    except Exception as e:
                        print(f"Error transferring notes for pair {child_contact_id} -> {master_contact_id}: {str(e)}")
                        errors += 1
            return {
                'success': errors == 0,
                'message': f'Notes transfer completed. {notes_transferred} notes transferred, {errors} errors.',
                'processing_id': processing_id,
                'dry_run': dry_run,
                'notes_transferred': notes_transferred,
                'error_count': errors
            }
        except Exception as e:
            print(f"Notes transfer process failed: {str(e)}")
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
        

        async def process_contact_pair(match, match_index, batch_start, client, account_api_keys, dry_run, total_matches, progress):
            contact_pair_number = batch_start + match_index + 1
            try:
                child_contact_id = match.get('child_contact_id')
                master_contact_id = match.get('master_contact_id')
                child_account_id = str(match.get('child_account_id', ''))
                master_account_id = str(match.get('master_account_id', ''))
                match_type = match.get('match_type', 'unknown')
                child_api_key = account_api_keys.get(child_account_id)
                master_api_key = account_api_keys.get(master_account_id)
                progress['status'] = f'processing_contact_{contact_pair_number}/{total_matches}'
                if not all([child_contact_id, master_contact_id, child_api_key, master_api_key]):
                    error_msg = f"Missing required data for contact pair {contact_pair_number}: {child_contact_id} -> {master_contact_id}"
                    progress['recent_errors'].append(error_msg)
                    progress['error_count'] += 1
                    logger.error(error_msg)
                    return 0, False
                logger.info(f"[{contact_pair_number}/{total_matches}] Processing {match_type} match: {child_contact_id} -> {master_contact_id}")
                child_notes = await self._get_contact_notes(client, child_contact_id, child_api_key)
                if not child_notes:
                    logger.info(f"[{contact_pair_number}/{total_matches}] No notes found for child contact {child_contact_id}")
                    progress['success_count'] += 1
                    return 0, True
                if not dry_run:
                    transferred_count = await self._transfer_notes_to_master(client, master_contact_id, master_api_key, child_notes)
                    progress['notes_transferred'] += transferred_count
                    logger.info(f"[{contact_pair_number}/{total_matches}] Successfully transferred {transferred_count}/{len(child_notes)} notes from {child_contact_id} to {master_contact_id}")
                else:
                    progress['notes_transferred'] += len(child_notes)
                    logger.info(f"[{contact_pair_number}/{total_matches}] [DRY RUN] Would transfer {len(child_notes)} notes from {child_contact_id} to {master_contact_id}")
                progress['success_count'] += 1
                return len(child_notes), True
            except Exception as e:
                error_msg = f"[{contact_pair_number}/{total_matches}] Exception processing contact pair {match.get('child_contact_id')} -> {match.get('master_contact_id')}: {str(e)}"
                progress['recent_errors'].append(error_msg)
                progress['error_count'] += 1
                logger.error(error_msg)
                return 0, False
            finally:
                progress['completed'] += 1
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
                if len(progress['recent_errors']) > 10:
                    progress['recent_errors'] = progress['recent_errors'][-10:]
                if progress['completed'] % 5 == 0 or progress['completed'] == total_matches:
                    logger.info(f"Progress: {progress['completed']}/{total_matches} contacts processed, {progress['success_count']} successful, {progress['notes_transferred']} notes transferred")

        async with httpx.AsyncClient(timeout=60) as client:
            for batch_start in range(0, total_matches, batch_size):
                batch_end = min(batch_start + batch_size, total_matches)
                batch_matches = matches[batch_start:batch_end]
                current_batch_num = batch_start // batch_size + 1
                progress['current_batch'] = current_batch_num
                progress['status'] = f'processing_batch_{current_batch_num}'
                logger.info(f"Processing batch {current_batch_num}: contacts {batch_start + 1}-{batch_end}")

                # Process each contact pair in the batch concurrently
                tasks = [process_contact_pair(match, match_index, batch_start, client, account_api_keys, dry_run, total_matches, progress)
                         for match_index, match in enumerate(batch_matches)]
                await asyncio.gather(*tasks)

                logger.info(f"Completed batch {current_batch_num}: {len(batch_matches)} contacts processed")
                if batch_end < total_matches:
                    await asyncio.sleep(0.5)  # Reduced delay for faster throughput

        progress['status'] = 'completed'
        logger.info(f"Notes transfer completed: {processed_contacts} contacts processed, {progress['success_count']} successful, {progress['error_count']} errors, {progress['notes_transferred']} notes transferred")
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
