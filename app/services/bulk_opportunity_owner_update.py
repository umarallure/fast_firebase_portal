import asyncio
import csv
import io
import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import pandas as pd
import httpx
from app.config import settings

logger = logging.getLogger(__name__)

class BulkOpportunityOwnerUpdateService:
    def __init__(self):
        self.base_dir = os.path.join(os.getcwd(), "opportunity_owner_updates")
        self.results_dir = os.path.join(self.base_dir, "results")
        self.logs_dir = os.path.join(self.base_dir, "logs")
        
        # Ensure directories exist
        for directory in [self.base_dir, self.results_dir, self.logs_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # In-memory progress tracking
        self.progress_tracker = {}

    def parse_csv(self, csv_content: str) -> Dict[str, Any]:
        """Parse and validate CSV file for opportunity owner updates"""
        
        try:
            # Parse CSV
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Normalize column names for consistent access
            # Handle variations in column names (case sensitivity, spaces, etc.)
            column_mapping = {}
            for col in df.columns:
                normalized = col.strip().lower()
                if normalized in ['account id', 'account_id', 'accountid']:
                    column_mapping[col] = 'Account ID'
                elif normalized in ['opportunity id', 'opportunity_id', 'opportunityid']:
                    column_mapping[col] = 'Opportunity ID'
                elif normalized in ['pipeline id', 'pipeline_id', 'pipelineid']:
                    column_mapping[col] = 'Pipeline ID'
                elif normalized == 'stage':
                    column_mapping[col] = 'stage'
                elif normalized in ['opportunity name', 'opportunity_name', 'opportunityname']:
                    column_mapping[col] = 'Opportunity Name'
                elif normalized in ['contact name', 'contact_name', 'contactname']:
                    column_mapping[col] = 'Contact Name'
            
            # Rename columns to standardized names
            df = df.rename(columns=column_mapping)
            
            # Required columns for opportunity owner update
            required_columns = ['Opportunity ID', 'Pipeline ID', 'Account ID', 'stage']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return {
                    'success': False,
                    'error': f'Missing required columns: {missing_columns}',
                    'opportunities': [],
                    'summary': {}
                }
            
            # Fill NaN values
            df = df.fillna('')
            
            # Extract opportunities data
            opportunities = []
            account_ids = set()
            
            # Define the three agents with their distribution ratios
            agents = {
                'bryan': {'id': '1iulHgfbKF7ufdpt6osS', 'name': 'Bryan', 'ratio': 0.40},
                'kyla': {'id': 'GkBzcbgHkCNaoSKKdIkZ', 'name': 'Kyla', 'ratio': 0.30},
                'ira': {'id': '3swinpnwu3nIQh3NrmTX', 'name': 'Ira', 'ratio': 0.30}
            }
            
            # Collect all opportunities first
            all_opportunities = []
            
            for idx, row in df.iterrows():
                opportunity_id = str(row.get('Opportunity ID', '')).strip()
                pipeline_id = str(row.get('Pipeline ID', '')).strip()
                account_id = str(row.get('Account ID', '')).strip()
                stage = str(row.get('stage', '')).strip()
                
                # Skip rows with missing critical data
                if not all([opportunity_id, pipeline_id, account_id]):
                    logger.warning(f"Skipping row {idx + 1}: Missing critical data")
                    continue
                
                opp_data = {
                    'opportunity_id': opportunity_id,
                    'pipeline_id': pipeline_id,
                    'account_id': account_id,
                    'opportunity_name': str(row.get('Opportunity Name', '')).strip(),
                    'contact_name': str(row.get('Contact Name', '')).strip(),
                    'current_stage': stage,
                    'status': str(row.get('status', '')).strip(),
                    'row_number': idx + 1
                }
                
                all_opportunities.append(opp_data)
            
            # Assign opportunities to agents based on ratio distribution
            opportunities = []
            total_opportunities = len(all_opportunities)
            
            if total_opportunities > 0:
                # Calculate distribution based on ratios
                bryan_count = round(total_opportunities * agents['bryan']['ratio'])
                kyla_count = round(total_opportunities * agents['kyla']['ratio'])
                ira_count = total_opportunities - bryan_count - kyla_count  # Ensure total matches exactly
                
                # Create assignment counters
                assignment_counts = {
                    'bryan': {'count': bryan_count, 'assigned': 0},
                    'kyla': {'count': kyla_count, 'assigned': 0},
                    'ira': {'count': ira_count, 'assigned': 0}
                }
                
                # Assign opportunities in round-robin fashion to ensure even distribution
                agent_keys = ['bryan', 'kyla', 'ira']
                current_agent_idx = 0
                
                for opp in all_opportunities:
                    # Find next agent with remaining capacity
                    assigned = False
                    attempts = 0
                    
                    while not assigned and attempts < len(agent_keys):
                        agent_key = agent_keys[current_agent_idx]
                        
                        if assignment_counts[agent_key]['assigned'] < assignment_counts[agent_key]['count']:
                            # Assign to this agent
                            agent_info = agents[agent_key]
                            opp['assigned_to'] = agent_info['id']
                            opp['assigned_owner_name'] = agent_info['name']
                            assignment_counts[agent_key]['assigned'] += 1
                            assigned = True
                        
                        current_agent_idx = (current_agent_idx + 1) % len(agent_keys)
                        attempts += 1
                    
                    if not assigned:
                        # Fallback: assign to Bryan if no capacity left (shouldn't happen with proper calculation)
                        opp['assigned_to'] = agents['bryan']['id']
                        opp['assigned_owner_name'] = agents['bryan']['name']
                    
                    opportunities.append(opp)
                    account_ids.add(opp['account_id'])
            
            # Calculate final assignment distribution for summary
            bryan_final = len([o for o in opportunities if o['assigned_to'] == agents['bryan']['id']])
            kyla_final = len([o for o in opportunities if o['assigned_to'] == agents['kyla']['id']])
            ira_final = len([o for o in opportunities if o['assigned_to'] == agents['ira']['id']])
            
            summary = {
                'total_opportunities': len(opportunities),
                'unique_accounts': len(account_ids),
                'account_ids': list(account_ids),
                'assignment_distribution': {
                    'bryan': {
                        'id': agents['bryan']['id'],
                        'name': agents['bryan']['name'],
                        'count': bryan_final,
                        'percentage': round((bryan_final / len(opportunities) * 100), 1) if opportunities else 0
                    },
                    'kyla': {
                        'id': agents['kyla']['id'],
                        'name': agents['kyla']['name'],
                        'count': kyla_final,
                        'percentage': round((kyla_final / len(opportunities) * 100), 1) if opportunities else 0
                    },
                    'ira': {
                        'id': agents['ira']['id'],
                        'name': agents['ira']['name'],
                        'count': ira_final,
                        'percentage': round((ira_final / len(opportunities) * 100), 1) if opportunities else 0
                    }
                }
            }
            
            logger.info(f"Parsed CSV: {len(opportunities)} opportunities across {len(account_ids)} accounts")
            
            return {
                'success': True,
                'opportunities': opportunities,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"CSV parsing error: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'opportunities': [],
                'summary': {}
            }

    async def validate_user_ids(self, opportunities: List[Dict], api_key: str) -> Dict[str, Any]:
        """Validate that assignedTo user IDs exist in GHL"""
        
        unique_user_ids = set(opp['assigned_to'] for opp in opportunities)
        validation_results = {
            'valid_users': [],
            'invalid_users': [],
            'validation_errors': []
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            for user_id in unique_user_ids:
                try:
                    # Try to get user info to validate the ID exists
                    url = f"https://rest.gohighlevel.com/v1/users/{user_id}"
                    headers = {
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    }
                    
                    response = await client.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        user_data = response.json()
                        validation_results['valid_users'].append({
                            'id': user_id,
                            'name': user_data.get('name', 'Unknown'),
                            'email': user_data.get('email', 'Unknown')
                        })
                    else:
                        validation_results['invalid_users'].append({
                            'id': user_id,
                            'error': f"HTTP {response.status_code}: {response.text[:100]}"
                        })
                        
                except Exception as e:
                    validation_results['invalid_users'].append({
                        'id': user_id,
                        'error': f"Exception: {str(e)}"
                    })
        
        return validation_results

    async def process_opportunity_owner_updates(
        self, 
        opportunities: List[Dict], 
        dry_run: bool = False,
        batch_size: int = 10
    ) -> Dict[str, Any]:
        """Process opportunity owner updates in batches"""
        
        processing_id = str(uuid.uuid4())
        operation_start = datetime.now()
        
        # Initialize progress tracking
        self.progress_tracker[processing_id] = {
            'status': 'initializing',
            'total': len(opportunities),
            'completed': 0,
            'success_count': 0,
            'error_count': 0,
            'current_batch': 0,
            'recent_errors': [],
            'start_time': operation_start.isoformat(),
            'eta': None,
            'rate': None,
            'dry_run': dry_run
        }
        
        try:
            # Prepare subaccount API keys
            subaccounts = settings.subaccounts_list
            account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}
            
            logger.info(f"Starting opportunity owner updates: {len(opportunities)} opportunities, batch size: {batch_size}, dry_run: {dry_run}")
            
            # Start background processing
            asyncio.create_task(self._process_update_batches(
                processing_id, opportunities, account_api_keys, batch_size, dry_run
            ))
            
            return {
                'success': True,
                'message': f'Opportunity owner update process started. Processing {len(opportunities)} opportunities.',
                'processing_id': processing_id,
                'dry_run': dry_run
            }
            
        except Exception as e:
            self.progress_tracker[processing_id]['status'] = 'failed'
            logger.error(f"Opportunity owner update process failed: {str(e)}")
            return {
                'success': False,
                'message': f'Process failed: {str(e)}',
                'processing_id': processing_id
            }

    async def _process_update_batches(
        self, 
        processing_id: str, 
        opportunities: List[Dict], 
        account_api_keys: Dict[str, str], 
        batch_size: int,
        dry_run: bool
    ):
        """Process opportunity owner updates in batches"""
        
        progress = self.progress_tracker[processing_id]
        total_opportunities = len(opportunities)
        processed_opportunities = 0
        
        logger.info(f"Processing {total_opportunities} opportunity owner updates")
        
        async with httpx.AsyncClient(timeout=60) as client:
            for batch_start in range(0, total_opportunities, batch_size):
                batch_end = min(batch_start + batch_size, total_opportunities)
                batch_opportunities = opportunities[batch_start:batch_end]
                current_batch_num = batch_start // batch_size + 1
                
                progress['current_batch'] = current_batch_num
                progress['status'] = f'processing_batch_{current_batch_num}'
                
                logger.info(f"Processing batch {current_batch_num}: opportunities {batch_start + 1}-{batch_end}")
                
                # Process each opportunity in the batch
                for opp_index, opportunity in enumerate(batch_opportunities):
                    opportunity_number = batch_start + opp_index + 1
                    
                    try:
                        opportunity_id = opportunity['opportunity_id']
                        pipeline_id = opportunity['pipeline_id']
                        assigned_to = opportunity['assigned_to']
                        account_id = opportunity['account_id']
                        
                        # Update progress status
                        progress['status'] = f'processing_opportunity_{opportunity_number}/{total_opportunities}'
                        
                        # Get API key for this account
                        api_key = account_api_keys.get(account_id)
                        if not api_key:
                            error_msg = f"[{opportunity_number}/{total_opportunities}] No API key found for account {account_id}"
                            progress['recent_errors'].append(error_msg)
                            progress['error_count'] += 1
                            logger.error(error_msg)
                            continue
                        
                        logger.info(f"[{opportunity_number}/{total_opportunities}] Updating opportunity {opportunity_id} owner to {assigned_to} (Account: {account_id})")
                        
                        # Update opportunity owner
                        if not dry_run:
                            success = await self._update_opportunity_owner(
                                client, pipeline_id, opportunity_id, assigned_to, api_key, opportunity
                            )
                            
                            if success:
                                progress['success_count'] += 1
                                logger.info(f"[{opportunity_number}/{total_opportunities}] Successfully updated opportunity {opportunity_id}")
                            else:
                                progress['error_count'] += 1
                                logger.error(f"[{opportunity_number}/{total_opportunities}] Failed to update opportunity {opportunity_id}")
                        else:
                            progress['success_count'] += 1
                            logger.info(f"[{opportunity_number}/{total_opportunities}] [DRY RUN] Would update opportunity {opportunity_id} owner to {assigned_to}")
                        
                        processed_opportunities += 1
                        
                    except Exception as e:
                        error_msg = f"[{opportunity_number}/{total_opportunities}] Exception updating opportunity {opportunity.get('opportunity_id')}: {str(e)}"
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
                            
                            if progress['completed'] < total_opportunities:
                                remaining = total_opportunities - progress['completed']
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
                        
                        # Log progress every 5 opportunities
                        if progress['completed'] % 5 == 0 or progress['completed'] == total_opportunities:
                            logger.info(f"Progress: {progress['completed']}/{total_opportunities} opportunities processed, {progress['success_count']} successful, {progress['error_count']} errors")
                
                # Log batch completion
                logger.info(f"Completed batch {current_batch_num}: {len(batch_opportunities)} opportunities processed")
                
                # Small delay between batches to prevent rate limiting
                if batch_end < total_opportunities:
                    await asyncio.sleep(1.0)
        
        # Mark as completed
        progress['status'] = 'completed'
        logger.info(f"Opportunity owner updates completed: {processed_opportunities} opportunities processed, {progress['success_count']} successful, {progress['error_count']} errors")
        
        # Save operation results
        await self._save_operation_results(processing_id, opportunities, progress)

    async def _update_opportunity_owner(
        self, 
        client: httpx.AsyncClient, 
        pipeline_id: str, 
        opportunity_id: str, 
        assigned_to: str,
        api_key: str,
        opportunity_data: Dict
    ) -> bool:
        """Update opportunity owner using GHL API"""
        
        try:
            url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities/{opportunity_id}"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            # Prepare update payload - only update the assignedTo field
            payload = {
                "assignedTo": assigned_to
            }
            
            # Add other fields if they exist to maintain data integrity
            if opportunity_data.get('opportunity_name'):
                payload['title'] = opportunity_data['opportunity_name']
            
            if opportunity_data.get('status'):
                payload['status'] = opportunity_data['status']
            
            response = await client.put(url, headers=headers, json=payload)
            response.raise_for_status()
            
            logger.info(f"Successfully updated opportunity {opportunity_id} owner to {assigned_to}")
            return True
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.error(f"Opportunity {opportunity_id} or pipeline {pipeline_id} not found")
            elif e.response.status_code == 403:
                logger.error(f"Access denied for opportunity {opportunity_id} - check API key permissions")
            else:
                logger.error(f"HTTP error updating opportunity {opportunity_id}: {e.response.status_code} - {e.response.text}")
            return False
            
        except Exception as e:
            logger.error(f"Error updating opportunity {opportunity_id}: {str(e)}")
            return False

    async def _save_operation_results(self, processing_id: str, opportunities: List[Dict], progress: Dict):
        """Save operation results to file"""
        
        operation_record = {
            'processing_id': processing_id,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_opportunities': progress['total'],
                'successful_updates': progress['success_count'],
                'failed_updates': progress['error_count'],
                'dry_run': progress['dry_run']
            },
            'opportunities_processed': len(opportunities),
            'final_errors': progress['recent_errors']
        }
        
        results_file = os.path.join(self.results_dir, f"opportunity_owner_update_{processing_id}.json")
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

    def generate_sample_csv(self) -> str:
        """Generate a sample CSV template for opportunity owner updates"""
        
        sample_data = [
            {
                'Opportunity Name': 'John Doe - Life Insurance',
                'Contact Name': 'John Doe',
                'phone': '15551234567',
                'email': 'john@example.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'First Draft Payment Failure',  # This will be assigned to Bryan (40%), Ira (20%), or Kyla (20%)
                'Lead Value': '235.17',
                'source': 'Jotform-call center',
                'Created on': '2025-07-15T12:39:22.999Z',
                'Updated on': '2025-07-15T12:39:23.115Z',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': 'Agent Name',
                'Notes': 'Sample note content',
                'tags': 'call center,master',
                'Engagement Score': '0',
                'status': 'open',
                'Opportunity ID': '5syMt7ZakkZvU0afiXff',  # Required
                'Contact ID': 'SWIxskF22h0FvMq6eYLF',
                'Pipeline Stage ID': '993da8ed-ccd9-4c57-a9f4-7fa749ced916',
                'Pipeline ID': 'OYXsfalmHRurVTGchofz',  # Required
                'Days Since Last Stage Change Date': '2 Days',
                'Days Since Last Status Change Date': '2 Days',
                'Days Since Last Updated': '2 Days',
                'Account ID': '26'  # Required
            },
            {
                'Opportunity Name': 'Jane Smith - Health Insurance',
                'Contact Name': 'Jane Smith',
                'phone': '15559876543',
                'email': 'jane@example.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Pending Lapse',  # This will be assigned to Ira (50%) or Kyla (50%)
                'Lead Value': '189.50',
                'source': 'Jotform-call center',
                'Created on': '2025-07-15T12:40:22.999Z',
                'Updated on': '2025-07-15T12:40:23.115Z',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': 'Agent Name',
                'Notes': 'Sample note content 2',
                'tags': 'call center,master',
                'Engagement Score': '0',
                'status': 'open',
                'Opportunity ID': 'HECOUT6lj9VYV750nJ1z',  # Required
                'Contact ID': 'BNHWsbOsJNB5WLToPM5N',
                'Pipeline Stage ID': '40d37746-094d-4cdd-8376-d6f58c9b33bb',
                'Pipeline ID': 'OYXsfalmHRurVTGchofz',  # Required
                'Days Since Last Stage Change Date': '3 Days',
                'Days Since Last Status Change Date': '3 Days',
                'Days Since Last Updated': '3 Days',
                'Account ID': '26'  # Required
            },
            {
                'Opportunity Name': 'Bob Wilson - Auto Insurance',
                'Contact Name': 'Bob Wilson',
                'phone': '15555678901',
                'email': 'bob@example.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Quote Sent',  # Other stage - will be assigned alternating between Ira and Kyla
                'Lead Value': '150.00',
                'source': 'Jotform-call center',
                'Created on': '2025-07-15T12:41:22.999Z',
                'Updated on': '2025-07-15T12:41:23.115Z',
                'lost reason ID': '',
                'lost reason name': '',
                'Followers': 'Agent Name',
                'Notes': 'Sample note content 3',
                'tags': 'call center,master',
                'Engagement Score': '0',
                'status': 'open',
                'Opportunity ID': 'XWcOEfbKspwqMmtyhVNv',  # Required
                'Contact ID': 'GfmTw3mDK2anTNAUSIqB',
                'Pipeline Stage ID': '12345678-1234-1234-1234-123456789012',
                'Pipeline ID': 'OYXsfalmHRurVTGchofz',  # Required
                'Days Since Last Stage Change Date': '1 Days',
                'Days Since Last Status Change Date': '1 Days',
                'Days Since Last Updated': '1 Days',
                'Account ID': '26'  # Required
            }
        ]
        
        # Convert to CSV
        output = io.StringIO()
        if sample_data:
            df = pd.DataFrame(sample_data)
            df.to_csv(output, index=False)
        
        return output.getvalue()

# Create global instance
bulk_opportunity_owner_service = BulkOpportunityOwnerUpdateService()
