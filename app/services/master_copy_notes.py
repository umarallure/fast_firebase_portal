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

class MasterCopyNotesService:
    def __init__(self):
        self.base_dir = os.path.join(os.getcwd(), "master_copy_data")
        self.backup_dir = os.path.join(self.base_dir, "backups")
        self.history_dir = os.path.join(self.base_dir, "history")
        self.template_dir = os.path.join(self.base_dir, "templates")
        
        # Ensure directories exist
        for directory in [self.base_dir, self.backup_dir, self.history_dir, self.template_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Initialize templates
        self._create_templates()
        
        # In-memory progress tracking
        self.progress_tracker = {}

    def _create_templates(self):
        """Create CSV templates for download"""
        
        # Basic template
        basic_template = pd.DataFrame({
            'Contact ID': ['contact_123', 'contact_456'],
            'Notes': ['Follow up on insurance quote', 'Customer requested policy review'],
            'Account Id': ['account_789', 'account_789']
        })
        basic_template.to_csv(os.path.join(self.template_dir, 'basic_template.csv'), index=False)
        
        # Advanced template with additional fields
        advanced_template = pd.DataFrame({
            'Contact ID': ['contact_123', 'contact_456'],
            'Notes': ['Follow up on insurance quote', 'Customer requested policy review'],
            'Account Id': ['account_789', 'account_789'],
            'Priority': [3, 1],
            'Category': ['Follow-up', 'Review'],
            'Scheduled Date': ['2025-07-20', '2025-07-25'],
            'Agent': ['John Doe', 'Jane Smith']
        })
        advanced_template.to_csv(os.path.join(self.template_dir, 'advanced_template.csv'), index=False)
        
        # Sample data with more realistic examples
        sample_data = pd.DataFrame({
            'Contact ID': [
                'example_contact_001', 'example_contact_002', 'example_contact_003',
                'example_contact_004', 'example_contact_005'
            ],
            'Notes': [
                'Initial consultation completed. Customer interested in term life insurance.',
                'Quote sent for auto insurance. Waiting for customer response.',
                'Policy renewal reminder sent. Customer needs to update beneficiary info.',
                'Claim processed successfully. Customer satisfied with service.',
                'New customer onboarding scheduled for next week.'
            ],
            'Account Id': [
                'sample_account_123', 'sample_account_123', 'sample_account_456',
                'sample_account_456', 'sample_account_789'
            ],
            'Priority': [2, 3, 1, 4, 2],
            'Category': ['Consultation', 'Quote', 'Renewal', 'Claim', 'Onboarding']
        })
        sample_data.to_csv(os.path.join(self.template_dir, 'sample_data.csv'), index=False)

    async def validate_csv(self, csv_content: str) -> Dict[str, Any]:
        """Validate CSV content and return validation results"""
        
        try:
            df = pd.read_csv(io.StringIO(csv_content))
            
            required_columns = ['Contact ID', 'Notes', 'Account Id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            validation_result = {
                'is_valid': len(missing_columns) == 0,
                'total_rows': len(df),
                'required_columns': required_columns,
                'missing_columns': missing_columns,
                'errors': [],
                'warnings': []
            }
            
            if not validation_result['is_valid']:
                validation_result['errors'].append(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Validate data integrity
            valid_rows = 0
            invalid_rows = 0
            empty_notes = 0
            
            for index, row in df.iterrows():
                row_errors = []
                
                # Check Contact ID
                if pd.isna(row.get('Contact ID')) or str(row.get('Contact ID')).strip() == '':
                    row_errors.append(f"Row {index + 2}: Missing Contact ID")
                
                # Check Account Id
                if pd.isna(row.get('Account Id')) or str(row.get('Account Id')).strip() == '':
                    row_errors.append(f"Row {index + 2}: Missing Account Id")
                
                # Check Notes
                notes = str(row.get('Notes', '')).strip()
                if not notes or notes == 'nan':
                    empty_notes += 1
                    validation_result['warnings'].append(f"Row {index + 2}: Empty notes (will be skipped)")
                
                if row_errors:
                    invalid_rows += 1
                    validation_result['errors'].extend(row_errors)
                else:
                    valid_rows += 1
            
            validation_result.update({
                'valid_rows': valid_rows,
                'invalid_rows': invalid_rows,
                'empty_notes': empty_notes
            })
            
            # Additional checks
            if len(df) == 0:
                validation_result['errors'].append("CSV file is empty")
                validation_result['is_valid'] = False
            
            # Check for duplicate Contact IDs
            duplicates = df[df.duplicated(subset=['Contact ID'], keep=False)]
            if not duplicates.empty:
                validation_result['warnings'].append(f"Found {len(duplicates)} duplicate Contact IDs")
            
            return validation_result
            
        except Exception as e:
            return {
                'is_valid': False,
                'total_rows': 0,
                'errors': [f"CSV parsing error: {str(e)}"],
                'warnings': []
            }

    async def create_backup(self, operation_name: str = None) -> Dict[str, Any]:
        """Create a backup of current data state"""
        
        backup_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        backup_info = {
            'id': backup_id,
            'name': operation_name or f"Manual Backup {timestamp}",
            'created_at': datetime.now().isoformat(),
            'description': f"Backup created for operation: {operation_name or 'Manual backup'}",
            'size': '0 KB',  # Will be updated after creation
            'record_count': 0
        }
        
        # In a real implementation, you would backup actual data here
        # For now, we'll create a placeholder backup file
        backup_filename = f"backup_{backup_id}_{timestamp}.json"
        backup_path = os.path.join(self.backup_dir, backup_filename)
        
        with open(backup_path, 'w') as f:
            json.dump(backup_info, f, indent=2)
        
        # Update size
        backup_info['size'] = f"{os.path.getsize(backup_path)} bytes"
        
        logger.info(f"Backup created: {backup_id}")
        return {'success': True, 'backup_id': backup_id, 'backup_info': backup_info}

    async def process_master_copy(
        self, 
        csv_content: str, 
        enable_backup: bool = True,
        enable_validation: bool = True,
        batch_size: int = 25,
        operation_name: str = None
    ) -> Dict[str, Any]:
        """Process the master copy notes operation"""
        
        processing_id = str(uuid.uuid4())
        operation_start = datetime.now()
        
        # Initialize progress tracking
        self.progress_tracker[processing_id] = {
            'status': 'initializing',
            'total': 0,
            'completed': 0,
            'success_count': 0,
            'error_count': 0,
            'current_batch': 0,
            'recent_errors': [],
            'start_time': operation_start,
            'eta': None,
            'rate': None
        }
        
        try:
            # Step 1: Validation
            if enable_validation:
                self.progress_tracker[processing_id]['status'] = 'validating'
                validation_result = await self.validate_csv(csv_content)
                
                if not validation_result['is_valid']:
                    self.progress_tracker[processing_id]['status'] = 'failed'
                    return {
                        'success': False,
                        'message': 'Validation failed. Please check the validation tab for details.',
                        'validation': validation_result,
                        'processing_id': processing_id
                    }
            
            # Step 2: Create backup
            if enable_backup:
                self.progress_tracker[processing_id]['status'] = 'creating_backup'
                backup_result = await self.create_backup(operation_name)
                if not backup_result['success']:
                    logger.warning("Backup creation failed, continuing without backup")
            
            # Step 3: Parse CSV data
            self.progress_tracker[processing_id]['status'] = 'parsing'
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Filter out rows with empty notes
            df = df.dropna(subset=['Notes'])
            df = df[df['Notes'].astype(str).str.strip() != '']
            
            total_rows = len(df)
            self.progress_tracker[processing_id]['total'] = total_rows
            
            if total_rows == 0:
                self.progress_tracker[processing_id]['status'] = 'failed'
                return {
                    'success': False,
                    'message': 'No valid rows found in CSV after filtering.',
                    'processing_id': processing_id
                }
            
            # Step 4: Prepare subaccount API keys
            subaccounts = settings.subaccounts_list
            account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}
            
            # Step 5: Process in batches
            self.progress_tracker[processing_id]['status'] = 'processing'
            errors = []
            success_count = 0
            
            # Start background processing
            asyncio.create_task(self._process_batches(
                processing_id, df, account_api_keys, batch_size, operation_name
            ))
            
            return {
                'success': True,
                'message': f'Master copy process started. Processing {total_rows} records in batches of {batch_size}.',
                'processing_id': processing_id,
                'validation': validation_result if enable_validation else None
            }
            
        except Exception as e:
            self.progress_tracker[processing_id]['status'] = 'failed'
            logger.error(f"Master copy process failed: {str(e)}")
            return {
                'success': False,
                'message': f'Process failed: {str(e)}',
                'processing_id': processing_id
            }

    async def _process_batches(
        self, 
        processing_id: str, 
        df: pd.DataFrame, 
        account_api_keys: Dict[str, str], 
        batch_size: int,
        operation_name: str
    ):
        """Process CSV data in batches"""
        
        progress = self.progress_tracker[processing_id]
        total_rows = len(df)
        
        async with httpx.AsyncClient(timeout=30) as client:
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                batch_df = df.iloc[batch_start:batch_end]
                
                progress['current_batch'] = batch_start // batch_size + 1
                progress['status'] = f'processing_batch_{progress["current_batch"]}'
                
                # Process batch
                for index, row in batch_df.iterrows():
                    try:
                        contact_id = str(row.get('Contact ID', '')).strip()
                        notes = str(row.get('Notes', '')).strip()
                        account_id = str(row.get('Account Id', '')).strip()
                        
                        api_key = account_api_keys.get(account_id)
                        
                        if not contact_id or not api_key or not notes:
                            error_msg = f"Missing data for contact {contact_id} (row {index + 2})"
                            progress['recent_errors'].append(error_msg)
                            progress['error_count'] += 1
                            continue
                        
                        # Make API call
                        url = f"https://rest.gohighlevel.com/v1/contacts/{contact_id}/notes/"
                        payload = {"body": notes}
                        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
                        
                        resp = await client.post(url, json=payload, headers=headers)
                        
                        if resp.status_code == 200:
                            progress['success_count'] += 1
                        else:
                            error_msg = f"API error for {contact_id}: {resp.text}"
                            progress['recent_errors'].append(error_msg)
                            progress['error_count'] += 1
                            
                    except Exception as e:
                        error_msg = f"Exception for contact {contact_id}: {str(e)}"
                        progress['recent_errors'].append(error_msg)
                        progress['error_count'] += 1
                    
                    finally:
                        progress['completed'] += 1
                        
                        # Update ETA and rate
                        elapsed = (datetime.now() - progress['start_time']).total_seconds()
                        if elapsed > 0:
                            rate = progress['completed'] / elapsed * 60  # items per minute
                            progress['rate'] = f"{rate:.1f}"
                            
                            if progress['completed'] > 0:
                                eta_seconds = (total_rows - progress['completed']) / (progress['completed'] / elapsed)
                                progress['eta'] = f"{eta_seconds/60:.1f} minutes"
                        
                        # Keep only recent errors (last 10)
                        if len(progress['recent_errors']) > 10:
                            progress['recent_errors'] = progress['recent_errors'][-10:]
                
                # Small delay between batches
                await asyncio.sleep(0.5)
        
        # Mark as completed
        progress['status'] = 'completed'
        
        # Save operation history
        await self._save_operation_history(processing_id, operation_name, progress)

    async def _save_operation_history(self, processing_id: str, operation_name: str, progress: Dict):
        """Save operation to history"""
        
        operation_record = {
            'id': processing_id,
            'name': operation_name or f"Operation {processing_id[:8]}",
            'created_at': progress['start_time'].isoformat(),
            'completed_at': datetime.now().isoformat(),
            'status': progress['status'],
            'total_records': progress['total'],
            'success_count': progress['success_count'],
            'error_count': progress['error_count'],
            'final_errors': progress['recent_errors']
        }
        
        history_file = os.path.join(self.history_dir, f"operation_{processing_id}.json")
        with open(history_file, 'w') as f:
            json.dump(operation_record, f, indent=2)

    def get_progress(self, processing_id: str) -> Dict[str, Any]:
        """Get progress for a processing operation"""
        
        if processing_id not in self.progress_tracker:
            return {'success': False, 'message': 'Processing ID not found'}
        
        return {
            'success': True,
            'progress': self.progress_tracker[processing_id]
        }

    def get_template(self, template_type: str) -> str:
        """Get CSV template content"""
        
        template_files = {
            'basic': 'basic_template.csv',
            'advanced': 'advanced_template.csv',
            'sample': 'sample_data.csv'
        }
        
        if template_type not in template_files:
            raise ValueError(f"Invalid template type: {template_type}")
        
        template_path = os.path.join(self.template_dir, template_files[template_type])
        
        if not os.path.exists(template_path):
            self._create_templates()  # Recreate if missing
        
        with open(template_path, 'r') as f:
            return f.read()

    def get_backups(self) -> List[Dict[str, Any]]:
        """Get list of available backups"""
        
        backups = []
        
        for filename in os.listdir(self.backup_dir):
            if filename.endswith('.json'):
                backup_path = os.path.join(self.backup_dir, filename)
                try:
                    with open(backup_path, 'r') as f:
                        backup_info = json.load(f)
                    backups.append(backup_info)
                except Exception as e:
                    logger.error(f"Error reading backup file {filename}: {str(e)}")
        
        # Sort by creation date (newest first)
        backups.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return backups

    def get_operation_history(self) -> List[Dict[str, Any]]:
        """Get operation history"""
        
        operations = []
        
        for filename in os.listdir(self.history_dir):
            if filename.endswith('.json'):
                history_path = os.path.join(self.history_dir, filename)
                try:
                    with open(history_path, 'r') as f:
                        operation_info = json.load(f)
                    operations.append(operation_info)
                except Exception as e:
                    logger.error(f"Error reading history file {filename}: {str(e)}")
        
        # Sort by creation date (newest first)
        operations.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return operations[:50]  # Return last 50 operations

# Create global instance
master_copy_service = MasterCopyNotesService()
