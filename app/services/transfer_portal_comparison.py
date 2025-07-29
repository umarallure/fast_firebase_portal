import pandas as pd
import json
import os
from datetime import datetime
import logging
from typing import List, Dict, Optional, Tuple
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransferPortalComparison:
    def __init__(self):
        self.account_mapping = self._load_account_mapping()
    
    def _load_account_mapping(self) -> Dict[str, str]:
        """Load account ID to name mapping from environment variables"""
        try:
            subaccounts_str = os.getenv('SUBACCOUNTS', '[]')
            subaccounts = json.loads(subaccounts_str)
            
            # Create mapping from ID to name
            mapping = {}
            for account in subaccounts:
                mapping[str(account['id'])] = account['name']
            
            logger.info(f"Loaded {len(mapping)} account mappings")
            return mapping
        except Exception as e:
            logger.error(f"Error loading account mapping: {e}")
            return {}
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison"""
        if pd.isna(name) or not name:
            return ""
        
        # Convert to lowercase, remove extra spaces, and common punctuation
        normalized = str(name).lower().strip()
        normalized = re.sub(r'[^\w\s]', '', normalized)  # Remove punctuation
        normalized = re.sub(r'\s+', ' ', normalized)     # Normalize spaces
        return normalized
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number for comparison"""
        if pd.isna(phone) or not phone:
            return ""
        
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str(phone))
        
        # If it starts with 1 and has 11 digits, remove the 1
        if len(digits_only) == 11 and digits_only.startswith('1'):
            digits_only = digits_only[1:]
        
        return digits_only
    
    def _load_csv_file(self, file_path: str) -> pd.DataFrame:
        """Load CSV file with error handling and encoding detection"""
        try:
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Loaded {len(df)} records from {file_path} using {encoding} encoding")
                    return df
                except UnicodeDecodeError:
                    continue
                    
            # If all encodings fail, try with error handling
            df = pd.read_csv(file_path, encoding='utf-8', errors='ignore')
            logger.warning(f"Loaded {len(df)} records from {file_path} with error handling (some characters may be corrupted)")
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {e}")
            raise
    
    def process_comparison(self, master_file_path: str, child_file_path: str, output_dir: str = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Compare child CSV with master CSV and return entries not found in master
        
        Args:
            master_file_path: Path to the master CSV file (transferportalmaster.csv)
            child_file_path: Path to the child CSV file (transferportalchild.csv)
            output_dir: Directory to save output files (optional)
        
        Returns:
            Tuple of (new_entries_df, processing_stats)
        """
        try:
            # Load CSV files
            master_df = self._load_csv_file(master_file_path)
            child_df = self._load_csv_file(child_file_path)
            
            # Normalize column names, phone numbers, and names
            master_df['normalized_phone'] = master_df['Customer Phone Number'].apply(self._normalize_phone)
            master_df['normalized_name'] = master_df['Name'].apply(self._normalize_name)
            child_df['normalized_phone'] = child_df['phone'].apply(self._normalize_phone)
            child_df['normalized_name'] = child_df['Contact Name'].apply(self._normalize_name)
            
            # Create composite keys for matching (phone + name)
            master_df['composite_key'] = master_df['normalized_phone'] + '|' + master_df['normalized_name']
            child_df['composite_key'] = child_df['normalized_phone'] + '|' + child_df['normalized_name']
            
            # Create set of composite keys from master for quick lookup
            master_keys = set(master_df['composite_key'].dropna())
            
            # Also create sets for individual phone and name matching (fallback)
            master_phones = set(master_df['normalized_phone'].dropna())
            master_names = set(master_df['normalized_name'].dropna())
            
            # Find child entries not in master using composite key first
            child_df['found_in_master_composite'] = child_df['composite_key'].isin(master_keys)
            child_df['found_in_master_phone'] = child_df['normalized_phone'].isin(master_phones)
            child_df['found_in_master_name'] = child_df['normalized_name'].isin(master_names)
            
            # Consider an entry as "found" if either composite key matches OR both phone and name match individually
            child_df['found_in_master'] = (
                child_df['found_in_master_composite'] | 
                (child_df['found_in_master_phone'] & child_df['found_in_master_name'])
            )
            
            new_entries = child_df[~child_df['found_in_master']].copy()
            
            # Process new entries to match master structure
            processed_entries = self._process_new_entries(new_entries)
            
            # Generate processing statistics
            stats = {
                'total_child_entries': len(child_df),
                'entries_found_in_master': len(child_df[child_df['found_in_master']]),
                'entries_found_by_composite': len(child_df[child_df['found_in_master_composite']]),
                'entries_found_by_phone_and_name': len(child_df[child_df['found_in_master_phone'] & child_df['found_in_master_name']]) - len(child_df[child_df['found_in_master_composite']]),
                'new_entries_count': len(new_entries),
                'processing_timestamp': datetime.now().isoformat(),
                'account_mapping_used': len(self.account_mapping) > 0,
                'matching_method': 'phone_and_name_composite'
            }
            
            # Save output if directory provided
            if output_dir:
                self._save_results(processed_entries, stats, output_dir)
            
            logger.info(f"Processing completed. Found {len(new_entries)} new entries out of {len(child_df)} total.")
            
            return processed_entries, stats
            
        except Exception as e:
            logger.error(f"Error in process_comparison: {e}")
            raise
    
    def _process_new_entries(self, new_entries_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process new entries to match master CSV structure
        
        Expected output columns: Customer Phone Number, Name, Policy Status, GHL Pipeline Stage, CALL CENTER
        """
        if new_entries_df.empty:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=['Customer Phone Number', 'Name', 'Policy Status', 'GHL Pipeline Stage', 'CALL CENTER'])
        
        processed_df = pd.DataFrame()
        
        # Map child columns to master structure - ensure phone numbers are formatted correctly
        processed_df['Customer Phone Number'] = new_entries_df['phone'].astype(str).str.replace(r'\.0$', '', regex=True)
        processed_df['Name'] = new_entries_df['Contact Name']
        processed_df['Policy Status'] = ''  # Leave empty as requested
        processed_df['GHL Pipeline Stage'] = new_entries_df['stage']
        
        # Map Account ID to Account Name using environment mapping
        def map_account_id(account_id):
            if pd.isna(account_id):
                return "Unknown Account"
            account_str = str(int(account_id) if isinstance(account_id, float) else account_id)
            return self.account_mapping.get(account_str, f"Unknown Account {account_str}")
        
        processed_df['CALL CENTER'] = new_entries_df['Account Id'].apply(map_account_id)
        
        # Add metadata columns for tracking
        processed_df['Source'] = 'Transfer Portal Child'
        processed_df['Processing Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        processed_df['Original Pipeline'] = new_entries_df['pipeline']
        
        return processed_df
    
    def _save_results(self, processed_entries: pd.DataFrame, stats: Dict, output_dir: str):
        """Save processing results to files"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save new entries CSV
            csv_filename = f"transfer_portal_new_entries_{timestamp}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            processed_entries.to_csv(csv_path, index=False)
            
            # Save processing stats
            stats_filename = f"transfer_portal_stats_{timestamp}.json"
            stats_path = os.path.join(output_dir, stats_filename)
            with open(stats_path, 'w') as f:
                json.dump(stats, f, indent=2)
            
            logger.info(f"Results saved to {output_dir}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            raise
    
    def generate_summary_report(self, processed_entries: pd.DataFrame, stats: Dict) -> str:
        """Generate a summary report of the processing"""
        report = []
        report.append("Transfer Portal Comparison Summary")
        report.append("=" * 40)
        report.append(f"Processing Date: {stats.get('processing_timestamp', 'Unknown')}")
        report.append(f"Matching Method: Phone Number + Customer Name")
        report.append(f"Total Child Entries Processed: {stats.get('total_child_entries', 0)}")
        report.append(f"Entries Found in Master: {stats.get('entries_found_in_master', 0)}")
        if stats.get('entries_found_by_composite', 0) > 0:
            report.append(f"  - Exact Phone+Name Match: {stats.get('entries_found_by_composite', 0)}")
        if stats.get('entries_found_by_phone_and_name', 0) > 0:
            report.append(f"  - Separate Phone & Name Match: {stats.get('entries_found_by_phone_and_name', 0)}")
        report.append(f"New Entries (Not in Master): {stats.get('new_entries_count', 0)}")
        
        if not processed_entries.empty:
            report.append("\nAccount Distribution of New Entries:")
            account_counts = processed_entries['CALL CENTER'].value_counts()
            for account, count in account_counts.items():
                report.append(f"  {account}: {count} entries")
            
            report.append("\nPipeline Stage Distribution:")
            stage_counts = processed_entries['GHL Pipeline Stage'].value_counts()
            for stage, count in stage_counts.items():
                report.append(f"  {stage}: {count} entries")
        
        return "\n".join(report)


def main():
    """Test function for standalone execution"""
    comparison = TransferPortalComparison()
    
    # Test with the provided CSV files
    master_file = "transferportalmaster.csv"
    child_file = "transferportalchild.csv"
    output_dir = "transfer_portal_results"
    
    try:
        processed_entries, stats = comparison.process_comparison(master_file, child_file, output_dir)
        
        print("Processing completed successfully!")
        print(f"New entries found: {len(processed_entries)}")
        
        # Generate and print summary
        summary = comparison.generate_summary_report(processed_entries, stats)
        print("\n" + summary)
        
    except Exception as e:
        print(f"Error during processing: {e}")


if __name__ == "__main__":
    main()
