"""
Test script for Transfer Portal Comparison automation
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from app.services.transfer_portal_comparison import TransferPortalComparison
import pandas as pd

def test_transfer_portal_comparison():
    """Test the transfer portal comparison with the provided CSV files"""
    
    print("Testing Transfer Portal Comparison Automation")
    print("=" * 50)
    
    # Initialize the comparison service
    comparison = TransferPortalComparison()
    
    # Test with the provided CSV files
    master_file = "transferportalmaster.csv"
    child_file = "transferportalchild.csv"
    output_dir = "test_results"
    
    try:
        # Process the comparison
        processed_entries, stats = comparison.process_comparison(master_file, child_file, output_dir)
        
        print("Processing completed successfully!")
        print(f"Processing Statistics:")
        print(f"- Total Child Entries: {stats.get('total_child_entries', 0)}")
        print(f"- Entries Found in Master: {stats.get('entries_found_in_master', 0)}")
        print(f"- New Entries (Not in Master): {stats.get('new_entries_count', 0)}")
        
        if not processed_entries.empty:
            print(f"\nFirst 5 new entries:")
            print(processed_entries[['Customer Phone Number', 'Name', 'GHL Pipeline Stage', 'CALL CENTER']].head())
            
            print(f"\nAccount Distribution:")
            account_counts = processed_entries['CALL CENTER'].value_counts()
            for account, count in account_counts.items():
                print(f"  {account}: {count} entries")
        else:
            print("\nNo new entries found - all child entries exist in master file.")
        
        # Generate and print summary
        summary = comparison.generate_summary_report(processed_entries, stats)
        print("\nDetailed Summary Report:")
        print("-" * 30)
        print(summary)
        
        return True
        
    except Exception as e:
        print(f"Error during processing: {e}")
        return False

def test_phone_normalization():
    """Test phone number normalization"""
    print("\nTesting Phone Number Normalization")
    print("-" * 30)
    
    comparison = TransferPortalComparison()
    
    test_phones = [
        "+13139350685",
        "(313) 935-0685", 
        "313-935-0685",
        "3139350685",
        "13139350685",
        "(309) 964-9062"
    ]
    
    for phone in test_phones:
        normalized = comparison._normalize_phone(phone)
        print(f"{phone:20} -> {normalized}")

def test_account_mapping():
    """Test account ID to name mapping"""
    print("\nTesting Account Mapping")
    print("-" * 20)
    
    comparison = TransferPortalComparison()
    
    print(f"Loaded {len(comparison.account_mapping)} account mappings:")
    for account_id, account_name in list(comparison.account_mapping.items())[:10]:  # Show first 10
        print(f"  ID {account_id}: {account_name}")
    
    if len(comparison.account_mapping) > 10:
        print(f"  ... and {len(comparison.account_mapping) - 10} more")

if __name__ == "__main__":
    print("Starting Transfer Portal Comparison Tests\n")
    
    # Test individual components
    test_phone_normalization()
    test_account_mapping()
    
    # Test the main functionality
    success = test_transfer_portal_comparison()
    
    if success:
        print("\n✅ All tests completed successfully!")
    else:
        print("\n❌ Tests failed!")
