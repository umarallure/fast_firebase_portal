"""
Quick Subaccount Test - Shows which API key will be used for each contact
=======================================================================

This script checks the first 5 contacts and shows which subaccount/API key
will be used for each contact based on the source field.
"""

import pandas as pd
import json
from app.config import settings

def test_subaccount_mapping():
    """Test which subaccount each contact maps to"""
    
    # Load CSV
    df = pd.read_csv("database.csv")
    test_df = df.head(5)
    
    # Load subaccounts
    subaccounts = settings.subaccounts_list
    
    print("=== SUBACCOUNT MAPPING TEST ===")
    print(f"Available subaccounts: {len(subaccounts)}")
    print()
    
    # Show available subaccounts
    print("Available Subaccounts:")
    for sub in subaccounts:
        print(f"  ID: {sub.get('id', 'N/A')} - Name: {sub.get('name', 'N/A')}")
    print()
    
    print("Contact -> Subaccount Mapping:")
    print("-" * 80)
    
    for index, row in test_df.iterrows():
        contact_id = row['contact_id']
        source_id = str(row['source'])
        contact_name = row['full_name']
        center = row['center']
        
        # Find matching subaccount
        matching_sub = None
        for sub in subaccounts:
            if str(sub.get("id")) == source_id:
                matching_sub = sub
                break
        
        if matching_sub:
            sub_name = matching_sub.get('name', 'Unknown')
            api_key_preview = matching_sub.get('api_key', '')[:20] + "..." if matching_sub.get('api_key') else 'None'
            status = "✓ FOUND"
        else:
            sub_name = "NOT FOUND"
            api_key_preview = "None"
            status = "✗ MISSING"
        
        print(f"Contact {index + 1}:")
        print(f"  Name: {contact_name}")
        print(f"  ID: {contact_id}")
        print(f"  Center: {center}")
        print(f"  Source: {source_id}")
        print(f"  Subaccount: {sub_name}")
        print(f"  API Key: {api_key_preview}")
        print(f"  Status: {status}")
        print()
    
    # Check if all contacts use the same source
    unique_sources = test_df['source'].unique()
    print(f"Unique sources in test data: {unique_sources}")
    
    if len(unique_sources) == 1:
        source = str(unique_sources[0])
        matching_sub = next((sub for sub in subaccounts if str(sub.get("id")) == source), None)
        if matching_sub:
            print(f"\n✓ All test contacts use source {source} ({matching_sub.get('name', 'Unknown')})")
            print("This is good for testing - all contacts will use the same API key.")
        else:
            print(f"\n✗ All test contacts use source {source} but no matching subaccount found!")
    else:
        print(f"\nℹ️  Test contacts use multiple sources: {unique_sources}")
        print("Each contact will use its corresponding subaccount API key.")

if __name__ == "__main__":
    try:
        test_subaccount_mapping()
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nMake sure:")
        print("1. database.csv exists in the current directory")
        print("2. .env file is properly configured")
        print("3. SUBACCOUNTS environment variable is set correctly")
