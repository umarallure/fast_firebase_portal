#!/usr/bin/env python3
"""
Test script for Supabase import - validates payload mapping and tests with sample records
"""

import pandas as pd
import json
from import_to_supabase import SupabaseImporter

def test_payload_mapping():
    """Test the payload mapping with sample data"""
    print("Testing CSV to payload mapping...")
    print("=" * 50)
    
    # Load CSV
    try:
        df = pd.read_csv("finaltransfercheckerimport.csv", encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv("finaltransfercheckerimport.csv", encoding='latin-1')
    
    # Filter out rows with empty names
    df = df.dropna(subset=['full_name'])
    df = df[df['full_name'].str.strip() != '']
    
    print(f"Loaded {len(df)} valid rows from CSV")
    print(f"Columns in CSV: {list(df.columns)}")
    print()
    
    # Create importer instance
    importer = SupabaseImporter()
    
    # Test with first 3 rows
    for i in range(min(3, len(df))):
        row = df.iloc[i]
        print(f"Testing Row {i+1}: {row.get('full_name', 'N/A')}")
        print("-" * 30)
        
        # Map to payload
        payload = importer.map_csv_to_payload(row)
        
        # Validate required fields
        is_valid, validation_error = importer.validate_required_fields(payload)
        
        print(f"Valid payload: {is_valid}")
        if not is_valid:
            print(f"Validation error: {validation_error}")
        
        print("Payload structure:")
        for key, value in payload.items():
            if value is not None:
                print(f"  {key}: {value}")
        
        print("\nJSON payload:")
        print(json.dumps(payload, indent=2))
        print("\n" + "="*80 + "\n")

def test_webhook_connection():
    """Test webhook connection with a sample payload"""
    print("Testing webhook connection...")
    print("=" * 50)
    
    # Load first valid row
    try:
        df = pd.read_csv("finaltransfercheckerimport.csv", encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv("finaltransfercheckerimport.csv", encoding='latin-1')
    
    df = df.dropna(subset=['full_name'])
    df = df[df['full_name'].str.strip() != '']
    
    if len(df) == 0:
        print("No valid rows found for testing")
        return
    
    # Create test payload from first row
    importer = SupabaseImporter()
    test_row = df.iloc[0]
    payload = importer.map_csv_to_payload(test_row)
    
    print(f"Testing with: {payload.get('full_name', 'N/A')}")
    print(f"Email: {payload.get('email', 'N/A')}")
    print(f"Phone: {payload.get('phone', 'N/A')}")
    
    # Ask for confirmation
    response = input("\nDo you want to send this test record to the webhook? (y/N): ").strip().lower()
    if response != 'y':
        print("Test cancelled.")
        return
    
    # Send test request
    success = importer.send_to_webhook(payload, "TEST")
    
    if success:
        print("✅ Test successful! Webhook is working correctly.")
    else:
        print("❌ Test failed. Check the error messages above.")
        if importer.errors:
            print("Error details:")
            for error in importer.errors:
                print(f"  {error}")

def analyze_csv_data():
    """Analyze the CSV data to understand its structure"""
    print("Analyzing CSV data structure...")
    print("=" * 50)
    
    try:
        df = pd.read_csv("finaltransfercheckerimport.csv", encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv("finaltransfercheckerimport.csv", encoding='latin-1')
    
    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print()
    
    # Check for empty names
    valid_names = df.dropna(subset=['full_name'])
    valid_names = valid_names[valid_names['full_name'].str.strip() != '']
    print(f"Rows with valid names: {len(valid_names)}")
    
    # Analyze key fields
    key_fields = ['full_name', 'email', 'phone', 'center', 'pipeline_id', 'to_stage', 'ghl_id']
    
    print("\nKey field analysis:")
    for field in key_fields:
        if field in df.columns:
            non_empty = df[field].notna() & (df[field].astype(str).str.strip() != '') & (df[field].astype(str).str.lower() != 'nan')
            print(f"  {field}: {non_empty.sum()}/{len(df)} non-empty ({non_empty.sum()/len(df)*100:.1f}%)")
        else:
            print(f"  {field}: Column not found")
    
    # Check unique centers
    if 'center' in df.columns:
        unique_centers = df['center'].dropna().unique()
        print(f"\nUnique centers ({len(unique_centers)}):")
        for center in sorted(unique_centers):
            count = df[df['center'] == center].shape[0]
            print(f"  {center}: {count} records")
    
    # Check unique pipeline_ids
    if 'pipeline_id' in df.columns:
        unique_pipelines = df['pipeline_id'].dropna().unique()
        print(f"\nUnique pipeline_ids ({len(unique_pipelines)}):")
        for pipeline in sorted(unique_pipelines):
            count = df[df['pipeline_id'] == pipeline].shape[0]
            print(f"  {pipeline}: {count} records")
    
    # Check unique stages
    if 'to_stage' in df.columns:
        unique_stages = df['to_stage'].dropna().unique()
        print(f"\nUnique to_stage values ({len(unique_stages)}):")
        for stage in sorted(unique_stages):
            count = df[df['to_stage'] == stage].shape[0]
            print(f"  {stage}: {count} records")

def main():
    """Main function for testing"""
    print("Supabase Import Test Suite")
    print("=" * 50)
    
    while True:
        print("\nSelect an option:")
        print("1. Analyze CSV data structure")
        print("2. Test payload mapping (first 3 rows)")
        print("3. Test webhook connection (send 1 test record)")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            analyze_csv_data()
        elif choice == '2':
            test_payload_mapping()
        elif choice == '3':
            test_webhook_connection()
        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
