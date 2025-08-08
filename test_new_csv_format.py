"""
Quick Test for New CSV Format
============================

This script tests the new DATABASEGHL.csv format and shows:
1. CSV structure analysis
2. Field mapping validation  
3. Sample processing with first 3 contacts
"""

import pandas as pd
import asyncio
import os
from datetime import datetime
from webhook_payload_enhancer import process_new_csv_format, get_api_key_for_source
from app.config import settings

def analyze_csv_structure():
    """Analyze the new CSV file structure"""
    
    csv_file = "DATABASEGHL.csv"
    if not os.path.exists(csv_file):
        print(f"❌ CSV file {csv_file} not found!")
        return False
    
    df = pd.read_csv(csv_file)
    
    print("🔍 CSV STRUCTURE ANALYSIS")
    print("=" * 50)
    print(f"📊 Total rows: {len(df)}")
    print(f"📋 Total columns: {len(df.columns)}")
    
    print(f"\n📝 COLUMNS FOUND:")
    for i, col in enumerate(df.columns, 1):
        sample_value = df[col].iloc[0] if len(df) > 0 else "N/A"
        print(f"  {i}. '{col}' - Sample: {sample_value}")
    
    # Check for required mappings
    print(f"\n🔧 FIELD MAPPING ANALYSIS:")
    
    mappings = {
        'Contact ID': 'contact_id (GHL Contact ID)',
        'Account Id': 'source (Subaccount/Source ID)', 
        'pipeline': 'pipeline_id (Pipeline identifier)',
        'current_stage': 'to_stage (Current stage for webhook)',
        'full_name': 'full_name (Contact name)',
        'phone': 'phone (Contact phone)',
        'email': 'email (Contact email)'
    }
    
    for csv_col, description in mappings.items():
        if csv_col in df.columns:
            print(f"  ✅ {csv_col} → {description}")
        else:
            print(f"  ❌ {csv_col} → {description} (MISSING)")
    
    # Show sample data
    print(f"\n📄 SAMPLE DATA (First 3 rows):")
    print(df.head(3).to_string())
    
    # Check unique sources
    if 'Account Id' in df.columns:
        unique_sources = df['Account Id'].unique()
        print(f"\n🎯 UNIQUE SOURCES FOUND: {len(unique_sources)}")
        for source in sorted(unique_sources):
            count = (df['Account Id'] == source).sum()
            print(f"  Source {source}: {count} contacts")
    
    return True

def test_subaccount_mapping():
    """Test subaccount mapping for the new format"""
    
    print(f"\n🔑 SUBACCOUNT MAPPING TEST")
    print("=" * 50)
    
    df = pd.read_csv("DATABASEGHL.csv")
    
    # Get unique sources
    unique_sources = df['Account Id'].unique()
    
    print(f"📋 Testing API key mapping for {len(unique_sources)} sources:")
    
    for source in sorted(unique_sources):
        api_key = get_api_key_for_source(str(source))
        contact_count = (df['Account Id'] == source).sum()
        
        if api_key:
            api_preview = api_key[:20] + "..." if len(api_key) > 20 else api_key
            status = "✅ FOUND"
        else:
            api_preview = "None"
            status = "❌ MISSING"
        
        print(f"  Source {source}: {contact_count} contacts | API Key: {api_preview} | {status}")
    
    # Find a source with API key for testing
    test_source = None
    for source in unique_sources:
        if get_api_key_for_source(str(source)):
            test_source = source
            break
    
    if test_source:
        print(f"\n✅ Ready for testing with Source {test_source}")
        return True
    else:
        print(f"\n❌ No sources have valid API keys configured")
        return False

async def test_single_contact():
    """Test processing a single contact from the new CSV"""
    
    print(f"\n🧪 SINGLE CONTACT TEST")
    print("=" * 50)
    
    df = pd.read_csv("DATABASEGHL.csv")
    
    # Get first contact with valid source
    test_contact = None
    test_df = df.head(5)
    for index, row in test_df.iterrows():  # Check first 5
        source_id = str(row['Account Id'])
        api_key = get_api_key_for_source(source_id)
        if api_key:
            test_contact = row
            break
    
    if test_contact is None:
        print("❌ No test contact found with valid API key")
        return
    
    contact_id = test_contact['Contact ID']
    source_id = str(test_contact['Account Id'])
    name = test_contact['full_name']
    
    print(f"🎯 Testing contact:")
    print(f"  Name: {name}")
    print(f"  Contact ID: {contact_id}")
    print(f"  Source: {source_id}")
    print(f"  Pipeline: {test_contact['pipeline']}")
    print(f"  Stage: {test_contact['current_stage']}")
    
    # Test API call
    from webhook_payload_enhancer import WebhookPayloadEnhancer
    
    api_key = get_api_key_for_source(source_id)
    enhancer = WebhookPayloadEnhancer(api_key)
    
    try:
        print(f"\n🔄 Fetching contact data from GoHighLevel...")
        webhook_data = await enhancer.get_contact_details(contact_id)
        
        if webhook_data:
            print(f"✅ Successfully fetched contact data!")
            
            # Show key webhook fields
            key_fields = ['full_name', 'email', 'phone', 'ghl_id', 'address', 'city', 'state']
            print(f"\n📋 KEY WEBHOOK FIELDS:")
            for field in key_fields:
                value = webhook_data.get(field, '')
                print(f"  {field}: {value}")
            
            # Count populated fields
            populated = sum(1 for v in webhook_data.values() if v and str(v).strip())
            total = len(webhook_data)
            print(f"\n📊 Field population: {populated}/{total} ({populated/total*100:.1f}%)")
            
        else:
            print(f"❌ Failed to fetch contact data")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    finally:
        await enhancer.close()

def main():
    """Run all tests for the new CSV format"""
    
    print("🚀 NEW CSV FORMAT VALIDATION")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Analyze CSV structure
    if not analyze_csv_structure():
        return
    
    # Step 2: Test subaccount mapping
    if not test_subaccount_mapping():
        print(f"\n⚠️  Warning: API key issues detected")
        print("Please check your .env configuration and SUBACCOUNTS setup")
    
    # Step 3: Test single contact processing
    print(f"\n" + "=" * 60)
    try:
        asyncio.run(test_single_contact())
    except Exception as e:
        print(f"❌ Single contact test failed: {str(e)}")
    
    print(f"\n" + "=" * 60)
    print(f"✅ CSV format validation complete!")
    print(f"\nNext steps:")
    print(f"1. If tests passed, run: python webhook_payload_enhancer.py")
    print(f"2. This will process ALL contacts and create webhook-ready CSV")
    print(f"3. Output will contain ONLY the webhook payload columns")

if __name__ == "__main__":
    main()
