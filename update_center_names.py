"""
Update Center Names in Webhook Output
====================================

This script updates the 'center' column in the webhook output CSV file 
to use the actual subaccount names instead of "Default Center".
"""

import pandas as pd
import os
from datetime import datetime
from app.config import settings

def get_subaccount_name_for_source(source_id: str) -> str:
    """Get subaccount name for a specific source/subaccount ID from settings"""
    subaccounts = settings.subaccounts_list
    
    # Find subaccount by source ID
    for sub in subaccounts:
        if str(sub.get("id")) == str(source_id):
            return sub.get("name", f"Source {source_id}")
    
    # Return a default name if not found
    return f"Source {source_id}"

def update_center_names(input_file: str):
    """Update center names in the webhook CSV file"""
    
    if not os.path.exists(input_file):
        print(f"❌ Input file {input_file} not found!")
        return
    
    print(f"🔄 UPDATING CENTER NAMES")
    print("=" * 50)
    print(f"📁 Input file: {input_file}")
    
    # Read the CSV file with proper encoding handling
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(input_file, encoding='latin-1')
            print("📝 Note: Using latin-1 encoding due to special characters")
        except UnicodeDecodeError:
            df = pd.read_csv(input_file, encoding='cp1252')
            print("📝 Note: Using cp1252 encoding due to special characters")
    
    print(f"📊 Total rows: {len(df):,}")
    
    # Show current center values
    current_centers = df['center'].value_counts()
    print(f"\n📋 CURRENT CENTER VALUES:")
    for center, count in current_centers.items():
        print(f"  {center}: {count:,} contacts")
    
    # Get unique sources and their mappings
    unique_sources = df['source'].unique()
    print(f"\n🎯 SOURCE TO SUBACCOUNT MAPPING:")
    
    source_to_name = {}
    for source in unique_sources:
        subaccount_name = get_subaccount_name_for_source(str(source))
        source_to_name[source] = subaccount_name
        contact_count = (df['source'] == source).sum()
        print(f"  Source {source}: {subaccount_name} ({contact_count:,} contacts)")
    
    # Update center names based on source
    print(f"\n🔄 UPDATING CENTER NAMES...")
    df['center'] = df['source'].map(source_to_name)
    
    # Show updated center values
    updated_centers = df['center'].value_counts()
    print(f"\n📋 UPDATED CENTER VALUES:")
    for center, count in updated_centers.items():
        print(f"  {center}: {count:,} contacts")
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"webhook_payload_ready_with_centers_{timestamp}.csv"
    
    # Save updated file with proper encoding
    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
    except UnicodeEncodeError:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("📝 Note: Saved with UTF-8 BOM for better compatibility")
    
    print(f"\n✅ CENTER NAMES UPDATED SUCCESSFULLY!")
    print("=" * 50)
    print(f"📁 Output file: {output_file}")
    print(f"📊 Total contacts: {len(df):,}")
    print(f"🎯 Centers updated: {len(updated_centers)}")
    
    # Show sample of updated data
    print(f"\n📄 SAMPLE UPDATED DATA:")
    sample_data = df[['full_name', 'center', 'source', 'ghl_id']].head(3)
    for _, row in sample_data.iterrows():
        print(f"  {row['full_name']} -> {row['center']} (Source: {row['source']}, GHL ID: {row['ghl_id']})")
    
    return output_file

def main():
    """Main function to update center names"""
    
    # Find the most recent webhook output file
    import glob
    webhook_files = glob.glob("webhook_payload_ready_*.csv")
    
    if not webhook_files:
        print("❌ No webhook payload files found!")
        print("Please run the webhook_payload_enhancer.py first.")
        return
    
    # Get the most recent file
    latest_file = max(webhook_files, key=os.path.getctime)
    
    print(f"🚀 CENTER NAME UPDATE PROCESS")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Processing: {latest_file}")
    
    try:
        output_file = update_center_names(latest_file)
        
        if output_file:
            print(f"\n🎉 SUCCESS!")
            print(f"✅ Center names have been updated based on subaccount names")
            print(f"📁 New file created: {output_file}")
            print(f"🎯 Ready for webhook processing with proper center names!")
        
    except Exception as e:
        print(f"\n❌ Error updating center names: {str(e)}")
        print("Please check your configuration and try again.")

if __name__ == "__main__":
    main()
