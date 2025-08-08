"""
Fix Center Names with Proper Subaccount Names
=============================================

This script fixes the center names to use actual subaccount names
instead of generic "Source X.0" format.
"""

import pandas as pd
import os
from datetime import datetime
from app.config import settings

def show_subaccount_mapping():
    """Show the actual subaccount configuration"""
    subaccounts = settings.subaccounts_list
    
    print(f"🔍 SUBACCOUNT CONFIGURATION:")
    print("-" * 50)
    for sub in subaccounts:
        print(f"  ID: {sub.get('id')} -> Name: {sub.get('name', 'Unknown')}")
    print()

def get_proper_subaccount_name(source_id) -> str:
    """Get proper subaccount name handling float values"""
    subaccounts = settings.subaccounts_list
    
    # Convert source_id to string and handle NaN
    if pd.isna(source_id):
        return "Unknown Source"
    
    source_str = str(int(float(source_id))) if str(source_id) != 'nan' else 'unknown'
    
    # Find subaccount by source ID
    for sub in subaccounts:
        if str(sub.get("id")) == source_str:
            return sub.get("name", f"Source {source_str}")
    
    # Return a descriptive name if not found
    return f"Unknown Source {source_str}"

def fix_center_names(input_file: str):
    """Fix center names with proper subaccount names"""
    
    if not os.path.exists(input_file):
        print(f"❌ Input file {input_file} not found!")
        return
    
    print(f"🔧 FIXING CENTER NAMES WITH PROPER SUBACCOUNT NAMES")
    print("=" * 70)
    print(f"📁 Input file: {input_file}")
    
    # Show subaccount configuration first
    show_subaccount_mapping()
    
    # Read the CSV file
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding='latin-1')
        print("📝 Note: Using latin-1 encoding")
    
    print(f"📊 Total rows: {len(df):,}")
    
    # Show current center values
    current_centers = df['center'].value_counts()
    print(f"\n📋 CURRENT CENTER VALUES:")
    for center, count in current_centers.head(10).items():
        print(f"  {center}: {count:,} contacts")
    
    # Update center names with proper subaccount names
    print(f"\n🔄 MAPPING SOURCES TO PROPER SUBACCOUNT NAMES...")
    
    # Create mapping
    unique_sources = df['source'].unique()
    source_mapping = {}
    
    for source in unique_sources:
        proper_name = get_proper_subaccount_name(source)
        source_mapping[source] = proper_name
        contact_count = (df['source'] == source).sum()
        
        if not pd.isna(source):
            source_display = str(int(float(source)))
        else:
            source_display = "NaN"
            
        print(f"  Source {source_display}: {proper_name} ({contact_count:,} contacts)")
    
    # Apply the mapping
    df['center'] = df['source'].map(source_mapping)
    
    # Show updated center values
    updated_centers = df['center'].value_counts()
    print(f"\n📋 UPDATED CENTER VALUES:")
    for center, count in updated_centers.head(10).items():
        print(f"  {center}: {count:,} contacts")
    
    if len(updated_centers) > 10:
        print(f"  ... and {len(updated_centers) - 10} more centers")
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"webhook_payload_ready_final_{timestamp}.csv"
    
    # Save updated file
    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
    except UnicodeEncodeError:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ CENTER NAMES FIXED SUCCESSFULLY!")
    print("=" * 70)
    print(f"📁 Output file: {output_file}")
    print(f"📊 Total contacts: {len(df):,}")
    print(f"🎯 Unique centers: {len(updated_centers)}")
    
    # Show sample of updated data
    print(f"\n📄 SAMPLE FINAL DATA:")
    sample_data = df[['full_name', 'center', 'source', 'ghl_id']].head(5)
    for _, row in sample_data.iterrows():
        source_display = str(int(float(row['source']))) if not pd.isna(row['source']) else "NaN"
        print(f"  {row['full_name']} -> {row['center']} (Source: {source_display})")
    
    return output_file

def main():
    """Main function"""
    
    # Find the most recent webhook output file with centers
    import glob
    center_files = glob.glob("webhook_payload_ready_with_centers_*.csv")
    
    if not center_files:
        # Fallback to regular webhook files
        webhook_files = glob.glob("webhook_payload_ready_*.csv")
        if not webhook_files:
            print("❌ No webhook payload files found!")
            return
        latest_file = max(webhook_files, key=os.path.getctime)
    else:
        latest_file = max(center_files, key=os.path.getctime)
    
    print(f"🚀 CENTER NAME FIXING PROCESS")
    print("=" * 70)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Processing: {latest_file}")
    
    try:
        output_file = fix_center_names(latest_file)
        
        if output_file:
            print(f"\n🎉 SUCCESS!")
            print(f"✅ Center names now use proper subaccount names")
            print(f"📁 Final file: {output_file}")
            print(f"🎯 Ready for webhook processing!")
        
    except Exception as e:
        print(f"\n❌ Error fixing center names: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
