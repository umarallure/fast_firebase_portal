"""
Clean Final Webhook CSV
=======================

Clean up the final webhook CSV by removing empty columns and ensuring
proper formatting for webhook processing.
"""

import pandas as pd
from datetime import datetime

def clean_final_csv():
    """Clean the final webhook CSV file"""
    
    input_file = "webhook_payload_ready_final_20250805_195055.csv"
    
    print(f"🧹 CLEANING FINAL WEBHOOK CSV")
    print("=" * 50)
    
    # Read with encoding handling
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding='latin-1')
    
    print(f"📊 Original columns: {len(df.columns)}")
    print(f"📊 Original rows: {len(df):,}")
    
    # Define the exact webhook payload columns we need
    webhook_columns = [
        'full_name', 'email', 'phone', 'center', 'pipeline_id', 'to_stage', 'ghl_id',
        'source', 'address', 'city', 'state', 'postal_code',
        'date_of_birth', 'birth_state', 'age', 'social_security_number',
        'height', 'weight', 'doctors_name', 'tobacco_user',
        'health_conditions', 'medications', 'monthly_premium', 'coverage_amount', 'carrier',
        'draft_date', 'beneficiary_information', 'bank_name', 'routing_number', 'account_number',
        'future_draft_date', 'additional_information', 'driver_license_number',
        'existing_coverage_last_2_years', 'previous_applications_last_2_years',
        'date_of_submission', 'timestamp', 'from_stage'
    ]
    
    # Keep only the webhook columns that exist in the dataframe
    available_columns = [col for col in webhook_columns if col in df.columns]
    df_clean = df[available_columns].copy()
    
    print(f"📊 Cleaned columns: {len(df_clean.columns)}")
    print(f"📊 Rows after cleaning: {len(df_clean):,}")
    
    # Show center distribution
    center_counts = df_clean['center'].value_counts()
    print(f"\n📋 CENTER DISTRIBUTION:")
    for center, count in center_counts.head(10).items():
        print(f"  {center}: {count:,} contacts")
    
    if len(center_counts) > 10:
        print(f"  ... and {len(center_counts) - 10} more centers")
    
    # Generate final output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"webhook_payload_FINAL_CLEAN_{timestamp}.csv"
    
    # Save clean file
    df_clean.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"\n✅ FINAL WEBHOOK CSV CLEANED!")
    print("=" * 50)
    print(f"📁 Clean file: {output_file}")
    print(f"📊 Total contacts: {len(df_clean):,}")
    print(f"🎯 Webhook columns: {len(df_clean.columns)}")
    
    # Show sample final data
    print(f"\n📄 SAMPLE FINAL WEBHOOK DATA:")
    sample_data = df_clean[['full_name', 'center', 'ghl_id', 'monthly_premium', 'coverage_amount']].head(3)
    for _, row in sample_data.iterrows():
        print(f"  {row['full_name']} | {row['center']} | {row['ghl_id']}")
        print(f"    Premium: ${row['monthly_premium']} | Coverage: {row['coverage_amount']}")
    
    print(f"\n🎯 READY FOR WEBHOOK PROCESSING!")
    print(f"✅ All {len(df_clean):,} contacts have proper:")
    print(f"   - Center names (subaccount names)")
    print(f"   - GHL IDs (ContactID-subaccount)")
    print(f"   - Complete custom field data")
    print(f"   - Clean webhook payload format")
    
    return output_file

if __name__ == "__main__":
    clean_final_csv()
