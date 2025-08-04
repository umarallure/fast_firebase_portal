"""
Test Script for Daily Deal Flow Master Sheet CSV
================================================
This script tests the new CSV format with different column structure
and identifies which BPO subaccounts have problematic stages.
"""

import pandas as pd
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
from app.config import Settings

async def test_daily_deal_flow_csv():
    """Test the Daily Deal Flow Master Sheet CSV format"""
    
    print("🧪 Testing Daily Deal Flow Master Sheet CSV")
    print("=" * 60)
    
    try:
        # Read the new CSV file
        csv_file = "Daily Deal Flow Master Sheet - Sheet40 (3).csv"
        df = pd.read_csv(csv_file)
        
        print(f"📊 CSV Structure Analysis:")
        print(f"   Total rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        # Identify the relevant columns
        phone_col = "Client Phone Number"
        vendor_col = "Lead Vender" 
        status_col = "Status"
        name_col = "INSURED NAME"
        
        # Define problematic stages to look for
        problematic_stages = [
            'Needs to be Fixed',
            'Returned to Center', 
            'Returned To Center',
            'Incomplete transfer',
            'Needs Carrier Application'
        ]
        
        print(f"\n🔍 Analyzing problematic stages...")
        
        # Group by Lead Vendor (BPO)
        bpo_analysis = {}
        total_problematic = 0
        
        for _, row in df.iterrows():
            vendor = str(row.get(vendor_col, 'Unknown')).strip()
            status = str(row.get(status_col, '')).strip()
            phone = str(row.get(phone_col, '')).strip()
            name = str(row.get(name_col, '')).strip()
            
            if vendor not in bpo_analysis:
                bpo_analysis[vendor] = {
                    'total_records': 0,
                    'problematic_records': [],
                    'stage_counts': {}
                }
            
            bpo_analysis[vendor]['total_records'] += 1
            
            # Check if this status is problematic
            if status in problematic_stages:
                total_problematic += 1
                bpo_analysis[vendor]['problematic_records'].append({
                    'name': name,
                    'phone': phone,
                    'status': status
                })
                
                if status not in bpo_analysis[vendor]['stage_counts']:
                    bpo_analysis[vendor]['stage_counts'][status] = 0
                bpo_analysis[vendor]['stage_counts'][status] += 1
        
        # Display results
        print(f"\n📈 RESULTS BY BPO VENDOR:")
        print("=" * 60)
        
        for vendor, data in bpo_analysis.items():
            if data['problematic_records']:
                print(f"\n🏢 BPO: {vendor}")
                print(f"   Total Records: {data['total_records']}")
                print(f"   Problematic Records: {len(data['problematic_records'])}")
                
                # Show stage breakdown
                for stage, count in data['stage_counts'].items():
                    print(f"   '{stage}': {count} records")
                
                # Show sample records
                print(f"   Sample Records:")
                for i, record in enumerate(data['problematic_records'][:3], 1):
                    print(f"     {i}. {record['name']} ({record['phone']}) -> '{record['status']}'")
        
        print(f"\n📊 SUMMARY:")
        print(f"   Total problematic records: {total_problematic}")
        print(f"   BPOs with issues: {len([v for v in bpo_analysis.values() if v['problematic_records']])}")
        
        # Now test with GHL API if we have configurations
        print(f"\n🔧 Testing GHL API Stage Resolution...")
        
        settings = Settings()
        
        # Try to get API key from environment or config
        api_key = getattr(settings, 'ghl_child_location_api_key', '') or getattr(settings, 'ghl_master_location_api_key', '')
        
        if not api_key:
            print("⚠️ No API key found in configuration. Skipping API tests.")
            print("   Please ensure your .env file has the correct API keys configured.")
            return bpo_analysis
        
        updater = GHLOpportunityUpdater(api_key)
        
        # Test stage resolution for each problematic stage
        unique_stages = set()
        for vendor_data in bpo_analysis.values():
            for record in vendor_data['problematic_records']:
                unique_stages.add(record['status'])
        
        print(f"\n🎯 Testing stage resolution for: {list(unique_stages)}")
        
        try:
            # Get pipelines first
            pipelines = await updater.get_pipelines()
            print(f"✓ Found {len(pipelines)} pipelines")
            
            for pipeline in pipelines:
                pipeline_id = pipeline.get('id')
                pipeline_name = pipeline.get('name', 'Unknown')
                print(f"\n📋 Pipeline: {pipeline_name} (ID: {pipeline_id})")
                
                # Get stages for this pipeline
                stages = await updater.get_pipeline_stages(pipeline_id)
                stage_names = [stage.get('name', '') for stage in stages]
                print(f"   Available stages: {stage_names}")
                
                # Test each problematic stage
                for stage_name in unique_stages:
                    stage_id = await updater.get_stage_id_from_name(stage_name, pipeline_id)
                    if stage_id:
                        print(f"   ✓ '{stage_name}' -> {stage_id}")
                    else:
                        print(f"   ✗ '{stage_name}' -> NOT FOUND")
        
        except Exception as e:
            print(f"⚠️ API testing failed: {e}")
            print("   This might be due to API key configuration")
        
        return bpo_analysis
        
    except Exception as e:
        print(f"❌ Error analyzing CSV: {e}")
        return None

if __name__ == "__main__":
    asyncio.run(test_daily_deal_flow_csv())
