"""
CORRECTED Multi-Subaccount Test for Daily Deal Flow CSV
=====================================================
This script correctly accesses the stage data structure from the GHL API.
"""

import asyncio
import json
import os
import sys

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
from app.config import Settings

async def test_corrected_subaccounts():
    """Test subaccounts with corrected stage data access"""
    
    print("🎯 CORRECTED Test for Daily Deal Flow CSV")
    print("=" * 55)
    
    # Load settings
    settings = Settings()
    
    # Parse subaccounts from environment
    try:
        subaccounts = json.loads(settings.subaccounts)
        print(f"✓ Loaded {len(subaccounts)} subaccounts from configuration")
    except Exception as e:
        print(f"❌ Error loading subaccounts: {e}")
        return
    
    # Filter to only test the BPOs that appear in our CSV
    csv_bpos = ['Corebiz', 'Digicon', 'Maverick', 'Vize BPO']
    
    # Find matching subaccounts
    matching_subaccounts = []
    for subaccount in subaccounts:
        name = subaccount.get('name', '')
        for bpo in csv_bpos:
            if bpo.lower() in name.lower() or name.lower() in bpo.lower():
                matching_subaccounts.append(subaccount)
                print(f"   📌 Found match: '{name}' for CSV BPO '{bpo}'")
                break
    
    # Test problematic stages
    problematic_stages = [
        'Needs to be Fixed',
        'Needs Carrier Application', 
        'Incomplete transfer',
        'Returned to Center'
    ]
    
    print(f"\n🔍 Testing {len(matching_subaccounts)} matching subaccounts...")
    
    results = {}
    
    for subaccount in matching_subaccounts:
        account_name = subaccount.get('name', 'Unknown')
        api_key = subaccount.get('api_key', '')
        
        print(f"\n🏢 Testing: {account_name}")
        print("-" * 50)
        
        try:
            # Initialize updater for this subaccount
            updater = GHLOpportunityUpdater(api_key)
            
            # Get pipelines
            pipelines = await updater.get_pipelines()
            print(f"   ✓ Found {len(pipelines)} pipelines")
            
            stage_matches = {}
            
            # Test each pipeline
            for pipeline in pipelines:
                pipeline_id = pipeline.get('id')
                pipeline_name = pipeline.get('name', 'Unknown')
                
                print(f"   📋 Pipeline: {pipeline_name}")
                
                # Get stages for this pipeline - THIS WAS THE BUG FIX!
                # The method returns a dict of {stage_id: stage_name}
                stage_dict = await updater.get_pipeline_stages(pipeline_id)
                stage_names = list(stage_dict.values())  # Get just the names
                
                print(f"      Available stages ({len(stage_names)}):")
                for i, stage_name in enumerate(stage_names[:5], 1):  # Show first 5
                    print(f"         {i}. {stage_name}")
                if len(stage_names) > 5:
                    print(f"         ... and {len(stage_names) - 5} more")
                
                # Test each problematic stage
                for stage_name in problematic_stages:
                    # Find stage by checking if the name matches any stage in this pipeline
                    for stage_id, stage_name_from_api in stage_dict.items():
                        if stage_name.lower() == stage_name_from_api.lower():
                            stage_matches[stage_name] = {
                                'pipeline': pipeline_name,
                                'pipeline_id': pipeline_id,
                                'stage_id': stage_id
                            }
                            print(f"      ✅ FOUND: '{stage_name}' -> {stage_id[:8]}...")
                            break
                
                if not any(stage_name in stage_dict.values() for stage_name in problematic_stages):
                    print(f"      ⚠️  No problematic stages found in this pipeline")
            
            results[account_name] = stage_matches
            
            # Summary for this account
            if stage_matches:
                print(f"\n   🎯 TOTAL MATCHES: {len(stage_matches)} problematic stages found!")
                for stage, info in stage_matches.items():
                    print(f"      ✓ '{stage}' in {info['pipeline']}")
            else:
                print(f"\n   ❌ No problematic stages found in any pipeline")
                
        except Exception as e:
            print(f"   ❌ Error testing {account_name}: {e}")
            import traceback
            print(f"      Debug: {traceback.format_exc()}")
            continue
    
    # Final summary
    print(f"\n" + "="*55)
    print(f"📊 FINAL RESULTS")
    print(f"="*55)
    
    working_accounts = {name: matches for name, matches in results.items() if matches}
    
    if working_accounts:
        print(f"🎉 SUCCESS! Found {len(working_accounts)} subaccounts with required stages:")
        
        for account_name, stage_matches in working_accounts.items():
            print(f"\n✅ {account_name}:")
            for stage, info in stage_matches.items():
                print(f"   • '{stage}' -> Pipeline: {info['pipeline']}")
        
        print(f"\n💡 NEXT STEPS:")
        print(f"   1. You can now update opportunities using these subaccount API keys")
        print(f"   2. Your CSV has 5 problematic records that can be processed")
        print(f"   3. Each record should be routed to its matching subaccount")
        
        # Show CSV mapping
        csv_mapping = {
            'Corebiz': 'Gary K Prichard - Needs Carrier Application',
            'Digicon': 'Estella Marlene Padlan - Needs to be Fixed', 
            'Maverick': 'Shylah Renee Lyon-Inman - Incomplete transfer',
            'Vize BPO': '2 records - Linda Rose Rondina & Deborah K Delacruz (Needs to be Fixed)'
        }
        
        print(f"\n📋 CSV RECORD MAPPING:")
        for bpo, record_info in csv_mapping.items():
            matching_account = None
            for account_name in working_accounts.keys():
                if bpo.lower() in account_name.lower():
                    matching_account = account_name
                    break
            
            if matching_account:
                available_stages = list(working_accounts[matching_account].keys())
                print(f"   ✅ {bpo} -> {matching_account}: {record_info}")
                print(f"      Available stages: {available_stages}")
            else:
                print(f"   ❌ {bpo}: {record_info} (No matching subaccount found)")
                
    else:
        print(f"❌ No working subaccounts found")
        print(f"   None of the matching subaccounts have the required stages")
        
        # Let's show what stages ARE available in each subaccount
        print(f"\n🔍 DEBUG: Available stages in each subaccount:")
        for account_name in results.keys():
            print(f"\n   {account_name}: No problematic stages found")
            print(f"      (Check logs above for available stage names)")

if __name__ == "__main__":
    asyncio.run(test_corrected_subaccounts())
