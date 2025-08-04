"""
Fixed Multi-Subaccount Test for Daily Deal Flow CSV
==================================================
This script tests the exact subaccounts that match your CSV BPOs
and validates stage resolution.
"""

import asyncio
import json
import os
import sys

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
from app.config import Settings

async def test_specific_subaccounts():
    """Test only the subaccounts that match CSV BPOs"""
    
    print("🎯 Testing Specific Subaccounts for Daily Deal Flow CSV")
    print("=" * 65)
    
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
    
    if not matching_subaccounts:
        print(f"❌ No matching subaccounts found for CSV BPOs: {csv_bpos}")
        return
    
    # Test problematic stages
    problematic_stages = [
        'Needs to be Fixed',
        'Needs Carrier Application', 
        'Incomplete transfer',
        'Returned to Center'
    ]
    
    print(f"\n🔍 Testing {len(matching_subaccounts)} matching subaccounts for stages: {problematic_stages}")
    
    results = {}
    
    for subaccount in matching_subaccounts:
        account_name = subaccount.get('name', 'Unknown')
        api_key = subaccount.get('api_key', '')
        
        print(f"\n🏢 Testing: {account_name}")
        print("-" * 50)
        
        if not api_key:
            print("   ❌ No API key found")
            continue
            
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
                
                # Get stages for this pipeline
                stages = await updater.get_pipeline_stages(pipeline_id)
                stage_names = [stage.get('name', '') for stage in stages]
                
                print(f"      Available stages ({len(stage_names)}):")
                for i, stage_name in enumerate(stage_names[:5], 1):  # Show first 5
                    print(f"         {i}. {stage_name}")
                if len(stage_names) > 5:
                    print(f"         ... and {len(stage_names) - 5} more")
                
                # Test each problematic stage
                pipeline_matches = {}
                for stage_name in problematic_stages:
                    stage_id = await updater.get_stage_id_from_name(stage_name, pipeline_id)
                    if stage_id:
                        pipeline_matches[stage_name] = stage_id
                        stage_matches[stage_name] = {
                            'pipeline': pipeline_name,
                            'pipeline_id': pipeline_id,
                            'stage_id': stage_id
                        }
                        print(f"      ✅ FOUND: '{stage_name}' -> {stage_id[:8]}...")
                
                if not pipeline_matches:
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
            continue
    
    # Final summary
    print(f"\n" + "="*65)
    print(f"📊 FINAL RESULTS")
    print(f"="*65)
    
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
            if any(bpo.lower() in account.lower() for account in working_accounts.keys()):
                print(f"   ✅ {bpo}: {record_info}")
            else:
                print(f"   ❌ {bpo}: {record_info} (No matching subaccount found)")
                
    else:
        print(f"❌ No working subaccounts found")
        print(f"   None of the matching subaccounts have the required stages")
        print(f"   You may need to:")
        print(f"   1. Check if the stage names are spelled differently")
        print(f"   2. Verify that the subaccounts have Transfer Portal pipelines")
        print(f"   3. Ensure the API keys have proper permissions")

if __name__ == "__main__":
    asyncio.run(test_specific_subaccounts())
