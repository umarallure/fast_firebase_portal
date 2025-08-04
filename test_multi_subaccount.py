"""
Enhanced Multi-Subaccount Test for Daily Deal Flow CSV
====================================================
This script tests each BPO subaccount from your CSV against your configured API keys
to find the correct matches and validate stage resolution.
"""

import asyncio
import json
import os
import sys
import pandas as pd

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
from app.config import Settings

async def test_multi_subaccount_daily_flow():
    """Test all subaccounts against the problematic stages from CSV"""
    
    print("🚀 Enhanced Multi-Subaccount Test for Daily Deal Flow CSV")
    print("=" * 70)
    
    # Load settings
    settings = Settings()
    
    # Parse subaccounts from environment
    try:
        subaccounts = json.loads(settings.subaccounts)
        print(f"✓ Loaded {len(subaccounts)} subaccounts from configuration")
    except Exception as e:
        print(f"❌ Error loading subaccounts: {e}")
        return
    
    # Read and analyze the CSV
    print(f"\n📊 Analyzing Daily Deal Flow CSV...")
    try:
        csv_file = "Daily Deal Flow Master Sheet - Sheet40 (3).csv"
        df = pd.read_csv(csv_file)
        
        # Find problematic records by BPO
        problematic_by_bpo = {}
        problematic_stages = [
            'Needs to be Fixed',
            'Needs Carrier Application', 
            'Incomplete transfer',
            'Returned to Center'
        ]
        
        for _, row in df.iterrows():
            vendor = str(row.get('Lead Vender', 'Unknown')).strip()
            status = str(row.get('Status', '')).strip()
            
            if status in problematic_stages:
                if vendor not in problematic_by_bpo:
                    problematic_by_bpo[vendor] = []
                problematic_by_bpo[vendor].append({
                    'name': str(row.get('INSURED NAME', '')).strip(),
                    'phone': str(row.get('Client Phone Number', '')).strip(),
                    'status': status
                })
        
        print(f"✓ Found problematic records in {len(problematic_by_bpo)} BPOs:")
        for bpo, records in problematic_by_bpo.items():
            print(f"   - {bpo}: {len(records)} records")
        
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return
    
    # Test each subaccount
    print(f"\n🔧 Testing Each Subaccount...")
    results = {}
    
    for subaccount in subaccounts:
        account_id = subaccount.get('id', 'Unknown')
        account_name = subaccount.get('name', 'Unknown')
        api_key = subaccount.get('api_key', '')
        
        print(f"\n🏢 Testing Subaccount: {account_name} (ID: {account_id})")
        
        if not api_key:
            print("   ❌ No API key found")
            continue
            
        try:
            # Initialize updater for this subaccount
            updater = GHLOpportunityUpdater(api_key)
            
            # Get pipelines
            pipelines = await updater.get_pipelines()
            print(f"   ✓ Found {len(pipelines)} pipelines")
            
            # Store results for this account
            account_results = {
                'pipelines': [],
                'stage_matches': {},
                'total_stages': 0
            }
            
            # Test each pipeline
            for pipeline in pipelines:
                pipeline_id = pipeline.get('id')
                pipeline_name = pipeline.get('name', 'Unknown')
                
                print(f"   📋 Pipeline: {pipeline_name}")
                
                # Get stages
                stages = await updater.get_pipeline_stages(pipeline_id)
                stage_names = [stage.get('name', '') for stage in stages]
                account_results['total_stages'] += len(stage_names)
                
                print(f"      Stages ({len(stage_names)}): {', '.join(stage_names[:3])}{'...' if len(stage_names) > 3 else ''}")
                
                # Test problematic stages
                pipeline_matches = {}
                for stage_name in problematic_stages:
                    stage_id = await updater.get_stage_id_from_name(stage_name, pipeline_id)
                    if stage_id:
                        pipeline_matches[stage_name] = stage_id
                        print(f"      ✅ '{stage_name}' -> {stage_id}")
                        
                        # Add to account matches
                        if stage_name not in account_results['stage_matches']:
                            account_results['stage_matches'][stage_name] = []
                        account_results['stage_matches'][stage_name].append({
                            'pipeline': pipeline_name,
                            'pipeline_id': pipeline_id,
                            'stage_id': stage_id
                        })
                
                account_results['pipelines'].append({
                    'name': pipeline_name,
                    'id': pipeline_id,
                    'stage_count': len(stage_names),
                    'matches': pipeline_matches
                })
            
            results[account_name] = account_results
            
            # Summary for this account
            total_matches = len(account_results['stage_matches'])
            if total_matches > 0:
                print(f"   🎯 FOUND {total_matches} problematic stage types!")
            else:
                print(f"   ⚠️  No problematic stages found")
                
        except Exception as e:
            print(f"   ❌ Error testing {account_name}: {e}")
            continue
    
    # Generate recommendations
    print(f"\n" + "="*70)
    print(f"📈 ANALYSIS RESULTS & RECOMMENDATIONS")
    print(f"="*70)
    
    # Find best matches between CSV BPOs and subaccounts
    print(f"\n🎯 MATCHING CSV BPOs TO SUBACCOUNTS:")
    
    csv_bpos = list(problematic_by_bpo.keys())
    subaccount_names = [sub['name'] for sub in subaccounts]
    
    for bpo in csv_bpos:
        records_count = len(problematic_by_bpo[bpo])
        print(f"\n📋 CSV BPO: '{bpo}' ({records_count} problematic records)")
        
        # Find exact matches
        exact_matches = [name for name in subaccount_names if bpo.lower() in name.lower() or name.lower() in bpo.lower()]
        
        if exact_matches:
            print(f"   ✅ Potential subaccount matches: {exact_matches}")
            
            # Check if these matches have the required stages
            for match in exact_matches:
                if match in results:
                    stage_matches = results[match]['stage_matches']
                    if stage_matches:
                        print(f"      🎯 '{match}' HAS required stages: {list(stage_matches.keys())}")
                    else:
                        print(f"      ⚠️  '{match}' has no required stages")
        else:
            print(f"   ❓ No obvious subaccount match found")
            print(f"      Available subaccounts: {', '.join(subaccount_names[:5])}{'...' if len(subaccount_names) > 5 else ''}")
    
    # Show subaccounts with stage matches
    print(f"\n🏆 SUBACCOUNTS WITH REQUIRED STAGES:")
    for account_name, account_data in results.items():
        if account_data['stage_matches']:
            print(f"\n✅ {account_name}:")
            for stage, matches in account_data['stage_matches'].items():
                print(f"   - '{stage}': Found in {len(matches)} pipeline(s)")
                for match in matches:
                    print(f"     └─ {match['pipeline']} (ID: {match['stage_id'][:8]}...)")
    
    # Specific recommendations for CSV records
    print(f"\n💡 SPECIFIC RECOMMENDATIONS:")
    print(f"Based on your CSV analysis, you should:")
    
    total_problematic = sum(len(records) for records in problematic_by_bpo.values())
    working_subaccounts = len([name for name, data in results.items() if data['stage_matches']])
    
    print(f"1. You have {total_problematic} problematic records across {len(problematic_by_bpo)} BPOs")
    print(f"2. You have {working_subaccounts} subaccounts with the required stages")
    print(f"3. Focus on testing these subaccounts first:")
    
    priority_accounts = []
    for account_name, account_data in results.items():
        stage_count = len(account_data['stage_matches'])
        if stage_count >= 2:  # Has at least 2 of the problematic stages
            priority_accounts.append((account_name, stage_count))
    
    priority_accounts.sort(key=lambda x: x[1], reverse=True)
    
    for account_name, stage_count in priority_accounts[:5]:
        print(f"   ✅ {account_name} ({stage_count}/4 stages available)")
    
    return results

if __name__ == "__main__":
    asyncio.run(test_multi_subaccount_daily_flow())
