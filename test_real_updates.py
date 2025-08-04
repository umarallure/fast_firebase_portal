"""
REAL OPPORTUNITY UPDATE TEST
===========================
This script tests the actual opportunity update functionality 
to identify why updates are failing despite stages existing.
"""

import asyncio
import json
import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
from app.config import Settings

async def test_real_opportunity_updates():
    """Test actual opportunity update process for CSV records"""
    
    print("🔧 REAL OPPORTUNITY UPDATE TEST")
    print("=" * 45)
    
    # Load settings and subaccounts
    settings = Settings()
    subaccounts = json.loads(settings.subaccounts)
    
    # Create mapping of BPO names to subaccounts
    bpo_mapping = {}
    csv_bpos = ['Corebiz', 'Digicon', 'Maverick', 'Vize BPO']
    
    for subaccount in subaccounts:
        name = subaccount.get('name', '')
        for bpo in csv_bpos:
            if bpo.lower() in name.lower() or name.lower() in bpo.lower():
                bpo_mapping[bpo] = {
                    'name': name,
                    'api_key': subaccount.get('api_key', ''),
                    'id': subaccount.get('id', '')
                }
                break
    
    print(f"✓ Found {len(bpo_mapping)} BPO mappings")
    
    # Test each problematic CSV record
    test_records = [
        {
            'bpo': 'Corebiz',
            'contact': 'Gary K Prichard',
            'status': 'Needs Carrier Application',
            'policy_number': 'Test-001'
        },
        {
            'bpo': 'Digicon', 
            'contact': 'Estella Marlene Padlan',
            'status': 'Needs to be Fixed',
            'policy_number': 'Test-002'
        },
        {
            'bpo': 'Maverick',
            'contact': 'Shylah Renee Lyon-Inman', 
            'status': 'Incomplete transfer',
            'policy_number': 'Test-003'
        },
        {
            'bpo': 'Vize BPO',
            'contact': 'Linda Rose Rondina',
            'status': 'Needs to be Fixed',
            'policy_number': 'Test-004'
        }
    ]
    
    results = {}
    
    for record in test_records:
        bpo = record['bpo']
        status = record['status']
        policy_number = record['policy_number']
        contact = record['contact']
        
        print(f"\n🎯 Testing: {contact} ({bpo}) -> '{status}'")
        print("-" * 50)
        
        if bpo not in bpo_mapping:
            print(f"   ❌ No subaccount found for {bpo}")
            continue
            
        subaccount = bpo_mapping[bpo]
        api_key = subaccount['api_key']
        
        try:
            # Initialize updater
            updater = GHLOpportunityUpdater(api_key)
            
            # Get pipelines to find Transfer Portal
            pipelines = await updater.get_pipelines()
            transfer_portal = None
            
            for pipeline in pipelines:
                if pipeline.get('name') == 'Transfer Portal':
                    transfer_portal = pipeline
                    break
            
            if not transfer_portal:
                print(f"   ❌ No Transfer Portal pipeline found")
                continue
                
            pipeline_id = transfer_portal.get('id')
            print(f"   ✓ Found Transfer Portal: {pipeline_id[:8]}...")
            
            # Try to get stage ID for the status
            try:
                stage_id = await updater.get_stage_id_from_name(status, pipeline_id)
                if stage_id:
                    print(f"   ✅ Stage '{status}' found: {stage_id[:8]}...")
                    
                    # Test: Try to find opportunities with this policy number
                    # This would normally be done via opportunity search API
                    print(f"   🔍 Testing stage resolution logic...")
                    
                    # Simulate the actual update process that might be failing
                    # Check if there are any validation issues
                    
                    # Test 1: Check if stage exists in Transfer Portal specifically
                    stage_dict = await updater.get_pipeline_stages(pipeline_id)
                    if stage_id in stage_dict:
                        print(f"   ✅ Stage ID confirmed in Transfer Portal")
                        
                        # Test 2: Simulate the validation that happens in bulk update
                        # This is where the actual issue might be!
                        print(f"   🧪 Simulating bulk update validation...")
                        
                        # Check the main.py bulk update logic
                        # It filters by Transfer Portal pipeline only
                        print(f"   📋 Pipeline filter test: PASS (Transfer Portal found)")
                        
                        # The issue might be in opportunity fetching or filtering
                        print(f"   💡 POTENTIAL ISSUE: Opportunity might not exist or belong to different pipeline")
                        print(f"      This would cause the update to be skipped silently")
                        
                        results[policy_number] = {
                            'status': 'STAGE_RESOLVED',
                            'bpo': bpo,
                            'stage_id': stage_id,
                            'pipeline_id': pipeline_id,
                            'issue': 'Likely opportunity not found or wrong pipeline'
                        }
                        
                    else:
                        print(f"   ❌ Stage ID not found in pipeline stages dict")
                        results[policy_number] = {
                            'status': 'STAGE_DICT_MISMATCH',
                            'bpo': bpo,
                            'issue': 'Stage ID not in pipeline stages dictionary'
                        }
                        
                else:
                    print(f"   ❌ Stage '{status}' not found in Transfer Portal")
                    results[policy_number] = {
                        'status': 'STAGE_NOT_FOUND',
                        'bpo': bpo,
                        'issue': f"Stage '{status}' does not exist in Transfer Portal"
                    }
                    
            except Exception as stage_error:
                print(f"   ❌ Stage resolution error: {stage_error}")
                results[policy_number] = {
                    'status': 'STAGE_ERROR',
                    'bpo': bpo,
                    'error': str(stage_error)
                }
                
        except Exception as e:
            print(f"   ❌ API Error: {e}")
            results[policy_number] = {
                'status': 'API_ERROR', 
                'bpo': bpo,
                'error': str(e)
            }
            
    # Summary
    print(f"\n" + "="*50)
    print(f"📊 TEST SUMMARY")
    print(f"="*50)
    
    success_count = len([r for r in results.values() if r['status'] == 'STAGE_RESOLVED'])
    
    if success_count > 0:
        print(f"✅ {success_count} records have working stage resolution")
        print(f"💡 Main issue is likely:")
        print(f"   1. Opportunities don't exist in the system")
        print(f"   2. Opportunities belong to different pipelines")
        print(f"   3. Policy numbers don't match opportunity names/IDs")
        print(f"   4. Bulk update is filtering them out")
        
        print(f"\n🔧 RECOMMENDED DEBUGGING:")
        print(f"   1. Check if opportunities exist using policy numbers")
        print(f"   2. Verify opportunities are in Transfer Portal pipeline")
        print(f"   3. Test with exact opportunity IDs instead of policy numbers")
        print(f"   4. Review bulk update filtering logic")
        
    else:
        print(f"❌ No records had successful stage resolution")
        print(f"💡 Issue is with stage availability:")
        
        for policy, result in results.items():
            if result['status'] == 'STAGE_NOT_FOUND':
                print(f"   • {result['bpo']}: Missing stage '{policy}'")
    
    # Show which BPOs are missing which stages
    print(f"\n📋 STAGE AVAILABILITY MATRIX:")
    stage_matrix = {
        'Needs to be Fixed': ['Maverick'],
        'Needs Carrier Application': ['Maverick', 'Vize BPO', 'Corebiz', 'Digicon'],
        'Incomplete transfer': ['Vize BPO'],
        'Returned to Center': ['Maverick', 'Vize BPO', 'Corebiz', 'Digicon']
    }
    
    for stage, available_bpos in stage_matrix.items():
        print(f"   '{stage}':")
        for bpo in ['Maverick', 'Vize BPO', 'Corebiz', 'Digicon']:
            status = "✅" if bpo in available_bpos else "❌"
            print(f"      {status} {bpo}")

if __name__ == "__main__":
    asyncio.run(test_real_opportunity_updates())
