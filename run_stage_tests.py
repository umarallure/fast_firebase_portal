"""
Run Stage Update Tests
====================

Execute this script to investigate stage update issues for 
"Needs to be Fixed" and "Returned to Center" stages.

Usage:
    python run_stage_tests.py
"""

import sys
import os
import asyncio

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_stage_update_issues import StageUpdateTester
from app.config import settings

async def main():
    print("🔧 Stage Update Issue Investigation")
    print("=" * 50)
    
    # Get the first few subaccounts to test with
    test_accounts = [
        ("Ark Tech", "ArkTech BPO"),  # Known to have issues from CSV
        ("Test", "Test"),              # First account
        ("GrowthOnics BPO", "GrowthOnics BPO")  # Third account
    ]
    
    tester = StageUpdateTester()
    
    for account_search, display_name in test_accounts:
        # Find matching subaccount
        matching_account = None
        for sub in settings.subaccounts_list:
            if account_search.lower() in sub.get("name", "").lower():
                matching_account = sub
                break
        
        if not matching_account:
            print(f"❌ Account '{account_search}' not found")
            continue
        
        print(f"\n🏢 Testing Account: {matching_account['name']}")
        print(f"   ID: {matching_account['id']}")
        print(f"   API Key: {matching_account['api_key'][:12]}...")
        
        try:
            # Run the most important tests
            print(f"\n   ➤ Testing Stage Name Matching...")
            await tester.test_scenario_1_stage_name_matching(
                matching_account["api_key"], 
                matching_account["name"]
            )
            
            print(f"\n   ➤ Testing Pipeline Filtering...")
            await tester.test_scenario_2_pipeline_filtering(
                matching_account["api_key"], 
                matching_account["id"], 
                matching_account["name"]
            )
            
        except Exception as e:
            print(f"❌ Error testing {matching_account['name']}: {e}")
    
    print(f"\n✅ Investigation complete!")
    print(f"📋 Check 'stage_update_test.log' for detailed results")

if __name__ == "__main__":
    # Ensure we're in the right directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Run the investigation
    asyncio.run(main())
