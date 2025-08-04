"""
Quick Test Runner for Stage Update Issues
========================================

Simple script to run individual test scenarios quickly.
Use this for targeted testing of specific issues.
"""

import asyncio
from test_stage_update_issues import StageUpdateTester
from app.config import settings

async def quick_test_stage_matching():
    """Quick test for stage name matching issues"""
    print("🔍 Testing Stage Name Matching...")
    
    # Test with Ark Tech (known to have issues)
    ark_tech = next(sub for sub in settings.subaccounts_list if sub["name"] == "Ark Tech")
    
    tester = StageUpdateTester()
    await tester.test_scenario_1_stage_name_matching(ark_tech["api_key"], ark_tech["name"])

async def quick_test_pipeline_filtering():
    """Quick test for pipeline filtering issues"""
    print("🔍 Testing Pipeline Filtering...")
    
    # Test with Ark Tech
    ark_tech = next(sub for sub in settings.subaccounts_list if sub["name"] == "Ark Tech")
    
    tester = StageUpdateTester()
    await tester.test_scenario_2_pipeline_filtering(ark_tech["api_key"], ark_tech["id"], ark_tech["name"])

async def quick_test_api_permissions():
    """Quick test for API permission issues"""
    print("🔍 Testing API Permissions...")
    
    # Test with Ark Tech
    ark_tech = next(sub for sub in settings.subaccounts_list if sub["name"] == "Ark Tech")
    
    tester = StageUpdateTester()
    await tester.test_scenario_3_api_permissions(ark_tech["api_key"], ark_tech["name"])

def quick_test_csv_processing():
    """Quick test for CSV data processing issues"""
    print("🔍 Testing CSV Data Processing...")
    
    tester = StageUpdateTester()
    tester.test_scenario_4_csv_data_processing()

async def quick_test_end_to_end():
    """Quick test for end-to-end update flow"""
    print("🔍 Testing End-to-End Update...")
    
    # Test with Ark Tech
    ark_tech = next(sub for sub in settings.subaccounts_list if sub["name"] == "Ark Tech")
    
    tester = StageUpdateTester()
    await tester.test_scenario_5_end_to_end_update(ark_tech["api_key"], ark_tech["id"], ark_tech["name"])

async def test_multiple_subaccounts():
    """Test problematic stages across multiple subaccounts"""
    print("🔍 Testing Multiple Subaccounts...")
    
    # Test with subaccounts known to have issues based on CSV data
    test_accounts = ["Ark Tech", "Libra BPO", "Maverick", "Reliant BPO"]
    
    tester = StageUpdateTester()
    
    for account_name in test_accounts:
        try:
            account = next(sub for sub in settings.subaccounts_list if account_name in sub["name"])
            print(f"\n--- Testing {account['name']} ---")
            await tester.test_scenario_1_stage_name_matching(account["api_key"], account["name"])
        except StopIteration:
            print(f"❌ Account '{account_name}' not found")
        except Exception as e:
            print(f"❌ Error testing {account_name}: {e}")

async def debug_specific_opportunity():
    """Debug a specific opportunity that's failing to update"""
    print("🔍 Debugging Specific Opportunity...")
    
    # You can modify this to test a specific opportunity
    ark_tech = next(sub for sub in settings.subaccounts_list if sub["name"] == "Ark Tech")
    
    from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
    
    updater = GHLOpportunityUpdater(ark_tech["api_key"])
    
    # Get all opportunities
    opportunities = await updater.get_all_opportunities_for_account(ark_tech["id"])
    
    # Find opportunities with problematic stages
    problem_opps = []
    for opp in opportunities:
        stage_name = opp.get("stage_name", "").lower()
        if "fixed" in stage_name or "center" in stage_name:
            problem_opps.append(opp)
    
    print(f"Found {len(problem_opps)} opportunities with problematic stages:")
    
    for opp in problem_opps[:5]:  # Test first 5
        print(f"\n🔧 Testing: {opp['opportunity_name']}")
        print(f"   Stage: '{opp['stage_name']}'")
        print(f"   Pipeline: '{opp['pipeline_name']}'")
        
        # Test stage ID resolution
        stage_id = await updater.get_stage_id_from_name(opp["pipeline_id"], opp["stage_name"])
        print(f"   Stage ID: {stage_id}")
        
        if stage_id:
            # Test update (to same stage - safe)
            try:
                result = await updater.update_opportunity_status(
                    opp["pipeline_id"], 
                    opp["opportunity_id"], 
                    "open", 
                    stage_id
                )
                print(f"   Update result: {'✅ Success' if result else '❌ Failed'}")
            except Exception as e:
                print(f"   Update error: {e}")

# Quick execution functions
def run_stage_matching():
    asyncio.run(quick_test_stage_matching())

def run_pipeline_filtering():
    asyncio.run(quick_test_pipeline_filtering())

def run_api_permissions():
    asyncio.run(quick_test_api_permissions())

def run_csv_processing():
    quick_test_csv_processing()

def run_end_to_end():
    asyncio.run(quick_test_end_to_end())

def run_multiple_accounts():
    asyncio.run(test_multiple_subaccounts())

def run_debug_opportunity():
    asyncio.run(debug_specific_opportunity())

if __name__ == "__main__":
    print("🧪 Quick Test Runner for Stage Update Issues")
    print("=" * 50)
    print("Available tests:")
    print("1. Stage Name Matching")
    print("2. Pipeline Filtering") 
    print("3. API Permissions")
    print("4. CSV Processing")
    print("5. End-to-End Update")
    print("6. Multiple Subaccounts")
    print("7. Debug Specific Opportunity")
    print("8. Run All Tests")
    
    choice = input("\nEnter test number (1-8): ").strip()
    
    if choice == "1":
        run_stage_matching()
    elif choice == "2":
        run_pipeline_filtering()
    elif choice == "3":
        run_api_permissions()
    elif choice == "4":
        run_csv_processing()
    elif choice == "5":
        run_end_to_end()
    elif choice == "6":
        run_multiple_accounts()
    elif choice == "7":
        run_debug_opportunity()
    elif choice == "8":
        from test_stage_update_issues import main
        asyncio.run(main())
    else:
        print("❌ Invalid choice. Running Stage Name Matching test by default...")
        run_stage_matching()
