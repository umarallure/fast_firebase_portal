"""
Test Cases for Investigating Stage Update Issues
==============================================

This file contains comprehensive test scenarios to identify why opportunities 
in "Needs to be Fixed" and "Returned to Center" stages cannot be updated.

Run these tests individually to isolate the specific issue.
"""

import asyncio
import logging
from typing import Dict, List, Any
from app.services.ghl_opportunity_updater import GHLOpportunityUpdater
from app.config import settings

# Configure logging for test output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [TEST] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('stage_update_test.log', mode='w', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class StageUpdateTester:
    def __init__(self):
        self.problematic_stages = [
            "Needs to be Fixed",
            "Returned to Center",
            "Returned To Center", 
            "Returned to center",
            "returned to center",
            "needs to be fixed"
        ]
        
    async def test_scenario_1_stage_name_matching(self, api_key: str, account_name: str = "Test Account"):
        """
        Scenario 1: Test if stage names are being properly resolved
        This tests the core stage matching logic for all problematic variations.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"SCENARIO 1: Stage Name Matching Test for {account_name}")
        logger.info(f"{'='*60}")
        
        updater = GHLOpportunityUpdater(api_key)
        
        try:
            # Fetch all pipelines
            pipelines = await updater.get_pipelines()
            logger.info(f"✓ Successfully fetched {len(pipelines)} pipelines")
            
            # Test stage resolution for each pipeline
            for pipeline in pipelines:
                pipeline_id = pipeline["id"]
                pipeline_name = pipeline.get("name", "Unknown")
                
                logger.info(f"\n--- Pipeline: {pipeline_name} (ID: {pipeline_id}) ---")
                
                # Get all stages for this pipeline
                stages = await updater.get_pipeline_stages(pipeline_id)
                logger.info(f"Available stages ({len(stages)}): {list(stages.values())}")
                
                # Test each problematic stage name
                for stage_name in self.problematic_stages:
                    stage_id = await updater.get_stage_id_from_name(pipeline_id, stage_name)
                    status = "✓ FOUND" if stage_id else "✗ NOT FOUND"
                    logger.info(f"  '{stage_name}' -> {stage_id} [{status}]")
                
                # Look for similar stage names (fuzzy matching)
                logger.info(f"  Fuzzy matching for 'fixed' or 'center':")
                for stage_id, stage_name in stages.items():
                    if any(keyword in stage_name.lower() for keyword in ['fixed', 'center', 'return']):
                        logger.info(f"    Similar: '{stage_name}' (ID: {stage_id})")
        
        except Exception as e:
            logger.error(f"✗ Scenario 1 failed: {e}")
            return False
        
        return True

    async def test_scenario_2_pipeline_filtering(self, api_key: str, account_id: str, account_name: str = "Test Account"):
        """
        Scenario 2: Test if opportunities are being filtered out by pipeline restrictions
        This checks if the "Transfer Portal" filtering is excluding valid opportunities.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"SCENARIO 2: Pipeline Filtering Test for {account_name}")
        logger.info(f"{'='*60}")
        
        updater = GHLOpportunityUpdater(api_key)
        
        try:
            # Get all opportunities for the account
            all_opps = await updater.get_all_opportunities_for_account(account_id)
            logger.info(f"✓ Found {len(all_opps)} total opportunities")
            
            # Analyze pipeline distribution
            pipeline_counts = {}
            stage_counts = {}
            problem_stage_opps = []
            
            for opp in all_opps:
                pipeline_name = opp.get("pipeline_name", "Unknown")
                stage_name = opp.get("stage_name", "Unknown")
                
                pipeline_counts[pipeline_name] = pipeline_counts.get(pipeline_name, 0) + 1
                stage_counts[stage_name] = stage_counts.get(stage_name, 0) + 1
                
                # Check for problematic stages
                if any(prob_stage.lower() in stage_name.lower() for prob_stage in self.problematic_stages):
                    problem_stage_opps.append(opp)
            
            # Report pipeline distribution
            logger.info(f"\nPipeline Distribution:")
            for pipeline, count in sorted(pipeline_counts.items()):
                logger.info(f"  {pipeline}: {count} opportunities")
            
            # Report stage distribution
            logger.info(f"\nStage Distribution:")
            for stage, count in sorted(stage_counts.items()):
                marker = "⚠️" if any(prob in stage.lower() for prob in ['fixed', 'center', 'return']) else ""
                logger.info(f"  {stage}: {count} opportunities {marker}")
            
            # Report problematic stage opportunities
            logger.info(f"\n🔍 Opportunities with problematic stages: {len(problem_stage_opps)}")
            for opp in problem_stage_opps[:10]:  # Show first 10
                logger.info(f"  - {opp.get('opportunity_name')} | Stage: '{opp.get('stage_name')}' | Pipeline: '{opp.get('pipeline_name')}'")
            
            if len(problem_stage_opps) > 10:
                logger.info(f"  ... and {len(problem_stage_opps) - 10} more")
            
            # Check if Transfer Portal filtering is too restrictive
            transfer_portal_opps = [opp for opp in all_opps if opp.get("pipeline_name", "").lower() == "transfer portal"]
            other_pipeline_opps = [opp for opp in all_opps if opp.get("pipeline_name", "").lower() != "transfer portal"]
            
            logger.info(f"\n📊 Transfer Portal Analysis:")
            logger.info(f"  Transfer Portal opportunities: {len(transfer_portal_opps)}")
            logger.info(f"  Other pipeline opportunities: {len(other_pipeline_opps)}")
            
            if other_pipeline_opps:
                logger.info(f"  ⚠️  WARNING: {len(other_pipeline_opps)} opportunities in non-Transfer Portal pipelines")
                logger.info(f"     These will be EXCLUDED by current filtering logic!")
        
        except Exception as e:
            logger.error(f"✗ Scenario 2 failed: {e}")
            return False
        
        return True

    async def test_scenario_3_api_permissions(self, api_key: str, account_name: str = "Test Account"):
        """
        Scenario 3: Test if the API key has proper permissions for stage updates
        This verifies basic API functionality and update permissions.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"SCENARIO 3: API Permissions Test for {account_name}")
        logger.info(f"{'='*60}")
        
        updater = GHLOpportunityUpdater(api_key)
        
        # Test 1: Basic API access
        try:
            pipelines = await updater.get_pipelines()
            logger.info(f"✓ Can fetch pipelines: {len(pipelines)} found")
        except Exception as e:
            logger.error(f"✗ Cannot fetch pipelines: {e}")
            return False
        
        # Test 2: Opportunity fetch
        try:
            # Use first available subaccount ID for this API key
            account_id = None
            for sub in settings.subaccounts_list:
                if sub.get("api_key") == api_key:
                    account_id = sub.get("id")
                    break
            
            if not account_id:
                logger.error(f"✗ No account ID found for API key")
                return False
            
            opportunities = await updater.get_all_opportunities_for_account(account_id)
            logger.info(f"✓ Can fetch opportunities: {len(opportunities)} found")
        except Exception as e:
            logger.error(f"✗ Cannot fetch opportunities: {e}")
            return False
        
        # Test 3: Stage update permissions (dry run with existing stage)
        if opportunities:
            sample_opp = opportunities[0]
            pipeline_id = sample_opp["pipeline_id"]
            opp_id = sample_opp["opportunity_id"]
            current_stage_id = sample_opp["stage_id"]
            
            logger.info(f"Testing with sample opportunity: {sample_opp['opportunity_name']}")
            logger.info(f"  Current stage: {sample_opp['stage_name']} (ID: {current_stage_id})")
            
            # Try to update to the same stage (should be safe)
            try:
                result = await updater.update_opportunity_status(pipeline_id, opp_id, "open", current_stage_id)
                if result:
                    logger.info(f"✓ Can update opportunity status successfully")
                else:
                    logger.error(f"✗ Update returned False - check logs for details")
            except Exception as e:
                logger.error(f"✗ Cannot update opportunity status: {e}")
                return False
        else:
            logger.warning(f"⚠️  No opportunities found to test update permissions")
        
        return True

    def test_scenario_4_csv_data_processing(self):
        """
        Scenario 4: Test how CSV data is being processed for problematic entries
        This simulates the CSV processing logic to identify formatting issues.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"SCENARIO 4: CSV Data Processing Test")
        logger.info(f"{'='*60}")
        
        # Sample problematic CSV rows based on actual data
        test_rows = [
            {"Status": "Needs to be Fixed", "INSURED NAME": "Test User 1"},
            {"Status": "Returned to Center", "INSURED NAME": "Test User 2"},
            {"Status": "Returned To Center", "INSURED NAME": "Test User 3"},
            {"Status": "returned to center ", "INSURED NAME": "Test User 4"},  # trailing space
            {"Status": " Needs to be Fixed ", "INSURED NAME": "Test User 5"},  # leading/trailing spaces
            {"Status": "NEEDS TO BE FIXED", "INSURED NAME": "Test User 6"},  # all caps
            {"Status": "Returned to center ", "INSURED NAME": "Test User 7"},  # different case + space
        ]
        
        logger.info(f"Testing CSV data processing for problematic stage names:")
        
        for i, row in enumerate(test_rows, 1):
            original_status = row.get('Status')
            processed_status = original_status.strip() if original_status else ""
            
            logger.info(f"  Row {i}:")
            logger.info(f"    Original: '{original_status}'")
            logger.info(f"    Processed: '{processed_status}'")
            logger.info(f"    Length: {len(processed_status)}")
            logger.info(f"    Lower: '{processed_status.lower()}'")
            
            # Check if it matches any of our known problematic stages
            matches = []
            for prob_stage in self.problematic_stages:
                if prob_stage.lower() == processed_status.lower():
                    matches.append(prob_stage)
            
            if matches:
                logger.info(f"    ✓ Matches: {matches}")
            else:
                logger.info(f"    ✗ No exact matches found")
        
        return True

    async def test_scenario_5_end_to_end_update(self, api_key: str, account_id: str, account_name: str = "Test Account"):
        """
        Scenario 5: Test the complete update flow for a single opportunity
        This simulates the exact update process for opportunities with problematic stages.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"SCENARIO 5: End-to-End Update Test for {account_name}")
        logger.info(f"{'='*60}")
        
        updater = GHLOpportunityUpdater(api_key)
        
        try:
            # Find opportunities with problematic stages
            all_opps = await updater.get_all_opportunities_for_account(account_id)
            target_stages = ["needs to be fixed", "returned to center"]
            
            target_opps = []
            for opp in all_opps:
                stage_name = opp.get("stage_name", "").lower()
                if any(target_stage in stage_name for target_stage in target_stages):
                    target_opps.append(opp)
            
            logger.info(f"Found {len(target_opps)} opportunities with problematic stages")
            
            if not target_opps:
                logger.warning(f"⚠️  No opportunities found with problematic stages to test")
                return True
            
            # Test with first few opportunities
            test_count = min(3, len(target_opps))
            logger.info(f"Testing with first {test_count} opportunities:")
            
            for i, target_opp in enumerate(target_opps[:test_count], 1):
                logger.info(f"\n--- Test {i}: {target_opp['opportunity_name']} ---")
                logger.info(f"Current stage: '{target_opp['stage_name']}'")
                logger.info(f"Pipeline: '{target_opp['pipeline_name']}'")
                logger.info(f"Stage ID: {target_opp['stage_id']}")
                
                pipeline_id = target_opp["pipeline_id"]
                opp_id = target_opp["opportunity_id"]
                current_stage = target_opp["stage_name"]
                
                # Test 1: Stage ID resolution
                logger.info(f"  Testing stage ID resolution...")
                stage_id = await updater.get_stage_id_from_name(pipeline_id, current_stage)
                if stage_id:
                    logger.info(f"  ✓ Stage ID resolved: '{current_stage}' -> {stage_id}")
                else:
                    logger.error(f"  ✗ Stage ID resolution failed for '{current_stage}'")
                    continue
                
                # Test 2: Verify stage ID matches current
                if stage_id == target_opp["stage_id"]:
                    logger.info(f"  ✓ Resolved stage ID matches current stage ID")
                else:
                    logger.warning(f"  ⚠️  Resolved stage ID ({stage_id}) differs from current ({target_opp['stage_id']})")
                
                # Test 3: Attempt update to same stage (safe test)
                logger.info(f"  Testing stage update...")
                try:
                    result = await updater.update_opportunity_status(pipeline_id, opp_id, "open", stage_id)
                    if result:
                        logger.info(f"  ✓ Update successful")
                    else:
                        logger.error(f"  ✗ Update failed - check detailed logs")
                except Exception as e:
                    logger.error(f"  ✗ Update exception: {e}")
        
        except Exception as e:
            logger.error(f"✗ Scenario 5 failed: {e}")
            return False
        
        return True

    async def test_scenario_6_stage_case_sensitivity(self, api_key: str, account_name: str = "Test Account"):
        """
        Scenario 6: Test stage name case sensitivity and exact matching
        This identifies if there are subtle differences in stage names.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"SCENARIO 6: Stage Case Sensitivity Test for {account_name}")
        logger.info(f"{'='*60}")
        
        updater = GHLOpportunityUpdater(api_key)
        
        try:
            pipelines = await updater.get_pipelines()
            
            # For each pipeline, examine stage names in detail
            for pipeline in pipelines:
                pipeline_id = pipeline["id"]
                pipeline_name = pipeline.get("name", "Unknown")
                stages = await updater.get_pipeline_stages(pipeline_id)
                
                logger.info(f"\n--- Pipeline: {pipeline_name} ---")
                
                # Look for stages containing "fix" or "center" or "return"
                relevant_stages = {}
                for stage_id, stage_name in stages.items():
                    stage_lower = stage_name.lower()
                    if any(keyword in stage_lower for keyword in ['fix', 'center', 'return']):
                        relevant_stages[stage_id] = stage_name
                
                if relevant_stages:
                    logger.info(f"  Relevant stages found:")
                    for stage_id, stage_name in relevant_stages.items():
                        logger.info(f"    '{stage_name}' (ID: {stage_id})")
                        
                        # Test exact character analysis
                        logger.info(f"      Length: {len(stage_name)}")
                        logger.info(f"      Bytes: {stage_name.encode('utf-8')}")
                        logger.info(f"      Repr: {repr(stage_name)}")
                        
                        # Test matching against our problematic stages
                        for prob_stage in self.problematic_stages:
                            if stage_name.lower() == prob_stage.lower():
                                logger.info(f"      ✓ EXACT MATCH with '{prob_stage}'")
                            elif prob_stage.lower() in stage_name.lower():
                                logger.info(f"      ~ PARTIAL MATCH with '{prob_stage}'")
                else:
                    logger.info(f"  No relevant stages found")
        
        except Exception as e:
            logger.error(f"✗ Scenario 6 failed: {e}")
            return False
        
        return True

    async def run_all_tests(self, api_key: str, account_id: str, account_name: str = "Test Account"):
        """
        Run all test scenarios in sequence
        """
        logger.info(f"\n🧪 STARTING COMPREHENSIVE STAGE UPDATE TESTING")
        logger.info(f"Account: {account_name}")
        logger.info(f"API Key: {api_key[:12]}...")
        logger.info(f"Account ID: {account_id}")
        logger.info(f"Timestamp: {asyncio.get_event_loop().time()}")
        
        test_results = {}
        
        # Run each scenario
        scenarios = [
            ("Stage Name Matching", self.test_scenario_1_stage_name_matching(api_key, account_name)),
            ("Pipeline Filtering", self.test_scenario_2_pipeline_filtering(api_key, account_id, account_name)),
            ("API Permissions", self.test_scenario_3_api_permissions(api_key, account_name)),
            ("CSV Data Processing", self.test_scenario_4_csv_data_processing()),
            ("End-to-End Update", self.test_scenario_5_end_to_end_update(api_key, account_id, account_name)),
            ("Case Sensitivity", self.test_scenario_6_stage_case_sensitivity(api_key, account_name))
        ]
        
        for test_name, test_coro in scenarios:
            try:
                if asyncio.iscoroutine(test_coro):
                    result = await test_coro
                else:
                    result = test_coro
                test_results[test_name] = "✓ PASSED" if result else "✗ FAILED"
            except Exception as e:
                test_results[test_name] = f"✗ ERROR: {e}"
        
        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"TEST SUMMARY")
        logger.info(f"{'='*60}")
        
        for test_name, result in test_results.items():
            logger.info(f"{test_name}: {result}")
        
        passed = sum(1 for result in test_results.values() if "PASSED" in result)
        total = len(test_results)
        logger.info(f"\nOverall: {passed}/{total} tests passed")
        
        return test_results


# Example usage and main execution
async def main():
    """
    Main function to run the tests
    Modify the API key and account details below for your testing
    """
    
    # 🔧 CONFIGURATION - UPDATE THESE VALUES FOR YOUR TESTING
    # =======================================================
    
    # Example: Test with first subaccount (Ark Tech)
    test_subaccount = settings.subaccounts_list[1]  # Index 1 = "Ark Tech"
    api_key = test_subaccount["api_key"]
    account_id = test_subaccount["id"] 
    account_name = test_subaccount["name"]
    
    print(f"Testing with: {account_name} (ID: {account_id})")
    print(f"API Key: {api_key[:12]}...")
    
    # Initialize tester
    tester = StageUpdateTester()
    
    # Choose what to run:
    
    # Option 1: Run individual scenarios
    # await tester.test_scenario_1_stage_name_matching(api_key, account_name)
    # await tester.test_scenario_2_pipeline_filtering(api_key, account_id, account_name)
    
    # Option 2: Run all tests
    await tester.run_all_tests(api_key, account_id, account_name)
    
    print("\n✅ Testing complete! Check 'stage_update_test.log' for detailed results.")


def test_specific_subaccount(subaccount_name: str):
    """
    Helper function to test a specific subaccount by name
    Usage: test_specific_subaccount("Ark Tech")
    """
    target_subaccount = None
    for sub in settings.subaccounts_list:
        if sub.get("name") == subaccount_name:
            target_subaccount = sub
            break
    
    if not target_subaccount:
        print(f"❌ Subaccount '{subaccount_name}' not found")
        print("Available subaccounts:")
        for sub in settings.subaccounts_list:
            print(f"  - {sub.get('name')}")
        return
    
    async def run_test():
        tester = StageUpdateTester()
        await tester.run_all_tests(
            target_subaccount["api_key"],
            target_subaccount["id"], 
            target_subaccount["name"]
        )
    
    asyncio.run(run_test())


if __name__ == "__main__":
    # Run the main test suite
    asyncio.run(main())
    
    # Or test specific subaccounts:
    # test_specific_subaccount("Ark Tech")
    # test_specific_subaccount("Libra BPO")
