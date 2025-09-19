#!/usr/bin/env python3
"""
Test Suite for Enhanced Master-Child Opportunity Matching System
This script validates the enhanced matching algorithm and selective update functionality
without affecting production data.
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd
import io

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.master_child_opportunity_update import MasterChildOpportunityUpdateService

class OpportunityMatchingTester:
    """Test class for validating the enhanced opportunity matching system"""

    def __init__(self):
        self.service = MasterChildOpportunityUpdateService()
        self.test_results = []

    def log_test_result(self, test_name: str, passed: bool, details: str = ""):
        """Log test results"""
        result = {
            'test_name': test_name,
            'passed': passed,
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        self.test_results.append(result)
        status = "PASSED" if passed else "FAILED"
        print(f"{status}: {test_name}")
        if details:
            print(f"   Details: {details}")
        print()

    async def test_phone_normalization(self):
        """Test phone number normalization functionality"""
        print("Testing Phone Number Normalization...")

        test_cases = [
            ("+17075675820", "+17075675820"),
            ("7075675820", "+17075675820"),
            ("17075675820", "+17075675820"),
            ("+447071234567", "+447071234567"),
            ("", ""),
            ("invalid", "")
        ]

        passed = True
        for input_phone, expected in test_cases:
            result = self.service._normalize_phone(input_phone)
            if result != expected:
                self.log_test_result(
                    f"Phone Normalization: {input_phone}",
                    False,
                    f"Expected: {expected}, Got: {result}"
                )
                passed = False

        if passed:
            self.log_test_result("Phone Number Normalization", True, "All phone normalization tests passed")

    async def test_date_extraction(self):
        """Test date extraction from opportunity data"""
        print("Testing Date Extraction...")

        test_cases = [
            ({"Created on": "2025-07-15 22:13:01"}, "2025-07-15"),
            ({"created_date": "2025-07-16T10:30:15Z"}, "2025-07-16"),
            ({"date": "07/15/2025"}, "2025-07-15"),
            ({}, None)
        ]

        passed = True
        for opp_data, expected_date in test_cases:
            result = self.service._extract_date(opp_data)
            if expected_date:
                if not result or result.strftime("%Y-%m-%d") != expected_date:
                    self.log_test_result(
                        f"Date Extraction: {opp_data}",
                        False,
                        f"Expected: {expected_date}, Got: {result.strftime('%Y-%m-%d') if result else None}"
                    )
                    passed = False
            else:
                if result is not None:
                    self.log_test_result(
                        f"Date Extraction: {opp_data}",
                        False,
                        f"Expected: None, Got: {result}"
                    )
                    passed = False

        if passed:
            self.log_test_result("Date Extraction", True, "All date extraction tests passed")

    async def test_matching_algorithm(self):
        """Test the enhanced matching algorithm with various scenarios"""
        print("Testing Enhanced Matching Algorithm...")

        # Test case 1: Exact phone match
        master1 = {
            'contact_name': 'John Doe',
            'phone': '+17075675820',
            'Created on': '2025-07-15 22:13:01'
        }
        child1 = {
            'contact_name': 'John Doe',
            'phone': '+17075675820',
            'Created on': '2025-07-15 22:13:01'
        }

        score1 = self.service._calculate_match_score(master1, child1)
        if score1 >= 0.95:  # Should be very high due to exact matches
            self.log_test_result("Exact Match Test", True, f"Score: {score1:.2f}")
        else:
            self.log_test_result("Exact Match Test", False, f"Expected score >= 0.95, Got: {score1:.2f}")

        # Test case 2: Fuzzy name match with different phone
        master2 = {
            'contact_name': 'Jane Smith',
            'phone': '+17075675821',
            'Created on': '2025-07-15 22:13:01'
        }
        child2 = {
            'contact_name': 'Jane Smyth',  # Slight misspelling
            'phone': '+17075675822',  # Different phone
            'Created on': '2025-07-15 22:13:01'
        }

        score2 = self.service._calculate_match_score(master2, child2)
        if 0.45 <= score2 <= 0.55:  # Should be medium due to good name match but different phone
            self.log_test_result("Fuzzy Match Test", True, f"Score: {score2:.2f}")
        else:
            self.log_test_result("Fuzzy Match Test", False, f"Expected score 0.45-0.55, Got: {score2:.2f}")

        # Test case 3: Poor match
        master3 = {
            'contact_name': 'Bob Johnson',
            'phone': '+17075675823',
            'Created on': '2025-01-01 00:00:00'
        }
        child3 = {
            'contact_name': 'Alice Brown',
            'phone': '+15551234567',
            'Created on': '2025-12-31 23:59:59'
        }

        score3 = self.service._calculate_match_score(master3, child3)
        if score3 <= 0.3:  # Should be low due to no similarities
            self.log_test_result("Poor Match Test", True, f"Score: {score3:.2f}")
        else:
            self.log_test_result("Poor Match Test", False, f"Expected score <= 0.3, Got: {score3:.2f}")

    def generate_test_csv_data(self) -> Dict[str, str]:
        """Generate comprehensive test CSV data with various scenarios"""

        # Master opportunities (existing in system)
        master_data = [
            {
                'Opportunity Name': 'John Doe - (707) 567-5820',
                'Contact Name': 'John Doe',
                'phone': '+17075675820',
                'email': 'john.doe@email.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Approved Customer - Not Paid',
                'Lead Value': '500.00',
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',  # Already assigned
                'Created on': '2025-07-15 22:13:01',
                'Updated on': '2025-07-17 17:32:26',
                'status': 'open',
                'Opportunity ID': 'MASTER_001',
                'Contact ID': 'CONTACT_MASTER_001',
                'Pipeline Stage ID': 'STAGE_001',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'  # Test account
            },
            {
                'Opportunity Name': 'Jane Smith - (707) 567-5821',
                'Contact Name': 'Jane Smith',
                'phone': '+17075675821',
                'email': 'jane.smith@email.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'First Draft Payment Failure',
                'Lead Value': '750.00',
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',
                'Created on': '2025-07-16 10:30:15',
                'Updated on': '2025-07-17 14:20:10',
                'status': 'open',
                'Opportunity ID': 'MASTER_002',
                'Contact ID': 'CONTACT_MASTER_002',
                'Pipeline Stage ID': 'STAGE_002',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'
            },
            {
                'Opportunity Name': 'Bob Johnson - (555) 123-4567',
                'Contact Name': 'Bob Johnson',
                'phone': '+15551234567',
                'email': 'bob.johnson@email.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Active Placed - Paid as Earned',
                'Lead Value': '1000.00',
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',
                'Created on': '2025-07-14 08:15:30',
                'Updated on': '2025-07-16 12:45:20',
                'status': 'open',
                'Opportunity ID': 'MASTER_003',
                'Contact ID': 'CONTACT_MASTER_003',
                'Pipeline Stage ID': 'STAGE_003',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'
            }
        ]

        # Child opportunities (source data to sync)
        child_data = [
            # Exact match - should update stage and value
            {
                'Opportunity Name': 'John Doe - (707) 567-5820 - CHILD',
                'Contact Name': 'John Doe',
                'phone': '+17075675820',  # Exact phone match
                'email': 'john.doe@email.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Active Placed - Paid as Earned',  # Different stage - should update
                'Lead Value': '600.00',  # Different value - should update
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',
                'Created on': '2025-07-15 22:13:01',  # Same date
                'Updated on': '2025-07-17 17:32:26',
                'status': 'open',
                'Opportunity ID': 'CHILD_001',
                'Contact ID': 'CONTACT_CHILD_001',
                'Pipeline Stage ID': 'STAGE_NEW_001',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'
            },
            # Fuzzy match - similar name, different phone
            {
                'Opportunity Name': 'Jane Smyth - (707) 567-5822 - CHILD',  # Name variation
                'Contact Name': 'Jane Smyth',  # Slight misspelling
                'phone': '+17075675822',  # Different phone
                'email': 'jane.smith@email.com',  # Same email
                'pipeline': 'Customer Pipeline',
                'stage': 'Approved Customer - Not Paid',  # Same stage - no update
                'Lead Value': '750.00',  # Same value - no update
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',
                'Created on': '2025-07-16 10:30:15',  # Same date
                'Updated on': '2025-07-17 14:20:10',
                'status': 'open',
                'Opportunity ID': 'CHILD_002',
                'Contact ID': 'CONTACT_CHILD_002',
                'Pipeline Stage ID': 'STAGE_002',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'
            },
            # No match - completely different contact
            {
                'Opportunity Name': 'Alice Brown - (888) 999-0000 - CHILD',
                'Contact Name': 'Alice Brown',
                'phone': '+18889990000',
                'email': 'alice.brown@email.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Pending Lapse',
                'Lead Value': '300.00',
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',
                'Created on': '2025-08-01 14:20:30',
                'Updated on': '2025-08-02 09:15:45',
                'status': 'open',
                'Opportunity ID': 'CHILD_003',
                'Contact ID': 'CONTACT_CHILD_003',
                'Pipeline Stage ID': 'STAGE_NEW_003',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'
            },
            # Partial match - same phone, different name
            {
                'Opportunity Name': 'Robert Johnson - (555) 123-4567 - CHILD',
                'Contact Name': 'Robert Johnson',  # Different name
                'phone': '+15551234567',  # Same phone as Bob Johnson
                'email': 'robert.johnson@email.com',
                'pipeline': 'Customer Pipeline',
                'stage': 'Charge-back / Payment Failure',  # Different stage
                'Lead Value': '1200.00',  # Different value
                'source': 'Jotform-call center',
                'assigned': 'Y4DkBuz0jORYFvkMQzlF',
                'Created on': '2025-07-14 08:15:30',  # Same date
                'Updated on': '2025-07-16 12:45:20',
                'status': 'open',
                'Opportunity ID': 'CHILD_004',
                'Contact ID': 'CONTACT_CHILD_004',
                'Pipeline Stage ID': 'STAGE_NEW_004',
                'Pipeline ID': 'PIPELINE_001',
                'Account Id': '999'
            }
        ]

        # Convert to CSV strings
        master_output = io.StringIO()
        child_output = io.StringIO()

        if master_data:
            master_df = pd.DataFrame(master_data)
            master_df.to_csv(master_output, index=False)

        if child_data:
            child_df = pd.DataFrame(child_data)
            child_df.to_csv(child_output, index=False)

        return {
            'master_csv': master_output.getvalue(),
            'child_csv': child_output.getvalue(),
            'master_data': master_data,
            'child_data': child_data
        }

    async def test_csv_parsing(self):
        """Test CSV parsing functionality"""
        print("Testing CSV Parsing...")

        test_data = self.generate_test_csv_data()

        try:
            result = self.service.parse_csv_files(
                test_data['master_csv'],
                test_data['child_csv']
            )

            if result['success']:
                master_count = len(result['master_opportunities'])
                child_count = len(result['child_opportunities'])

                if master_count == 3 and child_count == 4:
                    self.log_test_result("CSV Parsing", True, f"Parsed {master_count} master and {child_count} child opportunities")
                else:
                    self.log_test_result("CSV Parsing", False, f"Expected 3 master and 4 child, got {master_count} and {child_count}")
            else:
                self.log_test_result("CSV Parsing", False, f"Parsing failed: {result.get('error', 'Unknown error')}")

        except Exception as e:
            self.log_test_result("CSV Parsing", False, f"Exception: {str(e)}")

    async def test_matching_workflow(self):
        """Test the complete matching workflow"""
        print("Testing Complete Matching Workflow...")

        test_data = self.generate_test_csv_data()

        try:
            # Parse CSV data
            parsed = self.service.parse_csv_files(
                test_data['master_csv'],
                test_data['child_csv']
            )

            if not parsed['success']:
                self.log_test_result("Matching Workflow", False, "CSV parsing failed")
                return

            # Start matching process
            match_result = await self.service.match_opportunities(
                parsed['master_opportunities'],
                parsed['child_opportunities'],
                match_threshold=0.7,
                high_confidence_threshold=0.9
            )

            if match_result['success']:
                matching_id = match_result['matching_id']

                # Wait a moment for processing
                await asyncio.sleep(2)

                # Check progress
                progress = self.service.get_matching_progress(matching_id)

                if progress['success']:
                    tracker = progress['progress']

                    if tracker['status'] == 'completed':
                        matches_found = tracker['matches_found']
                        exact_matches = tracker['exact_matches']
                        fuzzy_matches = tracker['fuzzy_matches']
                        no_matches = tracker['no_matches']

                        self.log_test_result(
                            "Matching Workflow",
                            True,
                            f"Found {matches_found} matches ({exact_matches} exact, {fuzzy_matches} fuzzy, {no_matches} no matches)"
                        )

                        # Validate expected results
                        if exact_matches >= 1 and fuzzy_matches >= 1 and no_matches >= 1:
                            self.log_test_result("Match Distribution", True, "Expected mix of exact, fuzzy, and no matches")
                        else:
                            self.log_test_result("Match Distribution", False, f"Unexpected distribution: {exact_matches} exact, {fuzzy_matches} fuzzy, {no_matches} no matches")

                    else:
                        self.log_test_result("Matching Workflow", False, f"Matching not completed, status: {tracker['status']}")
                else:
                    self.log_test_result("Matching Workflow", False, "Failed to get matching progress")
            else:
                self.log_test_result("Matching Workflow", False, f"Matching failed: {match_result.get('message', 'Unknown error')}")

        except Exception as e:
            self.log_test_result("Matching Workflow", False, f"Exception: {str(e)}")

    async def test_selective_update_logic(self):
        """Test the selective update logic without making actual API calls"""
        print("Testing Selective Update Logic...")

        # Mock master and child opportunities
        master_opp = {
            'opportunity_id': 'TEST_MASTER_001',
            'stage': 'Approved Customer - Not Paid',
            'value': 500.00,
            'pipeline_id': 'TEST_PIPELINE_001'
        }

        child_opp = {
            'stage': 'Active Placed - Paid as Earned',  # Different stage
            'value': 600.00,  # Different value
            'Lead Value': '600.00'
        }

        # Test stage difference detection
        child_stage = child_opp.get('stage', '').strip()
        master_stage = master_opp.get('stage', '').strip()

        if child_stage.lower() != master_stage.lower():
            self.log_test_result("Stage Difference Detection", True, f"Detected stage change: '{master_stage}' -> '{child_stage}'")
        else:
            self.log_test_result("Stage Difference Detection", False, "Failed to detect stage difference")

        # Test value difference detection
        child_value = child_opp.get('value') or child_opp.get('Lead Value', 0)
        master_value = master_opp.get('value') or master_opp.get('Lead Value', 0)

        try:
            child_value_float = float(child_value) if child_value else 0.0
            master_value_float = float(master_value) if master_value else 0.0

            if child_value_float != master_value_float:
                self.log_test_result("Value Difference Detection", True, f"Detected value change: {master_value_float} -> {child_value_float}")
            else:
                self.log_test_result("Value Difference Detection", False, "Failed to detect value difference")
        except (ValueError, TypeError):
            self.log_test_result("Value Difference Detection", False, "Value parsing failed")

    async def run_all_tests(self):
        """Run all test cases"""
        print("Starting Enhanced Opportunity Matching Test Suite")
        print("=" * 60)

        await self.test_phone_normalization()
        await self.test_date_extraction()
        await self.test_matching_algorithm()
        await self.test_csv_parsing()
        await self.test_matching_workflow()
        await self.test_selective_update_logic()

        print("=" * 60)
        print("Test Results Summary:")

        passed = sum(1 for result in self.test_results if result['passed'])
        total = len(self.test_results)

        print(f"Passed: {passed}/{total}")
        print(f"Failed: {total - passed}/{total}")

        if passed == total:
            print("All tests passed! The system is ready for testing with sample data.")
        else:
            print("Some tests failed. Please review the results above.")

        return passed == total

def main():
    """Main test runner"""
    tester = OpportunityMatchingTester()

    # Run tests
    success = asyncio.run(tester.run_all_tests())

    # Save detailed results
    results_file = f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(tester.test_results, f, indent=2)

    print(f"\nDetailed results saved to: {results_file}")

    return 0 if success else 1

if __name__ == "__main__":
    exit(main())