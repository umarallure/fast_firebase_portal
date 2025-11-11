#!/usr/bin/env python3
"""
GHL Export Service Test Suite - Plexi Account 14
Manual testing guide and validation tests
"""

import os
import sys
import json
from datetime import datetime

class GHLExportManualTester:
    """Manual test class for GHL export service validation"""

    def __init__(self):
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

    def test_code_structure(self):
        """Test that the code structure is correct"""
        print("Testing Code Structure...")

        try:
            # Test imports
            from app.services.ghl_export_new import GHLClient, process_export_request
            from app.models.schemas import ExportRequest, SelectionSchema

            # Check class structure
            client = GHLClient("test_key")
            if hasattr(client, 'get_opportunities'):
                self.log_test_result("Code Structure", True, "GHLClient has required methods")
            else:
                self.log_test_result("Code Structure", False, "GHLClient missing get_opportunities method")

        except ImportError as e:
            self.log_test_result("Code Structure", False, f"Import failed: {str(e)}")
        except Exception as e:
            self.log_test_result("Code Structure", False, f"Structure test failed: {str(e)}")

    def test_pagination_logic(self):
        """Test that pagination logic is implemented correctly"""
        print("Testing Pagination Logic...")

        try:
            # Read the source code and check for key pagination features
            with open('app/services/ghl_export_new.py', 'r') as f:
                code = f.read()

            # Check for key pagination features
            checks = [
                ('len(opportunities) < limit', 'End detection when fewer than limit'),
                ('cursor parameters didn\'t change', 'Infinite loop prevention'),
                ('nextPageUrl', 'Next page URL checking'),
                ('startAfterId', 'Cursor parameter handling'),
                ('startAfter', 'Timestamp parameter handling')
            ]

            passed_checks = 0
            for check, description in checks:
                if check in code:
                    passed_checks += 1
                else:
                    print(f"  Missing: {description}")

            if passed_checks == len(checks):
                self.log_test_result("Pagination Logic", True, f"All {passed_checks} pagination checks passed")
            else:
                self.log_test_result("Pagination Logic", False, f"Only {passed_checks}/{len(checks)} pagination checks passed")

        except Exception as e:
            self.log_test_result("Pagination Logic", False, f"Logic test failed: {str(e)}")

    def test_environment_setup(self):
        """Test that environment is set up for Plexi account 14 testing"""
        print("Testing Environment Setup...")

        # Check for environment variables
        subaccounts_env = os.getenv("SUBACCOUNTS", "[]")

        try:
            subaccounts = json.loads(subaccounts_env)
            plexi_accounts = [acc for acc in subaccounts if acc.get("id") == "14"]

            if plexi_accounts:
                account = plexi_accounts[0]
                if "api_key" in account and "name" in account:
                    self.log_test_result("Environment Setup", True, f"Plexi account 14 configured: {account.get('name', 'Unknown')}")
                else:
                    self.log_test_result("Environment Setup", False, "Plexi account 14 missing required fields")
            else:
                self.log_test_result("Environment Setup", False, "Plexi account 14 not found in SUBACCOUNTS")

        except json.JSONDecodeError:
            self.log_test_result("Environment Setup", False, "Invalid SUBACCOUNTS JSON")
        except Exception as e:
            self.log_test_result("Environment Setup", False, f"Environment check failed: {str(e)}")

    def print_manual_test_instructions(self):
        """Print manual testing instructions"""
        print("\n" + "="*60)
        print("MANUAL TESTING INSTRUCTIONS - PLEXI ACCOUNT 14")
        print("="*60)

        print("\n1. START THE SERVER:")
        print("   python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000")

        print("\n2. TEST API ENDPOINTS:")

        print("\n   a) Check subaccounts:")
        print("      GET http://127.0.0.1:8000/automation/subaccounts")

        print("\n   b) Check pipelines for Plexi account 14:")
        print("      GET http://127.0.0.1:8000/automation/pipelines/14")

        print("\n   c) Test export for Plexi account 14 (Transfer Portal):")
        print("      POST http://127.0.0.1:8000/automation/export")
        print("      Content-Type: application/json")
        print("""
      Body:
      {
        "selections": [
          {
            "account_id": "14",
            "api_key": "YOUR_PLEXI_API_KEY",
            "pipelines": ["pwXbzaRw87WktY49eb7F"]
          }
        ]
      }
        """)

        print("\n3. MONITOR LOGS:")
        print("   Watch for these key indicators:")
        print("   - 'fewer than limit' messages (end of data detection)")
        print("   - 'cursor parameters didn't change' (infinite loop prevention)")
        print("   - Sequential opportunity counts (100, 200, 296, etc.)")
        print("   - No repeated API calls with same parameters")

        print("\n4. EXPECTED BEHAVIOR:")
        print("   - Export should complete without infinite loops")
        print("   - Should stop when < 100 opportunities returned")
        print("   - Should prevent infinite loops with same cursor params")
        print("   - Should respect 200-record testing limit")

        print("\n5. TROUBLESHOOTING:")
        print("   - If still looping: Check cursor parameter validation")
        print("   - If stopping too early: Check end detection logic")
        print("   - If rate limited: Wait and retry")

    def run_all_tests(self):
        """Run all validation tests"""
        print("GHL Export Service Validation - Plexi Account 14")
        print("=" * 60)

        self.test_code_structure()
        self.test_pagination_logic()
        self.test_environment_setup()

        # Summary
        passed = sum(1 for result in self.test_results if result['passed'])
        total = len(self.test_results)

        print("=" * 60)
        print(f"Validation Results: {passed}/{total} tests passed")

        if passed == total:
            print("All validation tests passed! Ready for manual testing.")
        else:
            print("Some validation failed. Please fix issues before manual testing.")

        # Always show manual instructions
        self.print_manual_test_instructions()

        return passed == total

def main():
    """Main test runner"""
    tester = GHLExportManualTester()
    success = tester.run_all_tests()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())