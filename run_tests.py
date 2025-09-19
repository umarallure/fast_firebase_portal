#!/usr/bin/env python3
"""
Simple Test Runner for Enhanced Opportunity Matching System
Run this script to validate the system before production use.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_tests():
    """Run the comprehensive test suite"""

    print("Enhanced Opportunity Matching System - Test Runner")
    print("=" * 60)

    # Check if we're in the right directory
    if not Path("test_enhanced_matching.py").exists():
        print("ERROR: test_enhanced_matching.py not found in current directory")
        print("Please run this script from the fast_firebase_portal directory")
        return False

    # Run the test suite
    print("Running comprehensive test suite...")
    print()

    try:
        result = subprocess.run([
            sys.executable,
            "test_enhanced_matching.py"
        ], capture_output=True, text=True, cwd=os.getcwd())

        # Print output
        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("Warnings/Errors:")
            print(result.stderr)

        # Check result
        if result.returncode == 0:
            print("\nAll tests passed! System is ready for testing.")
            return True
        else:
            print(f"\nTests failed with exit code: {result.returncode}")
            return False

    except Exception as e:
        print(f"Error running tests: {str(e)}")
        return False

def show_test_files():
    """Show information about test files"""

    print("\nTest Files Created:")
    print("-" * 30)

    test_files = [
        "test_enhanced_matching.py",
        "test_master_opportunities.csv",
        "test_child_opportunities.csv"
    ]

    for file in test_files:
        if Path(file).exists():
            size = Path(file).stat().st_size
            print(f"OK {file} ({size} bytes)")
        else:
            print(f"ERROR {file} - MISSING")

def show_next_steps():
    """Show next steps for testing"""

    print("\nNext Steps for Testing:")
    print("-" * 30)
    print("1. Run the test suite (just completed)")
    print("2. Review test results in the generated JSON file")
    print("3. Start the FastAPI server:")
    print("   python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000")
    print("4. Test CSV upload via API:")
    print("   POST http://127.0.0.1:8000/bulk-update-opportunity/upload")
    print("   Use test_master_opportunities.csv and test_child_opportunities.csv")
    print("5. Test matching process:")
    print("   POST http://127.0.0.1:8000/bulk-update-opportunity/match")
    print("6. Test processing with dry-run:")
    print("   POST http://127.0.0.1:8000/bulk-update-opportunity/process")
    print("   Set dry_run=true to avoid actual API calls")
    print("7. Test unmatched CSV download:")
    print("   GET http://127.0.0.1:8000/bulk-update-opportunity/unmatched-csv/{matching_id}")

def main():
    """Main function"""

    # Show test files
    show_test_files()

    # Run tests
    success = run_tests()

    # Show next steps
    show_next_steps()

    print("\n" + "=" * 60)
    if success:
        print("Test environment is ready! Proceed with API testing.")
    else:
        print("Please fix test failures before proceeding to API testing.")

    return 0 if success else 1

if __name__ == "__main__":
    exit(main())