"""
Quick Test Runner for Contact Enhancement
========================================

This script provides a simple way to test the contact enhancement functionality.
"""

import asyncio
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from webhook_contact_enhancer import create_webhook_ready_csv

def check_environment():
    """Check if required environment variables are set"""
    print("=== ENVIRONMENT CHECK ===")
    
    # Check for .env file
    env_file = ".env"
    if os.path.exists(env_file):
        print(f"✓ Found {env_file}")
    else:
        print(f"✗ No {env_file} file found")
        print(f"  Create one based on contact_enhancement_env_example.txt")
    
    # Check for CSV file
    csv_file = "database.csv"
    if os.path.exists(csv_file):
        print(f"✓ Found {csv_file}")
        # Count rows
        with open(csv_file, 'r') as f:
            line_count = sum(1 for line in f) - 1  # Subtract header
        print(f"  Contains {line_count} contact records")
    else:
        print(f"✗ No {csv_file} file found")
        return False
    
    # Check for SUBACCOUNTS configuration
    subaccounts_env = os.getenv("SUBACCOUNTS")
    if subaccounts_env:
        try:
            import json
            subaccounts = json.loads(subaccounts_env)
            if subaccounts and len(subaccounts) > 0:
                print(f"✓ Found SUBACCOUNTS configuration with {len(subaccounts)} subaccounts")
                
                # Check if we have API keys
                has_api_keys = any(sub.get("api_key") for sub in subaccounts)
                if has_api_keys:
                    print(f"✓ Subaccounts have API keys configured")
                    return True
                else:
                    print(f"✗ No API keys found in subaccounts")
                    return False
            else:
                print(f"✗ SUBACCOUNTS is empty")
                return False
        except Exception as e:
            print(f"✗ Invalid SUBACCOUNTS format: {str(e)}")
            return False
    
    # Fallback: Check individual API key environment variables
    api_keys = [
        "GHL_CHILD_LOCATION_API_KEY",
        "GHL_MASTER_LOCATION_API_KEY"
    ]
    
    found_key = False
    for key in api_keys:
        if os.getenv(key):
            print(f"✓ Found {key}")
            found_key = True
        else:
            print(f"✗ Missing {key}")
    
    if not found_key:
        print("\n⚠️  No API configuration found")
        print("   Please configure either:")
        print("   1. SUBACCOUNTS environment variable (recommended)")
        print("   2. Individual API keys:")
        for key in api_keys:
            print(f"      - {key}")
        return False
    
    print("\n✓ Environment check passed!")
    return True

def main():
    """Main test runner"""
    print("Contact Enhancement Test Runner")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check environment
    if not check_environment():
        print("\n❌ Environment check failed. Please fix the issues above.")
        return
    
    print("\n=== STARTING CONTACT ENHANCEMENT ===")
    print("Processing first 5 contacts from database.csv...")
    print("This will:")
    print("1. Read contact IDs from the CSV")
    print("2. Fetch detailed contact data from GoHighLevel API")
    print("3. Map data to webhook format")
    print("4. Create enhanced CSV file")
    print()
    
    # Run the enhancement
    try:
        asyncio.run(create_webhook_ready_csv())
        print("\n✅ Enhancement completed successfully!")
    except Exception as e:
        print(f"\n❌ Enhancement failed: {str(e)}")
        print("\nTroubleshooting tips:")
        print("- Check your API keys are valid")
        print("- Verify contact IDs exist in GoHighLevel")
        print("- Check your internet connection")
        print("- Review the logs above for specific errors")

if __name__ == "__main__":
    main()
