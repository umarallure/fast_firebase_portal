#!/usr/bin/env python3
"""
Test script for the new opportunities-only export functionality
"""

import requests
import json

def test_frontend():
    """Test the frontend page"""
    print("Testing frontend page...")
    try:
        response = requests.get('http://127.0.0.1:8000/ghl-opportunities-only-export', timeout=10)
        if response.status_code == 200:
            print("✓ Frontend page accessible")
            if 'GHL Opportunities Only Export' in response.text:
                print("✓ Page contains correct title")
            else:
                print("✗ Page title not found")
            if 'Fast Export' in response.text:
                print("✓ Page contains fast export description")
            else:
                print("✗ Fast export description not found")
        else:
            print(f"✗ Frontend returned status {response.status_code}")
    except Exception as e:
        print(f"✗ Frontend test failed: {e}")

def test_api_endpoint():
    """Test the API endpoint"""
    print("\nTesting API endpoint...")
    try:
        # Test with empty selections (should fail validation)
        response = requests.post(
            'http://127.0.0.1:8000/api/v1/automation/export-opportunities-only',
            json={'selections': []},
            timeout=10
        )
        print(f"API endpoint status: {response.status_code}")
        if response.status_code == 422:
            print("✓ API endpoint exists and validates input correctly")
        elif response.status_code != 404:
            print(f"✓ API endpoint exists (status {response.status_code})")
        else:
            print("✗ API endpoint not found")
    except Exception as e:
        print(f"✗ API test failed: {e}")

def test_subaccounts_api():
    """Test the subaccounts API"""
    print("\nTesting subaccounts API...")
    try:
        response = requests.get('http://127.0.0.1:8000/api/v1/subaccounts', timeout=10)
        if response.status_code == 200:
            print("✓ Subaccounts API accessible")
            try:
                data = response.json()
                print(f"✓ Subaccounts API returned {len(data)} subaccounts")
            except:
                print("✓ Subaccounts API returned data")
        else:
            print(f"✗ Subaccounts API returned status {response.status_code}")
    except Exception as e:
        print(f"✗ Subaccounts API test failed: {e}")

if __name__ == "__main__":
    print("Testing new GHL Opportunities Only Export functionality")
    print("=" * 60)

    test_frontend()
    test_api_endpoint()
    test_subaccounts_api()

    print("\n" + "=" * 60)
    print("Testing completed!")