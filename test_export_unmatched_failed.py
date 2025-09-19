#!/usr/bin/env python3
"""
Test script to export unmatched and failed updates to CSV
"""

import requests
import json

def test_export_unmatched_failed():
    """Test the export of unmatched and failed updates"""

    # Replace with your actual processing ID
    processing_id = "your_processing_id_here"

    # API endpoint
    url = f"http://127.0.0.1:8000/api/master-child-opportunity-update/export-unmatched-failed/{processing_id}"

    try:
        print(f"Exporting unmatched and failed updates for processing ID: {processing_id}")
        print(f"URL: {url}")

        response = requests.get(url)

        if response.status_code == 200:
            # Save the CSV content
            filename = f"unmatched_and_failed_{processing_id}.csv"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)

            print(f"✅ Successfully exported data to {filename}")
            print(f"Response content preview:")
            print(response.text[:500] + "..." if len(response.text) > 500 else response.text)

        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    print("🔍 Testing Unmatched and Failed Updates CSV Export")
    print("=" * 50)

    # You need to replace this with your actual processing ID
    processing_id = input("Enter processing ID: ").strip()

    if processing_id:
        test_export_unmatched_failed()
    else:
        print("❌ No processing ID provided")