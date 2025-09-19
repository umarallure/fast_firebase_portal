import requests

# Test the processing progress endpoint
processing_id = "a5aaa170-0dd5-4775-892c-d799b0a51e88"
url = f"http://127.0.0.1:8000/api/master-child-opportunity-update/progress/{processing_id}"

try:
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        progress_data = response.json()
        print("Progress Response:")
        print(f"  Success: {progress_data.get('success', False)}")
        
        if 'progress' in progress_data:
            progress = progress_data['progress']
            print(f"  Status: {progress.get('status', 'unknown')}")
            print(f"  Total Matches: {progress.get('total_matches', 0)}")
            print(f"  Processed: {progress.get('processed', 0)}")
            print(f"  Successful: {progress.get('successful', 0)}")
            print(f"  Failed: {progress.get('failed', 0)}")
            print(f"  Skipped: {progress.get('skipped', 0)}")
            
            # Show details of processing results if available
            if 'updates' in progress:
                updates = progress['updates']
                print(f"\n  Update Details ({len(updates)} total):")
                for i, update in enumerate(updates[:5]):  # Show first 5 updates
                    contact_name = update.get('contact_name', 'Unknown')
                    status = update.get('status', 'unknown')
                    success = update.get('success', False)
                    print(f"    {i+1}. {contact_name} - {status} (success: {success})")
                
                if len(updates) > 5:
                    print(f"    ... and {len(updates) - 5} more updates")
        else:
            print(f"  Message: {progress_data.get('message', '')}")
    else:
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"Error: {e}")