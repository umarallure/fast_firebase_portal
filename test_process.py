import requests
import json

# First, upload and match the CSV files
upload_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/upload"
match_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/match"

files = {
    'master_file': ('test_master_opportunities.csv', open('test_master_opportunities.csv', 'rb'), 'text/csv'),
    'child_file': ('test_child_clean.csv', open('test_child_clean.csv', 'rb'), 'text/csv')
}

print("Uploading CSV files...")
upload_response = requests.post(upload_url, files=files)

if upload_response.status_code != 200:
    print(f"Upload failed: {upload_response.status_code}")
    print(upload_response.text)
    exit(1)

upload_data = upload_response.json()
master_opportunities = upload_data['data']['master_opportunities']
child_opportunities = upload_data['data']['child_opportunities']

# Match the opportunities
data = {
    'master_opportunities': json.dumps(master_opportunities),
    'child_opportunities': json.dumps(child_opportunities),
    'match_threshold': '0.7',
    'high_confidence_threshold': '0.9'
}

print("Matching opportunities...")
match_response = requests.post(match_url, data=data)

if match_response.status_code != 200:
    print(f"Match failed: {match_response.status_code}")
    print(match_response.text)
    exit(1)

match_data = match_response.json()
matches = match_data['results']['matches']

print(f"Found {len(matches)} matches")

# Filter to only exact matches for simpler testing
exact_matches = [m for m in matches if m['match_type'] == 'exact']
print(f"Testing with {len(exact_matches)} exact matches only")

if not exact_matches:
    print("No exact matches found, using first fuzzy match for testing")
    exact_matches = matches[:1]

# Now test the processing endpoint with dry-run
process_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/process"

process_data = {
    'matches_data': json.dumps(exact_matches),
    'dry_run': 'true',  # Enable dry-run mode
    'batch_size': '1',  # Process one at a time
    'process_exact_only': 'false'
}

print("\nTesting processing with dry-run...")
try:
    process_response = requests.post(process_url, data=process_data)
    print(f"Process Status Code: {process_response.status_code}")
    
    if process_response.status_code == 200:
        process_result = process_response.json()
        print("Process Response:")
        print(f"  Success: {process_result.get('success', False)}")
        print(f"  Message: {process_result.get('message', '')}")
        print(f"  Processing ID: {process_result.get('processing_id', '')}")
        
        if 'results' in process_result:
            results = process_result['results']
            print(f"  Total Matches: {results.get('total_matches', 0)}")
            print(f"  Processed: {results.get('processed', 0)}")
            print(f"  Successful: {results.get('successful', 0)}")
            print(f"  Failed: {results.get('failed', 0)}")
            print(f"  Skipped: {results.get('skipped', 0)}")
            
            # Show details of processing results
            updates = results.get('updates', [])
            print(f"\n  Update Details ({len(updates)} total):")
            for i, update in enumerate(updates[:5]):  # Show first 5 updates
                contact_name = update.get('contact_name', 'Unknown')
                status = update.get('status', 'unknown')
                success = update.get('success', False)
                print(f"    {i+1}. {contact_name} - {status} (success: {success})")
            
            if len(updates) > 5:
                print(f"    ... and {len(updates) - 5} more updates")
        else:
            print("  No results data in response")
    else:
        print(f"Process Response: {process_response.text}")
        
except Exception as e:
    print(f"Error: {e}")