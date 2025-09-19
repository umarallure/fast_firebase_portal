import requests
import json

# First, upload the CSV files to get the parsed data
upload_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/upload"

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
print("Upload successful!")

# Extract the parsed data
master_opportunities = upload_data['data']['master_opportunities']
child_opportunities = upload_data['data']['child_opportunities']

print(f"Master opportunities: {len(master_opportunities)}")
print(f"Child opportunities: {len(child_opportunities)}")

# Now test the matching endpoint
match_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/match"

data = {
    'master_opportunities': json.dumps(master_opportunities),
    'child_opportunities': json.dumps(child_opportunities),
    'match_threshold': '0.7',
    'high_confidence_threshold': '0.9'
}

print("\nTesting matching process...")
try:
    match_response = requests.post(match_url, data=data)
    print(f"Match Status Code: {match_response.status_code}")
    
    if match_response.status_code == 200:
        match_data = match_response.json()
        print("Match Response:")
        print(f"  Success: {match_data.get('success', False)}")
        print(f"  Message: {match_data.get('message', '')}")
        print(f"  Matching ID: {match_data.get('matching_id', '')}")
        
        if 'results' in match_data:
            results = match_data['results']
            print(f"  Total Master: {results.get('total_master', 0)}")
            print(f"  Total Child: {results.get('total_child', 0)}")
            print(f"  Matches Found: {results.get('matches_found', 0)}")
            print(f"  Exact Matches: {results.get('exact_matches', 0)}")
            print(f"  Fuzzy Matches: {results.get('fuzzy_matches', 0)}")
            print(f"  No Matches: {results.get('no_matches', 0)}")
            
            # Show details of matches
            matches = results.get('matches', [])
            print(f"\n  Match Details ({len(matches)} total):")
            for i, match in enumerate(matches[:5]):  # Show first 5 matches
                master_name = match.get('master_opportunity', {}).get('contact_name', 'Unknown')
                match_type = match.get('match_type', 'unknown')
                score = match.get('match_score', 0.0)
                can_update = match.get('can_update', False)
                
                if match_type != 'no_match':
                    child_name = match.get('child_opportunity', {}).get('contact_name', 'Unknown')
                    print(f"    {i+1}. {master_name} -> {child_name} ({match_type}, score: {score:.2f}, can_update: {can_update})")
                else:
                    print(f"    {i+1}. {master_name} -> NO MATCH (score: {score:.2f})")
            
            if len(matches) > 5:
                print(f"    ... and {len(matches) - 5} more matches")
        else:
            print("  No results data in response")
    else:
        print(f"Match Response: {match_response.text}")
        
except Exception as e:
    print(f"Error: {e}")