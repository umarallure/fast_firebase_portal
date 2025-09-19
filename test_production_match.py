import sys
sys.path.append('.')

from app.services.master_child_opportunity_update import master_child_opportunity_service
import requests
import json

# Read the production CSV files
print("Reading production files...")

with open('child_production.csv', 'r', encoding='utf-8') as f:
    child_content = f.read()

with open('master_production.csv', 'r', encoding='utf-8') as f:
    master_content = f.read()

print("Testing matching with production data...")

# First, upload the CSV files to get the parsed data
upload_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/upload"

files = {
    'master_file': ('master_production.csv', open('master_production.csv', 'rb'), 'text/csv'),
    'child_file': ('child_production.csv', open('child_production.csv', 'rb'), 'text/csv')
}

print("Uploading CSV files...")
upload_response = requests.post(upload_url, files=files)

if upload_response.status_code != 200:
    print(f"Upload failed: {upload_response.status_code}")
    print(upload_response.text)
    exit(1)

upload_data = upload_response.json()
print("Upload successful!")

if not upload_data.get('success'):
    print(f"Upload parsing failed: {upload_data.get('message', 'Unknown error')}")
    exit(1)

# Extract parsed data
parsed_data = upload_data.get('data', {})
master_opportunities = parsed_data.get('master_opportunities', [])
child_opportunities = parsed_data.get('child_opportunities', [])

print(f"Parsed {len(master_opportunities)} master opportunities")
print(f"Parsed {len(child_opportunities)} child opportunities")

# Now test the matching with the parsed data
match_url = "http://127.0.0.1:8000/api/master-child-opportunity-update/match"

match_data = {
    'master_opportunities': json.dumps(master_opportunities),
    'child_opportunities': json.dumps(child_opportunities),
    'match_threshold': 0.7,
    'high_confidence_threshold': 0.9
}

print("Starting matching process...")
match_response = requests.post(match_url, data=match_data)

if match_response.status_code != 200:
    print(f"Match failed: {match_response.status_code}")
    print(match_response.text)
    exit(1)

match_data = match_response.json()
print(f"\nMatch Status Code: {match_response.status_code}")
print(f"Match Response: {json.dumps(match_data, indent=2)}")

# If matching was successful, proceed with processing
if match_data.get('success'):
    matching_id = match_data.get('matching_id')
    print(f"\n✅ Matching completed successfully!")
    print(f"Matching ID: {matching_id}")
    results = match_data.get('results', {})
    print(f"Total Master: {results.get('total_master', 0)}")
    print(f"Total Child: {results.get('total_child', 0)}")
    print(f"Matches Found: {results.get('matches_found', 0)}")
    print(f"Exact Matches: {results.get('exact_matches', 0)}")
    print(f"Fuzzy Matches: {results.get('fuzzy_matches', 0)}")
    print(f"No Matches: {results.get('no_matches', 0)}")

    # Show some match details
    match_details = results.get('matches', [])
    if match_details:
        print(f"\n📋 First 10 Match Results:")
        for i, match in enumerate(match_details[:10], 1):
            status = "✅ EXACT" if match.get('match_type') == 'exact' else "🔍 FUZZY" if match.get('match_type') == 'fuzzy' else "❌ NO MATCH"
            child_name = match.get('child_opportunity', {}).get('contact_name', 'Unknown') if match.get('child_opportunity') else 'No Match'
            master_opp = match.get('master_opportunity')
            master_name = master_opp.get('contact_name', 'No Match') if master_opp else 'No Match'
            score = match.get('match_score', 0)
            can_update = match.get('can_update', False)
            print(f"{i}. {status} | Child: {child_name} | Master: {master_name} | Score: {score:.2f} | Can Update: {can_update}")

    # Ask user if they want to proceed with processing
    if match_data.get('matches_found', 0) > 0:
        print(f"\n🚀 Ready to process {match_data.get('matches_found', 0)} matches!")
        print("Do you want to proceed with updating the master opportunities?")

        # For now, let's show what would happen
        process_url = f"http://127.0.0.1:8000/api/master-child-opportunity-update/process/{matching_id}"
        print(f"Process URL: {process_url}")
        print("You can call this endpoint to perform the actual updates.")

else:
    print(f"❌ Matching failed: {match_data.get('message', 'Unknown error')}")