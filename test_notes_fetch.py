import asyncio
import httpx
import os
from dotenv import load_dotenv
import json

load_dotenv()

async def test_notes_api():
    """Test the notes API endpoint directly"""
    
    # Load subaccounts
    subaccounts_json = os.getenv('SUBACCOUNTS', '[]')
    subaccounts = json.loads(subaccounts_json)
    
    if not subaccounts:
        print("No subaccounts found in .env")
        return
    
    # Use first subaccount
    account = subaccounts[0]
    api_key = account.get('api_key')
    account_id = account.get('id')
    account_name = account.get('name', 'Unknown')
    
    print(f"Testing with account: {account_name} (ID: {account_id})")
    print(f"API Key: {api_key[:20]}...")
    
    # First, get some opportunities to find contact IDs
    print("\n1. Fetching pipelines...")
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Get pipelines
        pipelines_url = f"https://rest.gohighlevel.com/v1/pipelines"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        resp = await client.get(pipelines_url, headers=headers)
        if resp.status_code != 200:
            print(f"Failed to fetch pipelines: {resp.status_code}")
            return
        
        pipelines = resp.json().get('pipelines', [])
        print(f"Found {len(pipelines)} pipelines")
        
        if not pipelines:
            print("No pipelines found")
            return
        
        # Try all pipelines to find one with opportunities
        contact_id_to_test = None
        opp_info = None
        
        for pipeline in pipelines:
            pipeline_id = pipeline['id']
            pipeline_name = pipeline['name']
            print(f"\n2. Checking pipeline: {pipeline_name}")
            
            # Get opportunities
            opp_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities?limit=10"
            opp_resp = await client.get(opp_url, headers=headers)
            
            if opp_resp.status_code != 200:
                print(f"   Failed to fetch opportunities: {opp_resp.status_code}")
                continue
            
            opportunities = opp_resp.json().get('opportunities', [])
            print(f"   Found {len(opportunities)} opportunities")
            
            if opportunities:
                # Found opportunities, get first contact
                for opp in opportunities:
                    contact_id = opp.get('contact', {}).get('id')
                    if contact_id:
                        contact_id_to_test = contact_id
                        opp_info = {
                            'name': opp.get('name', 'Unknown'),
                            'contact_name': opp.get('contact', {}).get('name', 'Unknown'),
                            'pipeline': pipeline_name
                        }
                        break
                if contact_id_to_test:
                    break
        
        if not contact_id_to_test:
            print("\nNo opportunities with contact IDs found in any pipeline")
            return
        
        print(f"\n3. Testing notes fetch for:")
        print(f"   Pipeline: {opp_info['pipeline']}")
        print(f"   Opportunity: {opp_info['name']}")
        print(f"   Contact: {opp_info['contact_name']} (ID: {contact_id_to_test})")
        
        # Try old API endpoint first
        print("\n   Testing OLD API (v1)...")
        notes_url_old = f"https://rest.gohighlevel.com/v1/contacts/{contact_id_to_test}/notes/"
        notes_resp_old = await client.get(notes_url_old, headers=headers)
        print(f"   Status: {notes_resp_old.status_code}")
        if notes_resp_old.status_code == 200:
            notes_old = notes_resp_old.json().get('notes', [])
            print(f"   Found {len(notes_old)} notes via old API")
            if notes_old:
                print(f"   Sample note: {notes_old[0].get('body', '')[:100]}...")
        else:
            print(f"   Error: {notes_resp_old.text[:200]}")
        
        # Try new API endpoint
        print("\n   Testing NEW API (leadconnectorhq)...")
        notes_url_new = f"https://services.leadconnectorhq.com/contacts/{contact_id_to_test}/notes"
        headers_new = {
            "Authorization": f"Bearer {api_key}",
            "Version": "2021-07-28",
            "Accept": "application/json"
        }
        notes_resp_new = await client.get(notes_url_new, headers=headers_new)
        print(f"   Status: {notes_resp_new.status_code}")
        if notes_resp_new.status_code == 200:
            notes_new = notes_resp_new.json().get('notes', [])
            print(f"   Found {len(notes_new)} notes via new API")
            if notes_new:
                print(f"   Sample note: {notes_new[0].get('body', '')[:100]}...")
                print(f"   Note fields: {list(notes_new[0].keys())}")
        else:
            print(f"   Error: {notes_resp_new.text[:200]}")

if __name__ == "__main__":
    asyncio.run(test_notes_api())
