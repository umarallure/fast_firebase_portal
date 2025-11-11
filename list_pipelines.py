import asyncio
import httpx
import os
from dotenv import load_dotenv
import json

load_dotenv()

async def list_all_pipelines_and_opportunities():
    """List all pipelines and their opportunities for the test account"""
    
    # Load subaccounts
    subaccounts_json = os.getenv('SUBACCOUNTS', '[]')
    subaccounts = json.loads(subaccounts_json)
    
    if not subaccounts:
        print("No subaccounts found")
        return
    
    # Use first subaccount
    account = subaccounts[0]
    api_key = account.get('api_key')
    account_name = account.get('name', 'Unknown')
    
    print(f"Account: {account_name}")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Get pipelines
        pipelines_url = "https://rest.gohighlevel.com/v1/pipelines"
        headers = {"Authorization": f"Bearer {api_key}"}
        
        resp = await client.get(pipelines_url, headers=headers)
        if resp.status_code != 200:
            print(f"Failed to fetch pipelines: {resp.status_code}")
            return
        
        pipelines = resp.json().get('pipelines', [])
        print(f"\nFound {len(pipelines)} pipelines:\n")
        
        for i, pipeline in enumerate(pipelines, 1):
            pipeline_id = pipeline['id']
            pipeline_name = pipeline['name']
            
            print(f"{i}. {pipeline_name}")
            print(f"   ID: {pipeline_id}")
            
            # Get opportunities for this pipeline
            opp_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities?limit=5"
            opp_resp = await client.get(opp_url, headers=headers)
            
            if opp_resp.status_code == 200:
                opportunities = opp_resp.json().get('opportunities', [])
                print(f"   Opportunities: {len(opportunities)}")
                
                if opportunities:
                    for j, opp in enumerate(opportunities, 1):
                        contact_name = opp.get('contact', {}).get('name', 'N/A')
                        contact_id = opp.get('contact', {}).get('id', 'N/A')
                        opp_name = opp.get('name', 'N/A')
                        print(f"      {j}. {opp_name} - Contact: {contact_name} ({contact_id})")
            else:
                print(f"   Error fetching opportunities: {opp_resp.status_code}")
            
            print()

if __name__ == "__main__":
    asyncio.run(list_all_pipelines_and_opportunities())
