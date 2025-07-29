import httpx
import json
import asyncio
from app.config import settings

async def test_pipeline_api():
    """Test the pipeline API to see the actual structure"""
    
    # Get the first subaccount for testing
    subaccounts = settings.subaccounts_list
    if not subaccounts:
        print("No subaccounts configured")
        return
    
    account = subaccounts[0]
    api_key = account.get('api_key')
    account_id = str(account['id'])
    
    if not api_key:
        print(f"No API key for account {account_id}")
        return
    
    print(f"Testing pipeline API for account {account_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            url = "https://rest.gohighlevel.com/v1/pipelines/"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            print(f"Fetching: {url}")
            response = await client.get(url, headers=headers)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("Pipeline API Response:")
                print(json.dumps(data, indent=2))
                
                # Show summary
                pipelines = data.get('pipelines', [])
                print(f"\nSummary: Found {len(pipelines)} pipelines")
                
                for pipeline in pipelines:
                    pipeline_id = pipeline['id']
                    pipeline_name = pipeline['name']
                    stages = pipeline.get('stages', [])
                    print(f"\nPipeline: {pipeline_name} (ID: {pipeline_id})")
                    print(f"  Stages ({len(stages)}):")
                    for stage in stages:
                        print(f"    - {stage['name']} (ID: {stage['id']})")
            else:
                print(f"Error: {response.text}")
                
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_pipeline_api())
