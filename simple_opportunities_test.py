#!/usr/bin/env python3
"""
Simple debug script to test opportunity fetching
"""

import asyncio
import sys
import os
import httpx

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.config import settings

async def test_opportunities_simple():
    """Test fetching opportunities directly"""
    
    # Find account 46
    test_account = None
    for account in settings.subaccounts_list:
        if str(account.get('id')) == '46':
            test_account = account
            break
    
    if not test_account:
        print("ERROR: Account 46 not found")
        return
    
    api_key = test_account.get('api_key')
    print(f"Testing with account: {test_account.get('name')}")
    print(f"API Key available: {bool(api_key)}")
    
    # Test direct API call to Transfer Portal pipeline
    pipeline_id = "AU9GXsQJHcWRC1fKqu8K"  # Transfer Portal
    
    async with httpx.AsyncClient(timeout=60) as client:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        response = await client.get(
            f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities", 
            headers=headers,
            params={"limit": 10}
        )
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            opportunities = data.get('opportunities', [])
            print(f"Found {len(opportunities)} opportunities")
            
            for i, opp in enumerate(opportunities):
                print(f"Opportunity {i+1}:")
                print(f"  Name: {opp.get('name')}")
                print(f"  ID: {opp.get('id')}")
                print(f"  Contact ID: {opp.get('contactId')}")
                print(f"  Status: {opp.get('status')}")
                print()
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text}")

if __name__ == "__main__":
    asyncio.run(test_opportunities_simple())