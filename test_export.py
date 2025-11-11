import requests
import json

def test_export():
    # Get subaccounts
    print("Getting subaccounts...")
    response = requests.get('http://localhost:8001/api/v1/subaccounts')
    if response.status_code != 200:
        print(f"Failed to get subaccounts: {response.status_code}")
        return

    subaccounts = response.json()
    print(f"Found {len(subaccounts)} subaccounts")

    if not subaccounts:
        print("No subaccounts available")
        return

    # Use first subaccount
    account = subaccounts[0]
    account_id = str(account['id'])
    print(f"Using account {account_id}: {account.get('name', 'Unknown')}")

    # Get pipelines
    print("Getting pipelines...")
    pipelines_response = requests.get(f'http://localhost:8001/api/v1/pipelines/{account_id}')
    if pipelines_response.status_code != 200:
        print(f"Failed to get pipelines: {pipelines_response.status_code}")
        return

    pipelines = pipelines_response.json()
    print(f"Found {len(pipelines)} pipelines")

    if not pipelines:
        print("No pipelines available")
        return

    # Use first pipeline
    pipeline_id = pipelines[0]['id']
    pipeline_name = pipelines[0]['name']
    print(f"Using pipeline {pipeline_id}: {pipeline_name}")

    # Create export request
    export_data = {
        'selections': [{
            'account_id': account_id,
            'pipelines': [pipeline_id]
        }]
    }

    print("Making export request...")
    print(f"Request data: {json.dumps(export_data, indent=2)}")

    # Make export request
    export_response = requests.post(
        'http://localhost:8001/api/v1/automation/export-opportunities-only',
        json=export_data,
        headers={'Content-Type': 'application/json'}
    )

    print(f"Export response status: {export_response.status_code}")
    if export_response.status_code == 200:
        content_length = len(export_response.content)
        print(f"Export successful! Content length: {content_length} bytes")

        # Save to file for inspection
        with open('test_export.xlsx', 'wb') as f:
            f.write(export_response.content)
        print("Saved export to test_export.xlsx")
    else:
        print(f"Export failed: {export_response.text}")

if __name__ == "__main__":
    test_export()
