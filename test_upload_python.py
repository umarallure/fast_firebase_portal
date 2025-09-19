import requests

# Test the upload endpoint
url = "http://127.0.0.1:8000/api/master-child-opportunity-update/upload"

files = {
    'master_file': ('test_master_opportunities.csv', open('test_master_opportunities.csv', 'rb'), 'text/csv'),
    'child_file': ('test_child_opportunities.csv', open('test_child_opportunities.csv', 'rb'), 'text/csv')
}

try:
    response = requests.post(url, files=files)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {e}")