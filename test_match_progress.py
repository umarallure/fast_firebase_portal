import requests

# Test the matching progress endpoint
matching_id = "f0ee900a-8fcf-48e2-aa2e-153853ee4c42"
url = f"http://127.0.0.1:8000/api/master-child-opportunity-update/match-progress/{matching_id}"

try:
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {e}")