import requests

API_KEY = "37e0830c723c38868d630c9d251f23b3"
FORM_ID = "260192517833458"
BASE_URL = "https://api.jotform.com"

def check_endpoint(method, path):
    url = f"{BASE_URL}{path}"
    headers = {"apiKey": API_KEY}
    print(f"Checking {method} {url}...")
    if method == "GET":
        response = requests.get(url, headers=headers)
    elif method == "POST":
        response = requests.post(url, headers=headers)
    
    print(f"Status: {response.status_code}")

    if response.status_code == 200:
        print(f"Success: {response.text[:200]}")
    else:
        print(f"Failed: {response.status_code}")

print("--- DEBUGGING JOTFORM API ---")
# Check specific property 'conditions'
check_endpoint("GET", f"/form/{FORM_ID}/properties/conditions")
