import requests

# Test the unmatched CSV download endpoint
matching_id = "6ab764b1-856e-48e0-bf3f-0777b59b9b1e"  # From the matching test
url = f"http://127.0.0.1:8000/api/master-child-opportunity-update/unmatched-csv/{matching_id}"

try:
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("Unmatched CSV downloaded successfully!")
        print("Content length:", len(response.text))
        print("First 500 characters:")
        print(response.text[:500])
    else:
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"Error: {e}")