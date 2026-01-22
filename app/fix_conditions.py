import requests
import json
import time

# --- CONFIGURATION ---
API_KEY = "37e0830c723c38868d630c9d251f23b3"
FORM_ID = "260192517833458" 

# The endpoint used in condition.py was wrong. 
# We should use the properties endpoint to update form properties, including conditions.
ENDPOINT = f"https://api.jotform.com/form/{FORM_ID}/properties"

# --- CONDITIONS DATA ---
# (Copied from your original script)
conditions_list = [
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "29", "operator": "equals", "value": "No"}],
        "link": "any",
        "target": "32"
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "33", "operator": "equals", "value": "Yes"}],
        "link": "any",
        "target": "34"
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "26", "operator": "equals", "value": "NO"}],
        "link": "any",
        "target": "40"
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "35", "operator": "equals", "value": "No"}],
        "link": "any",
        "target": "36"
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "16", "operator": "equals", "value": "NO"}],
        "link": "any",
        "target": "37"
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "20", "operator": "equals", "value": "NO"}],
        "link": "any",
        "target": "39"
    }
]

def apply_conditions():
    headers = {"apiKey": API_KEY}
    
    print(f"Applying {len(conditions_list)} conditions to Form {FORM_ID}...")
    
    # In JotForm API, 'conditions' is a property of the form.
    # We must stringify the entire list of conditions and send it as 'properties[conditions]'.
    # Warning: This REPLACES all existing conditions on the form.
    
    payload = {
        "properties[conditions]": json.dumps(conditions_list)
    }
    
    response = requests.post(ENDPOINT, data=payload, headers=headers)
    
    if response.status_code == 200:
        print("SUCCESS: Conditions applied.")
        print("Response:", response.text)
    else:
        print(f"FAILED: {response.status_code} - {response.text}")

if __name__ == "__main__":
    apply_conditions()
