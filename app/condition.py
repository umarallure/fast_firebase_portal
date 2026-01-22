import requests
import json

# --- CONFIGURATION ---
API_KEY = "37e0830c723c38868d630c9d251f23b3"
FORM_ID = "260192517833458"
# The correct endpoint for batch updating conditions is via form properties
ENDPOINT = f"https://api.jotform.com/form/{FORM_ID}/properties"

# --- CONDITIONS DATA ---
# Each entry defines a rule: IF [field] EQUALS [value] THEN SHOW [target_field]
conditions = [
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "29", "operator": "equals", "value": "No"}],
        "link": "any",
        "target": "32" # Show "Accident > 12 months" warning 
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "33", "operator": "equals", "value": "Yes"}],
        "link": "any",
        "target": "34" # Show "Attorney involved" warning 
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "26", "operator": "equals", "value": "NO"}],
        "link": "any",
        "target": "40" # Show "Other party must admit fault" warning 
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "35", "operator": "equals", "value": "No"}],
        "link": "any",
        "target": "36" # Show "No medical attention within 2 weeks" warning [cite: 76, 77]
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "16", "operator": "equals", "value": "NO"}],
        "link": "any",
        "target": "37" # Show "Must have police report" warning [cite: 78, 79]
    },
    {
        "type": "field",
        "action": "show",
        "terms": [{"field": "20", "operator": "equals", "value": "NO"}],
        "link": "any",
        "target": "39" # Show "Must be insured" warning 
    }
]

def apply_form_conditions():
    headers = {"apiKey": API_KEY}
    
    # We must send all conditions at once as a JSON string under 'properties[conditions]'
    payload = {
        "properties[conditions]": json.dumps(conditions)
    }
    
    print(f"Applying {len(conditions)} conditions to form {FORM_ID}...")
    
    # Note: Use POST to update properties
    response = requests.post(ENDPOINT, data=payload, headers=headers)
    
    if response.status_code == 200:
        print(f" - SUCCESS: Conditions applied.")
    else:
        print(f" - FAILED: {response.status_code} - {response.text}")

if __name__ == "__main__":
    apply_form_conditions()
