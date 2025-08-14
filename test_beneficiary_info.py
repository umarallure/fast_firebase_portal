import requests
import json

# Ark Tech subaccount details from .env
subaccount = {
    "id": "2",
    "name": "Ark Tech",
    "location_id": "4PGZ1Ak32mAxq8gGWptN",
    "api_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsb2NhdGlvbl9pZCI6IjRQR1oxQWszMm1BeHE4Z0dXcHROIiwidmVyc2lvbiI6MSwiaWF0IjoxNzQ0MjI3NTQ4MDMyLCJzdWIiOiJhSjhDSFRGT0huR0xUbmZqdTB5NiJ9.5LHIOyywSjfkhlqofzN_AsgYG3YMrAf4ylR2MFzwGxk",
    "access_token": "pit-b6a3892e-3143-4928-8093-2dac3644ebfb"
}

contact_id = "OzR3gXeOVuqwanGMICFQ"
location_id = subaccount["location_id"]
api_key = subaccount["api_key"]
access_token = subaccount["access_token"]

# 1. Get custom fields
custom_fields_url = f"https://services.leadconnectorhq.com/locations/{location_id}/customFields"
headers_cf = {
    "Authorization": f"Bearer {access_token}",
    "Version": "2021-07-28"
}
cf_resp = requests.get(custom_fields_url, headers=headers_cf)
cf_data = cf_resp.json()
print("Custom Fields:", json.dumps(cf_data, indent=2))

# 2. Find Beneficiary Information field
beneficiary_field = next(
    (f for f in cf_data.get("customFields", []) if "beneficiary" in f.get("name", "").lower()),
    None
)
if not beneficiary_field:
    print("Beneficiary Information field not found.")
    exit()

beneficiary_field_id = beneficiary_field["id"]
print("Beneficiary Field ID:", beneficiary_field_id)

# 3. Get contact data
contact_url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
headers_contact = {
    "Authorization": f"Bearer {access_token}",
    "Version": "2021-07-28"
}
contact_resp = requests.get(contact_url, headers=headers_contact)
contact_data = contact_resp.json()
print("Contact Data:", json.dumps(contact_data, indent=2))

# 4. Map and print Beneficiary Information value
custom_field_values = contact_data.get("contact", {}).get("customFields", [])
beneficiary_value = next(
    (f.get("value") for f in custom_field_values if f.get("id") == beneficiary_field_id),
    None
)
print("Beneficiary Information Value:", beneficiary_value)
