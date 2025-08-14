
import csv
import os
import requests
import time
import random
import json
from dotenv import load_dotenv

load_dotenv()

API_VERSION = "2021-07-28"
BASE_URL = "https://services.leadconnectorhq.com"
USER_ID = "sYUZNa30qZ9uH5V7hgXe"

# Load subaccount tokens from environment or config
SUBACCOUNTS = os.getenv("SUBACCOUNTS")
if SUBACCOUNTS:
    # Remove any wrapping single/double quotes
    if SUBACCOUNTS.startswith("'") and SUBACCOUNTS.endswith("'"):
        SUBACCOUNTS = SUBACCOUNTS[1:-1]
    if SUBACCOUNTS.startswith('"') and SUBACCOUNTS.endswith('"'):
        SUBACCOUNTS = SUBACCOUNTS[1:-1]
    subaccounts_list = json.loads(SUBACCOUNTS)
    subaccounts = {str(acc["id"]): acc for acc in subaccounts_list}
else:
    subaccounts = {}
print(f"Loaded subaccounts: {list(subaccounts.keys())}")

def create_note(contact_id, account_id, note_body):
    acc = subaccounts.get(str(account_id))
    if not acc:
        print(f"Account {account_id} not found for contact {contact_id}")
        return False
    access_token = acc.get("access_token")
    url = f"{BASE_URL}/contacts/{contact_id}/notes"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Version": API_VERSION,
        "Authorization": f"Bearer {access_token}"
    }
    payload = {
        "userId": USER_ID,
        "body": note_body
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 201:
        print(f"Note created for contact {contact_id} (account {account_id})")
        return True
    else:
        print(f"Failed to create note for contact {contact_id} (account {account_id}): {resp.status_code} {resp.text}")
        return False

def main():
    input_file = "output_beneficiary_info/beneficiary_info_33_with_notes.csv"
    with open(input_file, "r", encoding="latin1") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if row.get("Notes")]
    total = len(rows)
    print(f"Processing {total} contacts with notes...")
    for idx, row in enumerate(rows, start=1):
        contact_id = row["Contact ID"]
        account_id = row["Account Id"]
        note_body = row["Notes"]
        create_note(contact_id, account_id, note_body)
        print(f"Processed {idx}/{total}")
        time.sleep(1)  # avoid rate limits

if __name__ == "__main__":
    main()
