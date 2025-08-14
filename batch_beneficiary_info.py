import csv
import os
import requests
import logging
from dotenv import load_dotenv
from io import StringIO
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
API_VERSION = os.getenv("API_VERSION", "2021-07-28")
BASE_URL = "https://services.leadconnectorhq.com"
SUBACCOUNTS = os.getenv("SUBACCOUNTS")
subaccounts = {str(acc["id"]): acc for acc in json.loads(SUBACCOUNTS)} if SUBACCOUNTS else {}

# Helper to get custom field id by name (per location)
def get_custom_fields(location_id, access_token):
    url = f"{BASE_URL}/locations/{location_id}/customFields"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": API_VERSION
    }
    params = {"model": "contact"}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        return []
    return resp.json().get("customFields", [])

def get_custom_field_id(field_name, location_id, access_token):
    fields = get_custom_fields(location_id, access_token)
    for field in fields:
        if field.get("name") == field_name:
            return field.get("id")
    return None

def get_beneficiary_info(contact_id, field_id, access_token):
    url = f"{BASE_URL}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": API_VERSION
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return ""
    data = resp.json().get("contact", {})
    for field in data.get("customFields", []):
        if field.get("id") == field_id:
            return field.get("value", "")
    return ""

def process_subaccount_contacts(sub_id, contacts):
    acc = subaccounts.get(sub_id)
    if not acc:
        return None
    access_token = acc.get("access_token")
    location_id = acc.get("location_id")
    field_id = get_custom_field_id("Beneficiary Information", location_id, access_token)
    results = []
    def fetch(contact_id):
        try:
            if not field_id:
                return [contact_id, "Field not found"]
            value = get_beneficiary_info(contact_id, field_id, access_token)
            return [contact_id, value]
        except Exception as e:
            return [contact_id, f"Error: {str(e)}"]
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_contact = {executor.submit(fetch, cid): cid for cid in contacts}
        for future in as_completed(future_to_contact):
            results.append(future.result())
    return results

def batch_process_beneficiary_info(input_csv_path, output_dir):
    with open(input_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Read {len(rows)} rows from {input_csv_path}")
    # Group contacts by subaccount
    contacts_by_sub = {}
    for row in rows:
        contact_id = row.get("Contact ID") or row.get("Contact Id") or row.get("contact_id")
        if contact_id is None:
            contact_id = row.get("\ufeffContact ID")
        account_id = str(row.get("Account Id") or row.get("AccountID") or row.get("account_id"))
        if not contact_id:
            logging.warning(f"Missing contact ID for account {account_id} row: {row}")
            continue
        if account_id not in contacts_by_sub:
            contacts_by_sub[account_id] = []
        contacts_by_sub[account_id].append(contact_id)
    print(f"Found {len(contacts_by_sub)} subaccounts.")
    for sub_id, contact_ids in contacts_by_sub.items():
        print(f"Subaccount {sub_id}: {len(contact_ids)} contacts")
    os.makedirs(output_dir, exist_ok=True)
    output_files = []
    for sub_id, contact_ids in contacts_by_sub.items():
        results = process_subaccount_contacts(sub_id, contact_ids)
        if results is None:
            print(f"No results for subaccount {sub_id}")
            continue
        output_path = os.path.join(output_dir, f"beneficiary_info_{sub_id}.csv")
        with open(output_path, "w", newline='', encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            writer.writerow(["Contact ID", "Beneficiary Information"])
            for row in results:
                writer.writerow(row)
        print(f"Wrote {len(results)} rows to {output_path}")
        output_files.append(output_path)
    if not output_files:
        print("No output files created. Check input data and subaccount mapping.")
    return output_files



# Example usage:
# batch_process_beneficiary_info("childbeneficiery.csv", "python batch_beneficiary_info.py MasterContactsExport.csv output_beneficiary_info")

# Simple test: get data for 5 random entries from the datasheet
def test_random_beneficiary_info(input_csv_path):
    import random
    with open(input_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    sample_rows = random.sample(rows, min(5, len(rows)))
    for row in sample_rows:
        print(f"Row keys: {list(row.keys())}, Row values: {list(row.values())}")
        # Handle BOM in column name
        contact_id = row.get("Contact ID") or row.get("Contact Id") or row.get("contact_id")
        if contact_id is None:
            contact_id = row.get("\ufeffContact ID")
        account_id = str(row.get("Account Id") or row.get("AccountID") or row.get("account_id"))
        acc = subaccounts.get(account_id)
        if not acc:
            print(f"Account {account_id} not found for contact {contact_id}")
            continue
        access_token = acc.get("access_token")
        location_id = acc.get("location_id")
        field_id = get_custom_field_id("Beneficiary Information", location_id, access_token)
        value = get_beneficiary_info(contact_id, field_id, access_token)
        print(f"Contact ID: {contact_id}, Account Id: {account_id}, Beneficiary Information: {value}")

# Run batch if called from terminal
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python batch_beneficiary_info.py <input_csv_path> <output_dir>")
    else:
        batch_process_beneficiary_info(sys.argv[1], sys.argv[2])
