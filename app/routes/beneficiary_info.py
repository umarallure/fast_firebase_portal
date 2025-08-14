import random
import csv
import os
import requests
import logging
from dotenv import load_dotenv
from io import StringIO
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, UploadFile, File, Response

router = APIRouter()
load_dotenv()
API_VERSION = os.getenv("API_VERSION", "2021-07-28")
BASE_URL = "https://services.leadconnectorhq.com"
SUBACCOUNTS = os.getenv("SUBACCOUNTS")
subaccounts = {str(acc["id"]): acc for acc in json.loads(SUBACCOUNTS)} if SUBACCOUNTS else {}

@router.post("/test-beneficiary-info")
async def test_beneficiary_info_api(file: UploadFile = File(...)):
    content = await file.read()
    f = StringIO(content.decode())
    reader = csv.DictReader(f)
    rows = list(reader)
    # Group contacts by subaccount
    contacts_by_sub = {}
    for row in rows:
        contact_id = row.get("Contact ID")
        account_id = str(row.get("Account Id"))
        if account_id not in contacts_by_sub:
            contacts_by_sub[account_id] = []
        contacts_by_sub[account_id].append(contact_id)
    # For each subaccount, pick 5 random contacts
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Account Id", "Contact ID", "Beneficiary Information"])
    for sub_id, contact_ids in contacts_by_sub.items():
        sample_ids = random.sample(contact_ids, min(5, len(contact_ids)))
        results = process_subaccount_contacts(sub_id, sample_ids)
        for row in results:
            writer.writerow([sub_id] + row)
    return Response(content=output.getvalue(), media_type="text/csv")
from fastapi import APIRouter, UploadFile, File, Response
import csv
import os
import requests
import logging
from dotenv import load_dotenv
from io import StringIO
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter()
load_dotenv()
API_VERSION = os.getenv("API_VERSION", "2021-07-28")
BASE_URL = "https://services.leadconnectorhq.com"
SUBACCOUNTS = os.getenv("SUBACCOUNTS")
subaccounts = {str(acc["id"]): acc for acc in json.loads(SUBACCOUNTS)} if SUBACCOUNTS else {}

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

def get_beneficiary_info(contact_id, field_id, token):
    url = f"{BASE_URL}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {token}",
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
    api_key = acc.get("api_key")
    access_token = acc.get("access_token")
    location_id = acc.get("location_id")
    field_id = get_custom_field_id("Beneficiary Information", location_id, access_token)
    results = []
    def fetch(contact_id):
        try:
            if not field_id:
                return [contact_id, "Field not found"]
            value = get_beneficiary_info(contact_id, field_id, api_key)
            return [contact_id, value]
        except Exception as e:
            return [contact_id, f"Error: {str(e)}"]
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_contact = {executor.submit(fetch, cid): cid for cid in contacts}
        for future in as_completed(future_to_contact):
            results.append(future.result())
    return results

@router.post("/get-beneficiary-info")
async def get_beneficiary_info_api(file: UploadFile = File(...)):
    content = await file.read()
    f = StringIO(content.decode())
    reader = csv.DictReader(f)
    rows = list(reader)
    # Group contacts by subaccount
    contacts_by_sub = {}
    for row in rows:
        contact_id = row.get("Contact ID")
        account_id = str(row.get("Account Id"))
        if account_id not in contacts_by_sub:
            contacts_by_sub[account_id] = []
        contacts_by_sub[account_id].append(contact_id)
    # Prepare output files
    output_files = {}
    for sub_id, contact_ids in contacts_by_sub.items():
        results = process_subaccount_contacts(sub_id, contact_ids)
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Contact ID", "Beneficiary Information"])
        for row in results:
            writer.writerow(row)
        output_files[sub_id] = output.getvalue()
    # If only one subaccount, return as CSV
    if len(output_files) == 1:
        return Response(content=list(output_files.values())[0], media_type="text/csv")
    # If multiple, return as zip
    import zipfile
    import io as pyio
    zip_buffer = pyio.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        for sub_id, csv_content in output_files.items():
            zf.writestr(f"beneficiary_info_{sub_id}.csv", csv_content)
    zip_buffer.seek(0)
    return Response(content=zip_buffer.read(), media_type="application/zip")
