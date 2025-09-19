import os
import pandas as pd
import httpx
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
TOKEN = "pit-6a5701cc-7121-447e-ac5f-eda3ad51a4c4"  # From .env selection

# Read the CSV file
csv_file = r"c:\Users\Dell\Desktop\Data\Masterdeleteoppportunities.csv"
if not os.path.exists(csv_file):
    print(f"CSV file {csv_file} not found.")
    exit(1)

df = pd.read_csv(csv_file)

# Assume the column with opportunity IDs is 'Opportunity ID'
id_column = 'Opportunity ID'

if id_column not in df.columns:
    print(f"Column '{id_column}' not found in CSV. Available columns: {list(df.columns)}")
    exit(1)

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Version": API_VERSION,
    "Accept": "application/json"
}

for index, row in df.iterrows():
    opp_id = row[id_column]
    url = f"{API_BASE}/opportunities/{opp_id}"
    try:
        resp = httpx.delete(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            print(f"Successfully deleted opportunity {opp_id}")
        else:
            print(f"Failed to delete opportunity {opp_id}: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Error deleting opportunity {opp_id}: {e}")

print("Deletion process completed.")
