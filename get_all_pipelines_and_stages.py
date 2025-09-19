import os
import json
import pandas as pd
import httpx
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://services.leadconnectorhq.com"
API_VERSION = "2021-07-28"
SUBACCOUNTS = json.loads(os.getenv('SUBACCOUNTS', '[]'))

rows = []

for sub in SUBACCOUNTS:
    account_id = sub.get('id')
    account_name = sub.get('name')
    location_id = sub.get('location_id')
    access_token = sub.get('access_token')
    if not location_id or not access_token:
        print(f"Skipping {account_name} ({account_id}): missing location_id or access_token")
        continue

    url = f"{API_BASE}/opportunities/pipelines?locationId={location_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": API_VERSION,
        "Accept": "application/json"
    }
    try:
        resp = httpx.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"Failed for {account_name} ({account_id}): {resp.status_code} {resp.text}")
            continue
        pipelines = resp.json().get("pipelines", [])
        for pipeline in pipelines:
            pipeline_id = pipeline.get("id")
            pipeline_name = pipeline.get("name")
            for stage in pipeline.get("stages", []):
                stage_id = stage.get("id")
                stage_name = stage.get("name")
                rows.append({
                    "Account Id": account_id,
                    "Account Name": account_name,
                    "Pipeline Id": pipeline_id,
                    "Pipeline Name": pipeline_name,
                    "Stage Id": stage_id,
                    "Stage Name": stage_name
                })
    except Exception as e:
        print(f"Error for {account_name} ({account_id}): {e}")

df = pd.DataFrame(rows)
df.to_csv("all_pipelines_and_stages.csv", index=False)
print("CSV saved as all_pipelines_and_stages.csv")
