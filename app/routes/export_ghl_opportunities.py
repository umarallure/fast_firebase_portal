import os
import json
import requests
import pandas as pd
from fastapi import APIRouter, Query, Response
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from io import StringIO

load_dotenv()
router = APIRouter()

# Load subaccounts from .env
SUBACCOUNTS = json.loads(os.getenv('SUBACCOUNTS', '[]'))

# Column order for output CSV
CSV_COLUMNS = [
    'Opportunity Name', 'Contact Name', 'phone', 'email', 'pipeline', 'stage',
    'Created on', 'Updated on', 'Opportunity ID', 'Contact ID', 'Pipeline Stage ID', 'Pipeline ID', 'Account Id',
    'note1', 'note2', 'note3', 'note4'
]

@router.get('/export-ghl-opportunities')
def export_ghl_opportunities(subaccount_ids: list = Query(...)):
    """
    Export GHL opportunities and contacts for selected subaccounts as CSV.
    """
    # Placeholder: fetch and process data for each subaccount
    rows = []
    selected_names = []
    for sub in SUBACCOUNTS:
        if sub['id'] not in subaccount_ids:
            continue
        api_key = sub.get('api_key')
        account_id = sub.get('id')
        selected_names.append(sub.get('name', str(account_id)))
        # Fetch pipelines
        pipelines_resp = requests.get(
            'https://rest.gohighlevel.com/v1/pipelines/',
            headers={'Authorization': f'Bearer {api_key}'}
        )
        if pipelines_resp.status_code != 200:
            # Skip this subaccount if API fails
            pipelines = []
        else:
            pipelines = pipelines_resp.json().get('pipelines', [])
        for pipeline in pipelines:
            pipeline_id = pipeline.get('id')
            pipeline_name = pipeline.get('name')
            # ...existing code...
            # Build stageId -> stageName mapping for this pipeline
            stage_map = {}
            for stage in pipeline.get('stages', []):
                # ...existing code...
                stage_map[stage.get('id')] = stage.get('name')
            # ...existing code...
            # Fetch opportunities (handle pagination)
            next_page = True
            start_after_id = None
            while next_page:
                url = f'https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities?limit=100'
                if start_after_id:
                    url += f'&startAfterId={start_after_id}'
                opp_resp = requests.get(url, headers={'Authorization': f'Bearer {api_key}'})
                if opp_resp.status_code != 200:
                    break
                opp_data = opp_resp.json()
                opportunities = opp_data.get('opportunities', [])
                for opp in opportunities:
                    # Extract required fields
                    contact = opp.get('contact', {})
                    stage_id = opp.get('pipelineStageId', '')
                    # ...existing code...
                    stage_name = stage_map.get(stage_id, '')
                    # ...existing code...
                    # Fetch latest 4 notes for contact with debugging
                    contact_id = contact.get('id', '')
                    notes_url = f'https://rest.gohighlevel.com/v1/contacts/{contact_id}/notes/'
                    notes_list = []
                    if contact_id:
                        notes_resp = requests.get(notes_url, headers={'Authorization': f'Bearer {api_key}'})
                        # ...existing code...
                        if notes_resp.status_code == 200:
                            notes_json = notes_resp.json()
                            # ...existing code...
                            notes_data = notes_json.get('notes', [])
                            # Sort by createdAt DESC
                            notes_sorted = sorted(notes_data, key=lambda n: n.get('createdAt', ''), reverse=True)
                            notes_list = [n.get('body', '') for n in notes_sorted[:4]]
                    # Pad notes to 4
                    while len(notes_list) < 4:
                        notes_list.append('')
                    row = {
                        'Opportunity Name': opp.get('name', ''),
                        'Contact Name': contact.get('name', ''),
                        'phone': contact.get('phone', ''),
                        'email': contact.get('email', ''),
                        'pipeline': pipeline_name,
                        'stage': stage_name,
                        'Created on': opp.get('createdAt', ''),
                        'Updated on': opp.get('updatedAt', ''),
                        'Opportunity ID': opp.get('id', ''),
                        'Contact ID': contact.get('id', ''),
                        'Pipeline Stage ID': stage_id,
                        'Pipeline ID': pipeline_id,
                        'Account Id': account_id,
                        'note1': notes_list[0],
                        'note2': notes_list[1],
                        'note3': notes_list[2],
                        'note4': notes_list[3]
                    }
                    # ...existing code...
                    rows.append(row)
                # Pagination
                if len(opportunities) == 100:
                    start_after_id = opportunities[-1].get('id')
                else:
                    next_page = False
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    # Build filename from selected center names
    if selected_names:
        safe_names = [n.replace(' ', '_').replace(',', '') for n in selected_names]
        filename = f"ghl_export_{'_'.join(safe_names)}.csv"
    else:
        filename = "ghl_opportunities_export.csv"
    return StreamingResponse(csv_buffer, media_type='text/csv', headers={
        'Content-Disposition': f'attachment; filename={filename}'
    })
