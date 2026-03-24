import os
import json
import httpx
import pandas as pd
from fastapi import APIRouter, Query, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv
from io import StringIO
import asyncio
import uuid
from typing import Dict, Optional
import time
from datetime import datetime
from app.config import settings

load_dotenv()
router = APIRouter()

# Global progress tracking with timestamps
progress_store: Dict[str, Dict] = {}

# Column order for output CSV
CSV_COLUMNS = [
    'Opportunity Name', 'Contact Name', 'phone', 'email', 'pipeline', 'stage',
    'Created on', 'Updated on', 'Opportunity ID', 'Contact ID', 'Pipeline Stage ID', 'Pipeline ID', 'Account Id',
    'note1', 'note2', 'note3', 'note4'
]

class GHLClientV2:
    """GHL API V2 Client using LeadConnectorHQ endpoints"""
    
    def __init__(self, access_token: str, location_id: str):
        self.base_url = "https://services.leadconnectorhq.com"
        self.access_token = access_token
        self.location_id = location_id
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Version": "2021-07-28"
        }
        self.timeout = settings.ghl_api_timeout
    
    async def get_pipelines(self, client: httpx.AsyncClient) -> list:
        """Fetch all pipelines for the location using V2 API"""
        try:
            url = f"{self.base_url}/opportunities/pipelines?locationId={self.location_id}"
            response = await client.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return data.get("pipelines", [])
        except httpx.HTTPError as e:
            print(f"DEBUG: Error fetching pipelines for location {self.location_id}: {e}")
            return []
    
    async def search_opportunities(
        self, 
        client: httpx.AsyncClient, 
        pipeline_id: str = None,
        pipeline_stage_id: str = None
    ) -> list:
        """Search opportunities using V2 API - includes notes!"""
        all_opportunities = []
        page = 1
        limit = 100
        
        while True:
            try:
                # Build URL with search parameters
                url = f"{self.base_url}/opportunities/search?location_id={self.location_id}&limit={limit}&page={page}"
                
                if pipeline_id:
                    url += f"&pipeline_id={pipeline_id}"
                if pipeline_stage_id:
                    url += f"&pipeline_stage_id={pipeline_stage_id}"
                
                response = await client.get(url, headers=self.headers, timeout=self.timeout)
                
                if response.status_code == 429:
                    wait_time = 2
                    print(f"DEBUG: Rate limited on page {page}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status_code != 200:
                    print(f"DEBUG: Error {response.status_code}: {response.text}")
                    break
                
                data = response.json()
                opportunities = data.get("opportunities", [])
                meta = data.get("meta", {})
                
                if not opportunities:
                    break
                
                all_opportunities.extend(opportunities)
                
                # Check pagination
                current_page = meta.get("currentPage", page)
                total = meta.get("total", 0)
                total_pages = (total // limit) + 1
                
                if current_page >= total_pages:
                    break
                
                page += 1
                await asyncio.sleep(0.1)  # Rate limit protection
                
            except httpx.HTTPError as e:
                print(f"DEBUG: HTTP error searching opportunities: {e}")
                break
        
        return all_opportunities
    
    async def fetch_contact_notes(self, client: httpx.AsyncClient, contact_id: str) -> list:
        """Fetch notes for a contact using V2 API"""
        if not contact_id:
            return []
        
        try:
            url = f"{self.base_url}/contacts/{contact_id}/notes"
            response = await client.get(url, headers=self.headers, timeout=self.timeout)
            
            if response.status_code == 429:
                # Rate limited - wait and retry once
                await asyncio.sleep(2)
                response = await client.get(url, headers=self.headers, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                notes = data.get("notes", [])
                # Sort by created date (newest first) and return bodies
                notes_sorted = sorted(notes, key=lambda n: n.get("createdAt", ""), reverse=True)
                return [n.get("body", "") for n in notes_sorted[:4]]
            else:
                print(f"DEBUG: Error fetching notes for contact {contact_id}: {response.status_code}")
                return []
        except Exception as e:
            print(f"DEBUG: Exception fetching notes for contact {contact_id}: {e}")
            return []
    
    @staticmethod
    def format_opportunity(opp: dict, pipeline_name: str, stage_name: str, account_id: str, notes_list: list = []) -> dict:
        """Format opportunity data for CSV export"""
        contact = opp.get("contact", {})
        
        # Parse dates
        def parse_date(date_str):
            if not date_str:
                return ""
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return date_str
        
        # Use provided notes or empty list
        if notes_list is None:
            notes_list = []
        
        # Pad to 4 notes
        while len(notes_list) < 4:
            notes_list.append("")
        
        return {
            'Opportunity Name': opp.get("name", ""),
            'Contact Name': contact.get("name", ""),
            'phone': contact.get("phone", ""),
            'email': contact.get("email", ""),
            'pipeline': pipeline_name,
            'stage': stage_name,
            'Created on': parse_date(opp.get("createdAt")),
            'Updated on': parse_date(opp.get("updatedAt")),
            'Opportunity ID': opp.get("id", ""),
            'Contact ID': contact.get("id", ""),
            'Pipeline Stage ID': opp.get("pipelineStageId", ""),
            'Pipeline ID': opp.get("pipelineId", ""),
            'Account Id': account_id,
            'note1': notes_list[0] if len(notes_list) > 0 else "",
            'note2': notes_list[1] if len(notes_list) > 1 else "",
            'note3': notes_list[2] if len(notes_list) > 2 else "",
            'note4': notes_list[3] if len(notes_list) > 3 else ""
        }

@router.get('/export-ghl-opportunities')
async def export_ghl_opportunities(
    subaccount_ids: list = Query(...), 
    background_tasks: BackgroundTasks = None
):
    """
    Start GHL opportunities export for selected subaccounts using V2 API.
    """
    # Get subaccounts with V2 credentials
    subaccounts = settings.subaccounts_list
    selected_subs = []
    
    for sub in subaccounts:
        if str(sub.get('id')) in subaccount_ids:
            # Check for V2 credentials
            if sub.get('access_token') and sub.get('location_id'):
                selected_subs.append(sub)
            else:
                return JSONResponse(
                    status_code=400, 
                    content={"error": f"Subaccount {sub.get('name', sub['id'])} missing V2 credentials (access_token or location_id)"}
                )
    
    if not selected_subs:
        return JSONResponse(status_code=400, content={"error": "No valid subaccounts selected with V2 credentials"})
    
    # Generate unique task ID
    task_id = str(uuid.uuid4())
    
    # Initialize progress
    progress_store[task_id] = {
        "status": "starting",
        "progress": 0,
        "message": "Initializing export...",
        "total_subaccounts": len(selected_subs),
        "completed_subaccounts": 0,
        "current_subaccount": "",
        "total_opportunities": 0,
        "processed_opportunities": 0,
        "current_stage": "Initializing...",
        "csv_data": None,
        "filename": "",
        "start_time": time.time(),
        "estimated_time_remaining": None
    }
    
    # Start background export
    background_tasks.add_task(process_export_v2, task_id, selected_subs)
    
    return {"task_id": task_id, "status": "started", "message": "Export started successfully using V2 API"}

async def process_export_v2(task_id: str, selected_subs: list):
    """
    Background task to process the GHL export using V2 API.
    """
    print(f"DEBUG: Starting V2 export process for task {task_id}")
    rows = []
    selected_names = []
    
    async with httpx.AsyncClient() as client:
        total_subs = len(selected_subs)
        
        for idx, sub in enumerate(selected_subs):
            access_token = sub.get('access_token')
            location_id = sub.get('location_id')
            account_id = sub.get('id')
            selected_names.append(sub.get('name', str(account_id)))
            
            print(f"DEBUG: Processing subaccount {sub.get('name', str(account_id))} ({idx + 1}/{total_subs})")
            print(f"DEBUG: Using location_id: {location_id}")
            
            elapsed_time = time.time() - progress_store[task_id]["start_time"]
            progress_store[task_id].update({
                "status": "processing",
                "progress": (idx / total_subs) * 100,
                "message": f"Processing subaccount: {sub.get('name', str(account_id))}",
                "completed_subaccounts": idx,
                "current_subaccount": sub.get('name', str(account_id))
            })
            
            # Calculate estimated time remaining
            if elapsed_time > 0 and idx > 0:
                avg_time_per_subaccount = elapsed_time / idx
                remaining_subs = total_subs - idx
                estimated_remaining = avg_time_per_subaccount * remaining_subs
                progress_store[task_id]["estimated_time_remaining"] = estimated_remaining
            
            # Create V2 client
            ghl_client = GHLClientV2(access_token, location_id)
            
            # Fetch pipelines using V2 API
            progress_store[task_id]["current_stage"] = "Fetching pipelines..."
            pipelines = await ghl_client.get_pipelines(client)
            
            if not pipelines:
                print(f"DEBUG: No pipelines found for subaccount {sub.get('name', str(account_id))}")
                continue
            
            print(f"DEBUG: Found {len(pipelines)} pipelines")
            
            # Build stage mapping
            stage_map = {}
            pipeline_map = {}
            for pipeline in pipelines:
                pid = pipeline.get('id')
                pname = pipeline.get('name')
                pipeline_map[pid] = pname
                for stage in pipeline.get('stages', []):
                    stage_map[stage.get('id')] = stage.get('name')
            
            # Process each pipeline
            for pipeline in pipelines:
                pipeline_id = pipeline.get('id')
                pipeline_name = pipeline.get('name')
                
                progress_store[task_id]["current_stage"] = f"Searching opportunities in {pipeline_name}..."
                print(f"DEBUG: Searching opportunities for pipeline '{pipeline_name}'")
                
                # Search opportunities using V2 API (includes notes!)
                opportunities = await ghl_client.search_opportunities(
                    client, 
                    pipeline_id=pipeline_id
                )
                
                if not opportunities:
                    print(f"DEBUG: No opportunities found in pipeline {pipeline_name}")
                    continue
                
                print(f"DEBUG: Found {len(opportunities)} opportunities in {pipeline_name}")
                progress_store[task_id]["total_opportunities"] += len(opportunities)
                
                # Fetch notes for all opportunities in this pipeline
                progress_store[task_id]["current_stage"] = f"Fetching notes for {len(opportunities)} opportunities from {pipeline_name}..."
                
                # Collect unique contact IDs
                contact_ids = []
                opp_contact_map = {}  # Map opportunity index to contact_id
                for i, opp in enumerate(opportunities):
                    contact_id = opp.get('contact', {}).get('id')
                    if contact_id and contact_id not in contact_ids:
                        contact_ids.append(contact_id)
                    opp_contact_map[i] = contact_id
                
                print(f"DEBUG: Fetching notes for {len(contact_ids)} unique contacts...")
                
                # Fetch notes with concurrency limit
                semaphore = asyncio.Semaphore(10)  # Max 10 concurrent requests
                contact_notes = {}
                
                async def fetch_notes_with_limit(contact_id):
                    async with semaphore:
                        notes = await ghl_client.fetch_contact_notes(client, contact_id)
                        return contact_id, notes
                
                # Create tasks for all contacts
                note_tasks = [fetch_notes_with_limit(cid) for cid in contact_ids if cid]
                
                # Process in batches to show progress
                batch_size = 20
                for i in range(0, len(note_tasks), batch_size):
                    batch = note_tasks[i:i + batch_size]
                    
                    try:
                        batch_results = await asyncio.gather(*batch)
                        
                        for result in batch_results:
                            try:
                                cid, notes = result
                                contact_notes[cid] = notes
                            except Exception as e:
                                print(f"DEBUG: Error processing notes result: {e}")
                    except Exception as e:
                        print(f"DEBUG: Error in batch: {e}")
                    
                    # Update progress
                    progress_store[task_id]["current_stage"] = f"Fetched notes for {min(i + batch_size, len(note_tasks))}/{len(note_tasks)} contacts..."
                    await asyncio.sleep(0.1)  # Brief pause between batches
                
                print(f"DEBUG: Fetched notes for {len(contact_notes)} contacts")
                
                # Format opportunities with their notes
                progress_store[task_id]["current_stage"] = f"Processing {len(opportunities)} opportunities from {pipeline_name}..."
                
                for i, opp in enumerate(opportunities):
                    stage_id = opp.get('pipelineStageId', '')
                    stage_name = stage_map.get(stage_id, '')
                    contact_id = opp_contact_map.get(i)
                    notes_list = contact_notes.get(contact_id, []) if contact_id else []
                    
                    row = ghl_client.format_opportunity(opp, pipeline_name, stage_name, account_id, notes_list)
                    rows.append(row)
                    
                    # Update progress
                    progress_store[task_id]["processed_opportunities"] += 1
                
                # Small delay between pipelines
                await asyncio.sleep(0.2)
            
            # Small delay between subaccounts
            print(f"DEBUG: Finished processing subaccount {sub.get('name', str(account_id))}")
            await asyncio.sleep(0.5)
    
    # Generate CSV
    progress_store[task_id]["current_stage"] = "Generating CSV file..."
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Build filename
    if selected_names:
        safe_names = [n.replace(' ', '_').replace(',', '') for n in selected_names]
        filename = f"ghl_export_{'_'.join(safe_names)}.csv"
    else:
        filename = "ghl_opportunities_export.csv"
    
    # Update progress as completed
    total_time = time.time() - progress_store[task_id]["start_time"]
    print(f"DEBUG: V2 Export completed - Total rows: {len(rows)}")
    
    progress_store[task_id].update({
        "status": "completed",
        "progress": 100,
        "message": f"Export completed! Processed {len(rows)} opportunities in {total_time:.1f} seconds (V2 API).",
        "completed_subaccounts": total_subs,
        "csv_data": csv_buffer.getvalue(),
        "filename": filename,
        "total_time": total_time,
        "estimated_time_remaining": 0,
        "current_stage": "Complete"
    })

@router.get('/export-progress/{task_id}')
async def get_export_progress(task_id: str):
    """
    Get the progress of an export task.
    """
    if task_id not in progress_store:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    
    return progress_store[task_id]

@router.get('/download-export/{task_id}')
async def download_export(task_id: str):
    """
    Download the completed export CSV.
    """
    if task_id not in progress_store:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    
    task_data = progress_store[task_id]
    if task_data["status"] != "completed" or not task_data["csv_data"]:
        return JSONResponse(status_code=400, content={"error": "Export not completed yet"})
    
    # Clean up progress store after download
    csv_data = task_data["csv_data"]
    filename = task_data["filename"]
    del progress_store[task_id]
    
    return StreamingResponse(
        StringIO(csv_data), 
        media_type='text/csv', 
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )
