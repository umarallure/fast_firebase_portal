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
from typing import Dict
import time

load_dotenv()
router = APIRouter()

# Load subaccounts from .env
SUBACCOUNTS = json.loads(os.getenv('SUBACCOUNTS', '[]'))

# Global progress tracking with timestamps
progress_store: Dict[str, Dict] = {}

# Column order for output CSV
CSV_COLUMNS = [
    'Opportunity Name', 'Contact Name', 'phone', 'email', 'pipeline', 'stage',
    'Created on', 'Updated on', 'Opportunity ID', 'Contact ID', 'Pipeline Stage ID', 'Pipeline ID', 'Account Id',
    'note1', 'note2', 'note3', 'note4'
]

async def fetch_notes(client, contact_id, api_key):
    """Fetch latest 4 notes for a contact asynchronously with retry logic."""
    if not contact_id:
        return ['', '', '', '']
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            url = f'https://rest.gohighlevel.com/v1/contacts/{contact_id}/notes/'
            headers = {'Authorization': f'Bearer {api_key}'}
            resp = await client.get(url, headers=headers)
            
            if resp.status_code == 429:
                # Rate limited - wait with exponential backoff
                wait_time = 2 ** attempt
                print(f"DEBUG: Rate limited fetching notes for {contact_id}, attempt {attempt + 1}, waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            elif resp.status_code == 200:
                notes_data = resp.json().get('notes', [])
                notes_sorted = sorted(notes_data, key=lambda n: n.get('createdAt', ''), reverse=True)
                notes_list = [n.get('body', '') for n in notes_sorted[:4]]
                while len(notes_list) < 4:
                    notes_list.append('')
                return notes_list
            else:
                print(f"DEBUG: Error {resp.status_code} fetching notes for {contact_id}: {resp.text}")
                return ['', '', '', '']
        except Exception as e:
            print(f"Error fetching notes for {contact_id}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                await asyncio.sleep(wait_time)
                continue
            return ['', '', '', '']
    
    return ['', '', '', '']

@router.get('/export-ghl-opportunities')
async def export_ghl_opportunities(subaccount_ids: list = Query(...), background_tasks: BackgroundTasks = None):
    """
    Start GHL opportunities export for selected subaccounts and return task ID.
    """
    # Validate selected subaccounts
    selected_subs = [sub for sub in SUBACCOUNTS if str(sub['id']) in subaccount_ids]
    if not selected_subs:
        return JSONResponse(status_code=400, content={"error": "No valid subaccounts selected"})
    
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
        "notes_progress": 0,
        "total_notes": 0,
        "csv_data": None,
        "filename": "",
        "start_time": time.time(),
        "estimated_time_remaining": None
    }
    
    # Start background export
    background_tasks.add_task(process_export, task_id, selected_subs)
    
    return {"task_id": task_id, "status": "started", "message": "Export started successfully"}

async def process_export(task_id: str, selected_subs: list):
    """
    Background task to process the GHL export.
    """
    print(f"DEBUG: Starting export process for task {task_id}")
    rows = []
    selected_names = []
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        total_subs = len(selected_subs)
        subaccount_opportunities = {}  # Track opportunities per subaccount
        
        for idx, sub in enumerate(selected_subs):
            api_key = sub.get('api_key')
            account_id = sub.get('id')
            selected_names.append(sub.get('name', str(account_id)))
            
            print(f"DEBUG: Starting processing for subaccount {sub.get('name', str(account_id))} ({idx + 1}/{total_subs})")
            print(f"DEBUG: Using API key: {api_key[:10]}... for subaccount {account_id}")
            subaccount_opportunities[account_id] = 0  # Initialize counter
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
            
            # Fetch pipelines with rate limiting
            retry_count = 0
            max_retries = 3
            pipelines_resp = None
            
            while retry_count < max_retries:
                pipelines_resp = await client.get(
                    'https://rest.gohighlevel.com/v1/pipelines/',
                    headers={'Authorization': f'Bearer {api_key}'}
                )
                
                if pipelines_resp.status_code == 429:
                    wait_time = 2 ** retry_count
                    progress_store[task_id]["message"] = f"Rate limited fetching pipelines, waiting {wait_time}s..."
                    await asyncio.sleep(wait_time)
                    retry_count += 1
                    continue
                elif pipelines_resp.status_code != 200:
                    progress_store[task_id]["message"] = f"Error fetching pipelines for {sub.get('name', str(account_id))}"
                    break
                else:
                    break
            
            if not pipelines_resp or pipelines_resp.status_code != 200:
                continue
                
            pipelines = pipelines_resp.json().get('pipelines', [])
            print(f"DEBUG: Found {len(pipelines)} pipelines for subaccount {sub.get('name', str(account_id))}")
            
            for pipeline in pipelines:
                pipeline_id = pipeline.get('id')
                pipeline_name = pipeline.get('name')
                stage_map = {stage.get('id'): stage.get('name') for stage in pipeline.get('stages', [])}
                
                print(f"DEBUG: Processing pipeline '{pipeline_name}' for subaccount {sub.get('name', str(account_id))}")
                
                # Use cursor-based pagination with startAfterId and startAfter
                all_opportunities = []
                start_after_id = None
                start_after = None
                limit = 100
                
                while True:
                    # Build URL with cursor pagination parameters
                    url = f'https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities?limit={limit}'
                    if start_after_id and start_after:
                        url += f'&startAfterId={start_after_id}&startAfter={start_after}'
                    
                    opp_resp = await client.get(url, headers={'Authorization': f'Bearer {api_key}'})
                    
                    if opp_resp.status_code == 429:
                        wait_time = 2
                        print(f"DEBUG: Rate limited for pipeline {pipeline_name}, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    elif opp_resp.status_code != 200:
                        print(f"DEBUG: Error {opp_resp.status_code} for pipeline {pipeline_name}: {opp_resp.text}")
                        progress_store[task_id]["message"] = f"Error fetching opportunities for pipeline {pipeline_name}"
                        break
                    
                    # Success
                    opp_data = opp_resp.json()
                    opportunities = opp_data.get('opportunities', [])
                    
                    if not opportunities:
                        break  # No more opportunities
                    
                    all_opportunities.extend(opportunities)
                    
                    # Check if there's a next page using meta information
                    meta = opp_data.get('meta', {})
                    next_page_url = meta.get('nextPageUrl')
                    
                    if not next_page_url:
                        break  # No more pages
                    
                    # Extract pagination parameters from the last opportunity
                    if opportunities:
                        last_opp = opportunities[-1]
                        start_after_id = last_opp.get('id')
                        # Convert createdAt to timestamp if available
                        created_at = last_opp.get('createdAt')
                        if created_at:
                            from datetime import datetime
                            try:
                                dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                start_after = int(dt.timestamp() * 1000)  # Convert to milliseconds
                            except:
                                start_after = meta.get('startAfter')
                        else:
                            start_after = meta.get('startAfter')
                    
                    print(f"DEBUG: Pipeline {pipeline_name} - fetched {len(opportunities)} opportunities (total so far: {len(all_opportunities)})")
                    
                    # Small delay to avoid rate limits
                    await asyncio.sleep(0.1)
                
                print(f"DEBUG: Pipeline {pipeline_name} - fetched {len(all_opportunities)} opportunities")
                progress_store[task_id]["total_opportunities"] += len(all_opportunities)
                subaccount_opportunities[account_id] += len(all_opportunities)  # Update subaccount counter
                
                if not all_opportunities:
                    continue
                
                # Update progress for notes fetching
                progress_store[task_id]['current_stage'] = f'Fetching notes (batch processing)...'
                progress_store[task_id]['notes_progress'] = 0
                progress_store[task_id]['total_notes'] = len(all_opportunities)
                
                # Fetch all notes with controlled concurrency to avoid rate limits
                print(f"DEBUG: Fetching notes for {len(all_opportunities)} opportunities...")
                
                # Use semaphore to limit concurrent requests
                semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests
                
                async def fetch_note_with_semaphore(contact_id):
                    async with semaphore:
                        return await fetch_notes(client, contact_id, api_key)
                
                # Create tasks for all opportunities
                note_tasks = [fetch_note_with_semaphore(opp.get('contact', {}).get('id', '')) for opp in all_opportunities]
                
                # Process in batches to avoid overwhelming the API
                batch_size = 20
                notes_results = []
                
                for i in range(0, len(note_tasks), batch_size):
                    batch = note_tasks[i:i + batch_size]
                    batch_num = i//batch_size + 1
                    total_batches = (len(note_tasks) + batch_size - 1)//batch_size
                    print(f"DEBUG: Processing notes batch {batch_num}/{total_batches}")
                    
                    # Update progress for current batch
                    progress_store[task_id]['notes_progress'] = i
                    progress_store[task_id]['current_stage'] = f'Fetching notes (batch {batch_num}/{total_batches})...'
                    
                    try:
                        batch_results = await asyncio.gather(*batch, return_exceptions=True)
                        # Handle any exceptions in the batch
                        for j, result in enumerate(batch_results):
                            if isinstance(result, Exception):
                                print(f"DEBUG: Exception in batch {batch_num}, item {j}: {result}")
                                batch_results[j] = ['', '', '', '']
                        notes_results.extend(batch_results)
                    except Exception as e:
                        print(f"DEBUG: Error in batch {batch_num}: {e}")
                        # Add empty notes for failed batch
                        notes_results.extend([['', '', '', ''] for _ in batch])
                    
                    # Small delay between batches
                    await asyncio.sleep(0.5)
                
                # Final progress update
                progress_store[task_id]['notes_progress'] = len(all_opportunities)
                progress_store[task_id]['current_stage'] = 'Notes fetching complete'
                
                print(f"DEBUG: Finished fetching notes for pipeline {pipeline_name}")
                
                # Process all opportunities
                for i, opp in enumerate(all_opportunities):
                    contact = opp.get('contact', {})
                    stage_id = opp.get('pipelineStageId', '')
                    stage_name = stage_map.get(stage_id, '')
                    notes_list = notes_results[i]
                    
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
                    rows.append(row)
                    
                    # Update processed opportunities
                    progress_store[task_id]["processed_opportunities"] += 1
            
            # Small delay between subaccounts to avoid rate limits
            print(f"DEBUG: Finished processing subaccount {sub.get('name', str(account_id))} - Total opportunities: {subaccount_opportunities[account_id]}")
            await asyncio.sleep(1.0)
    
    # Generate CSV
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
    print(f"DEBUG: Export completed - Total rows: {len(rows)}, Total opportunities: {progress_store[task_id]['total_opportunities']}")
    progress_store[task_id].update({
        "status": "completed",
        "progress": 100,
        "message": f"Export completed! Processed {len(rows)} opportunities in {total_time:.1f} seconds.",
        "completed_subaccounts": total_subs,
        "csv_data": csv_buffer.getvalue(),
        "filename": filename,
        "total_time": total_time,
        "estimated_time_remaining": 0
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
