import httpx
import asyncio
from datetime import datetime
from typing import List, Dict, Any
import logging
import pandas as pd
from app.config import settings
from app.models.schemas import ExportRequest, SelectionSchema

logger = logging.getLogger(__name__)

class GHLClient:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=settings.ghl_api_timeout)

    async def get_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all pipelines for the current API key"""
        try:
            response = await self.client.get(f"{self.base_url}/pipelines")
            response.raise_for_status()
            return response.json().get("pipelines", [])
        except httpx.HTTPError as e:
            logger.error(f"Pipeline fetch failed: {str(e)}")
            return []

    async def get_opportunities(self, pipeline_id: str, pipeline_name: str, stage_map: Dict[str, str], account_id: str, max_records: int = None) -> List[Dict[str, Any]]:
        """Fetch all opportunities for a pipeline with proper pagination"""
        all_opportunities = []
        start_after_id = None
        start_after = None
        limit = 100

        while True:
            try:
                # Build URL with cursor pagination parameters
                url = f"{self.base_url}/pipelines/{pipeline_id}/opportunities?limit={limit}"
                if start_after_id and start_after:
                    url += f'&startAfterId={start_after_id}&startAfter={start_after}'

                logger.info(f"DEBUG: Pipeline {pipeline_name} - making request: {url}")

                response = await self.client.get(url)

                if response.status_code == 429:
                    wait_time = 2
                    logger.warning(f"DEBUG: Rate limited for pipeline {pipeline_name}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status_code != 200:
                    logger.error(f"DEBUG: Error {response.status_code} for pipeline {pipeline_name}: {response.text}")
                    break

                # Success
                data = response.json()
                opportunities = data.get("opportunities", [])
                meta = data.get("meta", {})

                logger.info(f"DEBUG: Pipeline {pipeline_name} - received {len(opportunities)} opportunities")
                logger.info(f"DEBUG: Pipeline {pipeline_name} - meta: {meta}")

                if not opportunities:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - no more opportunities, breaking")
                    break  # No more opportunities

                all_opportunities.extend(opportunities)

                logger.info(f"DEBUG: Pipeline {pipeline_name} - after extend: total_opportunities={len(all_opportunities)}, max_records={max_records}, current_batch_size={len(opportunities)}, limit={limit}")

                # Check if we've reached the max_records limit
                if max_records and len(all_opportunities) >= max_records:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - MAX_RECORDS CHECK: {len(all_opportunities)} >= {max_records} is {len(all_opportunities) >= max_records} - BREAKING")
                    all_opportunities = all_opportunities[:max_records]  # Trim to exact limit
                    break

                # CRITICAL: Check if we got fewer opportunities than requested (means end of data)
                # This MUST happen before any pagination logic
                if len(opportunities) < limit:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - FEWER_CHECK: {len(opportunities)} < {limit} is {len(opportunities) < limit} - BREAKING")
                    break

                # Only proceed with pagination if we got a full page
                # Check if there's a next page using meta information
                next_page_url = meta.get("nextPageUrl")
                logger.info(f"DEBUG: Pipeline {pipeline_name} - nextPageUrl present: {bool(next_page_url)}")

                if not next_page_url:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - no nextPageUrl, breaking")
                    break  # No more pages

                # Extract pagination parameters from the last opportunity
                if opportunities:
                    last_opp = opportunities[-1]
                    new_start_after_id = last_opp.get('id')
                    created_at = last_opp.get('createdAt')

                    logger.info(f"DEBUG: Pipeline {pipeline_name} - last opp data: id={new_start_after_id}, createdAt={created_at}")

                    if created_at:
                        try:
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            new_start_after = int(dt.timestamp() * 1000)  # Convert to milliseconds
                            logger.info(f"DEBUG: Pipeline {pipeline_name} - calculated start_after: {new_start_after} (from createdAt)")
                        except Exception as e:
                            logger.warning(f"DEBUG: Pipeline {pipeline_name} - failed to parse createdAt '{created_at}': {e}")
                            new_start_after = meta.get('startAfter')
                            logger.info(f"DEBUG: Pipeline {pipeline_name} - using meta startAfter: {new_start_after}")
                    else:
                        new_start_after = meta.get('startAfter')
                        logger.info(f"DEBUG: Pipeline {pipeline_name} - no createdAt, using meta startAfter: {new_start_after}")

                    # Check if cursor parameters changed - THIS IS THE KEY CHECK
                    if new_start_after_id == start_after_id and new_start_after == start_after:
                        logger.warning(f"DEBUG: Pipeline {pipeline_name} - CURSOR DID NOT CHANGE! start_after_id: {new_start_after_id} == {start_after_id}, start_after: {new_start_after} == {start_after} - BREAKING INFINITE LOOP")
                        break

                    logger.info(f"DEBUG: Pipeline {pipeline_name} - cursor changed, continuing: new_id={new_start_after_id}, new_after={new_start_after}")
                    start_after_id = new_start_after_id
                    start_after = new_start_after

                logger.info(f"DEBUG: Pipeline {pipeline_name} - fetched {len(opportunities)} opportunities (total so far: {len(all_opportunities)})")

                # Small delay to avoid rate limits
                await asyncio.sleep(0.1)

            except httpx.HTTPError as e:
                logger.error(f"Opportunity fetch failed for {pipeline_id}: {str(e)}")
                break

        logger.info(f"DEBUG: Pipeline {pipeline_name} - final count: {len(all_opportunities)} opportunities")
        return all_opportunities

    async def fetch_notes_batch(self, contact_ids: List[str], batch_size: int = 20) -> List[List[str]]:
        """Fetch notes for multiple contacts in batches with rate limiting"""
        import asyncio
        all_notes = []
        
        # Use semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests
        
        async def fetch_note_with_semaphore(contact_id):
            async with semaphore:
                return await self._fetch_notes_for_contact(contact_id)
        
        # Process in batches to avoid overwhelming the API
        for i in range(0, len(contact_ids), batch_size):
            batch = contact_ids[i:i + batch_size]
            logger.info(f"Processing notes batch {i//batch_size + 1}/{(len(contact_ids) + batch_size - 1)//batch_size}")
            
            # Create tasks for this batch
            tasks = [fetch_note_with_semaphore(contact_id) for contact_id in batch]
            
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                # Handle any exceptions in the batch
                for j, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        logger.warning(f"Exception fetching notes for contact {batch[j]}: {result}")
                        batch_results[j] = ['', '', '', '']
                all_notes.extend(batch_results)
            except Exception as e:
                logger.error(f"Error in notes batch: {e}")
                # Add empty notes for failed batch
                all_notes.extend([['', '', '', ''] for _ in batch])
            
            # Small delay between batches
            await asyncio.sleep(0.5)
        
        return all_notes

    async def _fetch_notes_for_contact(self, contact_id: str) -> List[str]:
        """Fetch latest 4 notes for a contact with retry logic"""
        if not contact_id:
            return ['', '', '', '']
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use the v1 API endpoint that works
                url = f"{self.base_url}/contacts/{contact_id}/notes/"
                
                response = await self.client.get(url)
                
                if response.status_code == 429:
                    # Rate limited - wait with exponential backoff
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited fetching notes for {contact_id}, attempt {attempt + 1}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status_code == 200:
                    notes_data = response.json().get('notes', [])
                    # Sort by creation date descending to get the latest notes first
                    notes_sorted = sorted(notes_data, key=lambda n: n.get('dateAdded', n.get('createdAt', '')), reverse=True)
                    notes_list = [n.get('body', '') for n in notes_sorted[:4]]
                    while len(notes_list) < 4:
                        notes_list.append('')
                    
                    if notes_list and any(notes_list):
                        logger.info(f"Fetched {len([n for n in notes_list if n])} notes for contact {contact_id}")
                    
                    return notes_list
                else:
                    logger.warning(f"Error {response.status_code} fetching notes for {contact_id}: {response.text[:200]}")
                    return ['', '', '', '']
            except Exception as e:
                logger.error(f"Exception fetching notes for {contact_id}: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)
                    continue
                return ['', '', '', '']
        
        return ['', '', '', '']

    @staticmethod
    def format_opportunity(opp: Dict[str, Any], pipeline_name: str, stage_map: Dict[str, str], account_id: str, notes: List[str] = None) -> Dict[str, Any]:
        """Format opportunity data into standardized schema"""
        contact = opp.get("contact", {})
        def parse_date(date_str): return datetime.fromisoformat(date_str[:-1]) if date_str else None
        def days_since(dt): return (datetime.now() - dt).days if dt else None

        created = parse_date(opp.get("createdAt"))
        updated = parse_date(opp.get("updatedAt"))
        stage_changed = parse_date(opp.get("lastStatusChangeAt"))
        stage_id = opp.get("pipelineStageId")
        stage_name = stage_map.get(stage_id, "")
        
        # Handle notes
        if notes is None:
            notes = ['', '', '', '']

        return {
            "Opportunity Name": opp.get("name"),
            "Contact Name": contact.get("name"),
            "phone": contact.get("phone"),
            "email": contact.get("email"),
            "pipeline": pipeline_name,
            "stage": stage_name,
            "Lead Value": opp.get("monetaryValue"),
            "source": opp.get("source"),
            "assigned": opp.get("assignedTo"),
            "Created on": created,
            "Updated on": updated,
            "lost reason ID": "",
            "lost reason name": "",
            "Followers": "",
            "Notes": "",
            "tags": ", ".join(contact.get("tags", [])) if contact.get("tags") else "",
            "Engagement Score": "",
            "status": opp.get("status"),
            "Opportunity ID": opp.get("id"),
            "Contact ID": contact.get("id"),
            "Pipeline Stage ID": stage_id,
            "Pipeline ID": opp.get("pipelineId"),
            "Days Since Last Stage Change": days_since(stage_changed),
            "Days Since Last Status Change": days_since(stage_changed),
            "Days Since Last Updated": days_since(updated),
            "Account Id": account_id,
            "note1": notes[0] if len(notes) > 0 else "",
            "note2": notes[1] if len(notes) > 1 else "",
            "note3": notes[2] if len(notes) > 2 else "",
            "note4": notes[3] if len(notes) > 3 else ""
        }

async def process_export_request(export_request: ExportRequest) -> bytes:
    """Process export request with multiple accounts and pipelines - OPPORTUNITIES WITH NOTES"""
    try:
        logger.info(f"DEBUG: Starting export processing for {len(export_request.selections)} selections")

        all_opportunities = []

        # Get subaccounts for API keys
        subaccounts = settings.subaccounts_list
        account_api_keys = {str(s['id']): s['api_key'] for s in subaccounts if s.get('api_key')}

        # Process each selection
        for selection in export_request.selections:
            account_id = str(selection.account_id)
            api_key = account_api_keys.get(account_id)

            if not api_key:
                logger.warning(f"No API key found for account {account_id}")
                continue

            logger.info(f"Processing account {account_id} with {len(selection.pipelines)} pipelines")

            # Create GHL client for this account
            client = GHLClient(api_key)

            # Get pipelines for this account
            pipelines = await client.get_pipelines()
            pipeline_map = {p['id']: p['name'] for p in pipelines}

            # Process each selected pipeline
            for pipeline_id in selection.pipelines:
                pipeline_name = pipeline_map.get(pipeline_id, f"Pipeline {pipeline_id}")

                logger.info(f"Fetching opportunities for pipeline {pipeline_name} ({pipeline_id})")

                # Get stage mapping for this pipeline from the fetched pipelines data
                stage_map = {}
                for p in pipelines:
                    if p['id'] == pipeline_id:
                        stages = p.get('stages', [])
                        stage_map = {s['id']: s['name'] for s in stages}
                        break

                logger.info(f"Found {len(stage_map)} stages for pipeline {pipeline_name}")

                # Fetch opportunities (raw data)
                opportunities = await client.get_opportunities(
                    pipeline_id=pipeline_id,
                    pipeline_name=pipeline_name,
                    stage_map=stage_map,
                    account_id=account_id
                )

                logger.info(f"Fetched {len(opportunities)} opportunities from pipeline {pipeline_name}")
                
                # Add pipeline info to each opportunity for later processing
                for opp in opportunities:
                    opp['_pipeline_name'] = pipeline_name
                    opp['_stage_map'] = stage_map
                    opp['_account_id'] = account_id
                
                all_opportunities.extend(opportunities)

        logger.info(f"Total opportunities collected: {len(all_opportunities)}")

        # Fetch notes for all opportunities
        if all_opportunities:
            logger.info("Fetching notes for all opportunities...")
            
            # Get all unique contact IDs
            contact_ids = list(set(opp.get('contact', {}).get('id') for opp in all_opportunities if opp.get('contact', {}).get('id')))
            logger.info(f"Found {len(contact_ids)} unique contacts to fetch notes for")
            
            # Group opportunities by API key for notes fetching
            opportunities_by_api_key = {}
            for opp in all_opportunities:
                account_id = opp.get('_account_id')
                if account_id not in opportunities_by_api_key:
                    opportunities_by_api_key[account_id] = []
                opportunities_by_api_key[account_id].append(opp)
            
            # Fetch notes for each account
            all_formatted_opportunities = []
            for account_id, opps in opportunities_by_api_key.items():
                api_key = account_api_keys.get(account_id)
                if not api_key:
                    continue
                    
                client = GHLClient(api_key)
                
                # Get contact IDs for this account
                account_contact_ids = [opp.get('contact', {}).get('id') for opp in opps if opp.get('contact', {}).get('id')]
                
                # Fetch notes for these contacts
                logger.info(f"Fetching notes for {len(account_contact_ids)} contacts in account {account_id}")
                notes_batch = await client.fetch_notes_batch(account_contact_ids)
                
                # Create mapping of contact_id to notes
                contact_notes_map = dict(zip(account_contact_ids, notes_batch))
                
                # Format opportunities with notes
                for opp in opps:
                    contact_id = opp.get('contact', {}).get('id')
                    notes = contact_notes_map.get(contact_id, ['', '', '', ''])
                    
                    formatted_opp = client.format_opportunity(
                        opp, 
                        opp.get('_pipeline_name'), 
                        opp.get('_stage_map'), 
                        opp.get('_account_id'),
                        notes
                    )
                    all_formatted_opportunities.append(formatted_opp)
            
            logger.info(f"Formatted {len(all_formatted_opportunities)} opportunities with notes")
        else:
            all_formatted_opportunities = []

        # Create Excel file from the data
        if all_formatted_opportunities:
            df = pd.DataFrame(all_formatted_opportunities)

            # Convert datetime columns to string for Excel
            datetime_columns = ['Created on', 'Updated on']
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else '')

            # Reorder columns to match the desired structure
            desired_columns = [
                "Opportunity Name", "Contact Name", "phone", "email", "pipeline", "stage",
                "Lead Value", "source", "assigned", "Created on", "Updated on", "lost reason ID",
                "lost reason name", "Followers", "Notes", "tags", "Engagement Score", "status",
                "Opportunity ID", "Contact ID", "Pipeline Stage ID", "Pipeline ID",
                "Days Since Last Stage Change", "Days Since Last Status Change",
                "Days Since Last Updated", "Account Id", "note1", "note2", "note3", "note4"
            ]
            
            # Add any missing columns
            for col in desired_columns:
                if col not in df.columns:
                    df[col] = ""
            
            # Reorder to desired column order
            df = df[desired_columns]

            # Create Excel file in memory
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Opportunities', index=False)

            excel_content = output.getvalue()
            logger.info(f"Created Excel file with {len(all_formatted_opportunities)} rows and notes")
        else:
            # Create empty Excel file if no data
            desired_columns = [
                "Opportunity Name", "Contact Name", "phone", "email", "pipeline", "stage",
                "Lead Value", "source", "assigned", "Created on", "Updated on", "lost reason ID",
                "lost reason name", "Followers", "Notes", "tags", "Engagement Score", "status",
                "Opportunity ID", "Contact ID", "Pipeline Stage ID", "Pipeline ID",
                "Days Since Last Stage Change", "Days Since Last Status Change",
                "Days Since Last Updated", "Account Id", "note1", "note2", "note3", "note4"
            ]
            df = pd.DataFrame(columns=desired_columns)
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Opportunities', index=False)
            excel_content = output.getvalue()
            logger.info("Created empty Excel file (no opportunities found)")

        return excel_content

    except Exception as e:
        logger.error(f"Error in process_export_request: {str(e)}")
        raise