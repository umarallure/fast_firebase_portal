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

    async def get_pipeline_stages(self, pipeline_id: str) -> Dict[str, str]:
        """Fetch all stages for a pipeline and return a mapping of stage ID to stage name"""
        try:
            response = await self.client.get(f"{self.base_url}/pipelines/{pipeline_id}")
            response.raise_for_status()
            data = response.json()
            stages = data.get("stages", [])
            return {stage["id"]: stage["name"] for stage in stages}
        except httpx.HTTPError as e:
            logger.error(f"Stage fetch failed for pipeline {pipeline_id}: {str(e)}")
            return {}

    async def get_opportunities(self, pipeline_id: str, pipeline_name: str, stage_map: Dict[str, str], account_id: str, max_records: int = None) -> List[Dict[str, Any]]:
        """Fetch all opportunities for a pipeline with pagination and include stage name and account id"""
        all_opportunities = []
        start_after_id = None
        start_after = None
        limit = 100
        seen_ids = set()  # Track seen opportunity IDs to detect duplicates

        while True:
            try:
                # Build URL with cursor pagination parameters
                url = f"{self.base_url}/pipelines/{pipeline_id}/opportunities?limit={limit}"
                if start_after_id and start_after:
                    url += f'&startAfterId={start_after_id}&startAfter={start_after}'
                
                logger.info(f"DEBUG: Pipeline {pipeline_name} - making request: {url}")
                logger.info(f"DEBUG: Pipeline {pipeline_name} - current pagination: start_after_id={start_after_id}, start_after={start_after}")
                
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
                
                logger.info(f"DEBUG: Pipeline {pipeline_name} - response meta: {meta}")
                logger.info(f"DEBUG: Pipeline {pipeline_name} - received {len(opportunities)} opportunities")
                
                if opportunities:
                    first_opp = opportunities[0]
                    last_opp = opportunities[-1]
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - first opp id: {first_opp.get('id')}, createdAt: {first_opp.get('createdAt')}")
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - last opp id: {last_opp.get('id')}, createdAt: {last_opp.get('createdAt')}")
                
                if not opportunities:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - no more opportunities, breaking")
                    break  # No more opportunities
                
                all_opportunities.extend(opportunities)
                
                # Check for duplicate opportunities (indicates pagination issue)
                duplicate_found = False
                for opp in opportunities:
                    opp_id = opp.get('id')
                    if opp_id in seen_ids:
                        logger.warning(f"DEBUG: Pipeline {pipeline_name} - DUPLICATE DETECTED: opportunity {opp_id} already seen")
                        duplicate_found = True
                        break
                    seen_ids.add(opp_id)
                
                if duplicate_found:
                    logger.warning(f"DEBUG: Pipeline {pipeline_name} - breaking due to duplicate opportunity")
                    break  # Break the while loop if duplicate was found
                
                # Check if we've reached the max_records limit
                if max_records and len(all_opportunities) >= max_records:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - reached max limit of {max_records}, breaking")
                    all_opportunities = all_opportunities[:max_records]  # Trim to exact limit
                    break
                
                # Check if there's a next page using meta information
                meta = data.get("meta", {})
                next_page_url = meta.get("nextPageUrl")
                
                logger.info(f"DEBUG: Pipeline {pipeline_name} - nextPageUrl present: {bool(next_page_url)}")
                if next_page_url:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - nextPageUrl: {next_page_url}")
                
                if not next_page_url:
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - no more pages, breaking")
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
                    
                    logger.info(f"DEBUG: Pipeline {pipeline_name} - setting pagination for next request: start_after_id={new_start_after_id}, start_after={new_start_after}")
                    start_after_id = new_start_after_id
                    start_after = new_start_after
                
                logger.info(f"DEBUG: Pipeline {pipeline_name} - fetched {len(opportunities)} opportunities (total so far: {len(all_opportunities)})")
                
                # Small delay to avoid rate limits
                await asyncio.sleep(0.1)

            except httpx.HTTPError as e:
                logger.error(f"Opportunity fetch failed for {pipeline_id}: {str(e)}")
                break

        logger.info(f"DEBUG: Pipeline {pipeline_name} - final count: {len(all_opportunities)} opportunities (max_records: {max_records})")
        logger.info(f"DEBUG: Pipeline {pipeline_name} - total unique opportunities seen: {len(seen_ids)}")
        return [self.format_opportunity(opp, pipeline_name, stage_map, account_id) for opp in all_opportunities]

    @staticmethod
    def format_opportunity(opp: Dict[str, Any], pipeline_name: str, stage_map: Dict[str, str], account_id: str) -> Dict[str, Any]:
        """Format opportunity data into standardized schema, using stage name instead of stage ID, and include account id"""
        contact = opp.get("contact", {})
        def parse_date(date_str): return datetime.fromisoformat(date_str[:-1]) if date_str else None
        def days_since(dt): return (datetime.now() - dt).days if dt else None

        created = parse_date(opp.get("createdAt"))
        updated = parse_date(opp.get("updatedAt"))
        stage_changed = parse_date(opp.get("lastStatusChangeAt"))
        stage_id = opp.get("pipelineStageId")
        stage_name = stage_map.get(stage_id, "")

        # Match the reference format exactly, use stage name for 'stage'
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
            "lost reason ID": "",  # not available in base response
            "lost reason name": "",  # not available in base response
            "Followers": "",  # optional/custom field
            "Notes": "",  # optional/custom field
            "tags": ", ".join(contact.get("tags", [])) if contact.get("tags") else "",
            "Engagement Score": "",  # not in API
            "status": opp.get("status"),
            "Opportunity ID": opp.get("id"),
            "Contact ID": contact.get("id"),
            "Pipeline Stage ID": stage_id,
            "Pipeline ID": opp.get("pipelineId"),
            "Days Since Last Stage Change": days_since(stage_changed),
            "Days Since Last Status Change": days_since(stage_changed),
            "Days Since Last Updated": days_since(updated),
            "Account Id": account_id
        }

async def process_export_request(export_request: ExportRequest) -> bytes:
    """Process export request with multiple accounts and pipelines and return Excel bytes, including stage name and account id"""
    tasks = []
    stage_maps = {}
    clients = {}
    pipeline_stage_map = {}  # (api_key, pipeline_id) -> {stage_id: stage_name}
    
    # Set max_records to 200 for testing (change to None for full export)
    max_records = 200
    
    for selection in export_request.selections:
        client = GHLClient(selection.api_key)
        clients[selection.api_key] = client
        pipelines = await client.get_pipelines()
        selected_pipelines = [
            pipe for pipe in pipelines 
            if pipe["id"] in selection.pipelines
        ]
        
        logger.info(f"DEBUG: Processing account {selection.account_id} with {len(selected_pipelines)} pipelines")
        for pipe in selected_pipelines:
            logger.info(f"DEBUG: Pipeline {pipe['name']} ({pipe['id']}) selected for account {selection.account_id}")
        
        # Build stage map from pipelines response
        for pipeline in selected_pipelines:
            stage_map = {stage["id"]: stage["name"] for stage in pipeline.get("stages", [])}
            pipeline_stage_map[(selection.api_key, pipeline["id"])] = stage_map
            tasks.append(
                client.get_opportunities(pipeline["id"], pipeline["name"], stage_map, selection.account_id, max_records)
            )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_opps = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Pipeline fetch failed for task {i}: {str(result)}")
            continue
        logger.info(f"DEBUG: Task {i} returned {len(result)} opportunities")
        all_opps.extend(result)
    
    logger.info(f"DEBUG: Total opportunities collected: {len(all_opps)}")

    # Ensure columns are in the exact order as the reference, with 'stage' as stage name and 'Account Id'
    columns = [
        "Opportunity Name", "Contact Name", "phone", "email", "pipeline", "stage",
        "Lead Value", "source", "assigned", "Created on", "Updated on",
        "lost reason ID", "lost reason name", "Followers", "Notes", "tags",
        "Engagement Score", "status", "Opportunity ID", "Contact ID",
        "Pipeline Stage ID", "Pipeline ID", "Days Since Last Stage Change",
        "Days Since Last Status Change", "Days Since Last Updated", "Account Id"
    ]
    df = pd.DataFrame(all_opps, columns=columns)
    from io import BytesIO
    output = BytesIO()
    df.to_excel(output, index=False)
    return output.getvalue()