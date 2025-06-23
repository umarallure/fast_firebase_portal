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

    async def get_opportunities(self, pipeline_id: str, pipeline_name: str, stage_map: Dict[str, str]) -> List[Dict[str, Any]]:
        """Fetch all opportunities for a pipeline with pagination and include stage name"""
        opportunities = []
        params = {"limit": 100}
        page = 1

        while True:
            try:
                response = await self.client.get(
                    f"{self.base_url}/pipelines/{pipeline_id}/opportunities",
                    params=params
                )
                response.raise_for_status()
                data = response.json()

                batch = data.get("opportunities", [])
                if not batch:
                    break

                opportunities.extend(batch)
                meta = data.get("meta", {})
                next_id = meta.get("startAfterId")
                next_time = meta.get("startAfter")

                if next_id and next_time and next_id != params.get("startAfterId"):
                    params["startAfterId"] = next_id
                    params["startAfter"] = next_time
                    page += 1
                    await asyncio.sleep(0.3)  # Rate limit
                else:
                    break

            except httpx.HTTPError as e:
                logger.error(f"Opportunity fetch failed for {pipeline_id}: {str(e)}")
                break

        return [self.format_opportunity(opp, pipeline_name, stage_map) for opp in opportunities]

    @staticmethod
    def format_opportunity(opp: Dict[str, Any], pipeline_name: str, stage_map: Dict[str, str]) -> Dict[str, Any]:
        """Format opportunity data into standardized schema, using stage name instead of stage ID"""
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
            "Days Since Last Updated": days_since(updated)
        }

async def process_export_request(export_request: ExportRequest) -> bytes:
    """Process export request with multiple accounts and pipelines and return Excel bytes, including stage name"""
    tasks = []
    stage_maps = {}
    clients = {}
    pipeline_stage_map = {}  # (api_key, pipeline_id) -> {stage_id: stage_name}
    for selection in export_request.selections:
        client = GHLClient(selection.api_key)
        clients[selection.api_key] = client
        pipelines = await client.get_pipelines()
        selected_pipelines = [
            pipe for pipe in pipelines 
            if pipe["id"] in selection.pipelines
        ]
        # Build stage map from pipelines response
        for pipeline in selected_pipelines:
            stage_map = {stage["id"]: stage["name"] for stage in pipeline.get("stages", [])}
            pipeline_stage_map[(selection.api_key, pipeline["id"])] = stage_map
            tasks.append(
                client.get_opportunities(pipeline["id"], pipeline["name"], stage_map)
            )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_opps = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Pipeline fetch failed: {str(result)}")
            continue
        all_opps.extend(result)

    # Ensure columns are in the exact order as the reference, with 'stage' as stage name
    columns = [
        "Opportunity Name", "Contact Name", "phone", "email", "pipeline", "stage",
        "Lead Value", "source", "assigned", "Created on", "Updated on",
        "lost reason ID", "lost reason name", "Followers", "Notes", "tags",
        "Engagement Score", "status", "Opportunity ID", "Contact ID",
        "Pipeline Stage ID", "Pipeline ID", "Days Since Last Stage Change",
        "Days Since Last Status Change", "Days Since Last Updated"
    ]
    df = pd.DataFrame(all_opps, columns=columns)
    from io import BytesIO
    output = BytesIO()
    df.to_excel(output, index=False)
    return output.getvalue()