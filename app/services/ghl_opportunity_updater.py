import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional
from app.config import settings

logger = logging.getLogger(__name__)

class GHLOpportunityUpdater:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=settings.ghl_api_timeout)

    async def get_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all pipelines for the current API key."""
        try:
            response = await self.client.get(f"{self.base_url}/pipelines")
            response.raise_for_status()
            return response.json().get("pipelines", [])
        except httpx.HTTPError as e:
            logger.error(f"Pipeline fetch failed: {str(e)}")
            return []

    async def get_pipeline_stages(self, pipeline_id: str) -> Dict[str, str]:
        """Fetch all stages for a pipeline and return a mapping of stage ID to stage name."""
        try:
            response = await self.client.get(f"{self.base_url}/pipelines/{pipeline_id}")
            response.raise_for_status()
            data = response.json()
            stages = data.get("stages", [])
            return {stage["id"]: stage["name"] for stage in stages}
        except httpx.HTTPError as e:
            logger.error(f"Stage fetch failed for pipeline {pipeline_id}: {str(e)}")
            return {}

    async def get_opportunities(self, pipeline_id: str, pipeline_name: str, account_id: str) -> List[Dict[str, Any]]:
        """Fetch all opportunities for a pipeline with pagination."""
        opportunities = []
        params = {"limit": 100}
        
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
                    await asyncio.sleep(0.3)  # Rate limit
                else:
                    break

            except httpx.HTTPError as e:
                logger.error(f"Opportunity fetch failed for {pipeline_id}: {str(e)}")
                break
        return opportunities

    async def get_all_opportunities_for_account(self, account_id: str) -> List[Dict[str, Any]]:
        """
        Fetches all opportunities across all pipelines for a given account.
        Returns a list of opportunities with additional context (pipeline_id, pipeline_name, stage_name).
        """
        all_opportunities = []
        pipelines = await self.get_pipelines()
        
        pipeline_tasks = []
        for pipeline in pipelines:
            pipeline_id = pipeline["id"]
            pipeline_name = pipeline["name"]
            pipeline_tasks.append(self.get_opportunities(pipeline_id, pipeline_name, account_id))
        
        results = await asyncio.gather(*pipeline_tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch opportunities for pipeline {pipelines[i]['name']}: {result}")
                continue
            
            pipeline_id = pipelines[i]["id"]
            pipeline_name = pipelines[i]["name"]
            
            # Fetch stages for the current pipeline to get stage name to ID mapping
            stage_id_to_name_map = await self.get_pipeline_stages(pipeline_id)

            for opp in result:
                contact = opp.get("contact", {})
                stage_id = opp.get("pipelineStageId")
                stage_name = stage_id_to_name_map.get(stage_id, "")

                all_opportunities.append({
                    "opportunity_id": opp.get("id"),
                    "pipeline_id": opp.get("pipelineId"),
                    "pipeline_name": pipeline_name,
                    "stage_id": stage_id,
                    "stage_name": stage_name,
                    "contact_id": contact.get("id"),
                    "contact_name": contact.get("name"),
                    "contact_phone": contact.get("phone"),
                    "opportunity_name": opp.get("name"),
                    "status": opp.get("status"),
                    "monetary_value": opp.get("monetaryValue"),
                    "assigned_to": opp.get("assignedTo"),
                    "email": contact.get("email"),
                    "tags": contact.get("tags", []),
                    "company_name": contact.get("companyName"),
                    "account_id": account_id # This is the subaccount ID from settings
                })
        return all_opportunities

    async def update_opportunity(self, pipeline_id: str, opportunity_id: str, payload: Dict[str, Any]) -> bool:
        """Update an opportunity in GHL."""
        try:
            response = await self.client.put(
                f"{self.base_url}/pipelines/{pipeline_id}/opportunities/{opportunity_id}",
                json=payload
            )
            response.raise_for_status()
            logger.info(f"Successfully updated opportunity {opportunity_id} in pipeline {pipeline_id}.")
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to update opportunity {opportunity_id} in pipeline {pipeline_id}: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"An error occurred while updating opportunity {opportunity_id}: {str(e)}")
            return False

    async def get_stage_id_from_name(self, pipeline_id: str, stage_name: str) -> Optional[str]:
        """Helper to get stage ID from stage name for a given pipeline."""
        stages = await self.get_pipeline_stages(pipeline_id)
        for stage_id, name in stages.items():
            if name.lower() == stage_name.lower():
                return stage_id
        return None