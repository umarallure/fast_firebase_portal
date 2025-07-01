import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional
from app.config import settings

# Logging configuration: print to console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ghl_update.log', mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class GHLOpportunityUpdater:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=settings.ghl_api_timeout)
        # Cache pipelines list for stage lookups
        self._pipelines_cache: Optional[List[Dict[str, Any]]] = None

    async def get_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all pipelines for the current API key and cache them."""
        pipelines = await self._fetch_pipelines()
        self._pipelines_cache = pipelines
        return pipelines

    async def _fetch_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch pipelines from the API."""
        try:
            response = await self.client.get(f"{self.base_url}/pipelines")
            response.raise_for_status()
            pipelines = response.json().get("pipelines", [])
            logger.info(f"[API KEY: {self.headers['Authorization'][:12]}...] Pipelines fetched: {[p['id'] for p in pipelines]}")
            for p in pipelines:
                logger.info(f"Pipeline: {p['id']} - {p.get('name')} | Stages: {[s['id'] for s in p.get('stages', [])]}")
            return pipelines
        except httpx.HTTPError as e:
            logger.error(f"Pipeline fetch failed: {str(e)}")
            return []

    async def get_pipeline_stages(self, pipeline_id: str) -> Dict[str, str]:
        """Get stages mapping for a pipeline from cached pipelines to avoid API 404."""
        # Use cached pipelines if available
        pipelines = self._pipelines_cache or await self.get_pipelines()
        for p in pipelines:
            if p.get("id") == pipeline_id:
                stages = p.get("stages", [])
                logger.info(f"Stages for pipeline {pipeline_id}: {[s['id'] for s in stages]}")
                return {stage["id"]: stage.get("name", "") for stage in stages}
        logger.error(f"Pipeline {pipeline_id} not found in cache; cannot fetch stages.")
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
        logger.info(f"Fetched {len(opportunities)} opportunities for pipeline {pipeline_id} ({pipeline_name})")
        for opp in opportunities:
            logger.info(f"Opportunity: {opp.get('id')} | Stage ID: {opp.get('pipelineStageId')} | Pipeline ID: {opp.get('pipelineId')}")
        return opportunities

    async def get_all_opportunities_for_account(self, account_id: str) -> List[Dict[str, Any]]:
        """
        Fetches all opportunities across all pipelines for a given account.
        Returns a list of opportunities with additional context (pipeline_id, pipeline_name, stage_name).
        """
        all_opportunities = []
        pipelines = await self.get_pipelines()
        pipeline_ids_from_list = [p["id"] for p in pipelines]
        logger.info(f"Pipeline IDs returned by /pipelines: {pipeline_ids_from_list}")

        pipeline_tasks = []
        for pipeline in pipelines:
            pipeline_id = pipeline["id"]
            pipeline_name = pipeline["name"]
            logger.info(f"Preparing to fetch opportunities for pipeline_id: {pipeline_id} ({pipeline_name})")
            pipeline_tasks.append(self.get_opportunities(pipeline_id, pipeline_name, account_id))
        
        results = await asyncio.gather(*pipeline_tasks, return_exceptions=True)

        for i, result in enumerate(results):
            pipeline_id = pipelines[i]["id"]
            pipeline_name = pipelines[i]["name"]
            logger.info(f"Fetching stages for pipeline_id: {pipeline_id} ({pipeline_name})")
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch opportunities for pipeline {pipelines[i]['name']}: {result}")
                continue
            # Try to fetch stages, but don't fail the rest if it errors
            try:
                stage_id_to_name_map = await self.get_pipeline_stages(pipeline_id)
            except Exception as e:
                logger.warning(f"Could not fetch stages for pipeline {pipeline_id}: {e}. Proceeding to process opportunities and update notes without stage names.")
                stage_id_to_name_map = {}
            for opp in result:
                contact = opp.get("contact", {})
                stage_id = opp.get("pipelineStageId")
                stage_name = stage_id_to_name_map.get(stage_id, "")
                if not stage_name and stage_id:
                    logger.warning(f"Stage ID {stage_id} not found in mapping for pipeline {pipeline_id}. Available: {list(stage_id_to_name_map.keys())}")
                # Get account_name from settings.subaccounts_list
                account_name = None
                for sub in settings.subaccounts_list:
                    if str(sub.get("id")) == str(account_id):
                        account_name = sub.get("name")
                        break
                all_opportunities.append({
                    "opportunity_id": opp.get("id"),
                    "pipeline_id": opp.get("pipelineId"),
                    "pipeline_name": pipeline_name,
                    "stage_id": stage_id,
                    "stage_name": stage_name,  # May be empty if stage fetch failed
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
                    "Created on": opp.get("dateCreated"),  # Add Created on date for matching
                    "account_id": account_id, # This is the subaccount ID from settings
                    "account_name": account_name
                })
        return all_opportunities

    async def update_opportunity(self, pipeline_id: str, opportunity_id: str, payload: Dict[str, Any]) -> bool:
        """Update an opportunity in GHL."""
        logger.info(f"Updating opportunity {opportunity_id} in pipeline {pipeline_id} with payload: {payload}")
        try:
            response = await self.client.put(
                f"{self.base_url}/pipelines/{pipeline_id}/opportunities/{opportunity_id}",
                json=payload
            )
            response.raise_for_status()
            logger.info(f"Successfully updated opportunity {opportunity_id} in pipeline {pipeline_id}. Response: {response.text}")
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to update opportunity {opportunity_id} in pipeline {pipeline_id}: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"An error occurred while updating opportunity {opportunity_id}: {str(e)}")
            return False

    async def update_opportunity_status(self, pipeline_id: str, opportunity_id: str, status: str, stage_id: str) -> bool:
        """Update an opportunity's status and pipeline stage in GHL using the dedicated status endpoint."""
        url = f"{self.base_url}/pipelines/{pipeline_id}/opportunities/{opportunity_id}/status"
        payload = {"status": status, "stageId": stage_id}
        logger.info(f"Updating opportunity status for {opportunity_id} in pipeline {pipeline_id} with payload: {payload}")
        try:
            response = await self.client.put(url, json=payload)
            response.raise_for_status()
            logger.info(f"Successfully updated opportunity status for {opportunity_id}. Response: {response.text}")
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to update opportunity status for {opportunity_id}: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"Error updating opportunity status for {opportunity_id}: {str(e)}")
            return False

    async def update_contact_notes(self, contact_id: str, notes: str) -> Dict[str, Any]:
        """
        Updates notes for a contact using the contact ID.
        Returns a dictionary with status and details for logging.
        """
        if not contact_id or not notes or not notes.strip():
            logger.warning(f"Skipping notes update - Missing contact_id ({contact_id}) or empty notes")
            return {"status": "skipped", "reason": "Missing contact_id or empty notes"}
        
        url = f"{self.base_url}/contacts/{contact_id}/notes/"
        payload = {"body": notes.strip()}
        
        logger.info(f"Updating notes for contact {contact_id}")
        logger.info(f"API URL: {url}")
        logger.info(f"Payload: {payload}")
        logger.info(f"Headers: {dict(self.headers)}")
        
        try:
            response = await self.client.post(url, json=payload)
            logger.info(f"Notes API Response Status: {response.status_code}")
            logger.info(f"Notes API Response Headers: {dict(response.headers)}")
            logger.info(f"Notes API Response Body: {response.text}")
            
            response.raise_for_status()
            logger.info(f"Successfully updated notes for contact {contact_id}")
            return {
                "status": "success", 
                "contact_id": contact_id,
                "notes": notes,
                "response_status": response.status_code,
                "response_body": response.text
            }
        except httpx.HTTPStatusError as e:
            error_msg = f"Failed to update notes for contact {contact_id}: Status {e.response.status_code}, Response: {e.response.text}"
            logger.error(error_msg)
            return {
                "status": "error",
                "contact_id": contact_id,
                "error": error_msg,
                "response_status": e.response.status_code,
                "response_body": e.response.text
            }
        except Exception as e:
            error_msg = f"Exception occurred while updating notes for contact {contact_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "contact_id": contact_id,
                "error": error_msg
            }

    async def get_stage_id_from_name(self, pipeline_id: str, stage_name: str) -> Optional[str]:
        """Helper to get stage ID from stage name for a given pipeline."""
        stages = await self.get_pipeline_stages(pipeline_id)
        for stage_id, name in stages.items():
            if name.lower() == stage_name.lower():
                return stage_id
        logger.warning(f"Stage name '{stage_name}' not found in pipeline {pipeline_id}. Available: {list(stages.values())}")
        return None