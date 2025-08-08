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
        logging.FileHandler('ghl_update_v2.log', mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class GHLOpportunityUpdaterV2:
    def __init__(self, access_token: str, location_id: str = None):
        self.base_url = "https://services.leadconnectorhq.com"
        self.location_id = location_id
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": "2021-07-28"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=30)
        self._pipelines_cache: Optional[List[Dict[str, Any]]] = None

    async def get_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all pipelines for the current access token and cache them."""
        if self._pipelines_cache is not None:
            return self._pipelines_cache
            
        try:
            # For GHL V2 API, pipelines require locationId as query parameter
            url = f"{self.base_url}/opportunities/pipelines"
            
            # Add locationId as query parameter if available
            params = {}
            if self.location_id:
                params["locationId"] = self.location_id
                logger.info(f"Using location ID: {self.location_id}")
            else:
                logger.warning("No location ID provided - this may cause the request to fail")
            
            logger.info(f"Fetching pipelines from: {url}")
            logger.info(f"Query params: {params}")
            
            response = await self.client.get(url, params=params)
            
            logger.info(f"Pipeline API Response Status: {response.status_code}")
            logger.info(f"Pipeline API Response: {response.text}")
            
            response.raise_for_status()
            data = response.json()
            
            # Handle different response structures
            if "pipelines" in data:
                pipelines = data["pipelines"]
            elif isinstance(data, list):
                pipelines = data
            else:
                logger.warning(f"Unexpected response structure: {data}")
                pipelines = []
            
            self._pipelines_cache = pipelines
            
            logger.info(f"Fetched {len(pipelines)} pipelines")
            for p in pipelines:
                logger.info(f"Pipeline: {p.get('id')} - {p.get('name')} | Stages: {len(p.get('stages', []))}")
            
            return pipelines
        except httpx.HTTPError as e:
            logger.error(f"Pipeline fetch failed: {str(e)}")
            logger.error(f"Response status: {e.response.status_code if hasattr(e, 'response') else 'N/A'}")
            logger.error(f"Response body: {e.response.text if hasattr(e, 'response') else 'N/A'}")
            return []

    async def test_connection(self) -> Dict[str, Any]:
        """Test the API connection and token validity"""
        try:
            # Test with the correct pipelines endpoint (with locationId query parameter)
            if not self.location_id:
                return {
                    "status": "error",
                    "message": "Location ID is required for V2 API calls"
                }
            
            pipelines_url = f"{self.base_url}/opportunities/pipelines"
            params = {"locationId": self.location_id}
            
            logger.info(f"Testing endpoint: {pipelines_url}")
            logger.info(f"Query params: {params}")
            
            response = await self.client.get(pipelines_url, params=params)
            logger.info(f"Test response status: {response.status_code}")
            logger.info(f"Test response: {response.text[:500]}...")
            
            if response.status_code == 200:
                # Try to parse pipelines
                data = response.json()
                pipelines = data.get("pipelines", data if isinstance(data, list) else [])
                return {
                    "status": "success",
                    "endpoint": f"{pipelines_url}?locationId={self.location_id}",
                    "message": f"Connection successful - Found {len(pipelines)} pipelines",
                    "pipelines": [{"id": p.get("id"), "name": p.get("name"), "stages": len(p.get("stages", []))} for p in pipelines[:5]]  # Return first 5 for preview
                }
            elif response.status_code == 401:
                return {
                    "status": "error",
                    "endpoint": pipelines_url,
                    "message": "Invalid access token"
                }
            elif response.status_code == 403:
                return {
                    "status": "error", 
                    "endpoint": pipelines_url,
                    "message": "Access token doesn't have required permissions (needs 'opportunities.readonly')"
                }
            elif response.status_code == 400:
                return {
                    "status": "error",
                    "endpoint": pipelines_url,
                    "message": "Bad request - check location ID format"
                }
            else:
                return {
                    "status": "error",
                    "endpoint": pipelines_url,
                    "message": f"Unexpected status code: {response.status_code}",
                    "response": response.text[:200]
                }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection test failed: {str(e)}"
            }

    async def get_pipeline_id_by_name(self, pipeline_name: str) -> Optional[str]:
        """Get pipeline ID by pipeline name"""
        pipelines = await self.get_pipelines()
        
        for pipeline in pipelines:
            if pipeline.get('name', '').lower() == pipeline_name.lower():
                return pipeline.get('id')
        
        logger.warning(f"Pipeline '{pipeline_name}' not found. Available pipelines: {[p.get('name') for p in pipelines]}")
        return None

    async def get_stage_id_by_name(self, pipeline_id: str, stage_name: str) -> Optional[str]:
        """Get stage ID by stage name within a specific pipeline"""
        pipelines = await self.get_pipelines()
        
        for pipeline in pipelines:
            if pipeline.get('id') == pipeline_id:
                stages = pipeline.get('stages', [])
                for stage in stages:
                    if stage.get('name', '').lower() == stage_name.lower():
                        return stage.get('id')
                
                logger.warning(f"Stage '{stage_name}' not found in pipeline '{pipeline.get('name')}'. Available stages: {[s.get('name') for s in stages]}")
                return None
        
        logger.error(f"Pipeline with ID '{pipeline_id}' not found")
        return None

    async def update_opportunity_v2(self, opportunity_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update an opportunity using the v2 API"""
        url = f"{self.base_url}/opportunities/{opportunity_id}"
        
        logger.info(f"Updating opportunity {opportunity_id} with payload: {payload}")
        logger.info(f"API URL: {url}")
        logger.info(f"Headers: {dict(self.headers)}")
        
        try:
            response = await self.client.put(url, json=payload)
            logger.info(f"API Response Status: {response.status_code}")
            logger.info(f"API Response Headers: {dict(response.headers)}")
            logger.info(f"API Response Body: {response.text}")
            
            # Handle the duplicate opportunity error specifically
            if response.status_code == 400 and "duplicate opportunity" in response.text.lower():
                logger.warning(f"Duplicate opportunity error for {opportunity_id}. This might happen when moving between pipelines.")
                
                # Try with a modified name to avoid conflicts
                modified_payload = payload.copy()
                import time
                timestamp = int(time.time())
                original_name = modified_payload.get("name", "Opportunity")
                modified_payload["name"] = f"{original_name} - Updated {timestamp}"
                
                logger.info(f"Retrying with modified name: {modified_payload['name']}")
                response = await self.client.put(url, json=modified_payload)
                logger.info(f"Retry Response Status: {response.status_code}")
                logger.info(f"Retry Response Body: {response.text}")
            
            response.raise_for_status()
            
            logger.info(f"Successfully updated opportunity {opportunity_id}")
            return {
                "status": "success",
                "opportunity_id": opportunity_id,
                "response_status": response.status_code,
                "response_data": response.json() if response.text else {}
            }
            
        except httpx.HTTPStatusError as e:
            error_msg = f"Failed to update opportunity {opportunity_id}: Status {e.response.status_code}, Response: {e.response.text}"
            logger.error(error_msg)
            return {
                "status": "error",
                "opportunity_id": opportunity_id,
                "error": error_msg,
                "response_status": e.response.status_code,
                "response_body": e.response.text
            }
        except Exception as e:
            error_msg = f"Exception occurred while updating opportunity {opportunity_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "opportunity_id": opportunity_id,
                "error": error_msg
            }

    async def update_contact_notes(self, contact_id: str, notes: str) -> Dict[str, Any]:
        """Create/add notes for a contact using the GHL V2 API"""
        # Skip if notes is null, empty, or just whitespace, or if notes value is "N/A"
        if not contact_id or not notes or not notes.strip() or notes.strip().upper() in ["N/A", "NULL", "NONE", ""]:
            logger.info(f"Skipping notes update - Contact ID: {contact_id}, Notes: '{notes}' (empty or N/A)")
            return {"status": "skipped", "reason": "Missing contact_id or empty/N/A notes"}
        
        # Use the correct GHL V2 API endpoint for creating notes
        url = f"{self.base_url}/contacts/{contact_id}/notes"
        payload = {
            "userId": "JddmeV1TlwAguInXBALL",  # Fixed user ID as provided
            "body": notes.strip()
        }
        
        logger.info(f"Creating notes for contact {contact_id}: '{notes.strip()}'")
        logger.info(f"API URL: {url}")
        logger.info(f"Payload: {payload}")
        
        try:
            response = await self.client.post(url, json=payload)
            logger.info(f"Notes API Response Status: {response.status_code}")
            logger.info(f"Notes API Response: {response.text}")
            
            response.raise_for_status()
            
            logger.info(f"Successfully created notes for contact {contact_id}")
            return {
                "status": "success", 
                "contact_id": contact_id,
                "notes": notes.strip(),
                "response_status": response.status_code,
                "response_data": response.json() if response.text else {}
            }
        except httpx.HTTPStatusError as e:
            error_msg = f"Failed to create notes for contact {contact_id}: Status {e.response.status_code}, Response: {e.response.text}"
            logger.error(error_msg)
            return {
                "status": "error",
                "contact_id": contact_id,
                "error": error_msg,
                "response_status": e.response.status_code,
                "response_body": e.response.text
            }
        except Exception as e:
            error_msg = f"Exception occurred while creating notes for contact {contact_id}: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "contact_id": contact_id,
                "error": error_msg
            }

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
