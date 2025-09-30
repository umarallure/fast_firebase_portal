import httpx
import asyncio
from datetime import datetime
from typing import List, Dict, Any
import logging
import pandas as pd
from app.config import settings
from io import BytesIO

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

    async def get_opportunities(self, pipeline_id: str, pipeline_name: str, stage_map: Dict[str, str], account_id: str) -> List[Dict[str, Any]]:
        """Fetch all opportunities for a pipeline with pagination and include stage name and account id"""
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

        return [self.format_opportunity(opp, pipeline_name, stage_map, account_id) for opp in opportunities]

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

        # Match the dashboard export format exactly
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

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

class CustomFieldsSSNExportService:
    def __init__(self):
        pass

    async def export_ssn_data(self, account_ids: List[str], include_contacts: bool = True, 
                             include_opportunities: bool = False, export_format: str = "csv") -> Dict[str, Any]:
        """Export opportunities data from selected accounts - now acts exactly like dashboard export"""
        try:
            all_opportunities = []
            processed_accounts = 0
            total_records = 0
            
            for account_id in account_ids:
                logger.info(f"Processing account {account_id}")
                
                # Get account configuration
                account_config = self._get_account_config(account_id)
                if not account_config:
                    logger.warning(f"Account {account_id} not found in configuration")
                    continue
                
                # Get opportunities using dashboard export logic
                opportunities_data = await self._process_account_export(account_config)
                all_opportunities.extend(opportunities_data)
                
                processed_accounts += 1
                total_records += len(opportunities_data)
                
                logger.info(f"Account {account_id}: Found {len(opportunities_data)} opportunities")
            
            return {
                "success": True,
                "data": all_opportunities,
                "summary": {
                    "processed_accounts": processed_accounts,
                    "total_records": total_records,
                    "ssn_fields_found": 0  # Legacy field for compatibility
                }
            }
            
        except Exception as e:
            logger.error(f"Error in export: {str(e)}")
            return {
                "success": False,
                "message": f"Export failed: {str(e)}"
            }

    async def preview_ssn_data(self, account_ids: List[str], include_contacts: bool = True, 
                              include_opportunities: bool = False, limit: int = 10) -> Dict[str, Any]:
        """Preview opportunities data before full export"""
        try:
            preview_data = []
            total_count = 0
            
            for account_id in account_ids:
                account_config = self._get_account_config(account_id)
                if not account_config:
                    continue
                
                # Get limited opportunities data
                opportunities_data = await self._process_account_export(account_config, limit=limit)
                preview_data.extend(opportunities_data[:limit])
                total_count += len(opportunities_data)
                
                if len(preview_data) >= limit:
                    break
            
            return {
                "success": True,
                "data": preview_data[:limit],
                "total_count": total_count
            }
            
        except Exception as e:
            logger.error(f"Error in preview: {str(e)}")
            return {
                "success": False,
                "message": f"Preview failed: {str(e)}"
            }

    async def _process_account_export(self, account_config: Dict[str, Any], limit: int = None) -> List[Dict[str, Any]]:
        """Process export for a single account using dashboard export logic"""
        try:
            api_key = account_config.get('api_key')
            account_id = account_config.get('id')
            
            if not api_key:
                logger.warning(f"No API key found for account {account_id}")
                return []
            
            # Create GHL client
            client = GHLClient(api_key)
            
            try:
                # Get all pipelines
                pipelines = await client.get_pipelines()
                
                all_opportunities = []
                opportunities_processed = 0
                
                for pipeline in pipelines:
                    pipeline_id = pipeline["id"]
                    pipeline_name = pipeline["name"]
                    
                    # Get stage mapping for this pipeline
                    stage_map = {stage["id"]: stage["name"] for stage in pipeline.get("stages", [])}
                    
                    # Get opportunities for this pipeline
                    opportunities = await client.get_opportunities(
                        pipeline_id, pipeline_name, stage_map, str(account_id)
                    )
                    
                    all_opportunities.extend(opportunities)
                    opportunities_processed += len(opportunities)
                    
                    # Apply limit if specified
                    if limit and opportunities_processed >= limit:
                        break
                
                return all_opportunities[:limit] if limit else all_opportunities
                
            finally:
                await client.close()
                
        except Exception as e:
            logger.error(f"Error processing account export: {str(e)}")
            return []

    async def test_account_connection(self, account_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test connection to a specific account"""
        try:
            api_key = account_config.get('api_key')
            account_name = account_config.get('name', f"Account {account_config.get('id')}")
            
            if not api_key:
                return {
                    "success": False,
                    "message": f"{account_name}: No API key configured"
                }
            
            # Test connection
            client = GHLClient(api_key)
            
            try:
                pipelines = await client.get_pipelines()
                
                if pipelines:
                    return {
                        "success": True,
                        "message": f"{account_name}: Connected successfully. Found {len(pipelines)} pipelines."
                    }
                else:
                    return {
                        "success": False,
                        "message": f"{account_name}: Connected but no pipelines found."
                    }
                    
            finally:
                await client.close()
            
        except Exception as e:
            logger.error(f"Error testing connection: {str(e)}")
            return {
                "success": False,
                "message": f"Connection test failed: {str(e)}"
            }

    # Legacy methods for API compatibility - just return empty/error responses
    async def get_custom_field_definitions(self, account_config: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy method - not used in dashboard export"""
        return {
            "success": False,
            "message": "Custom field definitions not needed for dashboard export"
        }

    async def get_pipeline_stage_mapping(self, account_config: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy method - pipeline/stage mapping is handled internally"""
        return {
            "success": False,
            "message": "Pipeline mapping handled internally"
        }

    async def debug_test_opportunities_with_ssn(self, account_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Debug method - now returns standard opportunities"""
        return await self._process_account_export(account_config, limit=2)

    def _get_account_config(self, account_id: str) -> Dict[str, Any]:
        """Get account configuration by ID"""
        for subaccount in settings.subaccounts_list:
            if str(subaccount.get('id')) == str(account_id):
                return subaccount
        return None