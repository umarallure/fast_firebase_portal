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
        self.v2_base_url = "https://services.leadconnectorhq.com"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        # Use specific token for V2 API to fetch custom fields
        self.v2_headers = {
            "Authorization": "Bearer pit-6a5701cc-7121-447e-ac5f-eda3ad51a4c4",
            "Accept": "application/json",
            "Version": "2021-07-28"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=settings.ghl_api_timeout)
        self.v2_client = httpx.AsyncClient(headers=self.v2_headers, timeout=settings.ghl_api_timeout)

    async def get_contacts_custom_fields_batch(self, contact_ids: List[str], custom_field_id: str = "yOhsGr96l5nFEE65b5DE") -> Dict[str, str]:
        """Fetch custom field values for multiple contacts in batches"""
        custom_field_values = {}
        
        if not contact_ids:
            return custom_field_values
        
        # Remove duplicates and filter out empty IDs
        unique_contact_ids = list(set([cid for cid in contact_ids if cid]))
        
        if not unique_contact_ids:
            return custom_field_values
        
        # Process in batches of 10 concurrent requests to avoid overwhelming the API
        batch_size = 10
        
        for i in range(0, len(unique_contact_ids), batch_size):
            batch = unique_contact_ids[i:i + batch_size]
            
            # Create concurrent tasks for this batch
            tasks = []
            for contact_id in batch:
                task = self.get_contact_custom_field_single(contact_id, custom_field_id)
                tasks.append(task)
            
            # Execute batch concurrently
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for contact_id, result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error(f"Error fetching custom field for contact {contact_id}: {result}")
                        custom_field_values[contact_id] = ""
                    else:
                        custom_field_values[contact_id] = result
                
                # Small delay between batches to respect rate limits
                if i + batch_size < len(unique_contact_ids):
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                # Set empty values for this batch
                for contact_id in batch:
                    custom_field_values[contact_id] = ""
        
        return custom_field_values

    async def get_contact_custom_field_single(self, contact_id: str, custom_field_id: str = "yOhsGr96l5nFEE65b5DE") -> str:
        """Fetch specific custom field value for a single contact using V2 API"""
        try:
            if not contact_id:
                return ""
            
            response = await self.v2_client.get(f"{self.v2_base_url}/contacts/{contact_id}")
            response.raise_for_status()
            contact_data = response.json()
            
            # Look for the specific custom field
            contact = contact_data.get("contact", {})
            custom_fields = contact.get("customFields", [])
            
            for field in custom_fields:
                if field.get("id") == custom_field_id:
                    return str(field.get("value", ""))
            
            return ""
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch custom field for contact {contact_id}: {str(e)}")
            return ""
        except Exception as e:
            logger.error(f"Error getting custom field for contact {contact_id}: {str(e)}")
            return ""

    async def get_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all pipelines for the current API key"""
        try:
            response = await self.client.get(f"{self.base_url}/pipelines")
            response.raise_for_status()
            return response.json().get("pipelines", [])
        except httpx.HTTPError as e:
            logger.error(f"Pipeline fetch failed: {str(e)}")
            return []
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

        # Format opportunities first
        formatted_opportunities = []
        contact_ids = []
        
        for opp in opportunities:
            formatted_opp = self.format_opportunity(opp, pipeline_name, stage_map, account_id)
            formatted_opportunities.append(formatted_opp)
            
            # Collect contact IDs for batch processing
            contact_id = opp.get("contact", {}).get("id")
            if contact_id:
                contact_ids.append(contact_id)

        # Batch fetch custom fields for all contacts
        logger.info(f"Fetching custom fields for {len(contact_ids)} contacts in batches...")
        custom_field_values = await self.get_contacts_custom_fields_batch(contact_ids)
        
        # Add custom field values to opportunities
        for i, opp in enumerate(opportunities):
            contact_id = opp.get("contact", {}).get("id")
            custom_field_value = custom_field_values.get(contact_id, "")
            formatted_opportunities[i]["Custom Field yOhsGr96l5nFEE65b5DE"] = custom_field_value

        logger.info(f"Completed processing {len(formatted_opportunities)} opportunities with custom fields")
        return formatted_opportunities

    @staticmethod
    def format_opportunity(opp: Dict[str, Any], pipeline_name: str, stage_map: Dict[str, str], account_id: str) -> Dict[str, Any]:
        """Format opportunity data into standardized schema, using stage name instead of stage ID, and include account id"""
        contact = opp.get("contact", {})
        def parse_date(date_str): 
            if date_str:
                try:
                    return datetime.fromisoformat(date_str[:-1]).isoformat()
                except:
                    return date_str
            return None
        def days_since(dt): 
            if dt:
                try:
                    parsed_dt = datetime.fromisoformat(dt.replace('Z', '')) if isinstance(dt, str) else dt
                    return (datetime.now() - parsed_dt).days
                except:
                    return None
            return None

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
            "Account Id": account_id,
            "Custom Field yOhsGr96l5nFEE65b5DE": ""  # Will be populated by get_opportunities method
        }

    async def close(self):
        """Close the HTTP clients"""
        await self.client.aclose()
        await self.v2_client.aclose()

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