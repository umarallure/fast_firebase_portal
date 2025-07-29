import httpx
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import pandas as pd
from app.config import settings
from app.models.schemas import ExportRequest, SelectionSchema

logger = logging.getLogger(__name__)

class EnhancedGHLClient:
    def __init__(self, api_key: str):
        self.base_url = "https://rest.gohighlevel.com/v1"
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.client = httpx.AsyncClient(headers=self.headers, timeout=settings.ghl_api_timeout)
        self._custom_field_cache = {}  # Cache custom field definitions

    async def get_custom_fields(self) -> Dict[str, str]:
        """Fetch all custom field definitions and return a mapping of ID to name"""
        if self._custom_field_cache:
            return self._custom_field_cache
            
        try:
            response = await self.client.get(f"{self.base_url}/custom-fields/")
            response.raise_for_status()
            data = response.json()
            custom_fields = data.get("customFields", [])
            
            # Create mapping of ID to field name
            field_mapping = {}
            for field in custom_fields:
                field_id = field.get("id")
                field_name = field.get("name", f"Custom Field {field_id}")
                if field_id:
                    field_mapping[field_id] = field_name
            
            self._custom_field_cache = field_mapping
            logger.info(f"Cached {len(field_mapping)} custom field definitions")
            return field_mapping
            
        except httpx.HTTPError as e:
            logger.error(f"Custom fields fetch failed: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching custom fields: {str(e)}")
            return {}

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

    async def get_contact_details(self, contact_id: str) -> Dict[str, Any]:
        """Fetch detailed contact information including custom fields"""
        try:
            # First, ensure we have custom field definitions
            custom_field_definitions = await self.get_custom_fields()
            
            # Fetch contact details
            response = await self.client.get(f"{self.base_url}/contacts/{contact_id}")
            response.raise_for_status()
            raw_response = response.json()
            contact_data = raw_response.get("contact", {})
            
            logger.info(f"Contact {contact_id} - Found contact data with keys: {list(contact_data.keys())}")
            
            # Extract custom fields from the GHL API structure
            custom_fields = {}
            
            # Handle the 'customField' array (singular, as per GHL API)
            if "customField" in contact_data:
                custom_field_array = contact_data["customField"]
                logger.info(f"Found customField array with {len(custom_field_array)} items")
                
                for field in custom_field_array:
                    field_id = field.get("id")
                    field_value = field.get("value", "")
                    
                    # Get the field name from our custom field definitions
                    field_name = custom_field_definitions.get(field_id, f"Custom Field {field_id}")
                    
                    # Use the actual field name or fall back to ID
                    custom_fields[field_name] = field_value
                    logger.info(f"Mapped custom field: {field_id} ({field_name}) = {field_value}")
            
            # Also check for any direct custom fields in contact_data (fallback)
            # Specific custom fields the user mentioned
            specific_custom_fields = {
                'date_of_submission': 'Date of Submission',
                'birth_state': 'Birth State', 
                'age': 'Age',
                'social_security_number': 'Social Security Number',
                'height': 'Height',
                'weight': 'Weight',
                'doctors_name': 'Doctors Name',
                'tobacco_user': 'Tobacco User?',
                'health_conditions': 'Health Conditions',
                'medications': 'Medications'
            }
            
            # Check for these specific fields in multiple possible formats
            for field_key, field_display_name in specific_custom_fields.items():
                # Check direct field name
                if field_key in contact_data:
                    custom_fields[field_display_name] = contact_data[field_key]
                    logger.info(f"Found specific custom field: {field_key} -> {field_display_name} = {contact_data[field_key]}")
                
                # Check camelCase version
                camel_case_key = ''.join(word.capitalize() if i > 0 else word for i, word in enumerate(field_key.split('_')))
                if camel_case_key in contact_data:
                    custom_fields[field_display_name] = contact_data[camel_case_key]
                    logger.info(f"Found camelCase custom field: {camel_case_key} -> {field_display_name} = {contact_data[camel_case_key]}")
                
                # Check if it exists in the custom field definitions by name
                for cf_id, cf_name in custom_field_definitions.items():
                    if cf_name.lower() == field_display_name.lower():
                        # Look for this ID in the customField array
                        if "customField" in contact_data:
                            for cf in contact_data["customField"]:
                                if cf.get("id") == cf_id:
                                    custom_fields[field_display_name] = cf.get("value", "")
                                    logger.info(f"Found custom field by definition match: {cf_id} -> {field_display_name} = {cf.get('value', '')}")
                                    break
            
            # Look for any non-standard fields that might be custom fields
            standard_fields = {
                'id', 'locationId', 'email', 'emailLowerCase', 'fingerprint', 'timezone', 'country',
                'source', 'dateAdded', 'customField', 'tags', 'name', 'firstName', 'lastName', 
                'phone', 'address1', 'address2', 'city', 'state', 'postalCode', 'companyName', 
                'website', 'dnd', 'type', 'dateUpdated', 'contactType', 'assignedTo', 'lastActivity'
            }
            
            for key, value in contact_data.items():
                if key not in standard_fields and not key.startswith('_') and not key.startswith('__'):
                    custom_fields[f"direct_{key}"] = value
                    logger.info(f"Found potential custom field: direct_{key} = {value}")
            
            logger.info(f"Total custom fields extracted for contact {contact_id}: {len(custom_fields)}")
            if custom_fields:
                logger.info(f"Custom field names: {list(custom_fields.keys())}")
            
            return {
                "contact_data": contact_data,
                "custom_fields": custom_fields,
                "raw_response": raw_response
            }
            
        except httpx.HTTPError as e:
            logger.error(f"Contact fetch failed for {contact_id}: {str(e)}")
            return {"contact_data": {}, "custom_fields": {}, "raw_response": {}}
        except Exception as e:
            logger.error(f"Unexpected error fetching contact {contact_id}: {str(e)}")
            return {"contact_data": {}, "custom_fields": {}, "raw_response": {}}

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

async def process_enhanced_export_request(export_request: ExportRequest) -> bytes:
    """Process export request with enhanced contact details and custom fields"""
    tasks = []
    clients = {}
    all_contact_ids = set()  # Track unique contact IDs
    
    # First pass: Get all opportunities
    for selection in export_request.selections:
        client = EnhancedGHLClient(selection.api_key)
        clients[selection.api_key] = client
        pipelines = await client.get_pipelines()
        selected_pipelines = [
            pipe for pipe in pipelines 
            if pipe["id"] in selection.pipelines
        ]
        # Build stage map from pipelines response
        for pipeline in selected_pipelines:
            stage_map = {stage["id"]: stage["name"] for stage in pipeline.get("stages", [])}
            tasks.append(
                client.get_opportunities(pipeline["id"], pipeline["name"], stage_map, selection.account_id)
            )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_opps = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Pipeline fetch failed: {str(result)}")
            continue
        all_opps.extend(result)
        # Collect contact IDs
        for opp in result:
            contact_id = opp.get("Contact ID")
            if contact_id:
                all_contact_ids.add(contact_id)

    logger.info(f"Found {len(all_contact_ids)} unique contacts to enhance")

    # Second pass: Get enhanced contact details
    contact_enhancement_map = {}
    
    # Group contact IDs by API key for efficient fetching
    contact_api_key_map = {}
    for selection in export_request.selections:
        for opp in all_opps:
            if opp.get("Account Id") == selection.account_id:
                contact_id = opp.get("Contact ID")
                if contact_id:
                    contact_api_key_map[contact_id] = selection.api_key

    # Fetch contact details in batches
    for contact_id, api_key in contact_api_key_map.items():
        if api_key not in clients:
            clients[api_key] = EnhancedGHLClient(api_key)
        
        contact_details = await clients[api_key].get_contact_details(contact_id)
        contact_enhancement_map[contact_id] = contact_details
        await asyncio.sleep(0.1)  # Rate limiting

    # Third pass: Enhance opportunities with contact details
    enhanced_opps = []
    all_custom_field_names = set()
    
    for opp in all_opps:
        contact_id = opp.get("Contact ID")
        enhanced_opp = opp.copy()
        
        if contact_id and contact_id in contact_enhancement_map:
            contact_details = contact_enhancement_map[contact_id]
            contact_data = contact_details.get("contact_data", {})
            custom_fields = contact_details.get("custom_fields", {})
            
            # Add enhanced contact information
            enhanced_opp["Enhanced Contact Name"] = contact_data.get("name", "")
            enhanced_opp["Enhanced Phone"] = contact_data.get("phone", "")
            enhanced_opp["Enhanced Email"] = contact_data.get("email", "")
            enhanced_opp["Contact First Name"] = contact_data.get("firstName", "")
            enhanced_opp["Contact Last Name"] = contact_data.get("lastName", "")
            enhanced_opp["Contact Company"] = contact_data.get("companyName", "")
            enhanced_opp["Contact Address"] = contact_data.get("address1", "")
            enhanced_opp["Contact City"] = contact_data.get("city", "")
            enhanced_opp["Contact State"] = contact_data.get("state", "")
            enhanced_opp["Contact Postal Code"] = contact_data.get("postalCode", "")
            enhanced_opp["Contact Country"] = contact_data.get("country", "")
            enhanced_opp["Contact Website"] = contact_data.get("website", "")
            enhanced_opp["Contact Timezone"] = contact_data.get("timezone", "")
            enhanced_opp["Contact DND"] = contact_data.get("dnd", "")
            enhanced_opp["Contact Type"] = contact_data.get("type", "")
            enhanced_opp["Contact Source"] = contact_data.get("source", "")
            enhanced_opp["Contact Date Added"] = contact_data.get("dateAdded", "")
            enhanced_opp["Contact Date Updated"] = contact_data.get("dateUpdated", "")
            
            # Add custom fields
            for field_name, field_value in custom_fields.items():
                enhanced_opp[f"Custom: {field_name}"] = field_value
                all_custom_field_names.add(f"Custom: {field_name}")
        else:
            # Add empty values for missing contact data
            enhanced_opp["Enhanced Contact Name"] = ""
            enhanced_opp["Enhanced Phone"] = ""
            enhanced_opp["Enhanced Email"] = ""
            enhanced_opp["Contact First Name"] = ""
            enhanced_opp["Contact Last Name"] = ""
            enhanced_opp["Contact Company"] = ""
            enhanced_opp["Contact Address"] = ""
            enhanced_opp["Contact City"] = ""
            enhanced_opp["Contact State"] = ""
            enhanced_opp["Contact Postal Code"] = ""
            enhanced_opp["Contact Country"] = ""
            enhanced_opp["Contact Website"] = ""
            enhanced_opp["Contact Timezone"] = ""
            enhanced_opp["Contact DND"] = ""
            enhanced_opp["Contact Type"] = ""
            enhanced_opp["Contact Source"] = ""
            enhanced_opp["Contact Date Added"] = ""
            enhanced_opp["Contact Date Updated"] = ""
        
        enhanced_opps.append(enhanced_opp)

    # Ensure all opportunities have all custom field columns
    for opp in enhanced_opps:
        for field_name in all_custom_field_names:
            if field_name not in opp:
                opp[field_name] = ""

    # Define column order - original columns first, then enhanced contact info, then custom fields
    base_columns = [
        "Opportunity Name", "Contact Name", "phone", "email", "pipeline", "stage",
        "Lead Value", "source", "assigned", "Created on", "Updated on",
        "lost reason ID", "lost reason name", "Followers", "Notes", "tags",
        "Engagement Score", "status", "Opportunity ID", "Contact ID",
        "Pipeline Stage ID", "Pipeline ID", "Days Since Last Stage Change",
        "Days Since Last Status Change", "Days Since Last Updated", "Account Id"
    ]
    
    enhanced_columns = [
        "Enhanced Contact Name", "Enhanced Phone", "Enhanced Email",
        "Contact First Name", "Contact Last Name", "Contact Company",
        "Contact Address", "Contact City", "Contact State", "Contact Postal Code",
        "Contact Country", "Contact Website", "Contact Timezone", "Contact DND",
        "Contact Type", "Contact Source", "Contact Date Added", "Contact Date Updated"
    ]
    
    custom_field_columns = sorted(list(all_custom_field_names))
    
    all_columns = base_columns + enhanced_columns + custom_field_columns
    
    df = pd.DataFrame(enhanced_opps, columns=all_columns)
    
    from io import BytesIO
    output = BytesIO()
    df.to_excel(output, index=False)
    return output.getvalue()
