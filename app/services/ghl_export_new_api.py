import httpx
import asyncio
from datetime import datetime
from typing import List, Dict, Any
import logging
import pandas as pd
from app.config import settings
from app.models.schemas import ExportRequest, SelectionSchema

logger = logging.getLogger(__name__)

class GHLClientNew:
    def __init__(self, access_token: str, location_id: str):
        self.base_url = "https://services.leadconnectorhq.com"
        self.access_token = access_token
        self.location_id = location_id
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Version": "2021-07-28"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=settings.ghl_api_timeout)

    async def get_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all pipelines for the location using the new API"""
        try:
            url = f"{self.base_url}/opportunities/pipelines?locationId={self.location_id}"
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("pipelines", [])
        except httpx.HTTPError as e:
            logger.error(f"Pipeline fetch failed for location {self.location_id}: {str(e)}")
            return []

    async def search_opportunities(self, pipeline_id: str = None, pipeline_stage_id: str = None,
                                 max_records: int = None) -> List[Dict[str, Any]]:
        """Search opportunities using the new search API"""
        all_opportunities = []
        page = 1
        limit = 100

        while True:
            try:
                # Build URL with search parameters
                url = f"{self.base_url}/opportunities/search?location_id={self.location_id}&limit={limit}&page={page}"

                if pipeline_id:
                    url += f"&pipeline_id={pipeline_id}"
                if pipeline_stage_id:
                    url += f"&pipeline_stage_id={pipeline_stage_id}"

                logger.info(f"DEBUG: Searching opportunities - page {page}, URL: {url}")

                response = await self.client.get(url)

                if response.status_code == 429:
                    wait_time = 2
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status_code != 200:
                    logger.error(f"Error {response.status_code}: {response.text}")
                    break

                # Success
                data = response.json()
                opportunities = data.get("opportunities", [])
                meta = data.get("meta", {})

                logger.info(f"DEBUG: Page {page} - received {len(opportunities)} opportunities")

                if not opportunities:
                    logger.info("No more opportunities, breaking")
                    break

                all_opportunities.extend(opportunities)

                # Check if we've reached the max_records limit
                if max_records and len(all_opportunities) >= max_records:
                    logger.info(f"MAX_RECORDS CHECK: {len(all_opportunities)} >= {max_records} - BREAKING")
                    all_opportunities = all_opportunities[:max_records]
                    break

                # Check pagination
                current_page = meta.get("currentPage", page)
                total_pages = meta.get("total", 1) // limit + 1

                if current_page >= total_pages:
                    logger.info("Reached last page, breaking")
                    break

                page += 1

                # Small delay to avoid rate limits
                await asyncio.sleep(0.1)

            except httpx.HTTPError as e:
                logger.error(f"Opportunity search failed: {str(e)}")
                break

        logger.info(f"Total opportunities found: {len(all_opportunities)}")
        return all_opportunities

    @staticmethod
    def format_opportunity_new(opp: Dict[str, Any], pipeline_name: str, stage_name: str, account_id: str) -> Dict[str, Any]:
        """Format opportunity data from the new search API"""
        contact = opp.get("contact", {})
        notes = opp.get("notes", [])

        # Parse dates
        def parse_date(date_str):
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None

        created = parse_date(opp.get("createdAt"))
        updated = parse_date(opp.get("updatedAt"))
        stage_changed = parse_date(opp.get("lastStageChangeAt"))
        last_action = parse_date(opp.get("lastActionDate"))

        def days_since(dt):
            if not dt:
                return None
            # Make current time timezone-aware to match the parsed datetime
            from datetime import timezone
            now = datetime.now(timezone.utc)
            return (now - dt).days

        # Format notes (take first 4)
        formatted_notes = []
        for i, note in enumerate(notes[:4]):
            formatted_notes.append(note.get("body", "") if isinstance(note, dict) else str(note))

        # Pad to 4 notes
        while len(formatted_notes) < 4:
            formatted_notes.append("")

        return {
            "Opportunity Name": opp.get("name", ""),
            "Contact Name": contact.get("name", ""),
            "phone": contact.get("phone", ""),
            "email": contact.get("email", ""),
            "pipeline": pipeline_name,
            "stage": stage_name,
            "Lead Value": opp.get("monetaryValue", 0),
            "source": opp.get("source", ""),
            "assigned": opp.get("assignedTo", ""),
            "Created on": created,
            "Updated on": updated,
            "lost reason ID": "",
            "lost reason name": "",
            "Followers": "",
            "Notes": "",
            "tags": ", ".join(contact.get("tags", [])) if contact.get("tags") else "",
            "Engagement Score": "",
            "status": opp.get("status", ""),
            "Opportunity ID": opp.get("id", ""),
            "Contact ID": contact.get("id", ""),
            "Pipeline Stage ID": opp.get("pipelineStageId", ""),
            "Pipeline ID": opp.get("pipelineId", ""),
            "Days Since Last Stage Change": days_since(stage_changed),
            "Days Since Last Status Change": days_since(stage_changed),
            "Days Since Last Updated": days_since(updated),
            "Account Id": account_id,
            "note1": formatted_notes[0],
            "note2": formatted_notes[1],
            "note3": formatted_notes[2],
            "note4": formatted_notes[3]
        }

async def process_export_request_new(export_request: ExportRequest) -> bytes:
    """Process export request using the new GHL APIs"""
    try:
        logger.info(f"Starting new API export processing for {len(export_request.selections)} selections")

        all_opportunities = []

        # Get subaccounts for access tokens and location IDs
        subaccounts = settings.subaccounts_list
        account_location_ids = {str(s['id']): s.get('location_id', '') for s in subaccounts}
        account_access_tokens = {str(s['id']): s.get('access_token', '') for s in subaccounts}

        # Process each selection
        for selection in export_request.selections:
            account_id = str(selection.account_id)
            access_token = account_access_tokens.get(account_id)
            location_id = account_location_ids.get(account_id)

            if not access_token or not location_id:
                logger.warning(f"No access token or location ID found for account {account_id}")
                continue

            logger.info(f"Processing account {account_id} with location {location_id}")

            # Create GHL client for this account
            client = GHLClientNew(access_token, location_id)

            # Get pipelines for this account
            pipelines = await client.get_pipelines()
            pipeline_map = {p['id']: p['name'] for p in pipelines}
            stage_map = {}

            # Build stage mapping from pipelines
            for pipeline in pipelines:
                pipeline_stages = pipeline.get('stages', [])
                for stage in pipeline_stages:
                    stage_map[stage['id']] = stage['name']

            logger.info(f"Found {len(pipeline_map)} pipelines and {len(stage_map)} stages for account {account_id}")

            # Process each selected pipeline
            for pipeline_id in selection.pipelines:
                pipeline_name = pipeline_map.get(pipeline_id, f"Pipeline {pipeline_id}")

                logger.info(f"Searching opportunities for pipeline {pipeline_name} ({pipeline_id})")

                # Search opportunities for this pipeline
                opportunities = await client.search_opportunities(
                    pipeline_id=pipeline_id,
                    max_records=export_request.max_records
                )

                logger.info(f"Found {len(opportunities)} opportunities in pipeline {pipeline_name}")

                # Format opportunities
                for opp in opportunities:
                    stage_id = opp.get('pipelineStageId')
                    stage_name = stage_map.get(stage_id, "")

                    formatted_opp = client.format_opportunity_new(
                        opp,
                        pipeline_name,
                        stage_name,
                        account_id
                    )
                    all_opportunities.append(formatted_opp)

        logger.info(f"Total formatted opportunities: {len(all_opportunities)}")

        # Create Excel file from the data
        if all_opportunities:
            df = pd.DataFrame(all_opportunities)

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
            logger.info(f"Created Excel file with {len(all_opportunities)} rows using new API")
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
        logger.error(f"Error in process_export_request_new: {str(e)}")
        raise