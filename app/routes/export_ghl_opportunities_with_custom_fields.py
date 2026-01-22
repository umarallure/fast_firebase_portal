from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.models.schemas import ExportRequestWithCustomFields
from app.services.ghl_export_with_custom_fields import process_export_request_with_custom_fields
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/export-opportunities-with-custom-fields")
async def export_opportunities_with_custom_fields(export_request: ExportRequestWithCustomFields):
    """
    Export GHL opportunities using the new LeadConnectorHQ APIs and fetching custom fields (SSN).
    """
    try:
        logger.info(f"Starting new API opportunities export with custom fields for {len(export_request.selections)} selections")

        # Validate request
        if not export_request.selections:
            raise HTTPException(status_code=400, detail="No selections provided")

        for selection in export_request.selections:
            if not selection.pipelines:
                raise HTTPException(status_code=400, detail=f"No pipelines selected for account {selection.account_id}")

        # Process the export request using new APIs
        excel_content = await process_export_request_with_custom_fields(export_request)

        # Return the Excel file
        from io import BytesIO
        output = BytesIO(excel_content)

        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment; filename=GHL_Opportunities_Export_With_SSN.xlsx'
            }
        )

    except Exception as e:
        logger.error(f"Error processing new API opportunities export with custom fields: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
