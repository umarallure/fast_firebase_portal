from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.models.schemas import ExportRequest
from app.services.ghl_export_new_api import process_export_request_new
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/export-opportunities-new-api")
async def export_opportunities_new_api(export_request: ExportRequest):
    """
    Export GHL opportunities using the new LeadConnectorHQ APIs.
    Uses services.leadconnectorhq.com endpoints with location_id parameter.
    Notes are included directly from the search API response.
    Returns an Excel file.
    """
    try:
        logger.info(f"Starting new API opportunities export for {len(export_request.selections)} selections")

        # Validate request
        if not export_request.selections:
            raise HTTPException(status_code=400, detail="No selections provided")

        for selection in export_request.selections:
            if not selection.pipelines:
                raise HTTPException(status_code=400, detail=f"No pipelines selected for account {selection.account_id}")

        # Process the export request using new APIs
        excel_content = await process_export_request_new(export_request)

        # Return the Excel file
        from io import BytesIO
        output = BytesIO(excel_content)

        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment; filename=GHL_Opportunities_New_API_Export.xlsx'
            }
        )

    except Exception as e:
        logger.error(f"Error processing new API opportunities export: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")