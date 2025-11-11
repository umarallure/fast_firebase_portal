from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.models.schemas import ExportRequest
from app.services.ghl_export_new import process_export_request
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/export-opportunities-only")
async def export_opportunities_only(export_request: ExportRequest):
    """
    Export GHL opportunities with notes for selected accounts and pipelines.
    Fetches the latest 4 notes for each contact and includes them in note1-note4 columns.
    Returns an Excel file.
    """
    try:
        logger.info(f"Starting opportunities export with notes for {len(export_request.selections)} selections")

        # Validate request
        if not export_request.selections:
            raise HTTPException(status_code=400, detail="No selections provided")

        for selection in export_request.selections:
            if not selection.pipelines:
                raise HTTPException(status_code=400, detail=f"No pipelines selected for account {selection.account_id}")

        # Process the export request
        excel_content = await process_export_request(export_request)

        # Return the Excel file
        from io import BytesIO
        output = BytesIO(excel_content)

        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment; filename=GHL_Opportunities_Only_Export.xlsx'
            }
        )

    except Exception as e:
        logger.error(f"Error processing opportunities-only export: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")