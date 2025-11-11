import asyncio
from app.services.ghl_export_new import process_export_request
from app.models.schemas import ExportRequest, SelectionSchema

async def test_export_direct():
    # Create a test export request
    export_request = ExportRequest(
        selections=[
            SelectionSchema(
                account_id="1",  # Test account
                pipelines=["qIBDmYws4nkCbm9Rlejc"]  # Test pipeline
            )
        ]
    )
    
    print("Testing direct export processing...")
    try:
        excel_content = await process_export_request(export_request)
        print(f"Export successful! Content length: {len(excel_content)} bytes")
        
        # Save to file
        with open('direct_test_export.xlsx', 'wb') as f:
            f.write(excel_content)
        print("Saved to direct_test_export.xlsx")
        
        # Check the file
        import pandas as pd
        df = pd.read_excel('direct_test_export.xlsx', sheet_name='Opportunities')
        print(f"Excel file has {df.shape[0]} rows and {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")
        
        notes_cols = [col for col in df.columns if 'note' in col.lower()]
        print(f"Notes columns: {notes_cols}")
        
    except Exception as e:
        print(f"Export failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_export_direct())