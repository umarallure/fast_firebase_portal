import asyncio
from app.services.ghl_export_new import process_export_request
from app.models.schemas import ExportRequest, SelectionSchema
import pandas as pd

async def test_export_with_notes():
    # Test with Customer Pipeline that has opportunities
    export_request = ExportRequest(
        selections=[
            SelectionSchema(
                account_id="1",  # Test account
                pipelines=["KKfybEiU8weJ97yIjn8p"]  # Customer Pipeline (correct ID)
            )
        ]
    )
    
    print("Testing export with Customer Pipeline (has opportunities)...")
    try:
        excel_content = await process_export_request(export_request)
        print(f"✓ Export successful! Content length: {len(excel_content)} bytes")
        
        # Save to file
        with open('test_with_notes.xlsx', 'wb') as f:
            f.write(excel_content)
        print("✓ Saved to test_with_notes.xlsx")
        
        # Check the file
        df = pd.read_excel('test_with_notes.xlsx', sheet_name='Opportunities')
        print(f"\n📊 Excel file has {df.shape[0]} rows and {df.shape[1]} columns")
        
        if df.shape[0] > 0:
            print(f"\n✓ Found {df.shape[0]} opportunity(ies)!")
            
            # Check for notes columns
            notes_cols = ['note1', 'note2', 'note3', 'note4']
            existing_notes = [col for col in notes_cols if col in df.columns]
            print(f"✓ Notes columns present: {existing_notes}")
            
            # Check if any notes have content
            if existing_notes:
                for col in existing_notes:
                    non_empty = df[col].notna() & (df[col] != '')
                    if non_empty.any():
                        print(f"  ✓ {col}: {non_empty.sum()} non-empty")
                        # Show first note
                        first_note = df[df[col].notna() & (df[col] != '')][col].iloc[0]
                        print(f"    Sample: {first_note[:80]}...")
                    else:
                        print(f"  ✗ {col}: empty")
            
            # Show key columns
            display_cols = ['Opportunity Name', 'Contact Name', 'phone', 'pipeline', 'stage', 'Contact ID']
            available_cols = [col for col in display_cols if col in df.columns]
            print(f"\n📋 Opportunity details:")
            print(df[available_cols].to_string(index=False))
        else:
            print("\n✗ No opportunities found in export")
        
    except Exception as e:
        print(f"✗ Export failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_export_with_notes())
