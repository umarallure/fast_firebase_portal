import sys
sys.path.append('.')

from app.services.master_child_opportunity_update import master_child_opportunity_service
import pandas as pd
import io

# Read the production CSV files
print("Reading production files...")

with open('child_production.csv', 'r', encoding='utf-8') as f:
    child_content = f.read()

with open('master_production.csv', 'r', encoding='utf-8') as f:
    master_content = f.read()

print("Testing CSV parsing with production files...")

# Count lines in each file
child_lines = child_content.strip().split('\n')
master_lines = master_content.strip().split('\n')

print(f"Child file: {len(child_lines)} total lines ({len(child_lines)-1} data rows)")
print(f"Master file: {len(master_lines)} total lines ({len(master_lines)-1} data rows)")

# Debug: Check each child row individually
print("\nDebugging child rows...")
child_df = pd.read_csv(io.StringIO(child_content))

for idx, row in child_df.iterrows():
    opportunity_id = str(row.get('Opportunity ID', '')).strip()
    pipeline_id = str(row.get('Pipeline ID', '')).strip()
    account_id = str(row.get('Account Id', '')).strip()
    contact_name = str(row.get('Contact Name', '')).strip()

    if not all([opportunity_id, pipeline_id, account_id, contact_name]):
        print(f"Row {idx + 2}: Missing data - ID:{opportunity_id}, Pipeline:{pipeline_id}, Account:{account_id}, Name:{contact_name}")

print(f"\nTotal child rows in DataFrame: {len(child_df)}")

# Test parsing
try:
    result = master_child_opportunity_service.parse_csv_files(master_content, child_content)

    print(f"\nParsing Result:")
    print(f"Success: {result.get('success', False)}")

    if result.get('success'):
        print(f"Master opportunities parsed: {len(result.get('master_opportunities', []))}")
        print(f"Child opportunities parsed: {len(result.get('child_opportunities', []))}")

        expected_child_count = len(child_df)
        actual_child_count = len(result.get('child_opportunities', []))

        if actual_child_count != expected_child_count:
            print(f"\n❌ ISSUE: Expected {expected_child_count} child opportunities but got {actual_child_count}")

            # Show first few child opportunities to debug
            child_opps = result.get('child_opportunities', [])[:5]
            print("\nFirst 5 parsed child opportunities:")
            for i, opp in enumerate(child_opps, 1):
                print(f"{i}. {opp.get('contact_name')} - {opp.get('phone')}")

    else:
        print(f"Error: {result.get('error', 'Unknown error')}")

except Exception as e:
    print(f"Exception during parsing: {e}")
    import traceback
    traceback.print_exc()