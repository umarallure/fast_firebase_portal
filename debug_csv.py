import sys
sys.path.append('.')

from app.services.master_child_opportunity_update import master_child_opportunity_service

# Read the CSV files with UTF-8 encoding
with open('test_master_opportunities.csv', 'r', encoding='utf-8') as f:
    master_content = f.read()

with open('test_child_clean.csv', 'r', encoding='utf-8') as f:
    child_content = f.read()

print("Testing CSV parsing directly...")
try:
    result = master_child_opportunity_service.parse_csv_files(master_content, child_content)
    print(f"Success: {result['success']}")
    if not result['success']:
        print(f"Error: {result['error']}")
    else:
        print(f"Master opportunities: {len(result['master_opportunities'])}")
        print(f"Child opportunities: {len(result['child_opportunities'])}")
except Exception as e:
    print(f"Exception: {e}")
    import traceback
    traceback.print_exc()