import pandas as pd
import csv

print("Debugging pandas vs CSV reader difference...")

# Read with pandas with error handling
try:
    df = pd.read_csv('child_production.csv', encoding='utf-8', error_bad_lines=False, warn_bad_lines=True)
    print(f'Pandas read {len(df)} rows')
except Exception as e:
    print(f'Pandas error: {e}')

# Read with csv module and check each row
with open('child_production.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    csv_rows = list(reader)

print(f'CSV reader found {len(csv_rows)} rows')

# Check for rows with different lengths
print("\nChecking row lengths:")
for i, row in enumerate(csv_rows):
    if len(row) != 26:
        print(f'Row {i}: {len(row)} columns (expected 26)')
        print(f'Content preview: {str(row)[:200]}...')
        if i < 5:  # Show first few columns
            for j, col in enumerate(row[:5]):
                print(f'  Col {j}: {repr(col)}')

# Try reading with pandas using different options
print("\nTrying pandas with different options...")
try:
    df2 = pd.read_csv('child_production.csv', encoding='utf-8', sep=',', quotechar='"', engine='python')
    print(f'Pandas with python engine: {len(df2)} rows')
except Exception as e:
    print(f'Python engine error: {e}')

try:
    df3 = pd.read_csv('child_production.csv', encoding='utf-8', sep=',', quotechar='"', engine='c')
    print(f'Pandas with c engine: {len(df3)} rows')
except Exception as e:
    print(f'C engine error: {e}')