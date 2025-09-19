import pandas as pd
import csv

print("Finding the problematic row...")

# Read all lines
with open('child_production.csv', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")

# Try to parse each line individually with pandas
problematic_rows = []

for i, line in enumerate(lines):
    if i == 0:  # Skip header
        continue

    try:
        # Try to read this single line as CSV
        from io import StringIO
        single_row_df = pd.read_csv(StringIO(lines[0] + '\n' + line), encoding='utf-8')
        if len(single_row_df) == 0:
            problematic_rows.append((i, line.strip()[:100]))
    except Exception as e:
        problematic_rows.append((i, f"Error: {e} - {line.strip()[:100]}"))

if problematic_rows:
    print(f"Found {len(problematic_rows)} problematic rows:")
    for row_num, content in problematic_rows[:3]:
        print(f"Row {row_num}: {content}...")
else:
    print("No obviously problematic rows found")

# Let's try reading the entire file but with different pandas options
print("\nTrying different pandas approaches...")

# Try with low_memory=False
try:
    df1 = pd.read_csv('child_production.csv', low_memory=False)
    print(f"low_memory=False: {len(df1)} rows")
except Exception as e:
    print(f"low_memory=False error: {e}")

# Try with dtype=str
try:
    df2 = pd.read_csv('child_production.csv', dtype=str)
    print(f"dtype=str: {len(df2)} rows")
except Exception as e:
    print(f"dtype=str error: {e}")

# Check if the issue is with the last row specifically
print(f"\nLast line content: {repr(lines[-1][:100])}")

# Try reading without the last line
try:
    content_without_last = ''.join(lines[:-1])
    df3 = pd.read_csv(StringIO(content_without_last))
    print(f"Without last line: {len(df3)} rows")
except Exception as e:
    print(f"Without last line error: {e}")

# Try reading only the last few lines
try:
    last_few_lines = ''.join(lines[-5:])
    df4 = pd.read_csv(StringIO(lines[0] + '\n' + last_few_lines))
    print(f"Last 5 lines: {len(df4)} rows")
except Exception as e:
    print(f"Last 5 lines error: {e}")