import sys
sys.path.append('.')

# Read the child file and count actual data rows
print("Analyzing child CSV file...")

with open('child_production.csv', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print(f"Total lines in file: {len(lines)}")
print(f"Header: {lines[0].strip()[:100]}...")

data_lines = 0
problematic_lines = []

for i, line in enumerate(lines[1:], 1):  # Skip header
    line = line.strip()
    if line:  # Non-empty line
        # Count commas to check if it's a complete CSV row
        comma_count = line.count(',')
        if comma_count >= 20:  # Should have at least 20 columns
            data_lines += 1
        else:
            problematic_lines.append((i, line[:100]))

print(f"Data lines found: {data_lines}")
print(f"Problematic lines: {len(problematic_lines)}")

if problematic_lines:
    print("\nProblematic lines:")
    for line_num, content in problematic_lines[:3]:  # Show first 3
        print(f"Line {line_num}: {content}...")

# Try reading with different CSV options
import csv
print("\nTrying CSV reader...")
with open('child_production.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    rows = list(reader)

print(f"CSV reader found {len(rows)} rows (including header)")
print(f"Data rows: {len(rows) - 1}")

# Check if any rows have different column counts
column_counts = [len(row) for row in rows]
unique_counts = set(column_counts)
print(f"Column count distribution: {dict((count, column_counts.count(count)) for count in unique_counts)}")

if len(unique_counts) > 1:
    print("❌ Rows have different column counts - this indicates malformed CSV")
    for i, (row, count) in enumerate(zip(rows, column_counts)):
        if count != max(unique_counts):
            print(f"Row {i}: {count} columns - {row[:3]}...")