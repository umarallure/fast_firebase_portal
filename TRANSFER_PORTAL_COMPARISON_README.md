# Transfer Portal Comparison Automation

## Overview
This automation compares two CSV files (master and child) to identify new entries in the child file that are not present in the master file. It's specifically designed for transfer portal data comparison based on phone number matching.

## Features

### 🔍 **Intelligent Phone Number Matching**
- Normalizes phone numbers for accurate comparison
- Handles various phone formats: `+1234567890`, `(123) 456-7890`, `123-456-7890`, etc.
- Removes country codes for consistent matching

### 📊 **Account Mapping**
- Automatically maps Account IDs to Account Names using environment configuration
- Supports 33+ predefined account mappings
- Fallback to "Unknown Account {ID}" for unmapped accounts

### 📁 **File Format Support**
- **Master CSV Format**: `Customer Phone Number`, `Name`, `Policy Status`, `GHL Pipeline Stage`, `CALL CENTER`
- **Child CSV Format**: `Contact Name`, `phone`, `pipeline`, `stage`, `Account Id`
- Multiple encoding support: UTF-8, Latin-1, CP1252, etc.

### 📈 **Comprehensive Reporting**
- Processing statistics and summary reports
- Account distribution analysis
- Pipeline stage distribution
- Export options: CSV download or JSON response

## How It Works

1. **Upload Files**: Upload your master database CSV and child CSV files
2. **Normalize Data**: Phone numbers are normalized for accurate comparison
3. **Compare**: System identifies child entries not found in master database
4. **Process**: Account IDs are converted to account names
5. **Export**: Get results as CSV file or JSON with detailed statistics

## File Structure

### Input Files

#### Master CSV (transferportalmaster.csv)
```csv
Customer Phone Number,Name,Policy Status,GHL Pipeline Stage,CALL CENTER
(309) 964-9062,Hash T,,,Cyber-leads BPO
(913) 342-5118,Dixie Frye,Customer has already been DQ from our agency,DQ'd Can't be sold,Cyber-leads BPO
```

#### Child CSV (transferportalchild.csv)
```csv
Contact Name,phone,pipeline,stage,Account Id
TONY FOSTER,+13139350685,Customer Pipeline,Approved Customer - Not Paid,2
Jay Kelly,+17075675820,Customer Pipeline,Approved Customer - Not Paid,2
```

### Output CSV Structure
The generated CSV maintains the master file structure:
```csv
Customer Phone Number,Name,Policy Status,GHL Pipeline Stage,CALL CENTER,Source,Processing Date,Original Pipeline
+13139350685,TONY FOSTER,,Approved Customer - Not Paid,Ark Tech,Transfer Portal Child,2025-07-28 15:55:02,Customer Pipeline
```

## Usage

### Web Interface
1. Navigate to `/transfer-portal-comparison`
2. Upload your master and child CSV files
3. Choose output format (CSV or JSON)
4. Click "Process Comparison"
5. Download results or view statistics

### API Endpoints
- `POST /api/transfer-portal-comparison/process` - Process comparison
- `POST /api/transfer-portal-comparison/preview` - Preview uploaded files
- `GET /api/transfer-portal-comparison/info` - Get automation information

## Example Results

### Processing Statistics
- **Total Child Entries**: 1,953
- **Entries Found in Master**: 2
- **New Entries (Not in Master)**: 1,951

### Account Distribution (Top 5)
- GrowthOnics BPO: 476 entries
- Maverick: 259 entries
- Plexi: 228 entries
- Corebiz: 220 entries
- Vize BPO: 209 entries

### Pipeline Stage Distribution (Top 5)
- ACTIVE PLACED - Paid as Advanced: 406 entries
- DQ'd Can't be sold: 248 entries
- Approved Customer - Not Paid: 176 entries
- Needs to be Fixed: 164 entries
- Pending Approval: 144 entries

## Configuration

### Environment Variables
The automation uses account mappings from the `SUBACCOUNTS` environment variable:
```json
[
  {"id": "1", "name": "Test", "api_key": "..."},
  {"id": "2", "name": "Ark Tech", "api_key": "..."},
  ...
]
```

### Supported Account IDs
Currently supports 33 account mappings including:
- Test, Ark Tech, GrowthOnics BPO, Maverick
- Orbit Insurance x Omnitalk BPO, Vize BPO, Vyn BPO
- Cyberleads, Corebiz, Digicon, Ambition
- And many more...

## Technical Details

### Phone Normalization Algorithm
1. Remove all non-digit characters
2. Remove leading '1' if 11 digits total
3. Convert to standard format for comparison

### Encoding Support
- Primary: UTF-8
- Fallback: UTF-8-SIG, Latin-1, CP1252, ISO-8859-1
- Error handling for corrupted characters

### Performance
- Processes thousands of records efficiently
- Memory-optimized for large datasets
- Progress tracking and error reporting

## Error Handling
- File encoding detection and fallback
- Missing column validation
- Account mapping fallbacks
- Detailed error messages and logging

## Output Files
Results are saved to timestamped files:
- `transfer_portal_new_entries_YYYYMMDD_HHMMSS.csv`
- `transfer_portal_stats_YYYYMMDD_HHMMSS.json`

## Testing
Run the test script to validate functionality:
```bash
python test_transfer_portal_comparison.py
```

This automation provides a robust solution for managing transfer portal data comparison with intelligent matching and comprehensive reporting capabilities.
