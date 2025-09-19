# GHL CSV Import Tool

This tool imports contact and opportunity data from a CSV file into GoHighLevel (GHL) using their REST API.

## Features

- 🔄 **Automatic Pipeline/Stage Resolution**: Dynamically fetches pipelines and stages from GHL API
- 👥 **Contact Creation**: Creates contacts with proper name parsing and validation
- 🎯 **Opportunity Creation**: Creates opportunities linked to contacts with proper stage mapping
- 🏷️ **Tag Support**: Handles comma-separated tags
- 📝 **Comprehensive Logging**: Detailed logs for debugging and monitoring
- ⚡ **Rate Limiting**: Built-in delays to respect GHL API limits
- 🧪 **Test Script**: Validation script to check configuration before import

## Prerequisites

1. **GHL API Access**: You need a GHL API token and location ID
2. **Python 3.7+**: Make sure Python is installed
3. **Required Python packages**: Install using `pip install -r ghl_import_requirements.txt`

## CSV Format

Your CSV should have the following columns:

| Column | Description | Required |
|--------|-------------|----------|
| Opportunity Name | Name of the opportunity | Yes |
| Customer Name | Full name of the customer | Yes |
| phone | Phone number | Yes* |
| email | Email address | Yes* |
| pipeline | Pipeline name (must match GHL) | Yes |
| stage | Stage name (must match GHL) | Yes |
| Lead Value | Monetary value (numeric) | No |
| source | Lead source | No |
| assigned | Assigned user ID | No |
| Notes | Notes for the opportunity | No |
| tags | Comma-separated tags | No |
| status | open/won/lost/abandoned | No |

*Either phone or email is required (or both)

## Quick Start

### 1. Install Dependencies

```bash
pip install -r ghl_import_requirements.txt
```

### 2. Configure API Credentials

Edit the configuration in the main scripts:

```python
# In import_csv_to_ghl.py and test_ghl_import.py
API_TOKEN = "your_actual_ghl_api_token_here"
LOCATION_ID = "your_actual_location_id_here"
CSV_FILE_PATH = r"path_to_your_csv_file.csv"
```

### 3. Test Your Configuration

Before running the full import, test your setup:

```bash
python test_ghl_import.py
```

This will:
- ✅ Test API connection
- 📋 Show available pipelines and stages
- 📊 Analyze your CSV data
- 🔍 Validate pipeline/stage mappings

### 4. Run the Import

If tests pass, run the full import:

```bash
python import_csv_to_ghl.py
```

## File Descriptions

### Core Files

- **`import_csv_to_ghl.py`**: Main import script
- **`test_ghl_import.py`**: Configuration validation script
- **`config_template.py`**: Configuration template with field mappings
- **`ghl_import_requirements.txt`**: Python dependencies

### Log Files (Generated)

- **`ghl_csv_import.log`**: Detailed import logs

## Script Features

### GHLImporter Class

The main `GHLImporter` class provides:

```python
class GHLImporter:
    def __init__(self, api_token, location_id)
    def get_pipelines()           # Fetch all pipelines and stages
    def create_contact(row)       # Create a contact from CSV row
    def create_opportunity(row, contact_id)  # Create opportunity
    def import_csv(csv_file_path) # Main import function
```

### Key Functions

1. **Pipeline/Stage Resolution**:
   - Automatically fetches all pipelines and stages
   - Caches them for quick lookup
   - Maps CSV pipeline/stage names to GHL IDs

2. **Contact Creation**:
   - Parses full names into first/last name
   - Validates email/phone requirements
   - Handles tags and custom fields

3. **Opportunity Creation**:
   - Links to created contact
   - Maps to correct pipeline/stage
   - Handles monetary values and status

4. **Error Handling**:
   - Comprehensive logging
   - Continues processing even if individual records fail
   - Provides detailed error messages

## Configuration Options

### Rate Limiting

Adjust delay between API calls:

```python
# In import_csv_to_ghl.py, main() function
stats = importer.import_csv(CSV_FILE_PATH, delay_seconds=1.5)
```

### Field Mappings

If your CSV has different column names, update the field mappings in `config_template.py`:

```python
FIELD_MAPPINGS = {
    'opportunity_name': 'Your_Opportunity_Column',
    'customer_name': 'Your_Customer_Column',
    # ... etc
}
```

## Troubleshooting

### Common Issues

1. **"Pipeline not found"**:
   - Run `test_ghl_import.py` to see available pipelines
   - Ensure pipeline names in CSV exactly match GHL

2. **"Stage not found"**:
   - Check that stage names match exactly (case-sensitive)
   - Verify the stage belongs to the correct pipeline

3. **"Contact creation failed"**:
   - Ensure each row has either email or phone
   - Check for valid email formats

4. **Rate limiting errors**:
   - Increase `delay_seconds` parameter
   - Consider processing in smaller batches

### Debug Mode

For detailed debugging, modify the logging level:

```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## API Reference

The script uses these GHL API endpoints:

- `GET /v1/pipelines/` - Fetch pipelines and stages
- `POST /v1/contacts/` - Create contacts  
- `POST /v1/pipelines/{pipelineId}/opportunities/` - Create opportunities

## Success Metrics

After import, the script reports:

- 📊 Total rows processed
- ✅ Contacts created successfully  
- ❌ Contacts that failed
- 🎯 Opportunities created successfully
- ⚠️ Opportunities that failed
- 📈 Overall success rate

## Security Notes

- Never commit API tokens to version control
- Use environment variables for production
- Regularly rotate API tokens
- Monitor API usage and limits

## Support

For issues:

1. Check the log file (`ghl_csv_import.log`)
2. Run the test script to validate configuration
3. Verify CSV format matches requirements
4. Check GHL API documentation for any changes

## Sample Output

```
2025-09-03 10:30:15 - INFO - Starting CSV import to GHL...
2025-09-03 10:30:16 - INFO - Successfully fetched 3 pipelines
2025-09-03 10:30:17 - INFO - Processing row 1: David Johnston
2025-09-03 10:30:18 - INFO - Successfully created contact: David Johnston (ID: abc123)
2025-09-03 10:30:19 - INFO - Successfully created opportunity: David Johnston (ID: xyz789)
...
2025-09-03 10:35:42 - INFO - Import completed!
2025-09-03 10:35:42 - INFO - Statistics:
2025-09-03 10:35:42 - INFO -   Total rows processed: 47
2025-09-03 10:35:42 - INFO -   Contacts created: 45
2025-09-03 10:35:42 - INFO -   Contacts failed: 2
2025-09-03 10:35:42 - INFO -   Opportunities created: 45
2025-09-03 10:35:42 - INFO -   Opportunities failed: 0
2025-09-03 10:35:42 - INFO -   Success rate: 95.7%
```
