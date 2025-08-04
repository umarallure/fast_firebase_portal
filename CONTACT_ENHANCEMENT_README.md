# Contact Enhancement Scripts

This collection of scripts helps you fetch additional contact data from GoHighLevel API based on contact IDs in your CSV file, creating a webhook-ready CSV with all required fields.

## Files Created

1. **`webhook_contact_enhancer.py`** - Main enhancement script
2. **`test_contact_enhancement.py`** - Detailed enhancement with all fields
3. **`run_contact_enhancement_test.py`** - Simple test runner
4. **`contact_enhancement_env_example.txt`** - Environment variables example

## Setup Instructions

### 1. Environment Configuration

Create a `.env` file in the project root with your GoHighLevel API credentials:

```env
# Copy from contact_enhancement_env_example.txt and fill in your values
GHL_CHILD_LOCATION_API_KEY=your_api_key_here
GHL_MASTER_LOCATION_API_KEY=your_master_api_key_here

# Optional: Subaccounts JSON format
SUBACCOUNTS=[{"id": "subaccount1", "api_key": "key1"}, {"id": "subaccount2", "api_key": "key2"}]
```

### 2. Install Dependencies

Make sure you have the required packages:

```bash
pip install pandas httpx python-dotenv
```

### 3. Verify Your CSV

Ensure your `database.csv` has the required columns:
- `contact_id` - GoHighLevel contact ID
- `opportunity_name`
- `full_name`
- `phone`
- `pipeline_id`
- `current_stage`
- `source`
- `opportunity_status`
- `center`
- `ghl_id`

## Usage

### Quick Test (Recommended)

Run the simple test script to enhance the first 5 contacts:

```bash
python run_contact_enhancement_test.py
```

This will:
1. Check your environment setup
2. Process first 5 contacts from `database.csv`
3. Create a webhook-ready CSV file

### Advanced Enhancement

For more detailed enhancement with all available fields:

```bash
python test_contact_enhancement.py
```

### Webhook-Ready Enhancement

For creating CSV specifically formatted for your webhook:

```bash
python webhook_contact_enhancer.py
```

## Output

The scripts will create CSV files with timestamps:
- `webhook_ready_contacts_YYYYMMDD_HHMMSS.csv` - Webhook formatted data
- `enhanced_contacts_test_YYYYMMDD_HHMMSS.csv` - Full enhancement data

## Webhook Fields Mapping

The script maps GoHighLevel contact data to these webhook fields:

### Required Fields
- `full_name` - Contact name
- `email` - Contact email
- `phone` - Contact phone
- `center` - From CSV data
- `pipeline_id` - From CSV data
- `to_stage` - Current stage from CSV
- `ghl_id` - GoHighLevel ID

### Optional Fields (Auto-mapped from custom fields)
- `date_of_birth` - Birth date
- `age` - Age
- `social_security_number` - SSN
- `height` - Height
- `weight` - Weight
- `doctors_name` - Doctor information
- `tobacco_user` - Tobacco usage
- `health_conditions` - Health conditions
- `medications` - Medications
- `monthly_premium` - Premium amount
- `coverage_amount` - Coverage amount
- `carrier` - Insurance carrier
- `address`, `city`, `state`, `postal_code` - Address info
- `driver_license_number` - License number
- And more...

## Custom Field Mapping

The script automatically attempts to map GoHighLevel custom fields to webhook fields based on field names. For example:

- Fields containing "birth", "dob" → `date_of_birth`
- Fields containing "age" → `age`
- Fields containing "ssn", "social" → `social_security_number`
- Fields containing "tobacco", "smoke" → `tobacco_user`
- etc.

## Troubleshooting

### Common Issues

1. **"No API key found"**
   - Check your `.env` file exists
   - Verify API key variable names
   - Ensure API keys are valid

2. **"Contact fetch failed"**
   - Verify contact IDs exist in GoHighLevel
   - Check API key permissions
   - Ensure correct subaccount/location

3. **"CSV file not found"**
   - Ensure `database.csv` is in the project root
   - Check file name spelling

### Debug Mode

Add logging to see detailed API responses:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Testing Single Contact

To test with a specific contact ID:

```python
# In webhook_contact_enhancer.py, modify the test_df line:
test_df = df[df['contact_id'] == 'your_specific_contact_id']
```

## API Endpoints Used

The scripts use these GoHighLevel API endpoints:

1. **Get Contact Details**
   ```
   GET /v1/contacts/{contact_id}
   ```

2. **Get Custom Fields**
   ```
   GET /v1/custom-fields/
   ```

## Security Notes

- Keep your API keys secure and never commit them to version control
- Use environment variables for all sensitive configuration
- Test with a small number of contacts first
- Respect GoHighLevel API rate limits

## Next Steps

After generating the enhanced CSV:

1. Review the output file for data quality
2. Map any unmapped custom fields manually if needed
3. Use the CSV data to call your webhook endpoint
4. Scale up to process all contacts once testing is successful

## Support

If you encounter issues:

1. Check the console output for specific error messages
2. Verify your API credentials and permissions
3. Test with a single contact first
4. Check GoHighLevel API documentation for any changes
