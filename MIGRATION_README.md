# GoHighLevel Migration System

A comprehensive automation system for migrating contacts, custom fields, and opportunities between GoHighLevel accounts using the V1 API.

## Features

### 🚀 Smart Migration System
- **Intelligent Mapping**: Automatically maps similar custom fields, pipelines, and stages between accounts
- **Contact Migration**: Transfers contacts with deduplication based on email/phone
- **Opportunity Migration**: Moves opportunities with proper pipeline and stage mapping
- **Custom Fields**: Creates missing custom fields and maps existing ones
- **Pipeline Management**: Maps stages between accounts with intelligent matching

### 🧠 Smart Mapping Strategy
- **Similarity Matching**: Uses text similarity algorithms to match fields and stages
- **Common Mappings**: Pre-defined mappings for common field and stage names
- **Type Validation**: Ensures custom field types match between accounts
- **Readiness Assessment**: Analyzes migration compatibility before execution

### 📊 Real-time Dashboard
- **Connection Testing**: Verify API connectivity to both accounts
- **Migration Preview**: See what data will be migrated before starting
- **Smart Analysis**: Get detailed mapping analysis and readiness scores
- **Progress Tracking**: Monitor migration progress in real-time
- **History**: View past migration attempts and results

## Setup

### Environment Variables

Add these to your `.env` file:

```bash
# Child Account (Source)
GHL_CHILD_LOCATION_ID=your_child_location_id
GHL_CHILD_LOCATION_API_KEY=your_child_api_key

# Master Account (Destination)
GHL_MASTER_LOCATION_ID=your_master_location_id
GHL_MASTER_LOCATION_API_KEY=your_master_api_key

# Migration Settings
MIGRATION_BATCH_SIZE=50
MIGRATION_RATE_LIMIT_DELAY=0.1
```

### API Keys

Get your API keys from GoHighLevel:
1. Go to Settings → API Key in each account
2. Copy the Location API Key (Bearer token)
3. Add to your environment variables

## Usage

### 1. Web Dashboard

Navigate to `/migration` in your browser to access the migration dashboard.

#### Dashboard Features:
- **Test Connections**: Verify API connectivity
- **Preview Migration**: See data counts and migration plan
- **Smart Analysis**: Get intelligent mapping analysis
- **Start Migration**: Begin the migration process
- **Monitor Progress**: Track migration in real-time

### 2. API Endpoints

#### Test Connections
```bash
GET /api/v1/migration/test-connection
```

#### Preview Migration Data
```bash
GET /api/v1/migration/preview
```

#### Smart Analysis
```bash
GET /api/v1/migration/analyze
```

#### Start Migration
```bash
POST /api/v1/migration/start-env
```

#### Check Migration Status
```bash
GET /api/v1/migration/status/{migration_id}
```

### 3. Command Line Testing

Run the test script to verify your setup:

```bash
python test_migration.py
```

This will test:
- API connections to both accounts
- Smart mapping functionality
- Data preview capabilities

## Migration Process

### Step 1: Custom Fields
- Fetches custom fields from both accounts
- Maps similar fields using intelligent matching
- Creates missing fields in master account
- Builds field ID mapping for data migration

### Step 2: Pipelines & Stages
- Fetches pipelines and stages from both accounts
- Maps similar pipelines and stages
- Creates missing stages in existing pipelines
- Builds pipeline/stage mapping for opportunities

### Step 3: Contacts
- Fetches all contacts from child account
- Optimizes processing order (email contacts first)
- Checks for duplicates in master account
- Creates new contacts with mapped custom fields
- Builds contact ID mapping for opportunities

### Step 4: Opportunities
- Fetches all opportunities from child account
- Maps to master contacts, pipelines, and stages
- Creates opportunities with mapped custom fields
- Tracks creation success/failure

## Smart Mapping Features

### Field Mapping
- **Exact Name Match**: Direct mapping for identical field names
- **Similarity Matching**: 80%+ similarity threshold for auto-mapping
- **Common Field Types**: Pre-defined mappings for standard fields like "Industry", "Budget", etc.
- **Type Validation**: Ensures field types match (TEXT, SELECT, etc.)

### Pipeline/Stage Mapping
- **Pipeline Matching**: Maps pipelines by name similarity
- **Stage Intelligence**: Recognizes common stage names like "Lead", "Qualified", "Closed Won"
- **Position Preservation**: Maintains stage order and positions
- **Missing Stage Creation**: Creates stages that don't exist in master account

### Contact Optimization
- **Priority Processing**: Processes contacts with emails first (easier deduplication)
- **Duplicate Detection**: Checks for existing contacts by email and phone
- **Data Completeness**: Prioritizes contacts with complete information

## Migration Readiness

The system provides a readiness score based on:

- **Pipeline Readiness**: % of child pipelines that can be mapped
- **Field Readiness**: % of custom fields that can be mapped
- **Overall Score**: Combined readiness percentage

### Readiness Levels:
- **High (80%+)**: Ready for migration
- **Medium (60-79%)**: Proceed with caution
- **Low (<60%)**: Review mapping issues first

## API Documentation

The system includes built-in API documentation for GHL endpoints:

```bash
GET /api/v1/migration/docs          # Complete documentation
GET /api/v1/migration/docs/contacts # Contact-specific docs
GET /api/v1/migration/docs/pipelines # Pipeline-specific docs
```

## Error Handling

### Common Issues:
1. **API Key Invalid**: Check that keys are correct and have proper permissions
2. **Rate Limiting**: System automatically handles rate limits with delays
3. **Missing Pipelines**: Create required pipelines in master account first
4. **Field Type Mismatches**: Manually resolve field type conflicts

### Logging:
- All migration activities are logged to `ghl_migration.log`
- Console output shows real-time progress
- Web dashboard displays live status updates

## Security

- API keys are stored securely in environment variables
- No data is permanently stored (migration runs in memory)
- Rate limiting prevents API abuse
- Error handling prevents data corruption

## Performance

### Batch Processing:
- Contacts processed in configurable batch sizes (default: 50)
- Concurrent requests with rate limiting
- Optimized processing order for better success rates

### Rate Limiting:
- Configurable delays between requests (default: 0.1s)
- Automatic retry on rate limit errors
- Exponential backoff for failed requests

## Troubleshooting

### Connection Issues:
1. Verify API keys are correct
2. Check location IDs match your accounts
3. Ensure accounts have proper permissions

### Mapping Issues:
1. Run smart analysis to identify problems
2. Check pipeline and field name similarities
3. Create missing pipelines/stages manually if needed

### Migration Failures:
1. Check logs for specific error messages
2. Verify all mappings are complete
3. Test with smaller batch sizes

## Support

For issues with the migration system:
1. Check the logs in `ghl_migration.log`
2. Run the test script to diagnose problems
3. Use the smart analysis feature to identify mapping issues
4. Review the API documentation for endpoint details

---

## GoHighLevel V1 API Reference

### Base URL
```
https://rest.gohighlevel.com/v1
```

### Authentication
```
Authorization: Bearer {LOCATION_API_KEY}
```

### Key Endpoints Used:
- `/v1/custom-fields` - Custom field management
- `/v1/contacts` - Contact management  
- `/v1/opportunities` - Opportunity management
- `/v1/pipelines` - Pipeline and stage management

### Rate Limits:
- Implement delays between requests
- Monitor for 429 (Too Many Requests) responses
- Use exponential backoff for retries
