# Bulk Update Pipeline & Stage (V2 API)

This automation allows you to bulk update opportunity pipelines, stages, lead values, and notes using the GHL V2 API with direct Opportunity IDs.

## Features

- **Direct Opportunity ID Updates**: No complex matching required - uses direct GHL Opportunity IDs
- **V2 API Integration**: Uses the latest GHL API for better reliability and performance 
- **Access Token Authentication**: Per-account access tokens for secure API access
- **Dynamic Pipeline/Stage Resolution**: Automatically resolves pipeline names and stage names to IDs
- **Bulk Processing**: Process multiple opportunities across multiple accounts
- **Comprehensive Error Handling**: Detailed error reporting and failed CSV downloads
- **Progress Tracking**: Real-time progress updates and detailed results

## CSV Format

Your CSV file must contain these columns:

| Column | Description | Required | Example |
|--------|-------------|----------|---------|
| `Opportunity ID` | Direct GHL Opportunity ID | Yes | `TF6F15FjZe37qasTHE1d` |
| `pipeline` | Target pipeline name | Yes | `Customer Pipeline` |
| `stage` | Target stage name | Yes | `Needs Carrier Application` |
| `Lead Value` | Monetary value to update | Yes | `150.00` |
| `Notes` | Notes to add to contact | No | `Policy status updated` |
| `Account Id` | Subaccount ID for token mapping | Yes | `2` |

## Configuration

### Access Token Setup

You need to configure access tokens for each account in your environment variables:

```json
SUBACCOUNTS=[
  {
    "id": "2",
    "name": "Account 2", 
    "api_key": "old_v1_key",
    "access_token": "your_v2_access_token_for_account_2"
  },
  {
    "id": "3",
    "name": "Account 3",
    "api_key": "old_v1_key", 
    "access_token": "your_v2_access_token_for_account_3"
  }
]
```

### How to Get Access Tokens

1. Log into your GHL account
2. Go to Settings > Integrations > API
3. Create a new API integration with these scopes:
   - `opportunities.write`
   - `contacts.write` (if updating notes)
   - `pipelines.readonly`
4. Copy the generated access token
5. Add it to your SUBACCOUNTS configuration

## API Details

The automation uses the GHL V2 API endpoint:

```
PUT https://services.leadconnectorhq.com/opportunities/{id}
```

With the following payload structure:

```json
{
  "pipelineId": "bCkKGpDsyPP4peuKowkG",
  "pipelineStageId": "7915dedc-8f18-44d5-8bc3-77c04e994a10", 
  "status": "open",
  "monetaryValue": 220
}
```

## Sample CSV

```csv
Opportunity ID,pipeline,stage,Lead Value,Notes,Account Id
TF6F15FjZe37qasTHE1d,Customer Pipeline,Needs Carrier Application,150.00,Policy status showing Not Taken,2
gez1VxzgYNBQYAf1hwlX,Customer Pipeline,Pending Lapse,250.50,Updated status from automation,2
42aAxkvtr9XrTF32x6YS,Transfer Portal,ACTIVE PLACED - Paid as Advanced,500.00,Moved to transfer portal,2
```

## Error Handling

- **Missing Access Token**: If an account doesn't have an access token configured, those opportunities will be skipped
- **Invalid Pipeline/Stage**: If pipeline or stage names don't exist, detailed errors are provided
- **API Errors**: Full API error responses are logged and reported
- **Failed CSV Download**: Download a CSV of any failed entries for troubleshooting

## Logging

All operations are logged to `ghl_update_v2.log` with detailed information about:
- API requests and responses
- Pipeline/stage resolution
- Success/failure status
- Error details

## Security

- Uses Bearer token authentication
- Access tokens are never logged
- HTTP client connections are properly closed after each account
- Temporary files are cleaned up automatically
