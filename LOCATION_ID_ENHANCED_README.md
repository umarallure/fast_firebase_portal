# Bulk Update Pipeline & Stage (V2 API) - Enhanced with Location IDs

This automation allows you to bulk update opportunity pipelines, stages, lead values, and notes using the GHL V2 API with direct Opportunity IDs and proper Location ID support.

## Features

- **Direct Opportunity ID Updates**: No complex matching required - uses direct GHL Opportunity IDs
- **V2 API Integration**: Uses the latest GHL API for better reliability and performance 
- **Location ID Support**: Proper GHL location-specific API calls
- **Access Token Authentication**: Per-location access tokens for secure API access
- **Dynamic Pipeline/Stage Resolution**: Automatically resolves pipeline names and stage names to IDs
- **Bulk Processing**: Process multiple opportunities across multiple accounts
- **Enhanced Testing**: Built-in connection testing and account configuration loading
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
| `Account Id` | Your internal account identifier | Yes | `1` |

## Enhanced Configuration

### Subaccount Setup with Location IDs

You need to configure access tokens and location IDs for each account:

```json
SUBACCOUNTS=[
  {
    "id": "1",
    "name": "Account 1", 
    "location_id": "ve9EPM428h8vShlRW1KT",
    "api_key": "old_v1_key",
    "access_token": "your_v2_access_token_for_account_1"
  },
  {
    "id": "2",
    "name": "Account 2",
    "location_id": "ab3CDef567ghIJKlmnOP", 
    "api_key": "old_v1_key",
    "access_token": "your_v2_access_token_for_account_2"
  }
]
```

### Understanding Different IDs

- **Account ID**: Your internal identifier (1, 2, 3, 4) used in CSV files for mapping
- **Location ID**: GHL's unique location identifier (required for V2 API calls)
- **Access Token**: Per-location V2 API authentication token

### How to Get Location IDs

1. Log into your GHL account
2. Go to Settings → Company → Locations
3. Click on the specific location
4. The Location ID is visible in the location details or URL
5. Copy the alphanumeric string (e.g., "ve9EPM428h8vShlRW1KT")

### How to Get Access Tokens

1. In GHL, go to Settings → Integrations → API
2. Create a new API integration with these scopes:
   - `opportunities.write`
   - `contacts.write` (if updating notes)
   - `pipelines.readonly`
   - `locations.readonly`
3. **Important**: Make sure the integration is created for the specific location
4. Copy the generated access token
5. Add it to your subaccount configuration

## Enhanced Testing & Debugging

### Quick Account Configuration Loading

1. Go to the bulk update page: http://127.0.0.1:8000/bulk-update-pipeline-stage
2. In the "Test V2 API Connection" section:
   - Enter your Account ID (1, 2, 3, 4) in "Quick Load"
   - Click "Load Config" to auto-fill location ID from your configuration
   - Manually add your access token
   - Click "Test Connection" to verify everything works

### Connection Testing Features

- **Configuration Validation**: Checks if account is properly configured
- **API Connectivity**: Tests if access token is valid
- **Pipeline Discovery**: Shows available pipelines for the location
- **Error Diagnosis**: Provides specific error messages for troubleshooting

## API Details

The automation uses location-specific GHL V2 API endpoints:

```
GET https://services.leadconnectorhq.com/locations/{location_id}/pipelines
PUT https://services.leadconnectorhq.com/opportunities/{id}
```

### Headers Used

```
Authorization: Bearer {access_token}
Content-Type: application/json
Accept: application/json
Version: 2021-07-28
```

### Update Payload Structure

```json
{
  "pipelineId": "bCkKGpDsyPP4peuKowkG",
  "pipelineStageId": "7915dedc-8f18-44d5-8bc3-77c04e994a10", 
  "status": "open",
  "monetaryValue": 220
}
```

## Sample Configuration & Testing

Based on your test CSV with Account Id = 1:

1. **Configure Account 1**:
   ```json
   {
     "id": "1",
     "name": "Main Account",
     "location_id": "YOUR_LOCATION_ID_HERE",
     "api_key": "legacy_v1_key",
     "access_token": "YOUR_V2_ACCESS_TOKEN_HERE"
   }
   ```

2. **Test the Configuration**:
   - Use Account ID "1" in the quick load
   - Verify location ID loads correctly
   - Add your access token
   - Test connection to see available pipelines

3. **Expected Pipelines** (from your CSV):
   - "Customer Pipeline"
   - "Transfer Portal"

## Troubleshooting

### Common Issues

1. **404 Not Found on Pipelines**:
   - ✅ Check location ID is correct
   - ✅ Verify access token belongs to that location
   - ✅ Ensure token has `pipelines.readonly` scope

2. **401 Unauthorized**:
   - ✅ Regenerate access token
   - ✅ Check token hasn't expired
   - ✅ Verify integration has proper scopes

3. **403 Forbidden**:
   - ✅ Add missing scopes to your integration
   - ✅ Check location permissions

4. **Pipeline Not Found**:
   - ✅ Use exact pipeline names from test results
   - ✅ Check case sensitivity

### Using the Enhanced Management

1. **Subaccount Management**: http://127.0.0.1:8000/subaccounts
   - Add/edit/delete subaccounts
   - Configure location IDs and access tokens
   - Visual status indicators

2. **Bulk Update with Testing**: http://127.0.0.1:8000/bulk-update-pipeline-stage
   - Test connections before processing
   - Load account configurations quickly
   - Comprehensive error reporting

## Security

- Location IDs are partially masked in display for security
- Access tokens are never logged or displayed
- HTTP client connections are properly closed after each operation
- Temporary files are automatically cleaned up

This enhanced version provides better debugging, proper location ID support, and improved error handling for reliable V2 API operations.
