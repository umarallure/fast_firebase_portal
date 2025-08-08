# Enhanced Subaccount Management

## Overview

The subaccount management page has been enhanced to support both V1 API keys and V2 access tokens, with full CRUD (Create, Read, Update, Delete) operations.

## Features

### ✨ **New Capabilities**

1. **Access Token Support**: Add V2 API access tokens for new automations
2. **Edit Existing Subaccounts**: Update any subaccount configuration
3. **Delete Subaccounts**: Remove unused subaccount configurations
4. **Visual Token Status**: See which accounts have V2 tokens configured
5. **Improved Security**: Partial display of sensitive API keys

### 🔧 **API Endpoints**

- `GET /api/subaccounts` - List all subaccounts
- `POST /api/subaccounts` - Add new subaccount
- `PUT /api/subaccounts/{id}` - Update existing subaccount
- `DELETE /api/subaccounts/{id}` - Delete subaccount

### 📊 **Data Structure**

Each subaccount now supports:

```json
{
  "id": "2",
  "name": "Account 2",
  "api_key": "v1_api_key_here",
  "access_token": "v2_access_token_here"
}
```

### 🌐 **UI Features**

#### Table View
- **ID**: Subaccount identifier
- **Name**: Human-readable account name
- **API Key**: Truncated display for security
- **Access Token**: Badge showing configuration status
- **Actions**: Edit and Delete buttons

#### Form Features
- **Add Mode**: Create new subaccounts
- **Edit Mode**: Modify existing subaccounts
- **Field Validation**: Required field checking
- **Help Text**: Guidance for each field

#### Visual Indicators
- 🟢 **Green Badge**: V2 access token configured
- 🟡 **Yellow Badge**: V2 access token missing
- **Truncated Keys**: Security through partial display

### 🔒 **Security Features**

1. **Partial Key Display**: Only shows first 20 characters + "..." for API keys
2. **Access Token Status**: Shows configured/missing without exposing values
3. **Validation**: Prevents duplicate IDs
4. **Error Handling**: Graceful handling of file read/write errors

### 📝 **Usage Instructions**

#### Adding New Subaccount
1. Fill in all required fields
2. Optionally add V2 access token
3. Click "Save"

#### Editing Existing Subaccount
1. Click "Edit" button for any subaccount
2. Modify fields as needed
3. Click "Update"
4. Use "Cancel Edit" to abort changes

#### Deleting Subaccount
1. Click "Delete" button
2. Confirm deletion in popup
3. Subaccount removed from configuration

#### Getting V2 Access Tokens
1. Login to your GHL account
2. Go to Settings → Integrations → API
3. Create new integration with scopes:
   - `opportunities.write`
   - `contacts.write`
   - `pipelines.readonly`
4. Copy the generated access token
5. Add/edit subaccount to include token

### 🔄 **Environment Integration**

All changes are automatically saved to your `.env` file:

```env
SUBACCOUNTS='[{"id":"2","name":"Account 2","api_key":"v1_key","access_token":"v2_token"}]'
```

The configuration is immediately available to all automations without restart.

### 🎯 **Benefits**

- **Centralized Management**: All account configurations in one place
- **Backward Compatibility**: V1 API keys continue to work
- **Future Ready**: V2 tokens enable new features
- **User Friendly**: Intuitive interface with clear guidance
- **Secure**: Safe handling of sensitive credentials

### 🚀 **Integration with V2 Automations**

The new "Bulk Update Pipeline & Stage" automation automatically uses V2 access tokens when available, providing:

- Better API reliability
- Enhanced error handling
- Improved performance
- Future-proof functionality

Access the enhanced subaccount management at: **http://127.0.0.1:8000/subaccounts**
