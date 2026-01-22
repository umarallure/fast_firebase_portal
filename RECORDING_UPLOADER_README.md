# Kixie Recording Downloader & Google Drive Uploader

This script downloads call recordings from Kixie (from CSV export) and uploads them to Google Drive.

## Prerequisites

1. **Python 3.7+** installed
2. **Google Cloud Project** with Drive API enabled
3. **OAuth credentials** from Google Cloud Console

## Setup Instructions

### 1. Install Required Packages

```bash
pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client requests tqdm
```

### 2. Set Up Google Drive API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the **Google Drive API**:
   - Go to "APIs & Services" > "Library"
   - Search for "Google Drive API"
   - Click "Enable"
4. Create OAuth 2.0 credentials:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - Choose "Desktop app" as application type
   - Download the credentials file
   - Rename it to `credentials.json`
   - Place it in the same folder as this script

### 3. Prepare Your CSV File

- Export your call data from Kixie
- Make sure it has these columns:
  - `Call Recording` or `Voicemail Recording` (URLs)
  - `Internal Call ID`
  - `Date`
  - `From Number`
  - `To Number`
- Save it as `samplerecordingdownload.csv` (or update the filename in the script)

## Usage

### Basic Usage

```bash
python download_upload_recordings.py
```

### First Time Running

1. The script will open a browser window
2. Sign in with your Google account
3. Grant permissions to access Google Drive
4. A `token.json` file will be created (reused for future runs)

### Configuration

Edit these variables in the `main()` function:

```python
CSV_FILE = 'samplerecordingdownload.csv'  # Your CSV file path
TEMP_DIR = 'temp_recordings'              # Temporary download folder
DRIVE_FOLDER = 'Kixie Recordings'         # Google Drive folder name
MAX_WORKERS = 5                            # Parallel downloads (1-10 recommended)
```

## Features

- ✅ **Parallel Processing**: Downloads and uploads multiple files simultaneously
- ✅ **Progress Tracking**: Real-time progress bar and statistics
- ✅ **Error Handling**: Automatic retries with exponential backoff
- ✅ **Resume Support**: Skips already processed files (use results CSV)
- ✅ **Logging**: Detailed logs saved to `recording_upload.log`
- ✅ **Results Export**: Creates CSV with Drive links for each recording
- ✅ **Memory Efficient**: Streams downloads and auto-cleans temp files

## Performance

For **30,000 recordings**:
- Estimated time: 8-15 hours (depending on file sizes and internet speed)
- Recommended: Run overnight or on a server
- Adjust `MAX_WORKERS` based on your internet bandwidth:
  - Slow connection (< 10 Mbps): `MAX_WORKERS = 2-3`
  - Medium connection (10-50 Mbps): `MAX_WORKERS = 5`
  - Fast connection (> 50 Mbps): `MAX_WORKERS = 8-10`

## Output Files

1. **`upload_results_YYYYMMDD_HHMMSS.csv`**: 
   - Contains Drive links for all uploaded recordings
   - Columns: file_name, drive_id, drive_link, original_url

2. **`recording_upload.log`**: 
   - Detailed execution logs
   - Error messages and retry attempts

3. **`token.json`**: 
   - Google OAuth token (auto-generated)
   - Don't share this file!

## Troubleshooting

### "credentials.json not found"
- Download OAuth credentials from Google Cloud Console
- Rename to `credentials.json`
- Place in the same folder as the script

### "Rate limit exceeded"
- Google Drive has upload quotas
- Script automatically retries with backoff
- Reduce `MAX_WORKERS` if issues persist

### Downloads failing
- Check internet connection
- Some Kixie URLs may expire
- Check `recording_upload.log` for specific errors

### Slow performance
- Increase `MAX_WORKERS` for faster uploads
- Check internet bandwidth
- Run on a server with better connection

## Advanced Usage

### Resume from Previous Run

If the script stops, you can modify it to skip already uploaded files:

1. Check the latest `upload_results_*.csv`
2. Filter out completed rows from your source CSV
3. Run the script again with the filtered CSV

### Custom Naming

To change filename format, edit the `process_recording()` function:

```python
file_name = f"{date_str}_{from_number}_to_{to_number}_{call_id}.mp3"
```

### Different Audio Formats

If recordings are not MP3, update the mimetype:

```python
media = MediaFileUpload(
    file_path,
    mimetype='audio/wav',  # or 'audio/ogg', etc.
    resumable=True
)
```

## Security Notes

- Keep `credentials.json` and `token.json` private
- Don't commit them to version control
- Add them to `.gitignore`:
  ```
  credentials.json
  token.json
  temp_recordings/
  *.log
  upload_results_*.csv
  ```

## Support

For issues or questions:
1. Check `recording_upload.log` for errors
2. Verify Google Drive API is enabled
3. Check CSV format matches expected columns
4. Ensure internet connection is stable

## License

This script is provided as-is for internal use.
