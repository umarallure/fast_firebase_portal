# Kixie Recording Uploader - Ubuntu Server

This is a packaged version of the Kixie recording uploader for easy deployment on Ubuntu servers.

## Quick Start

### 1. Initial Setup (One-time)

```bash
# Download and run the setup script
wget https://raw.githubusercontent.com/umarallure/fast_firebase_portal/main/setup_ubuntu.sh
chmod +x setup_ubuntu.sh
./setup_ubuntu.sh
```

### 2. Configure Google Drive

1. Place your `credentials.json` file in the `credentials/` folder
2. The first run will prompt you to authenticate with Google Drive

### 3. Upload Your CSV File

Place your Kixie recordings CSV file in the `data/` folder and rename it to `recordings.csv`

### 4. Run the Uploader

```bash
./run_uploader.sh
```

## Directory Structure

```
kixie-recorder/
├── config.ini              # Configuration file
├── upload_recordings.py    # Main Python script
├── run_uploader.sh         # Run script
├── venv/                   # Python virtual environment
├── data/                   # Place your CSV files here
├── credentials/            # Google Drive credentials
├── logs/                   # Log files
├── temp_recordings/        # Temporary download files
└── kixie-recorder.service  # Systemd service file (optional)
```

## Configuration

Edit `config.ini` to customize settings:

```ini
[UPLOADER]
csv_file = data/recordings.csv
temp_dir = temp_recordings
drive_folder = Kixie Recordings
max_workers = 2
credentials_file = credentials/credentials.json
log_file = logs/recording_upload.log
```

## Automatic Operation (Optional)

### Using systemd

1. Edit the service file:
   ```bash
   nano kixie-recorder.service
   ```
   Replace `YOUR_USERNAME` with your actual username.

2. Install the service:
   ```bash
   sudo cp kixie-recorder.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable kixie-recorder
   ```

3. Start the service:
   ```bash
   sudo systemctl start kixie-recorder
   ```

4. Check status:
   ```bash
   sudo systemctl status kixie-recorder
   ```

### Using Cron (Alternative)

Add to crontab for scheduled runs:

```bash
crontab -e
```

Add this line to run every 6 hours:
```
0 */6 * * * cd /home/YOUR_USERNAME/kixie-recorder && ./run_uploader.sh
```

## Monitoring

- Check logs: `tail -f logs/recording_upload.log`
- View results: `ls upload_results_*.csv`
- Check Google Drive folder for uploaded files

## Troubleshooting

### Common Issues

1. **"credentials.json not found"**
   - Place your Google Drive credentials in `credentials/credentials.json`

2. **"CSV file not found"**
   - Place your recordings CSV in `data/recordings.csv`

3. **Authentication issues**
   - Delete `credentials/token.json` and re-run to re-authenticate

4. **Permission issues**
   - Make sure the user has write access to all directories

### Logs

All logs are saved to `logs/recording_upload.log`. Check this file for detailed error information.

## Performance

- **Recommended workers**: 1-3 for most servers
- **Expected speed**: ~2-3 seconds per recording
- **Memory usage**: ~50MB base + temp files
- **Network**: Stable internet connection required

## Security

- Keep `credentials/` folder secure
- Don't commit credentials to version control
- Use strong server passwords
- Consider using SSH keys for access

## Support

For issues:
1. Check the log files
2. Verify CSV format matches expected columns
3. Ensure Google Drive API is enabled
4. Test with small CSV files first