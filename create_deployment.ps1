# Kixie Recorder - Deployment Script (PowerShell)
# This script creates a deployment package

Write-Host "📦 Creating deployment package..." -ForegroundColor Green

# Create deployment directory
$DEPLOY_DIR = "kixie-recorder-deploy"
if (Test-Path $DEPLOY_DIR) {
    Remove-Item -Recurse -Force $DEPLOY_DIR
}
New-Item -ItemType Directory -Path $DEPLOY_DIR | Out-Null

# Copy files
Copy-Item "setup_ubuntu.sh" "$DEPLOY_DIR/"
Copy-Item "upload_recordings.py" "$DEPLOY_DIR/"
Copy-Item "run_uploader.sh" "$DEPLOY_DIR/"
Copy-Item "config.ini" "$DEPLOY_DIR/"
Copy-Item "kixie-recorder.service" "$DEPLOY_DIR/"
Copy-Item "UBUNTU_DEPLOYMENT_README.md" "$DEPLOY_DIR/README.md"

# Create directory structure
New-Item -ItemType Directory -Path "$DEPLOY_DIR/data" | Out-Null
New-Item -ItemType Directory -Path "$DEPLOY_DIR/credentials" | Out-Null
New-Item -ItemType Directory -Path "$DEPLOY_DIR/logs" | Out-Null
New-Item -ItemType Directory -Path "$DEPLOY_DIR/temp_recordings" | Out-Null

# Create placeholder files
"# Place your recordings CSV file here and rename to recordings.csv" | Out-File -FilePath "$DEPLOY_DIR/data/README.txt"
"# Place your Google Drive credentials.json here" | Out-File -FilePath "$DEPLOY_DIR/credentials/README.txt"

# Create tar archive (using 7zip if available, otherwise just create folder)
$tarFile = "kixie-recorder-deploy.tar.gz"
if (Get-Command "7z" -ErrorAction SilentlyContinue) {
    Write-Host "Using 7-Zip to create archive..." -ForegroundColor Yellow
    & 7z a -ttar "$tarFile" "$DEPLOY_DIR/*"
} elseif (Get-Command "tar" -ErrorAction SilentlyContinue) {
    Write-Host "Using tar to create archive..." -ForegroundColor Yellow
    & tar -czf "$tarFile" "$DEPLOY_DIR"
} else {
    Write-Host "⚠️  Neither 7z nor tar found. Creating folder-only package." -ForegroundColor Yellow
    Write-Host "You can manually create the tar.gz archive on your Ubuntu server." -ForegroundColor Yellow
}

Write-Host "✅ Deployment package created!" -ForegroundColor Green
Write-Host ""
Write-Host "📤 To deploy on your Ubuntu server:" -ForegroundColor Cyan
Write-Host "1. Upload kixie-recorder-deploy.tar.gz to your server" -ForegroundColor White
Write-Host "2. Extract: tar -xzf kixie-recorder-deploy.tar.gz" -ForegroundColor White
Write-Host "3. cd kixie-recorder-deploy" -ForegroundColor White
Write-Host "4. chmod +x setup_ubuntu.sh run_uploader.sh" -ForegroundColor White
Write-Host "5. ./setup_ubuntu.sh" -ForegroundColor White
Write-Host ""
Write-Host "📁 Package contents:" -ForegroundColor Cyan
Write-Host "├── setup_ubuntu.sh      - Ubuntu setup script" -ForegroundColor White
Write-Host "├── upload_recordings.py - Main Python script" -ForegroundColor White
Write-Host "├── run_uploader.sh      - Run script" -ForegroundColor White
Write-Host "├── config.ini          - Configuration file" -ForegroundColor White
Write-Host "├── README.md           - Documentation" -ForegroundColor White
Write-Host "├── kixie-recorder.service - Systemd service" -ForegroundColor White
Write-Host "├── data/               - For CSV files" -ForegroundColor White
Write-Host "├── credentials/        - For Google Drive credentials" -ForegroundColor White
Write-Host "├── logs/               - Log files" -ForegroundColor White
Write-Host "└── temp_recordings/    - Temporary files" -ForegroundColor White