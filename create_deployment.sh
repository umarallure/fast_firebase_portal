#!/bin/bash

# Kixie Recorder - Deployment Script
# This script creates a deployment package

echo "📦 Creating deployment package..."

# Create deployment directory
DEPLOY_DIR="kixie-recorder-deploy"
mkdir -p "$DEPLOY_DIR"

# Copy files
cp setup_ubuntu.sh "$DEPLOY_DIR/"
cp upload_recordings.py "$DEPLOY_DIR/"
cp run_uploader.sh "$DEPLOY_DIR/"
cp config.ini "$DEPLOY_DIR/"
cp kixie-recorder.service "$DEPLOY_DIR/"
cp UBUNTU_DEPLOYMENT_README.md "$DEPLOY_DIR/README.md"

# Create directory structure
mkdir -p "$DEPLOY_DIR/data"
mkdir -p "$DEPLOY_DIR/credentials"
mkdir -p "$DEPLOY_DIR/logs"
mkdir -p "$DEPLOY_DIR/temp_recordings"

# Create placeholder files
echo "# Place your recordings CSV file here and rename to recordings.csv" > "$DEPLOY_DIR/data/README.txt"
echo "# Place your Google Drive credentials.json here" > "$DEPLOY_DIR/credentials/README.txt"

# Create tar archive
tar -czf kixie-recorder-deploy.tar.gz "$DEPLOY_DIR"

echo "✅ Deployment package created: kixie-recorder-deploy.tar.gz"
echo ""
echo "📤 To deploy on your Ubuntu server:"
echo "1. Upload kixie-recorder-deploy.tar.gz to your server"
echo "2. Extract: tar -xzf kixie-recorder-deploy.tar.gz"
echo "3. cd kixie-recorder-deploy"
echo "4. chmod +x setup_ubuntu.sh run_uploader.sh"
echo "5. ./setup_ubuntu.sh"
echo ""
echo "📁 Package contents:"
echo "├── setup_ubuntu.sh      - Ubuntu setup script"
echo "├── upload_recordings.py - Main Python script"
echo "├── run_uploader.sh      - Run script"
echo "├── config.ini          - Configuration file"
echo "├── README.md           - Documentation"
echo "├── kixie-recorder.service - Systemd service"
echo "├── data/               - For CSV files"
echo "├── credentials/        - For Google Drive credentials"
echo "├── logs/               - Log files"
echo "└── temp_recordings/    - Temporary files"