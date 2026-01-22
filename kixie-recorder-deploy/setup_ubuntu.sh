#!/bin/bash

# Kixie Recording Uploader - Ubuntu Setup Script
# This script sets up the environment for the Kixie recording uploader

set -e

echo "=== Kixie Recording Uploader Setup ==="
echo "Setting up Ubuntu server for automated recording uploads..."

# Update system packages
echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Python 3.7+ and pip
echo "Installing Python and pip..."
sudo apt install -y python3 python3-pip python3-venv

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib requests tqdm

# Create directory structure
echo "Creating directory structure..."
mkdir -p data
mkdir -p credentials
mkdir -p logs
mkdir -p temp_recordings

# Set permissions
echo "Setting permissions..."
chmod +x run_uploader.sh
chmod +x setup_ubuntu.sh

# Create systemd service (optional)
echo "Setting up systemd service..."
sudo cp kixie-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "Next steps:"
echo "1. Place your recordings CSV file in the 'data' folder and rename it to 'recordings.csv'"
echo "2. Place your Google Drive credentials.json in the 'credentials' folder"
echo "3. Edit config.ini if needed for custom settings"
echo "4. Run './run_uploader.sh' to start uploading recordings"
echo "5. Optional: Enable the systemd service with 'sudo systemctl enable kixie-recorder'"
echo ""
echo "For detailed instructions, see README.md"