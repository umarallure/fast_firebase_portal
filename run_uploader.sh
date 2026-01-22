#!/bin/bash

# Kixie Recording Uploader - Run Script
# This script runs the recording uploader

set -e

echo "🚀 Starting Kixie Recording Uploader"
echo "===================================="

# Check if we're in the right directory
if [ ! -f "config.ini" ]; then
    echo "❌ Error: config.ini not found. Please run this script from the kixie-recorder directory."
    exit 1
fi

# Check if CSV file exists
CSV_FILE=$(grep "^csv_file" config.ini | cut -d'=' -f2 | sed 's/^[[:space:]]*//')
if [ ! -f "$CSV_FILE" ]; then
    echo "❌ Error: CSV file '$CSV_FILE' not found."
    echo "Please place your recordings CSV file in the data/ folder."
    exit 1
fi

# Check if credentials exist
if [ ! -f "credentials/credentials.json" ]; then
    echo "❌ Error: credentials.json not found."
    echo "Please place your Google Drive credentials.json file in the credentials/ folder."
    exit 1
fi

# Activate virtual environment
echo "🌐 Activating virtual environment..."
source venv/bin/activate

# Run the uploader
echo "📤 Starting upload process..."
python download_upload_recordings.py

echo "✅ Upload process completed!"