# GHL Import Configuration
# Copy this file to config.py and update with your actual values

# GHL API Configuration
GHL_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsb2NhdGlvbl9pZCI6IlNUWXc0RnBvVnViejBCbHlBT3EyIiwidmVyc2lvbiI6MSwiaWF0IjoxNzU2OTA1OTE0ODc1LCJzdWIiOiJEakNNRlVDbVdISjFORTNaUDRITCJ9.8HcmXyBxrwyWgGvLhsAfmU-U84eIUTl49NdzX4Wpxt8"
GHL_LOCATION_ID = "YOUR_LOCATION_ID_HERE"

# CSV File Configuration
CSV_FILE_PATH = r"c:\Users\Dell\Downloads\original_opportunities.csv"

# Import Settings
DELAY_BETWEEN_REQUESTS = 1.5  # Seconds between API calls to avoid rate limiting
BATCH_SIZE = 10  # Number of records to process before taking a longer break

# Logging Configuration
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FILE = "ghl_csv_import.log"

# Field Mappings (if your CSV has different column names)
FIELD_MAPPINGS = {
    'opportunity_name': 'Opportunity Name',
    'customer_name': 'Customer Name',
    'phone': 'phone',
    'email': 'email',
    'pipeline': 'pipeline',
    'stage': 'stage',
    'lead_value': 'Lead Value',
    'source': 'source',
    'assigned': 'assigned',
    'notes': 'Notes',
    'tags': 'tags',
    'status': 'status'
}
