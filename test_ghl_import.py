"""
Test script to validate GHL API connection and pipeline/stage mapping
Run this before the full import to ensure everything is configured correctly
"""

import requests
import json
import sys
from typing import Dict, List
from import_csv_to_ghl import GHLImporter

def test_ghl_connection(api_token: str, location_id: str) -> bool:
    """
    Test basic connection to GHL API
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Test with pipelines endpoint
        url = "https://rest.gohighlevel.com/v1/pipelines/"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            print("✅ GHL API connection successful!")
            return True
        else:
            print(f"❌ GHL API connection failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing GHL connection: {str(e)}")
        return False

def get_pipelines_and_stages(api_token: str) -> Dict:
    """
    Fetch and display all pipelines and stages
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        url = "https://rest.gohighlevel.com/v1/pipelines/"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            pipelines_data = response.json()
            
            print("\n📋 Available Pipelines and Stages:")
            print("=" * 50)
            
            for pipeline in pipelines_data.get('pipelines', []):
                print(f"\n🔹 Pipeline: {pipeline['name']} (ID: {pipeline['id']})")
                for stage in pipeline.get('stages', []):
                    print(f"   └─ Stage: {stage['name']} (ID: {stage['id']})")
            
            return pipelines_data
        else:
            print(f"❌ Failed to fetch pipelines: {response.status_code} - {response.text}")
            return {}
            
    except Exception as e:
        print(f"❌ Error fetching pipelines: {str(e)}")
        return {}

def analyze_csv_data(csv_file_path: str) -> Dict:
    """
    Analyze the CSV file and show unique values for key fields
    """
    import csv
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            
            pipelines = set()
            stages = set()
            sources = set()
            statuses = set()
            assigned_users = set()
            
            total_rows = 0
            rows_with_email = 0
            rows_with_phone = 0
            rows_with_both = 0
            
            for row in csv_reader:
                total_rows += 1
                
                # Collect unique values
                if row.get('pipeline'):
                    pipelines.add(row['pipeline'])
                if row.get('stage'):
                    stages.add(row['stage'])
                if row.get('source'):
                    sources.add(row['source'])
                if row.get('status'):
                    statuses.add(row['status'])
                if row.get('assigned'):
                    assigned_users.add(row['assigned'])
                
                # Check contact info
                has_email = bool(row.get('email', '').strip())
                has_phone = bool(row.get('phone', '').strip())
                
                if has_email:
                    rows_with_email += 1
                if has_phone:
                    rows_with_phone += 1
                if has_email and has_phone:
                    rows_with_both += 1
            
            print(f"\n📊 CSV Analysis:")
            print("=" * 50)
            print(f"Total rows: {total_rows}")
            print(f"Rows with email: {rows_with_email}")
            print(f"Rows with phone: {rows_with_phone}")
            print(f"Rows with both: {rows_with_both}")
            print(f"Rows missing contact info: {total_rows - (rows_with_email + rows_with_phone - rows_with_both)}")
            
            print(f"\nUnique Pipelines in CSV: {sorted(pipelines)}")
            print(f"Unique Stages in CSV: {sorted(stages)}")
            print(f"Unique Sources in CSV: {sorted(sources)}")
            print(f"Unique Statuses in CSV: {sorted(statuses)}")
            print(f"Unique Assigned Users in CSV: {sorted(assigned_users)}")
            
            return {
                'total_rows': total_rows,
                'pipelines': pipelines,
                'stages': stages,
                'sources': sources,
                'statuses': statuses,
                'assigned_users': assigned_users
            }
            
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file_path}")
        return {}
    except Exception as e:
        print(f"❌ Error analyzing CSV: {str(e)}")
        return {}

def validate_pipeline_stage_mapping(csv_data: Dict, ghl_data: Dict) -> bool:
    """
    Validate that CSV pipelines and stages exist in GHL
    """
    print(f"\n🔍 Validating Pipeline/Stage Mapping:")
    print("=" * 50)
    
    # Create lookup for GHL pipelines and stages
    ghl_pipelines = {}
    ghl_stages = {}
    
    for pipeline in ghl_data.get('pipelines', []):
        pipeline_name = pipeline['name']
        ghl_pipelines[pipeline_name] = pipeline['id']
        
        for stage in pipeline.get('stages', []):
            stage_key = f"{pipeline_name}|{stage['name']}"
            ghl_stages[stage_key] = stage['id']
    
    # Check CSV pipelines
    missing_pipelines = []
    for pipeline in csv_data.get('pipelines', []):
        if pipeline not in ghl_pipelines:
            missing_pipelines.append(pipeline)
        else:
            print(f"✅ Pipeline '{pipeline}' found in GHL")
    
    # Check CSV stages
    missing_stages = []
    for pipeline in csv_data.get('pipelines', []):
        for stage in csv_data.get('stages', []):
            stage_key = f"{pipeline}|{stage}"
            if stage_key not in ghl_stages:
                missing_stages.append(stage_key)
            else:
                print(f"✅ Stage '{stage}' in pipeline '{pipeline}' found in GHL")
    
    if missing_pipelines:
        print(f"\n❌ Missing Pipelines in GHL: {missing_pipelines}")
    
    if missing_stages:
        print(f"\n❌ Missing Stages in GHL: {missing_stages}")
    
    if not missing_pipelines and not missing_stages:
        print(f"\n✅ All pipelines and stages are valid!")
        return True
    else:
        print(f"\n❌ Some pipelines or stages are missing in GHL")
        return False

def main():
    """
    Main test function
    """
    print("🧪 GHL Import Test Script")
    print("=" * 50)
    
    # Configuration - UPDATE THESE VALUES
    API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsb2NhdGlvbl9pZCI6IlNUWXc0RnBvVnViejBCbHlBT3EyIiwidmVyc2lvbiI6MSwiaWF0IjoxNzU2OTA1OTE0ODc1LCJzdWIiOiJEakNNRlVDbVdISjFORTNaUDRITCJ9.8HcmXyBxrwyWgGvLhsAfmU-U84eIUTl49NdzX4Wpxt8"  # Your actual API token
    LOCATION_ID = "STYw4FpoVubz0BlyAOq2"  # Your actual location ID
    CSV_FILE_PATH = r"c:\Users\Dell\Downloads\original_opportunities.csv"
    
    # Initialize importer
    importer = GHLImporter(API_TOKEN, LOCATION_ID)
    
    # Test 1: API Connection
    print("\n1️⃣ Testing GHL API Connection...")
    if not test_ghl_connection(API_TOKEN, LOCATION_ID):
        print("❌ Cannot proceed without valid API connection")
        return
    
    # Test 2: Fetch Pipelines and Stages
    print("\n2️⃣ Fetching Pipelines and Stages...")
    ghl_data = get_pipelines_and_stages(API_TOKEN)
    if not ghl_data:
        print("❌ Cannot proceed without pipeline data")
        return
    
    # Test 3: Analyze CSV
    print("\n3️⃣ Analyzing CSV Data...")
    csv_data = analyze_csv_data(CSV_FILE_PATH)
    if not csv_data:
        print("❌ Cannot proceed without CSV data")
        return
    
    # Test 4: Validate Mapping
    print("\n4️⃣ Validating Pipeline/Stage Mapping...")
    mapping_valid = validate_pipeline_stage_mapping(csv_data, ghl_data)
    
    # Final Summary
    print(f"\n📋 Test Summary:")
    print("=" * 50)
    print(f"✅ API Connection: Working")
    print(f"✅ Pipelines/Stages: Fetched ({len(ghl_data.get('pipelines', []))} pipelines)")
    print(f"✅ CSV Analysis: Complete ({csv_data.get('total_rows', 0)} rows)")
    print(f"{'✅' if mapping_valid else '❌'} Pipeline/Stage Mapping: {'Valid' if mapping_valid else 'Issues Found'}")
    
    if mapping_valid:
        print(f"\n🎉 All tests passed! You can proceed with the import.")
    else:
        print(f"\n⚠️ Please fix the pipeline/stage mapping issues before importing.")

if __name__ == "__main__":
    main()
