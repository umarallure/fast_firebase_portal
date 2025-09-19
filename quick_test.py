"""
Quick test to verify API credentials and basic functionality
"""

from import_csv_to_ghl import GHLImporter

def quick_test():
    # Your credentials
    API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsb2NhdGlvbl9pZCI6IlNUWXc0RnBvVnViejBCbHlBT3EyIiwidmVyc2lvbiI6MSwiaWF0IjoxNzU2OTA1OTE0ODc1LCJzdWIiOiJEakNNRlVDbVdISjFORTNaUDRITCJ9.8HcmXyBxrwyWgGvLhsAfmU-U84eIUTl49NdzX4Wpxt8"
    LOCATION_ID = "STYw4FpoVubz0BlyAOq2"
    
    print("🧪 Quick GHL API Test")
    print("=" * 30)
    
    # Initialize importer
    importer = GHLImporter(API_TOKEN, LOCATION_ID)
    
    # Test API connection by fetching pipelines
    print("Testing API connection...")
    pipelines_data = importer.get_pipelines()
    
    if pipelines_data:
        print("✅ API connection successful!")
        print(f"Found {len(pipelines_data.get('pipelines', []))} pipelines:")
        
        for pipeline in pipelines_data.get('pipelines', []):
            print(f"  📋 {pipeline['name']} (ID: {pipeline['id']})")
            for stage in pipeline.get('stages', []):
                print(f"    └─ {stage['name']} (ID: {stage['id']})")
    else:
        print("❌ API connection failed!")
        return False
    
    # Test pipeline/stage lookup with your CSV data
    print(f"\n🔍 Testing pipeline/stage lookup...")
    
    # Test with data from your CSV
    test_pipeline = "Booked Calls"  # From your CSV
    test_stages = ["Booked", "Appointment No Show", "Appointment Sat/Qualified", "Appointment Sat/Unqualified"]
    
    for stage in test_stages:
        stage_id, pipeline_id = importer.get_stage_id(test_pipeline, stage)
        if stage_id and pipeline_id:
            print(f"✅ Found: {test_pipeline} -> {stage}")
        else:
            print(f"❌ Missing: {test_pipeline} -> {stage}")
    
    print(f"\n✅ Test completed! Ready to import your CSV.")
    return True

if __name__ == "__main__":
    quick_test()
