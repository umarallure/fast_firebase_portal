#!/usr/bin/env python3
"""
Process opportunity owner updates from CSV file
"""

import sys
import os
import asyncio
import json

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

async def process_opportunities():
    """Process opportunity owner updates"""
    
    print("🔄 Processing Opportunity Owner Updates")
    print("=" * 50)
    
    # Read the CSV file
    csv_file_path = "assigned_deals_document.csv"
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
    except FileNotFoundError:
        print(f"❌ Error: Could not find {csv_file_path}")
        return
    
    # Create service instance and parse CSV
    service = BulkOpportunityOwnerUpdateService()
    result = service.parse_csv(csv_content)
    
    if not result['success']:
        print(f"❌ Error parsing CSV: {result['error']}")
        return
    
    print(f"✅ Successfully parsed CSV file")
    print(f"📊 Total opportunities: {result['summary']['total_opportunities']}")
    
    # Show distribution summary
    stage_dist = result['summary']['stage_distribution']
    
    print("\n📈 ASSIGNMENT SUMMARY:")
    print("-" * 30)
    
    if 'First Draft Payment Failure' in stage_dist:
        fdpf = stage_dist['First Draft Payment Failure']
        print(f"💥 First Draft Payment Failure: {fdpf['total']} opportunities")
        print(f"   └── Bryan: {fdpf['bryan']} ({fdpf['bryan']/fdpf['total']*100:.1f}%)")
        print(f"   └── Ira: {fdpf['ira']} ({fdpf['ira']/fdpf['total']*100:.1f}%)")
        print(f"   └── Kyla: {fdpf['kyla']} ({fdpf['kyla']/fdpf['total']*100:.1f}%)")
    
    if 'Pending Lapse' in stage_dist:
        pl = stage_dist['Pending Lapse']
        print(f"⏳ Pending Lapse: {pl['total']} opportunities")
        print(f"   └── Ira: {pl['ira']} ({pl['ira']/pl['total']*100:.1f}%)")
        print(f"   └── Kyla: {pl['kyla']} ({pl['kyla']/pl['total']*100:.1f}%)")
    
    # Ask user for confirmation
    print(f"\n🤔 Do you want to process these {result['summary']['total_opportunities']} opportunities?")
    print("   1. Dry Run (simulate only, no actual updates)")
    print("   2. Live Run (actual GHL API updates)")
    print("   3. Cancel")
    
    choice = input("\nEnter your choice (1/2/3): ").strip()
    
    if choice == "3":
        print("❌ Operation cancelled by user")
        return
    elif choice not in ["1", "2"]:
        print("❌ Invalid choice")
        return
    
    dry_run = (choice == "1")
    mode = "DRY RUN" if dry_run else "LIVE RUN"
    
    print(f"\n🚀 Starting {mode}...")
    print("-" * 30)
    
    # Start processing
    process_result = await service.process_opportunity_owner_updates(
        opportunities=result['opportunities'],
        dry_run=dry_run,
        batch_size=10
    )
    
    if not process_result['success']:
        print(f"❌ Failed to start processing: {process_result['message']}")
        return
    
    processing_id = process_result['processing_id']
    print(f"✅ Processing started with ID: {processing_id}")
    
    # Monitor progress
    print("\n📊 Monitoring progress...")
    while True:
        await asyncio.sleep(2)  # Check every 2 seconds
        
        progress_result = service.get_progress(processing_id)
        if not progress_result['success']:
            print("❌ Error getting progress")
            break
        
        progress = progress_result['progress']
        
        # Calculate percentage
        percentage = (progress['completed'] / progress['total'] * 100) if progress['total'] > 0 else 0
        
        print(f"Progress: {progress['completed']}/{progress['total']} ({percentage:.1f}%) - "
              f"Success: {progress['success_count']}, Errors: {progress['error_count']}")
        
        if progress['status'] == 'completed':
            print("\n✅ Processing completed!")
            print(f"📈 Final Results:")
            print(f"   └── Total: {progress['total']}")
            print(f"   └── Successful: {progress['success_count']}")
            print(f"   └── Failed: {progress['error_count']}")
            
            if progress['recent_errors']:
                print(f"\n❌ Recent errors:")
                for error in progress['recent_errors'][-5:]:  # Show last 5 errors
                    print(f"   └── {error}")
            
            break
        elif progress['status'] == 'failed':
            print("\n❌ Processing failed!")
            break

if __name__ == "__main__":
    asyncio.run(process_opportunities())
