#!/usr/bin/env python3
"""
Terminal-based bulk opportunity owner update for Eric's deals
All opportunities will be assigned to owner ID: mx5CbluI9tvwxS9nQfL6
"""

import sys
import os
import asyncio
import json
from datetime import datetime

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

async def run_eric_deals_update():
    """Run bulk opportunity owner update for Eric's deals via terminal"""
    
    print("🚀 ERIC DEALS - BULK OPPORTUNITY OWNER UPDATE")
    print("=" * 60)
    print("📋 Target Owner: mx5CbluI9tvwxS9nQfL6 (ALL deals)")
    print()
    
    # Read Eric's CSV file
    csv_file_path = "ericdeals.csv"
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        print(f"✅ Successfully loaded CSV file: {csv_file_path}")
    except FileNotFoundError:
        print(f"❌ Error: Could not find {csv_file_path}")
        print("   Make sure the file is in the current directory")
        return
    except Exception as e:
        print(f"❌ Error reading CSV file: {str(e)}")
        return
    
    # Create service instance
    service = BulkOpportunityOwnerUpdateService()
    
    # Parse CSV
    print("\n📊 PARSING CSV FILE...")
    print("-" * 30)
    
    result = service.parse_csv(csv_content)
    
    if not result['success']:
        print(f"❌ Error parsing CSV: {result['error']}")
        return
    
    opportunities = result['opportunities']
    summary = result['summary']
    
    print(f"✅ CSV parsed successfully!")
    print(f"📈 Total opportunities: {summary['total_opportunities']}")
    print(f"🏢 Unique accounts: {summary['unique_accounts']}")
    print(f"👤 Target owner: {summary['assignment_distribution']['single_owner']['name']} ({summary['assignment_distribution']['single_owner']['id']})")
    print()
    
    # Ask for confirmation
    print("🔍 ASSIGNMENT PREVIEW (First 5 opportunities):")
    print("-" * 50)
    for i, opp in enumerate(opportunities[:5]):
        stage_badge = {
            'Needs Carrier Application': '📋',
            'Needs to be Fixed': '🔧',
        }.get(opp['current_stage'], '📊')
        
        print(f"{i+1}. {stage_badge} {opp['current_stage']:<25} → {opp['assigned_owner_name']}")
        print(f"   Opportunity: {opp['opportunity_name'][:60]}...")
        print(f"   Account: {opp['account_id']} | Pipeline: {opp['pipeline_id']}")
        print()
    
    if len(opportunities) > 5:
        print(f"   ... and {len(opportunities) - 5} more opportunities")
    print()
    
    # Confirm execution
    while True:
        choice = input("🤔 Do you want to proceed? [y/n/d] (y=yes, n=no, d=dry run): ").lower().strip()
        if choice in ['y', 'yes']:
            dry_run = False
            break
        elif choice in ['d', 'dry']:
            dry_run = True
            print("🧪 Running in DRY RUN mode (no actual updates)")
            break
        elif choice in ['n', 'no']:
            print("❌ Operation cancelled")
            return
        else:
            print("Please enter 'y' for yes, 'n' for no, or 'd' for dry run")
    
    # Start processing
    print(f"\n🚀 STARTING BULK UPDATE {'(DRY RUN)' if dry_run else '(LIVE)'}...")
    print("-" * 50)
    
    processing_result = await service.process_opportunity_owner_updates(
        opportunities=opportunities,
        dry_run=dry_run,
        batch_size=5  # Smaller batch size for terminal display
    )
    
    if not processing_result['success']:
        print(f"❌ Failed to start processing: {processing_result['message']}")
        return
    
    processing_id = processing_result['processing_id']
    print(f"✅ Processing started with ID: {processing_id}")
    print(f"📊 Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
    print()
    
    # Monitor progress
    print("📈 PROGRESS TRACKING:")
    print("-" * 30)
    
    last_completed = 0
    while True:
        progress_result = service.get_progress(processing_id)
        
        if not progress_result['success']:
            print(f"❌ Error getting progress: {progress_result['message']}")
            break
        
        progress = progress_result['progress']
        
        # Display progress
        completed = progress['completed']
        total = progress['total']
        success_count = progress['success_count']
        error_count = progress['error_count']
        status = progress['status']
        
        # Show new completions
        if completed > last_completed:
            percentage = (completed / total * 100) if total > 0 else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Progress: {completed}/{total} ({percentage:.1f}%) | ✅ {success_count} | ❌ {error_count}")
            
            if progress.get('rate'):
                print(f"   Rate: {progress['rate']}/min | ETA: {progress.get('eta', 'Calculating...')}")
            
            # Show recent errors if any
            if progress.get('recent_errors') and len(progress['recent_errors']) > 0:
                print(f"   ⚠️  Recent errors: {len(progress['recent_errors'])}")
                for error in progress['recent_errors'][-2:]:  # Show last 2 errors
                    print(f"      {error}")
            
            print()
            last_completed = completed
        
        # Check if completed
        if status == 'completed':
            print("🎉 PROCESSING COMPLETED!")
            print("=" * 30)
            print(f"📊 Final Results:")
            print(f"   Total processed: {completed}")
            print(f"   ✅ Successful: {success_count}")
            print(f"   ❌ Errors: {error_count}")
            print(f"   📈 Success rate: {(success_count/completed*100):.1f}%" if completed > 0 else "N/A")
            
            if dry_run:
                print(f"\n🧪 DRY RUN COMPLETE - No actual updates were made")
                print(f"   All {success_count} opportunities would be assigned to owner: mx5CbluI9tvwxS9nQfL6")
            else:
                print(f"\n✅ LIVE UPDATE COMPLETE")
                print(f"   {success_count} opportunities successfully assigned to owner: mx5CbluI9tvwxS9nQfL6")
            
            break
        elif status == 'failed':
            print(f"❌ Processing failed with status: {status}")
            break
        
        # Wait before next check
        await asyncio.sleep(2)
    
    print(f"\n📝 Operation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        asyncio.run(run_eric_deals_update())
    except KeyboardInterrupt:
        print("\n⚠️  Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
