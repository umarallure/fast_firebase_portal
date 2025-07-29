#!/usr/bin/env python3
"""
Terminal-based bulk opportunity owner update for assigned_pending_lapse.csv
Distribution: Bryan 40%, Kyla 30%, Ira 30%
"""

import sys
import os
import asyncio
import json
from datetime import datetime

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

async def run_pending_lapse_update():
    """Run bulk opportunity owner update for pending lapse deals via terminal"""
    
    print("🚀 PENDING LAPSE DEALS - BULK OPPORTUNITY OWNER UPDATE")
    print("=" * 65)
    print("📋 Three-Agent Distribution:")
    print("   Bryan: 40% (ID: 1iulHgfbKF7ufdpt6osS)")
    print("   Kyla:  30% (ID: GkBzcbgHkCNaoSKKdIkZ)")
    print("   Ira:   30% (ID: 3swinpnwu3nIQh3NrmTX)")
    print()
    
    # Read the new CSV file
    csv_file_path = "assigned_pending_lapse.csv"
    
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
    print()
    
    # Display distribution
    assignment_dist = summary['assignment_distribution']
    
    print("👥 THREE-AGENT DISTRIBUTION:")
    print("-" * 45)
    
    bryan = assignment_dist['bryan']
    kyla = assignment_dist['kyla']
    ira = assignment_dist['ira']
    
    print(f"🎯 Bryan:  {bryan['count']:3d} opportunities ({bryan['percentage']:4.1f}%)")
    print(f"   └── Name: {bryan['name']}")
    print(f"   └── ID: {bryan['id']}")
    print()
    print(f"🎯 Kyla:   {kyla['count']:3d} opportunities ({kyla['percentage']:4.1f}%)")
    print(f"   └── Name: {kyla['name']}")
    print(f"   └── ID: {kyla['id']}")
    print()
    print(f"🎯 Ira:    {ira['count']:3d} opportunities ({ira['percentage']:4.1f}%)")
    print(f"   └── Name: {ira['name']}")
    print(f"   └── ID: {ira['id']}")
    print()
    
    # Verify total
    total_assigned = bryan['count'] + kyla['count'] + ira['count']
    print(f"📊 Total assigned: {total_assigned} (should equal {summary['total_opportunities']})")
    
    if total_assigned == summary['total_opportunities']:
        print("✅ Distribution total matches perfectly!")
    else:
        print("❌ Distribution total mismatch!")
    
    print()
    
    # Show sample assignments for each agent
    bryan_opps = [o for o in opportunities if o['assigned_to'] == bryan['id']]
    kyla_opps = [o for o in opportunities if o['assigned_to'] == kyla['id']]
    ira_opps = [o for o in opportunities if o['assigned_to'] == ira['id']]
    
    print("🔍 SAMPLE ASSIGNMENTS (First 3 for each agent):")
    print("-" * 60)
    
    print("📋 Bryan's Opportunities:")
    for i, opp in enumerate(bryan_opps[:3]):
        print(f"   {i+1}. 🔄 {opp['current_stage']:<20} | {opp['opportunity_name'][:45]}...")
    if len(bryan_opps) > 3:
        print(f"      ... and {len(bryan_opps) - 3} more")
    print()
    
    print("📋 Kyla's Opportunities:")
    for i, opp in enumerate(kyla_opps[:3]):
        print(f"   {i+1}. 🔄 {opp['current_stage']:<20} | {opp['opportunity_name'][:45]}...")
    if len(kyla_opps) > 3:
        print(f"      ... and {len(kyla_opps) - 3} more")
    print()
    
    print("📋 Ira's Opportunities:")
    for i, opp in enumerate(ira_opps[:3]):
        print(f"   {i+1}. 🔄 {opp['current_stage']:<20} | {opp['opportunity_name'][:45]}...")
    if len(ira_opps) > 3:
        print(f"      ... and {len(ira_opps) - 3} more")
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
                print(f"   Bryan would get: {bryan['count']} opportunities")
                print(f"   Kyla would get: {kyla['count']} opportunities")
                print(f"   Ira would get: {ira['count']} opportunities")
            else:
                print(f"\n✅ LIVE UPDATE COMPLETE")
                print(f"   Bryan assigned: {bryan['count']} opportunities")
                print(f"   Kyla assigned: {kyla['count']} opportunities")
                print(f"   Ira assigned: {ira['count']} opportunities")
            
            break
        elif status == 'failed':
            print(f"❌ Processing failed with status: {status}")
            break
        
        # Wait before next check
        await asyncio.sleep(2)
    
    print(f"\n📝 Operation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        asyncio.run(run_pending_lapse_update())
    except KeyboardInterrupt:
        print("\n⚠️  Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
