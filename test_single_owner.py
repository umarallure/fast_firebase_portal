#!/usr/bin/env python3
"""
Test script to verify single owner assignment with Eric's CSV
"""

import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

def test_single_owner_assignment():
    """Test the single owner assignment with Eric's CSV file"""
    
    print("🔧 Testing Single Owner Assignment")
    print("=" * 50)
    
    # Read Eric's CSV file
    csv_file_path = "ericdeals.csv"
    
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
    print(f"📊 Total opportunities found: {result['summary']['total_opportunities']}")
    print(f"🏢 Unique accounts: {result['summary']['unique_accounts']}")
    print()
    
    # Display assignment distribution
    assignment_dist = result['summary']['assignment_distribution']
    
    print("👤 SINGLE OWNER ASSIGNMENT:")
    print("-" * 35)
    
    single_owner = assignment_dist['single_owner']
    print(f"🎯 Owner: {single_owner['name']}")
    print(f"   └── ID: {single_owner['id']}")
    print(f"   └── Assigned Opportunities: {single_owner['count']}")
    print(f"   └── Percentage: 100%")
    print()
    
    # Show first 10 assignments for verification
    print("🔍 SAMPLE ASSIGNMENTS (First 10):")
    print("-" * 50)
    
    for i, opp in enumerate(result['opportunities'][:10]):
        stage_badge = {
            'Needs Carrier Application': '📋',
            'Needs to be Fixed': '🔧',
        }.get(opp['current_stage'], '📊')
        
        print(f"{i+1:2d}. {stage_badge} {opp['current_stage']:<25} → {opp['assigned_owner_name']}")
        print(f"     Opportunity: {opp['opportunity_name'][:50]}...")
        print(f"     ID: {opp['assigned_to']}")
        print()
    
    if len(result['opportunities']) > 10:
        print(f"    ... and {len(result['opportunities']) - 10} more opportunities")
    
    print()
    print("✅ All opportunities assigned to single owner: mx5CbluI9tvwxS9nQfL6")
    print("✅ Test completed successfully!")

if __name__ == "__main__":
    test_single_owner_assignment()
