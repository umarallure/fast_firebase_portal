#!/usr/bin/env python3
"""
Test script to verify stage-based opportunity owner assignment
"""

import csv
import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

def test_stage_distribution():
    """Test the stage-based distribution with the actual CSV file"""
    
    print("🔧 Testing Stage-Based Opportunity Owner Assignment")
    print("=" * 60)
    
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
    print(f"📊 Total opportunities found: {result['summary']['total_opportunities']}")
    print(f"🏢 Unique accounts: {result['summary']['unique_accounts']}")
    print()
    
    # Display stage distribution
    stage_dist = result['summary']['stage_distribution']
    assignment_dist = result['summary']['assignment_distribution']
    
    print("📈 STAGE DISTRIBUTION:")
    print("-" * 30)
    
    if 'First Draft Payment Failure' in stage_dist:
        fdpf = stage_dist['First Draft Payment Failure']
        print(f"💥 First Draft Payment Failure: {fdpf['total']} opportunities")
        print(f"   └── Bryan: {fdpf['bryan']} ({fdpf['bryan']/fdpf['total']*100:.1f}%) - Target: 40%")
        print(f"   └── Ira: {fdpf['ira']} ({fdpf['ira']/fdpf['total']*100:.1f}%) - Target: 20%")
        print(f"   └── Kyla: {fdpf['kyla']} ({fdpf['kyla']/fdpf['total']*100:.1f}%) - Target: 20%")
        print()
    
    if 'Pending Lapse' in stage_dist:
        pl = stage_dist['Pending Lapse']
        print(f"⏳ Pending Lapse: {pl['total']} opportunities")
        print(f"   └── Ira: {pl['ira']} ({pl['ira']/pl['total']*100:.1f}%) - Target: 50%")
        print(f"   └── Kyla: {pl['kyla']} ({pl['kyla']/pl['total']*100:.1f}%) - Target: 50%")
        print()
    
    if 'Other Stages' in stage_dist and stage_dist['Other Stages']['total'] > 0:
        other = stage_dist['Other Stages']
        print(f"📊 Other Stages: {other['total']} opportunities")
        print(f"   └── Alternating assignment between Ira and Kyla")
        print()
    
    print("👥 OVERALL ASSIGNMENT DISTRIBUTION:")
    print("-" * 40)
    
    for owner in ['ira', 'kyla', 'bryan']:
        if owner in assignment_dist:
            info = assignment_dist[owner]
            percentage = (info['count'] / result['summary']['total_opportunities']) * 100
            print(f"👤 {info['name']}: {info['count']} opportunities ({percentage:.1f}%)")
            print(f"   └── ID: {info['id']}")
    
    print()
    
    # Show first 10 assignments for verification
    print("🔍 SAMPLE ASSIGNMENTS (First 10):")
    print("-" * 50)
    
    for i, opp in enumerate(result['opportunities'][:10]):
        stage_badge = {
            'First Draft Payment Failure': '💥',
            'Pending Lapse': '⏳',
        }.get(opp['current_stage'], '📊')
        
        print(f"{i+1:2d}. {stage_badge} {opp['current_stage']:<25} → {opp['assigned_owner_name']}")
    
    if len(result['opportunities']) > 10:
        print(f"    ... and {len(result['opportunities']) - 10} more opportunities")
    
    print()
    print("✅ Test completed successfully!")

if __name__ == "__main__":
    test_stage_distribution()
