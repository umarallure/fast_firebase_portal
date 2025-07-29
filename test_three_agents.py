#!/usr/bin/env python3
"""
Test script to verify three-agent distribution for opportunity owner updates
Bryan: 40%, Kyla: 30%, Ira: 30%
"""

import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

def test_three_agent_distribution():
    """Test the three-agent distribution system"""
    
    print("🚀 THREE-AGENT OPPORTUNITY DISTRIBUTION TEST")
    print("=" * 60)
    print("📋 Distribution Ratios:")
    print("   Bryan: 40% (ID: 1iulHgfbKF7ufdpt6osS)")
    print("   Kyla:  30% (ID: GkBzcbgHkCNaoSKKdIkZ)")
    print("   Ira:   30% (ID: 3swinpnwu3nIQh3NrmTX)")
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
    
    # Create service instance and parse CSV
    service = BulkOpportunityOwnerUpdateService()
    result = service.parse_csv(csv_content)
    
    if not result['success']:
        print(f"❌ Error parsing CSV: {result['error']}")
        return
    
    opportunities = result['opportunities']
    summary = result['summary']
    
    print(f"\n📊 PARSING RESULTS:")
    print("-" * 30)
    print(f"✅ CSV parsed successfully!")
    print(f"📈 Total opportunities: {summary['total_opportunities']}")
    print(f"🏢 Unique accounts: {summary['unique_accounts']}")
    print()
    
    # Display distribution
    assignment_dist = summary['assignment_distribution']
    
    print("👥 AGENT DISTRIBUTION:")
    print("-" * 40)
    
    bryan = assignment_dist['bryan']
    kyla = assignment_dist['kyla']
    ira = assignment_dist['ira']
    
    print(f"🎯 Bryan:  {bryan['count']:3d} opportunities ({bryan['percentage']:4.1f}%)")
    print(f"   └── ID: {bryan['id']}")
    print()
    print(f"🎯 Kyla:   {kyla['count']:3d} opportunities ({kyla['percentage']:4.1f}%)")
    print(f"   └── ID: {kyla['id']}")
    print()
    print(f"🎯 Ira:    {ira['count']:3d} opportunities ({ira['percentage']:4.1f}%)")
    print(f"   └── ID: {ira['id']}")
    print()
    
    # Verify total
    total_assigned = bryan['count'] + kyla['count'] + ira['count']
    print(f"📊 Total assigned: {total_assigned} (should equal {summary['total_opportunities']})")
    
    if total_assigned == summary['total_opportunities']:
        print("✅ Distribution total matches!")
    else:
        print("❌ Distribution total mismatch!")
    
    print()
    
    # Show sample assignments for each agent
    bryan_opps = [o for o in opportunities if o['assigned_to'] == bryan['id']]
    kyla_opps = [o for o in opportunities if o['assigned_to'] == kyla['id']]
    ira_opps = [o for o in opportunities if o['assigned_to'] == ira['id']]
    
    print("🔍 SAMPLE ASSIGNMENTS (First 3 for each agent):")
    print("-" * 55)
    
    print("📋 Bryan's Opportunities:")
    for i, opp in enumerate(bryan_opps[:3]):
        stage_badge = {
            'Needs Carrier Application': '📋',
            'Needs to be Fixed': '🔧',
        }.get(opp['current_stage'], '📊')
        print(f"   {i+1}. {stage_badge} {opp['current_stage']:<25} | {opp['opportunity_name'][:40]}...")
    if len(bryan_opps) > 3:
        print(f"      ... and {len(bryan_opps) - 3} more")
    print()
    
    print("📋 Kyla's Opportunities:")
    for i, opp in enumerate(kyla_opps[:3]):
        stage_badge = {
            'Needs Carrier Application': '📋',
            'Needs to be Fixed': '🔧',
        }.get(opp['current_stage'], '📊')
        print(f"   {i+1}. {stage_badge} {opp['current_stage']:<25} | {opp['opportunity_name'][:40]}...")
    if len(kyla_opps) > 3:
        print(f"      ... and {len(kyla_opps) - 3} more")
    print()
    
    print("📋 Ira's Opportunities:")
    for i, opp in enumerate(ira_opps[:3]):
        stage_badge = {
            'Needs Carrier Application': '📋',
            'Needs to be Fixed': '🔧',
        }.get(opp['current_stage'], '📊')
        print(f"   {i+1}. {stage_badge} {opp['current_stage']:<25} | {opp['opportunity_name'][:40]}...")
    if len(ira_opps) > 3:
        print(f"      ... and {len(ira_opps) - 3} more")
    print()
    
    # Expected vs Actual for 314 opportunities
    expected_bryan = round(314 * 0.40)
    expected_kyla = round(314 * 0.30)
    expected_ira = 314 - expected_bryan - expected_kyla
    
    print("📊 EXPECTED vs ACTUAL DISTRIBUTION (for 314 opportunities):")
    print("-" * 60)
    print(f"Bryan: Expected {expected_bryan:3d} | Actual {bryan['count']:3d} | Difference: {bryan['count'] - expected_bryan:+d}")
    print(f"Kyla:  Expected {expected_kyla:3d} | Actual {kyla['count']:3d} | Difference: {kyla['count'] - expected_kyla:+d}")
    print(f"Ira:   Expected {expected_ira:3d} | Actual {ira['count']:3d} | Difference: {ira['count'] - expected_ira:+d}")
    print()
    
    print("✅ Three-agent distribution system test completed!")
    print(f"🎯 Ready to process {summary['total_opportunities']} opportunities with optimal distribution")

if __name__ == "__main__":
    test_three_agent_distribution()
