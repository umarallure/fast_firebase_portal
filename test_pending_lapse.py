#!/usr/bin/env python3
"""
Test script for the new assigned_pending_lapse.csv file
Testing 29 opportunities with Bryan (40%), Kyla (30%), Ira (30%) distribution
"""

import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bulk_opportunity_owner_update import BulkOpportunityOwnerUpdateService

def test_pending_lapse_distribution():
    """Test the three-agent distribution with pending lapse opportunities"""
    
    print("🚀 PENDING LAPSE OPPORTUNITIES - THREE-AGENT DISTRIBUTION")
    print("=" * 65)
    print("📋 Distribution Target:")
    print("   Bryan: 40% = 12 opportunities (ID: 1iulHgfbKF7ufdpt6osS)")
    print("   Kyla:  30% = 9 opportunities  (ID: GkBzcbgHkCNaoSKKdIkZ)")
    print("   Ira:   30% = 8 opportunities  (ID: 3swinpnwu3nIQh3NrmTX)")
    print("   Total: 29 opportunities")
    print()
    
    # Read the pending lapse CSV file
    csv_file_path = "assigned_pending_lapse.csv"
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        print(f"✅ Successfully loaded CSV file: {csv_file_path}")
    except FileNotFoundError:
        print(f"❌ Error: Could not find {csv_file_path}")
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
    print("-" * 35)
    print(f"✅ CSV parsed successfully!")
    print(f"📈 Total opportunities: {summary['total_opportunities']}")
    print(f"🏢 Unique accounts: {summary['unique_accounts']}")
    print(f"🎭 All opportunities are: Pending Lapse stage")
    print()
    
    # Display distribution
    assignment_dist = summary['assignment_distribution']
    
    print("👥 ACTUAL AGENT DISTRIBUTION:")
    print("-" * 40)
    
    bryan = assignment_dist['bryan']
    kyla = assignment_dist['kyla']
    ira = assignment_dist['ira']
    
    print(f"🎯 Bryan:  {bryan['count']:2d} opportunities ({bryan['percentage']:4.1f}%)")
    print(f"   └── ID: {bryan['id']}")
    print()
    print(f"🎯 Kyla:   {kyla['count']:2d} opportunities ({kyla['percentage']:4.1f}%)")
    print(f"   └── ID: {kyla['id']}")
    print()
    print(f"🎯 Ira:    {ira['count']:2d} opportunities ({ira['percentage']:4.1f}%)")
    print(f"   └── ID: {ira['id']}")
    print()
    
    # Verify total and target distribution
    total_assigned = bryan['count'] + kyla['count'] + ira['count']
    print(f"📊 Total assigned: {total_assigned} (should equal {summary['total_opportunities']})")
    
    # Check against target distribution
    target_bryan = 12
    target_kyla = 9
    target_ira = 8
    
    print(f"\n🎯 TARGET vs ACTUAL COMPARISON:")
    print("-" * 45)
    print(f"Bryan: Target {target_bryan:2d} | Actual {bryan['count']:2d} | Difference: {bryan['count'] - target_bryan:+d}")
    print(f"Kyla:  Target {target_kyla:2d} | Actual {kyla['count']:2d} | Difference: {kyla['count'] - target_kyla:+d}")
    print(f"Ira:   Target {target_ira:2d} | Actual {ira['count']:2d} | Difference: {ira['count'] - target_ira:+d}")
    
    if total_assigned == summary['total_opportunities']:
        print("\n✅ Distribution total matches!")
    else:
        print("\n❌ Distribution total mismatch!")
    
    # Show sample assignments for each agent
    bryan_opps = [o for o in opportunities if o['assigned_to'] == bryan['id']]
    kyla_opps = [o for o in opportunities if o['assigned_to'] == kyla['id']]
    ira_opps = [o for o in opportunities if o['assigned_to'] == ira['id']]
    
    print(f"\n🔍 SAMPLE OPPORTUNITY ASSIGNMENTS:")
    print("-" * 50)
    
    print(f"📋 Bryan's Opportunities ({len(bryan_opps)}):")
    for i, opp in enumerate(bryan_opps[:3]):
        print(f"   {i+1}. {opp['opportunity_name'][:45]}...")
    if len(bryan_opps) > 3:
        print(f"      ... and {len(bryan_opps) - 3} more")
    print()
    
    print(f"📋 Kyla's Opportunities ({len(kyla_opps)}):")
    for i, opp in enumerate(kyla_opps[:3]):
        print(f"   {i+1}. {opp['opportunity_name'][:45]}...")
    if len(kyla_opps) > 3:
        print(f"      ... and {len(kyla_opps) - 3} more")
    print()
    
    print(f"📋 Ira's Opportunities ({len(ira_opps)}):")
    for i, opp in enumerate(ira_opps[:3]):
        print(f"   {i+1}. {opp['opportunity_name'][:45]}...")
    if len(ira_opps) > 3:
        print(f"      ... and {len(ira_opps) - 3} more")
    print()
    
    print("✅ Three-agent distribution test completed!")
    print(f"🎯 Ready to process {summary['total_opportunities']} Pending Lapse opportunities")
    print("🚀 Upload this CSV through the web interface to execute the updates!")

if __name__ == "__main__":
    test_pending_lapse_distribution()
