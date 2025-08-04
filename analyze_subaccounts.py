"""
CSV Analysis Tool for Multiple Subaccounts
==========================================
This script analyzes the CSV data to identify which subaccounts 
have the problematic stages and need separate processing.
"""

import pandas as pd
import json
from collections import defaultdict

def analyze_csv_by_subaccount():
    """Analyze CSV data grouped by call center (subaccount)"""
    
    print("🔍 Analyzing CSV data by subaccount...")
    print("=" * 60)
    
    try:
        # Read the CSV file with proper encoding
        df = pd.read_csv('transferportalmaster.csv', encoding='latin-1')
        
        # Group by CALL CENTER (subaccount)
        subaccount_analysis = defaultdict(lambda: {
            'total_records': 0,
            'needs_to_be_fixed': 0,
            'returned_to_center': 0,
            'stage_variations': set(),
            'sample_records': []
        })
        
        # Define stage patterns to look for
        problematic_stages = [
            'Needs to be Fixed',
            'Returned to Center', 
            'Returned To Center',
            'Returned to center',
            'returned to center',
            'needs to be fixed'
        ]
        
        for _, row in df.iterrows():
            call_center = row.get('CALL CENTER', 'Unknown')
            stage = row.get('GHL Pipeline Stage', '')
            
            # Handle NaN values
            if pd.isna(stage):
                stage = ''
            if pd.isna(call_center):
                call_center = 'Unknown'
            
            stage = str(stage).strip()
            call_center = str(call_center).strip()
            
            subaccount_analysis[call_center]['total_records'] += 1
            
            # Check for problematic stages
            stage_lower = stage.lower().strip()
            if 'needs to be fixed' in stage_lower:
                subaccount_analysis[call_center]['needs_to_be_fixed'] += 1
                subaccount_analysis[call_center]['stage_variations'].add(stage)
                
            if 'returned to center' in stage_lower:
                subaccount_analysis[call_center]['returned_to_center'] += 1
                subaccount_analysis[call_center]['stage_variations'].add(stage)
            
            # Add sample record if it has problematic stages
            if any(prob_stage.lower() in stage_lower for prob_stage in ['needs to be fixed', 'returned to center']):
                if len(subaccount_analysis[call_center]['sample_records']) < 3:
                    subaccount_analysis[call_center]['sample_records'].append({
                        'phone': row.get('Customer Phone Number', ''),
                        'name': row.get('Name', ''),
                        'stage': stage
                    })
        
        # Print analysis results
        print(f"📊 SUBACCOUNT ANALYSIS RESULTS")
        print("=" * 60)
        
        for subaccount, data in subaccount_analysis.items():
            if data['needs_to_be_fixed'] > 0 or data['returned_to_center'] > 0:
                print(f"\n🏢 SUBACCOUNT: {subaccount}")
                print(f"   Total Records: {data['total_records']}")
                print(f"   'Needs to be Fixed': {data['needs_to_be_fixed']} records")
                print(f"   'Returned to Center': {data['returned_to_center']} records")
                
                if data['stage_variations']:
                    print(f"   Stage Variations Found:")
                    for stage in sorted(data['stage_variations']):
                        print(f"     - '{stage}'")
                
                if data['sample_records']:
                    print(f"   Sample Records:")
                    for i, record in enumerate(data['sample_records'], 1):
                        print(f"     {i}. {record['name']} ({record['phone']}) -> '{record['stage']}'")
        
        # Summary
        total_problematic = sum(data['needs_to_be_fixed'] + data['returned_to_center'] 
                              for data in subaccount_analysis.values())
        
        print(f"\n📈 SUMMARY:")
        print(f"   Total problematic records: {total_problematic}")
        print(f"   Subaccounts with issues: {len([s for s, d in subaccount_analysis.items() if d['needs_to_be_fixed'] > 0 or d['returned_to_center'] > 0])}")
        
        # Generate recommended approach
        print(f"\n💡 RECOMMENDED APPROACH:")
        print(f"   1. You need API keys for each subaccount listed above")
        print(f"   2. Process CSV data separately for each subaccount")
        print(f"   3. Each subaccount needs its own API configuration")
        
        return subaccount_analysis
        
    except Exception as e:
        print(f"❌ Error analyzing CSV: {e}")
        return None

if __name__ == "__main__":
    analyze_csv_by_subaccount()
