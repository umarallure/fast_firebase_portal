import pandas as pd
import os
import json

def get_center_name_mapping():
    """
    Get center name mapping from SUBACCOUNTS environment variable
    """
    subaccounts_json = os.getenv('SUBACCOUNTS', '[]')
    try:
        subaccounts = json.loads(subaccounts_json)
        # Create mapping from id to name
        mapping = {}
        for account in subaccounts:
            account_id = str(account.get('id', ''))
            name = account.get('name', '')
            # Get first 3 letters of the name
            short_name = name[:3].upper() if name else ''
            mapping[account_id] = {
                'full_name': name,
                'short_name': short_name
            }
        return mapping
    except json.JSONDecodeError:
        print("Error: Could not parse SUBACCOUNTS from environment")
        return {}

def match_unfound_leads():
    """
    Match unfound leads with GHL export database and create enriched CSV
    """
    
    # Load environment variables for center mapping
    from dotenv import load_dotenv
    load_dotenv()
    
    # Get center name mapping
    center_mapping = get_center_name_mapping()
    print(f"Loaded {len(center_mapping)} center mappings")
    
    # File paths
    unfound_leads_file = r"c:\Users\Dell\Downloads\unfound_leads_2025-09-12.csv"
    ghl_database_file = r"c:\Users\Dell\AppData\Roaming\kingsoft\office6\templates\et\en_US\ghl_export_database.csv"
    output_file = "enriched_unfound_leads_with_centers.csv"
    
    # Check if files exist
    if not os.path.exists(unfound_leads_file):
        print(f"Error: Unfound leads file not found at {unfound_leads_file}")
        return
    
    if not os.path.exists(ghl_database_file):
        print(f"Error: GHL database file not found at {ghl_database_file}")
        return
    
    try:
        # Read the CSV files
        print("Loading unfound leads data...")
        unfound_df = pd.read_csv(unfound_leads_file)
        
        print("Loading GHL export database...")
        ghl_df = pd.read_csv(ghl_database_file)
        
        print(f"Unfound leads: {len(unfound_df)} records")
        print(f"GHL database: {len(ghl_df)} records")
        
        # Display column names for verification
        print(f"\nUnfound leads columns: {list(unfound_df.columns)}")
        print(f"GHL database columns: {list(ghl_df.columns)}")
        
        # Perform the merge based on ghl_id
        print("\nMatching leads based on ghl_id...")
        
        # Both dataframes have 'ghl_id' column, merge on that
        merged_df = unfound_df.merge(
            ghl_df, 
            on='ghl_id',  # Both have ghl_id column
            how='left',
            suffixes=('_unfound', '_ghl')
        )
        
        # Create the enriched dataset with original unfound leads columns plus Account Id
        # Start with unfound leads columns (with _unfound suffix where duplicates exist)
        enriched_columns = []
        for col in unfound_df.columns:
            if col + '_unfound' in merged_df.columns:
                enriched_columns.append(col + '_unfound')
            else:
                enriched_columns.append(col)
        
        # Add Account Id from the GHL database
        if 'Account Id' in merged_df.columns:
            enriched_columns.append('Account Id')
            account_col = 'Account Id'
            
            # Add center name columns
            merged_df['Center Name'] = merged_df['Account Id'].astype(str).map(
                lambda x: center_mapping.get(x, {}).get('full_name', 'Unknown')
            )
            merged_df['Center Code'] = merged_df['Account Id'].astype(str).map(
                lambda x: center_mapping.get(x, {}).get('short_name', 'UNK')
            )
            
            enriched_columns.extend(['Center Name', 'Center Code'])
        else:
            print("Warning: No Account Id column found in merged data")
            account_col = None
        
        # Select only the columns we want in the final output
        final_df = merged_df[enriched_columns].copy()
        
        # Rename columns to remove _unfound suffix for cleaner output
        rename_dict = {}
        for col in final_df.columns:
            if col.endswith('_unfound'):
                rename_dict[col] = col.replace('_unfound', '')
        final_df.rename(columns=rename_dict, inplace=True)
        
        # Count matches
        matched_count = final_df[account_col].notna().sum() if account_col and account_col in final_df.columns else 0
        unmatched_count = len(final_df) - matched_count
        
        print(f"\nMatching results:")
        print(f"Total leads processed: {len(final_df)}")
        print(f"Matched leads: {matched_count}")
        print(f"Unmatched leads: {unmatched_count}")
        
        # Save the enriched dataset
        final_df.to_csv(output_file, index=False)
        print(f"\nEnriched dataset saved to: {output_file}")
        
        # Display sample of matched records
        if matched_count > 0 and account_col:
            print(f"\nSample of matched records:")
            matched_sample = final_df[final_df[account_col].notna()].head(3)
            display_cols = ['opportunity_name', 'full_name', 'ghl_id', account_col, 'Center Name', 'Center Code']
            # Only include columns that exist in the dataframe
            display_cols = [col for col in display_cols if col in final_df.columns]
            print(matched_sample[display_cols].to_string())
        
        # Display sample of unmatched records
        if unmatched_count > 0 and account_col:
            print(f"\nSample of unmatched records:")
            unmatched_sample = final_df[final_df[account_col].isna()].head(3)
            print(unmatched_sample[['opportunity_name', 'full_name', 'ghl_id']].to_string())
            
    except Exception as e:
        print(f"Error processing files: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    match_unfound_leads()