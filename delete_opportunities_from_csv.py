import os
import pandas as pd
import httpx
import time
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
API_BASE = 'https://services.leadconnectorhq.com'
API_VERSION = '2021-07-28'
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', '')  # Set your token in .env or replace here

# Rate limiting configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RATE_LIMIT_DELAY = 0.5  # delay between requests to avoid rate limiting


async def delete_opportunity(opp_id: str, token: str, session: httpx.AsyncClient):
    """
    Delete a single opportunity by ID.
    
    Args:
        opp_id: Opportunity ID to delete
        token: Bearer token for authentication
        session: Async HTTP client session
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    url = f"{API_BASE}/opportunities/{opp_id}"
    headers = {
        'Accept': 'application/json',
        'Version': API_VERSION,
        'Authorization': f'Bearer {token}'
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"[ATTEMPT {attempt + 1}] Deleting opportunity: {opp_id}")
            response = await session.delete(url, headers=headers, timeout=30.0)
            
            if response.status_code == 200:
                print(f"[SUCCESS] Deleted opportunity: {opp_id}")
                return True, "Successfully deleted"
            elif response.status_code == 404:
                print(f"[WARNING] Opportunity not found: {opp_id}")
                return False, "Opportunity not found (404)"
            elif response.status_code == 429:
                print(f"[RATE LIMIT] Hit rate limit for {opp_id}, waiting...")
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                print(f"[ERROR] Failed to delete {opp_id}: {error_msg}")
                return False, error_msg
                
        except httpx.TimeoutException:
            print(f"[TIMEOUT] Request timed out for {opp_id}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return False, "Request timed out after retries"
        except Exception as e:
            print(f"[EXCEPTION] Error deleting {opp_id}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return False, f"Exception: {str(e)}"
    
    return False, "Failed after max retries"


async def delete_opportunities_batch(opp_ids: list, token: str, batch_size: int = 5):
    """
    Delete opportunities in batches to manage rate limits.
    
    Args:
        opp_ids: List of opportunity IDs to delete
        token: Bearer token for authentication
        batch_size: Number of concurrent deletions
        
    Returns:
        Dictionary with results summary
    """
    results = {
        'total': len(opp_ids),
        'successful': 0,
        'failed': 0,
        'details': []
    }
    
    async with httpx.AsyncClient() as session:
        for i in range(0, len(opp_ids), batch_size):
            batch = opp_ids[i:i + batch_size]
            print(f"\n[BATCH {i//batch_size + 1}] Processing {len(batch)} opportunities...")
            
            # Process batch concurrently
            tasks = [delete_opportunity(opp_id, token, session) for opp_id in batch]
            batch_results = await asyncio.gather(*tasks)
            
            # Collect results
            for opp_id, (success, message) in zip(batch, batch_results):
                result = {
                    'opp_id': opp_id,
                    'success': success,
                    'message': message,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                results['details'].append(result)
                
                if success:
                    results['successful'] += 1
                else:
                    results['failed'] += 1
            
            # Rate limiting delay between batches
            if i + batch_size < len(opp_ids):
                print(f"[RATE LIMIT] Waiting {RATE_LIMIT_DELAY}s before next batch...")
                await asyncio.sleep(RATE_LIMIT_DELAY)
    
    return results


def save_results_to_csv(results: dict, output_file: str):
    """Save deletion results to a CSV file."""
    df = pd.DataFrame(results['details'])
    df.to_csv(output_file, index=False)
    print(f"\n[SAVED] Results saved to: {output_file}")


async def main():
    """Main function to orchestrate opportunity deletion."""
    print("=" * 60)
    print("OPPORTUNITY DELETION SCRIPT")
    print("=" * 60)
    
    # Get CSV file path from user
    csv_file = input("\nEnter the path to your CSV file (with 'OppID' column): ").strip()
    
    if not os.path.exists(csv_file):
        print(f"[ERROR] File not found: {csv_file}")
        return
    
    # Get access token
    token = ACCESS_TOKEN
    if not token:
        token = input("\nEnter your Access Token (Bearer token): ").strip()
    
    if not token:
        print("[ERROR] Access token is required!")
        return
    
    # Read CSV file
    try:
        df = pd.read_csv(csv_file)
        print(f"\n[INFO] Loaded CSV file with {len(df)} rows")
        print(f"[INFO] Columns found: {list(df.columns)}")
    except Exception as e:
        print(f"[ERROR] Failed to read CSV: {str(e)}")
        return
    
    # Check for OppID column (case-insensitive)
    opp_id_col = None
    for col in df.columns:
        if col.lower() == 'oppid' or col.lower() == 'opportunity id' or col.lower() == 'opportunityid':
            opp_id_col = col
            break
    
    if opp_id_col is None:
        print(f"[ERROR] 'OppID' column not found in CSV. Available columns: {list(df.columns)}")
        return
    
    # Extract opportunity IDs
    opp_ids = df[opp_id_col].dropna().astype(str).tolist()
    opp_ids = [opp_id.strip() for opp_id in opp_ids if opp_id.strip()]
    
    print(f"\n[INFO] Found {len(opp_ids)} opportunity IDs to delete")
    
    if len(opp_ids) == 0:
        print("[ERROR] No valid opportunity IDs found!")
        return
    
    # Show preview
    print(f"\n[PREVIEW] First 5 opportunity IDs:")
    for opp_id in opp_ids[:5]:
        print(f"  - {opp_id}")
    
    if len(opp_ids) > 5:
        print(f"  ... and {len(opp_ids) - 5} more")
    
    # Confirm deletion
    confirmation = input(f"\n⚠️  Are you sure you want to DELETE {len(opp_ids)} opportunities? (yes/no): ").strip().lower()
    
    if confirmation != 'yes':
        print("[CANCELLED] Deletion cancelled by user")
        return
    
    # Get batch size
    try:
        batch_size = int(input("\nEnter batch size (default 5, recommended 3-10): ") or "5")
    except ValueError:
        batch_size = 5
    
    print(f"\n[INFO] Starting deletion process with batch size: {batch_size}")
    print("=" * 60)
    
    # Start deletion
    start_time = datetime.now()
    results = await delete_opportunities_batch(opp_ids, token, batch_size)
    end_time = datetime.now()
    
    # Print summary
    print("\n" + "=" * 60)
    print("DELETION SUMMARY")
    print("=" * 60)
    print(f"Total opportunities: {results['total']}")
    print(f"Successfully deleted: {results['successful']}")
    print(f"Failed to delete: {results['failed']}")
    print(f"Time taken: {(end_time - start_time).total_seconds():.2f} seconds")
    print("=" * 60)
    
    # Save results to CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"deletion_results_{timestamp}.csv"
    save_results_to_csv(results, output_file)
    
    # Show failed deletions if any
    if results['failed'] > 0:
        print("\n[FAILED DELETIONS]")
        failed_details = [d for d in results['details'] if not d['success']]
        for detail in failed_details[:10]:  # Show first 10 failures
            print(f"  - {detail['opp_id']}: {detail['message']}")
        if len(failed_details) > 10:
            print(f"  ... and {len(failed_details) - 10} more (see CSV for full details)")
    
    print("\n[COMPLETED] Opportunity deletion process finished!")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
