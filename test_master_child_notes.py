import asyncio
from dotenv import load_dotenv
load_dotenv()
from app.services.master_child_notes import master_child_service

# Read CSV files
with open('c:/Users/Dell/Desktop/Unlimited-Insurance-Automation-Portal/fast_firebase_portal/MasterData.csv', 'r', encoding='utf-8') as f:
    master_csv = f.read()
with open('c:/Users/Dell/Desktop/Unlimited-Insurance-Automation-Portal/fast_firebase_portal/ChildData.csv', 'r', encoding='utf-8') as f:
    child_csv = f.read()

async def main():
    print('Matching contacts...')
    # Process all master entries
    result = await master_child_service.match_contacts(master_csv, child_csv)
    print('Match result:')
    print('Success:', result['success'])
    print('Total Matches:', len(result['matches']))
    print('Total Unmatched:', len(result['unmatched_master']))
    print('Summary:', result['summary'])

    print('\n--- MATCH DETAILS ---')
    for idx, m in enumerate(result['matches']):
        print(f"Match #{idx+1}:")
        print(f"  Master: {m['master_contact'].get('Contact Name', '')} (ID: {m['master_contact_id']})")
        print(f"  Child:  {m['child_contact'].get('Contact Name', '')} (ID: {m['child_contact_id']})")
        print(f"  Score:  {m['match_score']:.3f} | Type: {m['match_type']} | Potential Matches: {m['potential_matches_count']}")
        print('-' * 40)

    print('\n--- UNMATCHED MASTER CONTACTS ---')
    for idx, um in enumerate(result['unmatched_master']):
        print(f"Unmatched #{idx+1}: {um['master_contact'].get('Contact Name', '')} (ID: {um['master_contact_id']})")

    print('\nStarting actual notes transfer for all matches...')
    transfer_result = await master_child_service.process_notes_transfer(result['matches'], dry_run=False)
    print('Transfer started:', transfer_result['success'])
    print('Transfer message:', transfer_result['message'])

if __name__ == '__main__':
    asyncio.run(main())
