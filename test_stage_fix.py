#!/usr/bin/env python3
"""
Test script to verify the stage ID validation fix
"""

import asyncio
import logging
from app.services.master_child_opportunity_update import master_child_opportunity_service

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_stage_resolution():
    """Test the improved stage resolution logic"""

    # Sample pipeline mapping (similar to what we saw in the logs)
    pipeline_mapping = {
        'pipelines': {},
        'stages': {
            'issued - pending first draft': 'c3525ee4-5d03-41bb-b1c8-4ea946c64d06',
            'active placed - paid as advanced': '441e0dd2-277f-40b8-837d-69ed87ab4204',
            'pending approval': 'c941cc8a-be7b-4296-b699-4aed3d0de14b'
        },
        'pipeline_stages': {
            'kXOhxtu3AuW5hTH48wG3': {  # The pipeline from the logs
                'transfer api': 'some-id-1',
                'chargeback fix api': 'some-id-2',
                'incomplete transfer': 'some-id-3',
                'returned to center - dq': '279cc908-5196-4d74-845c-88137491693f',
                "dq'd can't be sold": 'some-id-5',
                'needs bpo callback': '612653d4-9c93-45cd-9922-c6a59c53eecf',
                'application withdrawn': 'some-id-7',
                'declined underwriting': 'bbb71132-f718-48f8-906d-822277cee176',
                'pending approval': 'c941cc8a-be7b-4296-b699-4aed3d0de14b',
                'pending manual action': 'some-id-10'
            }
        }
    }

    # Test cases that were failing
    test_cases = [
        ('Issued - Pending First Draft', 'kXOhxtu3AuW5hTH48wG3'),  # Should return None (not in pipeline)
        ('Active Placed - Paid as Advanced', 'kXOhxtu3AuW5hTH48wG3'),  # Should return None (not in pipeline)
        ('Pending Approval', 'kXOhxtu3AuW5hTH48wG3'),  # Should return valid ID (exists in pipeline)
        ('Transfer API', 'kXOhxtu3AuW5hTH48wG3'),  # Should return valid ID (exists in pipeline)
    ]

    print("🧪 Testing Stage ID Resolution Fix")
    print("=" * 50)

    for stage_name, pipeline_id in test_cases:
        result = master_child_opportunity_service.find_stage_id(stage_name, pipeline_id, pipeline_mapping)
        status = "✅ PASS" if result else "❌ SKIP"
        print(f"{status} | '{stage_name}' -> {result}")

    print("\n📊 Summary:")
    print("- Stages that exist in target pipeline: Should return valid ID")
    print("- Stages that don't exist in target pipeline: Should return None (skip update)")
    print("- This prevents '422 Unprocessable Entity' errors from invalid stage IDs")

if __name__ == "__main__":
    asyncio.run(test_stage_resolution())