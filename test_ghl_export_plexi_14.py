#!/usr/bin/env python3
"""
Test Suite for GHL Export Service - Plexi Account 14
Tests the pagination fixes and export functionality for Plexi account 14
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, Mock, patch
import pandas as pd
import io

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.ghl_export_new import GHLClient, process_export_request
from app.models.schemas import ExportRequest, SelectionSchema

class GHLExportTester:
    """Test class for validating the GHL export service with pagination fixes"""

    def __init__(self):
        self.test_results = []

    def log_test_result(self, test_name: str, passed: bool, details: str = ""):
        """Log test results"""
        result = {
            'test_name': test_name,
            'passed': passed,
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        self.test_results.append(result)
        status = "PASSED" if passed else "FAILED"
        print(f"{status}: {test_name}")
        if details:
            print(f"   Details: {details}")
        print()

    def mock_opportunities_response(self, opportunities: List[Dict], meta: Optional[Dict] = None, status_code: int = 200):
        """Create a mock response for opportunities endpoint"""
        if meta is None:
            meta = {"nextPageUrl": None}

        return Mock(
            status_code=status_code,
            json=Mock(return_value={
                "opportunities": opportunities,
                "meta": meta
            })
        )

    def mock_pipelines_response(self, pipelines: List[Dict], status_code: int = 200):
        """Create a mock response for pipelines endpoint"""
        return Mock(
            status_code=status_code,
            json=Mock(return_value={"pipelines": pipelines})
        )

    def mock_pipeline_stages_response(self, stages: List[Dict], status_code: int = 200):
        """Create a mock response for pipeline stages endpoint"""
        return Mock(
            status_code=status_code,
            json=Mock(return_value={"stages": stages})
        )

    async def test_pagination_end_detection(self):
        """Test that pagination stops when fewer than limit opportunities are returned"""
        print("Testing Pagination End Detection...")

        # Mock data for Plexi account 14
        opportunities_batch_1 = [
            {"id": f"opp_{i}", "name": f"Opportunity {i}", "contact": {"name": f"Contact {i}"},
             "createdAt": "2024-01-01T10:00:00.000Z", "pipelineId": "pipeline_1", "pipelineStageId": "stage_1"}
            for i in range(100)
        ]

        opportunities_batch_2 = [
            {"id": f"opp_{i+100}", "name": f"Opportunity {i+100}", "contact": {"name": f"Contact {i+100}"},
             "createdAt": "2024-01-01T10:00:00.000Z", "pipelineId": "pipeline_1", "pipelineStageId": "stage_1"}
            for i in range(96)  # Less than limit of 100
        ]

        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            # Mock opportunity batches only (get_opportunities doesn't call get_pipelines)
            mock_client.get.side_effect = [
                self.mock_opportunities_response(opportunities_batch_1, {"nextPageUrl": "next_page"}),  # First opportunities batch
                self.mock_opportunities_response(opportunities_batch_2, {"nextPageUrl": None})  # Second opportunities batch
            ]

            client = GHLClient("fake_api_key")
            results = await client.get_opportunities("pipeline_1", "Test Pipeline", {"stage_1": "Stage 1"}, "account_14", 200)

            # Should get exactly 196 opportunities (100 + 96)
            expected_count = 196
            if len(results) == expected_count:
                self.log_test_result("Pagination End Detection", True, f"Correctly stopped at {expected_count} opportunities")
            else:
                self.log_test_result("Pagination End Detection", False, f"Expected {expected_count}, got {len(results)}")

            # Verify the client was called 2 times (2 opportunity batches)
            expected_calls = 2
            actual_calls = mock_client.get.call_count
            if actual_calls == expected_calls:
                self.log_test_result("API Call Count", True, f"Correctly made {expected_calls} API calls")
            else:
                self.log_test_result("API Call Count", False, f"Expected {expected_calls} calls, got {actual_calls}")

    async def test_infinite_loop_prevention(self):
        """Test that infinite loops are prevented when cursor parameters don't change"""
        print("Testing Infinite Loop Prevention...")

        # Same opportunity returned repeatedly
        same_opportunity = [
            {"id": "same_opp", "name": "Same Opportunity", "contact": {"name": "Same Contact"},
             "createdAt": "2024-01-01T10:00:00.000Z", "pipelineId": "pipeline_1", "pipelineStageId": "stage_1"}
        ]

        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            # Mock opportunity calls - same data returned
            mock_client.get.side_effect = [
                self.mock_opportunities_response(same_opportunity, {"nextPageUrl": "next_page"}),
                self.mock_opportunities_response(same_opportunity, {"nextPageUrl": "next_page"}),  # Same data again
            ]

            client = GHLClient("fake_api_key")
            results = await client.get_opportunities("pipeline_1", "Test Pipeline", {"stage_1": "Stage 1"}, "account_14", 200)

            # Should get exactly 1 opportunity (not infinite)
            expected_count = 1
            if len(results) == expected_count:
                self.log_test_result("Infinite Loop Prevention", True, f"Correctly stopped at {expected_count} opportunity")
            else:
                self.log_test_result("Infinite Loop Prevention", False, f"Expected {expected_count}, got {len(results)}")

    async def test_max_records_limit(self):
        """Test that max_records limit is respected"""
        print("Testing Max Records Limit...")

        opportunities_data = [
            {"id": f"opp_{i}", "name": f"Opportunity {i}", "contact": {"name": f"Contact {i}"},
             "createdAt": "2024-01-01T10:00:00.000Z", "pipelineId": "pipeline_1", "pipelineStageId": "stage_1"}
            for i in range(150)  # More than the limit of 100
        ]

        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            # Mock single opportunity batch
            mock_client.get.side_effect = [
                self.mock_opportunities_response(opportunities_data[:150], {"nextPageUrl": None})
            ]

            client = GHLClient("fake_api_key")
            results = await client.get_opportunities("pipeline_1", "Test Pipeline", {"stage_1": "Stage 1"}, "account_14", 100)

            # Should get exactly 100 opportunities (limited)
            expected_count = 100
            if len(results) == expected_count:
                self.log_test_result("Max Records Limit", True, f"Correctly limited to {expected_count} opportunities")
            else:
                self.log_test_result("Max Records Limit", False, f"Expected {expected_count}, got {len(results)}")

    async def test_plexi_account_14_export(self):
        """Test full export process for Plexi account 14"""
        print("Testing Plexi Account 14 Export...")

        # Mock data representing Plexi account 14
        pipelines_data = [
            {
                "id": "pwXbzaRw87WktY49eb7F",
                "name": "Transfer Portal",
                "stages": [
                    {"id": "stage_1", "name": "New Lead"},
                    {"id": "stage_2", "name": "Contacted"}
                ]
            }
        ]

        opportunities_data = [
            {
                "id": f"plexi_opp_{i}",
                "name": f"Plexi Opportunity {i}",
                "contact": {
                    "id": f"contact_{i}",
                    "name": f"Plexi Contact {i}",
                    "phone": f"+1{i}0075675820",
                    "email": f"contact{i}@plexi.com"
                },
                "createdAt": "2024-01-01T10:00:00.000Z",
                "updatedAt": "2024-01-01T11:00:00.000Z",
                "pipelineId": "pwXbzaRw87WktY49eb7F",
                "pipelineStageId": "stage_1",
                "status": "open",
                "monetaryValue": 1000.00,
                "source": "website",
                "assignedTo": "agent_1"
            } for i in range(50)  # Limited data for testing
        ]

        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            # Mock all API calls
            mock_client.get.side_effect = [
                self.mock_pipelines_response(pipelines_data),
                self.mock_opportunities_response(opportunities_data, {"nextPageUrl": None})
            ]

            # Create export request for Plexi account 14
            export_request = ExportRequest(
                selections=[
                    SelectionSchema(
                        account_id="14",
                        api_key="plexi_api_key_14",
                        pipelines=["pwXbzaRw87WktY49eb7F"]
                    )
                ]
            )

            try:
                excel_bytes = await process_export_request(export_request)

                # Verify Excel file was generated
                if excel_bytes and len(excel_bytes) > 0:
                    # Try to read the Excel data to verify content
                    excel_data = io.BytesIO(excel_bytes)
                    df = pd.read_excel(excel_data)

                    expected_rows = 50
                    if len(df) == expected_rows:
                        self.log_test_result("Plexi Account 14 Export", True,
                                           f"Successfully exported {expected_rows} opportunities for account 14")
                    else:
                        self.log_test_result("Plexi Account 14 Export", False,
                                           f"Expected {expected_rows} rows, got {len(df)}")
                else:
                    self.log_test_result("Plexi Account 14 Export", False, "No Excel data generated")

            except Exception as e:
                self.log_test_result("Plexi Account 14 Export", False, f"Export failed: {str(e)}")

    async def test_rate_limiting_handling(self):
        """Test that rate limiting is handled properly"""
        print("Testing Rate Limiting Handling...")

        opportunities_data = [
            {"id": f"opp_{i}", "name": f"Opportunity {i}", "contact": {"name": f"Contact {i}"},
             "createdAt": "2024-01-01T10:00:00.000Z", "pipelineId": "pipeline_1", "pipelineStageId": "stage_1"}
            for i in range(10)
        ]

        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            # Mock rate limited response followed by success
            mock_client.get.side_effect = [
                Mock(status_code=429, json=Mock(return_value={})),  # Rate limited
                self.mock_opportunities_response(opportunities_data, {"nextPageUrl": None})
            ]

            client = GHLClient("fake_api_key")
            results = await client.get_opportunities("pipeline_1", "Test Pipeline", {"stage_1": "Stage 1"}, "account_14", 200)

            # Should eventually succeed and get the data
            expected_count = 10
            if len(results) == expected_count:
                self.log_test_result("Rate Limiting Handling", True, f"Successfully handled rate limit and got {expected_count} opportunities")
            else:
                self.log_test_result("Rate Limiting Handling", False, f"Expected {expected_count}, got {len(results)}")

    async def run_all_tests(self):
        """Run all test cases"""
        print("GHL Export Service Test Suite - Plexi Account 14")
        print("=" * 60)

        await self.test_pagination_end_detection()
        await self.test_infinite_loop_prevention()
        await self.test_max_records_limit()
        await self.test_plexi_account_14_export()
        await self.test_rate_limiting_handling()

        # Summary
        passed = sum(1 for result in self.test_results if result['passed'])
        total = len(self.test_results)

        print("=" * 60)
        print(f"Test Results: {passed}/{total} tests passed")

        if passed == total:
            print("All tests passed! GHL export service is working correctly.")
        else:
            print("Some tests failed. Please review the results above.")

        return passed == total

def main():
    """Main test runner"""
    tester = GHLExportTester()
    success = asyncio.run(tester.run_all_tests())
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())