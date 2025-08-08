import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.ghl_opportunity_updater_v2 import GHLOpportunityUpdaterV2

@pytest.fixture
def mock_access_token():
    return "test_access_token_12345"

@pytest.fixture
def sample_pipelines():
    return [
        {
            "id": "pipeline_1",
            "name": "Customer Pipeline",
            "stages": [
                {"id": "stage_1", "name": "Needs Carrier Application"},
                {"id": "stage_2", "name": "Pending Lapse"},
                {"id": "stage_3", "name": "Needs to be Fixed"}
            ]
        },
        {
            "id": "pipeline_2", 
            "name": "Transfer Portal",
            "stages": [
                {"id": "stage_4", "name": "ACTIVE PLACED - Paid as Advanced"},
                {"id": "stage_5", "name": "First Draft Payment Failure"}
            ]
        }
    ]

@pytest.mark.asyncio
async def test_get_pipeline_id_by_name(mock_access_token, sample_pipelines):
    """Test pipeline ID lookup by name"""
    with patch('httpx.AsyncClient') as mock_client:
        # Mock the response
        mock_response = MagicMock()
        mock_response.json.return_value = {"pipelines": sample_pipelines}
        mock_response.raise_for_status.return_value = None
        
        mock_client.return_value.get = AsyncMock(return_value=mock_response)
        
        updater = GHLOpportunityUpdaterV2(mock_access_token)
        
        # Test successful lookup
        pipeline_id = await updater.get_pipeline_id_by_name("Customer Pipeline")
        assert pipeline_id == "pipeline_1"
        
        # Test case insensitive lookup
        pipeline_id = await updater.get_pipeline_id_by_name("customer pipeline")
        assert pipeline_id == "pipeline_1"
        
        # Test not found
        pipeline_id = await updater.get_pipeline_id_by_name("Nonexistent Pipeline")
        assert pipeline_id is None
        
        await updater.close()

@pytest.mark.asyncio
async def test_get_stage_id_by_name(mock_access_token, sample_pipelines):
    """Test stage ID lookup by name within a pipeline"""
    with patch('httpx.AsyncClient') as mock_client:
        # Mock the response
        mock_response = MagicMock()
        mock_response.json.return_value = {"pipelines": sample_pipelines}
        mock_response.raise_for_status.return_value = None
        
        mock_client.return_value.get = AsyncMock(return_value=mock_response)
        
        updater = GHLOpportunityUpdaterV2(mock_access_token)
        
        # Test successful lookup
        stage_id = await updater.get_stage_id_by_name("pipeline_1", "Needs Carrier Application")
        assert stage_id == "stage_1"
        
        # Test case insensitive lookup
        stage_id = await updater.get_stage_id_by_name("pipeline_1", "needs carrier application")
        assert stage_id == "stage_1"
        
        # Test stage not found in pipeline
        stage_id = await updater.get_stage_id_by_name("pipeline_1", "Nonexistent Stage")
        assert stage_id is None
        
        # Test pipeline not found
        stage_id = await updater.get_stage_id_by_name("nonexistent_pipeline", "Any Stage")
        assert stage_id is None
        
        await updater.close()

@pytest.mark.asyncio
async def test_update_opportunity_v2_success(mock_access_token):
    """Test successful opportunity update"""
    with patch('httpx.AsyncClient') as mock_client:
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "opp_123", "status": "updated"}
        mock_response.text = '{"id": "opp_123", "status": "updated"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_response.raise_for_status.return_value = None
        
        mock_client.return_value.put = AsyncMock(return_value=mock_response)
        
        updater = GHLOpportunityUpdaterV2(mock_access_token)
        
        payload = {
            "pipelineId": "pipeline_1",
            "pipelineStageId": "stage_1", 
            "status": "open",
            "monetaryValue": 150.00
        }
        
        result = await updater.update_opportunity_v2("opp_123", payload)
        
        assert result["status"] == "success"
        assert result["opportunity_id"] == "opp_123"
        assert result["response_status"] == 200
        
        # Verify the API was called correctly
        mock_client.return_value.put.assert_called_once()
        call_args = mock_client.return_value.put.call_args
        assert call_args[0][0] == "https://services.leadconnectorhq.com/opportunities/opp_123"
        assert call_args[1]["json"] == payload
        
        await updater.close()

@pytest.mark.asyncio
async def test_update_opportunity_v2_failure(mock_access_token):
    """Test failed opportunity update"""
    with patch('httpx.AsyncClient') as mock_client:
        # Mock failed response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = '{"error": "Invalid pipeline ID"}'
        
        import httpx
        mock_client.return_value.put = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "Bad Request", 
                request=MagicMock(), 
                response=mock_response
            )
        )
        
        updater = GHLOpportunityUpdaterV2(mock_access_token)
        
        payload = {
            "pipelineId": "invalid_pipeline",
            "pipelineStageId": "invalid_stage",
            "status": "open",
            "monetaryValue": 150.00
        }
        
        result = await updater.update_opportunity_v2("opp_123", payload)
        
        assert result["status"] == "error"
        assert result["opportunity_id"] == "opp_123" 
        assert result["response_status"] == 400
        assert "Invalid pipeline ID" in result["response_body"]
        
        await updater.close()

@pytest.mark.asyncio
async def test_headers_configuration(mock_access_token):
    """Test that headers are configured correctly"""
    updater = GHLOpportunityUpdaterV2(mock_access_token)
    
    expected_headers = {
        "Authorization": f"Bearer {mock_access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json", 
        "Version": "2021-07-28"
    }
    
    assert updater.headers == expected_headers
    assert updater.base_url == "https://services.leadconnectorhq.com"
    
    await updater.close()

if __name__ == "__main__":
    # Run a simple test
    async def run_simple_test():
        print("Running simple V2 API service test...")
        
        # Test header configuration
        updater = GHLOpportunityUpdaterV2("test_token")
        assert "Bearer test_token" in updater.headers["Authorization"]
        assert updater.headers["Version"] == "2021-07-28"
        await updater.close()
        
        print("✅ V2 API service basic tests passed!")
    
    asyncio.run(run_simple_test())
