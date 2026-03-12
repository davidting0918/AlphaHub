"""
Tests for OKX Client

Integration tests that make live API calls.
Mark with @pytest.mark.integration to skip in CI.
"""

import pytest
from adaptor.okx import OKXClient, AsyncOKXClient, OKXClientError, OKXAPIError


class TestOKXClientUnit:
    """Unit tests for OKX client (no API calls)"""
    
    def test_client_initialization(self):
        """Test client initializes with correct defaults"""
        client = OKXClient()
        assert client.base_url == "https://www.okx.com"
        assert client.timeout == 30
        assert client.max_retries == 3
        client.close()
    
    def test_client_custom_config(self):
        """Test client accepts custom configuration"""
        client = OKXClient(
            base_url="https://custom.okx.com",
            timeout=60,
            max_retries=5
        )
        assert client.base_url == "https://custom.okx.com"
        assert client.timeout == 60
        assert client.max_retries == 5
        client.close()
    
    def test_client_context_manager(self):
        """Test client works as context manager"""
        with OKXClient() as client:
            assert client._session is not None
    
    def test_async_client_initialization(self):
        """Test async client initializes with correct defaults"""
        client = AsyncOKXClient()
        assert client.base_url == "https://www.okx.com"
        assert client.timeout == 30
        assert client.max_retries == 3


@pytest.mark.integration
class TestOKXClientIntegration:
    """Integration tests that make live API calls to OKX"""
    
    @pytest.fixture
    def client(self):
        """Create client for tests"""
        client = OKXClient()
        yield client
        client.close()
    
    def test_get_swap_instruments(self, client):
        """Test fetching SWAP instruments from live API"""
        response = client.get_instruments(inst_type="SWAP")
        
        assert response['code'] == '0'
        assert 'data' in response
        assert len(response['data']) > 0
        
        # Check first instrument has expected fields
        inst = response['data'][0]
        assert 'instId' in inst
        assert 'ctVal' in inst
        assert inst['instType'] == 'SWAP'
    
    def test_get_spot_instruments(self, client):
        """Test fetching SPOT instruments from live API"""
        response = client.get_instruments(inst_type="SPOT")
        
        assert response['code'] == '0'
        assert 'data' in response
        assert len(response['data']) > 0
        
        inst = response['data'][0]
        assert 'instId' in inst
        assert 'baseCcy' in inst
        assert 'quoteCcy' in inst
        assert inst['instType'] == 'SPOT'
    
    def test_get_funding_rate(self, client):
        """Test fetching current funding rate"""
        response = client.get_funding_rate(inst_id="BTC-USDT-SWAP")
        
        assert response['code'] == '0'
        assert 'data' in response
        assert len(response['data']) > 0
        
        rate = response['data'][0]
        assert rate['instId'] == 'BTC-USDT-SWAP'
        assert 'fundingRate' in rate
        assert 'fundingTime' in rate
    
    def test_get_funding_rate_history(self, client):
        """Test fetching funding rate history"""
        response = client.get_funding_rate_history(
            inst_id="BTC-USDT-SWAP",
            limit=10
        )
        
        assert response['code'] == '0'
        assert 'data' in response
        assert len(response['data']) > 0
        assert len(response['data']) <= 10
        
        rate = response['data'][0]
        assert 'fundingRate' in rate
        assert 'fundingTime' in rate
    
    def test_get_funding_rate_invalid_instrument(self, client):
        """Test error handling for invalid instrument"""
        with pytest.raises(OKXAPIError) as exc_info:
            client.get_funding_rate(inst_id="INVALID-INSTRUMENT")
        
        assert exc_info.value.code != '0'


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncOKXClientIntegration:
    """Async integration tests"""
    
    async def test_get_instruments_async(self):
        """Test async instrument fetching"""
        async with AsyncOKXClient() as client:
            response = await client.get_instruments(inst_type="SWAP")
            
            assert response['code'] == '0'
            assert 'data' in response
            assert len(response['data']) > 0
    
    async def test_get_funding_rate_async(self):
        """Test async funding rate fetching"""
        async with AsyncOKXClient() as client:
            response = await client.get_funding_rate(inst_id="BTC-USDT-SWAP")
            
            assert response['code'] == '0'
            assert 'data' in response
