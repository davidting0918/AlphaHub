"""
Tests for OKX Parser

Tests instrument and funding rate parsing with sample API responses.
"""

import pytest
from datetime import datetime, timezone

from adaptor.okx.parser import OKXParser


class TestOKXParser:
    """Test suite for OKXParser"""
    
    @pytest.fixture
    def parser(self):
        return OKXParser()
    
    # Sample API responses based on actual OKX data
    
    @pytest.fixture
    def sample_swap_response(self):
        """Sample SWAP instruments API response"""
        return {
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                    "instFamily": "BTC-USDT",
                    "uly": "BTC-USDT",
                    "settleCcy": "USDT",
                    "ctVal": "0.01",
                    "ctMult": "1",
                    "ctType": "linear",
                    "ctValCcy": "BTC",
                    "minSz": "1",
                    "lotSz": "1",
                    "tickSz": "0.1",
                    "lever": "125",
                    "state": "live",
                    "listTime": "1611916800000",
                    "expTime": "",
                    "maxLmtSz": "10000000",
                    "maxMktSz": "1000000",
                    "maxTwapSz": "10000000"
                },
                {
                    "instType": "SWAP",
                    "instId": "ETH-USDT-SWAP",
                    "instFamily": "ETH-USDT",
                    "uly": "ETH-USDT",
                    "settleCcy": "USDT",
                    "ctVal": "0.1",
                    "ctMult": "1",
                    "ctType": "linear",
                    "ctValCcy": "ETH",
                    "minSz": "1",
                    "lotSz": "1",
                    "tickSz": "0.01",
                    "lever": "100",
                    "state": "live",
                    "listTime": "1611916800000",
                    "expTime": "",
                    "maxLmtSz": "10000000",
                    "maxMktSz": "1000000",
                    "maxTwapSz": "10000000"
                }
            ]
        }
    
    @pytest.fixture
    def sample_spot_response(self):
        """Sample SPOT instruments API response"""
        return {
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "minSz": "0.00001",
                    "lotSz": "0.00000001",
                    "tickSz": "0.1",
                    "state": "live",
                    "listTime": "1548133200000"
                },
                {
                    "instType": "SPOT",
                    "instId": "ETH-USDT",
                    "baseCcy": "ETH",
                    "quoteCcy": "USDT",
                    "minSz": "0.0001",
                    "lotSz": "0.0000001",
                    "tickSz": "0.01",
                    "state": "live",
                    "listTime": "1548133200000"
                }
            ]
        }
    
    @pytest.fixture
    def sample_funding_response(self):
        """Sample funding rate history API response"""
        return {
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "instType": "SWAP",
                    "fundingRate": "0.0000054827979726",
                    "realizedRate": "0.0000054827979726",
                    "fundingTime": "1773216000000",
                    "nextFundingRate": "0.0000123456789",
                    "nextFundingTime": "1773244800000"
                },
                {
                    "instId": "BTC-USDT-SWAP",
                    "instType": "SWAP",
                    "fundingRate": "0.0001234567",
                    "realizedRate": "0.0001234567",
                    "fundingTime": "1773187200000",
                    "nextFundingRate": "",
                    "nextFundingTime": ""
                }
            ]
        }
    
    # SWAP Instrument Parsing Tests
    
    def test_parse_swap_instruments(self, parser, sample_swap_response):
        """Test parsing SWAP instruments"""
        instruments = parser.parse_instruments(sample_swap_response, inst_type="SWAP")
        
        assert len(instruments) == 2
        
        btc = instruments[0]
        assert btc['instrument_id'] == 'okx_PERP_BTC_USDT'
        assert btc['symbol'] == 'BTC-USDT-SWAP'
        assert btc['type'] == 'PERP'
        assert btc['base_currency'] == 'BTC'
        assert btc['quote_currency'] == 'USDT'
        assert btc['settle_currency'] == 'USDT'
        assert btc['contract_size'] == 0.01
        assert btc['multiplier'] == 1
        assert btc['min_size'] == 1.0
        assert btc['is_active'] is True
        assert btc['metadata']['tick_size'] == '0.1'
        assert btc['metadata']['ct_type'] == 'linear'
    
    def test_parse_swap_instrument_id_format(self, parser, sample_swap_response):
        """Test that instrument_id follows the correct format"""
        instruments = parser.parse_instruments(sample_swap_response, inst_type="SWAP")
        
        # Format should be: okx_PERP_{base}_{quote}
        assert instruments[0]['instrument_id'] == 'okx_PERP_BTC_USDT'
        assert instruments[1]['instrument_id'] == 'okx_PERP_ETH_USDT'
    
    def test_parse_swap_listing_time(self, parser, sample_swap_response):
        """Test parsing of listing time"""
        instruments = parser.parse_instruments(sample_swap_response, inst_type="SWAP")
        
        listing_time = instruments[0]['listing_time']
        assert listing_time is not None
        assert isinstance(listing_time, datetime)
        assert listing_time.tzinfo == timezone.utc
    
    # SPOT Instrument Parsing Tests
    
    def test_parse_spot_instruments(self, parser, sample_spot_response):
        """Test parsing SPOT instruments"""
        instruments = parser.parse_instruments(sample_spot_response, inst_type="SPOT")
        
        assert len(instruments) == 2
        
        btc = instruments[0]
        assert btc['instrument_id'] == 'okx_SPOT_BTC_USDT'
        assert btc['symbol'] == 'BTC-USDT'
        assert btc['type'] == 'SPOT'
        assert btc['base_currency'] == 'BTC'
        assert btc['quote_currency'] == 'USDT'
        assert btc['settle_currency'] == 'USDT'
        assert btc['contract_size'] is None  # N/A for spot
        assert btc['multiplier'] == 1
        assert btc['min_size'] == 0.00001
        assert btc['is_active'] is True
    
    def test_parse_spot_uses_base_quote_ccy_fields(self, parser, sample_spot_response):
        """Test that SPOT parsing uses baseCcy/quoteCcy fields"""
        instruments = parser.parse_instruments(sample_spot_response, inst_type="SPOT")
        
        # Should use baseCcy and quoteCcy, not parse from instId
        eth = instruments[1]
        assert eth['base_currency'] == 'ETH'
        assert eth['quote_currency'] == 'USDT'
    
    # Funding Rate Parsing Tests
    
    def test_parse_funding_rates(self, parser, sample_funding_response):
        """Test parsing funding rate history"""
        rates = parser.parse_funding_rates(sample_funding_response)
        
        assert len(rates) == 2
        
        rate = rates[0]
        assert rate['inst_id'] == 'BTC-USDT-SWAP'
        assert rate['funding_rate'] == pytest.approx(0.0000054827979726)
        assert rate['realized_rate'] == pytest.approx(0.0000054827979726)
        assert rate['funding_time'] is not None
        assert isinstance(rate['funding_time'], datetime)
    
    def test_parse_funding_rate_timestamp_conversion(self, parser, sample_funding_response):
        """Test that funding time is correctly converted from ms epoch"""
        rates = parser.parse_funding_rates(sample_funding_response)
        
        # 1773216000000 ms = timestamp in UTC
        funding_time = rates[0]['funding_time']
        assert funding_time.tzinfo == timezone.utc
        
        # Verify it's a valid datetime (year should be 2026 for this timestamp)
        assert funding_time.year == 2026
    
    def test_parse_funding_rate_with_empty_values(self, parser):
        """Test parsing funding rates with empty optional fields"""
        response = {
            "code": "0",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "fundingRate": "0.0001",
                    "fundingTime": "1700000000000",
                    "realizedRate": "",
                    "nextFundingRate": "",
                    "nextFundingTime": ""
                }
            ]
        }
        
        rates = parser.parse_funding_rates(response)
        assert len(rates) == 1
        
        rate = rates[0]
        assert rate['funding_rate'] == 0.0001
        assert rate['realized_rate'] is None or rate['realized_rate'] == 0.0
        assert rate['next_funding_rate'] is None or rate['next_funding_rate'] == 0.0
    
    # Edge Cases
    
    def test_parse_empty_response(self, parser):
        """Test parsing empty response"""
        empty_response = {"code": "0", "data": []}
        
        instruments = parser.parse_instruments(empty_response, inst_type="SWAP")
        assert instruments == []
        
        rates = parser.parse_funding_rates(empty_response)
        assert rates == []
    
    def test_parse_missing_data_key(self, parser):
        """Test parsing response without data key"""
        bad_response = {"code": "0"}
        
        instruments = parser.parse_instruments(bad_response, inst_type="SWAP")
        assert instruments == []
    
    def test_as_type_handles_none(self, parser):
        """Test as_type helper with None values"""
        assert parser.as_type(None, float) == 0.0
        assert parser.as_type(None, int) == 0
        assert parser.as_type(None, str) == ''
        assert parser.as_type(None, float, default=1.5) == 1.5
    
    def test_as_type_handles_empty_string(self, parser):
        """Test as_type helper with empty strings"""
        assert parser.as_type('', float) == 0.0
        assert parser.as_type('', int) == 0
        assert parser.as_type('', str) == ''
    
    def test_ms_to_datetime_handles_invalid(self, parser):
        """Test ms_to_datetime with invalid values"""
        assert parser.ms_to_datetime(None) is None
        assert parser.ms_to_datetime('') is None
        assert parser.ms_to_datetime('invalid') is None
