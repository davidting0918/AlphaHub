"""
OKX API Client - Sync and Async HTTP Client Layer

This module provides unified sync and async HTTP clients for OKX public API endpoints.
Supports instruments listing and funding rate queries (no authentication required).

Each exchange client declares EXCHANGE_ID for DB mapping and provides high-level
methods (getInstruments, getFundingRates) that return standardized/parsed output.
"""

import requests
import httpx
import logging
from typing import Optional, Dict, Any, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from httpx import Timeout, Limits

from .parser import OKXParser

logger = logging.getLogger(__name__)


class OKXClientError(Exception):
    """Base exception for OKX client errors"""
    pass


class OKXAPIError(OKXClientError):
    """Exception raised when API returns an error response"""
    def __init__(self, status_code: int, code: str, message: str, response: Optional[Dict] = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.response = response
        super().__init__(f"OKX API Error {code}: {message}")


class OKXClient:
    """Sync HTTP client for OKX public API"""
    
    BASE_URL = "https://www.okx.com"
    
    def __init__(
        self,
        base_url: str = None,
        timeout: int = 30,
        max_retries: int = 3,
        exchange_name: str = "OKX"
    ):
        self.base_url = base_url or self.BASE_URL
        self.timeout = timeout
        self.max_retries = max_retries
        self.exchange_name = exchange_name  # For instrument_id prefix
        self._parser = OKXParser()
        
        self._session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
    def close(self):
        """Close the HTTP session"""
        if self._session:
            self._session.close()
            
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
            
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Internal method to make HTTP requests with error handling
        
        Args:
            method: HTTP method (GET)
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Dict containing the API response data
            
        Raises:
            OKXAPIError: When API returns an error
            OKXClientError: For network or other client errors
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                timeout=self.timeout
            )
            
            response_data = response.json()
            
            # OKX returns code "0" for success
            if response.status_code == 200 and response_data.get('code') == '0':
                logger.debug(f"Successfully fetched {endpoint}")
                return response_data
            
            # Handle API errors
            error_code = response_data.get('code', 'unknown')
            error_msg = response_data.get('msg', 'Unknown error')
            logger.error(f"OKX API error for {endpoint}: [{error_code}] {error_msg}")
            raise OKXAPIError(
                status_code=response.status_code,
                code=error_code,
                message=error_msg,
                response=response_data
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            raise OKXClientError(f"Request failed: {str(e)}")
            
        except OKXAPIError:
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise OKXClientError(f"Unexpected error: {str(e)}")
    
    # Public API Methods

    def get_instruments(self, inst_type: str = "SWAP") -> Dict[str, Any]:
        """
        Get list of instruments
        
        Args:
            inst_type: Instrument type - "SWAP" for perpetuals, "SPOT" for spot
            
        Returns:
            API response with instruments data
        """
        endpoint = "/api/v5/public/instruments"
        params = {"instType": inst_type}
        return self._request("GET", endpoint, params=params)
    
    def get_funding_rate(self, inst_id: str) -> Dict[str, Any]:
        """
        Get current funding rate for an instrument
        
        Args:
            inst_id: Instrument ID (e.g., "BTC-USDT-SWAP")
            
        Returns:
            API response with current funding rate
        """
        endpoint = "/api/v5/public/funding-rate"
        params = {"instId": inst_id}
        return self._request("GET", endpoint, params=params)
    
    def get_klines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get candlestick/kline data for an instrument.

        Args:
            inst_id: Instrument ID (e.g., "BTC-USDT-SWAP")
            bar: Bar size - "1m","5m","15m","1H","4H","1D","1W" etc.
            limit: Number of results (max 100)
            before: Return results before this timestamp (ms) — pagination older
            after: Return results after this timestamp (ms) — pagination newer
        """
        endpoint = "/api/v5/market/candles"
        params = {"instId": inst_id, "bar": bar, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return self._request("GET", endpoint, params=params)

    def getKlines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get and parse klines, returning standardized dicts."""
        raw = self.get_klines(inst_id=inst_id, bar=bar, limit=limit, before=before, after=after)
        return self._parser.parse_klines(raw)

    def get_history_klines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get HISTORY candlestick data (older than what /candles returns).
        OKX /market/history-candles goes further back.
        """
        endpoint = "/api/v5/market/history-candles"
        params = {"instId": inst_id, "bar": bar, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return self._request("GET", endpoint, params=params)

    def getHistoryKlines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get and parse history klines."""
        raw = self.get_history_klines(inst_id=inst_id, bar=bar, limit=limit, before=before, after=after)
        return self._parser.parse_klines(raw)

    def get_funding_rate_history(
        self,
        inst_id: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get historical funding rates for an instrument
        
        Args:
            inst_id: Instrument ID (e.g., "BTC-USDT-SWAP")
            limit: Number of results (max 100)
            before: Pagination - return results before this fundingTime (ms)
            after: Pagination - return results after this fundingTime (ms)
            
        Returns:
            API response with funding rate history
        """
        endpoint = "/api/v5/public/funding-rate-history"
        params = {"instId": inst_id, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return self._request("GET", endpoint, params=params)

    # High-level methods that return parsed/standardized data

    def getInstruments(self) -> List[Dict[str, Any]]:
        """
        Get and parse perpetual instruments, returning standardized dicts.
        
        OKX uses inst_type="SWAP" for perpetual contracts.
        
        Returns:
            List of standardized instrument dicts with instrument_id prefixed
            by exchange_name (e.g., "OKX_PERP_BTC_USDT")
        """
        raw_response = self.get_instruments(inst_type="SWAP")
        instruments = self._parser.parse_instruments(raw_response, inst_type="SWAP")
        
        for inst in instruments:
            inst['instrument_id'] = f"{self.exchange_name}_PERP_{inst['base_currency']}_{inst['quote_currency']}"
        
        return instruments

    def getFundingRates(
        self,
        inst_id: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get and parse funding rate history, returning standardized dicts.
        
        Args:
            inst_id: Instrument ID (e.g., "BTC-USDT-SWAP")
            limit: Number of results (max 100)
            before: Pagination - return results before this fundingTime (ms)
            after: Pagination - return results after this fundingTime (ms)
            
        Returns:
            List of standardized funding rate dicts
        """
        raw_response = self.get_funding_rate_history(
            inst_id=inst_id,
            limit=limit,
            before=before,
            after=after
        )
        return self._parser.parse_funding_rates(raw_response)


class AsyncOKXClient:
    """Async HTTP client for OKX public API"""
    
    BASE_URL = "https://www.okx.com"
    
    def __init__(
        self,
        base_url: str = None,
        timeout: int = 30,
        max_retries: int = 3,
        max_connections: int = 100,
        exchange_name: str = "OKX"
    ):
        self.base_url = base_url or self.BASE_URL
        self.timeout = timeout
        self.max_retries = max_retries
        self.exchange_name = exchange_name  # For instrument_id prefix
        self._parser = OKXParser()
        
        timeout_config = Timeout(timeout)
        limits = Limits(
            max_keepalive_connections=max_connections,
            max_connections=max_connections
        )
        
        self._client: Optional[httpx.AsyncClient] = None
        self._timeout_config = timeout_config
        self._limits = limits
        
    async def __aenter__(self):
        transport = httpx.AsyncHTTPTransport(retries=self.max_retries)
        
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout_config,
            limits=self._limits,
            transport=transport
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            
    async def close(self):
        """Close the async HTTP client"""
        if self._client:
            await self._client.aclose()
            
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Internal async request method"""
        if not self._client:
            raise OKXClientError("Client not initialized. Use 'async with' context manager.")
        
        try:
            response = await self._client.request(
                method=method,
                url=endpoint,
                params=params
            )
            
            response_data = response.json()
            
            if response.status_code == 200 and response_data.get('code') == '0':
                logger.debug(f"Successfully fetched {endpoint}")
                return response_data
            
            error_code = response_data.get('code', 'unknown')
            error_msg = response_data.get('msg', 'Unknown error')
            logger.error(f"OKX API error for {endpoint}: [{error_code}] {error_msg}")
            raise OKXAPIError(
                status_code=response.status_code,
                code=error_code,
                message=error_msg,
                response=response_data
            )
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise OKXClientError(f"HTTP error: {str(e)}")
            
        except OKXAPIError:
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise OKXClientError(f"Unexpected error: {str(e)}")
    
    # Public API Methods (async versions)

    async def get_instruments(self, inst_type: str = "SWAP") -> Dict[str, Any]:
        """Get list of instruments"""
        endpoint = "/api/v5/public/instruments"
        params = {"instType": inst_type}
        return await self._request("GET", endpoint, params=params)
    
    async def get_funding_rate(self, inst_id: str) -> Dict[str, Any]:
        """Get current funding rate for an instrument"""
        endpoint = "/api/v5/public/funding-rate"
        params = {"instId": inst_id}
        return await self._request("GET", endpoint, params=params)
    
    async def get_klines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get candlestick/kline data."""
        endpoint = "/api/v5/market/candles"
        params = {"instId": inst_id, "bar": bar, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return await self._request("GET", endpoint, params=params)

    async def getKlines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get and parse klines."""
        raw = await self.get_klines(inst_id=inst_id, bar=bar, limit=limit, before=before, after=after)
        return self._parser.parse_klines(raw)

    async def get_history_klines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get HISTORY candlestick data (further back)."""
        endpoint = "/api/v5/market/history-candles"
        params = {"instId": inst_id, "bar": bar, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return await self._request("GET", endpoint, params=params)

    async def getHistoryKlines(
        self,
        inst_id: str,
        bar: str = "4H",
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get and parse history klines."""
        raw = await self.get_history_klines(inst_id=inst_id, bar=bar, limit=limit, before=before, after=after)
        return self._parser.parse_klines(raw)

    async def get_funding_rate_history(
        self,
        inst_id: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get historical funding rates for an instrument"""
        endpoint = "/api/v5/public/funding-rate-history"
        params = {"instId": inst_id, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        return await self._request("GET", endpoint, params=params)

    # High-level methods that return parsed/standardized data (async)

    async def getInstruments(self) -> List[Dict[str, Any]]:
        """
        Get and parse perpetual instruments, returning standardized dicts.
        
        OKX uses inst_type="SWAP" for perpetual contracts.
        
        Returns:
            List of standardized instrument dicts with instrument_id prefixed
            by exchange_name (e.g., "OKX_PERP_BTC_USDT")
        """
        raw_response = await self.get_instruments(inst_type="SWAP")
        instruments = self._parser.parse_instruments(raw_response, inst_type="SWAP")
        
        for inst in instruments:
            inst['instrument_id'] = f"{self.exchange_name}_PERP_{inst['base_currency']}_{inst['quote_currency']}"
        
        return instruments

    async def getFundingRates(
        self,
        inst_id: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get and parse funding rate history, returning standardized dicts.
        
        Args:
            inst_id: Instrument ID (e.g., "BTC-USDT-SWAP")
            limit: Number of results (max 100)
            before: Pagination - return results before this fundingTime (ms)
            after: Pagination - return results after this fundingTime (ms)
            
        Returns:
            List of standardized funding rate dicts
        """
        raw_response = await self.get_funding_rate_history(
            inst_id=inst_id,
            limit=limit,
            before=before,
            after=after
        )
        return self._parser.parse_funding_rates(raw_response)
