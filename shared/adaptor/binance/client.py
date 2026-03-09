"""
Binance API Client - Sync and Async HTTP Client Layer

This module provides unified sync and async HTTP clients for all Binance API endpoints.
Currently supports Binance Alpha API with extensibility for future APIs (Spot, Futures, etc.)
"""

import requests
import httpx
import asyncio
import logging
from typing import Optional, Dict, Any, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from httpx import Timeout, Limits


logger = logging.getLogger(__name__)


class BinanceClientError(Exception):
    """Base exception for Binance client errors"""
    pass


class BinanceAPIError(BinanceClientError):
    """Exception raised when API returns an error response"""
    def __init__(self, status_code: int, message: str, response: Optional[Dict] = None):
        self.status_code = status_code
        self.message = message
        self.response = response
        super().__init__(f"API Error {status_code}: {message}")


class BinanceClient:
    
    def __init__(
        self,
        base_url: str = "https://www.binance.com",
        timeout: int = 30,
        max_retries: int = 3
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        
        self._session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
    def close(self):
        if self._session:
            self._session.close()
            
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Internal method to make HTTP requests with error handling
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            data: Request body data
            headers: Custom headers
            
        Returns:
            Dict containing the API response JSON
            
        Raises:
            BinanceAPIError: When API returns an error
            BinanceClientError: For network or other client errors
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers,
                timeout=self.timeout
            )
            
            # Parse JSON response
            response_data = response.json()
            
            # Check for successful response
            if response.status_code == 200:
                logger.debug(f"Successfully fetched {endpoint}")
                return response_data
            
            # Handle API errors
            error_msg = response_data.get('msg', 'Unknown error')
            logger.error(f"API error for {endpoint}: {error_msg}")
            raise BinanceAPIError(
                status_code=response.status_code,
                message=error_msg,
                response=response_data
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            raise BinanceClientError(f"Request failed: {str(e)}")
            
        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise BinanceClientError(f"Unexpected error: {str(e)}")
    
    # Binance Alpha API Methods (Public)

    def get_alpha_token_list(self) -> Dict[str, Any]:
        endpoint = "/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
        return self._request("GET", endpoint)
    
    def get_alpha_klines(
        self,
        symbol: str,
        interval: str = "15s",
        limit: int = 100
    ) -> Dict[str, Any]:
        endpoint = "/bapi/defi/v1/public/alpha-trade/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        return self._request("GET", endpoint, params=params)
    
    def get_alpha_agg_trades(
        self,
        symbol: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        endpoint = "/bapi/defi/v1/public/alpha-trade/agg-trades"
        params = {
            "symbol": symbol,
            "limit": limit
        }
        return self._request("GET", endpoint, params=params)


# Async Client Implementation

class AsyncBinanceClient:
    """Async HTTP client for Binance API with batch request support"""
    
    def __init__(
        self,
        base_url: str = "https://www.binance.com",
        timeout: int = 30,
        max_retries: int = 3,
        max_connections: int = 100
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        
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
        if self._client:
            await self._client.aclose()
            
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        if not self._client:
            raise BinanceClientError("Client not initialized. Use 'async with' context manager.")
        
        url = f"{endpoint}"
        
        try:
            response = await self._client.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers
            )
            
            response_data = response.json()
            
            if response.status_code == 200:
                logger.debug(f"Successfully fetched {endpoint}")
                return response_data
            
            error_msg = response_data.get('msg', 'Unknown error')
            logger.error(f"API error for {endpoint}: {error_msg}")
            raise BinanceAPIError(
                status_code=response.status_code,
                message=error_msg,
                response=response_data
            )
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise BinanceClientError(f"HTTP error: {str(e)}")
            
        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise BinanceClientError(f"Unexpected error: {str(e)}")
    
    # Binance Alpha API Methods (Public) - Single Symbol

    async def get_alpha_token_list(self) -> Dict[str, Any]:
        endpoint = "/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
        return await self._request("GET", endpoint)
    
    async def get_alpha_klines(
        self,
        symbol: str,
        interval: str = "15s",
        limit: int = 100
    ) -> Dict[str, Any]:
        endpoint = "/bapi/defi/v1/public/alpha-trade/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        return await self._request("GET", endpoint, params=params)
    
    async def get_alpha_agg_trades(
        self,
        symbol: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        endpoint = "/bapi/defi/v1/public/alpha-trade/agg-trades"
        params = {
            "symbol": symbol,
            "limit": limit
        }
        return await self._request("GET", endpoint, params=params)
    
    # Batch Methods - Multiple Symbols Concurrently
    
    async def get_alpha_klines_batch(
        self,
        symbols: List[str],
        interval: str = "15s",
        limit: int = 100
    ) -> Dict[str, Dict[str, Any]]:
        async def fetch_kline(symbol: str) -> tuple[str, Optional[Dict[str, Any]]]:
            try:
                data = await self.get_alpha_klines(symbol, interval, limit)
                return symbol, data
            except Exception as e:
                logger.error(f"Failed to fetch klines for {symbol}: {str(e)}")
                return symbol, None
        
        results = await asyncio.gather(*[fetch_kline(symbol) for symbol in symbols])
        return {symbol: data for symbol, data in results}
    
    async def get_alpha_agg_trades_batch(
        self,
        symbols: List[str],
        limit: int = 100
    ) -> Dict[str, Dict[str, Any]]:
        async def fetch_agg_trades(symbol: str) -> tuple[str, Optional[Dict[str, Any]]]:
            try:
                data = await self.get_alpha_agg_trades(symbol, limit)
                return symbol, data
            except Exception as e:
                logger.error(f"Failed to fetch agg trades for {symbol}: {str(e)}")
                return symbol, None
        
        results = await asyncio.gather(*[fetch_agg_trades(symbol) for symbol in symbols])
        return {symbol: data for symbol, data in results}
