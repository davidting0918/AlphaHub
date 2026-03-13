"""
Deribit API Client — Sync and Async HTTP Client Layer

Public endpoints only (no authentication). Supports:
- Instruments listing (options, futures)
- Book summaries by currency
- Ticker data
- TradingView chart data (OHLCV)

Base URL: https://www.deribit.com/api/v2
Rate limit: ~10 requests/second for unauthenticated.
"""

import time
import requests
import httpx
import logging
from typing import Optional, Dict, Any, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from httpx import Timeout, Limits

from .parser import DeribitParser

logger = logging.getLogger(__name__)


class DeribitClientError(Exception):
    """Base exception for Deribit client errors."""
    pass


class DeribitAPIError(DeribitClientError):
    """Exception raised when API returns an error response."""
    def __init__(self, status_code: int, code: int, message: str, response: Optional[Dict] = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.response = response
        super().__init__(f"Deribit API Error {code}: {message}")


class DeribitClient:
    """Sync HTTP client for Deribit public API."""

    BASE_URL = "https://www.deribit.com/api/v2"

    def __init__(
        self,
        base_url: str = None,
        timeout: int = 30,
        max_retries: int = 3,
        exchange_name: str = "DERIBIT",
        rate_limit_delay: float = 0.12,
    ):
        self.base_url = base_url or self.BASE_URL
        self.timeout = timeout
        self.max_retries = max_retries
        self.exchange_name = exchange_name
        self.rate_limit_delay = rate_limit_delay
        self._parser = DeribitParser()
        self._last_request_time = 0.0

        self._session = requests.Session()

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def close(self):
        """Close the HTTP session."""
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.monotonic()

    def _request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Make a GET request to Deribit API.

        Deribit JSON-RPC style: response has {"jsonrpc":"2.0","result":...} on success,
        or {"jsonrpc":"2.0","error":{"code":...,"message":...}} on error.
        """
        url = f"{self.base_url}{endpoint}"
        self._rate_limit()

        try:
            response = self._session.request(
                method="GET",
                url=url,
                params=params,
                timeout=self.timeout,
            )

            response_data = response.json()

            if response.status_code == 200 and "result" in response_data:
                logger.debug(f"Successfully fetched {endpoint}")
                return response_data["result"]

            # Handle API errors
            error = response_data.get("error", {})
            error_code = error.get("code", -1)
            error_msg = error.get("message", "Unknown error")
            logger.error(f"Deribit API error for {endpoint}: [{error_code}] {error_msg}")
            raise DeribitAPIError(
                status_code=response.status_code,
                code=error_code,
                message=error_msg,
                response=response_data,
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            raise DeribitClientError(f"Request failed: {str(e)}")

        except DeribitAPIError:
            raise

        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise DeribitClientError(f"Unexpected error: {str(e)}")

    # ==================== Public API Methods ====================

    def get_instruments(
        self,
        currency: str = "BTC",
        kind: str = "option",
        expired: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get list of instruments.

        Args:
            currency: "BTC" or "ETH"
            kind: "option", "future", "spot", "future_combo", "option_combo"
            expired: Include expired instruments
        """
        params = {"currency": currency, "kind": kind, "expired": str(expired).lower()}
        return self._request("/public/get_instruments", params=params)

    def get_book_summary_by_currency(
        self,
        currency: str = "BTC",
        kind: str = "option",
    ) -> List[Dict[str, Any]]:
        """
        Get book summary (ticker-like data) for all instruments of a currency/kind.

        Returns list of dicts with: instrument_name, mark_price, bid_price, ask_price,
        volume_usd, open_interest, etc.
        """
        params = {"currency": currency, "kind": kind}
        return self._request("/public/get_book_summary_by_currency", params=params)

    def get_ticker(self, instrument_name: str) -> Dict[str, Any]:
        """
        Get detailed ticker for a single instrument.

        Returns greeks, IV, mark_price, bid/ask, volume, OI, etc.
        """
        params = {"instrument_name": instrument_name}
        return self._request("/public/ticker", params=params)

    def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "60",
    ) -> Dict[str, Any]:
        """
        Get TradingView OHLCV chart data.

        Args:
            instrument_name: e.g. "BTC-28MAR25-100000-C"
            start_timestamp: Start time in milliseconds
            end_timestamp: End time in milliseconds
            resolution: "1", "3", "5", "10", "15", "30", "60", "120", "180",
                       "360", "720", "1D"
        """
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "resolution": resolution,
        }
        return self._request("/public/get_tradingview_chart_data", params=params)

    def get_index_price(self, index_name: str = "btc_usd") -> Dict[str, Any]:
        """Get current index price (e.g. btc_usd, eth_usd)."""
        params = {"index_name": index_name}
        return self._request("/public/get_index_price", params=params)

    def get_historical_volatility(self, currency: str = "BTC") -> List[List]:
        """Get historical volatility data for a currency."""
        params = {"currency": currency}
        return self._request("/public/get_historical_volatility", params=params)

    # ==================== High-level Parsed Methods ====================

    def getInstruments(
        self,
        currency: str = "BTC",
        kind: str = "option",
        expired: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get and parse instruments, returning standardized dicts."""
        raw = self.get_instruments(currency=currency, kind=kind, expired=expired)
        instruments = self._parser.parse_instruments(raw)
        for inst in instruments:
            inst['instrument_id'] = f"{self.exchange_name}_OPT_{inst['symbol']}"
        return instruments

    def getBookSummary(
        self,
        currency: str = "BTC",
        kind: str = "option",
    ) -> List[Dict[str, Any]]:
        """Get and parse book summaries as ticker snapshots."""
        raw = self.get_book_summary_by_currency(currency=currency, kind=kind)
        return self._parser.parse_book_summaries(raw)

    def getTicker(self, instrument_name: str) -> Dict[str, Any]:
        """Get and parse a single ticker with greeks."""
        raw = self.get_ticker(instrument_name)
        return self._parser.parse_ticker(raw)

    def getChartData(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "60",
    ) -> List[Dict[str, Any]]:
        """Get and parse TradingView chart data as OHLCV bars."""
        raw = self.get_tradingview_chart_data(
            instrument_name, start_timestamp, end_timestamp, resolution
        )
        return self._parser.parse_chart_data(raw)


# ==================== Async Client ====================

class AsyncDeribitClient:
    """Async HTTP client for Deribit public API."""

    BASE_URL = "https://www.deribit.com/api/v2"

    def __init__(
        self,
        base_url: str = None,
        timeout: int = 30,
        max_retries: int = 3,
        max_connections: int = 50,
        exchange_name: str = "DERIBIT",
        rate_limit_delay: float = 0.12,
    ):
        self.base_url = base_url or self.BASE_URL
        self.timeout = timeout
        self.max_retries = max_retries
        self.exchange_name = exchange_name
        self.rate_limit_delay = rate_limit_delay
        self._parser = DeribitParser()

        timeout_config = Timeout(timeout)
        limits = Limits(
            max_keepalive_connections=max_connections,
            max_connections=max_connections,
        )

        self._client: Optional[httpx.AsyncClient] = None
        self._timeout_config = timeout_config
        self._limits = limits
        self._last_request_time = 0.0

    async def __aenter__(self):
        transport = httpx.AsyncHTTPTransport(retries=self.max_retries)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout_config,
            limits=self._limits,
            transport=transport,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()

    async def close(self):
        """Close the async HTTP client."""
        if self._client:
            await self._client.aclose()

    async def _rate_limit(self):
        """Enforce async rate limiting."""
        import asyncio
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.monotonic()

    async def _request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Internal async request method."""
        if not self._client:
            raise DeribitClientError("Client not initialized. Use 'async with' context manager.")

        await self._rate_limit()

        try:
            response = await self._client.request(
                method="GET",
                url=endpoint,
                params=params,
            )

            response_data = response.json()

            if response.status_code == 200 and "result" in response_data:
                logger.debug(f"Successfully fetched {endpoint}")
                return response_data["result"]

            error = response_data.get("error", {})
            error_code = error.get("code", -1)
            error_msg = error.get("message", "Unknown error")
            logger.error(f"Deribit API error for {endpoint}: [{error_code}] {error_msg}")
            raise DeribitAPIError(
                status_code=response.status_code,
                code=error_code,
                message=error_msg,
                response=response_data,
            )

        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise DeribitClientError(f"HTTP error: {str(e)}")

        except DeribitAPIError:
            raise

        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise DeribitClientError(f"Unexpected error: {str(e)}")

    # ==================== Public API Methods ====================

    async def get_instruments(
        self,
        currency: str = "BTC",
        kind: str = "option",
        expired: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get list of instruments."""
        params = {"currency": currency, "kind": kind, "expired": str(expired).lower()}
        return await self._request("/public/get_instruments", params=params)

    async def get_book_summary_by_currency(
        self,
        currency: str = "BTC",
        kind: str = "option",
    ) -> List[Dict[str, Any]]:
        """Get book summary for all instruments of a currency/kind."""
        params = {"currency": currency, "kind": kind}
        return await self._request("/public/get_book_summary_by_currency", params=params)

    async def get_ticker(self, instrument_name: str) -> Dict[str, Any]:
        """Get detailed ticker for a single instrument."""
        params = {"instrument_name": instrument_name}
        return await self._request("/public/ticker", params=params)

    async def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "60",
    ) -> Dict[str, Any]:
        """Get TradingView OHLCV chart data."""
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "resolution": resolution,
        }
        return await self._request("/public/get_tradingview_chart_data", params=params)

    async def get_index_price(self, index_name: str = "btc_usd") -> Dict[str, Any]:
        """Get current index price."""
        params = {"index_name": index_name}
        return await self._request("/public/get_index_price", params=params)

    async def get_historical_volatility(self, currency: str = "BTC") -> List[List]:
        """Get historical volatility data."""
        params = {"currency": currency}
        return await self._request("/public/get_historical_volatility", params=params)

    # ==================== High-level Parsed Methods ====================

    async def getInstruments(
        self,
        currency: str = "BTC",
        kind: str = "option",
        expired: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get and parse instruments, returning standardized dicts."""
        raw = await self.get_instruments(currency=currency, kind=kind, expired=expired)
        instruments = self._parser.parse_instruments(raw)
        for inst in instruments:
            inst['instrument_id'] = f"{self.exchange_name}_OPT_{inst['symbol']}"
        return instruments

    async def getBookSummary(
        self,
        currency: str = "BTC",
        kind: str = "option",
    ) -> List[Dict[str, Any]]:
        """Get and parse book summaries as ticker snapshots."""
        raw = await self.get_book_summary_by_currency(currency=currency, kind=kind)
        return self._parser.parse_book_summaries(raw)

    async def getTicker(self, instrument_name: str) -> Dict[str, Any]:
        """Get and parse a single ticker with greeks."""
        raw = await self.get_ticker(instrument_name)
        return self._parser.parse_ticker(raw)

    async def getChartData(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "60",
    ) -> List[Dict[str, Any]]:
        """Get and parse TradingView chart data as OHLCV bars."""
        raw = await self.get_tradingview_chart_data(
            instrument_name, start_timestamp, end_timestamp, resolution
        )
        return self._parser.parse_chart_data(raw)
