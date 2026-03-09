"""
Binance Alpha Adaptor - Sync and Async Coordination Layer

This module provides high-level sync and async adaptors for Binance Alpha API.
It coordinates between the Client (HTTP requests) and Parser (data transformation).
"""

import logging
from typing import Optional, Dict, Any, List

from .client import BinanceClient, AsyncBinanceClient
from .parser import BinanceAlphaParser


logger = logging.getLogger(__name__)


class BinanceAlpha:
    
    def __init__(
        self,
        base_url: str = "https://www.binance.com",
        timeout: int = 30,
        max_retries: int = 3
    ):
        self._client = BinanceClient(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries
        )
        self._parser = BinanceAlphaParser()
    
    def close(self):
        self._client.close()
    
    def get_token_list(self) -> Dict[str, Any]:
        return self._parser.parse_token_list(self._client.get_alpha_token_list())
    
    def get_klines(
        self,
        symbol: str,
        interval: str = "15s",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        return self._parser.parse_klines(self._client.get_alpha_klines(symbol, interval, limit))
    
    def get_agg_trades(
        self,
        symbol: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        return self._parser.parse_agg_trades(self._client.get_alpha_agg_trades(symbol, limit))


# Async Adaptor Implementation

class AsyncBinanceAlpha:
    """High-level async adaptor for Binance Alpha API with batch request support"""
    
    def __init__(
        self,
        base_url: str = "https://www.binance.com",
        timeout: int = 30,
        max_retries: int = 3,
        max_connections: int = 100
    ):
        self._client = AsyncBinanceClient(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            max_connections=max_connections
        )
        self._parser = BinanceAlphaParser()
        self._client_context = None
    
    async def __aenter__(self):
        self._client_context = await self._client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client_context:
            await self._client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def open(self):
        await self._client.__aenter__()
    
    async def close(self):
        await self._client.close()
    
    async def get_token_list(self) -> Dict[str, Any]:
        raw_data = await self._client.get_alpha_token_list()
        return self._parser.parse_token_list(raw_data)
    
    async def get_klines(
        self,
        symbols: List[str],
        interval: str = "15s",
        limit: int = 100
    ) -> Dict[str, List[Dict[str, Any]]]:
        raw_data_batch = await self._client.get_alpha_klines_batch(symbols, interval, limit)
        
        parsed_batch = {}
        for symbol, raw_data in raw_data_batch.items():
            if raw_data is not None:
                try:
                    parsed_batch[symbol] = self._parser.parse_klines(raw_data)
                except Exception as e:
                    logger.error(f"Failed to parse klines for {symbol}: {str(e)}")
                    parsed_batch[symbol] = None
            else:
                parsed_batch[symbol] = None
        
        return parsed_batch
    
    async def get_agg_trades(
        self,
        symbols: List[str],
        limit: int = 100
    ) -> Dict[str, List[Dict[str, Any]]]:
        raw_data_batch = await self._client.get_alpha_agg_trades_batch(symbols, limit)
        
        parsed_batch = {}
        for symbol, raw_data in raw_data_batch.items():
            if raw_data is not None:
                try:
                    parsed_batch[symbol] = self._parser.parse_agg_trades(raw_data)
                except Exception as e:
                    logger.error(f"Failed to parse agg trades for {symbol}: {str(e)}")
                    parsed_batch[symbol] = None
            else:
                parsed_batch[symbol] = None
        
        return parsed_batch
