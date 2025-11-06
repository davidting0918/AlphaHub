"""
Binance API Adaptor Package

This package provides both sync and async adapters for various Binance APIs:
- Alpha API: Trading and market data for Binance Alpha tokens

Architecture:
- Client: Handles HTTP requests (sync and async in same file)
- Parser: Transforms raw data into structured dicts (sync, reusable)
- Adaptor: Coordinates client and parser (sync and async in same file)

Sync Usage:
    from adaptor.binance import BinanceAlpha
    alpha = BinanceAlpha()
    tokens = alpha.get_token_list()

Async Usage:
    from adaptor.binance import AsyncBinanceAlpha
    async with AsyncBinanceAlpha() as alpha:
        tokens = await alpha.get_token_list()
        # Batch methods for concurrent requests
        results = await alpha.get_klines_batch(['BTCUSDT', 'ETHUSDT'])
"""

from .client import BinanceClient, AsyncBinanceClient, BinanceClientError, BinanceAPIError
from .binance_alpha import BinanceAlpha, AsyncBinanceAlpha
from . import parser

__all__ = [
    # Sync classes
    'BinanceClient',
    'BinanceClientError',
    'BinanceAPIError',
    'BinanceAlpha',
    # Async classes
    'AsyncBinanceClient',
    'AsyncBinanceAlpha',
    # Shared
    'parser',
]

__version__ = '2.1.0'
