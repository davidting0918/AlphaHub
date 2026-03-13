"""
Binance API Adaptor Package

Supports Futures (perpetual instruments, funding rates) and Binance Alpha API.

Sync Usage:
    from adaptor.binance import Client
    client = Client()
    instruments = client.getInstruments()

Async Usage:
    from adaptor.binance import AsyncBinanceClient
    async with AsyncBinanceClient() as client:
        instruments = await client.getInstruments()
"""

from .client import BinanceClient, AsyncBinanceClient, BinanceClientError, BinanceAPIError
from .binance_alpha import BinanceAlpha, AsyncBinanceAlpha

# Standard alias for dynamic import by pipeline
Client = BinanceClient

__all__ = [
    'Client',
    'BinanceClient',
    'AsyncBinanceClient',
    'BinanceClientError',
    'BinanceAPIError',
    'BinanceAlpha',
    'AsyncBinanceAlpha',
]
