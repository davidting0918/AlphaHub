"""
Deribit API Adaptor Package

Public API only — no authentication required for market data.
Supports options instruments, tickers, and historical chart data.

Sync Usage:
    from adaptor.deribit import Client
    client = Client()
    instruments = client.getInstruments(currency="BTC", kind="option")

Async Usage:
    from adaptor.deribit import AsyncDeribitClient
    async with AsyncDeribitClient() as client:
        instruments = await client.getInstruments(currency="BTC", kind="option")
"""

from .client import DeribitClient, AsyncDeribitClient, DeribitClientError, DeribitAPIError

# Standard alias for dynamic import by pipeline
Client = DeribitClient

__all__ = [
    'Client',
    'DeribitClient',
    'AsyncDeribitClient',
    'DeribitClientError',
    'DeribitAPIError',
]
