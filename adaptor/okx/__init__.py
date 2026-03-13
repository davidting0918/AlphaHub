"""
OKX API Adaptor Package

Sync Usage:
    from adaptor.okx import Client
    client = Client()
    instruments = client.getInstruments()

Async Usage:
    from adaptor.okx import AsyncOKXClient
    async with AsyncOKXClient() as client:
        instruments = await client.getInstruments()
"""

from .client import OKXClient, AsyncOKXClient, OKXClientError, OKXAPIError

# Standard alias for dynamic import by pipeline
Client = OKXClient

__all__ = [
    'Client',
    'OKXClient',
    'AsyncOKXClient',
    'OKXClientError',
    'OKXAPIError',
]
