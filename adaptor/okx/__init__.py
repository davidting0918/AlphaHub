"""
OKX API Adaptor Package

This package provides both sync and async adapters for OKX public APIs:
- Instruments: List perpetual swaps and spot instruments
- Funding Rates: Current and historical funding rates

Architecture:
- Client: Handles HTTP requests (sync and async)
- Parser: Transforms raw OKX data into standardized formats

Sync Usage:
    from adaptor.okx import OKXClient
    client = OKXClient()
    instruments = client.get_instruments(inst_type="SWAP")

Async Usage:
    from adaptor.okx import AsyncOKXClient
    async with AsyncOKXClient() as client:
        instruments = await client.get_instruments(inst_type="SWAP")
"""

from .client import OKXClient, AsyncOKXClient, OKXClientError, OKXAPIError
from .parser import OKXParser

__all__ = [
    'OKXClient',
    'AsyncOKXClient',
    'OKXClientError',
    'OKXAPIError',
    'OKXParser',
]

__version__ = '1.0.0'
