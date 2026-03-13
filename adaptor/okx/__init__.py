"""
OKX API Adaptor Package

This package provides both sync and async adapters for OKX public APIs:
- Instruments: List perpetual swaps and spot instruments
- Funding Rates: Current and historical funding rates

The clients expose high-level methods (getInstruments, getFundingRates) that
return standardized/parsed data. Parsing is handled internally.

Sync Usage:
    from adaptor.okx import OKXClient
    client = OKXClient()
    instruments = client.getInstruments(inst_type="SWAP")

Async Usage:
    from adaptor.okx import AsyncOKXClient
    async with AsyncOKXClient() as client:
        instruments = await client.getInstruments(inst_type="SWAP")
"""

from .client import OKXClient, AsyncOKXClient, OKXClientError, OKXAPIError

__all__ = [
    'OKXClient',
    'AsyncOKXClient',
    'OKXClientError',
    'OKXAPIError',
]

__version__ = '1.0.0'
