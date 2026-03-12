"""
AlphaHub Data Pipelines

This package contains data collection pipelines for fetching and storing
exchange data (instruments, funding rates, etc.) into the database.

Available pipelines:
- okx_instruments: Fetch and upsert OKX instruments (SWAP + SPOT)
- okx_funding_rates: Fetch and store OKX funding rate history

Usage:
    python -m pipeline.okx_instruments
    python -m pipeline.okx_funding_rates --backfill
"""

from .base import BasePipeline
from .notify import send_telegram

__all__ = [
    'BasePipeline',
    'send_telegram',
]
