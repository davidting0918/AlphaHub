"""
AlphaHub Data Pipelines

Modular job system for fetching and storing exchange data.

Usage:
    python -m pipeline.job_manager --name OKX_MAIN_01 instrument
    python -m pipeline.job_manager --name OKX_MAIN_01 --start 20260301 --end 20260313 funding_rate
"""

from .base_job import BaseJob
from .jobs.instrument_job import InstrumentJob
from .jobs.funding_rate_job import FundingRateJob

__all__ = [
    'BaseJob',
    'InstrumentJob',
    'FundingRateJob',
]
