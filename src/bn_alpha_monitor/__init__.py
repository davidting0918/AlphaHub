"""
Binance Alpha Stability Monitor

A comprehensive monitoring system for evaluating cryptocurrency token price stability
within a 15-minute time window for hedge trading strategies.
"""

from .core import AlphaStabilityIndicator
from .monitor import StabilityMonitor
from .models import StabilityResult, MonitorBatchResult, Signal

__version__ = "1.0.0"

__all__ = [
    "AlphaStabilityIndicator",
    "StabilityMonitor",
    "StabilityResult",
    "MonitorBatchResult",
    "Signal",
]

