"""
Scoring System

Contains scoring thresholds and calculation logic.
"""

from .thresholds import (
    VOLATILITY_THRESHOLDS,
    ATR_THRESHOLDS,
    PRICE_RANGE_THRESHOLDS,
    TREND_STRENGTH_THRESHOLDS,
    JUMP_FREQUENCY_THRESHOLDS,
    DEVIATION_THRESHOLDS,
    SIGNAL_THRESHOLDS
)
from .scorer import score_metric, determine_signal

__all__ = [
    "VOLATILITY_THRESHOLDS",
    "ATR_THRESHOLDS",
    "PRICE_RANGE_THRESHOLDS",
    "TREND_STRENGTH_THRESHOLDS",
    "JUMP_FREQUENCY_THRESHOLDS",
    "DEVIATION_THRESHOLDS",
    "SIGNAL_THRESHOLDS",
    "score_metric",
    "determine_signal",
]

