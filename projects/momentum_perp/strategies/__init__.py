"""
Trading Strategies Package

Contains all momentum-based trading strategies.
"""

from .base import BaseStrategy, Signal, SignalType
from .breakout_momentum import BreakoutMomentumStrategy
from .ema_cross_rsi import EMACrossRSIStrategy
from .vwap_deviation import VWAPDeviationStrategy
from .multi_tf_trend import MultiTimeframeTrendStrategy
from .volume_profile import VolumeProfileMomentumStrategy

__all__ = [
    "BaseStrategy",
    "Signal",
    "SignalType",
    "BreakoutMomentumStrategy",
    "EMACrossRSIStrategy",
    "VWAPDeviationStrategy",
    "MultiTimeframeTrendStrategy",
    "VolumeProfileMomentumStrategy",
]

STRATEGIES = {
    "breakout": BreakoutMomentumStrategy,
    "ema_cross": EMACrossRSIStrategy,
    "vwap": VWAPDeviationStrategy,
    "multi_tf": MultiTimeframeTrendStrategy,
    "volume": VolumeProfileMomentumStrategy,
}
