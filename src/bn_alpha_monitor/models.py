"""
Data Models and Type Definitions

Defines data structures, enums, and constants used throughout the monitoring system.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class Signal(str, Enum):
    """Trading signal levels"""
    GREEN = "green"    # Safe to trade
    YELLOW = "yellow"  # Caution advised
    RED = "red"        # Do not trade


@dataclass
class MetricResult:
    """Individual metric calculation result"""
    value: float
    score: float
    weight: float
    
    def to_dict(self) -> Dict[str, float]:
        return {
            "value": round(self.value, 6),
            "score": round(self.score, 2),
            "weight": self.weight
        }


@dataclass
class StabilityResult:
    """Complete stability analysis result for a single symbol"""
    symbol: str
    alpha_id: str
    timestamp: int
    signal: Signal
    composite_score: float
    metrics: Dict[str, MetricResult]
    recommendation: str
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary"""
        if self.error:
            return {
                "symbol": self.symbol,
                "alpha_id": self.alpha_id,
                "timestamp": self.timestamp,
                "error": self.error
            }
        
        return {
            "symbol": self.symbol,
            "alpha_id": self.alpha_id,
            "timestamp": self.timestamp,
            "signal": self.signal.value,
            "composite_score": round(self.composite_score, 2),
            "metrics": {
                name: metric.to_dict()
                for name, metric in self.metrics.items()
            },
            "recommendation": self.recommendation
        }


@dataclass
class MonitorBatchResult:
    """Batch monitoring result for multiple symbols"""
    timestamp: int
    total_symbols: int
    successful: int
    failed: int
    results: List[StabilityResult]
    summary: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary"""
        return {
            "timestamp": self.timestamp,
            "total_symbols": self.total_symbols,
            "successful": self.successful,
            "failed": self.failed,
            "results": [result.to_dict() for result in self.results],
            "summary": self.summary
        }


# Metric Weight Configuration
METRIC_WEIGHTS = {
    "rolling_volatility": 0.30,
    "atr": 0.25,
    "price_range": 0.15,
    "trend_strength": 0.10,
    "price_jump_frequency": 0.10,
    "realtime_deviation": 0.10
}


# Recommendation Messages
RECOMMENDATIONS = {
    Signal.GREEN: "can trade - market is stable",
    Signal.YELLOW: "caution - market is unstable",
    Signal.RED: "do not trade - market is volatile or illiquid"
}

