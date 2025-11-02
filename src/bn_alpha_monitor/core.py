"""
Alpha Stability Indicator Core

Main class that orchestrates all indicator calculations and produces
a comprehensive stability assessment.
"""

from typing import List, Dict, Any
import statistics
import time

from .models import (
    StabilityResult,
    MetricResult,
    Signal,
    METRIC_WEIGHTS,
    RECOMMENDATIONS
)
from .indicators import (
    calculate_rolling_volatility,
    calculate_atr,
    calculate_price_range,
    calculate_trend_strength,
    calculate_price_jump_frequency,
    calculate_realtime_deviation
)
from .scoring import (
    VOLATILITY_THRESHOLDS,
    ATR_THRESHOLDS,
    PRICE_RANGE_THRESHOLDS,
    TREND_STRENGTH_THRESHOLDS,
    JUMP_FREQUENCY_THRESHOLDS,
    DEVIATION_THRESHOLDS,
    SIGNAL_THRESHOLDS,
    score_metric,
    determine_signal
)


class AlphaStabilityIndicator:
    """
    Core stability indicator calculator
    
    Analyzes three types of market data to compute six stability metrics
    and generate a trading signal.
    """
    
    def __init__(
        self,
        symbol: str,
        klines_1m: List[Dict[str, Any]],
        klines_15s: List[Dict[str, Any]],
        agg_trades: List[Dict[str, Any]]
    ):
        """
        Initialize indicator with market data
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            klines_1m: List of 1-minute klines (15 candles for 15-min window)
            klines_15s: List of 15-second klines (20 candles for 5-min window)
            agg_trades: List of aggregated trades (recent 60-120 seconds)
        """
        self.symbol = symbol
        self.klines_1m = klines_1m
        self.klines_15s = klines_15s
        self.agg_trades = agg_trades
        self.timestamp = int(time.time() * 1000)
        
    def analyze(self) -> StabilityResult:
        """
        Perform complete stability analysis
        
        Returns:
            StabilityResult containing all metrics and final signal
        """
        try:
            # Calculate all six metrics
            metrics = self._calculate_all_metrics()
            
            # Calculate composite score
            composite_score = self._calculate_composite_score(metrics)
            
            # Determine signal
            individual_scores = {name: m.score for name, m in metrics.items()}
            signal = determine_signal(composite_score, individual_scores, SIGNAL_THRESHOLDS)
            
            # Get recommendation
            recommendation = RECOMMENDATIONS[signal]
            
            return StabilityResult(
                symbol=self.symbol,
                timestamp=self.timestamp,
                signal=signal,
                composite_score=composite_score,
                metrics=metrics,
                recommendation=recommendation
            )
            
        except Exception as e:
            # Return error result
            return StabilityResult(
                symbol=self.symbol,
                timestamp=self.timestamp,
                signal=Signal.RED,
                composite_score=0.0,
                metrics={},
                recommendation="計算錯誤",
                error=str(e)
            )
    
    def _calculate_all_metrics(self) -> Dict[str, MetricResult]:
        """Calculate all six stability metrics"""
        metrics = {}
        
        # 1. Rolling Volatility (30% weight)
        volatility_value = calculate_rolling_volatility(self.klines_1m)
        volatility_score = score_metric(volatility_value, VOLATILITY_THRESHOLDS)
        metrics["rolling_volatility"] = MetricResult(
            value=volatility_value,
            score=volatility_score,
            weight=METRIC_WEIGHTS["rolling_volatility"]
        )
        
        # 2. ATR (25% weight)
        atr_value = calculate_atr(self.klines_1m)
        atr_score = score_metric(atr_value, ATR_THRESHOLDS)
        metrics["atr"] = MetricResult(
            value=atr_value,
            score=atr_score,
            weight=METRIC_WEIGHTS["atr"]
        )
        
        # 3. Price Range (15% weight)
        price_range_value = calculate_price_range(self.klines_1m)
        price_range_score = score_metric(price_range_value, PRICE_RANGE_THRESHOLDS)
        metrics["price_range"] = MetricResult(
            value=price_range_value,
            score=price_range_score,
            weight=METRIC_WEIGHTS["price_range"]
        )
        
        # 4. Trend Strength (10% weight)
        trend_value = calculate_trend_strength(self.klines_1m)
        trend_score = score_metric(trend_value, TREND_STRENGTH_THRESHOLDS)
        metrics["trend_strength"] = MetricResult(
            value=trend_value,
            score=trend_score,
            weight=METRIC_WEIGHTS["trend_strength"]
        )
        
        # 5. Price Jump Frequency (10% weight)
        jump_value = calculate_price_jump_frequency(self.agg_trades)
        jump_score = score_metric(jump_value, JUMP_FREQUENCY_THRESHOLDS)
        metrics["price_jump_frequency"] = MetricResult(
            value=jump_value,
            score=jump_score,
            weight=METRIC_WEIGHTS["price_jump_frequency"]
        )
        
        # 6. Real-time Deviation (10% weight)
        # Calculate base price from 1m klines
        base_price = self._calculate_base_price()
        deviation_value = calculate_realtime_deviation(self.agg_trades, base_price)
        deviation_score = score_metric(deviation_value, DEVIATION_THRESHOLDS)
        metrics["realtime_deviation"] = MetricResult(
            value=deviation_value,
            score=deviation_score,
            weight=METRIC_WEIGHTS["realtime_deviation"]
        )
        
        return metrics
    
    def _calculate_composite_score(self, metrics: Dict[str, MetricResult]) -> float:
        """
        Calculate weighted composite score
        
        Args:
            metrics: Dictionary of all metric results
            
        Returns:
            Composite score (0-100)
        """
        weighted_sum = sum(
            metric.score * metric.weight
            for metric in metrics.values()
        )
        return weighted_sum
    
    def _calculate_base_price(self) -> float:
        """Calculate base price (15-minute average) for deviation calculation"""
        if not self.klines_1m:
            return 0.0
        
        close_prices = [k["close"] for k in self.klines_1m]
        return statistics.mean(close_prices)

