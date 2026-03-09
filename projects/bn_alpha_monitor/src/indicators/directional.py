"""
Directional Indicators

Implements price direction and trend metrics:
- Trend Strength Index
"""

from typing import List, Dict, Any


def calculate_trend_strength(klines_1m: List[Dict[str, Any]]) -> float:
    """
    Calculate trend strength index
    
    Evaluates whether price is in a sustained uptrend, downtrend, or consolidation.
    Strong trends (even if stable) indicate higher risk for hedge trading.
    
    Args:
        klines_1m: List of 1-minute klines (15 candles)
        
    Returns:
        Trend strength as a percentage (0-1.0, where 0 = consolidation, 1.0 = extreme trend)
    """
    if not klines_1m or len(klines_1m) < 1:
        return 0.0
    
    # Count bullish and bearish candles
    bullish_count = 0
    bearish_count = 0
    
    for candle in klines_1m:
        if candle["close"] > candle["open"]:
            bullish_count += 1
        elif candle["close"] < candle["open"]:
            bearish_count += 1
        # Neutral candles (close == open) are not counted
    
    total_candles = len(klines_1m)
    
    # Calculate directional bias
    directional_bias = abs(bullish_count - bearish_count) / total_candles
    
    return directional_bias

