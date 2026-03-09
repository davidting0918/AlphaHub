"""
Volatility Indicators

Implements price volatility metrics:
- Rolling Standard Deviation
- Average True Range (ATR)
- High-Low Price Range
"""

from typing import List, Dict, Any
import statistics


def calculate_rolling_volatility(klines_1m: List[Dict[str, Any]]) -> float:
    """
    Calculate rolling standard deviation volatility
    
    Measures the dispersion of closing prices relative to the mean price
    over a 15-minute window.
    
    Args:
        klines_1m: List of 1-minute klines (15 candles)
        
    Returns:
        Volatility as a percentage (e.g., 0.0025 = 0.25%)
    """
    if not klines_1m or len(klines_1m) < 2:
        return 0.0
    
    close_prices = [k["close"] for k in klines_1m]
    
    # Calculate standard deviation
    std_dev = statistics.stdev(close_prices)
    mean_price = statistics.mean(close_prices)
    
    # Return as percentage
    volatility = std_dev / mean_price if mean_price > 0 else 0.0
    return volatility


def calculate_atr(klines_1m: List[Dict[str, Any]]) -> float:
    """
    Calculate Average True Range (ATR)
    
    Measures the average true price range, considering gaps between candles.
    
    Args:
        klines_1m: List of 1-minute klines (15 candles)
        
    Returns:
        ATR as a percentage of current price
    """
    if not klines_1m or len(klines_1m) < 2:
        return 0.0
    
    true_ranges = []
    
    for i in range(1, len(klines_1m)):
        current = klines_1m[i]
        previous = klines_1m[i - 1]
        
        # Three options for True Range
        option_a = current["high"] - current["low"]
        option_b = abs(current["high"] - previous["close"])
        option_c = abs(current["low"] - previous["close"])
        
        # True Range is the maximum of the three
        true_range = max(option_a, option_b, option_c)
        true_ranges.append(true_range)
    
    # Calculate average
    atr = statistics.mean(true_ranges)
    
    # Get current price (last close)
    current_price = klines_1m[-1]["close"]
    
    # Return as percentage
    atr_percent = atr / current_price if current_price > 0 else 0.0
    return atr_percent


def calculate_price_range(klines_1m: List[Dict[str, Any]]) -> float:
    """
    Calculate high-low price range percentage
    
    Measures the difference between the highest and lowest prices
    in the 15-minute window.
    
    Args:
        klines_1m: List of 1-minute klines (15 candles)
        
    Returns:
        Price range as a percentage
    """
    if not klines_1m:
        return 0.0
    
    # Find global high and low
    highest = max(k["high"] for k in klines_1m)
    lowest = min(k["low"] for k in klines_1m)
    
    # Calculate mean price for reference
    close_prices = [k["close"] for k in klines_1m]
    mean_price = statistics.mean(close_prices)
    
    # Calculate range as percentage
    price_range = (highest - lowest) / mean_price if mean_price > 0 else 0.0
    return price_range

