"""
Real-time Indicators

Implements micro-level real-time metrics:
- Price Jump Frequency
- Real-time Price Deviation
"""

from typing import List, Dict, Any
import statistics


def calculate_price_jump_frequency(
    agg_trades: List[Dict[str, Any]],
    jump_threshold: float = 0.0005
) -> float:
    """
    Calculate price jump frequency
    
    Counts how often consecutive trades have price changes exceeding a threshold.
    High jump frequency indicates erratic price movement.
    
    Args:
        agg_trades: List of aggregated trades (most recent 60-120 seconds)
        jump_threshold: Minimum price change to count as a "jump" (default 0.05%)
        
    Returns:
        Jump frequency as a ratio (0-1.0)
    """
    if not agg_trades or len(agg_trades) < 2:
        return 0.0
    
    # Sort by timestamp to ensure chronological order
    sorted_trades = sorted(agg_trades, key=lambda x: x["timestamp"])
    
    jump_count = 0
    total_comparisons = len(sorted_trades) - 1
    
    for i in range(1, len(sorted_trades)):
        prev_price = sorted_trades[i - 1]["price"]
        curr_price = sorted_trades[i]["price"]
        
        if prev_price > 0:
            price_change = abs(curr_price - prev_price) / prev_price
            if price_change > jump_threshold:
                jump_count += 1
    
    # Return jump frequency ratio
    jump_frequency = jump_count / total_comparisons if total_comparisons > 0 else 0.0
    return jump_frequency


def calculate_realtime_deviation(
    agg_trades: List[Dict[str, Any]],
    base_price: float
) -> float:
    """
    Calculate real-time price deviation
    
    Measures how much recent trade prices deviate from the 15-minute average price.
    
    Args:
        agg_trades: List of aggregated trades (most recent 60-120 seconds)
        base_price: 15-minute average price for reference
        
    Returns:
        Deviation as a percentage
    """
    if not agg_trades or base_price <= 0:
        return 0.0
    
    # Extract recent trade prices
    recent_prices = [trade["price"] for trade in agg_trades]
    
    if len(recent_prices) < 2:
        return 0.0
    
    # Calculate standard deviation of recent prices
    std_dev = statistics.stdev(recent_prices)
    
    # Return as percentage of base price
    deviation = std_dev / base_price
    return deviation

