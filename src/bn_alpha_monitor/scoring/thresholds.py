"""
Scoring Thresholds Configuration

Defines threshold values for each metric and signal determination.
All volatility thresholds are in decimal form (e.g., 0.002 = 0.2%)
"""

# Volatility thresholds: rolling standard deviation
VOLATILITY_THRESHOLDS = [
    (0.002, 100),   # < 0.2%: extreme stable
    (0.003, 80),    # 0.2-0.3%: stable
    (0.005, 60),    # 0.3-0.5%: moderate
    (0.008, 30),    # 0.5-0.8%: volatile
    (float('inf'), 0)  # > 0.8%: unstable
]

# ATR thresholds: average true range percentage
ATR_THRESHOLDS = [
    (0.0015, 100),  # < 0.15%: extreme stable
    (0.0025, 80),   # 0.15-0.25%: stable
    (0.004, 60),    # 0.25-0.4%: moderate
    (0.006, 30),    # 0.4-0.6%: volatile
    (float('inf'), 0)  # > 0.6%: unstable
]

# Price range thresholds: high-low difference percentage
PRICE_RANGE_THRESHOLDS = [
    (0.004, 100),   # < 0.4%: extreme stable
    (0.006, 80),    # 0.4-0.6%: stable
    (0.010, 60),    # 0.6-1.0%: moderate
    (0.015, 30),    # 1.0-1.5%: volatile
    (float('inf'), 0)  # > 1.5%: unstable
]

# Trend strength thresholds: directional bias
# Note: Lower trend strength is better (consolidation)
TREND_STRENGTH_THRESHOLDS = [
    (0.20, 100),    # < 20%: consolidation (best)
    (0.40, 80),     # 20-40%: weak trend
    (0.60, 50),     # 40-60%: moderate trend
    (0.80, 20),     # 60-80%: strong trend
    (float('inf'), 0)  # > 80%: extreme trend (worst)
]

# Price jump frequency thresholds: percentage of jumpy trades
JUMP_FREQUENCY_THRESHOLDS = [
    (0.03, 100),    # < 3%: extreme stable
    (0.05, 80),     # 3-5%: stable
    (0.10, 50),     # 5-10%: moderate
    (0.20, 20),     # 10-20%: volatile
    (float('inf'), 0)  # > 20%: erratic
]

# Real-time deviation thresholds: recent price std dev
DEVIATION_THRESHOLDS = [
    (0.001, 100),   # < 0.1%: extreme stable
    (0.002, 80),    # 0.1-0.2%: stable
    (0.004, 50),    # 0.2-0.4%: moderate
    (0.006, 20),    # 0.4-0.6%: volatile
    (float('inf'), 0)  # > 0.6%: erratic
]

# Signal determination thresholds
SIGNAL_THRESHOLDS = {
    "green": {
        "min_composite_score": 80,
        "min_individual_score": 60,
        "max_low_scores": 0  # No individual scores below threshold
    },
    "yellow": {
        "min_composite_score": 60,
        "min_individual_score": 60,
        "max_low_scores": 2,  # Allow 1-2 individual scores below threshold
        "min_extreme_score": 30  # No score can be below 30
    }
    # Red: everything else
}

