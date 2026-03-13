"""
Funding Rate Arbitrage — Strategy Configuration
"""

# Exchange pair to compare
EXCHANGE_A = "OKX"           # exchange_id = 2
EXCHANGE_B = "BINANCEFUTURES"  # exchange_id = 4
EXCHANGE_A_ID = 2
EXCHANGE_B_ID = 4

# Instrument ID prefixes
PREFIX_A = "OKX_PERP_"
PREFIX_B = "BINANCEFUTURES_PERP_"

# Funding rate settlement frequency (per day)
# Both OKX and Binance settle 3x/day (every 8 hours)
SETTLEMENTS_PER_DAY = 3

# ==================== Filters ====================

# Minimum spread (absolute) to consider an opportunity
# 0.0001 = 0.01% per settlement = ~10.95% APR
MIN_SPREAD_ABS = 0.0001

# Minimum data points required for a pair to be analyzed
MIN_DATA_POINTS = 10

# ==================== Costs ====================

# Trading fees (taker) per side — conservative estimates
FEE_RATE_A = 0.0005   # OKX taker: 0.05%
FEE_RATE_B = 0.0004   # Binance taker: 0.04%

# Total round-trip cost: open both sides + close both sides
# = 2 * (FEE_A + FEE_B) for entry + exit
TOTAL_ROUND_TRIP_FEE = 2 * (FEE_RATE_A + FEE_RATE_B)

# Slippage estimate per side
SLIPPAGE_PER_SIDE = 0.0002  # 0.02%
TOTAL_SLIPPAGE = 4 * SLIPPAGE_PER_SIDE  # both sides, entry + exit

# Total cost to enter and exit a position
TOTAL_COST = TOTAL_ROUND_TRIP_FEE + TOTAL_SLIPPAGE

# ==================== Analysis ====================

# Top N pairs to show in reports
TOP_N = 20

# Annualization factor
# APR = spread × SETTLEMENTS_PER_DAY × 365
ANNUALIZATION_FACTOR = SETTLEMENTS_PER_DAY * 365
