"""
Options Backtester — Configuration

Strategy parameters, API endpoints, costs, and thresholds.
"""

# ==================== API Endpoints ====================

# Deribit (main source - best options data)
DERIBIT_BASE = "https://www.deribit.com/api/v2/public"
DERIBIT_ENDPOINTS = {
    "instruments": f"{DERIBIT_BASE}/get_instruments",          # list all options
    "book_summary": f"{DERIBIT_BASE}/get_book_summary_by_currency",  # IV, greeks
    "ticker": f"{DERIBIT_BASE}/ticker",                        # real-time ticker
    "index_price": f"{DERIBIT_BASE}/get_index_price",          # BTC/ETH index
    "historical_vol": f"{DERIBIT_BASE}/get_historical_volatility",   # RV
    "chart_data": f"{DERIBIT_BASE}/get_tradingview_chart_data",  # OHLCV
}

# Binance Options (European-style)
BINANCE_BASE = "https://eapi.binance.com/eapi/v1"
BINANCE_ENDPOINTS = {
    "exchange_info": f"{BINANCE_BASE}/exchangeInfo",  # list all options
    "ticker": f"{BINANCE_BASE}/ticker",               # option tickers
    "klines": f"{BINANCE_BASE}/klines",               # option klines
    "mark": f"{BINANCE_BASE}/mark",                   # mark price with greeks
}

# ==================== Supported Assets ====================

ASSETS = ["BTC", "ETH"]
DEFAULT_ASSET = "BTC"

# ==================== Strategy Parameters ====================

# Initial capital per strategy
INITIAL_CAPITAL = 10_000  # USD

# Option selection parameters
OTM_DELTA_CALL = 0.30      # Target delta for OTM calls (covered call)
OTM_DELTA_PUT = -0.30      # Target delta for OTM puts (cash-secured put)
OTM_DELTA_STRANGLE = 0.15  # Delta for strangle wings
OTM_DELTA_CONDOR_INNER = 0.20  # Inner wings for iron condor
OTM_DELTA_CONDOR_OUTER = 0.10  # Outer wings for iron condor

# Days to expiry targets
MIN_DTE = 7                # Minimum days to expiry
MAX_DTE = 45               # Maximum days to expiry
TARGET_DTE = 30            # Preferred days to expiry

# IV-RV Spread thresholds for short strangle entry
IV_RV_ENTRY_THRESHOLD = 0.10    # Enter when IV > RV + 10%
IV_RV_EXIT_THRESHOLD = 0.02     # Exit when IV-RV < 2%

# Position sizing
MAX_CONTRACTS_PER_TRADE = 1.0   # BTC-equivalent notional per position
MARGIN_REQUIREMENT = 0.15       # 15% margin for naked options

# ==================== Costs ====================

# Trading fees (taker) - both exchanges
FEE_RATE_DERIBIT = 0.0003       # 0.03% of underlying notional
FEE_RATE_BINANCE = 0.0003       # 0.03%

# Slippage estimate
SLIPPAGE_PCT = 0.005            # 0.5% of option premium

# ==================== Risk Management ====================

# Stop loss multipliers
STOP_LOSS_PREMIUM_MULTIPLE = 2.0    # Exit if loss > 2x premium collected
MAX_DRAWDOWN_PCT = 0.20              # 20% max drawdown per strategy

# ==================== Backtest Settings ====================

# Simulation parameters
SIMULATION_DAYS = 90                # Days to simulate
REBALANCE_FREQ_DAYS = 7             # Roll positions every 7 days
SETTLEMENT_FREQ_HOURS = 8           # Check positions every 8 hours

# Risk-free rate for Black-Scholes
RISK_FREE_RATE = 0.05               # 5% annualized

# ==================== Output ====================

# Chart styling (matches funding_rate_arb)
CHART_STYLE = {
    "facecolor": "#1a1a2e",
    "ax_facecolor": "#16213e",
    "text_color": "#e0e0e0",
    "positive_color": "#00d4aa",
    "negative_color": "#ff4757",
    "neutral_color": "#ffd700",
    "grid_color": "#333333",
}

# Top N results to display
TOP_N = 20

# ==================== API Rate Limits ====================

# Requests per second (conservative)
DERIBIT_RPS = 5
BINANCE_RPS = 10

# Sleep between batches
API_SLEEP_MS = 200
