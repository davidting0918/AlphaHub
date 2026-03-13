"""
Options Strategies — Central Configuration

Configuration constants for all options strategy modules:
- screener.py
- analyzer.py  
- covered_call_backtester.py
- iv_rv_backtester.py
"""

# ==================== Exchange Constants ====================

# Deribit is the primary options exchange
EXCHANGE_NAME = "DERIBIT"
EXCHANGE_ID = 5  # Adjust based on actual ID in exchanges table

# Supported underlyings
UNDERLYINGS = ["BTC", "ETH"]

# ==================== Filters ====================

# Minimum data points required for analysis
MIN_DATA_POINTS = 10

# Minimum IV to consider (filter out stale/zero IV)
MIN_IV = 0.05  # 5% annualized

# Maximum IV to consider (filter out obvious errors)
MAX_IV = 10.0  # 1000% annualized

# Minimum open interest for liquidity filtering
MIN_OPEN_INTEREST = 1.0

# Minimum volume for liquidity filtering
MIN_VOLUME_24H = 0.5

# Delta thresholds for categorization
DELTA_ATM_LOW = 0.4
DELTA_ATM_HIGH = 0.6
DELTA_OTM_THRESHOLD = 0.3

# Days to expiry filters
MIN_DTE = 1      # Exclude options expiring today
MAX_DTE = 365    # Exclude options > 1 year out

# ==================== Costs ====================

# Deribit fee structure
MAKER_FEE = 0.0003  # 0.03% for options
TAKER_FEE = 0.0003  # 0.03% for options
DELIVERY_FEE = 0.00015  # 0.015% delivery/exercise fee

# Spot trading fees (for covered call spot leg)
SPOT_FEE = 0.001  # 0.1% taker

# Slippage estimate per side
OPTION_SLIPPAGE = 0.002  # 0.2% bid-ask slippage on premium

# Total round-trip cost for option trades
# Open + close, taker both sides
ROUND_TRIP_FEE = 2 * TAKER_FEE

# ==================== Covered Call Defaults ====================

COVERED_CALL_DEFAULTS = {
    "underlying": "BTC",
    "target_delta": 0.2,     # OTM call delta target
    "expiry_period": 30,     # days to expiry
    "roll_timing": 1,        # roll N days before expiry
    "risk_free_rate": 0.045, # T-bill proxy
    "initial_capital": 100_000,
    "spot_fee": SPOT_FEE,
    "option_fee": TAKER_FEE,
    "slippage": OPTION_SLIPPAGE,
    "start_date": "2025-01-01",
}

# ==================== IV-RV Strategy Defaults ====================

IV_RV_DEFAULTS = {
    "underlying": "BTC",
    "iv_rv_threshold": 0.10,   # 10pp IV-RV spread
    "strike_width": 1.0,       # OTM strikes in std devs
    "expiry_days": 14,         # target DTE
    "stop_loss_multiple": 2.0, # close if loss > N × premium
    "max_positions": 3,
    "position_size_pct": 0.10, # 10% of capital per position
    "risk_free_rate": 0.045,
    "initial_capital": 100_000,
    "option_fee": TAKER_FEE,
    "slippage": OPTION_SLIPPAGE,
    "rv_window": 30,           # realized vol lookback
    "start_date": "2025-01-01",
}

# ==================== Screener Thresholds ====================

# IV percentile thresholds
IV_PERCENTILE_HIGH = 75   # Above this = high IV
IV_PERCENTILE_LOW = 25    # Below this = low IV

# IV-RV spread thresholds for opportunities
IVRV_SPREAD_HIGH = 0.10   # 10pp = premium selling opportunity
IVRV_SPREAD_LOW = -0.05   # -5pp = premium buying opportunity

# Skew thresholds
PUT_CALL_SKEW_THRESHOLD = 0.05  # 5pp difference = notable skew

# Term structure thresholds
TERM_STRUCTURE_INVERSION = -0.03  # Near IV > Far IV by this = backwardation

# Top N results to show
TOP_N = 20

# ==================== Analyzer Settings ====================

# RV calculation window (days)
RV_WINDOW_SHORT = 14
RV_WINDOW_MEDIUM = 30
RV_WINDOW_LONG = 60

# Moneyness buckets for smile analysis
MONEYNESS_BUCKETS = [0.80, 0.90, 0.95, 1.00, 1.05, 1.10, 1.20]
MONEYNESS_LABELS = ["80%", "90%", "95%", "ATM", "105%", "110%", "120%"]

# Term structure buckets (DTE)
DTE_BUCKETS = [7, 14, 30, 60, 90, 180, 365]
DTE_LABELS = ["1W", "2W", "1M", "2M", "3M", "6M", "1Y"]

# ==================== Chart Styling (Dark Theme) ====================

CHART_COLORS = {
    "background": "#1a1a2e",
    "panel": "#16213e",
    "text": "#e0e0e0",
    "grid": "#333333",
    "green": "#00d4aa",
    "red": "#ff4757",
    "gold": "#ffd700",
    "blue": "#4dabf7",
    "purple": "#b197fc",
    "cyan": "#22b8cf",
}

# ==================== Output Configuration ====================

OUTPUT_DIR = "output"
SCREENER_OUTPUT_DIR = "output/screener"
ANALYZER_OUTPUT_DIR = "output/analyzer"
COVERED_CALL_OUTPUT_DIR = "output/covered_call"
IV_RV_OUTPUT_DIR = "output/iv_rv"

# File formats
SAVE_CSV = True
SAVE_JSON = True
SAVE_CHARTS = True
CHART_DPI = 150

# ==================== Annualization ====================

# Trading days per year (for vol calculations)
TRADING_DAYS = 365  # Crypto trades 365 days

# Annualization factor for various periods
ANNUAL_FACTOR = 365
