"""
Deribit Exchange Configuration

Exchange-level constants for the Deribit adapter.
"""

# Exchange identity
EXCHANGE_NAME = "DERIBIT"
EXCHANGE_ADAPTOR = "deribit"

# API
BASE_URL = "https://www.deribit.com/api/v2"

# Rate limiting (unauthenticated)
# Deribit allows ~10 req/s for unauthenticated, ~20 req/s for authenticated
MAX_REQUESTS_PER_SECOND = 8   # conservative
RATE_LIMIT_DELAY = 1.0 / MAX_REQUESTS_PER_SECOND  # ~0.125s

# Supported currencies for options
SUPPORTED_CURRENCIES = ["BTC", "ETH"]

# Index names for spot reference prices
INDEX_MAP = {
    "BTC": "btc_usd",
    "ETH": "eth_usd",
}

# Settlement periods
SETTLEMENT_PERIODS = ["day", "week", "month"]

# Instrument ID prefix
INSTRUMENT_PREFIX = "DERIBIT_OPT"

# Fees (Deribit options, taker)
# Options: 0.03% of underlying or 0.0003 BTC per option, capped at 12.5% of option price
TAKER_FEE_RATE = 0.0003
MAKER_FEE_RATE = 0.0003

# Data fetching defaults
DEFAULT_CHART_RESOLUTION = "60"  # 1 hour candles
BATCH_SIZE_INSTRUMENTS = 500
BATCH_SIZE_TICKERS = 200
