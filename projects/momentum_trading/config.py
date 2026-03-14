"""Configuration for momentum trading backtester."""

import os
from datetime import datetime, timezone

# Database
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_mHEynLCfk69D@ep-sweet-unit-a1qsqfu5-pooler.ap-southeast-1.aws.neon.tech/trading?sslmode=require"
)

# Binance API
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
EXCHANGE_INFO_URL = f"{BINANCE_FUTURES_BASE}/fapi/v1/exchangeInfo"
TICKER_24H_URL = f"{BINANCE_FUTURES_BASE}/fapi/v1/ticker/24hr"
KLINES_URL = f"{BINANCE_FUTURES_BASE}/fapi/v1/klines"

# Data parameters
START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
START_TIME_MS = 1735689600000  # 2025-01-01 00:00 UTC
INTERVAL = "4h"
KLINE_LIMIT = 1500

# Volume filter (USDT)
MIN_DAILY_VOLUME = 1_000_000   # $1M
MAX_DAILY_VOLUME = 20_000_000  # $20M

# Backtester settings
INITIAL_CAPITAL = 10_000
POSITION_SIZE_PCT = 0.10  # 10% of capital per trade
TRADING_FEE = 0.0004      # 0.04% taker fee
SLIPPAGE = 0.001          # 0.1% slippage for low-liq

# Strategy parameters
STRATEGY_PARAMS = {
    "volume_breakout": {
        "volume_mult": 3.0,
        "lookback": 20,
        "atr_mult": 2.0,
        "max_hold": 5,
    },
    "rsi_momentum": {
        "rsi_period": 14,
        "rsi_entry": 60,
        "rsi_exit": 45,
        "ema_period": 50,
        "atr_mult": 2.0,
    },
    "vwap_breakout": {
        "vwap_period": 20,
        "atr_mult": 1.0,
        "volume_mult": 2.0,
        "max_hold": 8,
    },
    "obv_divergence": {
        "consolidation_period": 10,
        "breakout_mult": 1.0,
    },
    "multi_factor": {
        "roc_period": 10,
        "rsi_period": 14,
        "ema_period": 20,
        "entry_threshold": 0.7,
        "exit_threshold": 0.3,
    },
}

# Visualization
DARK_THEME = {
    "facecolor": "#1a1a2e",
    "textcolor": "#e0e0e0",
    "gridcolor": "#333355",
    "colors": ["#4cc9f0", "#f72585", "#7209b7", "#3a0ca3", "#4361ee", "#4895ef"],
}
