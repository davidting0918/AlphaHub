"""
Configuration Module

All configuration loaded from environment variables or .env file.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import List

# Load .env if exists
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


@dataclass
class OKXConfig:
    api_key: str = os.getenv("OKX_API_KEY", "")
    secret_key: str = os.getenv("OKX_SECRET_KEY", "")
    passphrase: str = os.getenv("OKX_PASSPHRASE", "")
    demo_flag: str = os.getenv("OKX_DEMO_FLAG", "1")  # 1 = demo trading
    base_url: str = "https://www.okx.com"


@dataclass
class DBConfig:
    url: str = os.getenv("DATABASE_URL", "")


@dataclass
class TelegramConfig:
    bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")


@dataclass
class TradingConfig:
    portfolio_name: str = os.getenv("PORTFOLIO_NAME", "OKXTEST_MAIN_01")
    
    # Risk parameters
    max_position_pct: float = 0.10  # 10% of equity per trade
    max_exposure_pct: float = 0.30  # 30% total exposure
    max_concurrent_positions: int = 5
    daily_loss_limit_pct: float = 0.05  # 5% daily loss limit
    default_leverage: int = 3
    
    # Trading pairs
    instruments: List[str] = None
    
    def __post_init__(self):
        if self.instruments is None:
            self.instruments = [
                "BTC-USDT-SWAP",
                "ETH-USDT-SWAP",
                "SOL-USDT-SWAP",
                "DOGE-USDT-SWAP",
                "XRP-USDT-SWAP",
            ]


# Global config instances
okx_config = OKXConfig()
db_config = DBConfig()
telegram_config = TelegramConfig()
trading_config = TradingConfig()
