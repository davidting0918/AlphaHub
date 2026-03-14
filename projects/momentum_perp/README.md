# Momentum Perp Trading System

A production-quality momentum-based perpetual futures trading system with multiple strategies, risk management, and Telegram reporting.

## Overview

This system trades perpetual futures on OKX (testnet) using 5 momentum-based strategies:

1. **Breakout Momentum** - Price breakout with volume confirmation
2. **EMA Cross + RSI** - Moving average crossovers filtered by RSI
3. **VWAP Deviation** - Mean reversion from VWAP bands
4. **Multi-Timeframe Trend** - Higher TF trend + lower TF entry
5. **Volume Profile** - Volume spikes + price momentum

## Quick Start

```bash
# Activate virtual environment
cd /home/ubuntu/clawd/repos/AlphaHub
source .venv/bin/activate

# Navigate to project
cd projects/momentum_perp

# Run database migrations
python run.py --migrate

# Test connections
python run.py --test

# Run all strategies (continuous)
python run.py

# Run single strategy
python run.py --strategy breakout

# Run once and exit
python run.py --once

# Send PnL report
python run.py --report

# Take balance snapshot
python run.py --snapshot
```

## Architecture

```
momentum_perp/
├── config.py           # Configuration from .env
├── okx_trader.py       # OKX authenticated trading client
├── strategies/         # Trading strategies
│   ├── base.py                 # Base strategy + indicators
│   ├── breakout_momentum.py    # Strategy 1
│   ├── ema_cross_rsi.py        # Strategy 2
│   ├── vwap_deviation.py       # Strategy 3
│   ├── multi_tf_trend.py       # Strategy 4
│   └── volume_profile.py       # Strategy 5
├── risk_manager.py     # Position sizing & risk controls
├── reporter.py         # Telegram notifications
├── db_manager.py       # Database operations
├── engine.py           # Main trading engine
└── run.py              # CLI entry point
```

## Configuration

All configuration is loaded from `.env`:

```env
# OKX API (Testnet)
OKX_API_KEY=your_api_key
OKX_SECRET_KEY=your_secret
OKX_PASSPHRASE=your_passphrase
OKX_DEMO_FLAG=1  # 1=testnet, 0=live

# Database
DATABASE_URL=postgresql://...

# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Portfolio
PORTFOLIO_NAME=OKXTEST_MAIN_01
```

## Risk Management

- **Max position size:** 10% of equity per trade
- **Max total exposure:** 30% of equity
- **Max concurrent positions:** 5
- **Daily loss limit:** 5% of equity
- **Stop loss:** Required on every trade
- **Default leverage:** 3x

## Strategies

### 1. Breakout Momentum (1H)
- Detects N-period high/low breakouts
- Confirms with 2x volume spike
- 2:1 risk/reward ratio
- ATR-based stops

### 2. EMA Cross + RSI (15m)
- Fast EMA (9) / Slow EMA (21) crossover
- RSI(14) filter: entry only when 30 < RSI < 70
- Trailing stop based on ATR
- Exit on opposite cross

### 3. VWAP Deviation (5m)
- Scalping strategy
- Entry at 2σ deviation from VWAP
- Target: return to VWAP
- Stop: 3σ deviation

### 4. Multi-Timeframe Trend (4H + 15m)
- 4H: Trend direction (EMA 50/200)
- 15m: Pullback entry (RSI oversold/overbought)
- Only trades with higher TF trend

### 5. Volume Profile Momentum (1H)
- Volume spike detection (2x average)
- Price momentum via ROC
- ADX trend strength filter
- Exits on momentum reversal

## Database Tables

- `account_snapshots` - Periodic balance snapshots
- `positions` - Open position tracking
- `trading_orders` - Order execution records
- `strategy_signals` - Signal log for analysis

## Telegram Notifications

The system sends notifications for:
- Trade executions
- Position opens/closes with PnL
- Daily summaries
- Balance snapshots
- Risk alerts
- Errors

## Instruments

Default trading pairs:
- BTC-USDT-SWAP
- ETH-USDT-SWAP
- SOL-USDT-SWAP
- DOGE-USDT-SWAP
- XRP-USDT-SWAP

## Development

### Adding a New Strategy

1. Create `strategies/my_strategy.py`:

```python
from .base import BaseStrategy, Signal, SignalType

class MyStrategy(BaseStrategy):
    name = "my_strategy"
    timeframe = "1H"
    
    def analyze(self, klines, current_position=None, secondary_klines=None) -> Signal:
        # Your logic here
        return self.no_signal(instrument)
```

2. Register in `strategies/__init__.py`
3. Add interval in `engine.py`

### Running Tests

```bash
python run.py --test
```

## Important Notes

- System uses OKX testnet by default (`flag='1'`)
- All DB operations are tagged with portfolio_name
- Positions are synced from exchange on each cycle
- Graceful shutdown on SIGTERM/SIGINT

## Portfolio

- **Name:** `OKXTEST_MAIN_01`
- **Testnet Balance:** ~29,490 USDT
- **Mode:** Demo/Simulated Trading

## License

Internal use only.
