# Momentum Trading Backtester

A momentum trading strategy backtester for low-liquidity crypto pairs on Binance Futures.

## Overview

This project identifies low-liquidity crypto pairs ($1M-$20M daily volume) where momentum signals can be caught early - before big moves happen. These pairs are liquid enough to trade but not so crowded that signals get arbitraged out immediately.

## Data

- **Source:** Binance Futures USDT perpetuals (public API)
- **Period:** 2025-01-01 to present (~14 months)
- **Interval:** 4h klines
- **Pairs:** 379 low-mid liquidity pairs filtered by volume
- **Storage:** PostgreSQL database (773k+ klines)

## Strategies

### 1. RSI Momentum (Best Performer: +$24k, 0.36 Sharpe)
- Signal: RSI(14) crosses above 60 from below
- Confirm: Price above EMA(50), Volume above average
- Entry: Long at next candle open
- Exit: RSI drops below 45 OR trailing stop 2x ATR

### 2. VWAP Breakout (+$14k, 0.35 Sharpe)
- Calculate rolling VWAP (20-period)
- Signal: Price breaks above VWAP + 1 ATR AND volume > 2x average
- Entry: Long at next candle open
- Exit: Price drops below VWAP OR 8 candles max

### 3. Multi-Factor Momentum Score (+$10k, 0.22 Sharpe)
- Combine: Rate of Change (10p), RSI(14), Volume Ratio, Price vs EMA(20)
- Score each factor 0-1, average = momentum score
- Entry: Score > 0.7
- Exit: Score < 0.3

### 4. Volume Breakout (-$6k)
- Signal: Volume > 3x 20-period average AND price > 20-period high
- Entry: Long at next candle open
- Exit: Trailing stop 2x ATR OR 5 candles max
- Note: Needs refinement

### 5. OBV Divergence (No trades)
- Logic needs refinement to generate signals

## Top Performing Pairs

1. **AUCTIONUSDT** - $6,600+ total PnL across strategies
2. **ACHUSDT** - $4,700+ 
3. **BANUSDT** - $4,100+
4. **CAKEUSDT** - $3,800+
5. **1000LUNCUSDT** - $2,700+

## Usage

```bash
cd /home/ubuntu/AlphaHub
source venv/bin/activate
cd projects/momentum_trading

# Fetch data (run once, takes ~45 min for all pairs)
python data_fetcher.py

# Run fast backtest (top 50 pairs, ~15 min)
python run_fast.py

# Run full backtest (all 379 pairs, ~2 hours)
python run.py
```

## Output

Results are saved to `output/`:
- `backtest_results.csv` - Per-pair, per-strategy results
- `strategy_summary.csv` - Aggregated strategy metrics
- `strategy_comparison.png` - PnL, Sharpe, Win Rate comparison
- `equity_curves.png` - Equity curves over time
- `top_pairs.png` - Best performing pairs
- `monthly_returns_heatmap.png` - Monthly returns by strategy
- `trade_distribution.png` - Trade PnL histogram
- `volume_vs_return.png` - Volume correlation analysis
- `signal_examples.png` - Example trade charts

## Backtest Settings

- Initial capital: $10,000
- Position size: 10% of capital per trade
- Trading fees: 0.04% taker (Binance futures)
- Slippage: 0.1% (higher for low-liq)
- Direction: LONG only

## Dependencies

```
asyncpg
httpx
pandas
numpy
matplotlib
scipy
```

## Key Findings

1. **RSI Momentum** works best for catching early momentum in low-liq pairs
2. **VWAP Breakout** provides consistent returns with lower drawdown
3. **Volume Breakout** alone is not sufficient - needs additional confirmation
4. Best pairs for momentum trading: AUCTION, ACH, BAN, CAKE
5. 4h timeframe provides good balance of signal vs noise
