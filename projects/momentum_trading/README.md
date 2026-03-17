# Momentum Trading Backtester

Backtests momentum strategies on low-liquidity crypto pairs from Binance Futures. Targets the $1M–$20M daily volume range where momentum signals can be caught early.

## Overview

Low-liquidity pairs are liquid enough to trade but not so crowded that signals get arbitraged out immediately. This project fetches historical data, runs multiple momentum strategies, and generates performance reports.

## Data

- **Source:** Binance Futures USDT perpetuals (public API)
- **Period:** 2025-01-01 to present (~14 months)
- **Interval:** 4h klines
- **Pairs:** 379 low-mid liquidity pairs filtered by volume
- **Storage:** PostgreSQL database (773k+ klines)

## Strategies

### 1. RSI Momentum (Best Performer: +$24k, 0.36 Sharpe)
- **Signal:** RSI(14) crosses above 60 from below
- **Confirm:** Price above EMA(50), Volume above average
- **Exit:** RSI drops below 45 OR trailing stop 2× ATR

### 2. VWAP Breakout (+$14k, 0.35 Sharpe)
- **Signal:** Price breaks above VWAP + 1 ATR with 2× volume
- **Exit:** Price drops below VWAP OR 8 candles max

### 3. Multi-Factor Momentum Score (+$10k, 0.22 Sharpe)
- **Score:** ROC(10) + RSI(14) + Volume Ratio + Price vs EMA(20)
- **Entry:** Score > 0.7 | **Exit:** Score < 0.3

### 4. Volume Breakout (-$6k)
- **Signal:** Volume > 3× 20-period avg AND price > 20-period high
- **Exit:** Trailing stop 2× ATR OR 5 candles max
- Needs refinement — too many false breakouts

### 5. OBV Divergence (No trades)
- Logic needs refinement to generate signals

## Key Findings

### Performance Summary

| Strategy | Total PnL | Sharpe | Status |
|----------|-----------|--------|--------|
| RSI Momentum | +$24,000 | 0.36 | ✅ Best performer |
| VWAP Breakout | +$14,000 | 0.35 | ✅ Consistent |
| Multi-Factor | +$10,000 | 0.22 | ✅ Moderate |
| Volume Breakout | -$6,000 | Negative | ❌ Needs work |
| OBV Divergence | N/A | N/A | ❌ No signals |

### Top Performing Pairs

1. **AUCTIONUSDT** — $6,600+ total PnL across strategies
2. **ACHUSDT** — $4,700+
3. **BANUSDT** — $4,100+
4. **CAKEUSDT** — $3,800+
5. **1000LUNCUSDT** — $2,700+

### Conclusions

1. **RSI Momentum is the clear winner** for low-liquidity crypto momentum trading. The combination of RSI threshold crossing + trend confirmation (EMA) + volume filter produces the most reliable signals with a 0.36 Sharpe ratio.

2. **VWAP Breakout provides consistent returns with lower drawdown.** It's more conservative than RSI Momentum but works well as a complementary strategy.

3. **Volume breakout alone is not sufficient.** Volume spikes in low-liquidity tokens are often noise (wash trading, single large orders) rather than genuine momentum. Additional confirmation filters are needed.

4. **4h timeframe is the sweet spot** for these pairs. Shorter timeframes (1h, 15m) are too noisy for low-liquidity tokens. Daily is too slow to catch momentum.

5. **Slippage is the practical limiting factor.** Backtested with 0.1% slippage, but real execution on $1-20M volume pairs could see 0.2-0.5%+, which would reduce actual performance significantly.

6. **LONG-only works in this universe.** These low-liquidity altcoins have a structural upside bias during momentum events. Short-selling would face high borrowing costs and squeeze risk.

## Usage

```bash
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
- `strategy_comparison.png` — PnL, Sharpe, Win Rate comparison
- `equity_curves.png` — Equity curves over time
- `top_pairs.png` — Best performing pairs
- `monthly_returns_heatmap.png` — Monthly returns by strategy
- `trade_distribution.png` — Trade PnL histogram
- `volume_vs_return.png` — Volume correlation analysis

## Backtest Settings

| Parameter | Value |
|-----------|-------|
| Initial capital | $10,000 |
| Position size | 10% of capital |
| Trading fees | 0.04% taker (Binance Futures) |
| Slippage | 0.1% (conservative for low-liq) |
| Direction | LONG only |

## Limitations

- **No output charts committed yet** — run the backtest to generate them
- **Backtest uses historical data only** — no live paper trading validation
- **Slippage model is conservative** — real slippage on thin books could be worse
- **No short strategies** — only long-side momentum tested
- **Pair universe changes** — new listings/delistings not handled dynamically
