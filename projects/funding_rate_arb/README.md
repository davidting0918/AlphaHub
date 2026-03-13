# Funding Rate Arbitrage Research

Research toolkit for analyzing and backtesting **funding rate arbitrage** on perpetual futures across OKX and Binance.

## Overview

Perpetual futures contracts charge/pay a **funding rate** every 8 hours (3× daily) to keep the contract price anchored to spot. When funding rates are consistently positive, short-perp holders get paid. This creates a delta-neutral yield opportunity:

> **Long Spot + Short Perp** → collect positive funding while remaining market-neutral.

This project provides three tools to evaluate these opportunities:

| Module | Purpose |
|--------|---------|
| `analyzer.py` | Cross-exchange spread analysis (OKX vs Binance) |
| `screener.py` | Single-exchange screening across all perp instruments |
| `backtester.py` | Historical simulation with equity curves, PnL breakdown, and charts |

## Strategy

### Single-Exchange (Spot-Perp)
1. Buy spot asset on exchange
2. Short equal-notional perpetual on the same exchange
3. Collect funding rate payments every 8 hours
4. Net position is delta-neutral (no directional risk)

### Cross-Exchange (Perp-Perp)
1. Long perp on exchange where you get paid (lower funding rate)
2. Short perp on exchange where you collect (higher funding rate)
3. Capture the spread between the two exchanges

## Modules

### `screener.py` — Opportunity Scanner

Screens all active perpetual instruments for funding rate profitability.

```bash
python3 -m projects.funding_rate_arb.screener
```

**Output:**
- Ranked list of instruments by net APR (after fees)
- Positive funding opportunities (long spot + short perp)
- Negative funding opportunities (short spot + long perp)
- Cross-exchange comparison for pairs listed on both OKX and Binance
- Summary statistics per exchange

**Key metrics:** Gross APR, Net APR, Sharpe ratio, positive funding %, recent trend, max drawdown.

### `analyzer.py` — Cross-Exchange Spread Analysis

Finds pairs where funding rates diverge between OKX and Binance.

```bash
python3 -m projects.funding_rate_arb.analyzer
```

**Output:**
- Spread statistics for all overlapping pairs
- Current live opportunities above the minimum threshold
- Direction recommendation (which exchange to long/short)

### `backtester.py` — Historical Simulation

Runs a full backtest of the spot-perp strategy using historical funding rate and kline data.

```bash
# Backtest all instruments across all exchanges
python3 -m projects.funding_rate_arb.backtester

# Filter by exchange
python3 -m projects.funding_rate_arb.backtester --exchange OKX
python3 -m projects.funding_rate_arb.backtester --exchange BINANCEFUTURES

# Backtest a single symbol
python3 -m projects.funding_rate_arb.backtester --exchange BINANCEFUTURES --symbol BTCUSDT
```

**Simulation details:**
- Initial capital: $10,000 per position
- Leverage: 1× (no leverage)
- Entry: buy spot + short perp at first available candle
- Settlement: collect/pay funding every 8 hours
- Exit: close both legs at last available candle
- Fees: 0.1% spot taker + 0.05% perp taker + 0.02% slippage per side

**Output files** (saved to `output/<exchange>/`):
| File | Description |
|------|-------------|
| `backtest_<ts>.csv` | Full results table for all instruments |
| `backtest_<ts>.json` | Detailed results with equity curves (top 30) |
| `overview_<ts>.png` | Dashboard: APR distribution, top-20 bars, Sharpe vs APR scatter, win rate plot |
| `equity_curves_<ts>.png` | Equity curves for the top 10 most profitable instruments |
| `pnl_breakdown_<ts>.png` | Funding revenue vs fees for top 20 instruments |

### `config.py` — Strategy Parameters

Central configuration for the cross-exchange analyzer:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MIN_SPREAD_ABS` | 0.0001 (0.01%) | Minimum spread to flag an opportunity |
| `MIN_DATA_POINTS` | 10 | Minimum funding rate samples required |
| `FEE_RATE_A` (OKX) | 0.05% | OKX taker fee |
| `FEE_RATE_B` (Binance) | 0.04% | Binance taker fee |
| `SLIPPAGE_PER_SIDE` | 0.02% | Estimated slippage |
| `TOP_N` | 20 | Number of results to display |

## Requirements

- Python 3.10+
- PostgreSQL database with populated `instruments`, `funding_rates`, and `klines` tables
- Dependencies: `pandas`, `numpy`, `matplotlib`, `asyncpg`

## Data Pipeline

This project depends on the AlphaHub pipeline for data collection:

1. **Instruments** — `instrument_job` fetches and stores all active perp contracts
2. **Funding Rates** — `funding_rate_job` collects historical + live funding rate data
3. **Klines** — `kline_job` fetches OHLCV candle data for price tracking

All data is stored in PostgreSQL via the `database.client.PostgresClient`.

## Example Results

From a backtest across all exchanges (Binance + OKX):

- **Profitable instruments:** 25+ with positive net PnL after fees
- **Top APR:** 100%+ annualized on select altcoins (BULLA, CL, GUA)
- **Typical range:** 10–50% APR for consistently positive funding pairs
- **Key insight:** Fees are the main drag — instruments need >0.03% avg funding rate per settlement to be profitable after costs

## Limitations

- **Backtest ≠ live trading:** Assumes perfect execution, no liquidation risk, no funding rate prediction
- **Basis risk:** Spot-perp price divergence is assumed to be zero (simplified)
- **Liquidity:** No position sizing based on order book depth
- **Data period:** Results are limited to the historical data available in the database
- **Market impact:** Not modeled — large positions will affect funding rates
