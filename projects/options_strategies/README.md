# Options Strategies — AlphaHub

Backtesting suite for systematic crypto options strategies using Deribit data.

## Strategies

### 1. Systematic Covered Call (`covered_call_backtester.py`)

**Logic:** Hold BTC/ETH spot and systematically sell OTM call options on a rolling schedule.

- Select strike by target delta (0.1–0.4)
- Roll at configurable intervals (weekly / biweekly / monthly)
- Collect premium income, accept capped upside

**Key Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `--underlying` | BTC | BTC or ETH |
| `--delta` | 0.2 | Target delta for OTM call |
| `--expiry` | 30 | Days to expiry per period |
| `--roll` | 1 | Days before expiry to roll |
| `--start` | 2025-01-01 | Backtest start date |

**Metrics:** Premium yield (annualised), call-away rate, total return vs HODL, Sharpe, max drawdown.

```bash
python3 -m projects.options_strategies.covered_call_backtester --underlying BTC --delta 0.2 --expiry 30
python3 -m projects.options_strategies.covered_call_backtester --underlying ETH --delta 0.3 --expiry 7
```

### 2. IV-RV Spread — Short Strangle (`iv_rv_backtester.py`)

**Logic:** Compare Implied Volatility vs Realized Volatility. When IV exceeds RV by a threshold, sell a strangle (OTM put + OTM call) to collect premium from vol mean-reversion.

- Entry signal: IV - RV > threshold
- Strikes set N standard deviations from spot
- Stop-loss if position loss exceeds N× premium

**Key Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `--underlying` | BTC | BTC or ETH |
| `--threshold` | 0.10 | IV-RV spread trigger (decimal) |
| `--width` | 1.0 | Strike width in std devs |
| `--expiry` | 14 | Days to expiry |
| `--stop-loss` | 2.0 | Stop loss as multiple of premium |

**Metrics:** Win rate, avg premium/loss, expected value, profit factor, Sharpe, IV-RV spread distribution.

```bash
python3 -m projects.options_strategies.iv_rv_backtester --underlying BTC --threshold 0.15 --width 1.5
python3 -m projects.options_strategies.iv_rv_backtester --underlying ETH --threshold 0.10 --expiry 7
```

## Data Requirements

Both backtests require:

1. **Spot price data** — PERP klines in the `klines` table (run `kline` pipeline job first)
2. **Options IV data** — In `options_tickers` table (run `fetch_deribit_options.py` first)

If IV data is unavailable, both strategies fall back to estimating IV from realized volatility.

### Data Pipeline

```bash
# 1. Fetch Deribit options data (instruments + tickers + vol surface)
python3 scripts/fetch_deribit_options.py --currency BTC ETH

# 2. Or use the pipeline job
python3 -m pipeline.jobs.options_data_job --currency BTC ETH
```

## Output

Each backtester generates:
- **Console report** — Summary metrics and trade details
- **CSV** — Period/trade-level data in `output/` subdirectory
- **Charts** — PNG visualizations (equity curves, PnL, IV/RV)

Output directory: `projects/options_strategies/output/`

## Architecture

```
projects/options_strategies/
├── __init__.py
├── README.md
├── covered_call_backtester.py    # Strategy 1
├── iv_rv_backtester.py           # Strategy 2
└── output/                       # Generated results
    ├── covered_call/
    └── iv_rv/
```

## Dependencies

- `numpy`, `pandas` — Numerical computation
- `matplotlib` — Chart generation
- `scipy` — Black-Scholes (norm.cdf)
- `asyncpg` — Database access

All included in the project's `requirements.txt`.
