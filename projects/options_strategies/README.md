# Options Strategies — AlphaHub

Comprehensive options analysis and backtesting suite for systematic crypto options strategies. Built for Deribit options data with full volatility surface analysis.

## Quick Start

```bash
cd ~/clawd/repos/AlphaHub

# 1. Screen for opportunities
python3 -m projects.options_strategies.screener

# 2. Analyze volatility surface
python3 -m projects.options_strategies.analyzer

# 3. Backtest strategies
python3 -m projects.options_strategies.covered_call_backtester --underlying BTC
python3 -m projects.options_strategies.iv_rv_backtester --underlying BTC
```

## Modules

### 📊 `screener.py` — Options Opportunity Screener

Scans all options data in the database to identify trading opportunities:

- **High IV options** — Premium selling targets ranked by IV
- **IV-RV spread** — Volatility arbitrage opportunities (IV vs realized vol)
- **Covered call candidates** — Optimal OTM calls for income strategies
- **Put-call skew** — Anomalies in put vs call pricing
- **Term structure** — Backwardation/contango analysis

```bash
# Screen all underlyings
python3 -m projects.options_strategies.screener

# Screen specific underlying with more results
python3 -m projects.options_strategies.screener --underlying BTC --top 30

# Screen without saving files
python3 -m projects.options_strategies.screener --no-save
```

**Output:** Console report + CSV/JSON files in `output/screener/`

---

### 📈 `analyzer.py` — Volatility Surface Analyzer

Deep analysis of the options volatility surface:

- **ATM IV vs Realized Vol** — Compare implied to historical volatility
- **IV Term Structure** — Near vs far expiry analysis (contango/backwardation)
- **Volatility Smile** — IV by moneyness with skew metrics
- **Greeks Summary** — Aggregate delta, gamma, vega (OI-weighted)
- **IV Percentile** — Where current IV ranks historically

```bash
# Analyze all underlyings
python3 -m projects.options_strategies.analyzer

# Analyze specific underlying with custom lookback
python3 -m projects.options_strategies.analyzer --underlying ETH --days 60

# Analyze without saving charts
python3 -m projects.options_strategies.analyzer --no-save
```

**Output:** Console report + PNG charts + JSON in `output/analyzer/`

**Charts generated:**
- Term structure plot (IV by DTE)
- Volatility smile curve
- IV vs RV comparison bars

---

### 💰 `covered_call_backtester.py` — Systematic Covered Call

**Strategy:** Hold BTC/ETH spot and systematically sell OTM call options on a rolling schedule.

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

**Metrics:** Premium yield (annualized), call-away rate, total return vs HODL, Sharpe, max drawdown.

```bash
python3 -m projects.options_strategies.covered_call_backtester --underlying BTC --delta 0.2 --expiry 30
python3 -m projects.options_strategies.covered_call_backtester --underlying ETH --delta 0.3 --expiry 7
```

**Output:** Console report + CSV + PNG charts in `output/covered_call/`

---

### 📉 `iv_rv_backtester.py` — IV-RV Spread Short Strangle

**Strategy:** Compare Implied Volatility vs Realized Volatility. When IV exceeds RV by a threshold, sell a strangle (OTM put + OTM call) to collect premium from vol mean-reversion.

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

**Output:** Console report + CSV + PNG charts in `output/iv_rv/`

---

### ⚙️ `config.py` — Central Configuration

All strategy parameters, thresholds, and constants in one place:

- Exchange constants (Deribit)
- Fee rates and slippage assumptions
- Filter thresholds (min IV, min OI, DTE bounds)
- Default strategy parameters
- Chart styling (dark theme colors)
- Output directory configuration

Edit this file to customize behavior across all modules.

## Data Requirements

All modules read from the database — they do **NOT** call the Deribit API directly.

**Required tables:**
- `options_instruments` — Option contract metadata
- `options_tickers` — Historical IV, Greeks, prices
- `volatility_surface` — IV surface snapshots (optional)
- `klines` — Spot price data (for RV calculation)

### Fetching Data

Before running screener/analyzer/backtests, populate the database:

```bash
# Option 1: Standalone script
python3 scripts/fetch_deribit_options.py --currency BTC ETH

# Option 2: Pipeline job
python3 -m pipeline.jobs.options_data_job --currency BTC ETH
```

**If no data exists:** The modules will print a helpful message directing you to run the fetch script. They will not crash.

### Data Fallbacks

- **IV data missing:** Both backtests estimate IV from realized volatility × 1.15
- **Options data missing:** Screener/analyzer print clear error message
- **Spot data missing:** Falls back to PERP kline data

## Output Structure

```
projects/options_strategies/
├── __init__.py
├── README.md
├── config.py                     # Central configuration
├── screener.py                   # Opportunity scanner
├── analyzer.py                   # Vol surface analyzer
├── covered_call_backtester.py    # Strategy 1
├── iv_rv_backtester.py           # Strategy 2
└── output/                       # Generated results
    ├── screener/                 # Screener CSVs + JSONs
    ├── analyzer/                 # Analysis charts + JSONs
    ├── covered_call/             # Backtest results
    └── iv_rv/                    # Backtest results
```

## Chart Theme

All charts use a consistent dark theme:
- Background: `#1a1a2e`
- Panel: `#16213e`
- Green (positive): `#00d4aa`
- Red (negative): `#ff4757`
- Gold (highlight): `#ffd700`

## Dependencies

- `numpy`, `pandas` — Numerical computation
- `matplotlib` — Chart generation
- `scipy` — Black-Scholes (norm.cdf)
- `asyncpg` — Database access

All included in the project's `requirements.txt`.

## Example Workflow

```bash
# 1. Fetch fresh data
python3 scripts/fetch_deribit_options.py --currency BTC ETH

# 2. Screen for current opportunities
python3 -m projects.options_strategies.screener --underlying BTC

# 3. Analyze vol surface in detail
python3 -m projects.options_strategies.analyzer --underlying BTC

# 4. Backtest a covered call strategy
python3 -m projects.options_strategies.covered_call_backtester \
    --underlying BTC --delta 0.25 --expiry 14

# 5. Backtest IV-RV spread strategy
python3 -m projects.options_strategies.iv_rv_backtester \
    --underlying BTC --threshold 0.12 --width 1.5

# Check generated outputs
ls -la projects/options_strategies/output/*/
```

## Notes

- **Deribit-only:** Currently supports Deribit options. Other exchanges can be added via adaptor layer.
- **Production-ready:** Code uses async DB, proper error handling, and graceful fallbacks.
- **No live trading:** These are research/backtesting tools, not execution systems.
