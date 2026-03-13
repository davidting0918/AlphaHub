# Funding Rate Arbitrage Analysis

Cross-exchange funding rate arbitrage between OKX and Binance perpetual contracts.

## Strategy

When funding rates diverge between exchanges for the same asset:
- **Long** on the exchange paying you (negative funding = longs get paid)
- **Short** on the exchange where you get paid (positive funding = shorts get paid)
- Collect the spread as risk-free yield (delta-neutral)

## Modules

- `analyzer.py` — Core analysis: spread calculation, opportunity scanning, historical stats
- `config.py` — Strategy parameters (thresholds, fees, filters)

## Usage

```bash
cd /home/ubuntu/clawd/repos/AlphaHub
DATABASE_URL='...' python3 -m projects.funding_rate_arb.analyzer
```

## Key Metrics

| Metric | Description |
|--------|-------------|
| Spread | `funding_rate_A - funding_rate_B` for same asset |
| APR | Annualized return from spread: `spread × 3 × 365 × 100` |
| Sharpe | Risk-adjusted return of the spread |
| Hit Rate | % of periods where spread > threshold |
