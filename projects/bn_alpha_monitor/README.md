# Binance Alpha Monitor

Monitors Binance Alpha tokens for stability signals. Analyzes boosted tokens using volatility, ATR, trend strength, and other indicators to generate trading signals.

## Features
- Fetches and filters boosted alpha tokens (multiplier > 1)
- Calculates 6 stability metrics per token
- Generates color-coded report images
- Sends alerts via Telegram

## Usage

```bash
# Run the monitor (one cycle)
python run_monitor.py

# Run the hourly market reporter
python crypto_reporter.py
```

## Metrics
- **Rolling Volatility** (30%) — Price volatility over 15-min window
- **ATR** (25%) — Average True Range
- **Price Range** (15%) — High-low spread
- **Trend Strength** (10%) — Directional momentum
- **Price Jump Frequency** (10%) — Sudden price movements
- **Realtime Deviation** (10%) — Deviation from rolling average
