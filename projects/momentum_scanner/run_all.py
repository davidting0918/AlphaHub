"""
Momentum Scanner — Orchestrator

Runs the full pipeline:
1. Data Collection (fetch_data.py)
2. Price Jump Detection (scanner.py)
3. Strategy Backtesting (backtester.py)
4. Visualization (visualize.py)
5. Generate README report

Usage:
    # Full run with test subset (3-5 symbols):
    python run_all.py --test

    # Full run with all mid/low liquidity tokens:
    python run_all.py

    # Skip data fetch (use cached data):
    python run_all.py --skip-fetch

    # Custom symbols:
    python run_all.py --symbols ACHUSDT SEIUSDT BLURUSDT
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Ensure we're running from the project directory
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(PROJECT_DIR)
sys.path.insert(0, PROJECT_DIR)

# Test subset: diverse mid-low liquidity tokens
TEST_SYMBOLS = [
    "ACHUSDT",   # low-cap gaming
    "SEIUSDT",   # L1 chain
    "BLURUSDT",  # NFT marketplace
    "BONDUSDT",  # DeFi
    "HIGHUSDT",  # DeFi
]


def phase1_fetch(symbols=None, skip=False):
    """Phase 1: Data Collection."""
    if skip:
        logger.info("⏭️  Skipping data fetch (using cached data)")
        from fetch_data import get_available_symbols
        avail = get_available_symbols()
        logger.info(f"  Available cached symbols: {len(avail)}")
        return True

    logger.info("📡 Phase 1: Data Collection")
    logger.info("=" * 60)
    from fetch_data import DataFetcher
    fetcher = DataFetcher(subset=symbols)
    result = fetcher.run()
    return result["symbols_fetched"] > 0


def phase2_scan(symbols=None):
    """Phase 2: Price Jump Detection."""
    logger.info("\n🔍 Phase 2: Price Jump Detection")
    logger.info("=" * 60)
    from scanner import JumpScanner
    scanner = JumpScanner()
    jumps = scanner.scan_all(symbols=symbols)
    return jumps is not None and not jumps.empty


def phase3_backtest():
    """Phase 3: Strategy Backtesting."""
    logger.info("\n📊 Phase 3: Strategy Backtesting")
    logger.info("=" * 60)
    from backtester import MomentumBacktester
    bt = MomentumBacktester()
    results = bt.run_all_strategies()
    return len(results) > 0 and any(r.total_trades > 0 for r in results)


def phase4_visualize():
    """Phase 4: Visualization."""
    logger.info("\n📈 Phase 4: Visualization")
    logger.info("=" * 60)
    from visualize import generate_all_charts
    output_dir = generate_all_charts()
    return output_dir is not None


def phase5_report():
    """Phase 5: Generate README report."""
    logger.info("\n📝 Phase 5: Generating Report")
    logger.info("=" * 60)

    from backtester import load_backtest_results, load_equity_curves
    from scanner import load_jumps
    from fetch_data import load_symbol_metadata, get_available_symbols

    summary = load_backtest_results()
    jumps = load_jumps()
    meta = load_symbol_metadata()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    num_symbols = len(get_available_symbols())

    # Build report
    report = f"""# Momentum Scanner — Research Report

> **Generated:** {now}
> **Data:** Binance USDT Perpetual Futures, 5-minute candles
> **Lookback:** 6 months

---

## Methodology

### Data Collection
- Fetched all Binance USDT perpetual futures symbols
- Classified by 24h volume: High (>$50M), Mid ($5M-$50M), Low (<$5M)
- Focused on **mid and low liquidity tokens** (more alpha potential)
- Downloaded 5-minute kline data for {num_symbols} symbols over 6 months
- Data stored in local parquet files for fast backtesting

### Price Jump Detection
Defined "price jumps" using three criteria:
1. **Single candle:** >3% absolute price change in one 5m candle
2. **Consecutive:** 3 candles totaling >5% cumulative change
3. **Volume spike:** >3x average volume (48-candle rolling average)

### Strategy Design
**Entry:** After a jump is detected, enter in the direction of the jump at the next candle's open.

**Exit strategies tested:**
| Strategy | Description |
|----------|-------------|
| TP2_SL1 through TP5_SL3 | Fixed take-profit (2/3/5%) and stop-loss (1/2/3%) |
| Hold6 through Hold48 | Time-based exit after N candles (30min to 4h) |
| Trail1, Trail2 | Trailing stop at 1% or 2% |

**Assumptions:**
- 0.04% taker fee per trade (0.08% round-trip)
- 2% of capital per position
- Starting capital: $10,000

---

## Key Findings
"""

    if jumps is not None and not jumps.empty:
        num_jumps = len(jumps)
        up_jumps = len(jumps[jumps["direction"] == "up"])
        down_jumps = len(jumps[jumps["direction"] == "down"])
        avg_change = jumps["abs_price_change_pct"].mean()
        avg_vol = jumps["volume_ratio"].mean()

        top_sym = jumps["symbol"].value_counts().head(5)

        report += f"""
### Jump Statistics
- **Total jumps detected:** {num_jumps:,}
- **Direction split:** {up_jumps:,} up ({up_jumps/num_jumps*100:.1f}%) / {down_jumps:,} down ({down_jumps/num_jumps*100:.1f}%)
- **Average price change:** {avg_change:.2f}%
- **Average volume ratio:** {avg_vol:.1f}x

### Most Active Tokens
| Token | Jumps |
|-------|-------|
"""
        for sym, count in top_sym.items():
            report += f"| {sym} | {count} |\n"

    if summary is not None and not summary.empty:
        best = summary.iloc[0]
        worst = summary.iloc[-1]
        profitable = summary[summary["total_pnl_usd"] > 0]

        report += f"""
---

## Strategy Performance

### Summary Table
| Strategy | Trades | Win Rate | Avg Return | PnL ($) | PnL (%) | Max DD | Sharpe | PF |
|----------|--------|----------|------------|---------|---------|--------|--------|-----|
"""
        for _, row in summary.iterrows():
            pf = f"{row['profit_factor']:.2f}" if row['profit_factor'] < 999 else "∞"
            report += (
                f"| {row['strategy']} | {row['total_trades']} | "
                f"{row['win_rate']:.1f}% | {row['avg_return_pct']:.3f}% | "
                f"${row['total_pnl_usd']:,.0f} | {row['total_pnl_pct']:.1f}% | "
                f"{row['max_drawdown_pct']:.1f}% | {row['sharpe_ratio']:.2f} | {pf} |\n"
            )

        report += f"""
### 🏆 Best Strategy: **{best['strategy']}**
- **Total PnL:** ${best['total_pnl_usd']:,.0f} ({best['total_pnl_pct']:.1f}%)
- **Win Rate:** {best['win_rate']:.1f}%
- **Sharpe Ratio:** {best['sharpe_ratio']:.2f}
- **Max Drawdown:** {best['max_drawdown_pct']:.1f}%
- **Profit Factor:** {best['profit_factor']:.2f}
- **Total Trades:** {best['total_trades']}

### Risk Metrics
- **{len(profitable)}/{len(summary)}** strategy variants were profitable
- Best risk-adjusted: Highest Sharpe = {summary.loc[summary['sharpe_ratio'].idxmax(), 'strategy']} ({summary['sharpe_ratio'].max():.2f})
- Lowest max drawdown: {summary.loc[summary['max_drawdown_pct'].idxmax(), 'strategy']} ({summary['max_drawdown_pct'].max():.1f}%)
"""

    report += """
---

## Charts

### Equity Curves
![Equity Curves](output/01_equity_curves.png)

### Return Distribution
![Return Distribution](output/02_return_distribution.png)

### Jump Frequency Heatmap
![Jump Heatmap](output/03_jump_heatmap.png)

### Top Tokens by Jumps
![Top Tokens](output/04_top_tokens.png)

### Win Rate by Hour
![Win Rate by Hour](output/05_win_rate_by_hour.png)

### Strategy PnL Comparison
![PnL Comparison](output/06_pnl_comparison.png)

---

## Technical Notes

- **Data source:** Binance USDT-M Futures API via existing AlphaHub BinanceClient
- **Storage:** Local parquet files (fast I/O, no DB overhead)
- **Rate limiting:** 0.12s between API calls (~500 req/min, well under Binance 1200/min limit)
- **Fee model:** 0.04% taker fee per trade (industry standard for Binance perps)
- **Position sizing:** 2% of capital per trade (conservative Kelly-like sizing)

---

*Generated by AlphaHub Momentum Scanner*
"""

    readme_path = os.path.join(PROJECT_DIR, "README.md")
    with open(readme_path, "w") as f:
        f.write(report)
    logger.info(f"Report saved to {readme_path}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Momentum Scanner — Full Pipeline")
    parser.add_argument("--test", action="store_true",
                        help="Run with test subset (5 symbols)")
    parser.add_argument("--symbols", nargs="*",
                        help="Custom symbols to analyze")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip data fetching (use cached)")
    parser.add_argument("--skip-scan", action="store_true",
                        help="Skip jump scanning (use cached)")
    parser.add_argument("--skip-backtest", action="store_true",
                        help="Skip backtesting (use cached)")
    args = parser.parse_args()

    symbols = args.symbols
    if args.test:
        symbols = TEST_SYMBOLS
        logger.info(f"🧪 TEST MODE: using {len(symbols)} symbols: {symbols}")

    start_time = time.time()
    logger.info(f"\n{'='*60}")
    logger.info(f"MOMENTUM SCANNER — FULL PIPELINE")
    logger.info(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"{'='*60}\n")

    success = True

    # Phase 1
    if not args.skip_scan or not args.skip_fetch:
        ok = phase1_fetch(symbols=symbols, skip=args.skip_fetch)
        if not ok:
            logger.error("Phase 1 failed! No data available.")
            success = False

    # Phase 2
    if success and not args.skip_scan:
        available = None
        if symbols:
            available = symbols
        ok = phase2_scan(symbols=available)
        if not ok:
            logger.error("Phase 2 failed! No jumps detected.")
            success = False

    # Phase 3
    if success and not args.skip_backtest:
        ok = phase3_backtest()
        if not ok:
            logger.warning("Phase 3: No trades generated (may need more data).")

    # Phase 4
    if success:
        phase4_visualize()

    # Phase 5
    if success:
        phase5_report()

    elapsed = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"PIPELINE COMPLETE — {elapsed:.0f}s ({elapsed/60:.1f} min)")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
