#!/usr/bin/env python3
"""
Options Backtester — CLI Entry Point

Run options strategy backtests with real market data from Deribit & Binance.

Usage:
    python3 -m projects.options_backtest.run
    python3 -m projects.options_backtest.run --underlying ETH
    python3 -m projects.options_backtest.run --capital 50000
    python3 -m projects.options_backtest.run --no-charts

Strategies tested:
    1. Covered Call
    2. Cash-Secured Put
    3. Short Strangle (IV-RV spread)
    4. Iron Condor
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from projects.options_backtest import config
from projects.options_backtest.backtester import OptionsBacktester
from projects.options_backtest.visualizer import OptionsVisualizer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main(
    underlying: str = "BTC",
    capital: float = None,
    output_dir: str = None,
    generate_charts: bool = True,
    save_csv: bool = True,
):
    """
    Main entry point for options backtester.
    
    Args:
        underlying: Asset to backtest (BTC or ETH)
        capital: Initial capital (default from config)
        output_dir: Directory for output files
        generate_charts: Whether to generate visualization charts
        save_csv: Whether to save results to CSV
    """
    start_time = datetime.now(timezone.utc)
    
    print("\n" + "=" * 80)
    print("  OPTIONS BACKTESTER")
    print("  Real-time data from Deribit & Binance public APIs")
    print("=" * 80)
    print(f"  Start: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Underlying: {underlying}")
    print(f"  Capital: ${capital or config.INITIAL_CAPITAL:,.0f}")
    print("=" * 80 + "\n")
    
    # Initialize backtester
    bt = OptionsBacktester(capital=capital)
    
    # Run all strategies
    logger.info(f"Fetching {underlying} market data and running backtests...")
    results = await bt.run_all_strategies(underlying)
    
    if not results:
        logger.error("No results generated. Check API connectivity.")
        return None
    
    # Print report
    bt.print_report(results)
    
    # Setup output directory
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save CSV results
    if save_csv:
        logger.info("Saving results to CSV...")
        csv_path = bt.save_results_csv(output_dir, results)
        print(f"  📊 Results saved: {csv_path}")
    
    # Generate charts
    chart_paths = []
    if generate_charts:
        logger.info("Generating visualizations...")
        viz = OptionsVisualizer(output_dir=output_dir)
        
        # Get market data from backtester
        market_data = getattr(bt, 'market_data', None)
        
        chart_paths = viz.generate_all_charts(results, market_data, underlying)
        
        print("\n  📈 Charts generated:")
        for path in chart_paths:
            print(f"     • {os.path.basename(path)}")
    
    # Summary
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    viable = [r for r in results.values() if r.viable]
    best = max(viable, key=lambda r: r.apr_pct) if viable else None
    
    print("\n" + "=" * 80)
    print("  BACKTEST COMPLETE")
    print("=" * 80)
    print(f"  Duration: {duration:.1f} seconds")
    print(f"  Strategies tested: {len(results)}")
    print(f"  Viable strategies: {len(viable)}")
    
    if best:
        print(f"\n  🏆 Best Strategy: {best.strategy}")
        print(f"     Expected APR: {best.apr_pct:.1f}%")
        print(f"     Win Rate: {best.win_rate_pct:.0f}%")
        print(f"     Max Drawdown: {best.max_drawdown_pct:.1f}%")
        print(f"     Net PnL: ${best.net_pnl:,.0f}")
    
    print(f"\n  Output directory: {output_dir}")
    print("=" * 80 + "\n")
    
    return {
        "results": results,
        "chart_paths": chart_paths,
        "output_dir": output_dir,
    }


def cli():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Options Backtester - Test strategies with real market data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 -m projects.options_backtest.run
    python3 -m projects.options_backtest.run --underlying ETH
    python3 -m projects.options_backtest.run --capital 50000 --no-charts
        """
    )
    
    parser.add_argument(
        "--underlying", "-u",
        type=str,
        default="BTC",
        choices=["BTC", "ETH"],
        help="Underlying asset (default: BTC)"
    )
    
    parser.add_argument(
        "--capital", "-c",
        type=float,
        default=None,
        help=f"Initial capital in USD (default: {config.INITIAL_CAPITAL})"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output directory for results and charts"
    )
    
    parser.add_argument(
        "--no-charts",
        action="store_true",
        help="Skip chart generation"
    )
    
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip CSV output"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run async main
    result = asyncio.run(main(
        underlying=args.underlying,
        capital=args.capital,
        output_dir=args.output,
        generate_charts=not args.no_charts,
        save_csv=not args.no_csv,
    ))
    
    return result


if __name__ == "__main__":
    cli()
