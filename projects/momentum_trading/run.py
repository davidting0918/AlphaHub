#!/usr/bin/env python3
"""Main entry point for momentum trading backtester."""

import asyncio
import os
import sys
from datetime import datetime

import asyncpg
import pandas as pd

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Force flush
def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)

from config import DATABASE_URL
from data_fetcher import fetch_all_data, load_klines_from_db, get_all_pairs, create_tables
from backtester import Backtester, aggregate_results, strategy_summary, top_pairs_by_strategy
from visualizer import generate_all_charts


async def check_data_exists(pool: asyncpg.Pool) -> tuple[int, int]:
    """Check if data already exists in database."""
    async with pool.acquire() as conn:
        pairs_count = await conn.fetchval("SELECT COUNT(*) FROM momentum_pairs WHERE is_active = TRUE")
        klines_count = await conn.fetchval("SELECT COUNT(*) FROM momentum_klines")
    return pairs_count or 0, klines_count or 0


async def run_backtest():
    """Run the complete backtesting pipeline."""
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    print_flush("=" * 60)
    print_flush("MOMENTUM TRADING BACKTESTER")
    print_flush("=" * 60)
    print_flush()
    
    # Connect to database
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    
    try:
        # Create tables if needed
        await create_tables(pool)
        
        # Check if data exists
        pairs_count, klines_count = await check_data_exists(pool)
        print_flush(f"Existing data: {pairs_count} pairs, {klines_count:,} klines")
        
        # Fetch data if needed
        if pairs_count == 0 or klines_count < 10000:
            print_flush("\nFetching data from Binance...")
            print_flush("-" * 40)
            pairs_count, klines_count = await fetch_all_data()
            print_flush(f"\n✓ Fetched {klines_count:,} klines for {pairs_count} pairs")
        else:
            print_flush("✓ Using existing data")
        
        # Get all pairs
        pairs = await get_all_pairs(pool)
        print_flush(f"\n{len(pairs)} pairs to backtest")
        
        # Load data for each pair
        print_flush("\nLoading klines from database...")
        klines_data = {}
        for symbol in pairs:
            df = await load_klines_from_db(pool, symbol)
            if len(df) > 100:  # Need enough data
                klines_data[symbol] = df
        
        print_flush(f"✓ Loaded data for {len(klines_data)} pairs")
        
        if not klines_data:
            print_flush("No data to backtest!")
            return
        
        # Run backtests
        print_flush("\nRunning backtests...")
        print_flush("-" * 40)
        
        backtester = Backtester()
        all_results = {}
        
        for i, (symbol, df) in enumerate(klines_data.items()):
            print_flush(f"[{i+1}/{len(klines_data)}] {symbol}...", end=" ")
            try:
                results = backtester.run_all_strategies(df, symbol)
                all_results[symbol] = results
                total_trades = sum(r.total_trades for r in results.values())
                print_flush(f"✓ {total_trades} trades")
            except Exception as e:
                print_flush(f"✗ Error: {e}")
        
        print_flush()
        
        # Aggregate results
        print_flush("Aggregating results...")
        results_df = aggregate_results(all_results)
        summary_df = strategy_summary(results_df)
        
        # Print summary
        print_flush("\n" + "=" * 60)
        print_flush("STRATEGY SUMMARY")
        print_flush("=" * 60)
        print_flush()
        print_flush(summary_df.to_string())
        
        # Top pairs
        print_flush("\n" + "=" * 60)
        print_flush("TOP PERFORMING PAIRS")
        print_flush("=" * 60)
        top_df = top_pairs_by_strategy(results_df, n=5)
        for strategy in top_df['strategy'].unique():
            print_flush(f"\n{strategy.upper().replace('_', ' ')}:")
            strat_df = top_df[top_df['strategy'] == strategy][['symbol', 'total_pnl', 'win_rate', 'total_trades']]
            print_flush(strat_df.to_string(index=False))
        
        # Generate charts
        print_flush("\n" + "=" * 60)
        print_flush("GENERATING CHARTS")
        print_flush("=" * 60)
        charts = generate_all_charts(summary_df, results_df, all_results, klines_data, output_dir)
        print_flush(f"\n✓ Generated {len(charts)} charts")
        for chart in charts:
            print_flush(f"  - {os.path.basename(chart)}")
        
        # Save CSV results
        csv_path = os.path.join(output_dir, 'backtest_results.csv')
        results_df.to_csv(csv_path, index=False)
        print_flush(f"\n✓ Saved results to {csv_path}")
        
        summary_csv = os.path.join(output_dir, 'strategy_summary.csv')
        summary_df.to_csv(summary_csv)
        print_flush(f"✓ Saved summary to {summary_csv}")
        
        # Final report
        print_flush("\n" + "=" * 60)
        print_flush("FINAL REPORT")
        print_flush("=" * 60)
        print_flush(f"""
Pairs analyzed: {len(klines_data)}
Total klines: {sum(len(df) for df in klines_data.values()):,}
Total trades: {results_df['total_trades'].sum():,}

Best strategy by PnL: {summary_df['total_pnl'].idxmax()}
Best strategy by Sharpe: {summary_df['sharpe_ratio'].idxmax()}
Best strategy by Win Rate: {summary_df['win_rate'].idxmax()}

Output directory: {output_dir}
        """)
        
        return results_df, summary_df, all_results
        
    finally:
        await pool.close()


def main():
    """Entry point."""
    asyncio.run(run_backtest())


if __name__ == "__main__":
    main()
