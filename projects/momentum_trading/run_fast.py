#!/usr/bin/env python3
"""Fast backtest runner - processes top 50 pairs only."""

import asyncio
import os
import sys
from datetime import datetime

import asyncpg
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DATABASE_URL, INITIAL_CAPITAL
from indicators import calculate_all_indicators
from backtester import Backtester, aggregate_results, strategy_summary
from visualizer import generate_all_charts

def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)


async def load_all_klines_fast(pool: asyncpg.Pool, limit_pairs: int = 50) -> dict:
    """Load klines for top pairs by data availability."""
    print_flush("Loading klines from database (optimized)...")
    
    async with pool.acquire() as conn:
        # Get top pairs by kline count (most data = better backtest)
        top_pairs = await conn.fetch("""
            SELECT symbol, COUNT(*) as cnt 
            FROM momentum_klines 
            WHERE exchange = 'BINANCE' AND interval = '4h'
            GROUP BY symbol 
            HAVING COUNT(*) > 500
            ORDER BY cnt DESC 
            LIMIT $1
        """, limit_pairs)
        
        symbols = [r['symbol'] for r in top_pairs]
        print_flush(f"Selected {len(symbols)} pairs with most data")
        
        # Load all klines in one query
        rows = await conn.fetch("""
            SELECT symbol, open_time, open, high, low, close, volume, quote_volume
            FROM momentum_klines
            WHERE symbol = ANY($1) AND exchange = 'BINANCE' AND interval = '4h'
            ORDER BY symbol, open_time
        """, symbols)
    
    print_flush(f"Loaded {len(rows)} total klines")
    
    # Convert to DataFrames
    klines_data = {}
    current_symbol = None
    current_rows = []
    
    for row in rows:
        if row['symbol'] != current_symbol:
            if current_symbol and current_rows:
                df = pd.DataFrame(current_rows)
                df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
                for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
                    df[col] = df[col].astype(float)
                df.set_index('open_time', inplace=True)
                klines_data[current_symbol] = df
            current_symbol = row['symbol']
            current_rows = []
        current_rows.append(dict(row))
    
    # Don't forget the last symbol
    if current_symbol and current_rows:
        df = pd.DataFrame(current_rows)
        df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
        for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
            df[col] = df[col].astype(float)
        df.set_index('open_time', inplace=True)
        klines_data[current_symbol] = df
    
    print_flush(f"✓ Prepared DataFrames for {len(klines_data)} pairs")
    return klines_data


async def run_fast_backtest(limit_pairs: int = 50):
    """Run a fast backtest on top pairs."""
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    print_flush("=" * 60)
    print_flush("MOMENTUM TRADING BACKTESTER (FAST MODE)")
    print_flush("=" * 60)
    print_flush()
    
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    
    try:
        # Load data
        klines_data = await load_all_klines_fast(pool, limit_pairs)
        
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
                print_flush(f"✗ {e}")
        
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
        
        # Top pairs per strategy
        print_flush("\n" + "=" * 60)
        print_flush("TOP PERFORMING PAIRS")
        print_flush("=" * 60)
        
        for strategy in summary_df.index:
            strat_df = results_df[results_df['strategy'] == strategy].nlargest(5, 'total_pnl')
            if not strat_df.empty:
                print_flush(f"\n{strategy.upper().replace('_', ' ')}:")
                print_flush(strat_df[['symbol', 'total_pnl', 'win_rate', 'total_trades']].to_string(index=False))
        
        # Generate charts
        print_flush("\n" + "=" * 60)
        print_flush("GENERATING CHARTS")
        print_flush("=" * 60)
        
        try:
            charts = generate_all_charts(summary_df, results_df, all_results, klines_data, output_dir)
            print_flush(f"\n✓ Generated {len(charts)} charts")
            for chart in charts:
                print_flush(f"  - {os.path.basename(chart)}")
        except Exception as e:
            print_flush(f"✗ Chart generation error: {e}")
        
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
        
        total_klines = sum(len(df) for df in klines_data.values())
        total_trades = results_df['total_trades'].sum()
        
        best_pnl_strategy = summary_df['total_pnl'].idxmax()
        best_sharpe_strategy = summary_df['sharpe_ratio'].idxmax()
        best_winrate_strategy = summary_df['win_rate'].idxmax()
        
        print_flush(f"""
Pairs analyzed: {len(klines_data)}
Total klines: {total_klines:,}
Total trades: {int(total_trades):,}

Best strategy by PnL: {best_pnl_strategy} (${summary_df.loc[best_pnl_strategy, 'total_pnl']:.2f})
Best strategy by Sharpe: {best_sharpe_strategy} ({summary_df.loc[best_sharpe_strategy, 'sharpe_ratio']:.2f})
Best strategy by Win Rate: {best_winrate_strategy} ({summary_df.loc[best_winrate_strategy, 'win_rate']*100:.1f}%)

Output directory: {output_dir}
        """)
        
        return results_df, summary_df, all_results
        
    finally:
        await pool.close()


def main():
    asyncio.run(run_fast_backtest(limit_pairs=50))


if __name__ == "__main__":
    main()
