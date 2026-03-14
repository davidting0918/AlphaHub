#!/usr/bin/env python3
"""
Main Backtest Runner

Runs all 5 strategies across 15 symbols, generates comparison reports,
and sends results to Telegram.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List
import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import requests

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from projects.momentum_perp.backtest.data_fetcher import (
    SYMBOLS, fetch_all_data, load_klines_from_parquet
)
from projects.momentum_perp.backtest.backtester import (
    STRATEGIES, backtest_strategy, get_strategy_instance, 
    get_strategy_timeframes, BacktestResult, INITIAL_CAPITAL
)

# Output directories
RESULTS_DIR = Path(__file__).parent / "results"
DATA_DIR = Path(__file__).parent / "data"

# Telegram config
TELEGRAM_BOT_TOKEN = "8787093176:AAHDll5L26z_bCvS6oPkcshGlhYpBBXXY-U"
TELEGRAM_CHAT_ID = "5394690467"


def run_all_backtests() -> Dict[str, List[BacktestResult]]:
    """Run all strategies across all symbols."""
    results = {name: [] for name in STRATEGIES}
    
    print("\n" + "=" * 70)
    print("RUNNING BACKTESTS")
    print("=" * 70)
    
    for strategy_name in STRATEGIES:
        print(f"\n📊 Strategy: {strategy_name}")
        print("-" * 50)
        
        strategy = get_strategy_instance(strategy_name)
        primary_tf, secondary_tf = get_strategy_timeframes(strategy_name)
        
        for symbol in SYMBOLS:
            # Load klines
            klines = load_klines_from_parquet(symbol, primary_tf)
            secondary_klines = None
            if secondary_tf:
                secondary_klines = load_klines_from_parquet(symbol, secondary_tf)
            
            if not klines:
                print(f"  ⚠️  {symbol}: No data")
                continue
            
            # Run backtest
            result = backtest_strategy(
                strategy=strategy,
                klines=klines,
                secondary_klines=secondary_klines,
                symbol=symbol
            )
            results[strategy_name].append(result)
            
            # Print summary
            print(f"  {symbol}: {result.num_trades} trades, "
                  f"PnL: ${result.total_pnl:.2f} ({result.total_pnl_pct:+.1f}%), "
                  f"Win: {result.win_rate:.0f}%")
    
    return results


def aggregate_results(results: Dict[str, List[BacktestResult]]) -> pd.DataFrame:
    """Aggregate results by strategy."""
    rows = []
    
    for strategy_name, symbol_results in results.items():
        if not symbol_results:
            continue
        
        # Aggregate across all symbols
        all_trades = []
        total_pnl = 0
        equity_curves = []
        
        for r in symbol_results:
            all_trades.extend(r.trades)
            total_pnl += r.total_pnl
            if r.equity_curve:
                equity_curves.append(r.equity_curve)
        
        num_trades = len(all_trades)
        wins = sum(1 for t in all_trades if t.pnl > 0)
        win_rate = (wins / num_trades * 100) if num_trades > 0 else 0
        
        # Calculate returns for Sharpe
        returns = [t.pnl_pct for t in all_trades]
        if len(returns) >= 2:
            mean_ret = np.mean(returns)
            std_ret = np.std(returns)
            sharpe = (mean_ret / std_ret) * np.sqrt(252 * 5) if std_ret > 0 else 0
        else:
            sharpe = 0
        
        # Calculate max drawdown from combined equity
        if equity_curves:
            combined_equity = [INITIAL_CAPITAL]
            for curve in equity_curves:
                if len(curve) > 1:
                    changes = [curve[i] - curve[i-1] for i in range(1, len(curve))]
                    for c in changes:
                        combined_equity.append(combined_equity[-1] + c)
            
            peak = combined_equity[0]
            max_dd = 0
            for eq in combined_equity:
                if eq > peak:
                    peak = eq
                dd = (peak - eq) / peak if peak > 0 else 0
                if dd > max_dd:
                    max_dd = dd
            max_dd_pct = max_dd * 100
        else:
            max_dd_pct = 0
        
        # Profit factor
        gross_profit = sum(t.pnl for t in all_trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in all_trades if t.pnl < 0))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        rows.append({
            'Strategy': strategy_name,
            'Total PnL ($)': round(total_pnl, 2),
            'Total PnL (%)': round(total_pnl / INITIAL_CAPITAL * 100, 2),
            'Trades': num_trades,
            'Win Rate (%)': round(win_rate, 1),
            'Sharpe': round(sharpe, 2),
            'Max DD (%)': round(max_dd_pct, 1),
            'Profit Factor': round(profit_factor, 2) if profit_factor != float('inf') else 'N/A'
        })
    
    df = pd.DataFrame(rows)
    df = df.sort_values('Total PnL ($)', ascending=False)
    return df


def generate_comparison_chart(results: Dict[str, List[BacktestResult]], summary_df: pd.DataFrame):
    """Generate comparison chart."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Momentum Strategy Backtest Comparison (30 Days)', fontsize=14, fontweight='bold')
    
    # Colors
    colors = plt.cm.Set2(np.linspace(0, 1, len(STRATEGIES)))
    strategy_colors = {name: colors[i] for i, name in enumerate(STRATEGIES)}
    
    # 1. Total PnL bar chart
    ax1 = axes[0, 0]
    strategies = summary_df['Strategy'].tolist()
    pnls = summary_df['Total PnL ($)'].tolist()
    bars = ax1.bar(strategies, pnls, color=[strategy_colors[s] for s in strategies])
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax1.set_ylabel('Total PnL ($)')
    ax1.set_title('Total PnL by Strategy')
    ax1.tick_params(axis='x', rotation=45)
    for bar, pnl in zip(bars, pnls):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, 
                 f'${pnl:.0f}', ha='center', va='bottom', fontsize=9)
    
    # 2. Win Rate bar chart
    ax2 = axes[0, 1]
    win_rates = summary_df['Win Rate (%)'].tolist()
    bars = ax2.bar(strategies, win_rates, color=[strategy_colors[s] for s in strategies])
    ax2.axhline(y=50, color='red', linestyle='--', linewidth=0.5, label='50%')
    ax2.set_ylabel('Win Rate (%)')
    ax2.set_title('Win Rate by Strategy')
    ax2.tick_params(axis='x', rotation=45)
    ax2.set_ylim(0, 100)
    for bar, wr in zip(bars, win_rates):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                 f'{wr:.0f}%', ha='center', va='bottom', fontsize=9)
    
    # 3. Equity curves
    ax3 = axes[1, 0]
    for strategy_name, symbol_results in results.items():
        if not symbol_results:
            continue
        
        # Combine equity curves
        combined_equity = [INITIAL_CAPITAL]
        for r in symbol_results:
            if len(r.equity_curve) > 1:
                changes = [r.equity_curve[i] - r.equity_curve[i-1] 
                          for i in range(1, len(r.equity_curve))]
                for c in changes:
                    combined_equity.append(combined_equity[-1] + c)
        
        ax3.plot(combined_equity, label=strategy_name, color=strategy_colors[strategy_name], linewidth=1.5)
    
    ax3.axhline(y=INITIAL_CAPITAL, color='gray', linestyle='--', linewidth=0.5)
    ax3.set_ylabel('Equity ($)')
    ax3.set_xlabel('Time (trade events)')
    ax3.set_title('Combined Equity Curves')
    ax3.legend(loc='upper left', fontsize=8)
    
    # 4. Trade count and metrics
    ax4 = axes[1, 1]
    x = np.arange(len(strategies))
    width = 0.35
    
    trades = summary_df['Trades'].tolist()
    sharpes = summary_df['Sharpe'].tolist()
    
    ax4_twin = ax4.twinx()
    bars1 = ax4.bar(x - width/2, trades, width, label='Trades', color='steelblue', alpha=0.7)
    bars2 = ax4_twin.bar(x + width/2, sharpes, width, label='Sharpe', color='coral', alpha=0.7)
    
    ax4.set_ylabel('Number of Trades', color='steelblue')
    ax4_twin.set_ylabel('Sharpe Ratio', color='coral')
    ax4.set_title('Trade Count & Sharpe Ratio')
    ax4.set_xticks(x)
    ax4.set_xticklabels(strategies, rotation=45)
    ax4.legend(loc='upper left')
    ax4_twin.legend(loc='upper right')
    
    plt.tight_layout()
    
    # Save
    chart_path = RESULTS_DIR / "strategy_comparison.png"
    plt.savefig(chart_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"\n📊 Saved comparison chart: {chart_path}")
    return chart_path


def generate_winner_report(winner_name: str, results: Dict[str, List[BacktestResult]]):
    """Generate detailed report for winning strategy."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    
    symbol_results = results.get(winner_name, [])
    if not symbol_results:
        return None
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f'Best Strategy: {winner_name.upper()} - Detailed Report', fontsize=14, fontweight='bold')
    
    # 1. PnL by symbol
    ax1 = axes[0, 0]
    symbol_pnls = [(r.symbol, r.total_pnl) for r in symbol_results]
    symbol_pnls.sort(key=lambda x: x[1], reverse=True)
    symbols = [x[0].replace('-USDT-SWAP', '') for x in symbol_pnls]
    pnls = [x[1] for x in symbol_pnls]
    colors = ['green' if p > 0 else 'red' for p in pnls]
    ax1.barh(symbols, pnls, color=colors, alpha=0.7)
    ax1.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
    ax1.set_xlabel('PnL ($)')
    ax1.set_title('PnL by Symbol')
    
    # 2. Equity curve (combined)
    ax2 = axes[0, 1]
    combined_equity = [INITIAL_CAPITAL]
    for r in symbol_results:
        if len(r.equity_curve) > 1:
            changes = [r.equity_curve[i] - r.equity_curve[i-1] for i in range(1, len(r.equity_curve))]
            for c in changes:
                combined_equity.append(combined_equity[-1] + c)
    ax2.plot(combined_equity, color='blue', linewidth=1.5)
    ax2.fill_between(range(len(combined_equity)), INITIAL_CAPITAL, combined_equity, 
                     where=[e >= INITIAL_CAPITAL for e in combined_equity], alpha=0.3, color='green')
    ax2.fill_between(range(len(combined_equity)), INITIAL_CAPITAL, combined_equity,
                     where=[e < INITIAL_CAPITAL for e in combined_equity], alpha=0.3, color='red')
    ax2.axhline(y=INITIAL_CAPITAL, color='gray', linestyle='--', linewidth=0.5)
    ax2.set_ylabel('Equity ($)')
    ax2.set_xlabel('Time')
    ax2.set_title('Combined Equity Curve')
    
    # 3. Trade distribution (wins vs losses)
    ax3 = axes[1, 0]
    all_trades = []
    for r in symbol_results:
        all_trades.extend(r.trades)
    
    win_pnls = [t.pnl for t in all_trades if t.pnl > 0]
    loss_pnls = [t.pnl for t in all_trades if t.pnl < 0]
    
    ax3.hist(win_pnls, bins=20, alpha=0.7, color='green', label=f'Wins ({len(win_pnls)})')
    ax3.hist(loss_pnls, bins=20, alpha=0.7, color='red', label=f'Losses ({len(loss_pnls)})')
    ax3.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
    ax3.set_xlabel('PnL ($)')
    ax3.set_ylabel('Frequency')
    ax3.set_title('Trade PnL Distribution')
    ax3.legend()
    
    # 4. Exit reason breakdown
    ax4 = axes[1, 1]
    exit_reasons = {}
    for t in all_trades:
        exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1
    
    if exit_reasons:
        reasons = list(exit_reasons.keys())
        counts = list(exit_reasons.values())
        ax4.pie(counts, labels=reasons, autopct='%1.1f%%', startangle=90)
        ax4.set_title('Exit Reason Breakdown')
    
    plt.tight_layout()
    
    report_path = RESULTS_DIR / "best_strategy_report.png"
    plt.savefig(report_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"📊 Saved winner report: {report_path}")
    return report_path


def send_to_telegram(image_path: Path, message: str):
    """Send image and message to Telegram."""
    # Send image
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    with open(image_path, 'rb') as f:
        files = {'photo': f}
        data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': message[:1024]}  # Caption limit
        response = requests.post(url, files=files, data=data)
    
    if response.status_code == 200:
        print(f"✅ Sent to Telegram: {image_path.name}")
    else:
        print(f"❌ Failed to send to Telegram: {response.text}")


def main():
    """Main entry point."""
    print("\n" + "=" * 70)
    print("MOMENTUM STRATEGY BACKTEST - 30 DAYS")
    print("=" * 70)
    print(f"Symbols: {len(SYMBOLS)}")
    print(f"Strategies: {len(STRATEGIES)}")
    print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
    print(f"Position Size: 10%")
    print("=" * 70)
    
    # Step 1: Fetch data
    print("\n📥 Fetching data...")
    fetch_all_data()
    
    # Step 2: Run backtests
    results = run_all_backtests()
    
    # Step 3: Aggregate and summarize
    print("\n" + "=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)
    
    summary_df = aggregate_results(results)
    print("\n" + summary_df.to_string(index=False))
    
    # Save CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS_DIR / "strategy_comparison.csv"
    summary_df.to_csv(csv_path, index=False)
    print(f"\n📄 Saved: {csv_path}")
    
    # Step 4: Generate charts
    comparison_chart = generate_comparison_chart(results, summary_df)
    
    # Step 5: Identify winner
    winner = summary_df.iloc[0]
    winner_name = winner['Strategy']
    
    print("\n" + "=" * 70)
    print("🏆 WINNER 🏆")
    print("=" * 70)
    print(f"""
WINNER: {winner_name}
Total PnL: ${winner['Total PnL ($)']:.2f} ({winner['Total PnL (%)']:.1f}%)
Win Rate: {winner['Win Rate (%)']:.1f}%
Sharpe: {winner['Sharpe']:.2f}
Max DD: {winner['Max DD (%)']:.1f}%
Trades: {winner['Trades']}
""")
    
    # Generate winner report
    winner_report = generate_winner_report(winner_name, results)
    
    # Step 6: Send to Telegram
    print("\n📱 Sending to Telegram...")
    
    summary_text = f"""📊 Momentum Strategy Backtest Results (30 Days)

🏆 WINNER: {winner_name}

💰 Total PnL: ${winner['Total PnL ($)']:.2f} ({winner['Total PnL (%)']:.1f}%)
📈 Win Rate: {winner['Win Rate (%)']:.1f}%
📉 Max Drawdown: {winner['Max DD (%)']:.1f}%
📐 Sharpe: {winner['Sharpe']:.2f}
🔢 Trades: {winner['Trades']}

Tested: {len(SYMBOLS)} symbols × 5 strategies
Period: 30 days
Capital: ${INITIAL_CAPITAL:,.0f}"""
    
    send_to_telegram(comparison_chart, summary_text)
    
    if winner_report:
        send_to_telegram(winner_report, f"📋 Detailed report for {winner_name}")
    
    # Step 7: Save all trade data
    all_trades_data = []
    for strategy_name, symbol_results in results.items():
        for r in symbol_results:
            for t in r.trades:
                all_trades_data.append({
                    'strategy': t.strategy,
                    'symbol': t.symbol,
                    'side': t.side,
                    'entry_time': t.entry_time,
                    'exit_time': t.exit_time,
                    'entry_price': t.entry_price,
                    'exit_price': t.exit_price,
                    'pnl': t.pnl,
                    'pnl_pct': t.pnl_pct,
                    'exit_reason': t.exit_reason,
                    'fees': t.fees
                })
    
    trades_df = pd.DataFrame(all_trades_data)
    trades_path = RESULTS_DIR / "all_trades.csv"
    trades_df.to_csv(trades_path, index=False)
    print(f"📄 Saved all trades: {trades_path}")
    
    print("\n" + "=" * 70)
    print("BACKTEST COMPLETE")
    print("=" * 70)
    
    return winner_name, summary_df


if __name__ == "__main__":
    winner_name, _ = main()
