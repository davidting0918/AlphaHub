"""Visualization for momentum trading backtester."""

import os
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle

from config import DARK_THEME, INITIAL_CAPITAL
from backtester import BacktestResult, Trade


def setup_dark_theme():
    """Configure matplotlib for dark theme."""
    plt.rcParams.update({
        'figure.facecolor': DARK_THEME['facecolor'],
        'axes.facecolor': DARK_THEME['facecolor'],
        'axes.edgecolor': DARK_THEME['gridcolor'],
        'axes.labelcolor': DARK_THEME['textcolor'],
        'text.color': DARK_THEME['textcolor'],
        'xtick.color': DARK_THEME['textcolor'],
        'ytick.color': DARK_THEME['textcolor'],
        'grid.color': DARK_THEME['gridcolor'],
        'legend.facecolor': DARK_THEME['facecolor'],
        'legend.edgecolor': DARK_THEME['gridcolor'],
    })


def strategy_comparison(summary_df: pd.DataFrame, output_dir: str) -> str:
    """Bar chart comparing all strategies on key metrics."""
    setup_dark_theme()
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), facecolor=DARK_THEME['facecolor'])
    
    strategies = summary_df.index.tolist()
    x = np.arange(len(strategies))
    colors = DARK_THEME['colors']
    
    # Total PnL
    ax = axes[0]
    pnl_values = summary_df['total_pnl'].values
    bars = ax.bar(x, pnl_values, color=[colors[0] if v >= 0 else '#ff4444' for v in pnl_values])
    ax.set_xticks(x)
    ax.set_xticklabels(strategies, rotation=45, ha='right')
    ax.set_ylabel('Total PnL ($)')
    ax.set_title('Total PnL by Strategy')
    ax.axhline(y=0, color=DARK_THEME['gridcolor'], linestyle='--', alpha=0.5)
    ax.grid(True, alpha=0.3)
    
    # Sharpe Ratio
    ax = axes[1]
    sharpe_values = summary_df['sharpe_ratio'].values
    sharpe_values = np.clip(sharpe_values, -5, 5)  # Clip extreme values
    bars = ax.bar(x, sharpe_values, color=colors[1])
    ax.set_xticks(x)
    ax.set_xticklabels(strategies, rotation=45, ha='right')
    ax.set_ylabel('Sharpe Ratio')
    ax.set_title('Sharpe Ratio by Strategy')
    ax.axhline(y=0, color=DARK_THEME['gridcolor'], linestyle='--', alpha=0.5)
    ax.grid(True, alpha=0.3)
    
    # Win Rate
    ax = axes[2]
    bars = ax.bar(x, summary_df['win_rate'].values * 100, color=colors[2])
    ax.set_xticks(x)
    ax.set_xticklabels(strategies, rotation=45, ha='right')
    ax.set_ylabel('Win Rate (%)')
    ax.set_title('Win Rate by Strategy')
    ax.axhline(y=50, color=DARK_THEME['gridcolor'], linestyle='--', alpha=0.5)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    path = os.path.join(output_dir, 'strategy_comparison.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def equity_curves(
    all_results: dict[str, dict[str, BacktestResult]],
    output_dir: str
) -> str:
    """Overlay equity curves for all strategies."""
    setup_dark_theme()
    fig, ax = plt.subplots(figsize=(14, 7), facecolor=DARK_THEME['facecolor'])
    
    # Aggregate equity curves by strategy
    strategy_equity = {}
    
    for symbol, strategy_results in all_results.items():
        for strategy, result in strategy_results.items():
            if strategy not in strategy_equity:
                strategy_equity[strategy] = []
            if not result.equity_curve.empty:
                # Normalize to percentage change from initial
                normalized = (result.equity_curve / INITIAL_CAPITAL - 1) * 100
                strategy_equity[strategy].append(normalized)
    
    colors = DARK_THEME['colors']
    for i, (strategy, curves) in enumerate(strategy_equity.items()):
        if not curves:
            continue
        # Combine all curves - take mean at each timestamp
        combined = pd.concat(curves, axis=1)
        mean_curve = combined.mean(axis=1)
        ax.plot(mean_curve.index, mean_curve.values, 
                label=strategy.replace('_', ' ').title(),
                color=colors[i % len(colors)], linewidth=1.5)
    
    ax.set_xlabel('Date')
    ax.set_ylabel('Return (%)')
    ax.set_title('Average Equity Curves by Strategy')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color=DARK_THEME['gridcolor'], linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    path = os.path.join(output_dir, 'equity_curves.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def top_pairs_chart(results_df: pd.DataFrame, output_dir: str, n: int = 15) -> str:
    """Chart showing top performing pairs."""
    setup_dark_theme()
    
    # Aggregate PnL by symbol
    pair_pnl = results_df.groupby('symbol')['total_pnl'].sum().sort_values(ascending=True)
    top_pairs = pair_pnl.tail(n)
    
    fig, ax = plt.subplots(figsize=(12, 8), facecolor=DARK_THEME['facecolor'])
    
    colors = [DARK_THEME['colors'][0] if v >= 0 else '#ff4444' for v in top_pairs.values]
    ax.barh(range(len(top_pairs)), top_pairs.values, color=colors)
    ax.set_yticks(range(len(top_pairs)))
    ax.set_yticklabels(top_pairs.index)
    ax.set_xlabel('Total PnL ($)')
    ax.set_title(f'Top {n} Performing Pairs (All Strategies Combined)')
    ax.axvline(x=0, color=DARK_THEME['gridcolor'], linestyle='--', alpha=0.5)
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    path = os.path.join(output_dir, 'top_pairs.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def monthly_returns_heatmap(
    all_results: dict[str, dict[str, BacktestResult]],
    output_dir: str
) -> str:
    """Monthly returns heatmap per strategy."""
    setup_dark_theme()
    
    # Calculate monthly returns for each strategy
    strategy_monthly = {}
    
    for symbol, strategy_results in all_results.items():
        for strategy, result in strategy_results.items():
            if strategy not in strategy_monthly:
                strategy_monthly[strategy] = {}
            
            for trade in result.trades:
                if trade.is_closed and trade.exit_time:
                    month_key = trade.exit_time.strftime('%Y-%m')
                    if month_key not in strategy_monthly[strategy]:
                        strategy_monthly[strategy][month_key] = 0
                    strategy_monthly[strategy][month_key] += trade.pnl
    
    if not strategy_monthly:
        return ""
    
    # Convert to DataFrame
    monthly_df = pd.DataFrame(strategy_monthly).T
    monthly_df = monthly_df.reindex(sorted(monthly_df.columns), axis=1)
    monthly_df = monthly_df.fillna(0)
    
    if monthly_df.empty:
        return ""
    
    fig, ax = plt.subplots(figsize=(14, 6), facecolor=DARK_THEME['facecolor'])
    
    # Create heatmap
    im = ax.imshow(monthly_df.values, cmap='RdYlGn', aspect='auto')
    
    # Labels
    ax.set_xticks(np.arange(len(monthly_df.columns)))
    ax.set_yticks(np.arange(len(monthly_df.index)))
    ax.set_xticklabels(monthly_df.columns, rotation=45, ha='right')
    ax.set_yticklabels([s.replace('_', ' ').title() for s in monthly_df.index])
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('PnL ($)', color=DARK_THEME['textcolor'])
    cbar.ax.yaxis.set_tick_params(color=DARK_THEME['textcolor'])
    plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color=DARK_THEME['textcolor'])
    
    # Add values
    for i in range(len(monthly_df.index)):
        for j in range(len(monthly_df.columns)):
            val = monthly_df.iloc[i, j]
            text = ax.text(j, i, f'${val:.0f}', ha='center', va='center',
                          color='black' if abs(val) > monthly_df.values.max()*0.3 else DARK_THEME['textcolor'],
                          fontsize=8)
    
    ax.set_title('Monthly Returns Heatmap by Strategy')
    plt.tight_layout()
    path = os.path.join(output_dir, 'monthly_returns_heatmap.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def trade_distribution(
    all_results: dict[str, dict[str, BacktestResult]],
    output_dir: str
) -> str:
    """Histogram of trade PnL distribution."""
    setup_dark_theme()
    
    fig, axes = plt.subplots(2, 3, figsize=(15, 10), facecolor=DARK_THEME['facecolor'])
    axes = axes.flatten()
    
    strategies = ['volume_breakout', 'rsi_momentum', 'vwap_breakout', 'obv_divergence', 'multi_factor']
    colors = DARK_THEME['colors']
    
    for idx, strategy in enumerate(strategies):
        ax = axes[idx]
        
        # Collect all trade PnLs for this strategy
        pnls = []
        for symbol, strategy_results in all_results.items():
            if strategy in strategy_results:
                result = strategy_results[strategy]
                pnls.extend([t.pnl for t in result.trades if t.is_closed])
        
        if pnls:
            # Clip extreme values for better visualization
            pnls = np.clip(pnls, -500, 500)
            ax.hist(pnls, bins=30, color=colors[idx], alpha=0.7, edgecolor='white')
            ax.axvline(x=0, color='white', linestyle='--', alpha=0.5)
            ax.axvline(x=np.mean(pnls), color='yellow', linestyle='-', alpha=0.8, label=f'Mean: ${np.mean(pnls):.2f}')
            ax.legend()
        
        ax.set_title(strategy.replace('_', ' ').title())
        ax.set_xlabel('PnL ($)')
        ax.set_ylabel('Frequency')
        ax.grid(True, alpha=0.3)
    
    # Remove unused subplot
    axes[5].axis('off')
    
    plt.suptitle('Trade PnL Distribution by Strategy', fontsize=14, color=DARK_THEME['textcolor'])
    plt.tight_layout()
    path = os.path.join(output_dir, 'trade_distribution.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def volume_vs_return(
    all_results: dict[str, dict[str, BacktestResult]],
    klines_data: dict[str, pd.DataFrame],
    output_dir: str
) -> str:
    """Scatter plot of volume at entry vs trade return."""
    setup_dark_theme()
    
    fig, ax = plt.subplots(figsize=(12, 8), facecolor=DARK_THEME['facecolor'])
    
    volumes = []
    returns = []
    strategies = []
    
    for symbol, strategy_results in all_results.items():
        if symbol not in klines_data:
            continue
        df = klines_data[symbol]
        
        for strategy, result in strategy_results.items():
            for trade in result.trades:
                if trade.is_closed and trade.entry_time in df.index:
                    entry_vol = df.loc[trade.entry_time, 'volume'] if 'volume' in df.columns else 0
                    if entry_vol > 0:
                        volumes.append(entry_vol)
                        returns.append(trade.pnl_pct * 100)
                        strategies.append(strategy)
    
    if volumes:
        # Color by strategy
        strategy_colors = {s: DARK_THEME['colors'][i] for i, s in enumerate(set(strategies))}
        colors = [strategy_colors[s] for s in strategies]
        
        ax.scatter(volumes, returns, c=colors, alpha=0.5, s=20)
        ax.set_xlabel('Entry Volume')
        ax.set_ylabel('Return (%)')
        ax.set_title('Volume at Entry vs Trade Return')
        ax.axhline(y=0, color=DARK_THEME['gridcolor'], linestyle='--', alpha=0.5)
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3)
        
        # Add legend
        for strategy, color in strategy_colors.items():
            ax.scatter([], [], c=color, label=strategy.replace('_', ' ').title(), s=50)
        ax.legend(loc='upper right')
    
    plt.tight_layout()
    path = os.path.join(output_dir, 'volume_vs_return.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def signal_examples(
    all_results: dict[str, dict[str, BacktestResult]],
    klines_data: dict[str, pd.DataFrame],
    output_dir: str,
    n_examples: int = 3
) -> str:
    """Chart showing example trades with entry/exit markers."""
    setup_dark_theme()
    
    # Find profitable trades with good data
    good_trades = []
    for symbol, strategy_results in all_results.items():
        if symbol not in klines_data:
            continue
        df = klines_data[symbol]
        
        for strategy, result in strategy_results.items():
            for trade in result.trades:
                if (trade.is_closed and trade.pnl > 50 and 
                    trade.entry_time in df.index and trade.exit_time in df.index):
                    good_trades.append((symbol, strategy, trade, df))
    
    if not good_trades:
        return ""
    
    # Sort by PnL and take top examples
    good_trades.sort(key=lambda x: x[2].pnl, reverse=True)
    examples = good_trades[:n_examples]
    
    fig, axes = plt.subplots(n_examples, 1, figsize=(14, 4*n_examples), facecolor=DARK_THEME['facecolor'])
    if n_examples == 1:
        axes = [axes]
    
    for idx, (symbol, strategy, trade, df) in enumerate(examples):
        ax = axes[idx]
        
        # Get data around the trade
        entry_idx = df.index.get_loc(trade.entry_time)
        start_idx = max(0, entry_idx - 20)
        exit_idx = df.index.get_loc(trade.exit_time)
        end_idx = min(len(df), exit_idx + 10)
        
        plot_df = df.iloc[start_idx:end_idx]
        
        # Plot price
        ax.plot(plot_df.index, plot_df['close'], color=DARK_THEME['colors'][0], linewidth=1.5)
        
        # Mark entry and exit
        ax.axvline(x=trade.entry_time, color='#00ff00', linestyle='--', alpha=0.7, label='Entry')
        ax.axvline(x=trade.exit_time, color='#ff0000', linestyle='--', alpha=0.7, label='Exit')
        
        # Add markers
        ax.scatter([trade.entry_time], [trade.entry_price], color='#00ff00', s=100, marker='^', zorder=5)
        ax.scatter([trade.exit_time], [trade.exit_price], color='#ff0000', s=100, marker='v', zorder=5)
        
        ax.set_title(f'{symbol} - {strategy.replace("_", " ").title()} | PnL: ${trade.pnl:.2f} ({trade.pnl_pct*100:.1f}%)')
        ax.set_xlabel('Time')
        ax.set_ylabel('Price')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    path = os.path.join(output_dir, 'signal_examples.png')
    plt.savefig(path, dpi=150, facecolor=DARK_THEME['facecolor'])
    plt.close()
    return path


def generate_all_charts(
    summary_df: pd.DataFrame,
    results_df: pd.DataFrame,
    all_results: dict[str, dict[str, BacktestResult]],
    klines_data: dict[str, pd.DataFrame],
    output_dir: str
) -> list[str]:
    """Generate all visualization charts."""
    os.makedirs(output_dir, exist_ok=True)
    
    charts = []
    
    print("Generating strategy comparison chart...")
    charts.append(strategy_comparison(summary_df, output_dir))
    
    print("Generating equity curves...")
    charts.append(equity_curves(all_results, output_dir))
    
    print("Generating top pairs chart...")
    charts.append(top_pairs_chart(results_df, output_dir))
    
    print("Generating monthly returns heatmap...")
    heatmap = monthly_returns_heatmap(all_results, output_dir)
    if heatmap:
        charts.append(heatmap)
    
    print("Generating trade distribution...")
    charts.append(trade_distribution(all_results, output_dir))
    
    print("Generating volume vs return scatter...")
    charts.append(volume_vs_return(all_results, klines_data, output_dir))
    
    print("Generating signal examples...")
    examples = signal_examples(all_results, klines_data, output_dir)
    if examples:
        charts.append(examples)
    
    return [c for c in charts if c]
