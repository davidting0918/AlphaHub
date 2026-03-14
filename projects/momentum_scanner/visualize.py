"""
Momentum Scanner — Phase 4: Visualization

Generates charts:
1. Equity curve for each strategy variant
2. Distribution of returns (histogram)
3. Heatmap: symbol × time showing jump frequency
4. Top 10 tokens by number of jumps
5. Win rate by hour of day
6. Cumulative PnL comparison of strategy variants

All charts saved to projects/momentum_scanner/output/
"""

import os
import sys
import json
import logging
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

from fetch_data import DATA_DIR
from scanner import load_jumps
from backtester import (
    load_backtest_results, load_equity_curves, load_hourly_stats,
    RESULTS_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# ── Style ───────────────────────────────────────────────────────
BG_COLOR = "#0d1117"
PANEL_COLOR = "#161b22"
TEXT_COLOR = "#c9d1d9"
GRID_COLOR = "#21262d"
ACCENT_GREEN = "#3fb950"
ACCENT_RED = "#f85149"
ACCENT_BLUE = "#58a6ff"
ACCENT_YELLOW = "#d29922"
ACCENT_PURPLE = "#bc8cff"

PALETTE = [ACCENT_BLUE, ACCENT_GREEN, ACCENT_YELLOW, ACCENT_RED,
           ACCENT_PURPLE, "#f778ba", "#79c0ff", "#ffa657",
           "#7ee787", "#ff7b72"]


def setup_style():
    """Apply dark theme globally."""
    plt.rcParams.update({
        "figure.facecolor": BG_COLOR,
        "axes.facecolor": PANEL_COLOR,
        "axes.edgecolor": GRID_COLOR,
        "axes.labelcolor": TEXT_COLOR,
        "text.color": TEXT_COLOR,
        "xtick.color": TEXT_COLOR,
        "ytick.color": TEXT_COLOR,
        "grid.color": GRID_COLOR,
        "grid.alpha": 0.3,
        "legend.facecolor": PANEL_COLOR,
        "legend.edgecolor": GRID_COLOR,
        "legend.labelcolor": TEXT_COLOR,
        "font.size": 10,
    })


def chart_equity_curves(equity_data: Dict, output_dir: str):
    """Chart 1: Equity curves for all strategy variants."""
    if not equity_data:
        return

    fig, ax = plt.subplots(figsize=(16, 8))

    for i, (name, curve) in enumerate(equity_data.items()):
        if len(curve) < 2:
            continue
        color = PALETTE[i % len(PALETTE)]
        ax.plot(curve, label=name, color=color, linewidth=1.2, alpha=0.85)

    ax.axhline(y=10000, color=TEXT_COLOR, linewidth=0.5, alpha=0.3, linestyle="--")
    ax.set_title("Equity Curves — All Strategy Variants", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Trade #", fontsize=12)
    ax.set_ylabel("Equity ($)", fontsize=12)
    ax.legend(fontsize=8, loc="upper left", ncol=2)
    ax.grid(True, alpha=0.2)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    path = os.path.join(output_dir, "01_equity_curves.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved: {path}")


def chart_return_distribution(output_dir: str):
    """Chart 2: Distribution of trade returns (histogram)."""
    # Load trades from best strategy
    summary = load_backtest_results()
    if summary is None or summary.empty:
        return

    best_strat = summary.iloc[0]["strategy"]
    trades_path = os.path.join(RESULTS_DIR, f"trades_{best_strat}.csv")
    if not os.path.exists(trades_path):
        return

    trades = pd.read_csv(trades_path)
    if trades.empty:
        return

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # Left: histogram of returns
    ax = axes[0]
    returns = trades["pnl_pct"].values
    bins = np.linspace(max(-10, returns.min()), min(10, returns.max()), 60)
    colors = [ACCENT_GREEN if r > 0 else ACCENT_RED for r in np.histogram(returns, bins=bins)[0]]

    n, bins_out, patches = ax.hist(returns, bins=bins, edgecolor="none", alpha=0.85)
    for patch, val in zip(patches, bins_out[:-1]):
        patch.set_facecolor(ACCENT_GREEN if val >= 0 else ACCENT_RED)

    ax.axvline(x=0, color=TEXT_COLOR, linewidth=1, alpha=0.5)
    ax.axvline(x=np.mean(returns), color=ACCENT_YELLOW, linewidth=1.5, linestyle="--",
               label=f"Mean: {np.mean(returns):.3f}%")
    ax.axvline(x=np.median(returns), color=ACCENT_BLUE, linewidth=1.5, linestyle="--",
               label=f"Median: {np.median(returns):.3f}%")
    ax.set_title(f"Return Distribution — {best_strat}", fontsize=14, fontweight="bold")
    ax.set_xlabel("Return (%)", fontsize=11)
    ax.set_ylabel("Count", fontsize=11)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.2)

    # Right: box plot of returns by exit reason
    ax2 = axes[1]
    if "exit_reason" in trades.columns:
        reasons = trades["exit_reason"].unique()
        data_by_reason = [trades[trades["exit_reason"] == r]["pnl_pct"].values for r in reasons]
        bp = ax2.boxplot(data_by_reason, labels=reasons, patch_artist=True,
                         medianprops=dict(color=ACCENT_YELLOW, linewidth=2))
        for i, patch in enumerate(bp["boxes"]):
            patch.set_facecolor(PALETTE[i % len(PALETTE)])
            patch.set_alpha(0.7)
        ax2.axhline(y=0, color=TEXT_COLOR, linewidth=0.5, alpha=0.3)
        ax2.set_title("Returns by Exit Reason", fontsize=14, fontweight="bold")
        ax2.set_ylabel("Return (%)", fontsize=11)
        ax2.grid(True, alpha=0.2)

    path = os.path.join(output_dir, "02_return_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved: {path}")


def chart_jump_heatmap(jumps: pd.DataFrame, output_dir: str):
    """Chart 3: Heatmap of jump frequency — symbol × hour of day."""
    if jumps is None or jumps.empty:
        return

    # Top 30 symbols by jump count
    top_syms = jumps["symbol"].value_counts().head(30).index.tolist()
    subset = jumps[jumps["symbol"].isin(top_syms)].copy()

    pivot = subset.pivot_table(
        index="symbol", columns="hour", values="price_change_pct",
        aggfunc="count", fill_value=0
    )
    # Ensure all hours 0-23
    for h in range(24):
        if h not in pivot.columns:
            pivot[h] = 0
    pivot = pivot[sorted(pivot.columns)]

    fig, ax = plt.subplots(figsize=(18, max(8, len(top_syms) * 0.35)))

    sns.heatmap(
        pivot, ax=ax, cmap="YlOrRd", linewidths=0.5, linecolor=GRID_COLOR,
        cbar_kws={"label": "Jump Count"},
        annot=True, fmt="g", annot_kws={"fontsize": 7},
    )
    ax.set_title("Price Jump Frequency — Symbol × Hour (UTC)", fontsize=15, fontweight="bold", pad=15)
    ax.set_xlabel("Hour (UTC)", fontsize=12)
    ax.set_ylabel("Symbol", fontsize=12)

    path = os.path.join(output_dir, "03_jump_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved: {path}")


def chart_top_tokens(jumps: pd.DataFrame, output_dir: str):
    """Chart 4: Top 10 tokens by number of jumps."""
    if jumps is None or jumps.empty:
        return

    top10 = jumps["symbol"].value_counts().head(10)

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # Left: bar chart of jump counts
    ax = axes[0]
    colors_bar = [PALETTE[i % len(PALETTE)] for i in range(len(top10))]
    bars = ax.barh(range(len(top10)), top10.values, color=colors_bar, alpha=0.85)
    ax.set_yticks(range(len(top10)))
    ax.set_yticklabels(top10.index, fontsize=10)
    ax.invert_yaxis()
    ax.set_title("Top 10 Tokens by Jump Count", fontsize=14, fontweight="bold")
    ax.set_xlabel("Number of Jumps", fontsize=11)
    ax.grid(True, alpha=0.2, axis="x")

    for bar, val in zip(bars, top10.values):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", fontsize=9, color=TEXT_COLOR)

    # Right: average jump magnitude for top 10
    ax2 = axes[1]
    avg_change = jumps[jumps["symbol"].isin(top10.index)].groupby("symbol")["abs_price_change_pct"].mean()
    avg_change = avg_change.reindex(top10.index)

    bars2 = ax2.barh(range(len(avg_change)), avg_change.values, color=colors_bar, alpha=0.85)
    ax2.set_yticks(range(len(avg_change)))
    ax2.set_yticklabels(avg_change.index, fontsize=10)
    ax2.invert_yaxis()
    ax2.set_title("Average Jump Magnitude (%)", fontsize=14, fontweight="bold")
    ax2.set_xlabel("Avg |Price Change| (%)", fontsize=11)
    ax2.grid(True, alpha=0.2, axis="x")

    for bar, val in zip(bars2, avg_change.values):
        ax2.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height() / 2,
                f"{val:.2f}%", va="center", fontsize=9, color=TEXT_COLOR)

    path = os.path.join(output_dir, "04_top_tokens.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved: {path}")


def chart_win_rate_by_hour(hourly_data: Dict, output_dir: str):
    """Chart 5: Win rate by hour of day."""
    if not hourly_data:
        return

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # Pick top 3 strategies by total trades for comparison
    strat_names = list(hourly_data.keys())[:6]

    # Left: win rate by hour
    ax = axes[0]
    for i, name in enumerate(strat_names):
        stats = hourly_data[name]
        hours = sorted([int(h) for h in stats.keys()])
        win_rates = [stats[str(h)]["win_rate"] for h in hours]
        ax.plot(hours, win_rates, marker="o", markersize=4,
                label=name, color=PALETTE[i % len(PALETTE)], linewidth=1.5, alpha=0.8)

    ax.axhline(y=50, color=TEXT_COLOR, linewidth=0.5, alpha=0.3, linestyle="--")
    ax.set_title("Win Rate by Hour of Day (UTC)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Hour (UTC)", fontsize=11)
    ax.set_ylabel("Win Rate (%)", fontsize=11)
    ax.set_xticks(range(0, 24))
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(True, alpha=0.2)

    # Right: trade count by hour (first strategy)
    ax2 = axes[1]
    if strat_names:
        stats = hourly_data[strat_names[0]]
        hours = sorted([int(h) for h in stats.keys()])
        counts = [stats[str(h)]["trades"] for h in hours]
        ax2.bar(hours, counts, color=ACCENT_BLUE, alpha=0.7, width=0.8)
        ax2.set_title(f"Trade Count by Hour — {strat_names[0]}", fontsize=14, fontweight="bold")
        ax2.set_xlabel("Hour (UTC)", fontsize=11)
        ax2.set_ylabel("Number of Trades", fontsize=11)
        ax2.set_xticks(range(0, 24))
        ax2.grid(True, alpha=0.2, axis="y")

    path = os.path.join(output_dir, "05_win_rate_by_hour.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved: {path}")


def chart_pnl_comparison(summary: pd.DataFrame, output_dir: str):
    """Chart 6: Cumulative PnL comparison + strategy metrics."""
    if summary is None or summary.empty:
        return

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))

    # Left: PnL bar chart
    ax = axes[0]
    summary_sorted = summary.sort_values("total_pnl_usd", ascending=True)
    colors = [ACCENT_GREEN if x > 0 else ACCENT_RED for x in summary_sorted["total_pnl_usd"]]
    bars = ax.barh(range(len(summary_sorted)), summary_sorted["total_pnl_usd"].values,
                   color=colors, alpha=0.85)
    ax.set_yticks(range(len(summary_sorted)))
    ax.set_yticklabels(summary_sorted["strategy"].values, fontsize=9)
    ax.set_title("Total PnL by Strategy", fontsize=14, fontweight="bold")
    ax.set_xlabel("PnL ($)", fontsize=11)
    ax.axvline(x=0, color=TEXT_COLOR, linewidth=0.5, alpha=0.3)
    ax.grid(True, alpha=0.2, axis="x")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Right: Sharpe vs Win Rate scatter
    ax2 = axes[1]
    for i, (_, row) in enumerate(summary.iterrows()):
        color = ACCENT_GREEN if row["total_pnl_usd"] > 0 else ACCENT_RED
        size = max(30, min(300, abs(row["total_pnl_usd"]) / 10))
        ax2.scatter(row["win_rate"], row["sharpe_ratio"],
                    c=color, s=size, alpha=0.8, edgecolors="none")
        ax2.annotate(row["strategy"], (row["win_rate"], row["sharpe_ratio"]),
                     fontsize=7, alpha=0.8, ha="center", va="bottom",
                     textcoords="offset points", xytext=(0, 5))

    ax2.axhline(y=0, color=TEXT_COLOR, linewidth=0.5, alpha=0.3, linestyle="--")
    ax2.axvline(x=50, color=TEXT_COLOR, linewidth=0.5, alpha=0.3, linestyle="--")
    ax2.set_title("Sharpe vs Win Rate (size = |PnL|)", fontsize=14, fontweight="bold")
    ax2.set_xlabel("Win Rate (%)", fontsize=11)
    ax2.set_ylabel("Sharpe Ratio", fontsize=11)
    ax2.grid(True, alpha=0.2)

    path = os.path.join(output_dir, "06_pnl_comparison.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved: {path}")


def generate_all_charts():
    """Generate all visualization charts."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    setup_style()

    logger.info("Generating charts...")

    # Load data
    jumps = load_jumps()
    summary = load_backtest_results()
    equity_data = load_equity_curves()
    hourly_data = load_hourly_stats()

    # Generate each chart
    chart_equity_curves(equity_data, OUTPUT_DIR)
    chart_return_distribution(OUTPUT_DIR)
    chart_jump_heatmap(jumps, OUTPUT_DIR)
    chart_top_tokens(jumps, OUTPUT_DIR)
    chart_win_rate_by_hour(hourly_data, OUTPUT_DIR)
    chart_pnl_comparison(summary, OUTPUT_DIR)

    logger.info(f"\nAll charts saved to {OUTPUT_DIR}")
    return OUTPUT_DIR


if __name__ == "__main__":
    generate_all_charts()
