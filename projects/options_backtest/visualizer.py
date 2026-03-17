"""
Options Backtester — Visualization Module

Generates charts for:
- Strategy comparison (APR bar chart)
- Equity curves
- IV vs RV time series
- PnL breakdown
- IV surface / smile
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd

# Set backend before importing pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

from . import config
from .backtester import BacktestResult

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class OptionsVisualizer:
    """Generate visualizations for options backtest results."""
    
    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(__file__), "output"
        )
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Chart styling
        self.style = config.CHART_STYLE
        self.text_color = self.style["text_color"]
    
    def _setup_figure(self, figsize=(16, 10), title: str = None):
        """Create styled figure."""
        fig = plt.figure(figsize=figsize, facecolor=self.style["facecolor"])
        if title:
            fig.suptitle(title, fontsize=18, color='white', fontweight='bold', y=0.98)
        return fig
    
    def _setup_axes(self, ax):
        """Apply dark theme styling to axes."""
        ax.set_facecolor(self.style["ax_facecolor"])
        ax.tick_params(colors=self.text_color)
        ax.spines['bottom'].set_color(self.style["grid_color"])
        ax.spines['top'].set_color(self.style["grid_color"])
        ax.spines['left'].set_color(self.style["grid_color"])
        ax.spines['right'].set_color(self.style["grid_color"])
        return ax
    
    def _save_figure(self, fig, name: str):
        """Save figure with timestamp."""
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')
        path = os.path.join(self.output_dir, f"{name}_{ts}.png")
        fig.savefig(path, dpi=150, facecolor=fig.get_facecolor(), bbox_inches='tight')
        plt.close(fig)
        logger.info(f"Chart saved: {path}")
        return path
    
    def plot_strategy_comparison(self, results: Dict[str, BacktestResult]) -> str:
        """
        Create strategy comparison bar chart.
        
        Shows APR, Return %, and Max Drawdown for each strategy.
        """
        viable = {k: v for k, v in results.items() if v.viable}
        
        if not viable:
            logger.warning("No viable results to plot")
            return None
        
        fig = self._setup_figure(figsize=(14, 8), title="Options Strategy Comparison")
        
        strategies = list(viable.keys())
        aprs = [viable[s].apr_pct for s in strategies]
        returns = [viable[s].total_return_pct for s in strategies]
        drawdowns = [abs(viable[s].max_drawdown_pct) for s in strategies]
        labels = [viable[s].strategy for s in strategies]
        
        x = np.arange(len(strategies))
        width = 0.25
        
        ax = fig.add_subplot(111)
        self._setup_axes(ax)
        
        # APR bars
        bars1 = ax.bar(x - width, aprs, width, label='APR %', 
                       color=self.style["positive_color"], alpha=0.85)
        
        # Return bars
        colors2 = [self.style["positive_color"] if r > 0 else self.style["negative_color"] 
                   for r in returns]
        bars2 = ax.bar(x, returns, width, label='Return %', color=colors2, alpha=0.6)
        
        # Drawdown bars (negative)
        bars3 = ax.bar(x + width, [-d for d in drawdowns], width, label='Max DD %',
                       color=self.style["negative_color"], alpha=0.5)
        
        ax.set_xticks(x)
        ax.set_xticklabels(labels, color=self.text_color, fontsize=11, rotation=15, ha='right')
        ax.set_ylabel('Percentage (%)', color=self.text_color, fontsize=12)
        ax.axhline(y=0, color='white', linewidth=0.5, alpha=0.3)
        
        ax.legend(facecolor=self.style["ax_facecolor"], edgecolor=self.style["grid_color"],
                  labelcolor=self.text_color, loc='upper right')
        
        # Add value labels
        for bars, offset in [(bars1, -width), (bars2, 0), (bars3, width)]:
            for bar in bars:
                height = bar.get_height()
                if abs(height) > 0:
                    ax.annotate(f'{height:.1f}%',
                                xy=(bar.get_x() + bar.get_width() / 2, height),
                                xytext=(0, 3 if height > 0 else -10),
                                textcoords="offset points",
                                ha='center', va='bottom' if height > 0 else 'top',
                                color=self.text_color, fontsize=9)
        
        return self._save_figure(fig, "strategy_comparison")
    
    def plot_equity_curves(self, results: Dict[str, BacktestResult]) -> str:
        """
        Plot equity curves for all viable strategies.
        """
        viable = {k: v for k, v in results.items() if v.viable}
        
        if not viable:
            return None
        
        fig = self._setup_figure(figsize=(14, 8), title="Strategy Equity Curves")
        ax = fig.add_subplot(111)
        self._setup_axes(ax)
        
        colors = [self.style["positive_color"], self.style["neutral_color"], 
                  '#9b59b6', '#3498db']
        
        capital = config.INITIAL_CAPITAL
        
        for i, (name, result) in enumerate(viable.items()):
            if result.equity_curve:
                x = range(len(result.equity_curve))
                ax.plot(x, result.equity_curve, 
                        color=colors[i % len(colors)],
                        linewidth=2, alpha=0.9,
                        label=f"{result.strategy} ({result.apr_pct:.1f}% APR)")
        
        ax.axhline(y=capital, color='white', linewidth=1, alpha=0.3, linestyle='--',
                   label=f'Initial Capital (${capital:,.0f})')
        
        ax.set_xlabel('Time (settlements)', color=self.text_color, fontsize=11)
        ax.set_ylabel('Portfolio Value ($)', color=self.text_color, fontsize=11)
        
        ax.legend(facecolor=self.style["ax_facecolor"], edgecolor=self.style["grid_color"],
                  labelcolor=self.text_color, loc='upper left')
        
        # Format y-axis as currency
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        return self._save_figure(fig, "equity_curves")
    
    def plot_iv_rv_comparison(self, results: Dict[str, BacktestResult],
                               market_data: Dict[str, Any] = None) -> str:
        """
        Plot IV vs RV comparison and term structure.
        """
        fig = self._setup_figure(figsize=(16, 10), title="Implied vs Realized Volatility Analysis")
        gs = GridSpec(2, 2, hspace=0.3, wspace=0.3)
        
        # 1. IV-RV Spread by Strategy
        ax1 = fig.add_subplot(gs[0, 0])
        self._setup_axes(ax1)
        
        viable = {k: v for k, v in results.items() if v.viable}
        
        if viable:
            strategies = list(viable.keys())
            ivs = [viable[s].avg_iv_entry for s in strategies]
            rvs = [viable[s].avg_rv_period for s in strategies]
            spreads = [viable[s].iv_rv_spread for s in strategies]
            labels = [viable[s].strategy for s in strategies]
            
            x = np.arange(len(strategies))
            width = 0.35
            
            ax1.bar(x - width/2, ivs, width, label='IV', color=self.style["positive_color"], alpha=0.8)
            ax1.bar(x + width/2, rvs, width, label='RV', color=self.style["neutral_color"], alpha=0.8)
            
            ax1.set_xticks(x)
            ax1.set_xticklabels(labels, color=self.text_color, fontsize=9, rotation=15, ha='right')
            ax1.set_ylabel('Volatility %', color=self.text_color)
            ax1.set_title('IV vs RV by Strategy', color=self.text_color, fontsize=12)
            ax1.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color)
        
        # 2. IV-RV Spread histogram
        ax2 = fig.add_subplot(gs[0, 1])
        self._setup_axes(ax2)
        
        if viable:
            colors = [self.style["positive_color"] if s > 0 else self.style["negative_color"] 
                      for s in spreads]
            ax2.bar(range(len(spreads)), spreads, color=colors, alpha=0.8)
            ax2.axhline(y=0, color='white', linewidth=0.5, alpha=0.5)
            ax2.axhline(y=config.IV_RV_ENTRY_THRESHOLD * 100, color=self.style["neutral_color"],
                        linewidth=2, linestyle='--', alpha=0.7, label=f'Entry threshold ({config.IV_RV_ENTRY_THRESHOLD*100:.0f}%)')
            ax2.set_xticks(range(len(spreads)))
            ax2.set_xticklabels(labels, color=self.text_color, fontsize=9, rotation=15, ha='right')
            ax2.set_ylabel('IV - RV (%)', color=self.text_color)
            ax2.set_title('IV-RV Spread', color=self.text_color, fontsize=12)
            ax2.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color)
        
        # 3. IV Term Structure (if market data available)
        ax3 = fig.add_subplot(gs[1, 0])
        self._setup_axes(ax3)
        
        if market_data and "iv_surface" in market_data:
            term_struct = market_data["iv_surface"].get("atm_term_structure", [])
            if term_struct:
                dtes = [t[0] for t in term_struct]
                ivs = [t[1] for t in term_struct]
                
                ax3.plot(dtes, ivs, color=self.style["positive_color"], 
                         linewidth=2, marker='o', markersize=6, alpha=0.9)
                ax3.fill_between(dtes, 0, ivs, color=self.style["positive_color"], alpha=0.15)
                
                ax3.set_xlabel('Days to Expiry', color=self.text_color)
                ax3.set_ylabel('ATM IV %', color=self.text_color)
                ax3.set_title('IV Term Structure', color=self.text_color, fontsize=12)
            else:
                ax3.text(0.5, 0.5, 'No term structure data', ha='center', va='center',
                         color=self.text_color, fontsize=14, transform=ax3.transAxes)
        else:
            ax3.text(0.5, 0.5, 'No market data', ha='center', va='center',
                     color=self.text_color, fontsize=14, transform=ax3.transAxes)
        
        # 4. Strategy metrics scatter
        ax4 = fig.add_subplot(gs[1, 1])
        self._setup_axes(ax4)
        
        if viable:
            for i, (name, result) in enumerate(viable.items()):
                color = [self.style["positive_color"], self.style["neutral_color"],
                         '#9b59b6', '#3498db'][i % 4]
                ax4.scatter(result.iv_rv_spread, result.apr_pct, 
                           s=abs(result.net_pnl) / 10 + 50,
                           c=color, alpha=0.8, edgecolors='white', linewidths=1,
                           label=result.strategy)
            
            ax4.axvline(x=0, color='white', linewidth=0.5, alpha=0.3)
            ax4.axhline(y=0, color='white', linewidth=0.5, alpha=0.3)
            ax4.set_xlabel('IV-RV Spread (%)', color=self.text_color)
            ax4.set_ylabel('Expected APR (%)', color=self.text_color)
            ax4.set_title('APR vs IV-RV Spread (size = PnL)', color=self.text_color, fontsize=12)
            ax4.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color,
                       loc='upper left', fontsize=9)
        
        return self._save_figure(fig, "iv_rv_analysis")
    
    def plot_pnl_breakdown(self, results: Dict[str, BacktestResult]) -> str:
        """
        Plot PnL breakdown showing premium, fees, and net PnL.
        """
        viable = {k: v for k, v in results.items() if v.viable}
        
        if not viable:
            return None
        
        fig = self._setup_figure(figsize=(14, 8), title="PnL Breakdown by Strategy")
        ax = fig.add_subplot(111)
        self._setup_axes(ax)
        
        strategies = list(viable.keys())
        labels = [viable[s].strategy for s in strategies]
        premiums = [viable[s].premium_collected for s in strategies]
        fees = [-viable[s].total_fees for s in strategies]
        net_pnls = [viable[s].net_pnl for s in strategies]
        
        x = np.arange(len(strategies))
        width = 0.25
        
        ax.bar(x - width, premiums, width, label='Premium Collected',
               color=self.style["positive_color"], alpha=0.85)
        ax.bar(x, fees, width, label='Fees & Slippage',
               color=self.style["negative_color"], alpha=0.85)
        
        net_colors = [self.style["positive_color"] if p > 0 else self.style["negative_color"]
                      for p in net_pnls]
        ax.bar(x + width, net_pnls, width, label='Net PnL',
               color=net_colors, alpha=0.6, edgecolor='white', linewidth=1)
        
        ax.set_xticks(x)
        ax.set_xticklabels(labels, color=self.text_color, fontsize=11, rotation=15, ha='right')
        ax.set_ylabel('USD ($)', color=self.text_color, fontsize=12)
        ax.axhline(y=0, color='white', linewidth=0.5, alpha=0.3)
        
        ax.legend(facecolor=self.style["ax_facecolor"], edgecolor=self.style["grid_color"],
                  labelcolor=self.text_color, loc='upper right')
        
        # Format y-axis
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        return self._save_figure(fig, "pnl_breakdown")
    
    def plot_iv_surface(self, market_data: Dict[str, Any], 
                         underlying: str = "BTC") -> str:
        """
        Plot IV surface / smile from market data.
        """
        if not market_data or "iv_surface" not in market_data:
            logger.warning("No IV surface data available")
            return None
        
        iv_surface = market_data["iv_surface"]
        spot_price = market_data["underlying"].index_price if market_data.get("underlying") else 0
        
        fig = self._setup_figure(figsize=(16, 10), 
                                  title=f"{underlying} Implied Volatility Surface")
        gs = GridSpec(2, 2, hspace=0.3, wspace=0.3)
        
        # 1. Call IV Smile (multiple expiries)
        ax1 = fig.add_subplot(gs[0, 0])
        self._setup_axes(ax1)
        
        call_data = iv_surface.get("calls", {})
        colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(call_data)))
        
        for i, (expiry, data) in enumerate(sorted(call_data.items())[:5]):  # Top 5 expiries
            if data:
                moneyness = [d[0] for d in data]
                ivs = [d[1] for d in data]
                ax1.plot(moneyness, ivs, color=colors[i], linewidth=2, 
                         marker='o', markersize=4, alpha=0.8, label=expiry)
        
        ax1.axvline(x=1.0, color='white', linewidth=1, linestyle='--', alpha=0.5, label='ATM')
        ax1.set_xlabel('Moneyness (S/K)', color=self.text_color)
        ax1.set_ylabel('Implied Volatility %', color=self.text_color)
        ax1.set_title('Call Option IV Smile', color=self.text_color, fontsize=12)
        ax1.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color,
                   fontsize=8, loc='upper right')
        
        # 2. Put IV Smile
        ax2 = fig.add_subplot(gs[0, 1])
        self._setup_axes(ax2)
        
        put_data = iv_surface.get("puts", {})
        colors = plt.cm.plasma(np.linspace(0.2, 0.9, len(put_data)))
        
        for i, (expiry, data) in enumerate(sorted(put_data.items())[:5]):
            if data:
                moneyness = [d[0] for d in data]
                ivs = [d[1] for d in data]
                ax2.plot(moneyness, ivs, color=colors[i], linewidth=2,
                         marker='o', markersize=4, alpha=0.8, label=expiry)
        
        ax2.axvline(x=1.0, color='white', linewidth=1, linestyle='--', alpha=0.5, label='ATM')
        ax2.set_xlabel('Moneyness (S/K)', color=self.text_color)
        ax2.set_ylabel('Implied Volatility %', color=self.text_color)
        ax2.set_title('Put Option IV Smile', color=self.text_color, fontsize=12)
        ax2.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color,
                   fontsize=8, loc='upper left')
        
        # 3. Term Structure
        ax3 = fig.add_subplot(gs[1, 0])
        self._setup_axes(ax3)
        
        term_struct = iv_surface.get("atm_term_structure", [])
        if term_struct:
            dtes = [t[0] for t in term_struct]
            ivs = [t[1] for t in term_struct]
            
            ax3.plot(dtes, ivs, color=self.style["positive_color"],
                     linewidth=2.5, marker='s', markersize=8, alpha=0.9)
            ax3.fill_between(dtes, 0, ivs, color=self.style["positive_color"], alpha=0.1)
            
            # Add trend line
            if len(dtes) >= 2:
                z = np.polyfit(dtes, ivs, 1)
                p = np.poly1d(z)
                ax3.plot(dtes, p(dtes), '--', color=self.style["neutral_color"],
                         linewidth=1.5, alpha=0.7, label='Trend')
        
        ax3.set_xlabel('Days to Expiry', color=self.text_color)
        ax3.set_ylabel('ATM IV %', color=self.text_color)
        ax3.set_title('ATM IV Term Structure', color=self.text_color, fontsize=12)
        ax3.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color)
        
        # 4. 3D Surface approximation (heatmap)
        ax4 = fig.add_subplot(gs[1, 1])
        self._setup_axes(ax4)
        
        # Build grid for heatmap
        all_options = market_data.get("options", [])
        if all_options:
            # Group by strike and DTE
            grid_data = {}
            for opt in all_options:
                if opt.iv and opt.iv > 0:
                    # Round strike to nearest 5000
                    strike_bucket = round(opt.strike / 5000) * 5000
                    dte_bucket = round(opt.dte / 7) * 7
                    key = (strike_bucket, dte_bucket)
                    if key not in grid_data:
                        grid_data[key] = []
                    grid_data[key].append(opt.iv)
            
            if grid_data:
                # Build heatmap
                strikes = sorted(set(k[0] for k in grid_data.keys()))
                dtes = sorted(set(k[1] for k in grid_data.keys()))
                
                Z = np.zeros((len(dtes), len(strikes)))
                for i, dte in enumerate(dtes):
                    for j, strike in enumerate(strikes):
                        key = (strike, dte)
                        if key in grid_data:
                            Z[i, j] = np.mean(grid_data[key])
                        else:
                            Z[i, j] = np.nan
                
                # Plot heatmap
                im = ax4.imshow(Z, cmap='viridis', aspect='auto',
                               extent=[0, len(strikes)-1, 0, len(dtes)-1],
                               origin='lower')
                
                # Labels
                ax4.set_xticks(range(0, len(strikes), max(1, len(strikes)//6)))
                ax4.set_xticklabels([f'{s/1000:.0f}K' for s in strikes[::max(1, len(strikes)//6)]],
                                    color=self.text_color, fontsize=8)
                ax4.set_yticks(range(0, len(dtes), max(1, len(dtes)//6)))
                ax4.set_yticklabels([f'{d:.0f}d' for d in dtes[::max(1, len(dtes)//6)]],
                                    color=self.text_color, fontsize=8)
                
                ax4.set_xlabel('Strike', color=self.text_color)
                ax4.set_ylabel('Days to Expiry', color=self.text_color)
                ax4.set_title('IV Surface Heatmap', color=self.text_color, fontsize=12)
                
                cbar = fig.colorbar(im, ax=ax4, shrink=0.8)
                cbar.ax.yaxis.set_tick_params(color=self.text_color)
                cbar.ax.set_ylabel('IV %', color=self.text_color)
                plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color=self.text_color)
        else:
            ax4.text(0.5, 0.5, 'No options data for heatmap', ha='center', va='center',
                     color=self.text_color, fontsize=14, transform=ax4.transAxes)
        
        return self._save_figure(fig, "iv_surface")
    
    def plot_overview_dashboard(self, results: Dict[str, BacktestResult],
                                 market_data: Dict[str, Any] = None) -> str:
        """
        Create comprehensive overview dashboard.
        """
        fig = self._setup_figure(figsize=(20, 14), 
                                  title="Options Backtester — Overview Dashboard")
        gs = GridSpec(3, 3, hspace=0.35, wspace=0.3,
                      left=0.06, right=0.96, top=0.92, bottom=0.06)
        
        viable = {k: v for k, v in results.items() if v.viable}
        
        # 1. Strategy APR comparison (large)
        ax1 = fig.add_subplot(gs[0, :2])
        self._setup_axes(ax1)
        
        if viable:
            strategies = list(viable.keys())
            labels = [viable[s].strategy for s in strategies]
            aprs = [viable[s].apr_pct for s in strategies]
            
            colors = [self.style["positive_color"] if a > 0 else self.style["negative_color"]
                      for a in aprs]
            
            bars = ax1.barh(range(len(aprs)), aprs, color=colors, alpha=0.85)
            ax1.set_yticks(range(len(labels)))
            ax1.set_yticklabels(labels, color=self.text_color, fontsize=11)
            ax1.set_xlabel('Expected APR %', color=self.text_color, fontsize=11)
            ax1.set_title('Strategy Performance Ranking', color=self.text_color, fontsize=13)
            ax1.axvline(x=0, color='white', linewidth=0.5, alpha=0.3)
            
            for bar, apr in zip(bars, aprs):
                ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                         f'{apr:.1f}%', va='center', color=self.text_color, fontsize=10)
        
        # 2. Key metrics summary box
        ax2 = fig.add_subplot(gs[0, 2])
        self._setup_axes(ax2)
        ax2.axis('off')
        
        if market_data and market_data.get("underlying"):
            spot = market_data["underlying"].index_price
            rv_30 = market_data.get("realized_vol", {}).get("30d", 0) * 100
            
            # Find avg IV from options
            options = market_data.get("options", [])
            ivs = [o.iv for o in options if o.iv and o.iv > 0]
            avg_iv = sum(ivs) / len(ivs) if ivs else 0
            
            summary_text = (
                f"{'═' * 30}\n"
                f"       MARKET SNAPSHOT\n"
                f"{'═' * 30}\n\n"
                f"  Underlying: {market_data['underlying'].symbol}\n"
                f"  Spot Price: ${spot:,.0f}\n\n"
                f"  Avg IV: {avg_iv:.1f}%\n"
                f"  RV 30d: {rv_30:.1f}%\n"
                f"  IV-RV Spread: {avg_iv - rv_30:.1f}%\n\n"
                f"  Options Found: {len(options)}\n"
                f"  Capital: ${config.INITIAL_CAPITAL:,.0f}\n"
                f"{'═' * 30}"
            )
            
            ax2.text(0.5, 0.5, summary_text, ha='center', va='center',
                     color=self.style["positive_color"], fontsize=11,
                     family='monospace', transform=ax2.transAxes)
        
        # 3. Win rate comparison
        ax3 = fig.add_subplot(gs[1, 0])
        self._setup_axes(ax3)
        
        if viable:
            labels = [viable[s].strategy for s in strategies]
            win_rates = [viable[s].win_rate_pct for s in strategies]
            
            colors = [self.style["positive_color"] if w >= 50 else self.style["neutral_color"]
                      for w in win_rates]
            
            ax3.bar(range(len(win_rates)), win_rates, color=colors, alpha=0.8)
            ax3.axhline(y=50, color='white', linewidth=1, linestyle='--', alpha=0.5)
            ax3.set_xticks(range(len(labels)))
            ax3.set_xticklabels(labels, color=self.text_color, fontsize=9, rotation=30, ha='right')
            ax3.set_ylabel('Win Rate %', color=self.text_color)
            ax3.set_title('Win Rate by Strategy', color=self.text_color, fontsize=12)
        
        # 4. Max Drawdown comparison
        ax4 = fig.add_subplot(gs[1, 1])
        self._setup_axes(ax4)
        
        if viable:
            dds = [viable[s].max_drawdown_pct for s in strategies]
            
            ax4.bar(range(len(dds)), dds, color=self.style["negative_color"], alpha=0.7)
            ax4.set_xticks(range(len(labels)))
            ax4.set_xticklabels(labels, color=self.text_color, fontsize=9, rotation=30, ha='right')
            ax4.set_ylabel('Max Drawdown %', color=self.text_color)
            ax4.set_title('Risk (Max Drawdown)', color=self.text_color, fontsize=12)
        
        # 5. Premium vs Net PnL
        ax5 = fig.add_subplot(gs[1, 2])
        self._setup_axes(ax5)
        
        if viable:
            premiums = [viable[s].premium_collected for s in strategies]
            net_pnls = [viable[s].net_pnl for s in strategies]
            
            x = np.arange(len(strategies))
            width = 0.35
            
            ax5.bar(x - width/2, premiums, width, label='Premium',
                    color=self.style["positive_color"], alpha=0.7)
            pnl_colors = [self.style["positive_color"] if p > 0 else self.style["negative_color"]
                          for p in net_pnls]
            ax5.bar(x + width/2, net_pnls, width, label='Net PnL', color=pnl_colors, alpha=0.7)
            
            ax5.set_xticks(x)
            ax5.set_xticklabels(labels, color=self.text_color, fontsize=9, rotation=30, ha='right')
            ax5.set_ylabel('USD ($)', color=self.text_color)
            ax5.set_title('Premium vs Net PnL', color=self.text_color, fontsize=12)
            ax5.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color,
                       fontsize=9)
            ax5.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # 6. Equity curves
        ax6 = fig.add_subplot(gs[2, :2])
        self._setup_axes(ax6)
        
        if viable:
            colors = [self.style["positive_color"], self.style["neutral_color"],
                      '#9b59b6', '#3498db']
            
            for i, (name, result) in enumerate(viable.items()):
                if result.equity_curve:
                    x = range(len(result.equity_curve))
                    ax6.plot(x, result.equity_curve,
                             color=colors[i % len(colors)],
                             linewidth=2, alpha=0.9,
                             label=f"{result.strategy}")
            
            ax6.axhline(y=config.INITIAL_CAPITAL, color='white', linewidth=1,
                        linestyle='--', alpha=0.3)
            ax6.set_xlabel('Time', color=self.text_color)
            ax6.set_ylabel('Portfolio Value ($)', color=self.text_color)
            ax6.set_title('Equity Curves', color=self.text_color, fontsize=12)
            ax6.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color,
                       loc='upper left', fontsize=9)
            ax6.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # 7. IV-RV scatter
        ax7 = fig.add_subplot(gs[2, 2])
        self._setup_axes(ax7)
        
        if viable:
            for i, (name, result) in enumerate(viable.items()):
                color = [self.style["positive_color"], self.style["neutral_color"],
                         '#9b59b6', '#3498db'][i % 4]
                size = max(100, abs(result.net_pnl) / 5)
                ax7.scatter(result.iv_rv_spread, result.apr_pct,
                           s=size, c=color, alpha=0.8,
                           edgecolors='white', linewidths=1,
                           label=result.strategy)
            
            ax7.axvline(x=0, color='white', linewidth=0.5, alpha=0.3)
            ax7.axhline(y=0, color='white', linewidth=0.5, alpha=0.3)
            ax7.axvline(x=config.IV_RV_ENTRY_THRESHOLD * 100, color=self.style["neutral_color"],
                        linewidth=1.5, linestyle='--', alpha=0.5)
            ax7.set_xlabel('IV-RV Spread (%)', color=self.text_color)
            ax7.set_ylabel('APR (%)', color=self.text_color)
            ax7.set_title('APR vs IV-RV Spread', color=self.text_color, fontsize=12)
            ax7.legend(facecolor=self.style["ax_facecolor"], labelcolor=self.text_color,
                       fontsize=8, loc='upper left')
        
        return self._save_figure(fig, "overview_dashboard")
    
    def generate_all_charts(self, results: Dict[str, BacktestResult],
                            market_data: Dict[str, Any] = None,
                            underlying: str = "BTC") -> List[str]:
        """
        Generate all visualization charts.
        
        Returns list of saved file paths.
        """
        paths = []
        
        logger.info("Generating strategy comparison chart...")
        path = self.plot_strategy_comparison(results)
        if path:
            paths.append(path)
        
        logger.info("Generating equity curves...")
        path = self.plot_equity_curves(results)
        if path:
            paths.append(path)
        
        logger.info("Generating IV-RV analysis...")
        path = self.plot_iv_rv_comparison(results, market_data)
        if path:
            paths.append(path)
        
        logger.info("Generating PnL breakdown...")
        path = self.plot_pnl_breakdown(results)
        if path:
            paths.append(path)
        
        if market_data:
            logger.info("Generating IV surface...")
            path = self.plot_iv_surface(market_data, underlying)
            if path:
                paths.append(path)
        
        logger.info("Generating overview dashboard...")
        path = self.plot_overview_dashboard(results, market_data)
        if path:
            paths.append(path)
        
        logger.info(f"Generated {len(paths)} charts in {self.output_dir}")
        return paths
