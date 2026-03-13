"""
Funding Rate Arbitrage Backtester — Single-Exchange Spot-Perp

Simulates: Long Spot + Short Perp, collecting funding rate payments.

For each instrument:
1. Enter at first candle: buy spot + short perp (delta neutral)
2. Every funding settlement (8h): collect/pay funding rate
3. Track cumulative PnL, max drawdown, margin usage
4. Exit at last candle: close both legs

Uses kline data for price tracking + margin estimation.
Uses funding_rate data for the actual PnL source.

Usage:
    python3 -m projects.funding_rate_arb.backtester
    python3 -m projects.funding_rate_arb.backtester --exchange OKX --top 30
    python3 -m projects.funding_rate_arb.backtester --exchange BINANCEFUTURES --symbol BTCUSDT
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from database.client import PostgresClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== Config ====================

INITIAL_CAPITAL = 10_000       # USD per position
LEVERAGE_PERP = 1              # 1x (no leverage for safety)
SPOT_FEE = 0.001               # 0.1% taker
PERP_FEE = 0.0005              # 0.05% taker
SLIPPAGE = 0.0002              # 0.02% per side
ENTRY_COST_RATE = SPOT_FEE + PERP_FEE + 2 * SLIPPAGE   # both legs open
EXIT_COST_RATE = SPOT_FEE + PERP_FEE + 2 * SLIPPAGE    # both legs close
SETTLEMENTS_PER_DAY = 3
MIN_FUNDING_POINTS = 10
MIN_KLINE_POINTS = 10


class BacktestResult:
    """Result for one instrument backtest."""

    def __init__(self, symbol: str, exchange: str, pair: str):
        self.symbol = symbol
        self.exchange = exchange
        self.pair = pair

        # Filled by backtest
        self.total_return_pct: float = 0
        self.apr_pct: float = 0
        self.sharpe: float = 0
        self.max_drawdown_pct: float = 0
        self.win_rate_pct: float = 0
        self.total_funding_pnl: float = 0
        self.total_basis_pnl: float = 0
        self.total_fee_cost: float = 0
        self.net_pnl: float = 0
        self.num_settlements: int = 0
        self.holding_days: float = 0
        self.avg_funding_rate: float = 0
        self.positive_rate_pct: float = 0
        self.entry_price: float = 0
        self.exit_price: float = 0
        self.price_change_pct: float = 0
        self.date_start: Optional[datetime] = None
        self.date_end: Optional[datetime] = None
        self.equity_curve: List[float] = []
        self.viable: bool = False    # passes minimum criteria
        self.reason: str = ""


class FundingRateBacktester:
    """Backtest spot-perp funding rate arbitrage."""

    def __init__(self, db: PostgresClient):
        self.db = db

    async def get_instruments(
        self, exchange_name: Optional[str] = None, symbol: Optional[str] = None
    ) -> List[Dict]:
        """Get PERP instruments to backtest."""
        query = """
            SELECT i.instrument_id, i.symbol, i.base_currency, i.quote_currency,
                   i.exchange_id, e.name AS exchange
            FROM instruments i
            JOIN exchanges e ON i.exchange_id = e.id
            WHERE i.type = 'PERP' AND i.is_active
        """
        params = []
        if exchange_name:
            query += f" AND e.name = ${len(params)+1}"
            params.append(exchange_name)
        if symbol:
            query += f" AND i.symbol = ${len(params)+1}"
            params.append(symbol)
        query += " ORDER BY i.symbol"
        return await self.db.read(query, *params)

    async def load_data(
        self, instrument_id: str, interval: str = "4h"
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load kline + funding rate data for an instrument."""
        klines = await self.db.read("""
            SELECT open_time, open, high, low, close, volume
            FROM klines
            WHERE instrument_id = $1 AND interval = $2
            ORDER BY open_time
        """, instrument_id, interval)

        funding = await self.db.read("""
            SELECT funding_rate, funding_time
            FROM funding_rates
            WHERE instrument_id = $1
            ORDER BY funding_time
        """, instrument_id)

        kdf = pd.DataFrame(klines) if klines else pd.DataFrame()
        fdf = pd.DataFrame(funding) if funding else pd.DataFrame()

        if not kdf.empty:
            kdf['open_time'] = pd.to_datetime(kdf['open_time'], utc=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                kdf[col] = kdf[col].astype(float)

        if not fdf.empty:
            fdf['funding_time'] = pd.to_datetime(fdf['funding_time'], utc=True)
            fdf['funding_rate'] = fdf['funding_rate'].astype(float)

        return kdf, fdf

    def run_backtest(
        self, symbol: str, exchange: str, pair: str,
        kdf: pd.DataFrame, fdf: pd.DataFrame,
    ) -> BacktestResult:
        """
        Run backtest for one instrument.

        Strategy: Long Spot + Short Perp (1:1 notional)
        - Enter at first available candle close
        - Collect/pay funding every settlement
        - Track equity curve
        - Exit at last candle close
        """
        result = BacktestResult(symbol, exchange, pair)

        if len(fdf) < MIN_FUNDING_POINTS:
            result.reason = f"insufficient funding data ({len(fdf)} points)"
            return result

        if len(kdf) < MIN_KLINE_POINTS:
            result.reason = f"insufficient kline data ({len(kdf)} points)"
            return result

        # Align time range
        start_time = max(kdf['open_time'].min(), fdf['funding_time'].min())
        end_time = min(kdf['open_time'].max(), fdf['funding_time'].max())

        if start_time >= end_time:
            result.reason = "no overlapping time range"
            return result

        fdf_range = fdf[(fdf['funding_time'] >= start_time) & (fdf['funding_time'] <= end_time)].copy()
        kdf_range = kdf[(kdf['open_time'] >= start_time) & (kdf['open_time'] <= end_time)].copy()

        if len(fdf_range) < MIN_FUNDING_POINTS:
            result.reason = f"insufficient overlapping funding data ({len(fdf_range)})"
            return result

        # Entry
        entry_price = float(kdf_range.iloc[0]['close'])
        exit_price = float(kdf_range.iloc[-1]['close'])

        if entry_price <= 0:
            result.reason = "invalid entry price"
            return result

        # Position sizing: use INITIAL_CAPITAL, split evenly
        # Spot: buy $CAPITAL worth at entry_price
        # Perp: short $CAPITAL notional at entry_price
        capital = INITIAL_CAPITAL
        notional = capital  # each leg = capital

        # Entry costs
        entry_fee = notional * ENTRY_COST_RATE * 2  # both legs

        # Simulate funding settlements
        funding_pnl = 0.0
        equity_curve = [capital]
        settlement_pnls = []

        for _, row in fdf_range.iterrows():
            rate = row['funding_rate']
            # Short perp collects positive funding, pays negative
            pnl = notional * rate
            funding_pnl += pnl
            settlement_pnls.append(pnl)
            equity_curve.append(capital + funding_pnl - entry_fee)

        # Basis PnL: price change affects both legs
        # Spot: (exit - entry) / entry * notional
        # Perp short: -(exit - entry) / entry * notional
        # Net basis PnL ≈ 0 (delta neutral), but not exactly due to funding
        # In practice there's some basis risk from perp mark price vs spot
        # For simplicity, assume perfect hedge (basis PnL = 0)
        basis_pnl = 0.0

        # Exit costs
        exit_fee = notional * EXIT_COST_RATE * 2
        total_fees = entry_fee + exit_fee

        # Net PnL
        net_pnl = funding_pnl + basis_pnl - total_fees

        # Metrics
        holding_time = (end_time - start_time).total_seconds()
        holding_days = holding_time / 86400

        if holding_days <= 0:
            result.reason = "zero holding period"
            return result

        total_return = net_pnl / capital
        apr = total_return * (365 / holding_days) if holding_days > 0 else 0

        # Sharpe from per-settlement returns
        if settlement_pnls:
            returns = np.array(settlement_pnls) / notional
            avg_ret = np.mean(returns)
            std_ret = np.std(returns)
            sharpe = (avg_ret / std_ret * np.sqrt(SETTLEMENTS_PER_DAY * 365)) if std_ret > 0 else 0
            positive_count = np.sum(np.array(settlement_pnls) > 0)
            win_rate = positive_count / len(settlement_pnls) * 100
        else:
            sharpe = 0
            win_rate = 0

        # Max drawdown from equity curve
        eq = np.array(equity_curve)
        running_max = np.maximum.accumulate(eq)
        drawdown = (eq - running_max) / running_max
        max_dd = drawdown.min() * 100

        # Price change
        price_change = (exit_price - entry_price) / entry_price * 100

        # Fill result
        result.total_return_pct = round(total_return * 100, 2)
        result.apr_pct = round(apr * 100, 2)
        result.sharpe = round(sharpe, 2)
        result.max_drawdown_pct = round(max_dd, 2)
        result.win_rate_pct = round(win_rate, 1)
        result.total_funding_pnl = round(funding_pnl, 2)
        result.total_basis_pnl = round(basis_pnl, 2)
        result.total_fee_cost = round(total_fees, 2)
        result.net_pnl = round(net_pnl, 2)
        result.num_settlements = len(settlement_pnls)
        result.holding_days = round(holding_days, 1)
        result.avg_funding_rate = float(np.mean(fdf_range['funding_rate']))
        result.positive_rate_pct = round(
            (fdf_range['funding_rate'] > 0).mean() * 100, 1
        )
        result.entry_price = entry_price
        result.exit_price = exit_price
        result.price_change_pct = round(price_change, 1)
        result.date_start = start_time
        result.date_end = end_time
        result.equity_curve = equity_curve
        result.viable = True
        return result

    def print_report(self, results: List[BacktestResult], exchange_filter: Optional[str] = None):
        """Print backtest report."""
        viable = [r for r in results if r.viable]
        non_viable = [r for r in results if not r.viable]

        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        print("\n" + "=" * 120)
        print(f"  FUNDING RATE ARB BACKTESTER — Spot-Perp Strategy")
        print(f"  {now_str}")
        print(f"  Capital: ${INITIAL_CAPITAL:,.0f} per position | Fees: {(ENTRY_COST_RATE+EXIT_COST_RATE)*100:.2f}% round-trip")
        print("=" * 120)

        if not viable:
            print("\n  No viable instruments found.")
            print(f"  ({len(non_viable)} instruments skipped)")
            return

        # Sort by APR
        viable.sort(key=lambda r: r.apr_pct, reverse=True)

        # Profitable
        profitable = [r for r in viable if r.net_pnl > 0]
        unprofitable = [r for r in viable if r.net_pnl <= 0]

        print(f"\n  Analyzed: {len(viable)} instruments | Profitable: {len(profitable)} | Unprofitable: {len(unprofitable)} | Skipped: {len(non_viable)}")

        # Top profitable
        print(f"\n{'─' * 120}")
        print(f"  ✅ PROFITABLE — Top Instruments (net PnL > 0)")
        print(f"{'─' * 120}")
        if profitable:
            print(f"  {'Exchange':<18} {'Pair':<14} {'Net PnL':>10} {'APR':>8} {'Return':>8} {'Sharpe':>8} {'MaxDD':>8} {'Win%':>6} {'Pos%':>6} {'Days':>6} {'Settl':>6}")
            print(f"  {'─'*18} {'─'*14} {'─'*10} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*6} {'─'*6} {'─'*6} {'─'*6}")
            for r in profitable[:40]:
                print(
                    f"  {r.exchange:<18} "
                    f"{r.pair:<14} "
                    f"${r.net_pnl:>9,.0f} "
                    f"{r.apr_pct:>7.1f}% "
                    f"{r.total_return_pct:>7.2f}% "
                    f"{r.sharpe:>8.2f} "
                    f"{r.max_drawdown_pct:>7.2f}% "
                    f"{r.win_rate_pct:>5.1f}% "
                    f"{r.positive_rate_pct:>5.1f}% "
                    f"{r.holding_days:>5.1f}d "
                    f"{r.num_settlements:>6}"
                )
        else:
            print("  None")

        # Unprofitable summary
        if unprofitable:
            print(f"\n{'─' * 120}")
            print(f"  ❌ UNPROFITABLE — Bottom 10")
            print(f"{'─' * 120}")
            unprofitable.sort(key=lambda r: r.apr_pct)
            print(f"  {'Exchange':<18} {'Pair':<14} {'Net PnL':>10} {'APR':>8} {'Reason'}")
            print(f"  {'─'*18} {'─'*14} {'─'*10} {'─'*8} {'─'*40}")
            for r in unprofitable[:10]:
                reason = f"funding PnL ${r.total_funding_pnl:.0f} - fees ${r.total_fee_cost:.0f}"
                print(
                    f"  {r.exchange:<18} "
                    f"{r.pair:<14} "
                    f"${r.net_pnl:>9,.0f} "
                    f"{r.apr_pct:>7.1f}% "
                    f" {reason}"
                )

        # Best opportunities summary
        if profitable:
            best = profitable[:5]
            print(f"\n{'─' * 120}")
            print(f"  🏆 TOP 5 RECOMMENDED")
            print(f"{'─' * 120}")
            for i, r in enumerate(best, 1):
                print(f"  {i}. {r.exchange} {r.pair}")
                print(f"     APR: {r.apr_pct:.1f}% | Sharpe: {r.sharpe:.2f} | MaxDD: {r.max_drawdown_pct:.2f}% | Win: {r.win_rate_pct:.0f}%")
                print(f"     Net PnL: ${r.net_pnl:,.0f} over {r.holding_days:.0f} days ({r.num_settlements} settlements)")
                print(f"     Avg funding: {r.avg_funding_rate*100:.4f}% | Positive: {r.positive_rate_pct:.0f}%")
                print()

        print(f"{'=' * 120}\n")

    # ==================== Save Results ====================

    def save_results(self, results: List[BacktestResult], output_dir: str):
        """Save all results to CSV + JSON."""
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')

        # Build DataFrame from all viable results
        viable = [r for r in results if r.viable]
        if not viable:
            logger.warning("No viable results to save")
            return

        rows = []
        for r in viable:
            rows.append({
                'exchange': r.exchange,
                'symbol': r.symbol,
                'pair': r.pair,
                'net_pnl': r.net_pnl,
                'total_return_pct': r.total_return_pct,
                'apr_pct': r.apr_pct,
                'sharpe': r.sharpe,
                'max_drawdown_pct': r.max_drawdown_pct,
                'win_rate_pct': r.win_rate_pct,
                'positive_rate_pct': r.positive_rate_pct,
                'total_funding_pnl': r.total_funding_pnl,
                'total_fee_cost': r.total_fee_cost,
                'num_settlements': r.num_settlements,
                'holding_days': r.holding_days,
                'avg_funding_rate': r.avg_funding_rate,
                'entry_price': r.entry_price,
                'exit_price': r.exit_price,
                'price_change_pct': r.price_change_pct,
                'date_start': r.date_start.isoformat() if r.date_start else None,
                'date_end': r.date_end.isoformat() if r.date_end else None,
                'profitable': r.net_pnl > 0,
            })

        df = pd.DataFrame(rows).sort_values('apr_pct', ascending=False)

        # Save CSV
        csv_path = os.path.join(output_dir, f"backtest_{ts}.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Results saved: {csv_path}")

        # Save JSON (detailed with equity curves for top instruments)
        import json
        profitable = [r for r in viable if r.net_pnl > 0]
        profitable.sort(key=lambda r: r.apr_pct, reverse=True)

        json_data = {
            'timestamp': ts,
            'config': {
                'initial_capital': INITIAL_CAPITAL,
                'spot_fee': SPOT_FEE,
                'perp_fee': PERP_FEE,
                'slippage': SLIPPAGE,
                'entry_cost_rate': ENTRY_COST_RATE,
                'exit_cost_rate': EXIT_COST_RATE,
            },
            'summary': {
                'total_analyzed': len(viable),
                'profitable': len(profitable),
                'unprofitable': len(viable) - len(profitable),
                'skipped': len(results) - len(viable),
            },
            'profitable_instruments': [
                {
                    'exchange': r.exchange, 'symbol': r.symbol, 'pair': r.pair,
                    'net_pnl': r.net_pnl, 'apr_pct': r.apr_pct, 'sharpe': r.sharpe,
                    'max_drawdown_pct': r.max_drawdown_pct, 'win_rate_pct': r.win_rate_pct,
                    'positive_rate_pct': r.positive_rate_pct,
                    'num_settlements': r.num_settlements, 'holding_days': r.holding_days,
                    'avg_funding_rate': r.avg_funding_rate,
                    'equity_curve': r.equity_curve,
                }
                for r in profitable[:30]
            ],
        }

        json_path = os.path.join(output_dir, f"backtest_{ts}.json")
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        logger.info(f"Details saved: {json_path}")

        return csv_path, json_path

    # ==================== Visualizations ====================

    def generate_charts(self, results: List[BacktestResult], output_dir: str):
        """Generate backtest visualization charts."""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.gridspec import GridSpec

        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')

        viable = [r for r in results if r.viable]
        profitable = sorted([r for r in viable if r.net_pnl > 0], key=lambda r: r.apr_pct, reverse=True)
        all_sorted = sorted(viable, key=lambda r: r.apr_pct, reverse=True)

        if not viable:
            return

        # ---- Chart 1: Overview Dashboard (4 subplots) ----
        fig = plt.figure(figsize=(20, 16), facecolor='#1a1a2e')
        fig.suptitle('Funding Rate Arbitrage — Backtest Report', fontsize=20, color='white', fontweight='bold', y=0.98)
        gs = GridSpec(2, 2, hspace=0.35, wspace=0.3, left=0.08, right=0.95, top=0.93, bottom=0.06)

        ax_colors = {'facecolor': '#16213e'}
        text_color = '#e0e0e0'

        # 1a: APR Distribution
        ax1 = fig.add_subplot(gs[0, 0], **ax_colors)
        aprs = [r.apr_pct for r in all_sorted]
        colors = ['#00d4aa' if a > 0 else '#ff4757' for a in aprs]
        ax1.bar(range(len(aprs)), aprs, color=colors, alpha=0.8, width=1.0)
        ax1.axhline(y=0, color='white', linewidth=0.5, alpha=0.5)
        ax1.set_title('APR Distribution (All Instruments)', color=text_color, fontsize=13)
        ax1.set_xlabel('Instruments (sorted by APR)', color=text_color, fontsize=10)
        ax1.set_ylabel('APR %', color=text_color, fontsize=10)
        ax1.tick_params(colors=text_color)
        ax1.set_xlim(-1, len(aprs))

        # 1b: Top 20 Profitable APR
        ax2 = fig.add_subplot(gs[0, 1], **ax_colors)
        top_n = profitable[:20]
        if top_n:
            labels = [f"{r.pair}" for r in top_n]
            vals = [r.apr_pct for r in top_n]
            y_pos = range(len(labels))
            bars = ax2.barh(y_pos, vals, color='#00d4aa', alpha=0.85)
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels(labels, color=text_color, fontsize=9)
            ax2.invert_yaxis()
            ax2.set_title('Top 20 Profitable — APR %', color=text_color, fontsize=13)
            ax2.set_xlabel('APR %', color=text_color, fontsize=10)
            ax2.tick_params(colors=text_color)
            for bar, v in zip(bars, vals):
                ax2.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                         f'{v:.1f}%', va='center', color=text_color, fontsize=8)

        # 1c: Sharpe vs APR scatter
        ax3 = fig.add_subplot(gs[1, 0], **ax_colors)
        for r in viable:
            color = '#00d4aa' if r.net_pnl > 0 else '#ff4757'
            alpha = 0.8 if r.net_pnl > 0 else 0.3
            size = max(20, min(200, abs(r.net_pnl) / 2))
            ax3.scatter(r.apr_pct, r.sharpe, c=color, s=size, alpha=alpha, edgecolors='none')
        ax3.axhline(y=0, color='white', linewidth=0.3, alpha=0.3)
        ax3.axvline(x=0, color='white', linewidth=0.3, alpha=0.3)
        ax3.set_title('Sharpe vs APR (size = PnL)', color=text_color, fontsize=13)
        ax3.set_xlabel('APR %', color=text_color, fontsize=10)
        ax3.set_ylabel('Sharpe Ratio', color=text_color, fontsize=10)
        ax3.tick_params(colors=text_color)

        # 1d: Win Rate vs Positive Funding %
        ax4 = fig.add_subplot(gs[1, 1], **ax_colors)
        for r in viable:
            color = '#00d4aa' if r.net_pnl > 0 else '#ff4757'
            alpha = 0.8 if r.net_pnl > 0 else 0.3
            ax4.scatter(r.positive_rate_pct, r.win_rate_pct, c=color, s=50, alpha=alpha, edgecolors='none')
        ax4.set_title('Win Rate vs Positive Funding %', color=text_color, fontsize=13)
        ax4.set_xlabel('Positive Funding Rate %', color=text_color, fontsize=10)
        ax4.set_ylabel('Settlement Win Rate %', color=text_color, fontsize=10)
        ax4.tick_params(colors=text_color)
        ax4.plot([0, 100], [0, 100], '--', color='white', alpha=0.2)

        overview_path = os.path.join(output_dir, f"overview_{ts}.png")
        fig.savefig(overview_path, dpi=150, facecolor=fig.get_facecolor())
        plt.close(fig)
        logger.info(f"Chart saved: {overview_path}")

        # ---- Chart 2: Equity Curves for Top 10 ----
        top_equity = profitable[:10]
        if top_equity:
            fig2, axes = plt.subplots(
                min(5, len(top_equity)), 2, figsize=(18, 4 * min(5, len(top_equity))),
                facecolor='#1a1a2e'
            )
            fig2.suptitle('Equity Curves — Top Profitable Instruments',
                          fontsize=18, color='white', fontweight='bold', y=1.01)

            if len(top_equity) == 1:
                axes = np.array([[axes[0], axes[1]]])
            elif len(top_equity) <= 2:
                axes = axes.reshape(1, -1)

            for idx, r in enumerate(top_equity):
                row = idx // 2
                col = idx % 2
                if row >= axes.shape[0]:
                    break
                ax = axes[row, col]
                ax.set_facecolor('#16213e')

                eq = np.array(r.equity_curve)
                x = range(len(eq))
                ax.plot(x, eq, color='#00d4aa', linewidth=1.5, alpha=0.9)
                ax.fill_between(x, INITIAL_CAPITAL, eq,
                                where=eq >= INITIAL_CAPITAL, color='#00d4aa', alpha=0.15)
                ax.fill_between(x, INITIAL_CAPITAL, eq,
                                where=eq < INITIAL_CAPITAL, color='#ff4757', alpha=0.15)
                ax.axhline(y=INITIAL_CAPITAL, color='white', linewidth=0.5, alpha=0.3, linestyle='--')

                ax.set_title(
                    f"{r.exchange} {r.pair}  |  APR {r.apr_pct:.1f}%  |  PnL ${r.net_pnl:,.0f}",
                    color=text_color, fontsize=10
                )
                ax.set_xlabel('Settlement #', color=text_color, fontsize=8)
                ax.set_ylabel('Equity ($)', color=text_color, fontsize=8)
                ax.tick_params(colors=text_color, labelsize=7)

            # Hide unused axes
            total_axes = axes.shape[0] * 2
            for idx in range(len(top_equity), total_axes):
                row, col = idx // 2, idx % 2
                if row < axes.shape[0]:
                    axes[row, col].set_visible(False)

            equity_path = os.path.join(output_dir, f"equity_curves_{ts}.png")
            fig2.savefig(equity_path, dpi=150, facecolor=fig2.get_facecolor(), bbox_inches='tight')
            plt.close(fig2)
            logger.info(f"Chart saved: {equity_path}")

        # ---- Chart 3: PnL Breakdown (funding vs fees) ----
        if profitable:
            fig3, ax = plt.subplots(figsize=(16, 8), facecolor='#1a1a2e')
            ax.set_facecolor('#16213e')

            top_pnl = profitable[:20]
            labels = [f"{r.pair}" for r in top_pnl]
            funding_pnls = [r.total_funding_pnl for r in top_pnl]
            fee_costs = [-r.total_fee_cost for r in top_pnl]
            net_pnls = [r.net_pnl for r in top_pnl]

            x = np.arange(len(labels))
            width = 0.25

            ax.bar(x - width, funding_pnls, width, label='Funding PnL', color='#00d4aa', alpha=0.85)
            ax.bar(x, fee_costs, width, label='Fees (cost)', color='#ff4757', alpha=0.85)
            ax.bar(x + width, net_pnls, width, label='Net PnL', color='#ffd700', alpha=0.85)

            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=45, ha='right', color=text_color, fontsize=9)
            ax.set_ylabel('USD ($)', color=text_color, fontsize=11)
            ax.set_title('PnL Breakdown — Funding Revenue vs Fees', color=text_color, fontsize=14)
            ax.legend(facecolor='#16213e', edgecolor='#333', labelcolor=text_color)
            ax.axhline(y=0, color='white', linewidth=0.3, alpha=0.3)
            ax.tick_params(colors=text_color)

            pnl_path = os.path.join(output_dir, f"pnl_breakdown_{ts}.png")
            fig3.savefig(pnl_path, dpi=150, facecolor=fig3.get_facecolor(), bbox_inches='tight')
            plt.close(fig3)
            logger.info(f"Chart saved: {pnl_path}")

        return output_dir


    async def bulk_load_data(
        self, instrument_ids: List[str], interval: str = "4h"
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load ALL kline + funding data in two bulk queries (memory efficient)."""
        logger.info(f"Bulk loading klines for {len(instrument_ids)} instruments...")
        klines = await self.db.read("""
            SELECT instrument_id, open_time, open, high, low, close, volume
            FROM klines
            WHERE instrument_id = ANY($1) AND interval = $2
            ORDER BY instrument_id, open_time
        """, instrument_ids, interval)

        logger.info(f"Bulk loading funding rates...")
        funding = await self.db.read("""
            SELECT instrument_id, funding_rate, funding_time
            FROM funding_rates
            WHERE instrument_id = ANY($1)
            ORDER BY instrument_id, funding_time
        """, instrument_ids)

        kdf = pd.DataFrame(klines) if klines else pd.DataFrame()
        fdf = pd.DataFrame(funding) if funding else pd.DataFrame()

        if not kdf.empty:
            kdf['open_time'] = pd.to_datetime(kdf['open_time'], utc=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                kdf[col] = kdf[col].astype(float)

        if not fdf.empty:
            fdf['funding_time'] = pd.to_datetime(fdf['funding_time'], utc=True)
            fdf['funding_rate'] = fdf['funding_rate'].astype(float)

        logger.info(f"Loaded {len(kdf)} klines, {len(fdf)} funding rates")
        return kdf, fdf


async def run_backtest(exchange: Optional[str] = None, symbol: Optional[str] = None, top: int = 0):
    """Main entry."""
    db = PostgresClient()
    await db.init_pool()

    bt = FundingRateBacktester(db)

    logger.info("Loading instruments...")
    instruments = await bt.get_instruments(exchange_name=exchange, symbol=symbol)
    logger.info(f"Found {len(instruments)} instruments")

    # Bulk load all data in 2 queries
    inst_ids = [i['instrument_id'] for i in instruments]
    all_klines, all_funding = await bt.bulk_load_data(inst_ids)

    results = []
    for i, inst in enumerate(instruments):
        iid = inst['instrument_id']
        kdf = all_klines[all_klines['instrument_id'] == iid] if not all_klines.empty else pd.DataFrame()
        fdf = all_funding[all_funding['instrument_id'] == iid] if not all_funding.empty else pd.DataFrame()

        result = bt.run_backtest(
            symbol=inst['symbol'],
            exchange=inst['exchange'],
            pair=f"{inst['base_currency']}/{inst['quote_currency']}",
            kdf=kdf, fdf=fdf,
        )
        results.append(result)

    bt.print_report(results, exchange)

    # Save results + generate charts
    output_dir = os.path.join(
        os.path.dirname(__file__), 'output',
        exchange.lower() if exchange else 'all',
    )
    bt.save_results(results, output_dir)
    bt.generate_charts(results, output_dir)

    await db.close()


def main():
    parser = argparse.ArgumentParser(description="Funding Rate Arb Backtester")
    parser.add_argument("--exchange", type=str, default=None, help="Filter by exchange (OKX, BINANCEFUTURES)")
    parser.add_argument("--symbol", type=str, default=None, help="Single symbol to test")
    parser.add_argument("--top", type=int, default=0, help="Show top N only")
    parser.add_argument("--output", type=str, default=None, help="Output directory")
    args = parser.parse_args()
    asyncio.run(run_backtest(exchange=args.exchange, symbol=args.symbol, top=args.top))


if __name__ == "__main__":
    main()
