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
    await db.close()


def main():
    parser = argparse.ArgumentParser(description="Funding Rate Arb Backtester")
    parser.add_argument("--exchange", type=str, default=None, help="Filter by exchange (OKX, BINANCEFUTURES)")
    parser.add_argument("--symbol", type=str, default=None, help="Single symbol to test")
    parser.add_argument("--top", type=int, default=0, help="Show top N only")
    args = parser.parse_args()
    asyncio.run(run_backtest(exchange=args.exchange, symbol=args.symbol, top=args.top))


if __name__ == "__main__":
    main()
