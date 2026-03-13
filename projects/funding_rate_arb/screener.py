"""
Funding Rate Arbitrage Screener — Single-Exchange Spot-Perp

Screens ALL perp instruments across OKX and Binance for funding rate
arbitrage opportunities (long spot + short perp when funding > 0).

Usage:
    python3 -m projects.funding_rate_arb.screener
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from database.client import PostgresClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== Config ====================

SETTLEMENTS_PER_DAY = 3
ANNUALIZATION = SETTLEMENTS_PER_DAY * 365  # 1095

# Costs per side (conservative)
SPOT_FEE = 0.001       # 0.1% spot taker
PERP_FEE = 0.0005      # 0.05% perp taker
ENTRY_COST = SPOT_FEE + PERP_FEE           # 0.15% to open both legs
EXIT_COST = SPOT_FEE + PERP_FEE            # 0.15% to close both legs
ROUND_TRIP_COST = ENTRY_COST + EXIT_COST   # 0.30%

# Filters
MIN_DATA_POINTS = 10     # at least ~3 days of data
MIN_POSITIVE_RATE = 0.6  # at least 60% of funding periods are positive
TOP_N = 30


class FundingRateScreener:
    """Screen for single-exchange funding rate arb opportunities."""

    def __init__(self, db: PostgresClient):
        self.db = db

    async def load_all_funding_rates(self) -> pd.DataFrame:
        """Load all funding rate data with instrument + exchange info."""
        rows = await self.db.read("""
            SELECT
                fr.instrument_id,
                fr.funding_rate,
                fr.funding_time,
                i.symbol,
                i.base_currency,
                i.quote_currency,
                i.exchange_id,
                e.name AS exchange
            FROM funding_rates fr
            JOIN instruments i ON fr.instrument_id = i.instrument_id
            JOIN exchanges e ON i.exchange_id = e.id
            WHERE i.type = 'PERP' AND i.is_active
            ORDER BY fr.funding_time
        """)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df['funding_time'] = pd.to_datetime(df['funding_time'], utc=True)
        return df

    def analyze_single_exchange(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Per-instrument funding rate statistics for spot-perp arb.

        Strategy: Long Spot + Short Perp when funding > 0
        - You COLLECT funding when rate is positive (shorts get paid)
        - You PAY funding when rate is negative (shorts pay longs)
        - Net = sum of all funding rates over the period
        """
        stats = []

        for (inst_id, exchange), group in df.groupby(['instrument_id', 'exchange']):
            rates = group['funding_rate']
            n = len(rates)

            if n < MIN_DATA_POINTS:
                continue

            base = group['base_currency'].iloc[0]
            quote = group['quote_currency'].iloc[0]
            symbol = group['symbol'].iloc[0]

            mean_rate = rates.mean()
            median_rate = rates.median()
            std_rate = rates.std()

            # Positive rate analysis (favorable for short perp)
            positive_count = (rates > 0).sum()
            negative_count = (rates < 0).sum()
            positive_pct = positive_count / n * 100

            # Negative rate analysis (favorable for long perp)
            negative_pct = negative_count / n * 100

            # APR calculation (for short perp strategy)
            # If mean_rate > 0: you earn this by shorting perp
            apr_gross = mean_rate * ANNUALIZATION * 100

            # Net APR after round-trip costs amortized
            # Assume position held for the full data period
            # Cost is one-time, so amortize over the period
            holding_periods = n  # number of funding settlements
            cost_per_period = ROUND_TRIP_COST / holding_periods if holding_periods > 0 else 0
            apr_net = (mean_rate - cost_per_period) * ANNUALIZATION * 100

            # Consistency: Sharpe-like ratio
            sharpe = (mean_rate / std_rate * np.sqrt(ANNUALIZATION)) if std_rate > 0 else 0

            # Recent trend: last 9 funding rates (~3 days)
            recent = rates.tail(9)
            recent_mean = recent.mean()
            recent_apr = recent_mean * ANNUALIZATION * 100

            # Current (last) rate
            current_rate = rates.iloc[-1]
            current_apr = current_rate * ANNUALIZATION * 100

            # Max drawdown: worst consecutive negative funding
            cumulative = rates.cumsum()
            running_max = cumulative.cummax()
            drawdown = (cumulative - running_max)
            max_drawdown = drawdown.min()
            max_drawdown_apr = max_drawdown * ANNUALIZATION * 100

            # Date range
            date_start = group['funding_time'].min()
            date_end = group['funding_time'].max()

            stats.append({
                'exchange': exchange,
                'pair': f"{base}/{quote}",
                'symbol': symbol,
                'instrument_id': inst_id,
                'mean_rate': mean_rate,
                'median_rate': median_rate,
                'std_rate': std_rate,
                'current_rate': current_rate,
                'current_apr_%': round(current_apr, 1),
                'recent_apr_%': round(recent_apr, 1),
                'apr_gross_%': round(apr_gross, 1),
                'apr_net_%': round(apr_net, 1),
                'sharpe': round(sharpe, 2),
                'positive_%': round(positive_pct, 1),
                'negative_%': round(negative_pct, 1),
                'max_dd_apr_%': round(max_drawdown_apr, 1),
                'data_points': n,
                'date_start': date_start,
                'date_end': date_end,
            })

        result = pd.DataFrame(stats)
        return result

    def compare_exchanges(self, stats: pd.DataFrame) -> pd.DataFrame:
        """
        For pairs available on multiple exchanges, compare which exchange
        offers the better funding rate for the arb.
        """
        # Group by pair, find which exchange has higher mean funding rate
        comparisons = []

        for pair, group in stats.groupby('pair'):
            if len(group) < 2:
                continue

            # Sort by apr_net descending
            group_sorted = group.sort_values('apr_net_%', ascending=False)
            best = group_sorted.iloc[0]
            second = group_sorted.iloc[1]

            comparisons.append({
                'pair': pair,
                'best_exchange': best['exchange'],
                'best_apr_%': best['apr_net_%'],
                'best_sharpe': best['sharpe'],
                'best_positive_%': best['positive_%'],
                'second_exchange': second['exchange'],
                'second_apr_%': second['apr_net_%'],
                'spread_apr_%': round(best['apr_net_%'] - second['apr_net_%'], 1),
            })

        return pd.DataFrame(comparisons).sort_values('best_apr_%', ascending=False)

    def print_report(self, stats: pd.DataFrame):
        """Print comprehensive screening report."""
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        print("\n" + "=" * 100)
        print(f"  FUNDING RATE SCREENER — Spot-Perp Arbitrage Opportunities")
        print(f"  {now_str}")
        print("=" * 100)

        # Per-exchange reports
        for exchange in sorted(stats['exchange'].unique()):
            ex_stats = stats[stats['exchange'] == exchange].copy()

            # === Positive funding (Short Perp strategy) ===
            positive = ex_stats[ex_stats['mean_rate'] > 0].sort_values('apr_net_%', ascending=False)

            print(f"\n{'─' * 100}")
            print(f"  {exchange} — TOP POSITIVE FUNDING (Long Spot + Short Perp)")
            print(f"  Strategy: Buy spot, short perp → collect funding when rate > 0")
            print(f"{'─' * 100}")

            if positive.empty:
                print("  No positive funding opportunities found.")
            else:
                top = positive.head(TOP_N)
                print(f"  {'Pair':<14} {'Gross APR':>10} {'Net APR':>10} {'Sharpe':>8} {'Pos%':>6} {'Current':>10} {'Recent':>10} {'MaxDD':>10} {'Pts':>5}")
                print(f"  {'─'*14} {'─'*10} {'─'*10} {'─'*8} {'─'*6} {'─'*10} {'─'*10} {'─'*10} {'─'*5}")
                for _, row in top.iterrows():
                    print(
                        f"  {row['pair']:<14} "
                        f"{row['apr_gross_%']:>9.1f}% "
                        f"{row['apr_net_%']:>9.1f}% "
                        f"{row['sharpe']:>8.2f} "
                        f"{row['positive_%']:>5.1f}% "
                        f"{row['current_apr_%']:>9.1f}% "
                        f"{row['recent_apr_%']:>9.1f}% "
                        f"{row['max_dd_apr_%']:>9.1f}% "
                        f"{row['data_points']:>5}"
                    )

            # === Negative funding (Long Perp strategy) ===
            negative = ex_stats[ex_stats['mean_rate'] < 0].copy()
            negative['abs_apr'] = negative['apr_gross_%'].abs()
            negative = negative.sort_values('abs_apr', ascending=False)

            print(f"\n{'─' * 100}")
            print(f"  {exchange} — TOP NEGATIVE FUNDING (Short Spot + Long Perp)")
            print(f"  Strategy: Short spot (or sell), long perp → collect funding when rate < 0")
            print(f"{'─' * 100}")

            if negative.empty:
                print("  No negative funding opportunities found.")
            else:
                top_neg = negative.head(15)
                print(f"  {'Pair':<14} {'Gross APR':>10} {'Neg%':>6} {'Current':>10} {'Recent':>10} {'Pts':>5}")
                print(f"  {'─'*14} {'─'*10} {'─'*6} {'─'*10} {'─'*10} {'─'*5}")
                for _, row in top_neg.iterrows():
                    print(
                        f"  {row['pair']:<14} "
                        f"{row['apr_gross_%']:>9.1f}% "
                        f"{row['negative_%']:>5.1f}% "
                        f"{row['current_apr_%']:>9.1f}% "
                        f"{row['recent_apr_%']:>9.1f}% "
                        f"{row['data_points']:>5}"
                    )

        # === Cross-exchange comparison ===
        comparison = self.compare_exchanges(stats)
        if not comparison.empty:
            print(f"\n{'─' * 100}")
            print(f"  CROSS-EXCHANGE COMPARISON — Same pair, best exchange")
            print(f"{'─' * 100}")
            top_comp = comparison.head(TOP_N)
            print(f"  {'Pair':<14} {'Best Exch':<20} {'APR':>8} {'Sharpe':>8} {'2nd Exch':<20} {'APR':>8} {'Δ APR':>8}")
            print(f"  {'─'*14} {'─'*20} {'─'*8} {'─'*8} {'─'*20} {'─'*8} {'─'*8}")
            for _, row in top_comp.iterrows():
                print(
                    f"  {row['pair']:<14} "
                    f"{row['best_exchange']:<20} "
                    f"{row['best_apr_%']:>7.1f}% "
                    f"{row['best_sharpe']:>8.2f} "
                    f"{row['second_exchange']:<20} "
                    f"{row['second_apr_%']:>7.1f}% "
                    f"{row['spread_apr_%']:>+7.1f}%"
                )

        # === Summary stats ===
        print(f"\n{'─' * 100}")
        print(f"  SUMMARY")
        print(f"{'─' * 100}")
        for exchange in sorted(stats['exchange'].unique()):
            ex = stats[stats['exchange'] == exchange]
            pos = ex[ex['mean_rate'] > 0]
            neg = ex[ex['mean_rate'] < 0]
            print(f"  {exchange}:")
            print(f"    Total instruments screened: {len(ex)}")
            print(f"    Positive mean funding: {len(pos)} ({len(pos[pos['apr_net_%'] > 5])} above 5% APR)")
            print(f"    Negative mean funding: {len(neg)} ({len(neg[neg['apr_gross_%'] < -5])} below -5% APR)")
            if not pos.empty:
                print(f"    Best opportunity: {pos.iloc[0]['pair']} @ {pos.iloc[0]['apr_net_%']:.1f}% net APR")

        print(f"\n  Cost assumptions: {ROUND_TRIP_COST*100:.2f}% round-trip (spot {SPOT_FEE*100:.2f}% + perp {PERP_FEE*100:.2f}% × 2)")
        print(f"  Note: APR assumes continuous position. Real returns depend on holding period.")
        print(f"\n{'=' * 100}\n")


async def run_screener():
    """Main entry point."""
    db = PostgresClient()
    await db.init_pool()

    screener = FundingRateScreener(db)

    logger.info("Loading all funding rate data...")
    df = await screener.load_all_funding_rates()
    logger.info(f"Loaded {len(df)} records across {df['exchange'].nunique()} exchanges, {df['instrument_id'].nunique()} instruments")

    logger.info("Analyzing...")
    stats = screener.analyze_single_exchange(df)
    logger.info(f"Analyzed {len(stats)} instruments")

    screener.print_report(stats)

    await db.close()


if __name__ == "__main__":
    asyncio.run(run_screener())
