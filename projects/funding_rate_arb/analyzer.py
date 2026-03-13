"""
Funding Rate Arbitrage Analyzer

Cross-exchange funding rate spread analysis between OKX and Binance.
Identifies delta-neutral arbitrage opportunities based on funding rate divergence.

Usage:
    python3 -m projects.funding_rate_arb.analyzer
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from database.client import PostgresClient
from projects.funding_rate_arb.config import (
    EXCHANGE_A, EXCHANGE_B, EXCHANGE_A_ID, EXCHANGE_B_ID,
    PREFIX_A, PREFIX_B, SETTLEMENTS_PER_DAY,
    MIN_SPREAD_ABS, MIN_DATA_POINTS,
    FEE_RATE_A, FEE_RATE_B, TOTAL_COST,
    ANNUALIZATION_FACTOR, TOP_N,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FundingRateArbAnalyzer:
    """Analyze funding rate arbitrage opportunities across exchanges."""

    def __init__(self, db: PostgresClient):
        self.db = db

    # ==================== Data Loading ====================

    async def get_overlapping_pairs(self) -> List[Dict[str, Any]]:
        """Find instruments that exist on both exchanges."""
        return await self.db.read("""
            SELECT i1.base_currency, i1.quote_currency,
                   i1.instrument_id AS id_a, i1.symbol AS symbol_a,
                   i2.instrument_id AS id_b, i2.symbol AS symbol_b
            FROM instruments i1
            JOIN instruments i2
              ON i1.base_currency = i2.base_currency
             AND i1.quote_currency = i2.quote_currency
            WHERE i1.exchange_id = $1 AND i2.exchange_id = $2
              AND i1.type = 'PERP' AND i2.type = 'PERP'
              AND i1.is_active AND i2.is_active
            ORDER BY i1.base_currency
        """, EXCHANGE_A_ID, EXCHANGE_B_ID)

    async def load_funding_rates(
        self,
        instrument_ids: List[str],
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Load funding rates for given instruments into a DataFrame."""
        query = """
            SELECT instrument_id, funding_rate, funding_time
            FROM funding_rates
            WHERE instrument_id = ANY($1)
        """
        params: list = [instrument_ids]

        if start:
            query += " AND funding_time >= $2"
            params.append(start)
        if end:
            idx = len(params) + 1
            query += f" AND funding_time <= ${idx}"
            params.append(end)

        query += " ORDER BY funding_time"

        rows = await self.db.read(query, *params)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df['funding_time'] = pd.to_datetime(df['funding_time'], utc=True)
        return df

    # ==================== Spread Calculation ====================

    def compute_spreads(
        self, df: pd.DataFrame, pairs: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Compute funding rate spreads for each overlapping pair.

        Spread = funding_rate_A - funding_rate_B
        Positive spread → short A, long B (collect spread)
        Negative spread → long A, short B (collect |spread|)
        """
        results = []

        for pair in pairs:
            id_a = pair['id_a']
            id_b = pair['id_b']
            base = pair['base_currency']
            quote = pair['quote_currency']

            df_a = df[df['instrument_id'] == id_a][['funding_time', 'funding_rate']].copy()
            df_b = df[df['instrument_id'] == id_b][['funding_time', 'funding_rate']].copy()

            if df_a.empty or df_b.empty:
                continue

            # Round funding_time to nearest hour for alignment
            df_a['funding_time'] = df_a['funding_time'].dt.round('h')
            df_b['funding_time'] = df_b['funding_time'].dt.round('h')

            # Merge on funding_time
            merged = pd.merge(
                df_a, df_b,
                on='funding_time',
                suffixes=('_a', '_b'),
                how='inner',
            )

            if len(merged) < MIN_DATA_POINTS:
                continue

            merged['spread'] = merged['funding_rate_a'] - merged['funding_rate_b']
            merged['abs_spread'] = merged['spread'].abs()
            merged['pair'] = f"{base}/{quote}"
            merged['id_a'] = id_a
            merged['id_b'] = id_b

            results.append(merged)

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    # ==================== Analysis ====================

    def analyze_spreads(self, spreads_df: pd.DataFrame) -> pd.DataFrame:
        """
        Per-pair spread statistics.

        Returns DataFrame with columns:
        - pair, mean_spread, median_spread, std_spread
        - mean_abs_spread, max_abs_spread
        - apr_mean (annualized mean abs spread)
        - apr_net (after costs)
        - sharpe (spread mean / spread std, annualized)
        - opportunity_rate (% of times abs_spread > MIN_SPREAD_ABS)
        - data_points, direction (which side to take on average)
        """
        stats = []

        for pair_name, group in spreads_df.groupby('pair'):
            spread = group['spread']
            abs_spread = group['abs_spread']
            n = len(spread)

            mean_spread = spread.mean()
            mean_abs = abs_spread.mean()
            std_spread = spread.std()

            # APR from mean absolute spread
            apr_mean = mean_abs * ANNUALIZATION_FACTOR * 100
            apr_net = max(0, (mean_abs - TOTAL_COST / ANNUALIZATION_FACTOR) * ANNUALIZATION_FACTOR * 100)

            # Sharpe: annualized
            sharpe = (mean_abs / std_spread * np.sqrt(ANNUALIZATION_FACTOR)) if std_spread > 0 else 0

            # Opportunity rate: how often is spread above threshold
            opp_rate = (abs_spread >= MIN_SPREAD_ABS).mean() * 100

            # Direction: if mean_spread > 0, A pays more → short A long B
            if mean_spread > 0:
                direction = f"Short {EXCHANGE_A} / Long {EXCHANGE_B}"
            else:
                direction = f"Long {EXCHANGE_A} / Short {EXCHANGE_B}"

            stats.append({
                'pair': pair_name,
                'mean_spread': mean_spread,
                'median_spread': spread.median(),
                'std_spread': std_spread,
                'mean_abs_spread': mean_abs,
                'max_abs_spread': abs_spread.max(),
                'apr_mean_%': round(apr_mean, 2),
                'apr_net_%': round(apr_net, 2),
                'sharpe': round(sharpe, 2),
                'opp_rate_%': round(opp_rate, 1),
                'data_points': n,
                'direction': direction,
                'id_a': group['id_a'].iloc[0],
                'id_b': group['id_b'].iloc[0],
            })

        result = pd.DataFrame(stats)
        if not result.empty:
            result = result.sort_values('apr_net_%', ascending=False).reset_index(drop=True)

        return result

    def current_opportunities(self, spreads_df: pd.DataFrame) -> pd.DataFrame:
        """
        Find current live opportunities: latest funding rate spread per pair.
        """
        latest = spreads_df.sort_values('funding_time').groupby('pair').last().reset_index()
        latest['apr_%'] = (latest['abs_spread'] * ANNUALIZATION_FACTOR * 100).round(2)

        # Direction
        latest['direction'] = latest['spread'].apply(
            lambda s: f"Short {EXCHANGE_A} / Long {EXCHANGE_B}" if s > 0
            else f"Long {EXCHANGE_A} / Short {EXCHANGE_B}"
        )

        return latest[latest['abs_spread'] >= MIN_SPREAD_ABS].sort_values(
            'abs_spread', ascending=False
        ).reset_index(drop=True)

    # ==================== Report ====================

    def print_report(
        self,
        stats: pd.DataFrame,
        opportunities: pd.DataFrame,
        spreads_df: pd.DataFrame,
    ):
        """Print analysis report to stdout."""
        print("\n" + "=" * 90)
        print(f"  FUNDING RATE ARBITRAGE REPORT: {EXCHANGE_A} vs {EXCHANGE_B}")
        print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print("=" * 90)

        # Overview
        n_pairs = stats['pair'].nunique()
        total_points = spreads_df.shape[0]
        date_range = f"{spreads_df['funding_time'].min()} → {spreads_df['funding_time'].max()}"
        print(f"\n  Pairs analyzed: {n_pairs}")
        print(f"  Total data points: {total_points:,}")
        print(f"  Date range: {date_range}")
        print(f"  Cost assumption: {TOTAL_COST*100:.2f}% round-trip (fees + slippage)")

        # Top pairs by net APR
        print(f"\n{'─' * 90}")
        print(f"  TOP {TOP_N} PAIRS BY NET APR (after costs)")
        print(f"{'─' * 90}")
        top = stats.head(TOP_N)
        print(f"  {'Pair':<12} {'APR(Gross)':>10} {'APR(Net)':>10} {'Sharpe':>8} {'Opp%':>6} {'Points':>7}  Direction")
        print(f"  {'─'*12} {'─'*10} {'─'*10} {'─'*8} {'─'*6} {'─'*7}  {'─'*30}")
        for _, row in top.iterrows():
            print(
                f"  {row['pair']:<12} "
                f"{row['apr_mean_%']:>9.1f}% "
                f"{row['apr_net_%']:>9.1f}% "
                f"{row['sharpe']:>8.2f} "
                f"{row['opp_rate_%']:>5.1f}% "
                f"{row['data_points']:>7} "
                f" {row['direction']}"
            )

        # Current live opportunities
        print(f"\n{'─' * 90}")
        print(f"  CURRENT LIVE OPPORTUNITIES (latest funding rate spread)")
        print(f"{'─' * 90}")
        if opportunities.empty:
            print("  No opportunities above threshold right now.")
        else:
            live = opportunities.head(TOP_N)
            print(f"  {'Pair':<12} {'Spread':>12} {'APR':>10} {'Rate A':>12} {'Rate B':>12}  Direction")
            print(f"  {'─'*12} {'─'*12} {'─'*10} {'─'*12} {'─'*12}  {'─'*30}")
            for _, row in live.iterrows():
                print(
                    f"  {row['pair']:<12} "
                    f"{row['spread']:>+12.8f} "
                    f"{row['apr_%']:>9.1f}% "
                    f"{row['funding_rate_a']:>+12.8f} "
                    f"{row['funding_rate_b']:>+12.8f} "
                    f" {row['direction']}"
                )

        # Negative APR pairs (avoid)
        negative = stats[stats['apr_net_%'] <= 0]
        if not negative.empty:
            print(f"\n  ⚠️  {len(negative)} pairs have negative net APR (costs > spread)")

        print(f"\n{'=' * 90}\n")


async def run_analysis():
    """Main entry point."""
    db = PostgresClient()
    await db.init_pool()

    analyzer = FundingRateArbAnalyzer(db)

    # 1. Find overlapping pairs
    logger.info("Finding overlapping pairs...")
    pairs = await analyzer.get_overlapping_pairs()
    logger.info(f"Found {len(pairs)} overlapping PERP pairs")

    if not pairs:
        logger.error("No overlapping pairs found. Run instrument jobs first.")
        await db.close()
        return

    # 2. Load funding rates
    all_ids = [p['id_a'] for p in pairs] + [p['id_b'] for p in pairs]
    logger.info("Loading funding rate data...")
    df = await analyzer.load_funding_rates(all_ids)
    logger.info(f"Loaded {len(df)} funding rate records")

    if df.empty:
        logger.error("No funding rate data. Run funding_rate jobs first.")
        await db.close()
        return

    # 3. Compute spreads
    logger.info("Computing spreads...")
    spreads_df = analyzer.compute_spreads(df, pairs)
    logger.info(f"Computed {len(spreads_df)} spread data points across {spreads_df['pair'].nunique()} pairs")

    # 4. Analyze
    logger.info("Analyzing...")
    stats = analyzer.analyze_spreads(spreads_df)
    opportunities = analyzer.current_opportunities(spreads_df)

    # 5. Report
    analyzer.print_report(stats, opportunities, spreads_df)

    await db.close()


if __name__ == "__main__":
    asyncio.run(run_analysis())
