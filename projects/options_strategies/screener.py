"""
Options Opportunity Screener

Screens all options data from the database to identify trading opportunities:
- Highest IV options (premium selling targets)
- Biggest IV-RV spreads (volatility arbitrage)
- Best covered call candidates
- Put-call skew anomalies
- Term structure analysis (backwardation/contango)

Reads from: options_instruments, options_tickers, volatility_surface tables
Does NOT call Deribit API — purely DB-based analysis.

Usage:
    python3 -m projects.options_strategies.screener
    python3 -m projects.options_strategies.screener --underlying BTC --top 30
"""

import asyncio
import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from database.client import PostgresClient
from projects.options_strategies.config import (
    UNDERLYINGS, MIN_DATA_POINTS, MIN_IV, MAX_IV, MIN_OPEN_INTEREST,
    DELTA_ATM_LOW, DELTA_ATM_HIGH, DELTA_OTM_THRESHOLD,
    MIN_DTE, MAX_DTE, IVRV_SPREAD_HIGH, IVRV_SPREAD_LOW,
    PUT_CALL_SKEW_THRESHOLD, TERM_STRUCTURE_INVERSION, TOP_N,
    RV_WINDOW_MEDIUM, CHART_COLORS, SCREENER_OUTPUT_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ScreenerResult:
    """Screening results container."""
    underlying: str
    timestamp: datetime
    
    # Counts
    total_instruments: int = 0
    active_instruments: int = 0
    total_tickers: int = 0
    
    # Top opportunities
    high_iv_options: List[Dict] = field(default_factory=list)
    iv_rv_opportunities: List[Dict] = field(default_factory=list)
    covered_call_candidates: List[Dict] = field(default_factory=list)
    skew_anomalies: List[Dict] = field(default_factory=list)
    term_structure: List[Dict] = field(default_factory=list)
    
    # Summary stats
    avg_iv: float = 0.0
    median_iv: float = 0.0
    iv_percentile_25: float = 0.0
    iv_percentile_75: float = 0.0
    iv_rv_spread_avg: float = 0.0
    put_call_skew_avg: float = 0.0


class OptionsScreener:
    """Screen options database for trading opportunities."""
    
    def __init__(self, db: PostgresClient):
        self.db = db

    # ==================== Data Loading ====================

    async def load_instruments(self, underlying: Optional[str] = None) -> pd.DataFrame:
        """Load options instruments from DB."""
        query = """
            SELECT 
                instrument_id, symbol, underlying, strike, expiry, option_type,
                is_active, contract_size
            FROM options_instruments
            WHERE is_active = TRUE
              AND expiry > NOW()
        """
        params = []
        
        if underlying:
            query += " AND underlying = $1"
            params.append(underlying)
        
        query += " ORDER BY underlying, expiry, strike"
        
        rows = await self.db.read(query, *params)
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)
        df['dte'] = (df['expiry'] - datetime.now(timezone.utc)).dt.total_seconds() / 86400
        return df

    async def load_latest_tickers(self, underlying: Optional[str] = None) -> pd.DataFrame:
        """Load most recent ticker snapshot for each option."""
        query = """
            WITH latest AS (
                SELECT DISTINCT ON (instrument_id)
                    ot.instrument_id,
                    ot.underlying,
                    ot.mark_price,
                    ot.bid_price,
                    ot.ask_price,
                    ot.delta,
                    ot.gamma,
                    ot.theta,
                    ot.vega,
                    ot.iv,
                    ot.volume_24h,
                    ot.open_interest,
                    ot.underlying_price,
                    ot.timestamp
                FROM options_tickers ot
                WHERE ot.iv IS NOT NULL AND ot.iv > 0
                ORDER BY instrument_id, timestamp DESC
            )
            SELECT l.*, 
                   oi.strike, 
                   oi.expiry, 
                   oi.option_type,
                   oi.symbol
            FROM latest l
            JOIN options_instruments oi ON l.instrument_id = oi.instrument_id
            WHERE oi.is_active = TRUE
              AND oi.expiry > NOW()
        """
        params = []
        
        if underlying:
            query = query.replace(
                "WHERE oi.is_active = TRUE",
                "WHERE oi.is_active = TRUE AND l.underlying = $1"
            )
            params.append(underlying)
        
        rows = await self.db.read(query, *params)
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['dte'] = (df['expiry'] - datetime.now(timezone.utc)).dt.total_seconds() / 86400
        
        # Filter by DTE
        df = df[(df['dte'] >= MIN_DTE) & (df['dte'] <= MAX_DTE)]
        
        # Filter by IV bounds
        df = df[(df['iv'] >= MIN_IV) & (df['iv'] <= MAX_IV)]
        
        return df

    async def load_spot_data(self, underlying: str, days: int = 60) -> pd.DataFrame:
        """Load spot price history for RV calculation."""
        # Find a PERP instrument for the underlying
        inst = await self.db.read_one("""
            SELECT instrument_id FROM instruments
            WHERE base_currency = $1 AND type = 'PERP' AND is_active = TRUE
            LIMIT 1
        """, underlying)
        
        if not inst:
            return pd.DataFrame()
        
        start_dt = datetime.now(timezone.utc) - timedelta(days=days)
        
        klines = await self.db.read("""
            SELECT open_time, close
            FROM klines
            WHERE instrument_id = $1
              AND interval IN ('1d', '1D', '1Dutc', '4h', '4H')
              AND open_time >= $2
            ORDER BY open_time
        """, inst['instrument_id'], start_dt)
        
        if not klines:
            return pd.DataFrame()
        
        df = pd.DataFrame(klines)
        df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
        df['close'] = df['close'].astype(float)
        df = df.set_index('open_time').resample('1D').last().dropna().reset_index()
        
        return df

    # ==================== Analysis ====================

    def compute_rv(self, spot_df: pd.DataFrame, window: int = RV_WINDOW_MEDIUM) -> float:
        """Compute annualized realized volatility from daily closes."""
        if len(spot_df) < window:
            return 0.0
        
        returns = np.log(spot_df['close'] / spot_df['close'].shift(1)).dropna()
        rv = returns.tail(window).std() * np.sqrt(365)
        return float(rv) if not np.isnan(rv) else 0.0

    def find_high_iv_options(self, df: pd.DataFrame, top_n: int = TOP_N) -> List[Dict]:
        """Find highest IV options (premium selling targets)."""
        if df.empty:
            return []
        
        # Rank by IV, prefer liquid options
        scored = df.copy()
        scored['liquidity_score'] = (
            scored['open_interest'].fillna(0) / scored['open_interest'].max()
            + scored['volume_24h'].fillna(0) / scored['volume_24h'].max()
        ) / 2
        
        # Combined score: high IV + reasonable liquidity
        scored['score'] = scored['iv'] * (0.7 + 0.3 * scored['liquidity_score'])
        
        top = scored.nlargest(top_n, 'iv')
        
        results = []
        for _, row in top.iterrows():
            results.append({
                'symbol': row['symbol'],
                'underlying': row['underlying'],
                'option_type': row['option_type'],
                'strike': round(row['strike'], 2),
                'dte': round(row['dte'], 0),
                'iv': round(row['iv'], 4),
                'iv_pct': round(row['iv'] * 100, 1),
                'delta': round(row['delta'], 3) if pd.notna(row['delta']) else None,
                'mark_price': round(row['mark_price'], 6) if pd.notna(row['mark_price']) else None,
                'open_interest': round(row['open_interest'], 2) if pd.notna(row['open_interest']) else 0,
                'underlying_price': round(row['underlying_price'], 2) if pd.notna(row['underlying_price']) else None,
            })
        
        return results

    def find_iv_rv_opportunities(
        self, df: pd.DataFrame, rv: float, top_n: int = TOP_N
    ) -> List[Dict]:
        """Find options with largest IV-RV spreads (vol arbitrage targets)."""
        if df.empty or rv <= 0:
            return []
        
        # Focus on ATM options for cleaner IV-RV comparison
        atm = df[df['delta'].abs().between(DELTA_ATM_LOW, DELTA_ATM_HIGH)].copy()
        
        if atm.empty:
            atm = df.copy()
        
        atm['iv_rv_spread'] = atm['iv'] - rv
        atm['iv_rv_spread_pct'] = atm['iv_rv_spread'] * 100
        
        # Split into premium selling (IV > RV) and buying (IV < RV) opportunities
        selling = atm[atm['iv_rv_spread'] > IVRV_SPREAD_HIGH].nlargest(top_n // 2, 'iv_rv_spread')
        buying = atm[atm['iv_rv_spread'] < IVRV_SPREAD_LOW].nsmallest(top_n // 2, 'iv_rv_spread')
        
        combined = pd.concat([selling, buying])
        
        results = []
        for _, row in combined.iterrows():
            results.append({
                'symbol': row['symbol'],
                'underlying': row['underlying'],
                'option_type': row['option_type'],
                'strike': round(row['strike'], 2),
                'dte': round(row['dte'], 0),
                'iv': round(row['iv'], 4),
                'rv': round(rv, 4),
                'iv_rv_spread': round(row['iv_rv_spread'], 4),
                'iv_rv_spread_pct': round(row['iv_rv_spread_pct'], 1),
                'signal': 'SELL_VOL' if row['iv_rv_spread'] > 0 else 'BUY_VOL',
                'delta': round(row['delta'], 3) if pd.notna(row['delta']) else None,
            })
        
        return sorted(results, key=lambda x: abs(x['iv_rv_spread']), reverse=True)

    def find_covered_call_candidates(
        self, df: pd.DataFrame, top_n: int = TOP_N
    ) -> List[Dict]:
        """
        Find best covered call candidates.
        Criteria: High IV + OTM call + reasonable delta (0.15-0.35) + liquid.
        """
        if df.empty:
            return []
        
        # Filter to OTM calls with reasonable delta
        calls = df[
            (df['option_type'] == 'C') &
            (df['delta'].abs() > 0.10) &
            (df['delta'].abs() < 0.40) &
            (df['dte'] >= 7) &
            (df['dte'] <= 45)
        ].copy()
        
        if calls.empty:
            return []
        
        # Score by: high IV, good delta, decent liquidity
        calls['liquidity'] = calls['open_interest'].fillna(0) + calls['volume_24h'].fillna(0)
        calls['score'] = (
            calls['iv'] * 0.5 +
            (1 - calls['delta'].abs()) * 0.3 +  # prefer lower delta
            (calls['liquidity'] / calls['liquidity'].max()) * 0.2
        )
        
        top = calls.nlargest(top_n, 'score')
        
        results = []
        for _, row in top.iterrows():
            # Calculate premium as % of underlying
            premium_pct = (row['mark_price'] / row['underlying_price'] * 100 
                          if pd.notna(row['mark_price']) and pd.notna(row['underlying_price']) 
                          else 0)
            
            # Annualized premium yield
            periods_per_year = 365 / row['dte'] if row['dte'] > 0 else 0
            ann_yield = premium_pct * periods_per_year
            
            results.append({
                'symbol': row['symbol'],
                'underlying': row['underlying'],
                'strike': round(row['strike'], 2),
                'dte': round(row['dte'], 0),
                'delta': round(row['delta'], 3),
                'iv': round(row['iv'], 4),
                'iv_pct': round(row['iv'] * 100, 1),
                'mark_price': round(row['mark_price'], 6) if pd.notna(row['mark_price']) else None,
                'premium_pct': round(premium_pct, 3),
                'ann_yield_pct': round(ann_yield, 1),
                'open_interest': round(row['open_interest'], 2) if pd.notna(row['open_interest']) else 0,
                'underlying_price': round(row['underlying_price'], 2) if pd.notna(row['underlying_price']) else None,
            })
        
        return results

    def analyze_skew(self, df: pd.DataFrame, top_n: int = TOP_N) -> List[Dict]:
        """
        Analyze put-call skew anomalies.
        Compare IV of puts vs calls at same strike/expiry.
        """
        if df.empty:
            return []
        
        # Group by underlying, strike, expiry
        skews = []
        
        for (underlying, strike, expiry), group in df.groupby(['underlying', 'strike', 'expiry']):
            calls = group[group['option_type'] == 'C']
            puts = group[group['option_type'] == 'P']
            
            if calls.empty or puts.empty:
                continue
            
            call_iv = calls['iv'].mean()
            put_iv = puts['iv'].mean()
            skew = put_iv - call_iv
            
            if abs(skew) < PUT_CALL_SKEW_THRESHOLD:
                continue
            
            dte = (expiry - datetime.now(timezone.utc)).total_seconds() / 86400
            
            skews.append({
                'underlying': underlying,
                'strike': round(strike, 2),
                'dte': round(dte, 0),
                'expiry': expiry.strftime('%Y-%m-%d'),
                'call_iv': round(call_iv, 4),
                'put_iv': round(put_iv, 4),
                'skew': round(skew, 4),
                'skew_pct': round(skew * 100, 1),
                'signal': 'PUT_RICH' if skew > 0 else 'CALL_RICH',
            })
        
        # Sort by absolute skew
        skews = sorted(skews, key=lambda x: abs(x['skew']), reverse=True)
        return skews[:top_n]

    def analyze_term_structure(self, df: pd.DataFrame) -> List[Dict]:
        """
        Analyze IV term structure by expiry.
        Detect backwardation (near > far) vs contango (near < far).
        """
        if df.empty:
            return []
        
        results = []
        
        for underlying, group in df.groupby('underlying'):
            # Get ATM options
            spot = group['underlying_price'].median()
            if pd.isna(spot):
                continue
            
            # Find options within 10% of ATM
            atm = group[
                (group['strike'] > spot * 0.9) &
                (group['strike'] < spot * 1.1)
            ].copy()
            
            if atm.empty:
                atm = group
            
            # Group by expiry and compute average IV
            term = atm.groupby('dte').agg({
                'iv': 'mean',
                'expiry': 'first',
            }).reset_index()
            
            term = term.sort_values('dte')
            
            if len(term) < 2:
                continue
            
            # Compute term structure slope
            near_iv = term.iloc[0]['iv']
            far_iv = term.iloc[-1]['iv']
            slope = (far_iv - near_iv) / (term.iloc[-1]['dte'] - term.iloc[0]['dte'])
            
            is_backwardation = near_iv > far_iv + abs(TERM_STRUCTURE_INVERSION)
            is_contango = far_iv > near_iv + abs(TERM_STRUCTURE_INVERSION)
            
            for _, row in term.iterrows():
                results.append({
                    'underlying': underlying,
                    'dte': round(row['dte'], 0),
                    'expiry': row['expiry'].strftime('%Y-%m-%d') if pd.notna(row['expiry']) else None,
                    'avg_iv': round(row['iv'], 4),
                    'iv_pct': round(row['iv'] * 100, 1),
                })
            
            # Add summary
            if is_backwardation or is_contango:
                logger.info(f"{underlying} term structure: {'BACKWARDATION' if is_backwardation else 'CONTANGO'}")
        
        return results

    # ==================== Main Screen ====================

    async def screen(
        self,
        underlying: Optional[str] = None,
        top_n: int = TOP_N,
    ) -> Dict[str, ScreenerResult]:
        """Run full screening for specified underlying(s)."""
        underlyings = [underlying] if underlying else UNDERLYINGS
        results = {}
        
        for ul in underlyings:
            logger.info(f"Screening {ul} options...")
            
            result = ScreenerResult(
                underlying=ul,
                timestamp=datetime.now(timezone.utc),
            )
            
            # Load data
            instruments = await self.load_instruments(ul)
            tickers = await self.load_latest_tickers(ul)
            spot_df = await self.load_spot_data(ul)
            
            result.total_instruments = len(instruments)
            result.active_instruments = len(instruments[instruments['is_active']]) if not instruments.empty else 0
            result.total_tickers = len(tickers)
            
            if tickers.empty:
                logger.warning(f"No ticker data for {ul}. Run options_data_job first.")
                results[ul] = result
                continue
            
            # Compute RV
            rv = self.compute_rv(spot_df) if not spot_df.empty else 0.0
            
            # Summary stats
            result.avg_iv = float(tickers['iv'].mean())
            result.median_iv = float(tickers['iv'].median())
            result.iv_percentile_25 = float(tickers['iv'].quantile(0.25))
            result.iv_percentile_75 = float(tickers['iv'].quantile(0.75))
            
            if rv > 0:
                iv_rv_spreads = tickers['iv'] - rv
                result.iv_rv_spread_avg = float(iv_rv_spreads.mean())
            
            # Run analyses
            result.high_iv_options = self.find_high_iv_options(tickers, top_n)
            result.iv_rv_opportunities = self.find_iv_rv_opportunities(tickers, rv, top_n)
            result.covered_call_candidates = self.find_covered_call_candidates(tickers, top_n)
            result.skew_anomalies = self.analyze_skew(tickers, top_n)
            result.term_structure = self.analyze_term_structure(tickers)
            
            results[ul] = result
            logger.info(f"{ul}: {len(result.high_iv_options)} high IV, {len(result.iv_rv_opportunities)} IV-RV opps, {len(result.covered_call_candidates)} CC candidates")
        
        return results

    # ==================== Report ====================

    def print_report(self, results: Dict[str, ScreenerResult]):
        """Print comprehensive screening report."""
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        
        print(f"\n{'='*100}")
        print(f"  OPTIONS OPPORTUNITY SCREENER")
        print(f"  {now_str}")
        print(f"{'='*100}")
        
        for ul, r in results.items():
            print(f"\n{'─'*100}")
            print(f"  {ul} — Summary")
            print(f"{'─'*100}")
            print(f"  Instruments: {r.total_instruments} total, {r.active_instruments} active")
            print(f"  Tickers: {r.total_tickers} with valid IV data")
            print(f"  Average IV: {r.avg_iv*100:.1f}% | Median IV: {r.median_iv*100:.1f}%")
            print(f"  IV Range (25-75%): {r.iv_percentile_25*100:.1f}% - {r.iv_percentile_75*100:.1f}%")
            print(f"  Avg IV-RV Spread: {r.iv_rv_spread_avg*100:+.1f}pp")
            
            # High IV Options
            if r.high_iv_options:
                print(f"\n{'─'*100}")
                print(f"  {ul} — TOP HIGH IV OPTIONS (Premium Selling Targets)")
                print(f"{'─'*100}")
                print(f"  {'Symbol':<30} {'Type':>4} {'Strike':>10} {'DTE':>5} {'IV':>8} {'Delta':>7} {'OI':>10}")
                print(f"  {'─'*30} {'─'*4} {'─'*10} {'─'*5} {'─'*8} {'─'*7} {'─'*10}")
                for opt in r.high_iv_options[:15]:
                    delta_str = f"{opt['delta']:.2f}" if opt['delta'] else "—"
                    print(
                        f"  {opt['symbol']:<30} "
                        f"{opt['option_type']:>4} "
                        f"${opt['strike']:>9,.0f} "
                        f"{opt['dte']:>5.0f} "
                        f"{opt['iv_pct']:>7.1f}% "
                        f"{delta_str:>7} "
                        f"{opt['open_interest']:>10.0f}"
                    )
            
            # IV-RV Opportunities
            if r.iv_rv_opportunities:
                print(f"\n{'─'*100}")
                print(f"  {ul} — IV-RV SPREAD OPPORTUNITIES (Vol Arbitrage)")
                print(f"{'─'*100}")
                print(f"  {'Symbol':<30} {'Type':>4} {'DTE':>5} {'IV':>8} {'RV':>8} {'Spread':>8} {'Signal':>10}")
                print(f"  {'─'*30} {'─'*4} {'─'*5} {'─'*8} {'─'*8} {'─'*8} {'─'*10}")
                for opt in r.iv_rv_opportunities[:15]:
                    print(
                        f"  {opt['symbol']:<30} "
                        f"{opt['option_type']:>4} "
                        f"{opt['dte']:>5.0f} "
                        f"{opt['iv']*100:>7.1f}% "
                        f"{opt['rv']*100:>7.1f}% "
                        f"{opt['iv_rv_spread_pct']:>+7.1f}pp "
                        f"{opt['signal']:>10}"
                    )
            
            # Covered Call Candidates
            if r.covered_call_candidates:
                print(f"\n{'─'*100}")
                print(f"  {ul} — COVERED CALL CANDIDATES")
                print(f"{'─'*100}")
                print(f"  {'Symbol':<30} {'Strike':>10} {'DTE':>5} {'Delta':>7} {'IV':>8} {'Prem%':>7} {'Ann%':>8}")
                print(f"  {'─'*30} {'─'*10} {'─'*5} {'─'*7} {'─'*8} {'─'*7} {'─'*8}")
                for opt in r.covered_call_candidates[:15]:
                    print(
                        f"  {opt['symbol']:<30} "
                        f"${opt['strike']:>9,.0f} "
                        f"{opt['dte']:>5.0f} "
                        f"{opt['delta']:>7.2f} "
                        f"{opt['iv_pct']:>7.1f}% "
                        f"{opt['premium_pct']:>6.2f}% "
                        f"{opt['ann_yield_pct']:>7.1f}%"
                    )
            
            # Skew Anomalies
            if r.skew_anomalies:
                print(f"\n{'─'*100}")
                print(f"  {ul} — PUT-CALL SKEW ANOMALIES")
                print(f"{'─'*100}")
                print(f"  {'Strike':>10} {'DTE':>5} {'Expiry':>12} {'Put IV':>8} {'Call IV':>8} {'Skew':>8} {'Signal':>10}")
                print(f"  {'─'*10} {'─'*5} {'─'*12} {'─'*8} {'─'*8} {'─'*8} {'─'*10}")
                for s in r.skew_anomalies[:10]:
                    print(
                        f"  ${s['strike']:>9,.0f} "
                        f"{s['dte']:>5.0f} "
                        f"{s['expiry']:>12} "
                        f"{s['put_iv']*100:>7.1f}% "
                        f"{s['call_iv']*100:>7.1f}% "
                        f"{s['skew_pct']:>+7.1f}pp "
                        f"{s['signal']:>10}"
                    )
            
            # Term Structure
            if r.term_structure:
                print(f"\n{'─'*100}")
                print(f"  {ul} — IV TERM STRUCTURE (ATM)")
                print(f"{'─'*100}")
                print(f"  {'DTE':>8} {'Expiry':>12} {'IV':>10}")
                print(f"  {'─'*8} {'─'*12} {'─'*10}")
                for t in r.term_structure[:10]:
                    print(f"  {t['dte']:>8.0f} {t['expiry'] or '—':>12} {t['iv_pct']:>9.1f}%")
        
        print(f"\n{'='*100}")
        print(f"  💡 Recommendations:")
        print(f"  • High IV + High OI → Good premium selling candidates")
        print(f"  • IV > RV by 10pp+ → Sell vol (strangle/condor)")
        print(f"  • IV < RV by 5pp+ → Buy vol (straddle)")
        print(f"  • Put-rich skew → Sell puts, buy calls for hedge")
        print(f"{'='*100}\n")

    def save_results(
        self,
        results: Dict[str, ScreenerResult],
        output_dir: str,
    ):
        """Save screening results to files."""
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')
        
        for ul, r in results.items():
            # Save JSON summary
            json_path = os.path.join(output_dir, f"screener_{ul}_{ts}.json")
            with open(json_path, 'w') as f:
                data = asdict(r)
                data['timestamp'] = r.timestamp.isoformat()
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Saved: {json_path}")
            
            # Save CSV for each category
            if r.high_iv_options:
                df = pd.DataFrame(r.high_iv_options)
                csv_path = os.path.join(output_dir, f"high_iv_{ul}_{ts}.csv")
                df.to_csv(csv_path, index=False)
            
            if r.iv_rv_opportunities:
                df = pd.DataFrame(r.iv_rv_opportunities)
                csv_path = os.path.join(output_dir, f"iv_rv_{ul}_{ts}.csv")
                df.to_csv(csv_path, index=False)
            
            if r.covered_call_candidates:
                df = pd.DataFrame(r.covered_call_candidates)
                csv_path = os.path.join(output_dir, f"covered_calls_{ul}_{ts}.csv")
                df.to_csv(csv_path, index=False)


# ==================== CLI Entry ====================

async def run_screener(
    underlying: Optional[str] = None,
    top_n: int = TOP_N,
    save: bool = True,
):
    """Main screener entry point."""
    try:
        db = PostgresClient()
        await db.init_pool()
    except ValueError as e:
        print("\n" + "="*80)
        print("  ⚠️  DATABASE CONNECTION ERROR")
        print("="*80)
        print(f"  {e}")
        print("\n  Please set DATABASE_URL environment variable:")
        print("    export DATABASE_URL='postgresql://user:pass@host:5432/db'")
        print("="*80 + "\n")
        return {}
    
    screener = OptionsScreener(db)
    
    # Check if we have any data
    check = await db.read_one("SELECT COUNT(*) as cnt FROM options_tickers")
    if not check or check.get('cnt', 0) == 0:
        print("\n" + "="*80)
        print("  ⚠️  NO OPTIONS DATA IN DATABASE")
        print("="*80)
        print("  The options_tickers table is empty.")
        print("  Please run the data fetch script first:")
        print("")
        print("    python3 scripts/fetch_deribit_options.py --currency BTC ETH")
        print("")
        print("  Or run the pipeline job:")
        print("")
        print("    python3 -m pipeline.jobs.options_data_job --currency BTC ETH")
        print("="*80 + "\n")
        await db.close()
        return {}
    
    results = await screener.screen(underlying=underlying, top_n=top_n)
    screener.print_report(results)
    
    if save:
        output_dir = os.path.join(os.path.dirname(__file__), SCREENER_OUTPUT_DIR)
        screener.save_results(results, output_dir)
    
    await db.close()
    return results


def main():
    parser = argparse.ArgumentParser(description="Options Opportunity Screener")
    parser.add_argument("--underlying", "-u", type=str, default=None, help="BTC or ETH (default: both)")
    parser.add_argument("--top", "-n", type=int, default=TOP_N, help="Top N results per category")
    parser.add_argument("--no-save", action="store_true", help="Don't save results to files")
    args = parser.parse_args()
    
    asyncio.run(run_screener(
        underlying=args.underlying,
        top_n=args.top,
        save=not args.no_save,
    ))


if __name__ == "__main__":
    main()
