"""
Volatility Surface Analyzer

Deep analysis of the options volatility surface:
- ATM IV vs realized volatility comparison
- IV term structure (near vs far expiry)
- Smile/skew analysis (IV by moneyness)
- Greeks heatmap data
- Historical IV percentile ranking

Generates charts and formatted reports.
Reads from: options_tickers, volatility_surface, klines tables.

Usage:
    python3 -m projects.options_strategies.analyzer
    python3 -m projects.options_strategies.analyzer --underlying BTC --days 30
"""

import asyncio
import argparse
import json
import logging
import math
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
    UNDERLYINGS, MIN_IV, MAX_IV, MIN_DTE, MAX_DTE,
    RV_WINDOW_SHORT, RV_WINDOW_MEDIUM, RV_WINDOW_LONG,
    MONEYNESS_BUCKETS, DTE_BUCKETS, DTE_LABELS,
    CHART_COLORS, ANALYZER_OUTPUT_DIR, CHART_DPI,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class VolAnalysisResult:
    """Volatility analysis results container."""
    underlying: str
    timestamp: datetime
    spot_price: float = 0.0
    
    # ATM IV
    atm_iv: float = 0.0
    atm_iv_percentile: float = 0.0  # Where current ATM IV ranks historically
    
    # Realized Vol
    rv_14d: float = 0.0
    rv_30d: float = 0.0
    rv_60d: float = 0.0
    
    # IV-RV Spread
    iv_rv_spread: float = 0.0
    
    # Term Structure
    term_structure: List[Dict] = field(default_factory=list)
    term_slope: float = 0.0  # +ve = contango, -ve = backwardation
    
    # Smile/Skew
    smile: List[Dict] = field(default_factory=list)
    put_call_skew: float = 0.0  # Put IV - Call IV at ATM
    skew_25d: float = 0.0  # 25D put IV - 25D call IV
    
    # Greeks Distribution
    total_delta: float = 0.0
    total_gamma: float = 0.0
    total_vega: float = 0.0
    
    # Data info
    data_points: int = 0
    date_range_start: Optional[datetime] = None
    date_range_end: Optional[datetime] = None


class VolatilityAnalyzer:
    """Analyze options volatility surface."""
    
    def __init__(self, db: PostgresClient):
        self.db = db

    # ==================== Data Loading ====================

    async def load_latest_tickers(self, underlying: str) -> pd.DataFrame:
        """Load most recent ticker data for analysis."""
        rows = await self.db.read("""
            WITH latest AS (
                SELECT DISTINCT ON (instrument_id)
                    ot.instrument_id,
                    ot.underlying,
                    ot.mark_price,
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
                WHERE ot.underlying = $1
                  AND ot.iv IS NOT NULL AND ot.iv > $2 AND ot.iv < $3
                ORDER BY instrument_id, timestamp DESC
            )
            SELECT l.*, oi.strike, oi.expiry, oi.option_type, oi.symbol
            FROM latest l
            JOIN options_instruments oi ON l.instrument_id = oi.instrument_id
            WHERE oi.is_active = TRUE AND oi.expiry > NOW()
        """, underlying, MIN_IV, MAX_IV)
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['dte'] = (df['expiry'] - datetime.now(timezone.utc)).dt.total_seconds() / 86400
        df = df[(df['dte'] >= MIN_DTE) & (df['dte'] <= MAX_DTE)]
        
        return df

    async def load_historical_iv(self, underlying: str, days: int = 90) -> pd.DataFrame:
        """Load historical ATM IV for percentile calculation."""
        start_dt = datetime.now(timezone.utc) - timedelta(days=days)
        
        rows = await self.db.read("""
            SELECT
                DATE_TRUNC('day', timestamp) AS date,
                AVG(iv) FILTER (WHERE ABS(delta) BETWEEN 0.4 AND 0.6) AS atm_iv,
                AVG(iv) AS avg_iv,
                COUNT(*) AS cnt
            FROM options_tickers
            WHERE underlying = $1
              AND timestamp >= $2
              AND iv IS NOT NULL AND iv > 0
            GROUP BY DATE_TRUNC('day', timestamp)
            ORDER BY date
        """, underlying, start_dt)
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df['iv'] = df['atm_iv'].fillna(df['avg_iv'])
        return df

    async def load_spot_data(self, underlying: str, days: int = 90) -> pd.DataFrame:
        """Load spot price history for RV calculation."""
        inst = await self.db.read_one("""
            SELECT instrument_id FROM instruments
            WHERE base_currency = $1 AND type = 'PERP' AND is_active = TRUE
            LIMIT 1
        """, underlying)
        
        if not inst:
            return pd.DataFrame()
        
        start_dt = datetime.now(timezone.utc) - timedelta(days=days)
        
        klines = await self.db.read("""
            SELECT open_time, open, high, low, close
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
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col].astype(float)
        df = df.set_index('open_time').resample('1D').last().dropna().reset_index()
        
        return df

    async def load_volatility_surface(self, underlying: str) -> pd.DataFrame:
        """Load volatility surface data if available."""
        rows = await self.db.read("""
            SELECT DISTINCT ON (expiry, strike, option_type)
                underlying, expiry, strike, option_type, iv, delta, underlying_price, timestamp
            FROM volatility_surface
            WHERE underlying = $1
            ORDER BY expiry, strike, option_type, timestamp DESC
        """, underlying)
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)
        df['dte'] = (df['expiry'] - datetime.now(timezone.utc)).dt.total_seconds() / 86400
        return df

    # ==================== Analysis ====================

    def compute_rv(self, spot_df: pd.DataFrame) -> Dict[str, float]:
        """Compute realized volatility for multiple windows."""
        if spot_df.empty:
            return {'rv_14d': 0.0, 'rv_30d': 0.0, 'rv_60d': 0.0}
        
        returns = np.log(spot_df['close'] / spot_df['close'].shift(1)).dropna()
        
        rv_14d = returns.tail(RV_WINDOW_SHORT).std() * np.sqrt(365) if len(returns) >= RV_WINDOW_SHORT else 0
        rv_30d = returns.tail(RV_WINDOW_MEDIUM).std() * np.sqrt(365) if len(returns) >= RV_WINDOW_MEDIUM else 0
        rv_60d = returns.tail(RV_WINDOW_LONG).std() * np.sqrt(365) if len(returns) >= RV_WINDOW_LONG else 0
        
        return {
            'rv_14d': float(rv_14d) if not np.isnan(rv_14d) else 0.0,
            'rv_30d': float(rv_30d) if not np.isnan(rv_30d) else 0.0,
            'rv_60d': float(rv_60d) if not np.isnan(rv_60d) else 0.0,
        }

    def compute_atm_iv(self, df: pd.DataFrame, spot: float) -> float:
        """Compute ATM IV from ticker data."""
        if df.empty:
            return 0.0
        
        # Find options closest to ATM
        atm_range = df[
            (df['strike'] > spot * 0.95) &
            (df['strike'] < spot * 1.05) &
            (df['dte'] > 7) &
            (df['dte'] < 45)
        ]
        
        if atm_range.empty:
            atm_range = df[df['delta'].abs().between(0.4, 0.6)]
        
        if atm_range.empty:
            return float(df['iv'].median())
        
        return float(atm_range['iv'].mean())

    def compute_iv_percentile(self, current_iv: float, historical_df: pd.DataFrame) -> float:
        """Compute where current IV ranks in historical distribution."""
        if historical_df.empty or current_iv <= 0:
            return 50.0
        
        ivs = historical_df['iv'].dropna().values
        if len(ivs) == 0:
            return 50.0
        
        percentile = (ivs < current_iv).sum() / len(ivs) * 100
        return float(percentile)

    def analyze_term_structure(self, df: pd.DataFrame, spot: float) -> Tuple[List[Dict], float]:
        """Analyze IV term structure across expiries."""
        if df.empty:
            return [], 0.0
        
        # Focus on ATM options
        atm = df[
            (df['strike'] > spot * 0.9) &
            (df['strike'] < spot * 1.1)
        ].copy()
        
        if atm.empty:
            atm = df.copy()
        
        # Group by DTE bucket and compute average IV
        term_data = []
        
        for i, dte_max in enumerate(DTE_BUCKETS):
            dte_min = DTE_BUCKETS[i-1] if i > 0 else 0
            bucket = atm[(atm['dte'] > dte_min) & (atm['dte'] <= dte_max)]
            
            if bucket.empty:
                continue
            
            avg_iv = bucket['iv'].mean()
            call_iv = bucket[bucket['option_type'] == 'C']['iv'].mean() if not bucket[bucket['option_type'] == 'C'].empty else avg_iv
            put_iv = bucket[bucket['option_type'] == 'P']['iv'].mean() if not bucket[bucket['option_type'] == 'P'].empty else avg_iv
            
            term_data.append({
                'dte_bucket': DTE_LABELS[i],
                'dte_avg': float(bucket['dte'].mean()),
                'avg_iv': round(float(avg_iv), 4),
                'call_iv': round(float(call_iv), 4),
                'put_iv': round(float(put_iv), 4),
                'put_call_skew': round(float(put_iv - call_iv), 4),
                'count': len(bucket),
            })
        
        # Compute slope
        if len(term_data) >= 2:
            near_iv = term_data[0]['avg_iv']
            far_iv = term_data[-1]['avg_iv']
            slope = (far_iv - near_iv) / (term_data[-1]['dte_avg'] - term_data[0]['dte_avg'])
        else:
            slope = 0.0
        
        return term_data, float(slope)

    def analyze_smile(self, df: pd.DataFrame, spot: float) -> Tuple[List[Dict], float, float]:
        """Analyze volatility smile/skew by moneyness."""
        if df.empty:
            return [], 0.0, 0.0
        
        # Add moneyness column
        df = df.copy()
        df['moneyness'] = df['strike'] / spot
        
        # Focus on near-term options (14-45 DTE) for cleaner smile
        near_term = df[(df['dte'] >= 14) & (df['dte'] <= 45)]
        if near_term.empty:
            near_term = df
        
        smile_data = []
        
        for i, m_max in enumerate(MONEYNESS_BUCKETS):
            m_min = MONEYNESS_BUCKETS[i-1] if i > 0 else 0.5
            bucket = near_term[(near_term['moneyness'] > m_min) & (near_term['moneyness'] <= m_max)]
            
            if bucket.empty:
                continue
            
            avg_iv = bucket['iv'].mean()
            call_iv = bucket[bucket['option_type'] == 'C']['iv'].mean()
            put_iv = bucket[bucket['option_type'] == 'P']['iv'].mean()
            
            smile_data.append({
                'moneyness': f"{m_max:.0%}",
                'moneyness_val': float(m_max),
                'avg_iv': round(float(avg_iv), 4) if not np.isnan(avg_iv) else 0,
                'call_iv': round(float(call_iv), 4) if not np.isnan(call_iv) else 0,
                'put_iv': round(float(put_iv), 4) if not np.isnan(put_iv) else 0,
                'count': len(bucket),
            })
        
        # Compute skew metrics
        put_call_skew = 0.0
        skew_25d = 0.0
        
        # ATM put-call skew
        atm = near_term[(near_term['moneyness'] > 0.97) & (near_term['moneyness'] < 1.03)]
        if not atm.empty:
            atm_calls = atm[atm['option_type'] == 'C']['iv'].mean()
            atm_puts = atm[atm['option_type'] == 'P']['iv'].mean()
            if not np.isnan(atm_calls) and not np.isnan(atm_puts):
                put_call_skew = atm_puts - atm_calls
        
        # 25-delta skew (common measure)
        calls_25d = near_term[(near_term['option_type'] == 'C') & (near_term['delta'].abs().between(0.2, 0.3))]
        puts_25d = near_term[(near_term['option_type'] == 'P') & (near_term['delta'].abs().between(0.2, 0.3))]
        if not calls_25d.empty and not puts_25d.empty:
            skew_25d = puts_25d['iv'].mean() - calls_25d['iv'].mean()
        
        return smile_data, float(put_call_skew), float(skew_25d) if not np.isnan(skew_25d) else 0.0

    def compute_greeks_summary(self, df: pd.DataFrame) -> Dict[str, float]:
        """Compute aggregate Greeks summary."""
        if df.empty:
            return {'total_delta': 0.0, 'total_gamma': 0.0, 'total_vega': 0.0}
        
        # Weight by open interest
        oi = df['open_interest'].fillna(0)
        
        total_delta = (df['delta'].fillna(0) * oi).sum()
        total_gamma = (df['gamma'].fillna(0) * oi).sum()
        total_vega = (df['vega'].fillna(0) * oi).sum()
        
        return {
            'total_delta': float(total_delta),
            'total_gamma': float(total_gamma),
            'total_vega': float(total_vega),
        }

    # ==================== Main Analysis ====================

    async def analyze(self, underlying: str, days: int = 90) -> VolAnalysisResult:
        """Run full volatility analysis."""
        logger.info(f"Analyzing {underlying} volatility surface...")
        
        result = VolAnalysisResult(
            underlying=underlying,
            timestamp=datetime.now(timezone.utc),
        )
        
        # Load data
        tickers = await self.load_latest_tickers(underlying)
        historical_iv = await self.load_historical_iv(underlying, days)
        spot_df = await self.load_spot_data(underlying, days)
        
        if tickers.empty:
            logger.warning(f"No ticker data for {underlying}")
            return result
        
        result.data_points = len(tickers)
        result.date_range_start = tickers['timestamp'].min()
        result.date_range_end = tickers['timestamp'].max()
        
        # Get spot price
        spot = tickers['underlying_price'].median()
        if pd.isna(spot) and not spot_df.empty:
            spot = spot_df['close'].iloc[-1]
        result.spot_price = float(spot) if not pd.isna(spot) else 0.0
        
        # ATM IV
        result.atm_iv = self.compute_atm_iv(tickers, spot)
        result.atm_iv_percentile = self.compute_iv_percentile(result.atm_iv, historical_iv)
        
        # Realized Vol
        rv = self.compute_rv(spot_df)
        result.rv_14d = rv['rv_14d']
        result.rv_30d = rv['rv_30d']
        result.rv_60d = rv['rv_60d']
        result.iv_rv_spread = result.atm_iv - result.rv_30d
        
        # Term Structure
        result.term_structure, result.term_slope = self.analyze_term_structure(tickers, spot)
        
        # Smile/Skew
        result.smile, result.put_call_skew, result.skew_25d = self.analyze_smile(tickers, spot)
        
        # Greeks
        greeks = self.compute_greeks_summary(tickers)
        result.total_delta = greeks['total_delta']
        result.total_gamma = greeks['total_gamma']
        result.total_vega = greeks['total_vega']
        
        return result

    # ==================== Report ====================

    def print_report(self, result: VolAnalysisResult):
        """Print formatted analysis report."""
        r = result
        now_str = r.timestamp.strftime('%Y-%m-%d %H:%M UTC')
        
        print(f"\n{'='*90}")
        print(f"  VOLATILITY SURFACE ANALYZER — {r.underlying}")
        print(f"  {now_str}")
        print(f"{'='*90}")
        
        print(f"\n  📈 SPOT & IMPLIED VOLATILITY")
        print(f"  {'Spot Price:':<30} ${r.spot_price:>12,.2f}")
        print(f"  {'ATM IV:':<30} {r.atm_iv*100:>12.1f}%")
        print(f"  {'IV Percentile:':<30} {r.atm_iv_percentile:>12.0f}th")
        
        print(f"\n  📉 REALIZED VOLATILITY")
        print(f"  {'14-Day RV:':<30} {r.rv_14d*100:>12.1f}%")
        print(f"  {'30-Day RV:':<30} {r.rv_30d*100:>12.1f}%")
        print(f"  {'60-Day RV:':<30} {r.rv_60d*100:>12.1f}%")
        
        print(f"\n  📊 IV-RV SPREAD")
        print(f"  {'IV - RV (30d):':<30} {r.iv_rv_spread*100:>+11.1f}pp")
        signal = "SELL_VOL" if r.iv_rv_spread > 0.10 else ("BUY_VOL" if r.iv_rv_spread < -0.05 else "NEUTRAL")
        print(f"  {'Signal:':<30} {signal:>12}")
        
        # Term Structure
        if r.term_structure:
            print(f"\n{'─'*90}")
            print(f"  IV TERM STRUCTURE")
            print(f"{'─'*90}")
            structure = "CONTANGO" if r.term_slope > 0 else ("BACKWARDATION" if r.term_slope < 0 else "FLAT")
            print(f"  Structure: {structure} (slope: {r.term_slope*10000:+.2f} bp/day)")
            print(f"\n  {'DTE':<8} {'Avg IV':>10} {'Call IV':>10} {'Put IV':>10} {'P-C Skew':>10}")
            print(f"  {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")
            for t in r.term_structure:
                print(
                    f"  {t['dte_bucket']:<8} "
                    f"{t['avg_iv']*100:>9.1f}% "
                    f"{t['call_iv']*100:>9.1f}% "
                    f"{t['put_iv']*100:>9.1f}% "
                    f"{t['put_call_skew']*100:>+9.1f}pp"
                )
        
        # Smile
        if r.smile:
            print(f"\n{'─'*90}")
            print(f"  VOLATILITY SMILE (by Moneyness)")
            print(f"{'─'*90}")
            print(f"  ATM Put-Call Skew: {r.put_call_skew*100:+.1f}pp | 25Δ Skew: {r.skew_25d*100:+.1f}pp")
            print(f"\n  {'Moneyness':<12} {'Avg IV':>10} {'Call IV':>10} {'Put IV':>10}")
            print(f"  {'─'*12} {'─'*10} {'─'*10} {'─'*10}")
            for s in r.smile:
                print(
                    f"  {s['moneyness']:<12} "
                    f"{s['avg_iv']*100:>9.1f}% "
                    f"{s['call_iv']*100:>9.1f}% "
                    f"{s['put_iv']*100:>9.1f}%"
                )
        
        # Greeks
        print(f"\n{'─'*90}")
        print(f"  AGGREGATE GREEKS (OI-weighted)")
        print(f"{'─'*90}")
        print(f"  {'Total Delta:':<30} {r.total_delta:>+15,.0f}")
        print(f"  {'Total Gamma:':<30} {r.total_gamma:>+15,.2f}")
        print(f"  {'Total Vega:':<30} {r.total_vega:>+15,.2f}")
        
        print(f"\n{'='*90}")
        print(f"  💡 Interpretation:")
        if r.iv_rv_spread > 0.10:
            print(f"  • IV > RV by {r.iv_rv_spread*100:.1f}pp → Options are expensive, consider selling vol")
        elif r.iv_rv_spread < -0.05:
            print(f"  • IV < RV by {abs(r.iv_rv_spread)*100:.1f}pp → Options are cheap, consider buying vol")
        if r.term_slope > 0.0001:
            print(f"  • Contango term structure → Far-dated options relatively expensive")
        elif r.term_slope < -0.0001:
            print(f"  • Backwardation → Near-dated options relatively expensive (fear/uncertainty)")
        if abs(r.put_call_skew) > 0.03:
            print(f"  • {'Puts' if r.put_call_skew > 0 else 'Calls'} are relatively expensive (skew)")
        print(f"{'='*90}\n")

    # ==================== Charts ====================

    def generate_charts(self, result: VolAnalysisResult, output_dir: str) -> List[str]:
        """Generate visualization charts."""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from mpl_toolkits.mplot3d import Axes3D
        
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')
        r = result
        paths = []
        
        colors = CHART_COLORS
        
        # ==================== Chart 1: Term Structure ====================
        if r.term_structure:
            fig, ax = plt.subplots(figsize=(12, 6), facecolor=colors['background'])
            ax.set_facecolor(colors['panel'])
            
            dtes = [t['dte_avg'] for t in r.term_structure]
            ivs = [t['avg_iv'] * 100 for t in r.term_structure]
            call_ivs = [t['call_iv'] * 100 for t in r.term_structure]
            put_ivs = [t['put_iv'] * 100 for t in r.term_structure]
            
            ax.plot(dtes, ivs, color=colors['gold'], linewidth=2, marker='o', markersize=8, label='ATM IV')
            ax.plot(dtes, call_ivs, color=colors['green'], linewidth=1.5, marker='s', markersize=6, alpha=0.8, label='Call IV')
            ax.plot(dtes, put_ivs, color=colors['red'], linewidth=1.5, marker='^', markersize=6, alpha=0.8, label='Put IV')
            
            ax.axhline(y=r.rv_30d * 100, color='white', linestyle='--', linewidth=1, alpha=0.5, label='30D RV')
            
            ax.set_xlabel('Days to Expiry', color=colors['text'], fontsize=11)
            ax.set_ylabel('Implied Volatility (%)', color=colors['text'], fontsize=11)
            ax.set_title(f'{r.underlying} — IV Term Structure', color=colors['text'], fontsize=14, fontweight='bold')
            ax.legend(facecolor=colors['panel'], edgecolor=colors['grid'], labelcolor=colors['text'])
            ax.tick_params(colors=colors['text'])
            ax.grid(True, alpha=0.2, color=colors['grid'])
            
            path = os.path.join(output_dir, f"term_structure_{r.underlying}_{ts}.png")
            fig.savefig(path, dpi=CHART_DPI, facecolor=fig.get_facecolor(), bbox_inches='tight')
            plt.close(fig)
            paths.append(path)
            logger.info(f"Saved: {path}")
        
        # ==================== Chart 2: Volatility Smile ====================
        if r.smile:
            fig, ax = plt.subplots(figsize=(12, 6), facecolor=colors['background'])
            ax.set_facecolor(colors['panel'])
            
            moneyness = [s['moneyness_val'] * 100 for s in r.smile]
            ivs = [s['avg_iv'] * 100 for s in r.smile]
            call_ivs = [s['call_iv'] * 100 for s in r.smile]
            put_ivs = [s['put_iv'] * 100 for s in r.smile]
            
            ax.plot(moneyness, ivs, color=colors['gold'], linewidth=2, marker='o', markersize=8, label='Avg IV')
            ax.plot(moneyness, call_ivs, color=colors['green'], linewidth=1.5, marker='s', markersize=6, alpha=0.8, label='Call IV')
            ax.plot(moneyness, put_ivs, color=colors['red'], linewidth=1.5, marker='^', markersize=6, alpha=0.8, label='Put IV')
            
            ax.axvline(x=100, color='white', linestyle='--', linewidth=1, alpha=0.5)
            
            ax.set_xlabel('Moneyness (%)', color=colors['text'], fontsize=11)
            ax.set_ylabel('Implied Volatility (%)', color=colors['text'], fontsize=11)
            ax.set_title(f'{r.underlying} — Volatility Smile', color=colors['text'], fontsize=14, fontweight='bold')
            ax.legend(facecolor=colors['panel'], edgecolor=colors['grid'], labelcolor=colors['text'])
            ax.tick_params(colors=colors['text'])
            ax.grid(True, alpha=0.2, color=colors['grid'])
            
            path = os.path.join(output_dir, f"vol_smile_{r.underlying}_{ts}.png")
            fig.savefig(path, dpi=CHART_DPI, facecolor=fig.get_facecolor(), bbox_inches='tight')
            plt.close(fig)
            paths.append(path)
            logger.info(f"Saved: {path}")
        
        # ==================== Chart 3: IV vs RV Summary ====================
        fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor=colors['background'])
        
        # Left: IV vs RV bars
        ax = axes[0]
        ax.set_facecolor(colors['panel'])
        
        labels = ['ATM IV', '14D RV', '30D RV', '60D RV']
        values = [r.atm_iv * 100, r.rv_14d * 100, r.rv_30d * 100, r.rv_60d * 100]
        bar_colors = [colors['gold'], colors['cyan'], colors['green'], colors['blue']]
        
        bars = ax.bar(labels, values, color=bar_colors, alpha=0.85, edgecolor='white', linewidth=0.5)
        
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, f'{val:.1f}%',
                   ha='center', va='bottom', color=colors['text'], fontsize=10)
        
        ax.set_ylabel('Volatility (%)', color=colors['text'], fontsize=11)
        ax.set_title(f'{r.underlying} — IV vs Realized Vol', color=colors['text'], fontsize=12, fontweight='bold')
        ax.tick_params(colors=colors['text'])
        ax.set_ylim(0, max(values) * 1.3)
        
        # Right: IV-RV Spread gauge
        ax = axes[1]
        ax.set_facecolor(colors['panel'])
        
        spread = r.iv_rv_spread * 100
        gauge_colors = [colors['green'] if spread > 5 else (colors['red'] if spread < -5 else colors['gold'])]
        ax.barh(['IV-RV Spread'], [spread], color=gauge_colors, alpha=0.85, height=0.4)
        ax.axvline(x=0, color='white', linewidth=1.5)
        ax.axvline(x=10, color=colors['green'], linewidth=1, linestyle='--', alpha=0.5)
        ax.axvline(x=-5, color=colors['red'], linewidth=1, linestyle='--', alpha=0.5)
        
        ax.text(spread + (2 if spread >= 0 else -2), 0, f'{spread:+.1f}pp', 
               ha='left' if spread >= 0 else 'right', va='center', color=colors['text'], fontsize=12, fontweight='bold')
        
        ax.set_xlim(-30, 30)
        ax.set_xlabel('IV - RV (percentage points)', color=colors['text'], fontsize=11)
        ax.set_title('IV-RV Spread (vs 30D RV)', color=colors['text'], fontsize=12, fontweight='bold')
        ax.tick_params(colors=colors['text'])
        
        # Add zone labels
        ax.text(-17, -0.6, 'BUY VOL', color=colors['red'], fontsize=9, alpha=0.7)
        ax.text(15, -0.6, 'SELL VOL', color=colors['green'], fontsize=9, alpha=0.7)
        
        plt.tight_layout()
        path = os.path.join(output_dir, f"iv_rv_summary_{r.underlying}_{ts}.png")
        fig.savefig(path, dpi=CHART_DPI, facecolor=fig.get_facecolor(), bbox_inches='tight')
        plt.close(fig)
        paths.append(path)
        logger.info(f"Saved: {path}")
        
        return paths

    def save_results(self, result: VolAnalysisResult, output_dir: str):
        """Save analysis results to JSON."""
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')
        
        data = asdict(result)
        data['timestamp'] = result.timestamp.isoformat()
        data['date_range_start'] = result.date_range_start.isoformat() if result.date_range_start else None
        data['date_range_end'] = result.date_range_end.isoformat() if result.date_range_end else None
        
        json_path = os.path.join(output_dir, f"analysis_{result.underlying}_{ts}.json")
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logger.info(f"Saved: {json_path}")


# ==================== CLI Entry ====================

async def run_analyzer(
    underlying: Optional[str] = None,
    days: int = 90,
    save: bool = True,
):
    """Main analyzer entry point."""
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
    
    analyzer = VolatilityAnalyzer(db)
    
    # Check for data
    check = await db.read_one("SELECT COUNT(*) as cnt FROM options_tickers")
    if not check or check.get('cnt', 0) == 0:
        print("\n" + "="*80)
        print("  ⚠️  NO OPTIONS DATA IN DATABASE")
        print("="*80)
        print("  The options_tickers table is empty.")
        print("  Please run the data fetch script first:")
        print("")
        print("    python3 scripts/fetch_deribit_options.py --currency BTC ETH")
        print("="*80 + "\n")
        await db.close()
        return {}
    
    underlyings = [underlying] if underlying else UNDERLYINGS
    results = {}
    
    for ul in underlyings:
        result = await analyzer.analyze(ul, days)
        results[ul] = result
        analyzer.print_report(result)
        
        if save and result.data_points > 0:
            output_dir = os.path.join(os.path.dirname(__file__), ANALYZER_OUTPUT_DIR)
            analyzer.generate_charts(result, output_dir)
            analyzer.save_results(result, output_dir)
    
    await db.close()
    return results


def main():
    parser = argparse.ArgumentParser(description="Volatility Surface Analyzer")
    parser.add_argument("--underlying", "-u", type=str, default=None, help="BTC or ETH (default: both)")
    parser.add_argument("--days", "-d", type=int, default=90, help="Historical lookback days")
    parser.add_argument("--no-save", action="store_true", help="Don't save results/charts")
    args = parser.parse_args()
    
    asyncio.run(run_analyzer(
        underlying=args.underlying,
        days=args.days,
        save=not args.no_save,
    ))


if __name__ == "__main__":
    main()
