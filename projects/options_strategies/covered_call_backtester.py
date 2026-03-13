"""
Systematic Covered Call Backtester

Strategy: Hold BTC/ETH spot, sell OTM call options on a weekly/monthly schedule.
Collect premium, accept capped upside if price rallies past strike.

Parameters:
- underlying: BTC or ETH
- target_delta: 0.1 - 0.4 (selects OTM strike by delta)
- expiry_period: 7, 14, or 30 days
- roll_timing: days before expiry to roll (0 = hold to expiry)

Metrics:
- premium_yield (annualised)
- call_away_rate (% of periods where spot > strike at expiry)
- total_return_vs_hodl
- max_drawdown
- sharpe_ratio

Data: Uses options_tickers (IV, greeks) + klines (spot price) from 2025-01-01 onward.

Usage:
    python3 -m projects.options_strategies.covered_call_backtester
    python3 -m projects.options_strategies.covered_call_backtester --underlying BTC --delta 0.2 --expiry 30
"""

import asyncio
import argparse
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm as _norm

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from database.client import PostgresClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ==================== Black-Scholes Helpers ====================

def _norm_cdf(x: float) -> float:
    """Standard normal CDF (thin wrapper around scipy)."""
    return float(_norm.cdf(x))


def bs_call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call price. T in years, sigma annualised."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(S - K, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def bs_delta(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call delta."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 1.0 if S > K else 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    return _norm_cdf(d1)


def find_strike_by_delta(
    S: float, T: float, r: float, sigma: float,
    target_delta: float, strike_min: float = None, strike_max: float = None,
) -> float:
    """Find OTM call strike that has approximately target_delta."""
    if strike_min is None:
        strike_min = S * 0.9
    if strike_max is None:
        strike_max = S * 3.0

    # Binary search for strike with target delta
    lo, hi = strike_min, strike_max
    for _ in range(100):
        mid = (lo + hi) / 2
        d = bs_delta(S, mid, T, r, sigma)
        if d > target_delta:
            lo = mid
        else:
            hi = mid
        if abs(d - target_delta) < 0.001:
            break
    return (lo + hi) / 2


# ==================== Config ====================

@dataclass
class CoveredCallConfig:
    underlying: str = "BTC"
    target_delta: float = 0.2      # OTM call delta target
    expiry_period: int = 30        # days to expiry for each sold call
    roll_timing: int = 1           # roll N days before expiry
    risk_free_rate: float = 0.045  # annualised (T-bill proxy)
    initial_capital: float = 100_000  # USD
    spot_fee: float = 0.001        # 0.1% taker
    option_fee: float = 0.0003     # Deribit taker fee
    slippage: float = 0.001        # option bid-ask slippage (fraction of premium)
    start_date: str = "2025-01-01"
    end_date: str = ""             # empty = now


@dataclass
class CoveredCallResult:
    """Backtest result for covered call strategy."""
    underlying: str = ""
    config: Optional[CoveredCallConfig] = None

    # Performance
    total_return_pct: float = 0.0
    hodl_return_pct: float = 0.0
    excess_return_pct: float = 0.0  # vs HODL
    annualised_return_pct: float = 0.0
    annualised_premium_yield_pct: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown_pct: float = 0.0

    # Strategy specifics
    num_periods: int = 0
    call_away_rate_pct: float = 0.0  # % of periods where called away
    avg_premium_pct: float = 0.0     # avg premium as % of spot
    total_premium_collected: float = 0.0
    total_call_away_loss: float = 0.0

    # Time
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    holding_days: float = 0.0

    # Curves
    equity_curve: List[float] = field(default_factory=list)
    hodl_curve: List[float] = field(default_factory=list)
    period_details: List[Dict] = field(default_factory=list)


# ==================== Backtester ====================

class CoveredCallBacktester:
    """Backtest systematic covered call on crypto options."""

    def __init__(self, db: PostgresClient, config: CoveredCallConfig):
        self.db = db
        self.config = config

    async def load_spot_data(self) -> pd.DataFrame:
        """
        Load daily spot price data from klines table.

        Uses the perpetual future as spot proxy (most liquid).
        Falls back to any available PERP with the right base currency.
        """
        # Try to find a perpetual instrument for the underlying
        inst = await self.db.read_one("""
            SELECT instrument_id FROM instruments
            WHERE base_currency = $1 AND type = 'PERP' AND is_active = TRUE
            ORDER BY instrument_id
            LIMIT 1
        """, self.config.underlying)

        if not inst:
            raise ValueError(
                f"No active PERP instrument found for {self.config.underlying}. "
                f"Run instrument_job first."
            )

        instrument_id = inst["instrument_id"]
        logger.info(f"Using {instrument_id} as spot proxy")

        start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = (
            datetime.strptime(self.config.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if self.config.end_date
            else datetime.now(timezone.utc)
        )

        klines = await self.db.read("""
            SELECT open_time, close
            FROM klines
            WHERE instrument_id = $1
              AND interval IN ('1d', '1D', '1Dutc')
              AND open_time >= $2 AND open_time <= $3
            ORDER BY open_time
        """, instrument_id, start_dt, end_dt)

        if not klines:
            # Try 4h and resample
            klines = await self.db.read("""
                SELECT open_time, close
                FROM klines
                WHERE instrument_id = $1
                  AND interval IN ('4h', '4H')
                  AND open_time >= $2 AND open_time <= $3
                ORDER BY open_time
            """, instrument_id, start_dt, end_dt)

        if not klines:
            raise ValueError(f"No kline data found for {instrument_id}")

        df = pd.DataFrame(klines)
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df["close"] = df["close"].astype(float)

        # Resample to daily
        df = df.set_index("open_time").resample("1D").last().dropna().reset_index()

        logger.info(f"Loaded {len(df)} daily spot prices for {self.config.underlying}")
        return df

    async def load_iv_data(self) -> pd.DataFrame:
        """
        Load implied volatility data from options_tickers.

        Aggregates ATM IV per day for the underlying.
        """
        start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = (
            datetime.strptime(self.config.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if self.config.end_date
            else datetime.now(timezone.utc)
        )

        iv_data = await self.db.read("""
            SELECT
                DATE_TRUNC('day', ot.timestamp) AS date,
                AVG(ot.iv) AS avg_iv,
                AVG(ot.iv) FILTER (WHERE ABS(ot.delta) BETWEEN 0.4 AND 0.6) AS atm_iv
            FROM options_tickers ot
            JOIN options_instruments oi ON oi.instrument_id = ot.instrument_id
            WHERE oi.underlying = $1
              AND ot.iv IS NOT NULL AND ot.iv > 0
              AND ot.timestamp >= $2 AND ot.timestamp <= $3
            GROUP BY DATE_TRUNC('day', ot.timestamp)
            ORDER BY date
        """, self.config.underlying, start_dt, end_dt)

        if iv_data:
            df = pd.DataFrame(iv_data)
            df["date"] = pd.to_datetime(df["date"], utc=True)
            # Prefer ATM IV, fall back to avg IV
            df["iv"] = df["atm_iv"].fillna(df["avg_iv"])
            logger.info(f"Loaded {len(df)} days of IV data")
            return df

        # Fallback: use historical volatility API data or estimate
        logger.warning(f"No IV data in DB for {self.config.underlying}. Using estimated IV from spot vol.")
        return pd.DataFrame()

    def estimate_rv(self, spot_df: pd.DataFrame, window: int = 30) -> pd.Series:
        """Estimate realized volatility from daily spot prices."""
        returns = np.log(spot_df["close"] / spot_df["close"].shift(1))
        rv = returns.rolling(window).std() * np.sqrt(365)
        return rv

    def run_backtest(
        self,
        spot_df: pd.DataFrame,
        iv_df: Optional[pd.DataFrame] = None,
    ) -> CoveredCallResult:
        """
        Run covered call backtest.

        Logic:
        - Start with initial_capital in spot (buy at first price)
        - Every expiry_period days, sell an OTM call with target_delta
        - At expiry: if spot < strike → keep premium; if spot > strike → capped at strike
        - Roll into new call
        """
        result = CoveredCallResult(
            underlying=self.config.underlying,
            config=self.config,
        )

        if len(spot_df) < 10:
            logger.warning("Insufficient spot data for backtest")
            return result

        cfg = self.config
        capital = cfg.initial_capital

        # Merge IV data if available
        spot_df = spot_df.copy()
        spot_df["date_key"] = spot_df["open_time"].dt.normalize()

        if iv_df is not None and not iv_df.empty:
            iv_df = iv_df.copy()
            iv_df["date_key"] = iv_df["date"].dt.normalize()
            spot_df = spot_df.merge(iv_df[["date_key", "iv"]], on="date_key", how="left")
        else:
            spot_df["iv"] = np.nan

        # Fill IV: use realized vol as fallback
        rv = self.estimate_rv(spot_df, window=30)
        spot_df["rv"] = rv
        spot_df["iv"] = spot_df["iv"].fillna(spot_df["rv"])
        spot_df["iv"] = spot_df["iv"].fillna(0.6)  # last resort default

        entry_price = float(spot_df.iloc[0]["close"])
        units = capital / entry_price  # number of "coins" held

        # Track state
        equity = capital
        equity_curve = [equity]
        hodl_curve = [capital]
        period_details = []
        call_aways = 0
        total_premium = 0.0
        total_call_away_loss = 0.0
        premium_pcts = []

        # Iterate over roll periods
        i = 0
        period_start = 0
        period_num = 0

        while period_start < len(spot_df) - 1:
            period_end = min(period_start + cfg.expiry_period, len(spot_df) - 1)
            period_num += 1

            spot_at_sell = float(spot_df.iloc[period_start]["close"])
            spot_at_expiry = float(spot_df.iloc[period_end]["close"])
            iv_at_sell = float(spot_df.iloc[period_start]["iv"])

            T = cfg.expiry_period / 365.0

            # Find strike by target delta
            strike = find_strike_by_delta(
                S=spot_at_sell, T=T, r=cfg.risk_free_rate,
                sigma=iv_at_sell, target_delta=cfg.target_delta,
            )

            # Compute call premium using BS
            premium_per_unit = bs_call_price(
                S=spot_at_sell, K=strike, T=T,
                r=cfg.risk_free_rate, sigma=iv_at_sell,
            )

            # Apply fees and slippage
            net_premium_per_unit = premium_per_unit * (1 - cfg.option_fee - cfg.slippage)
            total_period_premium = net_premium_per_unit * units

            total_premium += total_period_premium
            premium_pcts.append(net_premium_per_unit / spot_at_sell)

            # At expiry: check if called away
            called_away = spot_at_expiry > strike
            if called_away:
                call_aways += 1
                # Spot position capped at strike price
                spot_pnl_per_unit = strike - spot_at_sell
                # But we missed the upside above strike
                call_away_loss_per_unit = spot_at_expiry - strike
                total_call_away_loss += call_away_loss_per_unit * units
            else:
                spot_pnl_per_unit = spot_at_expiry - spot_at_sell
                call_away_loss_per_unit = 0.0

            # Update equity
            period_pnl = (spot_pnl_per_unit + net_premium_per_unit) * units
            equity += period_pnl

            # Rebalance: adjust units for new spot price
            effective_spot = min(spot_at_expiry, strike) if called_away else spot_at_expiry
            if effective_spot > 0:
                units = equity / spot_at_expiry  # re-enter at current spot

            equity_curve.append(equity)
            hodl_value = capital * (spot_at_expiry / entry_price)
            hodl_curve.append(hodl_value)

            period_details.append({
                "period": period_num,
                "start_date": spot_df.iloc[period_start]["open_time"],
                "end_date": spot_df.iloc[period_end]["open_time"],
                "spot_entry": spot_at_sell,
                "spot_exit": spot_at_expiry,
                "strike": round(strike, 2),
                "iv": round(iv_at_sell, 4),
                "premium": round(total_period_premium, 2),
                "premium_pct": round(net_premium_per_unit / spot_at_sell * 100, 3),
                "called_away": called_away,
                "period_pnl": round(period_pnl, 2),
                "equity": round(equity, 2),
            })

            # Move to next period
            period_start = period_end

        # ==================== Compute Metrics ====================

        final_equity = equity
        final_hodl = capital * (float(spot_df.iloc[-1]["close"]) / entry_price)

        total_return = (final_equity - capital) / capital
        hodl_return = (final_hodl - capital) / capital
        excess_return = total_return - hodl_return

        holding_days = (spot_df.iloc[-1]["open_time"] - spot_df.iloc[0]["open_time"]).total_seconds() / 86400
        ann_return = total_return * (365 / holding_days) if holding_days > 0 else 0

        # Premium yield
        if premium_pcts:
            avg_premium_pct = np.mean(premium_pcts)
            periods_per_year = 365 / cfg.expiry_period
            ann_premium_yield = avg_premium_pct * periods_per_year
        else:
            avg_premium_pct = 0
            ann_premium_yield = 0

        # Sharpe from equity curve
        eq_arr = np.array(equity_curve)
        eq_returns = np.diff(eq_arr) / eq_arr[:-1]
        if len(eq_returns) > 1:
            periods_per_year = 365 / cfg.expiry_period
            sharpe = (np.mean(eq_returns) / np.std(eq_returns)) * np.sqrt(periods_per_year) if np.std(eq_returns) > 0 else 0
        else:
            sharpe = 0

        # Max drawdown
        running_max = np.maximum.accumulate(eq_arr)
        drawdown = (eq_arr - running_max) / running_max
        max_dd = float(drawdown.min()) * 100

        # Fill result
        result.total_return_pct = round(total_return * 100, 2)
        result.hodl_return_pct = round(hodl_return * 100, 2)
        result.excess_return_pct = round(excess_return * 100, 2)
        result.annualised_return_pct = round(ann_return * 100, 2)
        result.annualised_premium_yield_pct = round(ann_premium_yield * 100, 2)
        result.sharpe_ratio = round(sharpe, 3)
        result.max_drawdown_pct = round(max_dd, 2)
        result.num_periods = period_num
        result.call_away_rate_pct = round(call_aways / period_num * 100, 1) if period_num else 0
        result.avg_premium_pct = round(float(avg_premium_pct) * 100, 3)
        result.total_premium_collected = round(total_premium, 2)
        result.total_call_away_loss = round(total_call_away_loss, 2)
        result.start_date = spot_df.iloc[0]["open_time"]
        result.end_date = spot_df.iloc[-1]["open_time"]
        result.holding_days = round(holding_days, 1)
        result.equity_curve = equity_curve
        result.hodl_curve = hodl_curve
        result.period_details = period_details

        return result

    def print_report(self, result: CoveredCallResult):
        """Print formatted backtest report."""
        cfg = self.config
        r = result

        print(f"\n{'='*80}")
        print(f"  COVERED CALL BACKTESTER — {r.underlying}")
        print(f"{'='*80}")
        print(f"  Target Delta: {cfg.target_delta} | Expiry: {cfg.expiry_period}d | Roll: {cfg.roll_timing}d before")
        print(f"  Capital: ${cfg.initial_capital:,.0f}")
        print(f"  Period: {r.start_date} → {r.end_date} ({r.holding_days:.0f} days)")
        print(f"{'─'*80}")

        print(f"\n  📊 PERFORMANCE")
        print(f"  {'Strategy Return:':<30} {r.total_return_pct:>8.2f}%")
        print(f"  {'HODL Return:':<30} {r.hodl_return_pct:>8.2f}%")
        print(f"  {'Excess vs HODL:':<30} {r.excess_return_pct:>8.2f}%")
        print(f"  {'Annualised Return:':<30} {r.annualised_return_pct:>8.2f}%")
        print(f"  {'Sharpe Ratio:':<30} {r.sharpe_ratio:>8.3f}")
        print(f"  {'Max Drawdown:':<30} {r.max_drawdown_pct:>8.2f}%")

        print(f"\n  📝 PREMIUM STATS")
        print(f"  {'Total Premium Collected:':<30} ${r.total_premium_collected:>10,.2f}")
        print(f"  {'Avg Premium / Period:':<30} {r.avg_premium_pct:>8.3f}% of spot")
        print(f"  {'Annualised Premium Yield:':<30} {r.annualised_premium_yield_pct:>8.2f}%")
        print(f"  {'Total Call-Away Loss:':<30} ${r.total_call_away_loss:>10,.2f}")

        print(f"\n  🔄 ROLL STATS")
        print(f"  {'Number of Periods:':<30} {r.num_periods:>8d}")
        print(f"  {'Call-Away Rate:':<30} {r.call_away_rate_pct:>8.1f}%")

        # Period details (last 5)
        if r.period_details:
            print(f"\n  📋 RECENT PERIODS")
            print(f"  {'Period':<8} {'Spot→':<20} {'Strike':>10} {'IV':>8} {'Premium':>10} {'Called':>8} {'PnL':>10}")
            print(f"  {'─'*8} {'─'*20} {'─'*10} {'─'*8} {'─'*10} {'─'*8} {'─'*10}")
            for pd_row in r.period_details[-10:]:
                print(
                    f"  {pd_row['period']:<8} "
                    f"${pd_row['spot_entry']:>8,.0f}→${pd_row['spot_exit']:>8,.0f} "
                    f"${pd_row['strike']:>9,.0f} "
                    f"{pd_row['iv']:>7.1%} "
                    f"${pd_row['premium']:>9,.0f} "
                    f"{'YES' if pd_row['called_away'] else 'no':>8} "
                    f"${pd_row['period_pnl']:>9,.0f}"
                )

        print(f"\n{'='*80}\n")

    def generate_charts(self, result: CoveredCallResult, output_dir: str):
        """Generate visualization charts."""
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

        r = result
        cfg = self.config

        fig, axes = plt.subplots(2, 2, figsize=(18, 12), facecolor="#1a1a2e")
        fig.suptitle(
            f"Covered Call — {r.underlying} | Δ={cfg.target_delta} | {cfg.expiry_period}d expiry",
            fontsize=16, color="white", fontweight="bold",
        )
        text_color = "#e0e0e0"

        # 1: Equity curve vs HODL
        ax = axes[0, 0]
        ax.set_facecolor("#16213e")
        ax.plot(r.equity_curve, color="#00d4aa", linewidth=1.5, label="Covered Call")
        ax.plot(r.hodl_curve, color="#ff4757", linewidth=1.0, alpha=0.7, label="HODL")
        ax.axhline(y=cfg.initial_capital, color="white", linewidth=0.5, alpha=0.3, linestyle="--")
        ax.set_title("Equity: Covered Call vs HODL", color=text_color, fontsize=12)
        ax.set_xlabel("Period", color=text_color)
        ax.set_ylabel("Equity ($)", color=text_color)
        ax.legend(facecolor="#16213e", edgecolor="#333", labelcolor=text_color)
        ax.tick_params(colors=text_color)

        # 2: Premium collected per period
        ax = axes[0, 1]
        ax.set_facecolor("#16213e")
        premiums = [p["premium"] for p in r.period_details]
        colors_bar = ["#ff4757" if p["called_away"] else "#00d4aa" for p in r.period_details]
        ax.bar(range(len(premiums)), premiums, color=colors_bar, alpha=0.8)
        ax.set_title("Premium per Period (red = called away)", color=text_color, fontsize=12)
        ax.set_xlabel("Period", color=text_color)
        ax.set_ylabel("Premium ($)", color=text_color)
        ax.tick_params(colors=text_color)

        # 3: Cumulative premium
        ax = axes[1, 0]
        ax.set_facecolor("#16213e")
        cum_premium = np.cumsum(premiums)
        ax.plot(cum_premium, color="#ffd700", linewidth=1.5)
        ax.fill_between(range(len(cum_premium)), 0, cum_premium, color="#ffd700", alpha=0.15)
        ax.set_title("Cumulative Premium Collected", color=text_color, fontsize=12)
        ax.set_xlabel("Period", color=text_color)
        ax.set_ylabel("Cumulative Premium ($)", color=text_color)
        ax.tick_params(colors=text_color)

        # 4: Period PnL distribution
        ax = axes[1, 1]
        ax.set_facecolor("#16213e")
        pnls = [p["period_pnl"] for p in r.period_details]
        ax.hist(pnls, bins=max(10, len(pnls) // 3), color="#00d4aa", alpha=0.8, edgecolor="#333")
        ax.axvline(x=0, color="#ff4757", linewidth=1, linestyle="--")
        ax.set_title("Period PnL Distribution", color=text_color, fontsize=12)
        ax.set_xlabel("PnL ($)", color=text_color)
        ax.set_ylabel("Count", color=text_color)
        ax.tick_params(colors=text_color)

        plt.tight_layout()
        path = os.path.join(output_dir, f"covered_call_{r.underlying}_{ts}.png")
        fig.savefig(path, dpi=150, facecolor=fig.get_facecolor())
        plt.close(fig)
        logger.info(f"Chart saved: {path}")
        return path


# ==================== CLI Entry ====================

async def run_backtest(
    underlying: str = "BTC",
    target_delta: float = 0.2,
    expiry_period: int = 30,
    roll_timing: int = 1,
    start_date: str = "2025-01-01",
    end_date: str = "",
):
    """Main backtest entry point."""
    db = PostgresClient()
    await db.init_pool()

    config = CoveredCallConfig(
        underlying=underlying,
        target_delta=target_delta,
        expiry_period=expiry_period,
        roll_timing=roll_timing,
        start_date=start_date,
        end_date=end_date,
    )

    bt = CoveredCallBacktester(db, config)

    logger.info(f"Loading spot data for {underlying}...")
    spot_df = await bt.load_spot_data()

    logger.info(f"Loading IV data for {underlying}...")
    iv_df = await bt.load_iv_data()

    logger.info(f"Running backtest...")
    result = bt.run_backtest(spot_df, iv_df)

    bt.print_report(result)

    output_dir = os.path.join(os.path.dirname(__file__), "output", "covered_call")
    bt.generate_charts(result, output_dir)

    # Save CSV summary
    os.makedirs(output_dir, exist_ok=True)
    if result.period_details:
        df = pd.DataFrame(result.period_details)
        csv_path = os.path.join(
            output_dir,
            f"covered_call_{underlying}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.csv"
        )
        df.to_csv(csv_path, index=False)
        logger.info(f"Results saved: {csv_path}")

    await db.close()
    return result


def main():
    parser = argparse.ArgumentParser(description="Systematic Covered Call Backtester")
    parser.add_argument("--underlying", type=str, default="BTC", help="BTC or ETH")
    parser.add_argument("--delta", type=float, default=0.2, help="Target delta (0.1-0.4)")
    parser.add_argument("--expiry", type=int, default=30, help="Expiry period in days (7/14/30)")
    parser.add_argument("--roll", type=int, default=1, help="Roll timing (days before expiry)")
    parser.add_argument("--start", type=str, default="2025-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default="", help="End date YYYY-MM-DD (default: now)")
    args = parser.parse_args()

    asyncio.run(run_backtest(
        underlying=args.underlying,
        target_delta=args.delta,
        expiry_period=args.expiry,
        roll_timing=args.roll,
        start_date=args.start,
        end_date=args.end,
    ))


if __name__ == "__main__":
    main()
