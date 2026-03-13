"""
IV-RV Spread Backtester — Short Strangle with IV Filter

Strategy: Compare Implied Volatility vs Realized Volatility.
When IV > RV by a threshold → sell strangle (OTM put + OTM call) or iron condor.
Collect premium when vol mean-reverts. Stop-loss if position moves against.

Parameters:
- iv_rv_threshold: minimum IV - RV spread to trigger entry (e.g. 0.10 = 10%)
- strike_width: OTM distance in standard deviations (e.g. 1.0 = 1 std dev)
- expiry: target days to expiry (7/14/30)
- stop_loss_multiple: close if loss exceeds N × premium collected
- max_positions: maximum concurrent positions

Metrics:
- iv_rv_spread (avg and distribution)
- win_rate
- avg_premium / avg_loss
- expected_value (EV per trade)
- profit_factor
- sharpe_ratio

Data: Uses options_tickers (IV, greeks) + klines (spot price) from 2025-01-01 onward.

Usage:
    python3 -m projects.options_strategies.iv_rv_backtester
    python3 -m projects.options_strategies.iv_rv_backtester --underlying BTC --threshold 0.15 --width 1.5
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from database.client import PostgresClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ==================== Black-Scholes Helpers ====================

def _norm_cdf(x: float) -> float:
    """Standard normal CDF using error function."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call price."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(S - K, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def bs_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes put price."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def strangle_premium(
    S: float, K_put: float, K_call: float, T: float, r: float, sigma: float
) -> float:
    """Total premium from selling a strangle (short put + short call)."""
    return bs_put(S, K_put, T, r, sigma) + bs_call(S, K_call, T, r, sigma)


def strangle_pnl_at_expiry(
    S_expiry: float, K_put: float, K_call: float, premium: float
) -> float:
    """
    PnL of a short strangle at expiry.

    - If S between strikes: full premium kept
    - If S < K_put: lose (K_put - S) minus premium
    - If S > K_call: lose (S - K_call) minus premium
    """
    if S_expiry < K_put:
        intrinsic_loss = K_put - S_expiry
        return premium - intrinsic_loss
    elif S_expiry > K_call:
        intrinsic_loss = S_expiry - K_call
        return premium - intrinsic_loss
    else:
        return premium


# ==================== Config ====================

@dataclass
class IVRVConfig:
    underlying: str = "BTC"
    iv_rv_threshold: float = 0.10   # Minimum IV-RV spread to enter (decimal, 0.10 = 10pp)
    strike_width: float = 1.0       # OTM strikes in std devs from spot
    expiry_days: int = 14           # Target days to expiry
    stop_loss_multiple: float = 2.0 # Close if loss > N × premium
    risk_free_rate: float = 0.045
    initial_capital: float = 100_000
    position_size_pct: float = 0.10 # Max 10% of capital per position
    max_positions: int = 3
    option_fee: float = 0.0003
    slippage: float = 0.002
    rv_window: int = 30             # RV lookback window in days
    start_date: str = "2025-01-01"
    end_date: str = ""


@dataclass
class TradeRecord:
    """Single trade record."""
    entry_date: datetime = None
    exit_date: datetime = None
    spot_entry: float = 0
    spot_exit: float = 0
    k_put: float = 0
    k_call: float = 0
    iv_at_entry: float = 0
    rv_at_entry: float = 0
    iv_rv_spread: float = 0
    premium: float = 0
    pnl: float = 0
    return_pct: float = 0
    stopped_out: bool = False
    reason: str = ""


@dataclass
class IVRVResult:
    """Backtest result."""
    underlying: str = ""
    config: Optional[IVRVConfig] = None

    # Performance
    total_return_pct: float = 0.0
    annualised_return_pct: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown_pct: float = 0.0

    # Trade stats
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate_pct: float = 0.0
    avg_premium: float = 0.0
    avg_loss: float = 0.0
    avg_win: float = 0.0
    expected_value: float = 0.0   # EV per trade
    profit_factor: float = 0.0   # gross profit / gross loss

    # IV-RV stats
    avg_iv_rv_spread: float = 0.0
    median_iv_rv_spread: float = 0.0
    num_signal_days: int = 0      # days where IV-RV > threshold

    # Time
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    holding_days: float = 0.0

    # Curves & details
    equity_curve: List[float] = field(default_factory=list)
    trades: List[TradeRecord] = field(default_factory=list)
    daily_iv_rv: List[Dict] = field(default_factory=list)


# ==================== Backtester ====================

class IVRVBacktester:
    """Backtest IV-RV spread strategy with short strangle."""

    def __init__(self, db: PostgresClient, config: IVRVConfig):
        self.db = db
        self.config = config

    async def load_spot_data(self) -> pd.DataFrame:
        """Load daily spot price from klines."""
        inst = await self.db.read_one("""
            SELECT instrument_id FROM instruments
            WHERE base_currency = $1 AND type = 'PERP' AND is_active = TRUE
            ORDER BY instrument_id LIMIT 1
        """, self.config.underlying)

        if not inst:
            raise ValueError(f"No PERP instrument for {self.config.underlying}")

        instrument_id = inst["instrument_id"]
        start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # Load extra history for RV calculation
        rv_start = start_dt - timedelta(days=self.config.rv_window + 10)
        end_dt = (
            datetime.strptime(self.config.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if self.config.end_date
            else datetime.now(timezone.utc)
        )

        klines = await self.db.read("""
            SELECT open_time, close
            FROM klines
            WHERE instrument_id = $1
              AND interval IN ('1d', '1D', '1Dutc', '4h', '4H')
              AND open_time >= $2 AND open_time <= $3
            ORDER BY open_time
        """, instrument_id, rv_start, end_dt)

        if not klines:
            raise ValueError(f"No kline data for {instrument_id}")

        df = pd.DataFrame(klines)
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df["close"] = df["close"].astype(float)
        df = df.set_index("open_time").resample("1D").last().dropna().reset_index()

        logger.info(f"Loaded {len(df)} daily spot prices for {self.config.underlying}")
        return df

    async def load_iv_data(self) -> pd.DataFrame:
        """Load daily average IV from options_tickers."""
        start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = (
            datetime.strptime(self.config.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if self.config.end_date
            else datetime.now(timezone.utc)
        )

        iv_data = await self.db.read("""
            SELECT
                DATE_TRUNC('day', ot.timestamp) AS date,
                AVG(ot.iv) FILTER (WHERE ABS(ot.delta) BETWEEN 0.2 AND 0.8) AS avg_iv,
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
            df["iv"] = df["atm_iv"].fillna(df["avg_iv"])
            return df

        logger.warning("No IV data in DB, will estimate from spot vol")
        return pd.DataFrame()

    def compute_rv(self, spot_df: pd.DataFrame) -> pd.Series:
        """Compute annualised realized volatility from daily closes."""
        returns = np.log(spot_df["close"] / spot_df["close"].shift(1))
        rv = returns.rolling(self.config.rv_window).std() * np.sqrt(365)
        return rv

    def run_backtest(
        self,
        spot_df: pd.DataFrame,
        iv_df: Optional[pd.DataFrame] = None,
    ) -> IVRVResult:
        """
        Run IV-RV spread backtest.

        Logic:
        - Each day: compute IV and RV
        - If IV - RV > threshold and no open position → sell strangle
        - At expiry: settle PnL
        - During holding: check stop-loss daily
        """
        cfg = self.config
        result = IVRVResult(underlying=cfg.underlying, config=cfg)

        if len(spot_df) < cfg.rv_window + 10:
            logger.warning("Insufficient data for backtest")
            return result

        # Compute RV
        spot_df = spot_df.copy()
        spot_df["rv"] = self.compute_rv(spot_df)
        spot_df["date_key"] = spot_df["open_time"].dt.normalize()

        # Merge IV
        if iv_df is not None and not iv_df.empty:
            iv_df = iv_df.copy()
            iv_df["date_key"] = iv_df["date"].dt.normalize()
            spot_df = spot_df.merge(iv_df[["date_key", "iv"]], on="date_key", how="left")
        else:
            spot_df["iv"] = np.nan

        # Fill IV with RV * 1.15 as estimation (IV typically > RV)
        spot_df["iv"] = spot_df["iv"].fillna(spot_df["rv"] * 1.15)
        spot_df["iv"] = spot_df["iv"].fillna(0.6)

        # Compute IV-RV spread
        spot_df["iv_rv_spread"] = spot_df["iv"] - spot_df["rv"]

        # Filter to actual backtest period
        start_dt = datetime.strptime(cfg.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        bt_df = spot_df[spot_df["open_time"] >= start_dt].reset_index(drop=True)

        if len(bt_df) < 10:
            logger.warning("Insufficient data in backtest period")
            return result

        # Track state
        capital = cfg.initial_capital
        equity = capital
        equity_curve = [equity]
        trades = []
        daily_iv_rv = []

        # Active positions
        active_positions = []

        for i in range(len(bt_df)):
            row = bt_df.iloc[i]
            date = row["open_time"]
            spot = float(row["close"])
            iv = float(row["iv"]) if not pd.isna(row["iv"]) else 0
            rv = float(row["rv"]) if not pd.isna(row["rv"]) else 0
            iv_rv = float(row["iv_rv_spread"]) if not pd.isna(row["iv_rv_spread"]) else 0

            daily_iv_rv.append({
                "date": date,
                "spot": spot,
                "iv": round(iv, 4),
                "rv": round(rv, 4),
                "iv_rv_spread": round(iv_rv, 4),
            })

            # ---- Check stop-loss on active positions ----
            closed_positions = []
            for pos_idx, pos in enumerate(active_positions):
                # Check stop-loss: if current strangle loss > stop_loss_multiple × premium
                current_pnl = strangle_pnl_at_expiry(spot, pos["k_put"], pos["k_call"], pos["premium"])
                if current_pnl < -pos["premium"] * cfg.stop_loss_multiple:
                    # Stop out
                    trade = pos["trade"]
                    trade.exit_date = date
                    trade.spot_exit = spot
                    fees = pos["notional"] * (cfg.option_fee + cfg.slippage) * 2
                    trade.pnl = current_pnl * pos["units"] - fees
                    trade.return_pct = trade.pnl / pos["notional"] * 100
                    trade.stopped_out = True
                    trade.reason = "stop_loss"
                    trades.append(trade)
                    equity += trade.pnl
                    closed_positions.append(pos_idx)
                    logger.debug(f"  Stop-loss: {date.date()} PnL=${trade.pnl:.0f}")

                # Check expiry
                elif (date - pos["entry_date"]).days >= cfg.expiry_days:
                    trade = pos["trade"]
                    trade.exit_date = date
                    trade.spot_exit = spot
                    final_pnl = strangle_pnl_at_expiry(spot, pos["k_put"], pos["k_call"], pos["premium"])
                    fees = pos["notional"] * (cfg.option_fee + cfg.slippage) * 2
                    trade.pnl = final_pnl * pos["units"] - fees
                    trade.return_pct = trade.pnl / pos["notional"] * 100
                    trade.reason = "expiry"
                    trades.append(trade)
                    equity += trade.pnl
                    closed_positions.append(pos_idx)

            # Remove closed positions (reverse order)
            for idx in sorted(closed_positions, reverse=True):
                active_positions.pop(idx)

            # ---- Entry signal ----
            if (
                iv_rv > cfg.iv_rv_threshold
                and rv > 0
                and len(active_positions) < cfg.max_positions
            ):
                # Size the position
                position_capital = equity * cfg.position_size_pct
                T = cfg.expiry_days / 365.0

                # Compute strikes: N standard deviations out
                daily_std = spot * rv / math.sqrt(365)
                period_std = daily_std * math.sqrt(cfg.expiry_days)
                k_put = spot - cfg.strike_width * period_std
                k_call = spot + cfg.strike_width * period_std

                # Ensure reasonable strikes
                k_put = max(k_put, spot * 0.7)
                k_call = min(k_call, spot * 1.5)

                # Compute premium
                premium_per_unit = strangle_premium(
                    S=spot, K_put=k_put, K_call=k_call,
                    T=T, r=cfg.risk_free_rate, sigma=iv,
                )

                if premium_per_unit <= 0:
                    continue

                # Apply fees/slippage to premium received
                net_premium_per_unit = premium_per_unit * (1 - cfg.option_fee - cfg.slippage)
                units = position_capital / spot
                notional = position_capital

                trade = TradeRecord(
                    entry_date=date,
                    spot_entry=spot,
                    k_put=round(k_put, 2),
                    k_call=round(k_call, 2),
                    iv_at_entry=round(iv, 4),
                    rv_at_entry=round(rv, 4),
                    iv_rv_spread=round(iv_rv, 4),
                    premium=round(net_premium_per_unit * units, 2),
                )

                active_positions.append({
                    "entry_date": date,
                    "k_put": k_put,
                    "k_call": k_call,
                    "premium": net_premium_per_unit,
                    "units": units,
                    "notional": notional,
                    "trade": trade,
                })

            equity_curve.append(equity)

        # Close any remaining open positions at last price
        final_spot = float(bt_df.iloc[-1]["close"])
        final_date = bt_df.iloc[-1]["open_time"]
        for pos in active_positions:
            trade = pos["trade"]
            trade.exit_date = final_date
            trade.spot_exit = final_spot
            final_pnl = strangle_pnl_at_expiry(final_spot, pos["k_put"], pos["k_call"], pos["premium"])
            fees = pos["notional"] * (cfg.option_fee + cfg.slippage) * 2
            trade.pnl = final_pnl * pos["units"] - fees
            trade.return_pct = trade.pnl / pos["notional"] * 100
            trade.reason = "end_of_data"
            trades.append(trade)
            equity += trade.pnl

        equity_curve.append(equity)

        # ==================== Compute Metrics ====================

        total_return = (equity - capital) / capital
        holding_days = (bt_df.iloc[-1]["open_time"] - bt_df.iloc[0]["open_time"]).total_seconds() / 86400
        ann_return = total_return * (365 / holding_days) if holding_days > 0 else 0

        # Trade stats
        winners = [t for t in trades if t.pnl > 0]
        losers = [t for t in trades if t.pnl <= 0]
        win_rate = len(winners) / len(trades) * 100 if trades else 0

        gross_profit = sum(t.pnl for t in winners) if winners else 0
        gross_loss = abs(sum(t.pnl for t in losers)) if losers else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        avg_win = np.mean([t.pnl for t in winners]) if winners else 0
        avg_loss = np.mean([abs(t.pnl) for t in losers]) if losers else 0
        avg_premium = np.mean([t.premium for t in trades]) if trades else 0
        ev = np.mean([t.pnl for t in trades]) if trades else 0

        # Sharpe from equity curve
        eq_arr = np.array(equity_curve)
        eq_returns = np.diff(eq_arr) / eq_arr[:-1]
        eq_returns = eq_returns[~np.isnan(eq_returns)]
        if len(eq_returns) > 1 and np.std(eq_returns) > 0:
            sharpe = np.mean(eq_returns) / np.std(eq_returns) * np.sqrt(365)
        else:
            sharpe = 0

        # Max drawdown
        running_max = np.maximum.accumulate(eq_arr)
        drawdown = (eq_arr - running_max) / running_max
        max_dd = float(drawdown.min()) * 100

        # IV-RV stats
        spreads = [d["iv_rv_spread"] for d in daily_iv_rv if d["iv_rv_spread"] != 0]
        signal_days = len([s for s in spreads if s > cfg.iv_rv_threshold])

        # Fill result
        result.total_return_pct = round(total_return * 100, 2)
        result.annualised_return_pct = round(ann_return * 100, 2)
        result.sharpe_ratio = round(sharpe, 3)
        result.max_drawdown_pct = round(max_dd, 2)
        result.total_trades = len(trades)
        result.winning_trades = len(winners)
        result.losing_trades = len(losers)
        result.win_rate_pct = round(win_rate, 1)
        result.avg_premium = round(float(avg_premium), 2)
        result.avg_loss = round(float(avg_loss), 2)
        result.avg_win = round(float(avg_win), 2)
        result.expected_value = round(float(ev), 2)
        result.profit_factor = round(profit_factor, 2)
        result.avg_iv_rv_spread = round(float(np.mean(spreads)), 4) if spreads else 0
        result.median_iv_rv_spread = round(float(np.median(spreads)), 4) if spreads else 0
        result.num_signal_days = signal_days
        result.start_date = bt_df.iloc[0]["open_time"]
        result.end_date = bt_df.iloc[-1]["open_time"]
        result.holding_days = round(holding_days, 1)
        result.equity_curve = equity_curve
        result.trades = trades
        result.daily_iv_rv = daily_iv_rv

        return result

    def print_report(self, result: IVRVResult):
        """Print formatted backtest report."""
        cfg = self.config
        r = result

        print(f"\n{'='*80}")
        print(f"  IV-RV SPREAD BACKTESTER — {r.underlying} Short Strangle")
        print(f"{'='*80}")
        print(f"  IV-RV Threshold: {cfg.iv_rv_threshold:.0%} | Strike Width: {cfg.strike_width}σ | Expiry: {cfg.expiry_days}d")
        print(f"  Stop Loss: {cfg.stop_loss_multiple}× premium | Max Positions: {cfg.max_positions}")
        print(f"  Capital: ${cfg.initial_capital:,.0f}")
        print(f"  Period: {r.start_date} → {r.end_date} ({r.holding_days:.0f} days)")
        print(f"{'─'*80}")

        print(f"\n  📊 PERFORMANCE")
        print(f"  {'Total Return:':<30} {r.total_return_pct:>8.2f}%")
        print(f"  {'Annualised Return:':<30} {r.annualised_return_pct:>8.2f}%")
        print(f"  {'Sharpe Ratio:':<30} {r.sharpe_ratio:>8.3f}")
        print(f"  {'Max Drawdown:':<30} {r.max_drawdown_pct:>8.2f}%")
        print(f"  {'Profit Factor:':<30} {r.profit_factor:>8.2f}")

        print(f"\n  📈 TRADE STATS")
        print(f"  {'Total Trades:':<30} {r.total_trades:>8d}")
        print(f"  {'Win Rate:':<30} {r.win_rate_pct:>8.1f}%")
        print(f"  {'Avg Win:':<30} ${r.avg_win:>10,.2f}")
        print(f"  {'Avg Loss:':<30} ${r.avg_loss:>10,.2f}")
        print(f"  {'Avg Premium:':<30} ${r.avg_premium:>10,.2f}")
        print(f"  {'Expected Value (per trade):':<30} ${r.expected_value:>10,.2f}")

        print(f"\n  📉 VOLATILITY")
        print(f"  {'Avg IV-RV Spread:':<30} {r.avg_iv_rv_spread:>8.2%}")
        print(f"  {'Median IV-RV Spread:':<30} {r.median_iv_rv_spread:>8.2%}")
        print(f"  {'Signal Days:':<30} {r.num_signal_days:>8d}")

        # Trade details (last 10)
        if r.trades:
            stopped = [t for t in r.trades if t.stopped_out]
            print(f"\n  ⚠️  Stopped Out: {len(stopped)} of {len(r.trades)} trades")

            print(f"\n  📋 RECENT TRADES")
            print(f"  {'Date':<12} {'Spot':>10} {'Put K':>10} {'Call K':>10} {'IV':>7} {'RV':>7} {'Spread':>8} {'PnL':>10} {'Reason':<12}")
            print(f"  {'─'*12} {'─'*10} {'─'*10} {'─'*10} {'─'*7} {'─'*7} {'─'*8} {'─'*10} {'─'*12}")
            for t in r.trades[-15:]:
                date_str = t.entry_date.strftime("%Y-%m-%d") if t.entry_date else "?"
                print(
                    f"  {date_str:<12} "
                    f"${t.spot_entry:>9,.0f} "
                    f"${t.k_put:>9,.0f} "
                    f"${t.k_call:>9,.0f} "
                    f"{t.iv_at_entry:>6.1%} "
                    f"{t.rv_at_entry:>6.1%} "
                    f"{t.iv_rv_spread:>7.1%} "
                    f"${t.pnl:>9,.0f} "
                    f"{t.reason:<12}"
                )

        print(f"\n{'='*80}\n")

    def generate_charts(self, result: IVRVResult, output_dir: str):
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
            f"IV-RV Spread — {r.underlying} | Threshold={cfg.iv_rv_threshold:.0%} | {cfg.strike_width}σ | {cfg.expiry_days}d",
            fontsize=16, color="white", fontweight="bold",
        )
        text_color = "#e0e0e0"

        # 1: Equity curve
        ax = axes[0, 0]
        ax.set_facecolor("#16213e")
        ax.plot(r.equity_curve, color="#00d4aa", linewidth=1.5)
        ax.axhline(y=cfg.initial_capital, color="white", linewidth=0.5, alpha=0.3, linestyle="--")
        ax.set_title("Equity Curve", color=text_color, fontsize=12)
        ax.set_xlabel("Day", color=text_color)
        ax.set_ylabel("Equity ($)", color=text_color)
        ax.tick_params(colors=text_color)

        # 2: IV vs RV
        ax = axes[0, 1]
        ax.set_facecolor("#16213e")
        if r.daily_iv_rv:
            dates = [d["date"] for d in r.daily_iv_rv]
            ivs = [d["iv"] for d in r.daily_iv_rv]
            rvs = [d["rv"] for d in r.daily_iv_rv]
            ax.plot(dates, ivs, color="#ffd700", linewidth=1.0, label="IV", alpha=0.9)
            ax.plot(dates, rvs, color="#00d4aa", linewidth=1.0, label="RV", alpha=0.9)
            ax.axhline(y=0, color="white", linewidth=0.3, alpha=0.3)
            ax.legend(facecolor="#16213e", edgecolor="#333", labelcolor=text_color)
        ax.set_title("IV vs RV", color=text_color, fontsize=12)
        ax.set_xlabel("Date", color=text_color)
        ax.set_ylabel("Volatility", color=text_color)
        ax.tick_params(colors=text_color)

        # 3: Trade PnL distribution
        ax = axes[1, 0]
        ax.set_facecolor("#16213e")
        if r.trades:
            pnls = [t.pnl for t in r.trades]
            colors_hist = ["#00d4aa" if p > 0 else "#ff4757" for p in pnls]
            ax.bar(range(len(pnls)), pnls, color=colors_hist, alpha=0.8)
            ax.axhline(y=0, color="white", linewidth=0.5, alpha=0.3)
        ax.set_title("Trade PnL", color=text_color, fontsize=12)
        ax.set_xlabel("Trade #", color=text_color)
        ax.set_ylabel("PnL ($)", color=text_color)
        ax.tick_params(colors=text_color)

        # 4: IV-RV spread distribution
        ax = axes[1, 1]
        ax.set_facecolor("#16213e")
        if r.daily_iv_rv:
            spreads = [d["iv_rv_spread"] for d in r.daily_iv_rv]
            ax.hist(spreads, bins=30, color="#00d4aa", alpha=0.8, edgecolor="#333")
            ax.axvline(x=cfg.iv_rv_threshold, color="#ffd700", linewidth=1.5, linestyle="--",
                      label=f"Threshold ({cfg.iv_rv_threshold:.0%})")
            ax.legend(facecolor="#16213e", edgecolor="#333", labelcolor=text_color)
        ax.set_title("IV-RV Spread Distribution", color=text_color, fontsize=12)
        ax.set_xlabel("IV - RV", color=text_color)
        ax.set_ylabel("Count", color=text_color)
        ax.tick_params(colors=text_color)

        plt.tight_layout()
        path = os.path.join(output_dir, f"iv_rv_{r.underlying}_{ts}.png")
        fig.savefig(path, dpi=150, facecolor=fig.get_facecolor())
        plt.close(fig)
        logger.info(f"Chart saved: {path}")
        return path


# ==================== CLI Entry ====================

async def run_backtest(
    underlying: str = "BTC",
    iv_rv_threshold: float = 0.10,
    strike_width: float = 1.0,
    expiry_days: int = 14,
    stop_loss_multiple: float = 2.0,
    start_date: str = "2025-01-01",
    end_date: str = "",
):
    """Main backtest entry point."""
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
        return None

    config = IVRVConfig(
        underlying=underlying,
        iv_rv_threshold=iv_rv_threshold,
        strike_width=strike_width,
        expiry_days=expiry_days,
        stop_loss_multiple=stop_loss_multiple,
        start_date=start_date,
        end_date=end_date,
    )

    bt = IVRVBacktester(db, config)

    logger.info(f"Loading spot data for {underlying}...")
    spot_df = await bt.load_spot_data()

    logger.info(f"Loading IV data for {underlying}...")
    iv_df = await bt.load_iv_data()

    logger.info(f"Running backtest...")
    result = bt.run_backtest(spot_df, iv_df)

    bt.print_report(result)

    output_dir = os.path.join(os.path.dirname(__file__), "output", "iv_rv")
    bt.generate_charts(result, output_dir)

    # Save trade log
    os.makedirs(output_dir, exist_ok=True)
    if result.trades:
        rows = []
        for t in result.trades:
            rows.append({
                "entry_date": t.entry_date,
                "exit_date": t.exit_date,
                "spot_entry": t.spot_entry,
                "spot_exit": t.spot_exit,
                "k_put": t.k_put,
                "k_call": t.k_call,
                "iv": t.iv_at_entry,
                "rv": t.rv_at_entry,
                "iv_rv_spread": t.iv_rv_spread,
                "premium": t.premium,
                "pnl": t.pnl,
                "return_pct": t.return_pct,
                "stopped_out": t.stopped_out,
                "reason": t.reason,
            })
        df = pd.DataFrame(rows)
        csv_path = os.path.join(
            output_dir,
            f"iv_rv_{underlying}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.csv"
        )
        df.to_csv(csv_path, index=False)
        logger.info(f"Trade log saved: {csv_path}")

    await db.close()
    return result


def main():
    parser = argparse.ArgumentParser(description="IV-RV Spread Backtester (Short Strangle)")
    parser.add_argument("--underlying", type=str, default="BTC", help="BTC or ETH")
    parser.add_argument("--threshold", type=float, default=0.10, help="IV-RV threshold (e.g. 0.10 = 10pp)")
    parser.add_argument("--width", type=float, default=1.0, help="Strike width in std devs")
    parser.add_argument("--expiry", type=int, default=14, help="Expiry days (7/14/30)")
    parser.add_argument("--stop-loss", type=float, default=2.0, help="Stop loss multiple of premium")
    parser.add_argument("--start", type=str, default="2025-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default="", help="End date YYYY-MM-DD (default: now)")
    args = parser.parse_args()

    asyncio.run(run_backtest(
        underlying=args.underlying,
        iv_rv_threshold=args.threshold,
        strike_width=args.width,
        expiry_days=args.expiry,
        stop_loss_multiple=args.stop_loss,
        start_date=args.start,
        end_date=args.end,
    ))


if __name__ == "__main__":
    main()
