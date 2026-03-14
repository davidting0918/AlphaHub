"""
Momentum Scanner — Phase 3: Strategy Backtester

Backtests momentum strategies on detected price jumps:
- Entry: market order at next candle open after jump detected
- Exit strategies:
  a) Fixed TP/SL (take-profit / stop-loss)
  b) Time-based exit (hold for N candles)
  c) Trailing stop
- Tracks: win rate, avg return, max drawdown, Sharpe, total PnL
- Assumes 0.04% taker fee per trade (Binance perps)
"""

import os
import sys
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from fetch_data import load_kline_data, get_available_symbols, DATA_DIR
from scanner import load_jumps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────
TAKER_FEE = 0.0004  # 0.04% per trade (entry + exit = 0.08% round trip)
INITIAL_CAPITAL = 10_000  # starting capital
POSITION_SIZE_PCT = 0.02  # 2% of capital per trade
MAX_CONCURRENT = 10  # max concurrent positions

RESULTS_DIR = os.path.join(DATA_DIR, "backtest_results")


# ── Strategy Definitions ────────────────────────────────────────
@dataclass
class StrategyConfig:
    """Defines a single strategy variant."""
    name: str
    exit_type: str  # "tp_sl", "time", "trailing"
    take_profit: float = 0.0  # as decimal (0.03 = 3%)
    stop_loss: float = 0.0
    hold_candles: int = 0
    trailing_pct: float = 0.0

    @property
    def label(self) -> str:
        if self.exit_type == "tp_sl":
            return f"TP{self.take_profit*100:.0f}_SL{self.stop_loss*100:.0f}"
        elif self.exit_type == "time":
            return f"Hold{self.hold_candles}"
        elif self.exit_type == "trailing":
            return f"Trail{self.trailing_pct*100:.0f}"
        return self.name


def get_strategy_variants() -> List[StrategyConfig]:
    """Generate all strategy variants to test."""
    strategies = []

    # Fixed TP/SL combinations
    for tp in [0.02, 0.03, 0.05]:
        for sl in [0.01, 0.02, 0.03]:
            strategies.append(StrategyConfig(
                name=f"TP{tp*100:.0f}_SL{sl*100:.0f}",
                exit_type="tp_sl",
                take_profit=tp,
                stop_loss=sl,
            ))

    # Time-based exits
    for hold in [6, 12, 24, 48]:
        strategies.append(StrategyConfig(
            name=f"Hold{hold}",
            exit_type="time",
            hold_candles=hold,
        ))

    # Trailing stops
    for trail in [0.01, 0.02]:
        strategies.append(StrategyConfig(
            name=f"Trail{trail*100:.0f}",
            exit_type="trailing",
            trailing_pct=trail,
        ))

    return strategies


@dataclass
class Trade:
    """Single trade record."""
    symbol: str
    direction: str  # "long" or "short"
    entry_time_ms: int
    entry_price: float
    exit_time_ms: int = 0
    exit_price: float = 0.0
    pnl_pct: float = 0.0
    pnl_usd: float = 0.0
    exit_reason: str = ""
    position_size: float = 0.0
    fees_paid: float = 0.0
    jump_type: str = ""
    jump_change_pct: float = 0.0


@dataclass
class BacktestResult:
    """Result for one strategy variant."""
    strategy: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    avg_return_pct: float = 0.0
    median_return_pct: float = 0.0
    total_pnl_usd: float = 0.0
    total_pnl_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    profit_factor: float = 0.0
    avg_holding_candles: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    hourly_stats: Dict = field(default_factory=dict)


class MomentumBacktester:
    """Backtests momentum strategies on price jump signals."""

    def __init__(
        self,
        initial_capital: float = INITIAL_CAPITAL,
        position_size_pct: float = POSITION_SIZE_PCT,
        taker_fee: float = TAKER_FEE,
    ):
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.taker_fee = taker_fee
        self._kline_cache: Dict[str, pd.DataFrame] = {}

    def _get_klines(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get kline data for symbol (cached)."""
        if symbol not in self._kline_cache:
            df = load_kline_data(symbol)
            if df is not None and not df.empty:
                df = df.sort_values("open_time_ms").reset_index(drop=True)
                self._kline_cache[symbol] = df
            else:
                self._kline_cache[symbol] = None
        return self._kline_cache[symbol]

    def _simulate_trade_tp_sl(
        self, klines: pd.DataFrame, entry_idx: int,
        direction: str, tp: float, sl: float
    ) -> Tuple[int, float, str]:
        """
        Simulate a TP/SL trade.
        Entry at klines.iloc[entry_idx].open.
        Scan subsequent candles for TP or SL hit.
        Returns (exit_idx, exit_price, reason).
        """
        entry_price = klines.iloc[entry_idx]["open"]
        max_candles = min(200, len(klines) - entry_idx)  # cap lookback

        for i in range(entry_idx, entry_idx + max_candles):
            row = klines.iloc[i]
            high = row["high"]
            low = row["low"]

            if direction == "long":
                # Check SL first (within same candle, SL takes priority if both hit)
                if (entry_price - low) / entry_price >= sl:
                    return i, entry_price * (1 - sl), "stop_loss"
                if (high - entry_price) / entry_price >= tp:
                    return i, entry_price * (1 + tp), "take_profit"
            else:  # short
                if (high - entry_price) / entry_price >= sl:
                    return i, entry_price * (1 + sl), "stop_loss"
                if (entry_price - low) / entry_price >= tp:
                    return i, entry_price * (1 - tp), "take_profit"

        # Max candles reached — exit at last close
        exit_idx = min(entry_idx + max_candles - 1, len(klines) - 1)
        return exit_idx, klines.iloc[exit_idx]["close"], "max_candles"

    def _simulate_trade_time(
        self, klines: pd.DataFrame, entry_idx: int,
        direction: str, hold_candles: int
    ) -> Tuple[int, float, str]:
        """Simulate a time-based exit trade."""
        exit_idx = min(entry_idx + hold_candles, len(klines) - 1)
        exit_price = klines.iloc[exit_idx]["close"]
        return exit_idx, exit_price, "time_exit"

    def _simulate_trade_trailing(
        self, klines: pd.DataFrame, entry_idx: int,
        direction: str, trail_pct: float
    ) -> Tuple[int, float, str]:
        """Simulate a trailing stop trade."""
        entry_price = klines.iloc[entry_idx]["open"]
        max_candles = min(200, len(klines) - entry_idx)

        if direction == "long":
            peak = entry_price
            for i in range(entry_idx, entry_idx + max_candles):
                row = klines.iloc[i]
                peak = max(peak, row["high"])
                trail_level = peak * (1 - trail_pct)
                if row["low"] <= trail_level:
                    return i, trail_level, "trailing_stop"
        else:  # short
            trough = entry_price
            for i in range(entry_idx, entry_idx + max_candles):
                row = klines.iloc[i]
                trough = min(trough, row["low"])
                trail_level = trough * (1 + trail_pct)
                if row["high"] >= trail_level:
                    return i, trail_level, "trailing_stop"

        exit_idx = min(entry_idx + max_candles - 1, len(klines) - 1)
        return exit_idx, klines.iloc[exit_idx]["close"], "max_candles"

    def backtest_strategy(
        self, jumps: pd.DataFrame, strategy: StrategyConfig
    ) -> BacktestResult:
        """
        Backtest a single strategy variant across all jumps.
        """
        result = BacktestResult(strategy=strategy.name)

        capital = self.initial_capital
        equity_curve = [capital]
        trades = []

        # Sort jumps by time
        jumps_sorted = jumps.sort_values("open_time_ms").reset_index(drop=True)

        # Track active positions to prevent overlap per symbol
        active_positions: Dict[str, int] = {}  # symbol → exit_time_ms

        for _, jump in jumps_sorted.iterrows():
            symbol = jump["symbol"]

            # Skip if we have an active position in this symbol
            if symbol in active_positions:
                if jump["open_time_ms"] <= active_positions[symbol]:
                    continue

            klines = self._get_klines(symbol)
            if klines is None:
                continue

            # Find the entry candle (next candle after jump)
            entry_time_ms = jump["open_time_ms"] + (5 * 60 * 1000)  # next 5m candle
            entry_mask = klines["open_time_ms"] >= entry_time_ms
            if not entry_mask.any():
                continue

            entry_idx = entry_mask.idxmax()
            if entry_idx >= len(klines) - 2:
                continue

            entry_price = klines.iloc[entry_idx]["open"]
            if entry_price <= 0:
                continue

            # Determine direction
            direction = "long" if jump["direction"] == "up" else "short"

            # Simulate trade based on exit type
            if strategy.exit_type == "tp_sl":
                exit_idx, exit_price, reason = self._simulate_trade_tp_sl(
                    klines, entry_idx, direction,
                    strategy.take_profit, strategy.stop_loss
                )
            elif strategy.exit_type == "time":
                exit_idx, exit_price, reason = self._simulate_trade_time(
                    klines, entry_idx, direction,
                    strategy.hold_candles
                )
            elif strategy.exit_type == "trailing":
                exit_idx, exit_price, reason = self._simulate_trade_trailing(
                    klines, entry_idx, direction,
                    strategy.trailing_pct
                )
            else:
                continue

            # Calculate PnL
            if direction == "long":
                raw_return = (exit_price - entry_price) / entry_price
            else:
                raw_return = (entry_price - exit_price) / entry_price

            # Deduct fees (entry + exit)
            fees = self.taker_fee * 2
            net_return = raw_return - fees

            position_size = capital * self.position_size_pct
            pnl_usd = position_size * net_return
            fees_usd = position_size * fees

            # Update capital
            capital += pnl_usd
            equity_curve.append(capital)

            # Record trade
            exit_time_ms = int(klines.iloc[exit_idx]["open_time_ms"])
            holding_candles = exit_idx - entry_idx

            trade = Trade(
                symbol=symbol,
                direction=direction,
                entry_time_ms=int(klines.iloc[entry_idx]["open_time_ms"]),
                entry_price=entry_price,
                exit_time_ms=exit_time_ms,
                exit_price=exit_price,
                pnl_pct=round(net_return * 100, 4),
                pnl_usd=round(pnl_usd, 2),
                exit_reason=reason,
                position_size=position_size,
                fees_paid=round(fees_usd, 4),
                jump_type=jump["jump_type"],
                jump_change_pct=jump["price_change_pct"],
            )
            trades.append(trade)
            active_positions[symbol] = exit_time_ms

        # Compute metrics
        result.trades = trades
        result.equity_curve = equity_curve
        result.total_trades = len(trades)

        if not trades:
            return result

        returns = np.array([t.pnl_pct for t in trades])
        pnl_usds = np.array([t.pnl_usd for t in trades])

        result.winning_trades = int(np.sum(returns > 0))
        result.losing_trades = int(np.sum(returns <= 0))
        result.win_rate = result.winning_trades / result.total_trades * 100
        result.avg_return_pct = float(np.mean(returns))
        result.median_return_pct = float(np.median(returns))
        result.total_pnl_usd = float(np.sum(pnl_usds))
        result.total_pnl_pct = (capital - self.initial_capital) / self.initial_capital * 100
        result.best_trade_pct = float(np.max(returns))
        result.worst_trade_pct = float(np.min(returns))

        # Max drawdown
        eq = np.array(equity_curve)
        running_max = np.maximum.accumulate(eq)
        drawdowns = (eq - running_max) / running_max
        result.max_drawdown_pct = float(drawdowns.min() * 100)

        # Sharpe ratio (annualized, assume ~105k 5m candles per year)
        if len(returns) > 1 and np.std(returns) > 0:
            # Average trades per day ≈ total_trades / trading_days
            trading_days = (trades[-1].exit_time_ms - trades[0].entry_time_ms) / (86400 * 1000)
            trades_per_day = len(trades) / max(trading_days, 1)
            daily_return = np.mean(returns) * trades_per_day
            daily_std = np.std(returns) * np.sqrt(trades_per_day)
            result.sharpe_ratio = round((daily_return / daily_std) * np.sqrt(365), 2)
        else:
            result.sharpe_ratio = 0.0

        # Profit factor
        gross_profit = float(np.sum(pnl_usds[pnl_usds > 0]))
        gross_loss = float(np.abs(np.sum(pnl_usds[pnl_usds < 0])))
        result.profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

        # Average holding time
        holding_times = [(t.exit_time_ms - t.entry_time_ms) / (5 * 60 * 1000) for t in trades]
        result.avg_holding_candles = float(np.mean(holding_times))

        # Hourly stats
        hourly = {}
        for t in trades:
            h = pd.Timestamp(t.entry_time_ms, unit="ms", tz="UTC").hour
            if h not in hourly:
                hourly[h] = {"trades": 0, "wins": 0, "total_pnl": 0}
            hourly[h]["trades"] += 1
            if t.pnl_pct > 0:
                hourly[h]["wins"] += 1
            hourly[h]["total_pnl"] += t.pnl_pct
        for h in hourly:
            hourly[h]["win_rate"] = round(hourly[h]["wins"] / hourly[h]["trades"] * 100, 1)
        result.hourly_stats = hourly

        return result

    def run_all_strategies(
        self, jumps: Optional[pd.DataFrame] = None,
        strategies: Optional[List[StrategyConfig]] = None,
    ) -> List[BacktestResult]:
        """Run all strategy variants and return results."""
        if jumps is None:
            jumps = load_jumps()
            if jumps is None or jumps.empty:
                logger.error("No jumps data found. Run scanner.py first.")
                return []

        if strategies is None:
            strategies = get_strategy_variants()

        logger.info(f"Backtesting {len(strategies)} strategy variants on {len(jumps)} jumps...")
        results = []

        for i, strat in enumerate(strategies, 1):
            logger.info(f"  [{i}/{len(strategies)}] {strat.name}...")
            result = self.backtest_strategy(jumps, strat)
            results.append(result)

            logger.info(
                f"    Trades: {result.total_trades} | "
                f"Win: {result.win_rate:.1f}% | "
                f"PnL: ${result.total_pnl_usd:,.0f} ({result.total_pnl_pct:.1f}%) | "
                f"Sharpe: {result.sharpe_ratio:.2f} | "
                f"MaxDD: {result.max_drawdown_pct:.1f}%"
            )

        # Sort by total PnL
        results.sort(key=lambda r: r.total_pnl_usd, reverse=True)

        # Save results
        self._save_results(results)
        self._print_summary(results)

        return results

    def _save_results(self, results: List[BacktestResult]):
        """Save backtest results to files."""
        os.makedirs(RESULTS_DIR, exist_ok=True)

        # Summary CSV
        rows = []
        for r in results:
            rows.append({
                "strategy": r.strategy,
                "total_trades": r.total_trades,
                "win_rate": round(r.win_rate, 1),
                "avg_return_pct": round(r.avg_return_pct, 4),
                "median_return_pct": round(r.median_return_pct, 4),
                "total_pnl_usd": round(r.total_pnl_usd, 2),
                "total_pnl_pct": round(r.total_pnl_pct, 2),
                "max_drawdown_pct": round(r.max_drawdown_pct, 2),
                "sharpe_ratio": r.sharpe_ratio,
                "profit_factor": r.profit_factor,
                "avg_holding_candles": round(r.avg_holding_candles, 1),
                "best_trade_pct": round(r.best_trade_pct, 2),
                "worst_trade_pct": round(r.worst_trade_pct, 2),
                "winning_trades": r.winning_trades,
                "losing_trades": r.losing_trades,
            })
        pd.DataFrame(rows).to_csv(
            os.path.join(RESULTS_DIR, "strategy_summary.csv"), index=False
        )

        # Detailed trades for each strategy
        for r in results:
            if r.trades:
                trade_rows = [asdict(t) for t in r.trades]
                pd.DataFrame(trade_rows).to_csv(
                    os.path.join(RESULTS_DIR, f"trades_{r.strategy}.csv"),
                    index=False,
                )

        # Equity curves
        eq_data = {}
        for r in results:
            eq_data[r.strategy] = r.equity_curve
        # Save as JSON for visualization
        with open(os.path.join(RESULTS_DIR, "equity_curves.json"), "w") as f:
            json.dump(eq_data, f)

        # Hourly stats
        hourly_data = {}
        for r in results:
            hourly_data[r.strategy] = r.hourly_stats
        with open(os.path.join(RESULTS_DIR, "hourly_stats.json"), "w") as f:
            json.dump(hourly_data, f)

        logger.info(f"Results saved to {RESULTS_DIR}")

    def _print_summary(self, results: List[BacktestResult]):
        """Print backtest comparison table."""
        logger.info(f"\n{'='*100}")
        logger.info(f"MOMENTUM STRATEGY BACKTEST — COMPARISON")
        logger.info(f"Capital: ${self.initial_capital:,.0f} | Position: {self.position_size_pct*100:.0f}% | Fee: {self.taker_fee*100:.2f}%/trade")
        logger.info(f"{'='*100}")
        logger.info(
            f"{'Strategy':<16} {'Trades':>7} {'WinRate':>8} {'AvgRet':>8} "
            f"{'PnL($)':>10} {'PnL(%)':>8} {'MaxDD':>8} {'Sharpe':>8} {'PF':>6}"
        )
        logger.info(f"{'─'*16} {'─'*7} {'─'*8} {'─'*8} {'─'*10} {'─'*8} {'─'*8} {'─'*8} {'─'*6}")

        for r in results:
            pf_str = f"{r.profit_factor:.2f}" if r.profit_factor != float("inf") else "∞"
            logger.info(
                f"{r.strategy:<16} {r.total_trades:>7} {r.win_rate:>7.1f}% "
                f"{r.avg_return_pct:>7.3f}% "
                f"${r.total_pnl_usd:>9,.0f} {r.total_pnl_pct:>7.1f}% "
                f"{r.max_drawdown_pct:>7.1f}% {r.sharpe_ratio:>8.2f} {pf_str:>6}"
            )

        # Best strategy
        if results and results[0].total_trades > 0:
            best = results[0]
            logger.info(f"\n🏆 Best Strategy: {best.strategy}")
            logger.info(f"   PnL: ${best.total_pnl_usd:,.0f} ({best.total_pnl_pct:.1f}%)")
            logger.info(f"   Win Rate: {best.win_rate:.1f}% | Sharpe: {best.sharpe_ratio:.2f}")
        logger.info(f"{'='*100}")


def load_backtest_results() -> Optional[pd.DataFrame]:
    """Load the strategy summary CSV."""
    path = os.path.join(RESULTS_DIR, "strategy_summary.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


def load_equity_curves() -> Optional[Dict]:
    """Load equity curves JSON."""
    path = os.path.join(RESULTS_DIR, "equity_curves.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def load_hourly_stats() -> Optional[Dict]:
    """Load hourly stats JSON."""
    path = os.path.join(RESULTS_DIR, "hourly_stats.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


if __name__ == "__main__":
    bt = MomentumBacktester()
    results = bt.run_all_strategies()
