"""
Backtester Module

Walks through historical klines candle-by-candle, feeds windows to strategies,
and simulates trades with realistic assumptions.
"""

import sys
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Type
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from projects.momentum_perp.strategies.base import BaseStrategy, Signal, SignalType
from projects.momentum_perp.strategies.breakout_momentum import BreakoutMomentumStrategy
from projects.momentum_perp.strategies.ema_cross_rsi import EMACrossRSIStrategy
from projects.momentum_perp.strategies.vwap_deviation import VWAPDeviationStrategy
from projects.momentum_perp.strategies.multi_tf_trend import MultiTimeframeTrendStrategy
from projects.momentum_perp.strategies.volume_profile import VolumeProfileMomentumStrategy


# Trading assumptions
SLIPPAGE_PCT = 0.0005  # 0.05%
FEE_PCT = 0.0006  # 0.06% per side
POSITION_SIZE_PCT = 0.10  # 10% of capital
INITIAL_CAPITAL = 10000.0
WINDOW_SIZE = 100  # Candles to feed to strategy


@dataclass
class Trade:
    """Record of a completed trade."""
    strategy: str
    symbol: str
    side: str  # "long" or "short"
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    size: float
    pnl: float
    pnl_pct: float
    exit_reason: str
    fees: float


@dataclass
class Position:
    """Active position."""
    side: str
    entry_time: datetime
    entry_price: float
    size: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


@dataclass
class BacktestResult:
    """Results from backtesting a strategy on a symbol."""
    strategy: str
    symbol: str
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    
    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)
    
    @property
    def total_pnl_pct(self) -> float:
        return (self.total_pnl / INITIAL_CAPITAL) * 100
    
    @property
    def num_trades(self) -> int:
        return len(self.trades)
    
    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.pnl > 0)
        return (wins / len(self.trades)) * 100
    
    @property
    def max_drawdown(self) -> float:
        if not self.equity_curve:
            return 0.0
        peak = self.equity_curve[0]
        max_dd = 0.0
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100
    
    @property
    def sharpe_ratio(self) -> float:
        if len(self.trades) < 2:
            return 0.0
        returns = [t.pnl_pct for t in self.trades]
        mean_return = np.mean(returns)
        std_return = np.std(returns)
        if std_return == 0:
            return 0.0
        # Annualized (assuming ~250 trading days, ~5 trades/day avg)
        return (mean_return / std_return) * np.sqrt(252 * 5)


def apply_slippage(price: float, side: str, is_entry: bool) -> float:
    """Apply slippage to price."""
    if side == "long":
        # Long entry: buy higher, exit: sell lower
        return price * (1 + SLIPPAGE_PCT) if is_entry else price * (1 - SLIPPAGE_PCT)
    else:
        # Short entry: sell lower, exit: buy higher
        return price * (1 - SLIPPAGE_PCT) if is_entry else price * (1 + SLIPPAGE_PCT)


def calculate_pnl(entry_price: float, exit_price: float, size: float, side: str) -> tuple:
    """Calculate PnL and fees for a trade."""
    if side == "long":
        gross_pnl = (exit_price - entry_price) * size / entry_price * INITIAL_CAPITAL * POSITION_SIZE_PCT
    else:
        gross_pnl = (entry_price - exit_price) * size / entry_price * INITIAL_CAPITAL * POSITION_SIZE_PCT
    
    notional = INITIAL_CAPITAL * POSITION_SIZE_PCT
    fees = notional * FEE_PCT * 2  # Entry + exit
    
    net_pnl = gross_pnl - fees
    pnl_pct = (net_pnl / (INITIAL_CAPITAL * POSITION_SIZE_PCT)) * 100
    
    return net_pnl, pnl_pct, fees


def check_sl_tp(position: Position, current_high: float, current_low: float) -> tuple:
    """Check if SL or TP was hit. Returns (hit, exit_price, reason)."""
    if position.side == "long":
        if position.stop_loss and current_low <= position.stop_loss:
            return True, position.stop_loss, "stop_loss"
        if position.take_profit and current_high >= position.take_profit:
            return True, position.take_profit, "take_profit"
    else:
        if position.stop_loss and current_high >= position.stop_loss:
            return True, position.stop_loss, "stop_loss"
        if position.take_profit and current_low <= position.take_profit:
            return True, position.take_profit, "take_profit"
    return False, 0.0, ""


def backtest_strategy(
    strategy: BaseStrategy,
    klines: List[Dict[str, Any]],
    secondary_klines: Optional[List[Dict[str, Any]]] = None,
    symbol: str = "unknown"
) -> BacktestResult:
    """
    Backtest a strategy on historical klines.
    
    Args:
        strategy: Strategy instance to test
        klines: Primary timeframe klines (sorted oldest first)
        secondary_klines: Secondary timeframe klines for multi-TF strategies
        symbol: Symbol being tested
    
    Returns:
        BacktestResult with trades and equity curve
    """
    result = BacktestResult(strategy=strategy.name, symbol=symbol)
    
    if len(klines) < WINDOW_SIZE:
        return result
    
    position: Optional[Position] = None
    equity = INITIAL_CAPITAL
    result.equity_curve.append(equity)
    
    # For multi-TF strategies, we need to track which secondary kline to use
    secondary_idx = 0
    
    # Walk through klines starting after we have enough for a window
    for i in range(WINDOW_SIZE, len(klines)):
        window = klines[i - WINDOW_SIZE:i]
        current_kline = klines[i - 1]  # Last complete candle
        current_time = current_kline.get('open_time', datetime.now(timezone.utc))
        current_close = current_kline['close']
        current_high = current_kline['high']
        current_low = current_kline['low']
        
        # Build secondary window for multi-TF strategies
        secondary_window = None
        if secondary_klines and strategy.secondary_timeframe:
            # Find secondary klines up to current time
            secondary_window = [
                k for k in secondary_klines 
                if k['open_time'] <= current_time
            ][-WINDOW_SIZE:] if secondary_klines else None
            
            # Need enough secondary data
            if not secondary_window or len(secondary_window) < 50:
                continue
        
        # Check SL/TP for existing position
        if position:
            sl_tp_hit, exit_price, reason = check_sl_tp(position, current_high, current_low)
            if sl_tp_hit:
                # Close position
                exit_price = apply_slippage(exit_price, position.side, is_entry=False)
                pnl, pnl_pct, fees = calculate_pnl(
                    position.entry_price, exit_price, 1.0, position.side
                )
                
                trade = Trade(
                    strategy=strategy.name,
                    symbol=symbol,
                    side=position.side,
                    entry_time=position.entry_time,
                    exit_time=current_time,
                    entry_price=position.entry_price,
                    exit_price=exit_price,
                    size=1.0,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    exit_reason=reason,
                    fees=fees
                )
                result.trades.append(trade)
                equity += pnl
                position = None
        
        # Get signal from strategy
        current_position_dict = None
        if position:
            current_position_dict = {
                'side': position.side,
                'avg_entry_price': position.entry_price
            }
        
        try:
            signal = strategy.analyze(
                klines=window,
                current_position=current_position_dict,
                secondary_klines=secondary_window
            )
        except Exception as e:
            continue
        
        # Process signal
        if signal.signal_type == SignalType.NO_ACTION:
            pass
        
        elif signal.is_entry and position is None:
            # Open new position
            entry_price = apply_slippage(current_close, signal.side, is_entry=True)
            position = Position(
                side=signal.side,
                entry_time=current_time,
                entry_price=entry_price,
                size=1.0,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit
            )
        
        elif signal.is_exit and position:
            # Strategy signals exit
            exit_price = apply_slippage(current_close, position.side, is_entry=False)
            pnl, pnl_pct, fees = calculate_pnl(
                position.entry_price, exit_price, 1.0, position.side
            )
            
            reason = "signal_exit"
            if signal.signal_type == SignalType.STOP_LOSS:
                reason = "stop_loss"
            elif signal.signal_type == SignalType.TAKE_PROFIT:
                reason = "take_profit"
            
            trade = Trade(
                strategy=strategy.name,
                symbol=symbol,
                side=position.side,
                entry_time=position.entry_time,
                exit_time=current_time,
                entry_price=position.entry_price,
                exit_price=exit_price,
                size=1.0,
                pnl=pnl,
                pnl_pct=pnl_pct,
                exit_reason=reason,
                fees=fees
            )
            result.trades.append(trade)
            equity += pnl
            position = None
        
        result.equity_curve.append(equity)
    
    # Close any remaining position at end
    if position and len(klines) > 0:
        current_close = klines[-1]['close']
        current_time = klines[-1].get('open_time', datetime.now(timezone.utc))
        exit_price = apply_slippage(current_close, position.side, is_entry=False)
        pnl, pnl_pct, fees = calculate_pnl(
            position.entry_price, exit_price, 1.0, position.side
        )
        
        trade = Trade(
            strategy=strategy.name,
            symbol=symbol,
            side=position.side,
            entry_time=position.entry_time,
            exit_time=current_time,
            entry_price=position.entry_price,
            exit_price=exit_price,
            size=1.0,
            pnl=pnl,
            pnl_pct=pnl_pct,
            exit_reason="end_of_backtest",
            fees=fees
        )
        result.trades.append(trade)
        equity += pnl
        result.equity_curve.append(equity)
    
    return result


# Strategy registry with their required timeframes
STRATEGIES = {
    "breakout_momentum": {
        "class": BreakoutMomentumStrategy,
        "primary_tf": "1H",
        "secondary_tf": None
    },
    "ema_cross_rsi": {
        "class": EMACrossRSIStrategy,
        "primary_tf": "15m",
        "secondary_tf": None
    },
    "vwap_deviation": {
        "class": VWAPDeviationStrategy,
        "primary_tf": "5m",
        "secondary_tf": None
    },
    "multi_tf_trend": {
        "class": MultiTimeframeTrendStrategy,
        "primary_tf": "15m",
        "secondary_tf": "4H"
    },
    "volume_profile": {
        "class": VolumeProfileMomentumStrategy,
        "primary_tf": "1H",
        "secondary_tf": None
    }
}


def get_strategy_instance(name: str) -> BaseStrategy:
    """Get strategy instance by name."""
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {name}")
    return STRATEGIES[name]["class"]()


def get_strategy_timeframes(name: str) -> tuple:
    """Get required timeframes for a strategy."""
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {name}")
    return STRATEGIES[name]["primary_tf"], STRATEGIES[name]["secondary_tf"]
