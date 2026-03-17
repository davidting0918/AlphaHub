"""Momentum trading backtesting engine."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import numpy as np
import pandas as pd

from config import (
    INITIAL_CAPITAL, POSITION_SIZE_PCT, TRADING_FEE, SLIPPAGE, STRATEGY_PARAMS
)
from indicators import calculate_all_indicators


@dataclass
class Trade:
    """Represents a single trade."""
    symbol: str
    strategy: str
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    position_size: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    fees: float = 0.0
    bars_held: int = 0
    exit_reason: str = ""
    
    @property
    def is_closed(self) -> bool:
        return self.exit_time is not None


@dataclass
class BacktestResult:
    """Results from backtesting a strategy."""
    strategy: str
    symbol: str
    trades: list[Trade] = field(default_factory=list)
    equity_curve: pd.Series = field(default_factory=pd.Series)
    
    @property
    def total_trades(self) -> int:
        return len([t for t in self.trades if t.is_closed])
    
    @property
    def winning_trades(self) -> int:
        return len([t for t in self.trades if t.is_closed and t.pnl > 0])
    
    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades
    
    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades if t.is_closed)
    
    @property
    def total_fees(self) -> float:
        return sum(t.fees for t in self.trades if t.is_closed)
    
    @property
    def avg_pnl(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.total_pnl / self.total_trades
    
    @property
    def avg_holding_period(self) -> float:
        closed = [t for t in self.trades if t.is_closed]
        if not closed:
            return 0.0
        return sum(t.bars_held for t in closed) / len(closed)
    
    @property
    def profit_factor(self) -> float:
        gains = sum(t.pnl for t in self.trades if t.is_closed and t.pnl > 0)
        losses = abs(sum(t.pnl for t in self.trades if t.is_closed and t.pnl < 0))
        if losses == 0:
            return float('inf') if gains > 0 else 0.0
        return gains / losses
    
    @property
    def max_drawdown(self) -> float:
        if self.equity_curve.empty:
            return 0.0
        peak = self.equity_curve.expanding().max()
        drawdown = (self.equity_curve - peak) / peak
        return abs(drawdown.min())
    
    @property
    def sharpe_ratio(self) -> float:
        if self.equity_curve.empty or len(self.equity_curve) < 2:
            return 0.0
        returns = self.equity_curve.pct_change().dropna()
        if returns.std() == 0:
            return 0.0
        # Annualized for 4h bars (~1460 per year)
        return (returns.mean() / returns.std()) * np.sqrt(1460)


class Backtester:
    """Main backtesting engine."""
    
    def __init__(self, initial_capital: float = INITIAL_CAPITAL):
        self.initial_capital = initial_capital
        self.results: dict[str, list[BacktestResult]] = {}
    
    def run_strategy(self, df: pd.DataFrame, symbol: str, strategy: str) -> BacktestResult:
        """Run a single strategy on a symbol."""
        df = calculate_all_indicators(df)
        
        strategy_funcs = {
            "volume_breakout": self._strategy_volume_breakout,
            "rsi_momentum": self._strategy_rsi_momentum,
            "vwap_breakout": self._strategy_vwap_breakout,
            "obv_divergence": self._strategy_obv_divergence,
            "multi_factor": self._strategy_multi_factor,
        }
        
        if strategy not in strategy_funcs:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        return strategy_funcs[strategy](df, symbol, strategy)
    
    def _calculate_position_size(self, capital: float, entry_price: float) -> float:
        """Calculate position size in units."""
        position_value = capital * POSITION_SIZE_PCT
        return position_value / entry_price
    
    def _apply_slippage(self, price: float, is_entry: bool) -> float:
        """Apply slippage to price."""
        if is_entry:
            return price * (1 + SLIPPAGE)  # Pay more on entry
        return price * (1 - SLIPPAGE)  # Get less on exit
    
    def _calculate_fees(self, value: float) -> float:
        """Calculate trading fees."""
        return value * TRADING_FEE
    
    def _simulate_trades(
        self,
        df: pd.DataFrame,
        symbol: str,
        strategy: str,
        signals: pd.Series,
        exit_func
    ) -> BacktestResult:
        """Simulate trades based on entry signals and exit logic."""
        trades = []
        equity = [self.initial_capital]
        capital = self.initial_capital
        position: Optional[Trade] = None
        
        for i in range(1, len(df)):
            current_time = df.index[i]
            current_bar = df.iloc[i]
            prev_bar = df.iloc[i-1]
            
            # Check for exit if in position
            if position is not None:
                should_exit, exit_reason = exit_func(df, i, position)
                
                if should_exit:
                    exit_price = self._apply_slippage(current_bar['open'], is_entry=False)
                    position.exit_time = current_time
                    position.exit_price = exit_price
                    position.bars_held = i - df.index.get_loc(position.entry_time)
                    
                    # Calculate PnL
                    exit_value = position.position_size * exit_price
                    entry_value = position.position_size * position.entry_price
                    exit_fee = self._calculate_fees(exit_value)
                    position.pnl = exit_value - entry_value - position.fees - exit_fee
                    position.fees += exit_fee
                    position.pnl_pct = position.pnl / entry_value
                    position.exit_reason = exit_reason
                    
                    capital += position.pnl
                    trades.append(position)
                    position = None
            
            # Check for entry if not in position (use previous bar's signal)
            if position is None and i > 0 and signals.iloc[i-1]:
                entry_price = self._apply_slippage(current_bar['open'], is_entry=True)
                position_size = self._calculate_position_size(capital, entry_price)
                entry_fee = self._calculate_fees(position_size * entry_price)
                
                position = Trade(
                    symbol=symbol,
                    strategy=strategy,
                    entry_time=current_time,
                    entry_price=entry_price,
                    position_size=position_size,
                    fees=entry_fee
                )
            
            equity.append(capital + (
                (position.position_size * current_bar['close'] - position.position_size * position.entry_price)
                if position else 0
            ))
        
        # Close any remaining position at end
        if position is not None:
            exit_price = self._apply_slippage(df.iloc[-1]['close'], is_entry=False)
            position.exit_time = df.index[-1]
            position.exit_price = exit_price
            position.bars_held = len(df) - df.index.get_loc(position.entry_time)
            
            exit_value = position.position_size * exit_price
            entry_value = position.position_size * position.entry_price
            exit_fee = self._calculate_fees(exit_value)
            position.pnl = exit_value - entry_value - position.fees - exit_fee
            position.fees += exit_fee
            position.pnl_pct = position.pnl / entry_value
            position.exit_reason = "end_of_data"
            trades.append(position)
        
        equity_curve = pd.Series(equity, index=df.index[:len(equity)])
        
        return BacktestResult(
            strategy=strategy,
            symbol=symbol,
            trades=trades,
            equity_curve=equity_curve
        )
    
    def _strategy_volume_breakout(self, df: pd.DataFrame, symbol: str, strategy: str) -> BacktestResult:
        """Volume Breakout Momentum strategy."""
        params = STRATEGY_PARAMS["volume_breakout"]
        
        # Entry signal: Volume > 3x average AND price > 20-period high
        signals = (
            (df['volume_ratio'] > params['volume_mult']) &
            (df['close'] > df['high_20'].shift(1))
        )
        
        def exit_logic(df: pd.DataFrame, i: int, trade: Trade) -> tuple[bool, str]:
            current = df.iloc[i]
            entry_idx = df.index.get_loc(trade.entry_time)
            bars_held = i - entry_idx
            
            # Max hold
            if bars_held >= params['max_hold']:
                return True, "max_hold"
            
            # Trailing stop: price dropped below entry - 2*ATR
            atr = df.iloc[i]['atr_14']
            trailing_stop = trade.entry_price - params['atr_mult'] * atr
            if current['low'] < trailing_stop:
                return True, "trailing_stop"
            
            return False, ""
        
        return self._simulate_trades(df, symbol, strategy, signals, exit_logic)
    
    def _strategy_rsi_momentum(self, df: pd.DataFrame, symbol: str, strategy: str) -> BacktestResult:
        """RSI Momentum strategy."""
        params = STRATEGY_PARAMS["rsi_momentum"]
        
        # Entry: RSI crosses above 60, price > EMA50, volume above average
        rsi_cross = (df['rsi_14'] > params['rsi_entry']) & (df['rsi_14'].shift(1) <= params['rsi_entry'])
        signals = (
            rsi_cross &
            (df['close'] > df['ema_50']) &
            (df['volume_ratio'] > 1.0)
        )
        
        def exit_logic(df: pd.DataFrame, i: int, trade: Trade) -> tuple[bool, str]:
            current = df.iloc[i]
            
            # RSI drops below 45
            if current['rsi_14'] < params['rsi_exit']:
                return True, "rsi_exit"
            
            # Trailing stop
            atr = current['atr_14']
            trailing_stop = trade.entry_price - params['atr_mult'] * atr
            if current['low'] < trailing_stop:
                return True, "trailing_stop"
            
            return False, ""
        
        return self._simulate_trades(df, symbol, strategy, signals, exit_logic)
    
    def _strategy_vwap_breakout(self, df: pd.DataFrame, symbol: str, strategy: str) -> BacktestResult:
        """VWAP Breakout + Volume Surge strategy."""
        params = STRATEGY_PARAMS["vwap_breakout"]
        
        # Entry: Price > VWAP + ATR AND volume > 2x average
        signals = (
            (df['close'] > df['vwap_20'] + params['atr_mult'] * df['atr_14']) &
            (df['volume_ratio'] > params['volume_mult'])
        )
        
        def exit_logic(df: pd.DataFrame, i: int, trade: Trade) -> tuple[bool, str]:
            current = df.iloc[i]
            entry_idx = df.index.get_loc(trade.entry_time)
            bars_held = i - entry_idx
            
            # Max hold
            if bars_held >= params['max_hold']:
                return True, "max_hold"
            
            # Price below VWAP
            if current['close'] < current['vwap_20']:
                return True, "below_vwap"
            
            return False, ""
        
        return self._simulate_trades(df, symbol, strategy, signals, exit_logic)
    
    def _strategy_obv_divergence(self, df: pd.DataFrame, symbol: str, strategy: str) -> BacktestResult:
        """OBV Divergence + Momentum strategy."""
        params = STRATEGY_PARAMS["obv_divergence"]
        
        # Entry: OBV new high while price consolidating, then price breaks out
        obv_new_high = df['obv'] >= df['obv_high_20']
        price_consolidating = (df['high'] < df['range_high_10']) & (df['low'] > df['range_low_10'])
        price_breakout = df['close'] > df['range_high_10']
        
        # Signal when we see accumulation (OBV high + consolidation) then breakout
        signals = obv_new_high.shift(1) & price_breakout
        
        def exit_logic(df: pd.DataFrame, i: int, trade: Trade) -> tuple[bool, str]:
            current = df.iloc[i]
            prev = df.iloc[i-1] if i > 0 else current
            
            # OBV declining
            if current['obv'] < current['obv_sma_10'] and prev['obv'] >= prev['obv_sma_10']:
                return True, "obv_decline"
            
            # Trailing stop
            atr = current['atr_14']
            trailing_stop = trade.entry_price - 2 * atr
            if current['low'] < trailing_stop:
                return True, "trailing_stop"
            
            return False, ""
        
        return self._simulate_trades(df, symbol, strategy, signals, exit_logic)
    
    def _strategy_multi_factor(self, df: pd.DataFrame, symbol: str, strategy: str) -> BacktestResult:
        """Multi-Factor Momentum Score strategy."""
        params = STRATEGY_PARAMS["multi_factor"]
        
        # Calculate momentum score (0-1 for each factor)
        def normalize(series: pd.Series, low: float, high: float) -> pd.Series:
            return ((series - low) / (high - low)).clip(0, 1)
        
        # ROC score (positive = good)
        roc_score = normalize(df['roc_10'], -10, 20)
        
        # RSI score (50-80 = good momentum zone)
        rsi_score = normalize(df['rsi_14'], 40, 80)
        
        # Volume ratio score
        vol_score = normalize(df['volume_ratio'], 0.5, 3.0)
        
        # Price vs EMA score
        price_ema_pct = (df['close'] - df['ema_20']) / df['ema_20'] * 100
        price_score = normalize(price_ema_pct, -5, 10)
        
        # Combined score
        df['momentum_score'] = (roc_score + rsi_score + vol_score + price_score) / 4
        
        # Entry: Score > 0.7
        signals = df['momentum_score'] > params['entry_threshold']
        
        def exit_logic(df: pd.DataFrame, i: int, trade: Trade) -> tuple[bool, str]:
            current = df.iloc[i]
            
            # Score drops below 0.3
            if current['momentum_score'] < params['exit_threshold']:
                return True, "score_low"
            
            return False, ""
        
        return self._simulate_trades(df, symbol, strategy, signals, exit_logic)
    
    def run_all_strategies(self, df: pd.DataFrame, symbol: str) -> dict[str, BacktestResult]:
        """Run all strategies on a symbol."""
        strategies = ["volume_breakout", "rsi_momentum", "vwap_breakout", "obv_divergence", "multi_factor"]
        results = {}
        
        for strategy in strategies:
            try:
                result = self.run_strategy(df, symbol, strategy)
                results[strategy] = result
            except Exception as e:
                print(f"Error running {strategy} on {symbol}: {e}")
        
        return results


def aggregate_results(all_results: dict[str, dict[str, BacktestResult]]) -> pd.DataFrame:
    """Aggregate results across all symbols and strategies."""
    rows = []
    
    for symbol, strategy_results in all_results.items():
        for strategy, result in strategy_results.items():
            rows.append({
                "symbol": symbol,
                "strategy": strategy,
                "total_trades": result.total_trades,
                "win_rate": result.win_rate,
                "total_pnl": result.total_pnl,
                "avg_pnl": result.avg_pnl,
                "total_fees": result.total_fees,
                "avg_holding_period": result.avg_holding_period,
                "profit_factor": result.profit_factor,
                "max_drawdown": result.max_drawdown,
                "sharpe_ratio": result.sharpe_ratio,
            })
    
    return pd.DataFrame(rows)


def strategy_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize results by strategy."""
    return df.groupby("strategy").agg({
        "total_trades": "sum",
        "win_rate": "mean",
        "total_pnl": "sum",
        "avg_pnl": "mean",
        "total_fees": "sum",
        "profit_factor": "mean",
        "max_drawdown": "mean",
        "sharpe_ratio": "mean",
    }).round(4)


def top_pairs_by_strategy(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Find top performing pairs for each strategy."""
    return df.sort_values(["strategy", "total_pnl"], ascending=[True, False]).groupby("strategy").head(n)
