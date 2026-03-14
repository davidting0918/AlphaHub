#!/usr/bin/env python3
"""
EMA Cross RSI Strategy Optimizer

Finds optimal parameters through grid search with realistic simulation.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from itertools import product
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Add project root
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from projects.momentum_perp.strategies.base import BaseStrategy, Signal, SignalType

# Simulation parameters
SLIPPAGE_PCT = 0.0005  # 0.05%
FEE_PCT = 0.0006  # 0.06% per side
POSITION_SIZE_PCT = 0.10  # 10% of capital
INITIAL_CAPITAL = 10000.0
WINDOW_SIZE = 100


@dataclass
class Trade:
    side: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    pnl: float
    pnl_pct: float
    exit_reason: str


@dataclass
class BacktestResult:
    params: Dict[str, Any]
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    
    @property
    def total_pnl_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        return ((self.equity_curve[-1] - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100
    
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
    def profit_factor(self) -> float:
        gross_profit = sum(t.pnl for t in self.trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return gross_profit / gross_loss
    
    @property
    def max_drawdown(self) -> float:
        if not self.equity_curve:
            return 0.0
        peak = self.equity_curve[0]
        max_dd = 0.0
        for eq in self.equity_curve:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100
    
    @property
    def sharpe_ratio(self) -> float:
        if len(self.trades) < 5:
            return -999.0
        returns = [t.pnl_pct for t in self.trades]
        mean_ret = np.mean(returns)
        std_ret = np.std(returns)
        if std_ret == 0:
            return 0.0
        # Annualized
        return (mean_ret / std_ret) * np.sqrt(252 * 4)  # ~4 trades per day avg
    
    @property
    def avg_trade_pnl(self) -> float:
        if not self.trades:
            return 0.0
        return np.mean([t.pnl_pct for t in self.trades])


class OptimizedEMACrossRSI:
    """
    Optimized EMA Cross RSI Strategy with trend filters.
    
    Parameters:
        fast_ema: Fast EMA period
        slow_ema: Slow EMA period
        rsi_period: RSI period
        rsi_lower: RSI lower bound for entry
        rsi_upper: RSI upper bound for entry
        atr_mult_sl: ATR multiplier for stop loss
        tp_sl_ratio: Take profit to stop loss ratio
        trend_ema: Trend EMA period (0 to disable)
        adx_period: ADX period (0 to disable)
        adx_threshold: ADX threshold for trend strength
        vol_mult: Volume multiplier filter (0 to disable)
        use_macd: Use MACD confirmation
    """
    
    def __init__(self, params: Dict[str, Any]):
        self.fast_ema = params.get('fast_ema', 9)
        self.slow_ema = params.get('slow_ema', 21)
        self.rsi_period = params.get('rsi_period', 14)
        self.rsi_lower = params.get('rsi_lower', 35)
        self.rsi_upper = params.get('rsi_upper', 65)
        self.atr_mult_sl = params.get('atr_mult_sl', 1.5)
        self.tp_sl_ratio = params.get('tp_sl_ratio', 2.0)
        self.atr_period = params.get('atr_period', 14)
        
        # Filters
        self.trend_ema = params.get('trend_ema', 50)  # 0 to disable
        self.adx_period = params.get('adx_period', 14)
        self.adx_threshold = params.get('adx_threshold', 20)
        self.vol_mult = params.get('vol_mult', 0)  # 0 to disable
        self.use_macd = params.get('use_macd', False)
        
        self.params = params
    
    @staticmethod
    def ema(data: List[float], period: int) -> List[float]:
        if len(data) < period:
            return [np.nan] * len(data)
        ema = [np.nan] * (period - 1)
        mult = 2 / (period + 1)
        ema.append(sum(data[:period]) / period)
        for price in data[period:]:
            ema.append((price - ema[-1]) * mult + ema[-1])
        return ema
    
    @staticmethod
    def sma(data: List[float], period: int) -> List[float]:
        if len(data) < period:
            return [np.nan] * len(data)
        sma = [np.nan] * (period - 1)
        for i in range(period - 1, len(data)):
            sma.append(sum(data[i - period + 1:i + 1]) / period)
        return sma
    
    @staticmethod
    def rsi(data: List[float], period: int = 14) -> List[float]:
        if len(data) < period + 1:
            return [np.nan] * len(data)
        deltas = [data[i] - data[i-1] for i in range(1, len(data))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        rsi_vals = [np.nan] * period
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        if avg_loss == 0:
            rsi_vals.append(100)
        else:
            rs = avg_gain / avg_loss
            rsi_vals.append(100 - (100 / (1 + rs)))
        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                rsi_vals.append(100)
            else:
                rs = avg_gain / avg_loss
                rsi_vals.append(100 - (100 / (1 + rs)))
        return rsi_vals
    
    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
        if len(highs) < period + 1:
            return [np.nan] * len(highs)
        tr = [highs[0] - lows[0]]
        for i in range(1, len(highs)):
            tr.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            ))
        atr_vals = [np.nan] * (period - 1)
        atr_vals.append(sum(tr[:period]) / period)
        for i in range(period, len(tr)):
            atr_vals.append((atr_vals[-1] * (period - 1) + tr[i]) / period)
        return atr_vals
    
    @staticmethod
    def adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Tuple[List[float], List[float], List[float]]:
        """Returns (ADX, +DI, -DI)"""
        if len(highs) < period * 2:
            nans = [np.nan] * len(highs)
            return nans, nans, nans
        
        plus_dm = [0.0]
        minus_dm = [0.0]
        tr = [highs[0] - lows[0]]
        
        for i in range(1, len(highs)):
            up_move = highs[i] - highs[i-1]
            down_move = lows[i-1] - lows[i]
            plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0)
            minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0)
            tr.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            ))
        
        def smooth(data, p):
            s = [sum(data[:p])]
            for i in range(p, len(data)):
                s.append(s[-1] - s[-1]/p + data[i])
            return s
        
        tr_s = smooth(tr, period)
        plus_dm_s = smooth(plus_dm, period)
        minus_dm_s = smooth(minus_dm, period)
        
        plus_di = [100 * p / t if t != 0 else 0 for p, t in zip(plus_dm_s, tr_s)]
        minus_di = [100 * m / t if t != 0 else 0 for m, t in zip(minus_dm_s, tr_s)]
        
        dx = [100 * abs(p - m) / (p + m) if (p + m) != 0 else 0 for p, m in zip(plus_di, minus_di)]
        
        adx_vals = [np.nan] * (period * 2 - 1)
        if len(dx) >= period:
            adx_vals.append(sum(dx[:period]) / period)
            for i in range(period, len(dx)):
                adx_vals.append((adx_vals[-1] * (period - 1) + dx[i]) / period)
        
        while len(adx_vals) < len(highs):
            adx_vals.insert(0, np.nan)
        while len(plus_di) < len(highs):
            plus_di.insert(0, np.nan)
        while len(minus_di) < len(highs):
            minus_di.insert(0, np.nan)
        
        return adx_vals, plus_di, minus_di
    
    @staticmethod
    def macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
        """Returns (MACD line, Signal line, Histogram)"""
        if len(closes) < slow + signal:
            nans = [np.nan] * len(closes)
            return nans, nans, nans
        
        fast_ema = OptimizedEMACrossRSI.ema(closes, fast)
        slow_ema = OptimizedEMACrossRSI.ema(closes, slow)
        
        macd_line = [f - s if not (np.isnan(f) or np.isnan(s)) else np.nan 
                     for f, s in zip(fast_ema, slow_ema)]
        
        valid_macd = [m for m in macd_line if not np.isnan(m)]
        signal_line_vals = OptimizedEMACrossRSI.ema(valid_macd, signal) if len(valid_macd) >= signal else [np.nan] * len(valid_macd)
        
        # Pad signal line
        signal_line = [np.nan] * (len(macd_line) - len(signal_line_vals)) + signal_line_vals
        
        histogram = [m - s if not (np.isnan(m) or np.isnan(s)) else np.nan 
                     for m, s in zip(macd_line, signal_line)]
        
        return macd_line, signal_line, histogram


def load_data(symbol: str, timeframe: str = "15m") -> List[Dict[str, Any]]:
    """Load parquet data for a symbol."""
    data_dir = Path(__file__).parent / "data"
    file_path = data_dir / f"{symbol}_USDT_SWAP_{timeframe}.parquet"
    
    if not file_path.exists():
        return []
    
    df = pd.read_parquet(file_path)
    
    klines = []
    for _, row in df.iterrows():
        klines.append({
            'open_time': row.get('open_time', row.get('ts', datetime.now(timezone.utc))),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row['volume']),
            'instrument': symbol
        })
    
    return klines


def run_backtest(strategy: OptimizedEMACrossRSI, all_klines: Dict[str, List[Dict]]) -> BacktestResult:
    """Run backtest across all symbols."""
    result = BacktestResult(params=strategy.params)
    equity = INITIAL_CAPITAL
    result.equity_curve.append(equity)
    
    for symbol, klines in all_klines.items():
        if len(klines) < WINDOW_SIZE + 50:
            continue
        
        position = None
        
        for i in range(WINDOW_SIZE, len(klines)):
            window = klines[i - WINDOW_SIZE:i]
            current = klines[i - 1]
            
            opens = [k['open'] for k in window]
            highs = [k['high'] for k in window]
            lows = [k['low'] for k in window]
            closes = [k['close'] for k in window]
            volumes = [k['volume'] for k in window]
            
            # Calculate indicators
            fast_ema = strategy.ema(closes, strategy.fast_ema)
            slow_ema = strategy.ema(closes, strategy.slow_ema)
            rsi = strategy.rsi(closes, strategy.rsi_period)
            atr = strategy.atr(highs, lows, closes, strategy.atr_period)
            
            current_close = closes[-1]
            current_high = highs[-1]
            current_low = lows[-1]
            current_fast = fast_ema[-1]
            current_slow = slow_ema[-1]
            prev_fast = fast_ema[-2]
            prev_slow = slow_ema[-2]
            current_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50
            current_atr = atr[-1] if len(atr) > 0 and not np.isnan(atr[-1]) else 0
            current_time = current.get('open_time', datetime.now(timezone.utc))
            
            if np.isnan(current_fast) or np.isnan(current_slow) or current_atr == 0:
                continue
            
            # Check position exit first
            if position:
                exit_signal = False
                exit_reason = ""
                exit_price = current_close
                
                # SL/TP check
                if position['side'] == 'long':
                    if current_low <= position['sl']:
                        exit_signal = True
                        exit_reason = "stop_loss"
                        exit_price = position['sl']
                    elif current_high >= position['tp']:
                        exit_signal = True
                        exit_reason = "take_profit"
                        exit_price = position['tp']
                else:
                    if current_high >= position['sl']:
                        exit_signal = True
                        exit_reason = "stop_loss"
                        exit_price = position['sl']
                    elif current_low <= position['tp']:
                        exit_signal = True
                        exit_reason = "take_profit"
                        exit_price = position['tp']
                
                # Opposite cross exit
                if not exit_signal:
                    bullish_cross = prev_fast <= prev_slow and current_fast > current_slow
                    bearish_cross = prev_fast >= prev_slow and current_fast < current_slow
                    
                    if position['side'] == 'long' and bearish_cross:
                        exit_signal = True
                        exit_reason = "signal_exit"
                    elif position['side'] == 'short' and bullish_cross:
                        exit_signal = True
                        exit_reason = "signal_exit"
                
                if exit_signal:
                    # Apply slippage
                    if position['side'] == 'long':
                        exit_price *= (1 - SLIPPAGE_PCT)
                        pnl = (exit_price - position['entry']) / position['entry']
                    else:
                        exit_price *= (1 + SLIPPAGE_PCT)
                        pnl = (position['entry'] - exit_price) / position['entry']
                    
                    # Apply fees
                    notional = INITIAL_CAPITAL * POSITION_SIZE_PCT
                    fees = notional * FEE_PCT * 2
                    net_pnl = (pnl * notional) - fees
                    pnl_pct = (net_pnl / notional) * 100
                    
                    result.trades.append(Trade(
                        side=position['side'],
                        entry_time=position['entry_time'],
                        exit_time=current_time,
                        entry_price=position['entry'],
                        exit_price=exit_price,
                        pnl=net_pnl,
                        pnl_pct=pnl_pct,
                        exit_reason=exit_reason
                    ))
                    
                    equity += net_pnl
                    result.equity_curve.append(equity)
                    position = None
                    continue
            
            # Entry signals
            if position is None:
                bullish_cross = prev_fast <= prev_slow and current_fast > current_slow
                bearish_cross = prev_fast >= prev_slow and current_fast < current_slow
                
                signal = None
                
                # RSI filter
                rsi_ok_long = strategy.rsi_lower < current_rsi < strategy.rsi_upper
                rsi_ok_short = strategy.rsi_lower < current_rsi < strategy.rsi_upper
                
                # Trend filter
                trend_ok_long = True
                trend_ok_short = True
                if strategy.trend_ema > 0:
                    trend_ema = strategy.ema(closes, strategy.trend_ema)
                    if len(trend_ema) > 0 and not np.isnan(trend_ema[-1]):
                        trend_ok_long = current_close > trend_ema[-1]
                        trend_ok_short = current_close < trend_ema[-1]
                
                # ADX filter
                adx_ok = True
                if strategy.adx_threshold > 0:
                    adx_vals, plus_di, minus_di = strategy.adx(highs, lows, closes, strategy.adx_period)
                    if len(adx_vals) > 0 and not np.isnan(adx_vals[-1]):
                        adx_ok = adx_vals[-1] > strategy.adx_threshold
                
                # Volume filter
                vol_ok = True
                if strategy.vol_mult > 0:
                    vol_sma = strategy.sma(volumes, 20)
                    if len(vol_sma) > 0 and not np.isnan(vol_sma[-1]) and vol_sma[-1] > 0:
                        vol_ok = volumes[-1] > vol_sma[-1] * strategy.vol_mult
                
                # MACD filter
                macd_ok_long = True
                macd_ok_short = True
                if strategy.use_macd:
                    macd_line, signal_line, hist = strategy.macd(closes)
                    if len(hist) > 1 and not np.isnan(hist[-1]) and not np.isnan(hist[-2]):
                        macd_ok_long = hist[-1] > hist[-2]  # MACD histogram rising
                        macd_ok_short = hist[-1] < hist[-2]  # MACD histogram falling
                
                if bullish_cross and rsi_ok_long and trend_ok_long and adx_ok and vol_ok and macd_ok_long:
                    signal = 'long'
                elif bearish_cross and rsi_ok_short and trend_ok_short and adx_ok and vol_ok and macd_ok_short:
                    signal = 'short'
                
                if signal:
                    entry_price = current_close * (1 + SLIPPAGE_PCT if signal == 'long' else 1 - SLIPPAGE_PCT)
                    
                    if signal == 'long':
                        sl = entry_price - (current_atr * strategy.atr_mult_sl)
                        tp = entry_price + (current_atr * strategy.atr_mult_sl * strategy.tp_sl_ratio)
                    else:
                        sl = entry_price + (current_atr * strategy.atr_mult_sl)
                        tp = entry_price - (current_atr * strategy.atr_mult_sl * strategy.tp_sl_ratio)
                    
                    position = {
                        'side': signal,
                        'entry': entry_price,
                        'entry_time': current_time,
                        'sl': sl,
                        'tp': tp
                    }
    
    return result


def optimize():
    """Run parameter optimization."""
    print("=" * 60)
    print("EMA Cross RSI Strategy Optimizer")
    print("=" * 60)
    
    # Load all data
    symbols = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK']
    all_klines = {}
    
    print("\nLoading data...")
    for sym in symbols:
        klines = load_data(sym, "15m")
        if klines:
            all_klines[sym] = klines
            print(f"  {sym}: {len(klines)} candles")
    
    print(f"\nLoaded {len(all_klines)} symbols")
    
    # First run baseline
    print("\n" + "-" * 40)
    print("BASELINE (current params)")
    print("-" * 40)
    
    baseline_params = {
        'fast_ema': 9,
        'slow_ema': 21,
        'rsi_period': 14,
        'rsi_lower': 30,
        'rsi_upper': 70,
        'atr_mult_sl': 2.0,
        'tp_sl_ratio': 2.0,
        'trend_ema': 0,
        'adx_threshold': 0,
        'vol_mult': 0,
        'use_macd': False
    }
    
    baseline_strat = OptimizedEMACrossRSI(baseline_params)
    baseline_result = run_backtest(baseline_strat, all_klines)
    
    print(f"Trades: {baseline_result.num_trades}")
    print(f"Win Rate: {baseline_result.win_rate:.1f}%")
    print(f"Total PnL: {baseline_result.total_pnl_pct:.2f}%")
    print(f"Max DD: {baseline_result.max_drawdown:.2f}%")
    print(f"Sharpe: {baseline_result.sharpe_ratio:.2f}")
    print(f"Profit Factor: {baseline_result.profit_factor:.2f}")
    
    # Parameter grid
    param_grid = {
        'fast_ema': [8, 9, 12],
        'slow_ema': [21, 26, 30],
        'rsi_period': [14],
        'rsi_lower': [35, 40, 45],
        'rsi_upper': [55, 60, 65],
        'atr_mult_sl': [1.5, 2.0, 2.5],
        'tp_sl_ratio': [1.5, 2.0, 2.5, 3.0],
        'trend_ema': [0, 50, 100],
        'adx_threshold': [0, 20, 25],
        'vol_mult': [0, 1.2, 1.5],
        'use_macd': [False, True]
    }
    
    # Generate all combinations
    keys = list(param_grid.keys())
    combinations = list(product(*[param_grid[k] for k in keys]))
    
    print(f"\nTesting {len(combinations)} parameter combinations...")
    
    results = []
    best_result = None
    best_score = -float('inf')
    
    for idx, combo in enumerate(combinations):
        params = dict(zip(keys, combo))
        
        # Skip invalid combinations
        if params['fast_ema'] >= params['slow_ema']:
            continue
        if params['rsi_lower'] >= params['rsi_upper']:
            continue
        
        strategy = OptimizedEMACrossRSI(params)
        result = run_backtest(strategy, all_klines)
        
        # Score: prioritize profit factor and win rate, with min trade requirement
        if result.num_trades >= 10:
            score = result.profit_factor * (result.win_rate / 100) * min(result.sharpe_ratio + 2, 5)
            if result.total_pnl_pct < 0:
                score *= 0.5
        else:
            score = -999
        
        results.append({
            **params,
            'trades': result.num_trades,
            'win_rate': result.win_rate,
            'pnl_pct': result.total_pnl_pct,
            'max_dd': result.max_drawdown,
            'sharpe': result.sharpe_ratio,
            'profit_factor': result.profit_factor,
            'avg_trade': result.avg_trade_pnl,
            'score': score
        })
        
        if score > best_score:
            best_score = score
            best_result = result
        
        if (idx + 1) % 500 == 0:
            print(f"  Processed {idx + 1}/{len(combinations)} combinations...")
    
    # Sort by score
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('score', ascending=False)
    
    # Save results
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    results_df.to_csv(results_dir / "optimization_results.csv", index=False)
    
    print("\n" + "=" * 60)
    print("TOP 10 PARAMETER SETS")
    print("=" * 60)
    
    top10 = results_df.head(10)
    for i, row in top10.iterrows():
        print(f"\n#{results_df.index.get_loc(i) + 1}:")
        print(f"  Fast EMA: {row['fast_ema']}, Slow EMA: {row['slow_ema']}")
        print(f"  RSI: {row['rsi_period']} ({row['rsi_lower']}-{row['rsi_upper']})")
        print(f"  ATR SL: {row['atr_mult_sl']}, TP:SL: {row['tp_sl_ratio']}")
        print(f"  Trend EMA: {row['trend_ema']}, ADX: {row['adx_threshold']}, Vol: {row['vol_mult']}, MACD: {row['use_macd']}")
        print(f"  Results: {row['trades']} trades, {row['win_rate']:.1f}% WR, {row['pnl_pct']:.2f}% PnL")
        print(f"  Sharpe: {row['sharpe']:.2f}, PF: {row['profit_factor']:.2f}, Score: {row['score']:.2f}")
    
    # Get best params
    best_row = results_df.iloc[0]
    best_params = {
        'fast_ema': int(best_row['fast_ema']),
        'slow_ema': int(best_row['slow_ema']),
        'rsi_period': int(best_row['rsi_period']),
        'rsi_lower': int(best_row['rsi_lower']),
        'rsi_upper': int(best_row['rsi_upper']),
        'atr_mult_sl': float(best_row['atr_mult_sl']),
        'tp_sl_ratio': float(best_row['tp_sl_ratio']),
        'trend_ema': int(best_row['trend_ema']),
        'adx_threshold': int(best_row['adx_threshold']),
        'vol_mult': float(best_row['vol_mult']),
        'use_macd': bool(best_row['use_macd'])
    }
    
    print("\n" + "=" * 60)
    print("BEST PARAMETERS")
    print("=" * 60)
    print(f"\nfast_ema: {best_params['fast_ema']}")
    print(f"slow_ema: {best_params['slow_ema']}")
    print(f"rsi_period: {best_params['rsi_period']}")
    print(f"rsi_lower: {best_params['rsi_lower']}")
    print(f"rsi_upper: {best_params['rsi_upper']}")
    print(f"atr_mult_sl: {best_params['atr_mult_sl']}")
    print(f"tp_sl_ratio: {best_params['tp_sl_ratio']}")
    print(f"trend_ema: {best_params['trend_ema']}")
    print(f"adx_threshold: {best_params['adx_threshold']}")
    print(f"vol_mult: {best_params['vol_mult']}")
    print(f"use_macd: {best_params['use_macd']}")
    
    # Run final comparison
    print("\n" + "-" * 40)
    print("BEFORE vs AFTER")
    print("-" * 40)
    
    optimized_strat = OptimizedEMACrossRSI(best_params)
    optimized_result = run_backtest(optimized_strat, all_klines)
    
    print(f"\n{'Metric':<20} {'Before':<15} {'After':<15} {'Change':<15}")
    print("-" * 65)
    print(f"{'Trades':<20} {baseline_result.num_trades:<15} {optimized_result.num_trades:<15}")
    print(f"{'Win Rate':<20} {baseline_result.win_rate:.1f}%{'':<10} {optimized_result.win_rate:.1f}%{'':<10} {optimized_result.win_rate - baseline_result.win_rate:+.1f}%")
    print(f"{'Total PnL':<20} {baseline_result.total_pnl_pct:.2f}%{'':<9} {optimized_result.total_pnl_pct:.2f}%{'':<9} {optimized_result.total_pnl_pct - baseline_result.total_pnl_pct:+.2f}%")
    print(f"{'Max Drawdown':<20} {baseline_result.max_drawdown:.2f}%{'':<9} {optimized_result.max_drawdown:.2f}%")
    print(f"{'Sharpe Ratio':<20} {baseline_result.sharpe_ratio:.2f}{'':<12} {optimized_result.sharpe_ratio:.2f}")
    print(f"{'Profit Factor':<20} {baseline_result.profit_factor:.2f}{'':<12} {optimized_result.profit_factor:.2f}")
    
    # Generate comparison chart
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('EMA Cross RSI Strategy Optimization Results', fontsize=14, fontweight='bold')
    
    # Equity curves
    ax1 = axes[0, 0]
    ax1.plot(baseline_result.equity_curve, label='Before (Original)', color='red', alpha=0.7)
    ax1.plot(optimized_result.equity_curve, label='After (Optimized)', color='green', alpha=0.7)
    ax1.axhline(y=INITIAL_CAPITAL, color='gray', linestyle='--', alpha=0.5)
    ax1.set_title('Equity Curve Comparison')
    ax1.set_xlabel('Trade #')
    ax1.set_ylabel('Equity ($)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Win rate comparison
    ax2 = axes[0, 1]
    metrics = ['Win Rate (%)', 'Total PnL (%)', 'Profit Factor']
    before_vals = [baseline_result.win_rate, baseline_result.total_pnl_pct, baseline_result.profit_factor]
    after_vals = [optimized_result.win_rate, optimized_result.total_pnl_pct, optimized_result.profit_factor]
    
    x = np.arange(len(metrics))
    width = 0.35
    ax2.bar(x - width/2, before_vals, width, label='Before', color='red', alpha=0.7)
    ax2.bar(x + width/2, after_vals, width, label='After', color='green', alpha=0.7)
    ax2.set_xticks(x)
    ax2.set_xticklabels(metrics)
    ax2.set_title('Key Metrics Comparison')
    ax2.legend()
    ax2.axhline(y=0, color='gray', linestyle='-', alpha=0.3)
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Trade distribution
    ax3 = axes[1, 0]
    if optimized_result.trades:
        trade_pnls = [t.pnl_pct for t in optimized_result.trades]
        colors = ['green' if p > 0 else 'red' for p in trade_pnls]
        ax3.bar(range(len(trade_pnls)), trade_pnls, color=colors, alpha=0.7)
        ax3.axhline(y=0, color='gray', linestyle='-', alpha=0.5)
        ax3.set_title('Optimized Strategy: Trade PnL Distribution')
        ax3.set_xlabel('Trade #')
        ax3.set_ylabel('PnL (%)')
        ax3.grid(True, alpha=0.3, axis='y')
    
    # Parameters text
    ax4 = axes[1, 1]
    ax4.axis('off')
    param_text = f"""OPTIMIZED PARAMETERS
    
Fast EMA: {best_params['fast_ema']}
Slow EMA: {best_params['slow_ema']}
RSI Period: {best_params['rsi_period']}
RSI Range: {best_params['rsi_lower']}-{best_params['rsi_upper']}
ATR SL Multiplier: {best_params['atr_mult_sl']}
TP:SL Ratio: {best_params['tp_sl_ratio']}
Trend EMA: {best_params['trend_ema']} {'(disabled)' if best_params['trend_ema'] == 0 else ''}
ADX Threshold: {best_params['adx_threshold']} {'(disabled)' if best_params['adx_threshold'] == 0 else ''}
Volume Filter: {best_params['vol_mult']}x {'(disabled)' if best_params['vol_mult'] == 0 else ''}
MACD Confirm: {best_params['use_macd']}

RESULTS
Trades: {optimized_result.num_trades}
Win Rate: {optimized_result.win_rate:.1f}%
Total PnL: {optimized_result.total_pnl_pct:.2f}%
Max Drawdown: {optimized_result.max_drawdown:.2f}%
Sharpe Ratio: {optimized_result.sharpe_ratio:.2f}
Profit Factor: {optimized_result.profit_factor:.2f}
"""
    ax4.text(0.1, 0.9, param_text, transform=ax4.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(results_dir / "optimized_comparison.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"\n✓ Results saved to {results_dir / 'optimization_results.csv'}")
    print(f"✓ Chart saved to {results_dir / 'optimized_comparison.png'}")
    
    return best_params, baseline_result, optimized_result


if __name__ == "__main__":
    optimize()
