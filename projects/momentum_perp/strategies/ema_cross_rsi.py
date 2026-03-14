"""
EMA Crossover + RSI Filter Strategy (OPTIMIZED)

Uses EMA crossovers for trend direction with RSI filter to avoid
overbought/oversold entries.

OPTIMIZED PARAMETERS (2024-03):
- Entry: Fast EMA (8) crosses slow EMA (26) - wider gap for stronger signals
- Filter: RSI(14) between 35-65 (stricter filter to avoid extremes)
- Exit: Opposite cross or TP/SL with 3:1 ratio
- Stop Loss: 2.5x ATR (wider to avoid whipsaws)
- Timeframe: 15m

Changes from baseline:
- fast_ema: 9 → 8 (faster response)
- slow_ema: 21 → 26 (wider separation = stronger signal)
- rsi_lower: 30 → 35 (stricter filter)
- rsi_upper: 70 → 65 (stricter filter)
- trail_atr_mult: 2.0 → 2.5 (wider stops)
- tp_sl_ratio: 2.0 → 3.0 (let winners run)
"""

from typing import List, Dict, Any, Optional
import numpy as np

from .base import BaseStrategy, Signal, SignalType


class EMACrossRSIStrategy(BaseStrategy):
    """
    EMA crossover strategy with RSI filter.
    
    Optimized Parameters:
        fast_ema: Fast EMA period (default: 8)
        slow_ema: Slow EMA period (default: 26)
        rsi_period: RSI period (default: 14)
        rsi_lower: Lower RSI bound for entries (default: 35)
        rsi_upper: Upper RSI bound for entries (default: 65)
        atr_period: ATR period for stops (default: 14)
        trail_atr_mult: Stop loss ATR multiplier (default: 2.5)
        tp_sl_ratio: Take profit to stop loss ratio (default: 3.0)
    """
    
    name = "ema_cross_rsi"
    timeframe = "15m"
    
    def _setup_params(self):
        # OPTIMIZED DEFAULTS
        self.fast_ema = self.params.get("fast_ema", 8)
        self.slow_ema = self.params.get("slow_ema", 26)
        self.rsi_period = self.params.get("rsi_period", 14)
        self.rsi_lower = self.params.get("rsi_lower", 35)
        self.rsi_upper = self.params.get("rsi_upper", 65)
        self.atr_period = self.params.get("atr_period", 14)
        self.trail_atr_mult = self.params.get("trail_atr_mult", 2.5)
        self.tp_sl_ratio = self.params.get("tp_sl_ratio", 3.0)
    
    def analyze(
        self,
        klines: List[Dict[str, Any]],
        current_position: Optional[Dict[str, Any]] = None,
        secondary_klines: Optional[List[Dict[str, Any]]] = None,
    ) -> Signal:
        """Analyze for EMA cross signals with RSI filter."""
        min_periods = max(self.slow_ema, self.rsi_period, self.atr_period) + 2
        
        if len(klines) < min_periods:
            return self.no_signal(klines[0].get("instrument", "unknown"))
        
        instrument = klines[-1].get("instrument", "unknown")
        opens, highs, lows, closes, volumes = self.extract_ohlcv(klines)
        
        # Calculate indicators
        fast_ema = self.ema(closes, self.fast_ema)
        slow_ema = self.ema(closes, self.slow_ema)
        rsi = self.rsi(closes, self.rsi_period)
        atr = self.atr(highs, lows, closes, self.atr_period)
        
        current_close = closes[-1]
        current_fast = fast_ema[-1]
        current_slow = slow_ema[-1]
        prev_fast = fast_ema[-2]
        prev_slow = slow_ema[-2]
        current_rsi = rsi[-1] if rsi else 50
        current_atr = atr[-1] if atr else 0
        
        if np.isnan(current_fast) or np.isnan(current_slow) or np.isnan(current_rsi):
            return self.no_signal(instrument)
        
        indicators = {
            "close": current_close,
            "fast_ema": current_fast,
            "slow_ema": current_slow,
            "rsi": current_rsi,
            "atr": current_atr,
            "ema_diff": current_fast - current_slow,
            "ema_diff_pct": ((current_fast - current_slow) / current_slow * 100) if current_slow != 0 else 0,
        }
        
        # Detect crossovers
        bullish_cross = prev_fast <= prev_slow and current_fast > current_slow
        bearish_cross = prev_fast >= prev_slow and current_fast < current_slow
        
        # Handle existing position
        if current_position:
            return self._check_exit(
                current_position, current_close, current_atr,
                bullish_cross, bearish_cross, current_rsi,
                instrument, indicators
            )
        
        # RSI filter - stricter to avoid overbought/oversold entries
        rsi_ok_for_long = self.rsi_lower < current_rsi < self.rsi_upper
        rsi_ok_for_short = self.rsi_lower < current_rsi < self.rsi_upper
        
        # Entry signals
        if bullish_cross and rsi_ok_for_long:
            stop_loss = current_close - (current_atr * self.trail_atr_mult)
            take_profit = current_close + (current_atr * self.trail_atr_mult * self.tp_sl_ratio)
            
            # Signal strength based on EMA separation and RSI
            ema_strength = min(1.0, abs(current_fast - current_slow) / current_atr) if current_atr > 0 else 0.5
            rsi_strength = 1 - abs(current_rsi - 50) / 50  # Best at RSI=50
            strength = (ema_strength + rsi_strength) / 2
            
            return Signal(
                signal_type=SignalType.ENTRY_LONG,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Bullish EMA cross (RSI: {current_rsi:.1f})"
            )
        
        elif bearish_cross and rsi_ok_for_short:
            stop_loss = current_close + (current_atr * self.trail_atr_mult)
            take_profit = current_close - (current_atr * self.trail_atr_mult * self.tp_sl_ratio)
            
            ema_strength = min(1.0, abs(current_fast - current_slow) / current_atr) if current_atr > 0 else 0.5
            rsi_strength = 1 - abs(current_rsi - 50) / 50
            strength = (ema_strength + rsi_strength) / 2
            
            return Signal(
                signal_type=SignalType.ENTRY_SHORT,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Bearish EMA cross (RSI: {current_rsi:.1f})"
            )
        
        return self.no_signal(instrument)
    
    def _check_exit(
        self,
        position: Dict[str, Any],
        current_price: float,
        atr: float,
        bullish_cross: bool,
        bearish_cross: bool,
        rsi: float,
        instrument: str,
        indicators: Dict
    ) -> Signal:
        """Check for exit signals on existing position."""
        side = position.get("side", "long")
        entry_price = position.get("avg_entry_price", current_price)
        
        # Exit on opposite crossover
        if side == "long" and bearish_cross:
            return Signal(
                signal_type=SignalType.EXIT_LONG,
                instrument=instrument,
                strength=0.8,
                indicators=indicators,
                notes="Exit long on bearish EMA cross"
            )
        
        elif side == "short" and bullish_cross:
            return Signal(
                signal_type=SignalType.EXIT_SHORT,
                instrument=instrument,
                strength=0.8,
                indicators=indicators,
                notes="Exit short on bullish EMA cross"
            )
        
        # RSI extreme exits - take profit at extremes
        if side == "long" and rsi > 75:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.7,
                indicators=indicators,
                notes=f"Take profit on overbought RSI ({rsi:.1f})"
            )
        
        elif side == "short" and rsi < 25:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.7,
                indicators=indicators,
                notes=f"Take profit on oversold RSI ({rsi:.1f})"
            )
        
        # Stop loss check using wider ATR multiplier
        if side == "long":
            if current_price < entry_price - (atr * self.trail_atr_mult):
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    instrument=instrument,
                    strength=1.0,
                    indicators=indicators,
                    notes="Stop loss hit"
                )
        else:
            if current_price > entry_price + (atr * self.trail_atr_mult):
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    instrument=instrument,
                    strength=1.0,
                    indicators=indicators,
                    notes="Stop loss hit"
                )
        
        return self.no_signal(instrument)
