"""
EMA Crossover + RSI Filter Strategy

Uses EMA crossovers for trend direction with RSI filter to avoid
overbought/oversold entries.

- Entry: Fast EMA (9) crosses slow EMA (21)
- Filter: RSI(14) between 30-70 (avoid extremes)
- Exit: Opposite cross or trailing stop
- Timeframe: 15m
"""

from typing import List, Dict, Any, Optional
import numpy as np

from .base import BaseStrategy, Signal, SignalType


class EMACrossRSIStrategy(BaseStrategy):
    """
    EMA crossover strategy with RSI filter.
    
    Parameters:
        fast_ema: Fast EMA period (default: 9)
        slow_ema: Slow EMA period (default: 21)
        rsi_period: RSI period (default: 14)
        rsi_lower: Lower RSI bound for entries (default: 30)
        rsi_upper: Upper RSI bound for entries (default: 70)
        atr_period: ATR period for stops (default: 14)
        trail_atr_mult: Trailing stop ATR multiplier (default: 2.0)
    """
    
    name = "ema_cross_rsi"
    timeframe = "15m"
    
    def _setup_params(self):
        self.fast_ema = self.params.get("fast_ema", 9)
        self.slow_ema = self.params.get("slow_ema", 21)
        self.rsi_period = self.params.get("rsi_period", 14)
        self.rsi_lower = self.params.get("rsi_lower", 30)
        self.rsi_upper = self.params.get("rsi_upper", 70)
        self.atr_period = self.params.get("atr_period", 14)
        self.trail_atr_mult = self.params.get("trail_atr_mult", 2.0)
    
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
        
        # RSI filter - avoid overbought/oversold entries
        rsi_ok_for_long = self.rsi_lower < current_rsi < self.rsi_upper
        rsi_ok_for_short = self.rsi_lower < current_rsi < self.rsi_upper
        
        # Entry signals
        if bullish_cross and rsi_ok_for_long:
            stop_loss = current_close - (current_atr * self.trail_atr_mult)
            take_profit = current_close + (current_atr * self.trail_atr_mult * 2)
            
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
            take_profit = current_close - (current_atr * self.trail_atr_mult * 2)
            
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
        
        # RSI extreme exits
        if side == "long" and rsi > 80:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.7,
                indicators=indicators,
                notes=f"Take profit on overbought RSI ({rsi:.1f})"
            )
        
        elif side == "short" and rsi < 20:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.7,
                indicators=indicators,
                notes=f"Take profit on oversold RSI ({rsi:.1f})"
            )
        
        # Trailing stop check
        if side == "long":
            trailing_stop = current_price - (atr * self.trail_atr_mult)
            if current_price < entry_price - (atr * self.trail_atr_mult):
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    instrument=instrument,
                    strength=1.0,
                    indicators=indicators,
                    notes="Trailing stop hit"
                )
        else:
            if current_price > entry_price + (atr * self.trail_atr_mult):
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    instrument=instrument,
                    strength=1.0,
                    indicators=indicators,
                    notes="Trailing stop hit"
                )
        
        return self.no_signal(instrument)
