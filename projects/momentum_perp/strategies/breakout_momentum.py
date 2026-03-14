"""
Breakout Momentum Strategy

Detects price breakouts from N-period high/low with volume confirmation.
- Entry: Price breaks above/below N-period range
- Confirmation: Volume spike (>2x average)
- SL: Below breakout level
- TP: 2:1 R:R ratio
- Timeframe: 1H
"""

from typing import List, Dict, Any, Optional
import numpy as np

from .base import BaseStrategy, Signal, SignalType


class BreakoutMomentumStrategy(BaseStrategy):
    """
    Breakout momentum strategy with volume confirmation.
    
    Parameters:
        lookback_period: Period for high/low detection (default: 20)
        volume_multiplier: Min volume spike multiplier (default: 2.0)
        risk_reward_ratio: Target R:R ratio (default: 2.0)
        atr_stop_multiplier: ATR multiplier for stop loss (default: 1.5)
    """
    
    name = "breakout_momentum"
    timeframe = "1H"
    
    def _setup_params(self):
        self.lookback_period = self.params.get("lookback_period", 20)
        self.volume_multiplier = self.params.get("volume_multiplier", 2.0)
        self.risk_reward_ratio = self.params.get("risk_reward_ratio", 2.0)
        self.atr_stop_multiplier = self.params.get("atr_stop_multiplier", 1.5)
        self.atr_period = self.params.get("atr_period", 14)
    
    def analyze(
        self,
        klines: List[Dict[str, Any]],
        current_position: Optional[Dict[str, Any]] = None,
        secondary_klines: Optional[List[Dict[str, Any]]] = None,
    ) -> Signal:
        """Analyze for breakout signals."""
        if len(klines) < self.lookback_period + self.atr_period:
            return self.no_signal(klines[0].get("instrument", "unknown"))
        
        instrument = klines[-1].get("instrument", "unknown")
        opens, highs, lows, closes, volumes = self.extract_ohlcv(klines)
        
        # Current candle
        current_close = closes[-1]
        current_volume = volumes[-1]
        
        # Calculate indicators
        highest_high = self.highest(highs, self.lookback_period)
        lowest_low = self.lowest(lows, self.lookback_period)
        avg_volume = self.sma(volumes, self.lookback_period)
        atr = self.atr(highs, lows, closes, self.atr_period)
        
        prev_high = highest_high[-2] if len(highest_high) > 1 else np.nan
        prev_low = lowest_low[-2] if len(lowest_low) > 1 else np.nan
        current_avg_vol = avg_volume[-1] if avg_volume else 0
        current_atr = atr[-1] if atr else 0
        
        if np.isnan(prev_high) or np.isnan(prev_low) or current_atr == 0:
            return self.no_signal(instrument)
        
        # Volume confirmation
        volume_spike = current_volume > (current_avg_vol * self.volume_multiplier)
        
        # Breakout detection
        bullish_breakout = current_close > prev_high and volume_spike
        bearish_breakout = current_close < prev_low and volume_spike
        
        indicators = {
            "close": current_close,
            "prev_high": prev_high,
            "prev_low": prev_low,
            "volume": current_volume,
            "avg_volume": current_avg_vol,
            "volume_ratio": current_volume / current_avg_vol if current_avg_vol > 0 else 0,
            "atr": current_atr,
        }
        
        # Handle existing position - check for exits
        if current_position:
            return self._check_exit(current_position, current_close, current_atr, instrument, indicators)
        
        # Check for entry signals
        if bullish_breakout:
            stop_loss = current_close - (current_atr * self.atr_stop_multiplier)
            risk = current_close - stop_loss
            take_profit = current_close + (risk * self.risk_reward_ratio)
            
            # Calculate signal strength based on volume spike magnitude
            volume_ratio = current_volume / current_avg_vol if current_avg_vol > 0 else 0
            strength = min(1.0, (volume_ratio - self.volume_multiplier) / 2 + 0.5)
            
            return Signal(
                signal_type=SignalType.ENTRY_LONG,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Bullish breakout above {prev_high:.2f} with {volume_ratio:.1f}x volume"
            )
        
        elif bearish_breakout:
            stop_loss = current_close + (current_atr * self.atr_stop_multiplier)
            risk = stop_loss - current_close
            take_profit = current_close - (risk * self.risk_reward_ratio)
            
            volume_ratio = current_volume / current_avg_vol if current_avg_vol > 0 else 0
            strength = min(1.0, (volume_ratio - self.volume_multiplier) / 2 + 0.5)
            
            return Signal(
                signal_type=SignalType.ENTRY_SHORT,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Bearish breakout below {prev_low:.2f} with {volume_ratio:.1f}x volume"
            )
        
        return self.no_signal(instrument)
    
    def _check_exit(
        self,
        position: Dict[str, Any],
        current_price: float,
        atr: float,
        instrument: str,
        indicators: Dict
    ) -> Signal:
        """Check if we should exit existing position."""
        side = position.get("side", "long")
        entry_price = position.get("avg_entry_price", current_price)
        
        if side == "long":
            # Trail stop using ATR
            trailing_stop = current_price - (atr * self.atr_stop_multiplier)
            if current_price < entry_price - (atr * self.atr_stop_multiplier):
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    instrument=instrument,
                    strength=1.0,
                    indicators=indicators,
                    notes="Stop loss hit for long position"
                )
        else:
            trailing_stop = current_price + (atr * self.atr_stop_multiplier)
            if current_price > entry_price + (atr * self.atr_stop_multiplier):
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    instrument=instrument,
                    strength=1.0,
                    indicators=indicators,
                    notes="Stop loss hit for short position"
                )
        
        return self.no_signal(instrument)
