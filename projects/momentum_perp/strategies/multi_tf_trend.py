"""
Multi-Timeframe Trend Following Strategy

Uses higher timeframe for trend direction and lower timeframe for entries.
- 4H: Trend direction (EMA 50/200 relationship)
- 15m: Entry on pullbacks (RSI oversold in uptrend, overbought in downtrend)
- Only trades in direction of higher timeframe trend
- Timeframes: 4H + 15m
"""

from typing import List, Dict, Any, Optional
import numpy as np

from .base import BaseStrategy, Signal, SignalType


class MultiTimeframeTrendStrategy(BaseStrategy):
    """
    Multi-timeframe trend following strategy.
    
    Parameters:
        htf_fast_ema: Higher TF fast EMA (default: 50)
        htf_slow_ema: Higher TF slow EMA (default: 200)
        ltf_rsi_period: Lower TF RSI period (default: 14)
        rsi_oversold: RSI level for oversold (default: 30)
        rsi_overbought: RSI level for overbought (default: 70)
        atr_stop_mult: ATR multiplier for stops (default: 2.0)
    """
    
    name = "multi_tf_trend"
    timeframe = "15m"
    secondary_timeframe = "4H"
    
    def _setup_params(self):
        self.htf_fast_ema = self.params.get("htf_fast_ema", 50)
        self.htf_slow_ema = self.params.get("htf_slow_ema", 200)
        self.ltf_rsi_period = self.params.get("ltf_rsi_period", 14)
        self.rsi_oversold = self.params.get("rsi_oversold", 30)
        self.rsi_overbought = self.params.get("rsi_overbought", 70)
        self.atr_stop_mult = self.params.get("atr_stop_mult", 2.0)
        self.atr_period = self.params.get("atr_period", 14)
    
    def analyze(
        self,
        klines: List[Dict[str, Any]],  # 15m data
        current_position: Optional[Dict[str, Any]] = None,
        secondary_klines: Optional[List[Dict[str, Any]]] = None,  # 4H data
    ) -> Signal:
        """Analyze multi-timeframe trend alignment."""
        min_ltf = max(self.ltf_rsi_period, self.atr_period) + 2
        min_htf = self.htf_slow_ema + 2
        
        if len(klines) < min_ltf:
            return self.no_signal(klines[0].get("instrument", "unknown"))
        
        if secondary_klines is None or len(secondary_klines) < min_htf:
            return self.no_signal(klines[0].get("instrument", "unknown"))
        
        instrument = klines[-1].get("instrument", "unknown")
        
        # Higher timeframe analysis (4H)
        _, htf_highs, htf_lows, htf_closes, _ = self.extract_ohlcv(secondary_klines)
        htf_fast_ema = self.ema(htf_closes, self.htf_fast_ema)
        htf_slow_ema = self.ema(htf_closes, self.htf_slow_ema)
        
        current_htf_fast = htf_fast_ema[-1]
        current_htf_slow = htf_slow_ema[-1]
        
        if np.isnan(current_htf_fast) or np.isnan(current_htf_slow):
            return self.no_signal(instrument)
        
        # Determine trend direction
        is_uptrend = current_htf_fast > current_htf_slow
        is_downtrend = current_htf_fast < current_htf_slow
        
        # Lower timeframe analysis (15m)
        _, ltf_highs, ltf_lows, ltf_closes, _ = self.extract_ohlcv(klines)
        ltf_rsi = self.rsi(ltf_closes, self.ltf_rsi_period)
        ltf_atr = self.atr(ltf_highs, ltf_lows, ltf_closes, self.atr_period)
        
        current_close = ltf_closes[-1]
        current_rsi = ltf_rsi[-1] if ltf_rsi else 50
        current_atr = ltf_atr[-1] if ltf_atr else 0
        
        if np.isnan(current_rsi):
            return self.no_signal(instrument)
        
        # Trend strength
        htf_diff = current_htf_fast - current_htf_slow
        htf_diff_pct = (htf_diff / current_htf_slow * 100) if current_htf_slow != 0 else 0
        
        indicators = {
            "close": current_close,
            "htf_fast_ema": current_htf_fast,
            "htf_slow_ema": current_htf_slow,
            "htf_trend": "uptrend" if is_uptrend else "downtrend" if is_downtrend else "neutral",
            "htf_diff_pct": htf_diff_pct,
            "ltf_rsi": current_rsi,
            "ltf_atr": current_atr,
        }
        
        # Handle existing position
        if current_position:
            return self._check_exit(
                current_position, current_close, current_rsi, current_atr,
                is_uptrend, is_downtrend, instrument, indicators
            )
        
        # Entry logic: trade pullbacks in trend direction
        if is_uptrend and current_rsi < self.rsi_oversold:
            # Uptrend + RSI oversold = buy the dip
            stop_loss = current_close - (current_atr * self.atr_stop_mult)
            take_profit = current_close + (current_atr * self.atr_stop_mult * 2)
            
            # Signal strength based on trend strength and RSI extreme
            trend_strength = min(1.0, abs(htf_diff_pct) / 5)
            rsi_strength = (self.rsi_oversold - current_rsi) / self.rsi_oversold
            strength = (trend_strength + rsi_strength) / 2
            
            return Signal(
                signal_type=SignalType.ENTRY_LONG,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Long pullback in uptrend (RSI: {current_rsi:.1f}, HTF diff: {htf_diff_pct:.1f}%)"
            )
        
        elif is_downtrend and current_rsi > self.rsi_overbought:
            # Downtrend + RSI overbought = sell the rally
            stop_loss = current_close + (current_atr * self.atr_stop_mult)
            take_profit = current_close - (current_atr * self.atr_stop_mult * 2)
            
            trend_strength = min(1.0, abs(htf_diff_pct) / 5)
            rsi_strength = (current_rsi - self.rsi_overbought) / (100 - self.rsi_overbought)
            strength = (trend_strength + rsi_strength) / 2
            
            return Signal(
                signal_type=SignalType.ENTRY_SHORT,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Short rally in downtrend (RSI: {current_rsi:.1f}, HTF diff: {htf_diff_pct:.1f}%)"
            )
        
        return self.no_signal(instrument)
    
    def _check_exit(
        self,
        position: Dict[str, Any],
        current_price: float,
        rsi: float,
        atr: float,
        is_uptrend: bool,
        is_downtrend: bool,
        instrument: str,
        indicators: Dict
    ) -> Signal:
        """Check for exits based on trend change or RSI extremes."""
        side = position.get("side", "long")
        entry_price = position.get("avg_entry_price", current_price)
        
        # Exit if trend changes
        if side == "long" and is_downtrend:
            return Signal(
                signal_type=SignalType.EXIT_LONG,
                instrument=instrument,
                strength=0.9,
                indicators=indicators,
                notes="Exit long: HTF trend changed to downtrend"
            )
        
        elif side == "short" and is_uptrend:
            return Signal(
                signal_type=SignalType.EXIT_SHORT,
                instrument=instrument,
                strength=0.9,
                indicators=indicators,
                notes="Exit short: HTF trend changed to uptrend"
            )
        
        # Take profit on RSI extremes
        if side == "long" and rsi > self.rsi_overbought:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.7,
                indicators=indicators,
                notes=f"Take profit: RSI overbought ({rsi:.1f})"
            )
        
        elif side == "short" and rsi < self.rsi_oversold:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.7,
                indicators=indicators,
                notes=f"Take profit: RSI oversold ({rsi:.1f})"
            )
        
        # Stop loss check
        if side == "long" and current_price < entry_price - (atr * self.atr_stop_mult):
            return Signal(
                signal_type=SignalType.STOP_LOSS,
                instrument=instrument,
                strength=1.0,
                indicators=indicators,
                notes="Stop loss hit"
            )
        
        elif side == "short" and current_price > entry_price + (atr * self.atr_stop_mult):
            return Signal(
                signal_type=SignalType.STOP_LOSS,
                instrument=instrument,
                strength=1.0,
                indicators=indicators,
                notes="Stop loss hit"
            )
        
        return self.no_signal(instrument)
