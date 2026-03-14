"""
Volume Profile Momentum Strategy

Tracks volume spikes combined with price momentum for trend continuation.
- Entry: High volume + strong price momentum (ROC)
- Filter: ADX for trend strength
- Exit: Volume dries up or momentum reverses
- Timeframe: 1H
"""

from typing import List, Dict, Any, Optional
import numpy as np

from .base import BaseStrategy, Signal, SignalType


class VolumeProfileMomentumStrategy(BaseStrategy):
    """
    Volume-based momentum strategy.
    
    Parameters:
        volume_spike_mult: Volume spike multiplier (default: 2.0)
        roc_period: Rate of change period (default: 10)
        roc_threshold: Min ROC for momentum (default: 2.0%)
        adx_period: ADX period (default: 14)
        adx_threshold: Min ADX for trend strength (default: 25)
        atr_stop_mult: ATR multiplier for stops (default: 2.0)
    """
    
    name = "volume_profile"
    timeframe = "1H"
    
    def _setup_params(self):
        self.volume_spike_mult = self.params.get("volume_spike_mult", 2.0)
        self.roc_period = self.params.get("roc_period", 10)
        self.roc_threshold = self.params.get("roc_threshold", 2.0)
        self.adx_period = self.params.get("adx_period", 14)
        self.adx_threshold = self.params.get("adx_threshold", 25)
        self.atr_period = self.params.get("atr_period", 14)
        self.atr_stop_mult = self.params.get("atr_stop_mult", 2.0)
        self.volume_avg_period = self.params.get("volume_avg_period", 20)
    
    def analyze(
        self,
        klines: List[Dict[str, Any]],
        current_position: Optional[Dict[str, Any]] = None,
        secondary_klines: Optional[List[Dict[str, Any]]] = None,
    ) -> Signal:
        """Analyze volume profile momentum signals."""
        min_periods = max(
            self.volume_avg_period,
            self.roc_period,
            self.adx_period * 2,
            self.atr_period
        ) + 2
        
        if len(klines) < min_periods:
            return self.no_signal(klines[0].get("instrument", "unknown"))
        
        instrument = klines[-1].get("instrument", "unknown")
        opens, highs, lows, closes, volumes = self.extract_ohlcv(klines)
        
        current_close = closes[-1]
        current_volume = volumes[-1]
        
        # Calculate indicators
        avg_volume = self.sma(volumes, self.volume_avg_period)
        roc = self.roc(closes, self.roc_period)
        adx = self.adx(highs, lows, closes, self.adx_period)
        atr = self.atr(highs, lows, closes, self.atr_period)
        
        current_avg_vol = avg_volume[-1] if avg_volume else 0
        current_roc = roc[-1] if roc else 0
        current_adx = adx[-1] if adx else 0
        current_atr = atr[-1] if atr else 0
        
        if np.isnan(current_roc) or np.isnan(current_adx):
            return self.no_signal(instrument)
        
        # Volume analysis
        volume_ratio = current_volume / current_avg_vol if current_avg_vol > 0 else 0
        is_volume_spike = volume_ratio > self.volume_spike_mult
        
        # Trend strength
        is_trending = current_adx > self.adx_threshold
        
        # Momentum direction
        is_bullish_momentum = current_roc > self.roc_threshold
        is_bearish_momentum = current_roc < -self.roc_threshold
        
        indicators = {
            "close": current_close,
            "volume": current_volume,
            "avg_volume": current_avg_vol,
            "volume_ratio": volume_ratio,
            "roc": current_roc,
            "adx": current_adx,
            "atr": current_atr,
            "is_volume_spike": is_volume_spike,
            "is_trending": is_trending,
        }
        
        # Handle existing position
        if current_position:
            return self._check_exit(
                current_position, current_close, current_roc, current_atr,
                volume_ratio, instrument, indicators
            )
        
        # Entry signals: Volume spike + momentum + trend
        if is_volume_spike and is_trending:
            if is_bullish_momentum:
                stop_loss = current_close - (current_atr * self.atr_stop_mult)
                take_profit = current_close + (current_atr * self.atr_stop_mult * 2)
                
                # Signal strength based on volume, momentum, and trend strength
                vol_strength = min(1.0, (volume_ratio - self.volume_spike_mult) / 2)
                mom_strength = min(1.0, current_roc / (self.roc_threshold * 3))
                adx_strength = min(1.0, (current_adx - self.adx_threshold) / 25)
                strength = (vol_strength + mom_strength + adx_strength) / 3
                
                return Signal(
                    signal_type=SignalType.ENTRY_LONG,
                    instrument=instrument,
                    strength=strength,
                    entry_price=current_close,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    indicators=indicators,
                    notes=f"Long: Vol {volume_ratio:.1f}x, ROC {current_roc:.1f}%, ADX {current_adx:.0f}"
                )
            
            elif is_bearish_momentum:
                stop_loss = current_close + (current_atr * self.atr_stop_mult)
                take_profit = current_close - (current_atr * self.atr_stop_mult * 2)
                
                vol_strength = min(1.0, (volume_ratio - self.volume_spike_mult) / 2)
                mom_strength = min(1.0, abs(current_roc) / (self.roc_threshold * 3))
                adx_strength = min(1.0, (current_adx - self.adx_threshold) / 25)
                strength = (vol_strength + mom_strength + adx_strength) / 3
                
                return Signal(
                    signal_type=SignalType.ENTRY_SHORT,
                    instrument=instrument,
                    strength=strength,
                    entry_price=current_close,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    indicators=indicators,
                    notes=f"Short: Vol {volume_ratio:.1f}x, ROC {current_roc:.1f}%, ADX {current_adx:.0f}"
                )
        
        return self.no_signal(instrument)
    
    def _check_exit(
        self,
        position: Dict[str, Any],
        current_price: float,
        roc: float,
        atr: float,
        volume_ratio: float,
        instrument: str,
        indicators: Dict
    ) -> Signal:
        """Check for exits based on momentum reversal or volume dry-up."""
        side = position.get("side", "long")
        entry_price = position.get("avg_entry_price", current_price)
        
        # Exit on momentum reversal
        if side == "long" and roc < -self.roc_threshold:
            return Signal(
                signal_type=SignalType.EXIT_LONG,
                instrument=instrument,
                strength=0.8,
                indicators=indicators,
                notes=f"Exit long: Momentum reversed (ROC: {roc:.1f}%)"
            )
        
        elif side == "short" and roc > self.roc_threshold:
            return Signal(
                signal_type=SignalType.EXIT_SHORT,
                instrument=instrument,
                strength=0.8,
                indicators=indicators,
                notes=f"Exit short: Momentum reversed (ROC: {roc:.1f}%)"
            )
        
        # Take profit on volume dry-up after good move
        pnl_pct = (current_price - entry_price) / entry_price * 100
        if side == "long" and pnl_pct > 2 and volume_ratio < 0.5:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.6,
                indicators=indicators,
                notes=f"Take profit: Volume drying up (ratio: {volume_ratio:.1f})"
            )
        
        elif side == "short" and pnl_pct < -2 and volume_ratio < 0.5:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.6,
                indicators=indicators,
                notes=f"Take profit: Volume drying up (ratio: {volume_ratio:.1f})"
            )
        
        # Stop loss
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
