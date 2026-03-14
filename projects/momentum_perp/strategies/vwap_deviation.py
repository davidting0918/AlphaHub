"""
VWAP Deviation Strategy

Mean reversion strategy based on price deviation from VWAP.
- Entry: Price deviates >2σ from VWAP (fade the move)
- TP: Price returns to VWAP
- SL: Price extends to 3σ
- Timeframe: 5m (scalping)
"""

from typing import List, Dict, Any, Optional
import numpy as np

from .base import BaseStrategy, Signal, SignalType


class VWAPDeviationStrategy(BaseStrategy):
    """
    VWAP mean reversion strategy.
    
    Parameters:
        entry_std: Standard deviations for entry (default: 2.0)
        stop_std: Standard deviations for stop loss (default: 3.0)
        min_volume_ratio: Min volume vs average for entry (default: 0.8)
    """
    
    name = "vwap_deviation"
    timeframe = "5m"
    
    def _setup_params(self):
        self.entry_std = self.params.get("entry_std", 2.0)
        self.stop_std = self.params.get("stop_std", 3.0)
        self.min_volume_ratio = self.params.get("min_volume_ratio", 0.8)
        self.lookback = self.params.get("lookback", 100)  # VWAP calculation window
    
    def analyze(
        self,
        klines: List[Dict[str, Any]],
        current_position: Optional[Dict[str, Any]] = None,
        secondary_klines: Optional[List[Dict[str, Any]]] = None,
    ) -> Signal:
        """Analyze for VWAP deviation signals."""
        if len(klines) < self.lookback:
            return self.no_signal(klines[0].get("instrument", "unknown"))
        
        instrument = klines[-1].get("instrument", "unknown")
        
        # Use most recent N candles for VWAP
        recent_klines = klines[-self.lookback:]
        opens, highs, lows, closes, volumes = self.extract_ohlcv(recent_klines)
        
        current_close = closes[-1]
        current_volume = volumes[-1]
        
        # Calculate VWAP and bands
        vwap_values, upper_band, lower_band = self.vwap(highs, lows, closes, volumes)
        
        current_vwap = vwap_values[-1]
        current_upper = upper_band[-1]
        current_lower = lower_band[-1]
        
        # Calculate current standard deviation
        typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
        
        # Calculate deviation in terms of standard deviations
        cum_vol = sum(volumes)
        if cum_vol == 0:
            return self.no_signal(instrument)
            
        cum_tp_vol = sum(tp * vol for tp, vol in zip(typical_prices, volumes))
        cum_tp_vol_sq = sum((tp ** 2) * vol for tp, vol in zip(typical_prices, volumes))
        
        variance = (cum_tp_vol_sq / cum_vol) - ((cum_tp_vol / cum_vol) ** 2)
        std_dev = np.sqrt(max(0, variance))
        
        if std_dev == 0:
            return self.no_signal(instrument)
        
        deviation_std = (current_close - current_vwap) / std_dev
        
        # Volume check
        avg_volume = sum(volumes) / len(volumes)
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
        
        indicators = {
            "close": current_close,
            "vwap": current_vwap,
            "upper_2std": current_vwap + (2 * std_dev),
            "lower_2std": current_vwap - (2 * std_dev),
            "upper_3std": current_vwap + (3 * std_dev),
            "lower_3std": current_vwap - (3 * std_dev),
            "deviation_std": deviation_std,
            "std_dev": std_dev,
            "volume_ratio": volume_ratio,
        }
        
        # Handle existing position
        if current_position:
            return self._check_exit(
                current_position, current_close, current_vwap, deviation_std,
                instrument, indicators
            )
        
        # Volume filter
        if volume_ratio < self.min_volume_ratio:
            return self.no_signal(instrument)
        
        # Entry signals - mean reversion
        if deviation_std >= self.entry_std:
            # Price is too high - short for mean reversion
            stop_loss = current_vwap + (self.stop_std * std_dev)
            take_profit = current_vwap
            
            # Strength based on deviation magnitude
            strength = min(1.0, (deviation_std - self.entry_std) / 2 + 0.5)
            
            return Signal(
                signal_type=SignalType.ENTRY_SHORT,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Short: Price {deviation_std:.1f}σ above VWAP"
            )
        
        elif deviation_std <= -self.entry_std:
            # Price is too low - long for mean reversion
            stop_loss = current_vwap - (self.stop_std * std_dev)
            take_profit = current_vwap
            
            strength = min(1.0, (abs(deviation_std) - self.entry_std) / 2 + 0.5)
            
            return Signal(
                signal_type=SignalType.ENTRY_LONG,
                instrument=instrument,
                strength=strength,
                entry_price=current_close,
                stop_loss=stop_loss,
                take_profit=take_profit,
                indicators=indicators,
                notes=f"Long: Price {abs(deviation_std):.1f}σ below VWAP"
            )
        
        return self.no_signal(instrument)
    
    def _check_exit(
        self,
        position: Dict[str, Any],
        current_price: float,
        vwap: float,
        deviation_std: float,
        instrument: str,
        indicators: Dict
    ) -> Signal:
        """Check for exit on existing position."""
        side = position.get("side", "long")
        
        # Take profit at VWAP
        if side == "long" and current_price >= vwap:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.9,
                indicators=indicators,
                notes="Price returned to VWAP - take profit"
            )
        
        elif side == "short" and current_price <= vwap:
            return Signal(
                signal_type=SignalType.TAKE_PROFIT,
                instrument=instrument,
                strength=0.9,
                indicators=indicators,
                notes="Price returned to VWAP - take profit"
            )
        
        # Stop loss at 3σ
        if side == "long" and deviation_std <= -self.stop_std:
            return Signal(
                signal_type=SignalType.STOP_LOSS,
                instrument=instrument,
                strength=1.0,
                indicators=indicators,
                notes=f"Stop loss: Price at {abs(deviation_std):.1f}σ below VWAP"
            )
        
        elif side == "short" and deviation_std >= self.stop_std:
            return Signal(
                signal_type=SignalType.STOP_LOSS,
                instrument=instrument,
                strength=1.0,
                indicators=indicators,
                notes=f"Stop loss: Price at {deviation_std:.1f}σ above VWAP"
            )
        
        return self.no_signal(instrument)
