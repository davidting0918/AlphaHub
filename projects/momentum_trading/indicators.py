"""Technical indicators for momentum strategies."""

import numpy as np
import pandas as pd


def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    """Simple Moving Average."""
    return series.rolling(window=period).mean()


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Average True Range."""
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """On-Balance Volume."""
    direction = np.sign(close.diff())
    direction.iloc[0] = 0
    return (direction * volume).cumsum()


def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 20) -> pd.Series:
    """Rolling VWAP (Volume Weighted Average Price)."""
    typical_price = (high + low + close) / 3
    tp_volume = typical_price * volume
    return tp_volume.rolling(window=period).sum() / volume.rolling(window=period).sum()


def roc(close: pd.Series, period: int = 10) -> pd.Series:
    """Rate of Change (percentage)."""
    return ((close - close.shift(period)) / close.shift(period)) * 100


def volume_ratio(volume: pd.Series, period: int = 20) -> pd.Series:
    """Volume relative to moving average."""
    return volume / volume.rolling(window=period).mean()


def rolling_high(close: pd.Series, period: int = 20) -> pd.Series:
    """Rolling highest high."""
    return close.rolling(window=period).max()


def rolling_low(close: pd.Series, period: int = 20) -> pd.Series:
    """Rolling lowest low."""
    return close.rolling(window=period).min()


def consolidation_range(high: pd.Series, low: pd.Series, period: int = 10) -> tuple:
    """Calculate consolidation range over period."""
    range_high = high.rolling(window=period).max()
    range_low = low.rolling(window=period).min()
    return range_high, range_low


def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all indicators needed for strategies."""
    df = df.copy()
    
    # Basic indicators
    df['ema_20'] = ema(df['close'], 20)
    df['ema_50'] = ema(df['close'], 50)
    df['sma_20'] = sma(df['close'], 20)
    
    # RSI
    df['rsi_14'] = rsi(df['close'], 14)
    
    # ATR
    df['atr_14'] = atr(df['high'], df['low'], df['close'], 14)
    
    # Volume indicators
    df['volume_sma_20'] = sma(df['volume'], 20)
    df['volume_ratio'] = volume_ratio(df['volume'], 20)
    
    # VWAP
    df['vwap_20'] = vwap(df['high'], df['low'], df['close'], df['volume'], 20)
    
    # OBV
    df['obv'] = obv(df['close'], df['volume'])
    
    # Rate of Change
    df['roc_10'] = roc(df['close'], 10)
    
    # Rolling highs/lows
    df['high_20'] = rolling_high(df['close'], 20)
    df['low_20'] = rolling_low(df['close'], 20)
    
    # Consolidation range
    df['range_high_10'], df['range_low_10'] = consolidation_range(df['high'], df['low'], 10)
    
    # OBV momentum
    df['obv_sma_10'] = sma(df['obv'], 10)
    df['obv_high_20'] = rolling_high(df['obv'], 20)
    
    return df
