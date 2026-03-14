"""
Data Fetcher for Backtesting

Fetches 30 days of kline data for all symbols at required timeframes.
Saves to parquet files for efficient reuse.
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from adaptor.okx.client import OKXClient

# Symbols to backtest
SYMBOLS = [
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP",
    "ADA-USDT-SWAP", "AVAX-USDT-SWAP", "LINK-USDT-SWAP", "DOT-USDT-SWAP", "MATIC-USDT-SWAP",
    "ARB-USDT-SWAP", "OP-USDT-SWAP", "APT-USDT-SWAP", "NEAR-USDT-SWAP", "SUI-USDT-SWAP"
]

# Timeframes needed by strategies
TIMEFRAMES = {
    "5m": {"minutes": 5, "bars_30d": 30 * 24 * 12},       # 8640 bars
    "15m": {"minutes": 15, "bars_30d": 30 * 24 * 4},      # 2880 bars
    "1H": {"minutes": 60, "bars_30d": 30 * 24},           # 720 bars
    "4H": {"minutes": 240, "bars_30d": 30 * 6},           # 180 bars
}

DATA_DIR = Path(__file__).parent / "data"


def fetch_klines_paginated(client: OKXClient, symbol: str, bar: str, days: int = 30) -> list:
    """
    Fetch klines with pagination to get full 30 days.
    OKX returns newest first, max 100 per request.
    Use 'before' param with oldest timestamp from previous batch.
    """
    all_klines = []
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    start_ts_ms = int(start_time.timestamp() * 1000)
    
    before = None  # Pagination cursor
    max_iterations = 100  # Safety limit
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        try:
            # Use history endpoint for older data, regular for recent
            klines = client.getKlines(
                inst_id=symbol,
                bar=bar,
                limit=100,
                before=before
            )
            
            if not klines:
                # Try history endpoint if regular returns empty
                if before is not None:
                    klines = client.getHistoryKlines(
                        inst_id=symbol,
                        bar=bar,
                        limit=100,
                        before=before
                    )
                if not klines:
                    break
            
            all_klines.extend(klines)
            
            # Get oldest timestamp from this batch for pagination
            oldest_ts = min(k['open_time'] for k in klines)
            oldest_ts_ms = int(oldest_ts.timestamp() * 1000)
            
            # Stop if we've gone back far enough
            if oldest_ts_ms <= start_ts_ms:
                break
                
            # Set before to oldest timestamp for next request
            before = str(oldest_ts_ms)
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"    Error fetching {symbol} {bar}: {e}")
            break
    
    # Filter to only include data within our time range
    all_klines = [k for k in all_klines if k['open_time'] >= start_time]
    
    # Sort by time ascending (oldest first) - strategies expect this
    all_klines.sort(key=lambda x: x['open_time'])
    
    return all_klines


def save_klines_to_parquet(klines: list, symbol: str, timeframe: str):
    """Save klines to parquet file."""
    if not klines:
        return
    
    df = pd.DataFrame(klines)
    
    # Ensure proper types
    df['open_time'] = pd.to_datetime(df['open_time'])
    for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
        df[col] = df[col].astype(float)
    
    # Add instrument column
    df['instrument'] = symbol
    
    # Save
    filename = DATA_DIR / f"{symbol.replace('-', '_')}_{timeframe}.parquet"
    df.to_parquet(filename, index=False)
    print(f"    Saved {len(df)} bars to {filename.name}")


def load_klines_from_parquet(symbol: str, timeframe: str) -> list:
    """Load klines from parquet file."""
    filename = DATA_DIR / f"{symbol.replace('-', '_')}_{timeframe}.parquet"
    if not filename.exists():
        return []
    
    df = pd.read_parquet(filename)
    
    # Convert back to list of dicts for strategies
    records = df.to_dict('records')
    for r in records:
        if pd.notna(r.get('open_time')):
            r['open_time'] = pd.Timestamp(r['open_time']).to_pydatetime()
    
    return records


def fetch_all_data():
    """Fetch all required data for backtesting."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    client = OKXClient()
    
    total_symbols = len(SYMBOLS)
    total_timeframes = len(TIMEFRAMES)
    
    print(f"Fetching 30 days of data for {total_symbols} symbols x {total_timeframes} timeframes")
    print("=" * 60)
    
    for i, symbol in enumerate(SYMBOLS, 1):
        print(f"\n[{i}/{total_symbols}] {symbol}")
        
        for tf in TIMEFRAMES:
            # Check if data already exists
            filename = DATA_DIR / f"{symbol.replace('-', '_')}_{tf}.parquet"
            if filename.exists():
                df = pd.read_parquet(filename)
                age = datetime.now(timezone.utc) - pd.Timestamp(df['open_time'].max()).to_pydatetime().replace(tzinfo=timezone.utc)
                if age < timedelta(hours=2):
                    print(f"  {tf}: Cached ({len(df)} bars, {age.total_seconds()/3600:.1f}h old)")
                    continue
            
            print(f"  {tf}: Fetching...", end=" ", flush=True)
            klines = fetch_klines_paginated(client, symbol, tf)
            save_klines_to_parquet(klines, symbol, tf)
    
    print("\n" + "=" * 60)
    print("Data fetch complete!")
    
    client.close()


if __name__ == "__main__":
    fetch_all_data()
