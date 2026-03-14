"""Fetch kline data from Binance Futures and store in PostgreSQL."""

import asyncio
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import httpx
import pandas as pd

# Force flush print output
def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)

from config import (
    DATABASE_URL, EXCHANGE_INFO_URL, TICKER_24H_URL, KLINES_URL,
    START_TIME_MS, INTERVAL, KLINE_LIMIT, MIN_DAILY_VOLUME, MAX_DAILY_VOLUME
)


async def create_tables(pool: asyncpg.Pool) -> None:
    """Create database tables if they don't exist."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS momentum_klines (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(30) NOT NULL,
                exchange VARCHAR(20) NOT NULL DEFAULT 'BINANCE',
                interval VARCHAR(10) NOT NULL DEFAULT '4h',
                open_time TIMESTAMPTZ NOT NULL,
                open DECIMAL(20,8) NOT NULL,
                high DECIMAL(20,8) NOT NULL,
                low DECIMAL(20,8) NOT NULL,
                close DECIMAL(20,8) NOT NULL,
                volume DECIMAL(30,8) NOT NULL,
                quote_volume DECIMAL(30,8),
                trades INTEGER,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(symbol, exchange, interval, open_time)
            );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_momentum_klines_symbol ON momentum_klines(symbol);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_momentum_klines_time ON momentum_klines(open_time);")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS momentum_pairs (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(30) NOT NULL UNIQUE,
                exchange VARCHAR(20) NOT NULL DEFAULT 'BINANCE',
                avg_daily_volume DECIMAL(30,2),
                avg_daily_trades INTEGER,
                volatility_30d DECIMAL(10,4),
                is_active BOOLEAN DEFAULT TRUE,
                last_updated TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    print_flush("✓ Database tables created")


async def get_usdt_perpetuals(client: httpx.AsyncClient) -> list[str]:
    """Get all USDT perpetual futures symbols."""
    resp = await client.get(EXCHANGE_INFO_URL)
    resp.raise_for_status()
    data = resp.json()
    
    symbols = [
        s['symbol'] for s in data['symbols']
        if s['quoteAsset'] == 'USDT' 
        and s['contractType'] == 'PERPETUAL'
        and s['status'] == 'TRADING'
    ]
    print_flush(f"✓ Found {len(symbols)} USDT perpetuals")
    return symbols


async def get_24h_volumes(client: httpx.AsyncClient) -> dict[str, float]:
    """Get 24h quote volume for all symbols."""
    resp = await client.get(TICKER_24H_URL)
    resp.raise_for_status()
    data = resp.json()
    return {t['symbol']: float(t['quoteVolume']) for t in data}


def filter_low_liquidity_pairs(symbols: list[str], volumes: dict[str, float]) -> list[tuple[str, float]]:
    """Filter to low-mid liquidity pairs ($1M-$20M daily volume)."""
    filtered = []
    for symbol in symbols:
        vol = volumes.get(symbol, 0)
        if MIN_DAILY_VOLUME <= vol <= MAX_DAILY_VOLUME:
            filtered.append((symbol, vol))
    
    # Sort by volume
    filtered.sort(key=lambda x: x[1], reverse=True)
    print_flush(f"✓ Filtered to {len(filtered)} low-mid liquidity pairs (${MIN_DAILY_VOLUME/1e6:.0f}M-${MAX_DAILY_VOLUME/1e6:.0f}M)")
    return filtered


async def fetch_klines(
    client: httpx.AsyncClient,
    symbol: str,
    start_time: int,
    end_time: Optional[int] = None
) -> list[list]:
    """Fetch klines with pagination."""
    all_klines = []
    current_start = start_time
    
    while True:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": current_start,
            "limit": KLINE_LIMIT,
        }
        if end_time:
            params["endTime"] = end_time
            
        try:
            resp = await client.get(KLINES_URL, params=params)
            resp.raise_for_status()
            klines = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print_flush(f"  Rate limited, sleeping 60s...")
                await asyncio.sleep(60)
                continue
            raise
        
        if not klines:
            break
            
        all_klines.extend(klines)
        
        # Check if we got less than limit (no more data)
        if len(klines) < KLINE_LIMIT:
            break
            
        # Next batch starts after last candle
        current_start = klines[-1][6] + 1  # close_time + 1
        await asyncio.sleep(0.1)  # Rate limit
    
    return all_klines


async def store_klines(pool: asyncpg.Pool, symbol: str, klines: list[list]) -> int:
    """Store klines in database."""
    if not klines:
        return 0
    
    async with pool.acquire() as conn:
        # Prepare data
        records = []
        for k in klines:
            open_time = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
            records.append((
                symbol, 'BINANCE', '4h', open_time,
                float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                float(k[5]), float(k[7]), int(k[8])
            ))
        
        # Upsert
        await conn.executemany("""
            INSERT INTO momentum_klines 
            (symbol, exchange, interval, open_time, open, high, low, close, volume, quote_volume, trades)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol, exchange, interval, open_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trades = EXCLUDED.trades
        """, records)
    
    return len(records)


async def store_pair_info(pool: asyncpg.Pool, pairs: list[tuple[str, float]]) -> None:
    """Store pair metadata."""
    async with pool.acquire() as conn:
        for symbol, volume in pairs:
            await conn.execute("""
                INSERT INTO momentum_pairs (symbol, exchange, avg_daily_volume, last_updated)
                VALUES ($1, 'BINANCE', $2, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    avg_daily_volume = EXCLUDED.avg_daily_volume,
                    last_updated = NOW()
            """, symbol, volume)


async def load_klines_from_db(pool: asyncpg.Pool, symbol: str) -> pd.DataFrame:
    """Load klines for a symbol from database."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT open_time, open, high, low, close, volume, quote_volume, trades
            FROM momentum_klines
            WHERE symbol = $1 AND exchange = 'BINANCE' AND interval = '4h'
            ORDER BY open_time
        """, symbol)
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame([dict(r) for r in rows])
    df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
    for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
        df[col] = df[col].astype(float)
    df.set_index('open_time', inplace=True)
    return df


async def get_all_pairs(pool: asyncpg.Pool) -> list[str]:
    """Get all pairs from database."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT symbol FROM momentum_pairs WHERE is_active = TRUE")
    return [r['symbol'] for r in rows]


async def fetch_all_data() -> tuple[int, int]:
    """Main function to fetch all data."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    
    try:
        await create_tables(pool)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get symbols and volumes
            symbols = await get_usdt_perpetuals(client)
            volumes = await get_24h_volumes(client)
            
            # Filter to target liquidity range
            pairs = filter_low_liquidity_pairs(symbols, volumes)
            
            if not pairs:
                print_flush("No pairs found in target volume range!")
                return 0, 0
            
            # Store pair info
            await store_pair_info(pool, pairs)
            
            # Fetch klines for each pair
            total_klines = 0
            now_ms = int(time.time() * 1000)
            
            for i, (symbol, vol) in enumerate(pairs):
                print_flush(f"[{i+1}/{len(pairs)}] Fetching {symbol} (${vol/1e6:.1f}M vol)...", end=" ")
                
                try:
                    klines = await fetch_klines(client, symbol, START_TIME_MS, now_ms)
                    stored = await store_klines(pool, symbol, klines)
                    total_klines += stored
                    print_flush(f"✓ {stored} candles")
                except Exception as e:
                    print_flush(f"✗ Error: {e}")
                
                await asyncio.sleep(0.1)
            
            return len(pairs), total_klines
    
    finally:
        await pool.close()


if __name__ == "__main__":
    pairs, klines = asyncio.run(fetch_all_data())
    print_flush(f"\n✓ Done! Fetched {klines:,} klines for {pairs} pairs")
