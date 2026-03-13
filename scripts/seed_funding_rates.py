"""
Fetch last 6 months of funding rates for OKX + Binance (BTC, ETH)
and batch insert into DB.
"""
import asyncio
import time
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/home/ubuntu/clawd/repos/AlphaHub')

from adaptor.binance.client import BinanceClient
from adaptor.okx.client import OKXClient
from database.client import PostgresClient

SIX_MONTHS_AGO_MS = int((datetime.now(timezone.utc) - timedelta(days=180)).timestamp() * 1000)


def fetch_binance(symbol: str) -> list:
    """Fetch Binance funding rates from 6 months ago to now."""
    client = BinanceClient(exchange_name='BINANCEFUTURES')
    all_rates = []
    cursor = SIX_MONTHS_AGO_MS

    while True:
        rates = client.getFundingRates(inst_id=symbol, limit=1000, start_time=cursor)
        if not rates:
            break
        all_rates.extend(rates)
        if len(rates) < 1000:
            break
        newest = max(rates, key=lambda r: r['funding_time'] if r['funding_time'] else datetime.min.replace(tzinfo=timezone.utc))
        if newest['funding_time']:
            new_cursor = int(newest['funding_time'].timestamp() * 1000) + 1
            if new_cursor <= cursor:
                break
            cursor = new_cursor
        else:
            break
        time.sleep(0.05)

    client.close()
    return all_rates


def fetch_okx(symbol: str) -> list:
    """Fetch OKX funding rates (API returns ~3 months max)."""
    client = OKXClient(exchange_name='OKX')
    all_rates = []
    cursor = None

    while True:
        kwargs = {"inst_id": symbol, "limit": 100}
        if cursor:
            kwargs["before"] = str(cursor)
        rates = client.getFundingRates(**kwargs)
        if not rates:
            break
        all_rates.extend(rates)
        oldest = min(rates, key=lambda r: r['funding_time'] if r['funding_time'] else datetime.max.replace(tzinfo=timezone.utc))
        if oldest['funding_time']:
            new_cursor = int(oldest['funding_time'].timestamp() * 1000)
            if cursor and new_cursor >= cursor:
                break
            cursor = new_cursor
        else:
            break
        time.sleep(0.1)

    client.close()
    return all_rates


async def batch_insert(db: PostgresClient, exchange_id: int, instrument_id: str, rates: list):
    """Batch insert funding rates using executemany."""
    if not rates:
        return 0

    # Deduplicate by funding_time
    seen = {}
    for r in rates:
        if r['funding_time']:
            key = r['funding_time']
            seen[key] = r
    unique_rates = list(seen.values())

    # Prepare tuples for executemany
    rows = [
        (exchange_id, instrument_id, r['funding_rate'], r.get('next_funding_rate'), r['funding_time'])
        for r in unique_rates
        if r['funding_time']
    ]

    await db.execute_many(
        """INSERT INTO funding_rates (
            exchange_id, instrument_id, funding_rate,
            predicted_rate, funding_time, updated_at
        ) VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (instrument_id, funding_time) DO NOTHING""",
        rows,
    )
    return len(rows)


async def main():
    db = PostgresClient()
    await db.init_pool()

    # Clear existing funding rates to avoid conflicts with partial data
    await db.execute("DELETE FROM funding_rates WHERE 1=1")
    print("Cleared existing funding_rates")

    tasks = [
        # (exchange, exchange_id, symbol, instrument_id, fetch_fn)
        ("Binance", 4, "BTCUSDT", "BINANCEFUTURES_PERP_BTC_USDT", fetch_binance),
        ("Binance", 4, "ETHUSDT", "BINANCEFUTURES_PERP_ETH_USDT", fetch_binance),
        ("OKX", 2, "BTC-USDT-SWAP", "OKX_PERP_BTC_USDT", fetch_okx),
        ("OKX", 2, "ETH-USDT-SWAP", "OKX_PERP_ETH_USDT", fetch_okx),
    ]

    for exchange, eid, symbol, inst_id, fetch_fn in tasks:
        print(f"\n[{exchange}] Fetching {symbol}...")
        rates = fetch_fn(symbol)
        print(f"  Fetched {len(rates)} rates")
        if rates:
            oldest = min(r['funding_time'] for r in rates if r['funding_time'])
            newest = max(r['funding_time'] for r in rates if r['funding_time'])
            print(f"  Range: {oldest} → {newest}")

        print(f"  Batch inserting...")
        count = await batch_insert(db, eid, inst_id, rates)
        print(f"  Inserted {count} rows ✅")

    # Snapshot
    print("\n" + "=" * 70)
    print("SNAPSHOT: funding_rates (latest 10 per exchange)")
    print("=" * 70)

    for exchange, eid in [("OKX", 2), ("BINANCEFUTURES", 4)]:
        rows = await db.read("""
            SELECT instrument_id, funding_rate, funding_time
            FROM funding_rates
            WHERE exchange_id = $1
            ORDER BY funding_time DESC
            LIMIT 10
        """, eid)
        print(f"\n--- {exchange} ---")
        for r in rows:
            print(f"  {r['instrument_id']:35s} | {r['funding_rate']:+.10f} | {r['funding_time']}")

    # Stats
    print("\n--- STATS ---")
    stats = await db.read("""
        SELECT e.name, count(*) as cnt,
               min(fr.funding_time) as oldest,
               max(fr.funding_time) as newest
        FROM funding_rates fr
        JOIN exchanges e ON e.id = fr.exchange_id
        GROUP BY e.name ORDER BY e.name
    """)
    for s in stats:
        print(f"  {s['name']:20s} | rows={s['cnt']:>6} | {s['oldest']} → {s['newest']}")

    await db.close()
    print("\nDone! 🎉")


if __name__ == "__main__":
    asyncio.run(main())
