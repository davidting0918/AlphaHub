#!/usr/bin/env python3
"""
Deribit Options Data Fetcher — Standalone Script

Fetches and stores ALL BTC and ETH options data from Deribit.
- Instruments (active + recently expired)
- Book summaries (bulk ticker snapshots)
- Individual ticker data with full greeks (for high-value instruments)
- Volatility surface construction

Can be run standalone. Shows progress. Handles rate limiting.

Usage:
    python3 scripts/fetch_deribit_options.py
    python3 scripts/fetch_deribit_options.py --currency BTC
    python3 scripts/fetch_deribit_options.py --currency BTC ETH --detailed
    python3 scripts/fetch_deribit_options.py --include-expired
"""

import asyncio
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database.client import PostgresClient
from adaptor.deribit.client import DeribitClient, DeribitClientError
from adaptor.deribit.parser import DeribitParser
from adaptor.deribit.config import (
    EXCHANGE_NAME,
    SUPPORTED_CURRENCIES,
    RATE_LIMIT_DELAY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 200


class DeribitOptionsFetcher:
    """Standalone Deribit options data fetcher with progress reporting."""

    def __init__(
        self,
        currencies: List[str],
        include_expired: bool = False,
        detailed: bool = False,
        max_detailed_per_currency: int = 200,
    ):
        self.currencies = currencies
        self.include_expired = include_expired
        self.detailed = detailed
        self.max_detailed = max_detailed_per_currency
        self.db: Optional[PostgresClient] = None
        self.client: Optional[DeribitClient] = None
        self.exchange_id: Optional[int] = None

        # Stats
        self.stats = {
            "instruments_fetched": 0,
            "instruments_stored": 0,
            "tickers_fetched": 0,
            "tickers_stored": 0,
            "detailed_tickers": 0,
            "vol_surface_points": 0,
            "errors": 0,
            "start_time": None,
        }

    async def init(self):
        """Initialize database and client."""
        self.stats["start_time"] = time.monotonic()

        # Init DB
        self.db = PostgresClient()
        await self.db.init_pool()
        logger.info("✅ Database connected")

        # Init client
        self.client = DeribitClient(rate_limit_delay=RATE_LIMIT_DELAY)
        logger.info("✅ Deribit client initialized")

        # Ensure exchange exists
        row = await self.db.read_one(
            "SELECT id FROM exchanges WHERE name = $1", EXCHANGE_NAME
        )
        if row:
            self.exchange_id = row["id"]
        else:
            result = await self.db.insert_one("exchanges", {
                "name": EXCHANGE_NAME,
                "adaptor": "deribit",
            })
            self.exchange_id = result["id"]
            logger.info(f"Created exchange '{EXCHANGE_NAME}' (id={self.exchange_id})")

        # Ensure tables exist
        await self._ensure_tables()

        logger.info(f"✅ Exchange ID: {self.exchange_id}")

    async def _ensure_tables(self):
        """Create options tables if they don't exist."""
        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "database", "schema", "options_tables.sql"
        )
        if os.path.exists(schema_path):
            with open(schema_path) as f:
                sql = f.read()
            # asyncpg execute() handles one statement at a time —
            # split on semicolons and run each DDL statement separately.
            try:
                async with self.db.get_connection() as conn:
                    for statement in sql.split(";"):
                        statement = statement.strip()
                        if statement and not statement.startswith("--"):
                            await conn.execute(statement)
                logger.info("✅ Options tables verified/created")
            except Exception as e:
                # Tables might already exist, that's fine
                logger.debug(f"Table creation note: {e}")

    async def cleanup(self):
        """Clean up resources."""
        if self.client:
            self.client.close()
        if self.db:
            await self.db.close()

    def _progress(self, msg: str):
        """Print progress with timestamp."""
        elapsed = time.monotonic() - self.stats["start_time"]
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        print(f"  [{mins:02d}:{secs:02d}] {msg}")

    # ==================== Instruments ====================

    async def fetch_instruments(self, currency: str) -> List[Dict[str, Any]]:
        """Fetch and store options instruments."""
        self._progress(f"📦 Fetching {currency} instruments (expired={self.include_expired})...")

        try:
            raw = self.client.get_instruments(
                currency=currency, kind="option", expired=self.include_expired
            )
        except DeribitClientError as e:
            logger.error(f"Failed to fetch instruments: {e}")
            self.stats["errors"] += 1
            return []

        parser = DeribitParser()
        instruments = parser.parse_instruments(raw)
        self.stats["instruments_fetched"] += len(instruments)
        self._progress(f"  Fetched {len(instruments)} {currency} instruments")

        if not instruments:
            return []

        rows = []
        for inst in instruments:
            instrument_id = f"{EXCHANGE_NAME}_OPT_{inst['symbol']}"
            rows.append((
                instrument_id,
                self.exchange_id,
                inst["symbol"],
                inst["underlying"],
                inst.get("quote_currency", "USD"),
                inst["strike"],
                inst["expiry"],
                inst["option_type"],
                inst.get("settlement", "cash"),
                inst.get("contract_size", 1.0),
                inst.get("min_trade_amount", 0.1),
                inst.get("tick_size", 0.0005),
                inst.get("is_active", True),
                inst.get("creation_time"),
                inst.get("expiration_time"),
                json.dumps(inst.get("metadata", {})),
            ))

        query = """
            INSERT INTO options_instruments (
                instrument_id, exchange_id, symbol, underlying, quote_currency,
                strike, expiry, option_type, settlement, contract_size,
                min_trade_amount, tick_size, is_active, creation_time,
                expiration_time, metadata, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb, NOW())
            ON CONFLICT (instrument_id) DO UPDATE SET
                is_active = EXCLUDED.is_active,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
        """

        chunks = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
        for i, chunk in enumerate(chunks):
            await self.db.execute_many(query, chunk)
            if len(chunks) > 1:
                self._progress(f"  Stored batch {i+1}/{len(chunks)}")

        self.stats["instruments_stored"] += len(rows)
        self._progress(f"  ✅ Stored {len(rows)} {currency} instruments")
        return instruments

    # ==================== Book Summaries (Bulk Tickers) ====================

    async def fetch_book_summaries(self, currency: str) -> int:
        """Fetch book summaries (bulk tickers) for all options."""
        self._progress(f"📊 Fetching {currency} book summaries...")

        try:
            raw = self.client.get_book_summary_by_currency(currency=currency, kind="option")
        except DeribitClientError as e:
            logger.error(f"Failed to fetch book summaries: {e}")
            self.stats["errors"] += 1
            return 0

        parser = DeribitParser()
        summaries = parser.parse_book_summaries(raw)
        self.stats["tickers_fetched"] += len(summaries)
        self._progress(f"  Fetched {len(summaries)} {currency} summaries")

        if not summaries:
            return 0

        now = datetime.now(timezone.utc)
        rows = []
        for s in summaries:
            instrument_id = f"{EXCHANGE_NAME}_OPT_{s['instrument_name']}"
            iv_decimal = s["iv"] / 100.0 if s.get("iv") and s["iv"] > 0 else None
            rows.append((
                instrument_id,
                self.exchange_id,
                s.get("underlying", currency),
                s.get("mark_price"),
                s.get("last_price"),
                s.get("bid_price"),
                s.get("ask_price"),
                s.get("delta"),
                s.get("gamma"),
                s.get("theta"),
                s.get("vega"),
                s.get("rho"),
                iv_decimal,
                s.get("volume_24h", 0),
                s.get("open_interest", 0),
                s.get("underlying_price"),
                s.get("underlying_index", ""),
                now,
            ))

        query = """
            INSERT INTO options_tickers (
                instrument_id, exchange_id, underlying,
                mark_price, last_price, bid_price, ask_price,
                delta, gamma, theta, vega, rho, iv,
                volume_24h, open_interest,
                underlying_price, underlying_index, timestamp
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """

        chunks = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
        for i, chunk in enumerate(chunks):
            await self.db.execute_many(query, chunk)

        self.stats["tickers_stored"] += len(rows)
        self._progress(f"  ✅ Stored {len(rows)} {currency} ticker snapshots")
        return len(rows)

    # ==================== Detailed Tickers ====================

    async def fetch_detailed_tickers(self, currency: str, instruments: List[Dict]) -> int:
        """Fetch individual tickers with full greeks for top instruments."""
        # Filter to active, liquid instruments
        active = [i for i in instruments if i.get("is_active")]

        # Sort by some heuristic (closer expiry = more interesting)
        now = datetime.now(timezone.utc)
        active.sort(key=lambda x: abs((x.get("expiry") or now) - now).total_seconds())

        targets = active[:self.max_detailed]
        self._progress(f"🔍 Fetching detailed tickers for {len(targets)} {currency} instruments...")

        parser = DeribitParser()
        rows = []
        errors = 0

        for i, inst in enumerate(targets):
            name = inst["symbol"]
            try:
                raw = self.client.get_ticker(name)
                t = parser.parse_ticker(raw)

                instrument_id = f"{EXCHANGE_NAME}_OPT_{name}"
                iv_decimal = t["iv"] / 100.0 if t.get("iv") and t["iv"] > 0 else None

                rows.append((
                    instrument_id,
                    self.exchange_id,
                    currency,
                    t.get("mark_price"),
                    t.get("last_price"),
                    t.get("bid_price"),
                    t.get("ask_price"),
                    t.get("delta"),
                    t.get("gamma"),
                    t.get("theta"),
                    t.get("vega"),
                    t.get("rho"),
                    iv_decimal,
                    t.get("volume_24h", 0),
                    t.get("open_interest", 0),
                    t.get("underlying_price"),
                    t.get("underlying_index", ""),
                    t.get("timestamp") or now,
                ))

            except Exception as e:
                errors += 1
                if errors <= 3:
                    logger.warning(f"Failed ticker {name}: {e}")

            if (i + 1) % 25 == 0:
                self._progress(f"  Progress: {i+1}/{len(targets)}")

        if rows:
            query = """
                INSERT INTO options_tickers (
                    instrument_id, exchange_id, underlying,
                    mark_price, last_price, bid_price, ask_price,
                    delta, gamma, theta, vega, rho, iv,
                    volume_24h, open_interest,
                    underlying_price, underlying_index, timestamp
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
            """

            chunks = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
            for chunk in chunks:
                await self.db.execute_many(query, chunk)

        self.stats["detailed_tickers"] += len(rows)
        self._progress(f"  ✅ Stored {len(rows)} detailed tickers ({errors} errors)")
        return len(rows)

    # ==================== Volatility Surface ====================

    async def build_vol_surface(self, currency: str) -> int:
        """Build and store vol surface from latest ticker data."""
        self._progress(f"📐 Building {currency} volatility surface...")

        surface_data = await self.db.read("""
            SELECT
                oi.underlying,
                oi.expiry,
                oi.strike,
                oi.option_type,
                ot.iv,
                ot.delta,
                ot.underlying_price,
                ot.timestamp
            FROM options_tickers ot
            JOIN options_instruments oi ON oi.instrument_id = ot.instrument_id
            WHERE oi.underlying = $1
              AND ot.iv IS NOT NULL AND ot.iv > 0
              AND ot.timestamp = (
                  SELECT MAX(timestamp) FROM options_tickers WHERE underlying = $1
              )
            ORDER BY oi.expiry, oi.strike
        """, currency)

        if not surface_data:
            self._progress(f"  ⚠️  No vol surface data for {currency}")
            return 0

        rows = [(
            r["underlying"], r["expiry"], r["strike"], r["option_type"],
            r["iv"], r.get("delta"), r.get("underlying_price"), r["timestamp"],
        ) for r in surface_data]

        query = """
            INSERT INTO volatility_surface (
                underlying, expiry, strike, option_type, iv,
                delta, underlying_price, timestamp
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (underlying, expiry, strike, option_type, timestamp)
            DO UPDATE SET iv = EXCLUDED.iv, delta = EXCLUDED.delta
        """

        chunks = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
        for chunk in chunks:
            await self.db.execute_many(query, chunk)

        self.stats["vol_surface_points"] += len(rows)
        self._progress(f"  ✅ Stored {len(rows)} {currency} surface points")
        return len(rows)

    # ==================== Main ====================

    async def run(self):
        """Run the full fetch pipeline."""
        await self.init()

        print(f"\n{'='*60}")
        print(f"  Deribit Options Data Fetcher")
        print(f"  Currencies: {', '.join(self.currencies)}")
        print(f"  Expired: {self.include_expired} | Detailed: {self.detailed}")
        print(f"{'='*60}\n")

        try:
            for currency in self.currencies:
                print(f"\n{'─'*60}")
                print(f"  🔄 Processing {currency}")
                print(f"{'─'*60}")

                # Step 1: Instruments
                instruments = await self.fetch_instruments(currency)

                # Step 2: Book summaries (bulk tickers)
                await self.fetch_book_summaries(currency)

                # Step 3: Detailed tickers (optional)
                if self.detailed and instruments:
                    await self.fetch_detailed_tickers(currency, instruments)

                # Step 4: Vol surface
                await self.build_vol_surface(currency)

                time.sleep(1)  # Pause between currencies

        except Exception as e:
            logger.exception(f"Fatal error: {e}")
            self.stats["errors"] += 1
        finally:
            await self.cleanup()

        # Print summary
        elapsed = time.monotonic() - self.stats["start_time"]
        print(f"\n{'='*60}")
        print(f"  ✅ FETCH COMPLETE")
        print(f"{'─'*60}")
        print(f"  Instruments fetched:   {self.stats['instruments_fetched']:>8,}")
        print(f"  Instruments stored:    {self.stats['instruments_stored']:>8,}")
        print(f"  Tickers (bulk):        {self.stats['tickers_stored']:>8,}")
        print(f"  Tickers (detailed):    {self.stats['detailed_tickers']:>8,}")
        print(f"  Vol surface points:    {self.stats['vol_surface_points']:>8,}")
        print(f"  Errors:                {self.stats['errors']:>8,}")
        print(f"  Duration:              {elapsed:>7.1f}s")
        print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Fetch Deribit options data")
    parser.add_argument(
        "--currency", type=str, nargs="+", default=SUPPORTED_CURRENCIES,
        help="Currencies to fetch (default: BTC ETH)",
    )
    parser.add_argument(
        "--include-expired", action="store_true",
        help="Include expired instruments",
    )
    parser.add_argument(
        "--detailed", action="store_true",
        help="Fetch detailed individual tickers (slower, more data)",
    )
    parser.add_argument(
        "--max-detailed", type=int, default=200,
        help="Max instruments for detailed ticker fetch per currency",
    )

    args = parser.parse_args()

    fetcher = DeribitOptionsFetcher(
        currencies=args.currency,
        include_expired=args.include_expired,
        detailed=args.detailed,
        max_detailed_per_currency=args.max_detailed,
    )

    asyncio.run(fetcher.run())


if __name__ == "__main__":
    main()
