"""
Options Data Job (Async)

Fetches options instruments and ticker data from Deribit for BTC and ETH.
Stores data in options_instruments, options_tickers, and volatility_surface tables.

This job is standalone — it does NOT use the BaseJob portfolio resolution
because Deribit is an options exchange not (yet) in the portfolio/exchange tables.
It directly instantiates the Deribit client and DB.

Usage:
    python3 -m pipeline.jobs.options_data_job
    python3 -m pipeline.jobs.options_data_job --currency BTC --start 20250101
"""

import asyncio
import argparse
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from database.client import PostgresClient
from adaptor.deribit.client import DeribitClient
from adaptor.deribit.parser import DeribitParser
from adaptor.deribit.config import (
    EXCHANGE_NAME,
    SUPPORTED_CURRENCIES,
    RATE_LIMIT_DELAY,
    INDEX_MAP,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 200


class OptionsDataJob:
    """
    Fetch and store Deribit options data.

    Steps:
    1. Ensure Deribit exchange exists in exchanges table (get or create exchange_id)
    2. Fetch all active options instruments for BTC + ETH
    3. Upsert into options_instruments
    4. Fetch book summaries (bulk ticker snapshots) for all instruments
    5. Insert into options_tickers
    6. Build and insert volatility_surface entries
    """

    JOB_NAME = "OptionsDataJob"

    def __init__(
        self,
        currencies: Optional[List[str]] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        include_expired: bool = False,
    ):
        self.currencies = currencies or SUPPORTED_CURRENCIES
        self.start = start
        self.end = end or datetime.now(timezone.utc)
        self.include_expired = include_expired

        self.db: Optional[PostgresClient] = None
        self.client: Optional[DeribitClient] = None
        self.exchange_id: Optional[int] = None

    async def setup(self):
        """Initialize DB and exchange client."""
        self.db = PostgresClient()
        await self.db.init_pool()
        self.client = DeribitClient(rate_limit_delay=RATE_LIMIT_DELAY)

        # Ensure DERIBIT exchange exists in exchanges table
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
            logger.info(f"Created exchange '{EXCHANGE_NAME}' with id={self.exchange_id}")

        logger.info(f"[{self.JOB_NAME}] Setup complete — exchange_id={self.exchange_id}")

    async def teardown(self):
        """Clean up resources."""
        if self.client:
            self.client.close()
        if self.db:
            await self.db.close()

    # ==================== Instruments ====================

    async def fetch_and_store_instruments(self, currency: str) -> List[Dict[str, Any]]:
        """Fetch options instruments for a currency and upsert into DB."""
        logger.info(f"Fetching {currency} options instruments (expired={self.include_expired})...")

        raw_instruments = self.client.get_instruments(
            currency=currency,
            kind="option",
            expired=self.include_expired,
        )
        parser = DeribitParser()
        instruments = parser.parse_instruments(raw_instruments)

        logger.info(f"Fetched {len(instruments)} {currency} options instruments")

        if not instruments:
            return []

        # Prepare rows for batch upsert
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
        await asyncio.gather(*[self.db.execute_many(query, c) for c in chunks])

        logger.info(f"Upserted {len(rows)} {currency} options instruments")
        return instruments

    # ==================== Tickers (Book Summaries) ====================

    async def fetch_and_store_tickers(self, currency: str) -> int:
        """
        Fetch book summaries (bulk ticker snapshots) and insert into options_tickers.

        Book summary includes: mark_price, bid, ask, IV, volume, OI for ALL
        active instruments of a currency.
        """
        logger.info(f"Fetching {currency} options book summaries...")

        raw_summaries = self.client.get_book_summary_by_currency(
            currency=currency, kind="option"
        )
        parser = DeribitParser()
        summaries = parser.parse_book_summaries(raw_summaries)

        logger.info(f"Fetched {len(summaries)} {currency} book summaries")

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
        await asyncio.gather(*[self.db.execute_many(query, c) for c in chunks])

        logger.info(f"Inserted {len(rows)} {currency} ticker snapshots")
        return len(rows)

    # ==================== Volatility Surface ====================

    async def build_and_store_vol_surface(self, currency: str) -> int:
        """
        Build volatility surface from the latest ticker data.

        Reads options_tickers for the most recent timestamp and groups by
        (expiry, strike, option_type) to build the IV surface.
        """
        logger.info(f"Building {currency} volatility surface...")

        # Get latest ticker data joined with instrument metadata
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
              AND ot.iv IS NOT NULL
              AND ot.iv > 0
              AND ot.timestamp = (
                  SELECT MAX(timestamp) FROM options_tickers
                  WHERE underlying = $1
              )
            ORDER BY oi.expiry, oi.strike
        """, currency)

        if not surface_data:
            logger.info(f"No vol surface data available for {currency}")
            return 0

        rows = []
        for row in surface_data:
            rows.append((
                row["underlying"],
                row["expiry"],
                row["strike"],
                row["option_type"],
                row["iv"],
                row.get("delta"),
                row.get("underlying_price"),
                row["timestamp"],
            ))

        query = """
            INSERT INTO volatility_surface (
                underlying, expiry, strike, option_type, iv,
                delta, underlying_price, timestamp
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (underlying, expiry, strike, option_type, timestamp)
            DO UPDATE SET iv = EXCLUDED.iv, delta = EXCLUDED.delta
        """

        chunks = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
        await asyncio.gather(*[self.db.execute_many(query, c) for c in chunks])

        logger.info(f"Stored {len(rows)} {currency} vol surface points")
        return len(rows)

    # ==================== Detailed Ticker Fetch (individual) ====================

    async def fetch_detailed_tickers(
        self,
        instrument_names: List[str],
        currency: str,
        max_instruments: int = 500,
    ) -> int:
        """
        Fetch individual ticker data for specific instruments.

        Uses /public/ticker for full greeks. More detailed than book summary
        but requires one API call per instrument. Use for targeted fetches.
        """
        targets = instrument_names[:max_instruments]
        logger.info(f"Fetching detailed tickers for {len(targets)} {currency} instruments...")

        now = datetime.now(timezone.utc)
        parser = DeribitParser()
        rows = []
        errors = 0

        for i, name in enumerate(targets):
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
                if errors <= 5:
                    logger.warning(f"Failed to fetch ticker for {name}: {e}")

            if (i + 1) % 50 == 0:
                logger.info(f"  Progress: {i + 1}/{len(targets)} tickers fetched")

        if not rows:
            return 0

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
        await asyncio.gather(*[self.db.execute_many(query, c) for c in chunks])

        logger.info(f"Inserted {len(rows)} detailed tickers ({errors} errors)")
        return len(rows)

    # ==================== Main Execution ====================

    async def run(self):
        """Execute the full data pipeline."""
        total_instruments = 0
        total_tickers = 0
        total_surface = 0

        for currency in self.currencies:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {currency} options")
            logger.info(f"{'='*60}")

            # Step 1: Fetch instruments
            instruments = await self.fetch_and_store_instruments(currency)
            total_instruments += len(instruments)

            # Step 2: Fetch book summaries (bulk tickers)
            ticker_count = await self.fetch_and_store_tickers(currency)
            total_tickers += ticker_count

            # Step 3: Build volatility surface
            surface_count = await self.build_and_store_vol_surface(currency)
            total_surface += surface_count

            time.sleep(1)  # Brief pause between currencies

        logger.info(f"\n{'='*60}")
        logger.info(f"OptionsDataJob Complete")
        logger.info(f"  Instruments: {total_instruments}")
        logger.info(f"  Tickers:     {total_tickers}")
        logger.info(f"  Vol Surface: {total_surface}")
        logger.info(f"{'='*60}")

    async def execute(self):
        """Full lifecycle: setup → run → teardown."""
        try:
            await self.setup()
            await self.run()
        except Exception as e:
            logger.exception(f"[{self.JOB_NAME}] Failed: {e}")
            raise
        finally:
            await self.teardown()


# ==================== CLI Entry Point ====================

async def async_main():
    parser = argparse.ArgumentParser(description="Deribit Options Data Fetcher")
    parser.add_argument(
        "--currency", type=str, nargs="+", default=None,
        help="Currencies to fetch (default: BTC ETH)"
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start date (YYYYMMDD or YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date (YYYYMMDD or YYYY-MM-DD)"
    )
    parser.add_argument(
        "--include-expired", action="store_true",
        help="Include expired instruments"
    )

    args = parser.parse_args()

    start = None
    end = None
    if args.start:
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                start = datetime.strptime(args.start, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
    if args.end:
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                end = datetime.strptime(args.end, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue

    job = OptionsDataJob(
        currencies=args.currency,
        start=start,
        end=end,
        include_expired=args.include_expired,
    )
    await job.execute()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
