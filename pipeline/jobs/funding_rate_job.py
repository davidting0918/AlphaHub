"""
Funding Rate Job (Async)

Fetches funding rate history for PERP instruments and inserts into DB.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 --start 20260301 --end 20260313 funding_rate
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipeline.base_job import BaseJob

logger = logging.getLogger(__name__)


class FundingRateJob(BaseJob):
    JOB_NAME = "FundingRateJob"
    RATE_LIMIT_DELAY = 0.1  # 100ms between API calls

    async def _get_perp_instruments(self, exchange_id: int) -> List[Dict[str, Any]]:
        """Get all active PERP instruments for the exchange."""
        return await self.db.read("""
            SELECT instrument_id, symbol
            FROM instruments
            WHERE exchange_id = $1 AND type = 'PERP' AND is_active = TRUE
            ORDER BY symbol
        """, exchange_id)

    async def _get_latest_funding_time(self, instrument_id: str) -> Optional[int]:
        """Get the most recent funding_time as ms timestamp."""
        row = await self.db.read_one("""
            SELECT EXTRACT(EPOCH FROM funding_time) * 1000 AS funding_time_ms
            FROM funding_rates
            WHERE instrument_id = $1
            ORDER BY funding_time DESC
            LIMIT 1
        """, instrument_id)
        return int(row['funding_time_ms']) if row else None

    def _fetch_funding_history(
        self,
        symbol: str,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch funding rate history with pagination (sync — exchange API)."""
        all_rates = []
        before_cursor = end_ms

        while True:
            rates = self.exchange_client.getFundingRates(
                inst_id=symbol,
                limit=100,
                before=str(before_cursor) if before_cursor else None,
            )

            if not rates:
                break

            if start_ms:
                filtered = [
                    r for r in rates
                    if r['funding_time'] and r['funding_time'].timestamp() * 1000 >= start_ms
                ]
                all_rates.extend(filtered)
                if len(filtered) < len(rates):
                    break
            else:
                all_rates.extend(rates)

            if not start_ms and not end_ms:
                break

            oldest = min(
                rates,
                key=lambda r: r['funding_time'] if r['funding_time'] else datetime.max.replace(tzinfo=timezone.utc)
            )
            if oldest['funding_time']:
                new_cursor = int(oldest['funding_time'].timestamp() * 1000)
                if before_cursor and new_cursor >= before_cursor:
                    break
                before_cursor = new_cursor
            else:
                break

            time.sleep(self.RATE_LIMIT_DELAY)

            if len(all_rates) > 50000:
                logger.warning(f"Safety limit for {symbol}, stopping")
                break

        return all_rates

    async def _insert_funding_rates(
        self, exchange_id: int, instrument_id: str, rates: List[Dict[str, Any]]
    ) -> int:
        """Insert funding rates, skip duplicates. Returns new row count."""
        inserted = 0
        for rate in rates:
            if not rate['funding_time']:
                continue
            result = await self.db.execute("""
                INSERT INTO funding_rates (
                    exchange_id, instrument_id, funding_rate,
                    predicted_rate, funding_time, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (instrument_id, funding_time) DO NOTHING
            """,
                exchange_id,
                instrument_id,
                rate['funding_rate'],
                rate.get('next_funding_rate'),
                rate['funding_time'],
            )
            # asyncpg returns "INSERT 0 1" or "INSERT 0 0"
            if result and result.endswith("1"):
                inserted += 1
        return inserted

    async def run(self):
        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        start_ms = int(self.start.timestamp() * 1000) if self.start else None
        end_ms = int(self.end.timestamp() * 1000) if self.end else None

        total_inserted = 0
        instruments_processed = 0

        instruments = await self._get_perp_instruments(exchange_id)
        logger.info(f"Found {len(instruments)} active PERP instruments on {exchange_name}")

        for inst in instruments:
            symbol = inst['symbol']
            instrument_id = inst['instrument_id']

            effective_start = start_ms
            if not start_ms:
                effective_start = await self._get_latest_funding_time(instrument_id)

            logger.info(
                f"Fetching {symbol}" +
                (f" from {self.start}" if self.start else "") +
                (f" to {self.end}" if self.end else "")
            )

            # Sync exchange API call
            rates = self._fetch_funding_history(symbol, effective_start, end_ms)

            if rates:
                inserted = await self._insert_funding_rates(exchange_id, instrument_id, rates)
                total_inserted += inserted
                logger.info(f"  {symbol}: fetched {len(rates)}, inserted {inserted} new")

            instruments_processed += 1
            time.sleep(self.RATE_LIMIT_DELAY)

        mode = "range" if start_ms or end_ms else "incremental"
        summary = (
            f"Portfolio: {self.portfolio_name}\n"
            f"Exchange: {exchange_name}\n"
            f"Mode: {mode}\n"
            f"Instruments: {instruments_processed}\n"
            f"New rates: {total_inserted}"
        )
        logger.info(f"Complete: {summary}")
