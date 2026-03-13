"""
Funding Rate Job (Async)

Fetches funding rate history for PERP instruments and batch inserts into DB.
Supports OKX and Binance pagination styles via adaptor dispatch.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 funding_rate
    python3 -m pipeline.job_manager --name BINANCEFUTURES_MAIN_01 funding_rate
    python3 -m pipeline.job_manager --name OKX_MAIN_01 --start 20260101 --end 20260301 funding_rate
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipeline.base_job import BaseJob

logger = logging.getLogger(__name__)

MAX_RECORDS_PER_INSTRUMENT = 100_000


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

    async def _get_latest_funding_time(self, instrument_id: str) -> Optional[datetime]:
        """Get the most recent funding_time for incremental fetching."""
        row = await self.db.read_one("""
            SELECT funding_time FROM funding_rates
            WHERE instrument_id = $1
            ORDER BY funding_time DESC LIMIT 1
        """, instrument_id)
        return row['funding_time'] if row else None

    # ==================== Fetch (per adaptor) ====================

    def _fetch_okx(
        self, symbol: str, start_ms: Optional[int], end_ms: Optional[int]
    ) -> List[Dict[str, Any]]:
        """OKX: paginate backward from latest, limit 100 per page."""
        all_rates = []
        cursor = end_ms

        while True:
            kwargs = {"inst_id": symbol, "limit": 100}
            if cursor:
                kwargs["before"] = str(cursor)

            rates = self.exchange_client.getFundingRates(**kwargs)
            if not rates:
                break

            if start_ms:
                filtered = [r for r in rates if r['funding_time'] and r['funding_time'].timestamp() * 1000 >= start_ms]
                all_rates.extend(filtered)
                if len(filtered) < len(rates):
                    break
            else:
                all_rates.extend(rates)

            oldest = min(rates, key=lambda r: r['funding_time'] or datetime.max.replace(tzinfo=timezone.utc))
            if oldest['funding_time']:
                new_cursor = int(oldest['funding_time'].timestamp() * 1000)
                if cursor and new_cursor >= cursor:
                    break
                cursor = new_cursor
            else:
                break

            time.sleep(self.RATE_LIMIT_DELAY)
            if len(all_rates) >= MAX_RECORDS_PER_INSTRUMENT:
                logger.warning(f"Safety limit for {symbol} at {len(all_rates)}")
                break

        return all_rates

    def _fetch_binance(
        self, symbol: str, start_ms: Optional[int], end_ms: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Binance: paginate forward from startTime, limit 1000 per page."""
        all_rates = []
        cursor = start_ms

        while True:
            rates = self.exchange_client.getFundingRates(
                inst_id=symbol, limit=1000,
                start_time=cursor, end_time=end_ms,
            )
            if not rates:
                break

            all_rates.extend(rates)
            if len(rates) < 1000:
                break

            newest = max(rates, key=lambda r: r['funding_time'] or datetime.min.replace(tzinfo=timezone.utc))
            if newest['funding_time']:
                new_cursor = int(newest['funding_time'].timestamp() * 1000) + 1
                if cursor and new_cursor <= cursor:
                    break
                cursor = new_cursor
            else:
                break

            time.sleep(self.RATE_LIMIT_DELAY)
            if len(all_rates) >= MAX_RECORDS_PER_INSTRUMENT:
                logger.warning(f"Safety limit for {symbol} at {len(all_rates)}")
                break

        return all_rates

    _FETCH_DISPATCH = {
        "okx": _fetch_okx,
        "binance": _fetch_binance,
    }

    def _fetch_funding_history(
        self, symbol: str, start_ms: Optional[int], end_ms: Optional[int]
    ) -> List[Dict[str, Any]]:
        adaptor = self.portfolio["adaptor"]
        fetch_fn = self._FETCH_DISPATCH.get(adaptor)
        if not fetch_fn:
            raise ValueError(f"Funding rate fetch not implemented for adaptor: {adaptor}")
        return fetch_fn(self, symbol, start_ms, end_ms)

    # ==================== Batch Insert ====================

    async def _batch_insert_funding_rates(
        self, exchange_id: int, instrument_id: str, rates: List[Dict[str, Any]]
    ) -> int:
        """Batch insert funding rates using executemany. Skips duplicates."""
        valid = [r for r in rates if r.get('funding_time')]
        if not valid:
            return 0

        # Deduplicate by funding_time
        seen = {}
        for r in valid:
            seen[r['funding_time']] = r
        unique = list(seen.values())

        rows = [
            (exchange_id, instrument_id, r['funding_rate'],
             r.get('next_funding_rate'), r['funding_time'])
            for r in unique
        ]

        await self.db.execute_many("""
            INSERT INTO funding_rates (
                exchange_id, instrument_id, funding_rate,
                predicted_rate, funding_time, updated_at
            ) VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (instrument_id, funding_time) DO NOTHING
        """, rows)

        return len(rows)

    # ==================== Run ====================

    async def run(self):
        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        start_ms = int(self.start.timestamp() * 1000) if self.start else None
        end_ms = int(self.end.timestamp() * 1000) if self.end else None

        total_inserted = 0
        total_fetched = 0

        instruments = await self._get_perp_instruments(exchange_id)
        logger.info(f"Found {len(instruments)} active PERP instruments on {exchange_name}")

        for inst in instruments:
            symbol = inst['symbol']
            instrument_id = inst['instrument_id']

            effective_start = start_ms
            if not start_ms:
                latest = await self._get_latest_funding_time(instrument_id)
                if latest:
                    effective_start = int(latest.timestamp() * 1000) + 1
                    logger.info(f"  {symbol}: incremental from {latest}")

            logger.info(
                f"Fetching {symbol}"
                + (f" from {datetime.fromtimestamp(effective_start / 1000, tz=timezone.utc)}" if effective_start else " (all history)")
                + (f" to {self.end}" if self.end else "")
            )

            rates = self._fetch_funding_history(symbol, effective_start, end_ms)
            total_fetched += len(rates)

            if rates:
                inserted = await self._batch_insert_funding_rates(exchange_id, instrument_id, rates)
                total_inserted += inserted
                logger.info(f"  {symbol}: fetched {len(rates)}, inserted {inserted}")
            else:
                logger.info(f"  {symbol}: no new rates")

            time.sleep(self.RATE_LIMIT_DELAY)

        logger.info(
            f"Complete: {self.portfolio_name} | {exchange_name} | "
            f"instruments={len(instruments)} | fetched={total_fetched} | inserted={total_inserted}"
        )
