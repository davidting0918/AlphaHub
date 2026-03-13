"""
Funding Rate Job

Fetches funding rate history for PERP instruments on the exchange
(resolved via portfolio) and inserts into funding_rates table.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 --start 20260101 --end 20260301 funding_rate
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from adaptor.okx.parser import OKXParser
from pipeline.base_job import BaseJob

logger = logging.getLogger(__name__)


class FundingRateJob(BaseJob):
    JOB_NAME = "FundingRateJob"
    RATE_LIMIT_DELAY = 0.1  # 100ms between API calls

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser = OKXParser()

    def _get_perp_instruments(self, cursor, exchange_id: int) -> List[Dict[str, Any]]:
        """Get all active PERP instruments for the exchange"""
        cursor.execute("""
            SELECT id, instrument_id, symbol
            FROM instruments
            WHERE exchange_id = %s AND type = 'PERP' AND is_active = TRUE
            ORDER BY symbol
        """, (exchange_id,))
        return [dict(row) for row in cursor.fetchall()]

    def _get_latest_funding_time(self, cursor, instrument_db_id: int) -> Optional[int]:
        """Get the most recent funding_time for an instrument (as ms timestamp)"""
        cursor.execute("""
            SELECT EXTRACT(EPOCH FROM funding_time) * 1000 AS funding_time_ms
            FROM funding_rates
            WHERE instrument_id = %s
            ORDER BY funding_time DESC
            LIMIT 1
        """, (instrument_db_id,))
        row = cursor.fetchone()
        return int(row['funding_time_ms']) if row else None

    def _fetch_funding_history(
        self,
        symbol: str,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch funding rate history with pagination."""
        all_rates = []
        before_cursor = end_ms

        while True:
            response = self.exchange_client.get_funding_rate_history(
                inst_id=symbol,
                limit=100,
                before=str(before_cursor) if before_cursor else None,
            )

            rates = self.parser.parse_funding_rates(response)
            if not rates:
                break

            # Filter by start time
            if start_ms:
                filtered = [
                    r for r in rates
                    if r['funding_time'] and r['funding_time'].timestamp() * 1000 >= start_ms
                ]
                all_rates.extend(filtered)
                # If some rates were filtered out, we've gone past start
                if len(filtered) < len(rates):
                    break
            else:
                all_rates.extend(rates)

            # If no explicit range, just get first page (incremental)
            if not start_ms and not end_ms:
                break

            # Paginate
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

    def _insert_funding_rates(
        self, cursor, exchange_id: int, instrument_db_id: int, rates: List[Dict[str, Any]]
    ) -> int:
        """Insert funding rates, skip duplicates. Returns new row count."""
        inserted = 0
        for rate in rates:
            if not rate['funding_time']:
                continue
            cursor.execute("""
                INSERT INTO funding_rates (
                    exchange_id, instrument_id, funding_rate,
                    predicted_rate, funding_time, updated_at
                ) VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (instrument_id, funding_time) DO NOTHING
                RETURNING id
            """, (
                exchange_id,
                instrument_db_id,
                rate['funding_rate'],
                rate.get('next_funding_rate'),
                rate['funding_time'],
            ))
            if cursor.fetchone():
                inserted += 1
        return inserted

    def run(self):
        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        start_ms = int(self.start.timestamp() * 1000) if self.start else None
        end_ms = int(self.end.timestamp() * 1000) if self.end else None

        total_inserted = 0
        instruments_processed = 0

        with self.get_cursor() as cursor:
            instruments = self._get_perp_instruments(cursor, exchange_id)
            logger.info(f"Found {len(instruments)} active PERP instruments on {exchange_name}")

            for inst in instruments:
                symbol = inst['symbol']
                instrument_db_id = inst['id']

                # Incremental: use latest known time if no explicit start
                effective_start = start_ms
                if not start_ms:
                    effective_start = self._get_latest_funding_time(cursor, instrument_db_id)

                logger.info(
                    f"Fetching {symbol}" +
                    (f" from {self.start}" if self.start else "") +
                    (f" to {self.end}" if self.end else "")
                )

                rates = self._fetch_funding_history(symbol, effective_start, end_ms)

                if rates:
                    inserted = self._insert_funding_rates(cursor, exchange_id, instrument_db_id, rates)
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
        self.notify_success(summary)
