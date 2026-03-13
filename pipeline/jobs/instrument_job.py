"""
Instrument Job (Async)

Fetches perpetual instruments from the exchange and batch upserts into DB.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 instrument
    python3 -m pipeline.job_manager --name BINANCEFUTURES_MAIN_01 instrument
"""

import json
import logging
from typing import Any, Dict, List

from pipeline.base_job import BaseJob

logger = logging.getLogger(__name__)


class InstrumentJob(BaseJob):
    JOB_NAME = "InstrumentJob"

    async def _upsert_instruments(
        self, exchange_id: int, instruments: List[Dict[str, Any]]
    ) -> int:
        """Batch upsert instruments into DB using executemany."""
        if not instruments:
            return 0

        # Deduplicate by instrument_id (keep last occurrence)
        seen = {}
        for inst in instruments:
            seen[inst['instrument_id']] = inst
        unique = list(seen.values())

        rows = [
            (
                inst['instrument_id'], exchange_id,
                inst['symbol'], inst['type'],
                inst['base_currency'], inst['quote_currency'],
                inst['settle_currency'], inst.get('contract_size'),
                inst.get('multiplier', 1), inst.get('min_size'),
                inst.get('is_active', True), inst.get('listing_time'),
                json.dumps(inst.get('metadata', {})),
            )
            for inst in unique
        ]

        await self.db.execute_many("""
            INSERT INTO instruments (
                instrument_id, exchange_id, symbol, type,
                base_currency, quote_currency, settle_currency,
                contract_size, multiplier, min_size, is_active,
                listing_time, metadata, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, NOW())
            ON CONFLICT (instrument_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                contract_size = EXCLUDED.contract_size,
                multiplier = EXCLUDED.multiplier,
                min_size = EXCLUDED.min_size,
                is_active = EXCLUDED.is_active,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
        """, rows)

        return len(rows)

    async def run(self):
        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        logger.info(f"Fetching perpetual instruments from {exchange_name}...")
        instruments = self.exchange_client.getInstruments()
        logger.info(f"Fetched {len(instruments)} perpetual instruments from {exchange_name}")

        count = await self._upsert_instruments(exchange_id, instruments)

        logger.info(
            f"Complete: {self.portfolio_name} | {exchange_name} | "
            f"fetched={len(instruments)} | upserted={count}"
        )
