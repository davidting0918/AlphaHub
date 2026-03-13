"""
Instrument Job

Fetches SWAP and SPOT instruments from the exchange (resolved via portfolio)
and upserts into the instruments table.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 instrument
"""

import json
import logging
from typing import Any, Dict, List

from pipeline.base_job import BaseJob

logger = logging.getLogger(__name__)


class InstrumentJob(BaseJob):
    JOB_NAME = "InstrumentJob"

    def _upsert_instruments(
        self, cursor, exchange_id: int, instruments: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Batch upsert instruments into DB. Returns {'inserted': n, 'updated': n}."""
        if not instruments:
            return {'inserted': 0, 'updated': 0}

        from psycopg2.extras import execute_values

        # Deduplicate by instrument_id (keep last occurrence)
        seen = {}
        for inst in instruments:
            seen[inst['instrument_id']] = inst
        instruments = list(seen.values())

        # Prepare rows as tuples
        rows = []
        for inst in instruments:
            rows.append((
                inst['instrument_id'],
                exchange_id,
                inst['symbol'],
                inst['type'],
                inst['base_currency'],
                inst['quote_currency'],
                inst['settle_currency'],
                inst.get('contract_size'),
                inst.get('multiplier', 1),
                inst.get('min_size'),
                inst.get('is_active', True),
                inst.get('listing_time'),
                json.dumps(inst.get('metadata', {})),
            ))

        # Batch upsert
        sql = """
            INSERT INTO instruments (
                instrument_id, exchange_id, symbol, type,
                base_currency, quote_currency, settle_currency,
                contract_size, multiplier, min_size, is_active,
                listing_time, metadata, updated_at
            ) VALUES %s
            ON CONFLICT (instrument_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                contract_size = EXCLUDED.contract_size,
                multiplier = EXCLUDED.multiplier,
                min_size = EXCLUDED.min_size,
                is_active = EXCLUDED.is_active,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
        """

        template = "(%(0)s,%(1)s,%(2)s,%(3)s,%(4)s,%(5)s,%(6)s,%(7)s,%(8)s,%(9)s,%(10)s,%(11)s,%(12)s,NOW())"

        # Use execute_values for batch performance
        execute_values(
            cursor, sql, rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
            page_size=200,
        )

        total = len(rows)
        return {'inserted': total, 'updated': 0}  # can't distinguish in batch

    def run(self):
        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        # Fetch instruments from exchange using high-level method
        # getInstruments() returns parsed data with correct instrument_id prefix
        logger.info(f"Fetching SWAP instruments from {exchange_name}...")
        swap_instruments = self.exchange_client.getInstruments(inst_type="SWAP")
        logger.info(f"Fetched {len(swap_instruments)} SWAP instruments")

        logger.info(f"Fetching SPOT instruments from {exchange_name}...")
        spot_instruments = self.exchange_client.getInstruments(inst_type="SPOT")
        logger.info(f"Fetched {len(spot_instruments)} SPOT instruments")

        all_instruments = swap_instruments + spot_instruments

        # Upsert to DB
        with self.get_cursor() as cursor:
            stats = self._upsert_instruments(cursor, exchange_id, all_instruments)

        summary = (
            f"Portfolio: {self.portfolio_name}\n"
            f"Exchange: {exchange_name}\n"
            f"SWAP: {len(swap_instruments)}, SPOT: {len(spot_instruments)}\n"
            f"Inserted: {stats['inserted']}, Updated: {stats['updated']}"
        )
        logger.info(f"Complete: {summary}")
