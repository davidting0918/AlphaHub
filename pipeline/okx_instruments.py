"""
OKX Instruments Pipeline

Fetches SWAP (perpetual) and SPOT instruments from OKX and upserts them
into the instruments table.

Usage:
    python -m pipeline.okx_instruments
"""

import logging
import json
import argparse
from typing import Dict, Any, List

from .base import BasePipeline
from adaptor.okx import OKXClient, OKXParser


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OKXInstrumentsPipeline(BasePipeline):
    """Pipeline to fetch and upsert OKX instruments"""
    
    EXCHANGE_NAME = 'okx'
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = OKXClient()
        self.parser = OKXParser()
    
    def ensure_exchange(self, cursor) -> int:
        """Ensure OKX exchange exists in exchanges table, return exchange_id"""
        cursor.execute("""
            INSERT INTO exchanges (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
            RETURNING id
        """, (self.EXCHANGE_NAME,))
        row = cursor.fetchone()
        return row['id'] if row else row[0]
    
    def upsert_instruments(
        self,
        cursor,
        exchange_id: int,
        instruments: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Upsert instruments into database
        
        Returns:
            Dict with 'inserted' and 'updated' counts
        """
        inserted = 0
        updated = 0
        
        for inst in instruments:
            cursor.execute("""
                INSERT INTO instruments (
                    instrument_id, exchange_id, symbol, type,
                    base_currency, quote_currency, settle_currency,
                    contract_size, multiplier, min_size, is_active,
                    listing_time, metadata, updated_at
                ) VALUES (
                    %(instrument_id)s, %(exchange_id)s, %(symbol)s, %(type)s,
                    %(base_currency)s, %(quote_currency)s, %(settle_currency)s,
                    %(contract_size)s, %(multiplier)s, %(min_size)s, %(is_active)s,
                    %(listing_time)s, %(metadata)s, NOW()
                )
                ON CONFLICT (instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    contract_size = EXCLUDED.contract_size,
                    multiplier = EXCLUDED.multiplier,
                    min_size = EXCLUDED.min_size,
                    is_active = EXCLUDED.is_active,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
            """, {
                **inst,
                'exchange_id': exchange_id,
                'metadata': json.dumps(inst.get('metadata', {}))
            })
            
            row = cursor.fetchone()
            if row and row[0]:
                inserted += 1
            else:
                updated += 1
        
        return {'inserted': inserted, 'updated': updated}
    
    def run(self):
        """Execute the instruments sync pipeline"""
        try:
            # Fetch instruments from OKX
            logger.info("Fetching SWAP instruments from OKX...")
            swap_response = self.client.get_instruments(inst_type="SWAP")
            swap_instruments = self.parser.parse_instruments(swap_response, inst_type="SWAP")
            logger.info(f"Fetched {len(swap_instruments)} SWAP instruments")
            
            logger.info("Fetching SPOT instruments from OKX...")
            spot_response = self.client.get_instruments(inst_type="SPOT")
            spot_instruments = self.parser.parse_instruments(spot_response, inst_type="SPOT")
            logger.info(f"Fetched {len(spot_instruments)} SPOT instruments")
            
            all_instruments = swap_instruments + spot_instruments
            
            # Upsert to database
            with self.get_cursor() as cursor:
                exchange_id = self.ensure_exchange(cursor)
                logger.info(f"Using exchange_id={exchange_id} for OKX")
                
                stats = self.upsert_instruments(cursor, exchange_id, all_instruments)
            
            # Report success
            summary = (
                f"SWAP: {len(swap_instruments)}, SPOT: {len(spot_instruments)}\n"
                f"Inserted: {stats['inserted']}, Updated: {stats['updated']}"
            )
            logger.info(f"Sync complete: {summary}")
            self.notify_success(summary)
            
        finally:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(description='Sync OKX instruments to database')
    parser.parse_args()
    
    pipeline = OKXInstrumentsPipeline()
    pipeline.execute()


if __name__ == '__main__':
    main()
