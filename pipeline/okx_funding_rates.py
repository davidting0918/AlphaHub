"""
OKX Funding Rates Pipeline

Fetches funding rate history for OKX perpetual instruments and stores
them in the funding_rates table.

Usage:
    python -m pipeline.okx_funding_rates           # Latest rates only
    python -m pipeline.okx_funding_rates --backfill  # Full history
"""

import logging
import argparse
import time
from typing import Dict, Any, List, Optional

from .base import BasePipeline
from adaptor.okx import OKXClient, OKXParser


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OKXFundingRatesPipeline(BasePipeline):
    """Pipeline to fetch and store OKX funding rates"""
    
    EXCHANGE_NAME = 'okx'
    RATE_LIMIT_DELAY = 0.1  # 100ms between API calls to respect rate limits
    
    def __init__(self, backfill: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.backfill = backfill
        self.client = OKXClient()
        self.parser = OKXParser()
    
    def get_exchange_id(self, cursor) -> Optional[int]:
        """Get OKX exchange ID from database"""
        cursor.execute(
            "SELECT id FROM exchanges WHERE name = %s",
            (self.EXCHANGE_NAME,)
        )
        row = cursor.fetchone()
        return row['id'] if row else None
    
    def get_perp_instruments(self, cursor, exchange_id: int) -> List[Dict[str, Any]]:
        """Get all active PERP instruments for OKX"""
        cursor.execute("""
            SELECT id, instrument_id, symbol
            FROM instruments
            WHERE exchange_id = %s AND type = 'PERP' AND is_active = TRUE
            ORDER BY symbol
        """, (exchange_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_latest_funding_time(self, cursor, instrument_db_id: int) -> Optional[int]:
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
    
    def fetch_funding_history(
        self,
        symbol: str,
        after_ms: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch funding rate history for a symbol
        
        Args:
            symbol: OKX instrument ID (e.g., "BTC-USDT-SWAP")
            after_ms: Only fetch rates after this timestamp (for incremental)
            
        Returns:
            List of parsed funding rate records
        """
        all_rates = []
        before_cursor = None
        
        while True:
            response = self.client.get_funding_rate_history(
                inst_id=symbol,
                limit=100,
                before=before_cursor
            )
            
            rates = self.parser.parse_funding_rates(response)
            
            if not rates:
                break
            
            # Filter rates after our cutoff if doing incremental sync
            if after_ms and not self.backfill:
                rates = [r for r in rates if r['funding_time'] and 
                        r['funding_time'].timestamp() * 1000 > after_ms]
                all_rates.extend(rates)
                break  # Only one page for incremental
            
            all_rates.extend(rates)
            
            # If not backfilling, just get the first page
            if not self.backfill:
                break
            
            # Paginate using the oldest funding time
            oldest_rate = min(rates, key=lambda r: r['funding_time'] if r['funding_time'] else float('inf'))
            if oldest_rate['funding_time']:
                before_cursor = str(int(oldest_rate['funding_time'].timestamp() * 1000))
            else:
                break
            
            # Rate limiting
            time.sleep(self.RATE_LIMIT_DELAY)
            
            # Safety check - OKX has ~3 years of data max
            if len(all_rates) > 50000:
                logger.warning(f"Hit safety limit for {symbol}, stopping pagination")
                break
        
        return all_rates
    
    def insert_funding_rates(
        self,
        cursor,
        exchange_id: int,
        instrument_db_id: int,
        rates: List[Dict[str, Any]]
    ) -> int:
        """
        Insert funding rates, ignoring duplicates
        
        Returns:
            Number of new rows inserted
        """
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
                rate['funding_time']
            ))
            
            if cursor.fetchone():
                inserted += 1
        
        return inserted
    
    def run(self):
        """Execute the funding rates sync pipeline"""
        try:
            total_inserted = 0
            instruments_processed = 0
            
            with self.get_cursor() as cursor:
                exchange_id = self.get_exchange_id(cursor)
                if not exchange_id:
                    raise ValueError("OKX exchange not found in database. Run okx_instruments first.")
                
                instruments = self.get_perp_instruments(cursor, exchange_id)
                logger.info(f"Found {len(instruments)} active PERP instruments")
                
                for inst in instruments:
                    symbol = inst['symbol']
                    instrument_db_id = inst['id']
                    
                    # For incremental sync, get the latest known funding time
                    after_ms = None
                    if not self.backfill:
                        after_ms = self.get_latest_funding_time(cursor, instrument_db_id)
                    
                    logger.info(f"Fetching funding rates for {symbol}" + 
                               (f" (after {after_ms})" if after_ms else " (full history)" if self.backfill else ""))
                    
                    rates = self.fetch_funding_history(symbol, after_ms)
                    
                    if rates:
                        inserted = self.insert_funding_rates(
                            cursor, exchange_id, instrument_db_id, rates
                        )
                        total_inserted += inserted
                        logger.info(f"  {symbol}: fetched {len(rates)}, inserted {inserted} new rates")
                    else:
                        logger.debug(f"  {symbol}: no new rates")
                    
                    instruments_processed += 1
                    
                    # Rate limiting between instruments
                    time.sleep(self.RATE_LIMIT_DELAY)
            
            # Report success
            mode = "backfill" if self.backfill else "incremental"
            summary = (
                f"Mode: {mode}\n"
                f"Instruments: {instruments_processed}\n"
                f"New rates: {total_inserted}"
            )
            logger.info(f"Sync complete: {summary}")
            self.notify_success(summary)
            
        finally:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(description='Sync OKX funding rates to database')
    parser.add_argument(
        '--backfill',
        action='store_true',
        help='Fetch full history (paginate all available data)'
    )
    args = parser.parse_args()
    
    pipeline = OKXFundingRatesPipeline(backfill=args.backfill)
    pipeline.execute()


if __name__ == '__main__':
    main()
