"""
OKX Funding Rates Pipeline

Fetches funding rate history for OKX perpetual instruments and stores
them in the funding_rates table.

Usage:
    # Fetch latest rates for a portfolio's exchange
    python -m pipeline.okx_funding_rates --name my_portfolio --funding_rate

    # Fetch with date range
    python -m pipeline.okx_funding_rates --name my_portfolio --funding_rate --start 2026-01-01 --end 2026-03-01

    # Backfill all history
    python -m pipeline.okx_funding_rates --name my_portfolio --funding_rate --backfill
"""

import logging
import argparse
import time
from datetime import datetime, timezone
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

    RATE_LIMIT_DELAY = 0.1  # 100ms between API calls

    def __init__(
        self,
        portfolio_name: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        backfill: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.portfolio_name = portfolio_name
        self.start = start
        self.end = end
        self.backfill = backfill
        self.client = OKXClient()
        self.parser = OKXParser()

    def get_portfolio(self, cursor) -> Dict[str, Any]:
        """Look up portfolio by name, return portfolio + exchange info"""
        cursor.execute("""
            SELECT p.id AS portfolio_id,
                   p.name AS portfolio_name,
                   p.strategy_id,
                   p.exchange_id,
                   p.config,
                   e.name AS exchange_name
            FROM portfolios p
            JOIN exchanges e ON e.id = p.exchange_id
            WHERE p.name = %s
        """, (self.portfolio_name,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Portfolio '{self.portfolio_name}' not found in database")
        return dict(row)

    def get_perp_instruments(self, cursor, exchange_id: int) -> List[Dict[str, Any]]:
        """Get all active PERP instruments for the exchange"""
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
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch funding rate history for a symbol within time range.

        Args:
            symbol: OKX instrument ID (e.g., "BTC-USDT-SWAP")
            start_ms: Only fetch rates after this timestamp (ms)
            end_ms: Only fetch rates before this timestamp (ms)

        Returns:
            List of parsed funding rate records
        """
        all_rates = []
        before_cursor = end_ms if end_ms else None

        while True:
            response = self.client.get_funding_rate_history(
                inst_id=symbol,
                limit=100,
                before=str(before_cursor) if before_cursor else None,
            )

            rates = self.parser.parse_funding_rates(response)

            if not rates:
                break

            # Filter by start time
            if start_ms:
                rates = [
                    r for r in rates
                    if r['funding_time'] and r['funding_time'].timestamp() * 1000 >= start_ms
                ]
                # If we got rates older than start, we've gone past the range
                if len(rates) < len(self.parser.parse_funding_rates(response)):
                    all_rates.extend(rates)
                    break

            all_rates.extend(rates)

            # If not backfilling and no explicit date range, just get first page
            if not self.backfill and not start_ms:
                break

            # Paginate using the oldest funding time
            oldest_rate = min(
                self.parser.parse_funding_rates(response),
                key=lambda r: r['funding_time'] if r['funding_time'] else datetime.max.replace(tzinfo=timezone.utc)
            )
            if oldest_rate['funding_time']:
                new_cursor = int(oldest_rate['funding_time'].timestamp() * 1000)
                if before_cursor and new_cursor >= before_cursor:
                    break  # No progress, stop
                before_cursor = new_cursor
            else:
                break

            # Rate limiting
            time.sleep(self.RATE_LIMIT_DELAY)

            # Safety limit
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
        """Insert funding rates, ignoring duplicates. Returns count of new rows."""
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
        """Execute the funding rates pipeline"""
        try:
            total_inserted = 0
            instruments_processed = 0

            # Convert start/end to ms timestamps
            start_ms = int(self.start.timestamp() * 1000) if self.start else None
            end_ms = int(self.end.timestamp() * 1000) if self.end else None

            with self.get_cursor() as cursor:
                # Look up portfolio
                portfolio = self.get_portfolio(cursor)
                exchange_id = portfolio['exchange_id']
                exchange_name = portfolio['exchange_name']

                logger.info(
                    f"Portfolio: {portfolio['portfolio_name']} | "
                    f"Exchange: {exchange_name} (id={exchange_id})"
                )

                # Get instruments for this exchange
                instruments = self.get_perp_instruments(cursor, exchange_id)
                logger.info(f"Found {len(instruments)} active PERP instruments on {exchange_name}")

                for inst in instruments:
                    symbol = inst['symbol']
                    instrument_db_id = inst['id']

                    # For incremental sync (no backfill, no explicit range), use latest known time
                    effective_start_ms = start_ms
                    if not self.backfill and not start_ms:
                        effective_start_ms = self.get_latest_funding_time(cursor, instrument_db_id)

                    logger.info(
                        f"Fetching {symbol}" +
                        (f" from {self.start}" if self.start else "") +
                        (f" to {self.end}" if self.end else "") +
                        (" (backfill)" if self.backfill else "")
                    )

                    rates = self.fetch_funding_history(symbol, effective_start_ms, end_ms)

                    if rates:
                        inserted = self.insert_funding_rates(
                            cursor, exchange_id, instrument_db_id, rates
                        )
                        total_inserted += inserted
                        logger.info(f"  {symbol}: fetched {len(rates)}, inserted {inserted} new")
                    else:
                        logger.debug(f"  {symbol}: no new rates")

                    instruments_processed += 1
                    time.sleep(self.RATE_LIMIT_DELAY)

            # Report
            mode = "backfill" if self.backfill else ("range" if start_ms or end_ms else "incremental")
            summary = (
                f"Portfolio: {self.portfolio_name}\n"
                f"Mode: {mode}\n"
                f"Instruments: {instruments_processed}\n"
                f"New rates: {total_inserted}"
            )
            logger.info(f"Complete: {summary}")
            self.notify_success(summary)

        finally:
            self.client.close()


def parse_datetime(s: str) -> datetime:
    """Parse date string to UTC datetime"""
    for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s}")


def main():
    parser = argparse.ArgumentParser(description='Fetch OKX funding rates')
    parser.add_argument('--name', required=True, help='Portfolio name')
    parser.add_argument('--funding_rate', action='store_true', help='Fetch funding rates')
    parser.add_argument('--start', type=str, default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None, help='End date (YYYY-MM-DD)')
    parser.add_argument('--backfill', action='store_true', help='Fetch all history')
    args = parser.parse_args()

    if not args.funding_rate:
        parser.error("--funding_rate flag is required")

    start = parse_datetime(args.start) if args.start else None
    end = parse_datetime(args.end) if args.end else None

    pipeline = OKXFundingRatesPipeline(
        portfolio_name=args.name,
        start=start,
        end=end,
        backfill=args.backfill,
    )
    pipeline.execute()


if __name__ == '__main__':
    main()
