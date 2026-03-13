"""
Kline (Candlestick) Job

Fetches kline/candlestick data for PERP instruments and batch inserts into DB.
Supports OKX and Binance with configurable interval.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 kline
    python3 -m pipeline.job_manager --name BINANCEFUTURES_MAIN_01 kline
    python3 -m pipeline.job_manager --name OKX_MAIN_01 --start 20260101 kline

Environment:
    KLINE_INTERVAL: candle interval (default: "4h")
        OKX format:  "1m","5m","15m","1H","4H","1D","1W"
        Binance fmt: "1m","5m","15m","1h","4h","1d","1w"
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from pipeline.base_job import BaseJob

logger = logging.getLogger(__name__)

MAX_RECORDS_PER_INSTRUMENT = 50_000

# Map our normalized interval to exchange-specific format
INTERVAL_MAP_OKX = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6Hutc", "12h": "12Hutc",
    "1d": "1Dutc", "1w": "1Wutc",
}

INTERVAL_MAP_BINANCE = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
    "1d": "1d", "3d": "3d", "1w": "1w",
}

# Interval → duration in ms (for close_time calculation)
INTERVAL_DURATION_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
    "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
    "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000,
}


class KlineJob(BaseJob):
    JOB_NAME = "KlineJob"
    RATE_LIMIT_DELAY = 0.1  # 100ms between API calls

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interval = os.environ.get("KLINE_INTERVAL", "4h")

    async def _get_perp_instruments(self, exchange_id: int) -> List[Dict[str, Any]]:
        """Get all active PERP instruments for the exchange."""
        return await self.db.read("""
            SELECT instrument_id, symbol
            FROM instruments
            WHERE exchange_id = $1 AND type = 'PERP' AND is_active = TRUE
            ORDER BY symbol
        """, exchange_id)

    async def _get_latest_kline_time(self, instrument_id: str, interval: str) -> Optional[datetime]:
        """Get the most recent open_time for incremental fetching."""
        row = await self.db.read_one("""
            SELECT open_time FROM klines
            WHERE instrument_id = $1 AND interval = $2
            ORDER BY open_time DESC LIMIT 1
        """, instrument_id, interval)
        return row['open_time'] if row else None

    # ==================== Fetch (per adaptor) ====================

    def _fetch_okx(
        self, symbol: str, bar: str, start_ms: Optional[int], end_ms: Optional[int]
    ) -> List[Dict[str, Any]]:
        """
        OKX: paginate backward. /candles returns latest 1440 candles,
        /history-candles goes further back.
        """
        all_klines = []
        cursor = end_ms

        # First try /candles (recent data)
        while True:
            kwargs = {"inst_id": symbol, "bar": bar, "limit": 100}
            if cursor:
                kwargs["before"] = str(cursor)

            klines = self.exchange_client.getKlines(**kwargs)
            if not klines:
                break

            if start_ms:
                filtered = [k for k in klines if k['open_time'] and k['open_time'].timestamp() * 1000 >= start_ms]
                all_klines.extend(filtered)
                if len(filtered) < len(klines):
                    break
            else:
                all_klines.extend(klines)

            oldest = min(klines, key=lambda k: k['open_time'] or datetime.max.replace(tzinfo=timezone.utc))
            if oldest['open_time']:
                new_cursor = int(oldest['open_time'].timestamp() * 1000)
                if cursor and new_cursor >= cursor:
                    break
                cursor = new_cursor
            else:
                break

            time.sleep(self.RATE_LIMIT_DELAY)
            if len(all_klines) >= MAX_RECORDS_PER_INSTRUMENT:
                break

        # If we want older data and haven't reached start, try /history-candles
        if start_ms and cursor and cursor > start_ms and len(all_klines) < MAX_RECORDS_PER_INSTRUMENT:
            while True:
                kwargs = {"inst_id": symbol, "bar": bar, "limit": 100}
                if cursor:
                    kwargs["before"] = str(cursor)

                klines = self.exchange_client.getHistoryKlines(**kwargs)
                if not klines:
                    break

                filtered = [k for k in klines if k['open_time'] and k['open_time'].timestamp() * 1000 >= start_ms]
                all_klines.extend(filtered)
                if len(filtered) < len(klines):
                    break

                oldest = min(klines, key=lambda k: k['open_time'] or datetime.max.replace(tzinfo=timezone.utc))
                if oldest['open_time']:
                    new_cursor = int(oldest['open_time'].timestamp() * 1000)
                    if cursor and new_cursor >= cursor:
                        break
                    cursor = new_cursor
                else:
                    break

                time.sleep(self.RATE_LIMIT_DELAY)
                if len(all_klines) >= MAX_RECORDS_PER_INSTRUMENT:
                    break

        return all_klines

    def _fetch_binance(
        self, symbol: str, interval: str, start_ms: Optional[int], end_ms: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Binance: paginate forward from startTime, limit 1500 per page."""
        all_klines = []
        cursor = start_ms

        while True:
            klines = self.exchange_client.getKlines(
                inst_id=symbol, interval=interval, limit=1500,
                start_time=cursor, end_time=end_ms,
            )
            if not klines:
                break

            all_klines.extend(klines)
            if len(klines) < 1500:
                break

            newest = max(klines, key=lambda k: k['open_time'] or datetime.min.replace(tzinfo=timezone.utc))
            if newest['open_time']:
                new_cursor = int(newest['open_time'].timestamp() * 1000) + 1
                if cursor and new_cursor <= cursor:
                    break
                cursor = new_cursor
            else:
                break

            time.sleep(self.RATE_LIMIT_DELAY)
            if len(all_klines) >= MAX_RECORDS_PER_INSTRUMENT:
                logger.warning(f"Safety limit for {symbol} at {len(all_klines)}")
                break

        return all_klines

    def _fetch_klines(
        self, symbol: str, start_ms: Optional[int], end_ms: Optional[int]
    ) -> List[Dict[str, Any]]:
        adaptor = self.portfolio["adaptor"]
        if adaptor == "okx":
            bar = INTERVAL_MAP_OKX.get(self.interval, self.interval)
            return self._fetch_okx(symbol, bar, start_ms, end_ms)
        elif adaptor == "binance":
            interval = INTERVAL_MAP_BINANCE.get(self.interval, self.interval)
            return self._fetch_binance(symbol, interval, start_ms, end_ms)
        else:
            raise ValueError(f"Kline fetch not implemented for adaptor: {adaptor}")

    # ==================== Batch Insert ====================

    BATCH_SIZE = 500

    async def _batch_insert_klines(
        self, exchange_id: int, instrument_id: str, interval: str,
        klines: List[Dict[str, Any]]
    ) -> int:
        """Batch insert klines with dedup."""
        valid = [k for k in klines if k.get('open_time')]
        if not valid:
            return 0

        # Deduplicate by open_time
        seen = {}
        for k in valid:
            seen[k['open_time']] = k
        unique = list(seen.values())

        duration_ms = INTERVAL_DURATION_MS.get(interval, 14_400_000)

        rows = []
        for k in unique:
            close_time = k.get('close_time')
            if not close_time:
                close_time = datetime.fromtimestamp(
                    k['open_time'].timestamp() + duration_ms / 1000 - 0.001,
                    tz=timezone.utc,
                )
            rows.append((
                exchange_id, instrument_id, interval,
                k['open_time'], close_time,
                k['open'], k['high'], k['low'], k['close'],
                k.get('volume', 0), k.get('quote_volume', 0),
                k.get('trade_count', 0),
            ))

        query = """
            INSERT INTO klines (
                exchange_id, instrument_id, interval,
                open_time, close_time,
                open, high, low, close,
                volume, quote_volume, trade_count, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, NOW())
            ON CONFLICT (instrument_id, interval, open_time)
            DO UPDATE SET
                high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count,
                updated_at = NOW()
        """

        chunks = [rows[i:i + self.BATCH_SIZE] for i in range(0, len(rows), self.BATCH_SIZE)]
        await asyncio.gather(*[self.db.execute_many(query, c) for c in chunks])

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
        logger.info(f"Interval: {self.interval}")

        for inst in instruments:
            symbol = inst['symbol']
            instrument_id = inst['instrument_id']

            effective_start = start_ms
            if not start_ms:
                latest = await self._get_latest_kline_time(instrument_id, self.interval)
                if latest:
                    effective_start = int(latest.timestamp() * 1000) + 1
                    logger.info(f"  {symbol}: incremental from {latest}")

            logger.info(
                f"Fetching {symbol} [{self.interval}]"
                + (f" from {datetime.fromtimestamp(effective_start / 1000, tz=timezone.utc)}" if effective_start else " (all history)")
                + (f" to {self.end}" if self.end else "")
            )

            try:
                klines = self._fetch_klines(symbol, effective_start, end_ms)
            except Exception as e:
                logger.error(f"  {symbol}: fetch error: {e}")
                continue

            total_fetched += len(klines)

            if klines:
                inserted = await self._batch_insert_klines(exchange_id, instrument_id, self.interval, klines)
                total_inserted += inserted
                logger.info(f"  {symbol}: fetched {len(klines)}, upserted {inserted}")
            else:
                logger.info(f"  {symbol}: no new klines")

            time.sleep(self.RATE_LIMIT_DELAY)

        logger.info(
            f"Complete: {self.portfolio_name} | {exchange_name} | interval={self.interval} | "
            f"instruments={len(instruments)} | fetched={total_fetched} | upserted={total_inserted}"
        )
