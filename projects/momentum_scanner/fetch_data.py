"""
Momentum Scanner — Phase 1: Data Collection

Fetches ALL Binance USDT perpetual futures symbols, classifies by liquidity,
and downloads 5-minute kline data for the past 6 months.
Saves to local parquet files for fast backtesting.

Uses the existing AlphaHub BinanceClient for API calls.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from adaptor.binance.client import BinanceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
KLINE_DIR = os.path.join(DATA_DIR, "klines")
META_FILE = os.path.join(DATA_DIR, "symbols_meta.json")
PROGRESS_FILE = os.path.join(DATA_DIR, "fetch_progress.json")

INTERVAL = "5m"
INTERVAL_MS = 5 * 60 * 1000  # 5 minutes in ms
CANDLES_PER_REQUEST = 1500  # Binance max
LOOKBACK_DAYS = 180  # 6 months
RATE_LIMIT_SLEEP = 0.12  # ~500 req/min to stay well under 1200/min limit

# Liquidity thresholds (24h quote volume in USDT)
HIGH_VOL_THRESHOLD = 50_000_000    # >$50M
MID_VOL_THRESHOLD = 5_000_000     # $5M-$50M
# Low = <$5M


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(KLINE_DIR, exist_ok=True)


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {}


def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


class DataFetcher:
    """Fetches and stores Binance Futures kline data."""

    def __init__(self, subset: Optional[List[str]] = None):
        self.client = BinanceClient()
        self.subset = subset  # For testing: only fetch these symbols

    def fetch_all_symbols(self) -> List[Dict]:
        """Get all USDT perpetual futures symbols with 24h volume."""
        logger.info("Fetching exchange info...")
        info = self.client.get_futures_exchange_info()
        symbols = []
        for s in info.get("symbols", []):
            if (s.get("contractType") == "PERPETUAL"
                    and s.get("quoteAsset") == "USDT"
                    and s.get("status") == "TRADING"):
                symbols.append({
                    "symbol": s["symbol"],
                    "base": s["baseAsset"],
                    "quote": s["quoteAsset"],
                    "onboard_date": s.get("onboardDate"),
                })
        logger.info(f"Found {len(symbols)} USDT perpetual symbols")
        return symbols

    def fetch_24h_volumes(self, symbols: List[Dict]) -> List[Dict]:
        """Fetch 24h ticker data to get volume for classification."""
        logger.info("Fetching 24h ticker data for volume classification...")
        url = f"{self.client.FUTURES_BASE_URL}/fapi/v1/ticker/24hr"
        try:
            resp = self.client._session.get(url, timeout=30)
            tickers = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch 24h tickers: {e}")
            return symbols

        vol_map = {}
        for t in tickers:
            vol_map[t["symbol"]] = float(t.get("quoteVolume", 0))

        for s in symbols:
            s["volume_24h"] = vol_map.get(s["symbol"], 0)
            if s["volume_24h"] > HIGH_VOL_THRESHOLD:
                s["liquidity"] = "high"
            elif s["volume_24h"] > MID_VOL_THRESHOLD:
                s["liquidity"] = "mid"
            else:
                s["liquidity"] = "low"

        # Sort by volume
        symbols.sort(key=lambda x: x["volume_24h"], reverse=True)
        return symbols

    def classify_and_save_metadata(self, symbols: List[Dict]) -> List[Dict]:
        """Classify symbols and save metadata. Returns mid+low liquidity symbols."""
        high = [s for s in symbols if s["liquidity"] == "high"]
        mid = [s for s in symbols if s["liquidity"] == "mid"]
        low = [s for s in symbols if s["liquidity"] == "low"]

        logger.info(f"Liquidity classification:")
        logger.info(f"  HIGH (>{HIGH_VOL_THRESHOLD/1e6:.0f}M): {len(high)} symbols")
        logger.info(f"  MID  ({MID_VOL_THRESHOLD/1e6:.0f}M-{HIGH_VOL_THRESHOLD/1e6:.0f}M): {len(mid)} symbols")
        logger.info(f"  LOW  (<{MID_VOL_THRESHOLD/1e6:.0f}M): {len(low)} symbols")

        # Save metadata
        meta = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "total_symbols": len(symbols),
            "high_count": len(high),
            "mid_count": len(mid),
            "low_count": len(low),
            "symbols": symbols,
        }
        with open(META_FILE, "w") as f:
            json.dump(meta, f, indent=2)
        logger.info(f"Metadata saved to {META_FILE}")

        # Focus on mid + low liquidity
        target = mid + low
        if self.subset:
            target = [s for s in symbols if s["symbol"] in self.subset]
            logger.info(f"Using subset: {[s['symbol'] for s in target]}")

        return target

    def fetch_klines_for_symbol(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> pd.DataFrame:
        """
        Fetch all 5m klines for a symbol between start_ms and end_ms.
        Handles pagination (1500 candles per request).
        Saves incrementally to parquet.
        """
        parquet_path = os.path.join(KLINE_DIR, f"{symbol}.parquet")

        # Check existing data
        existing_df = None
        last_ts = start_ms
        if os.path.exists(parquet_path):
            try:
                existing_df = pd.read_parquet(parquet_path)
                if not existing_df.empty:
                    last_ts = int(existing_df["open_time_ms"].max()) + INTERVAL_MS
                    if last_ts >= end_ms:
                        logger.info(f"  {symbol}: already up to date ({len(existing_df)} candles)")
                        return existing_df
                    logger.info(f"  {symbol}: resuming from {datetime.fromtimestamp(last_ts/1000, tz=timezone.utc).strftime('%Y-%m-%d')}")
            except Exception:
                existing_df = None
                last_ts = start_ms

        all_candles = []
        current_start = last_ts
        request_count = 0

        while current_start < end_ms:
            try:
                raw = self.client.get_futures_klines(
                    symbol=symbol,
                    interval=INTERVAL,
                    start_time=current_start,
                    end_time=end_ms,
                    limit=CANDLES_PER_REQUEST,
                )
                request_count += 1

                if not raw:
                    break

                for candle in raw:
                    all_candles.append({
                        "open_time_ms": int(candle[0]),
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5]),
                        "close_time_ms": int(candle[6]),
                        "quote_volume": float(candle[7]),
                        "trade_count": int(candle[8]),
                        "taker_buy_base_vol": float(candle[9]),
                        "taker_buy_quote_vol": float(candle[10]),
                    })

                last_candle_ts = int(raw[-1][0])
                if last_candle_ts <= current_start:
                    break
                current_start = last_candle_ts + INTERVAL_MS

                time.sleep(RATE_LIMIT_SLEEP)

            except Exception as e:
                logger.warning(f"  {symbol}: API error at {current_start}: {e}")
                time.sleep(2)
                continue

        if not all_candles:
            if existing_df is not None and not existing_df.empty:
                return existing_df
            return pd.DataFrame()

        new_df = pd.DataFrame(all_candles)

        # Merge with existing
        if existing_df is not None and not existing_df.empty:
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined.drop_duplicates(subset=["open_time_ms"], keep="last", inplace=True)
            combined.sort_values("open_time_ms", inplace=True)
            combined.reset_index(drop=True, inplace=True)
        else:
            combined = new_df
            combined.sort_values("open_time_ms", inplace=True)
            combined.reset_index(drop=True, inplace=True)

        # Save to parquet
        combined.to_parquet(parquet_path, index=False)
        logger.info(
            f"  {symbol}: saved {len(combined)} candles "
            f"({len(all_candles)} new, {request_count} requests)"
        )
        return combined

    def fetch_all_klines(self, symbols: List[Dict]) -> Dict[str, int]:
        """Fetch klines for all target symbols."""
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = now_ms - (LOOKBACK_DAYS * 24 * 60 * 60 * 1000)

        progress = load_progress()
        results = {}
        total = len(symbols)

        logger.info(f"\nFetching {INTERVAL} klines for {total} symbols "
                     f"({LOOKBACK_DAYS} days lookback)...")
        logger.info(f"Time range: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')} → "
                     f"{datetime.fromtimestamp(now_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')}")

        for i, sym_info in enumerate(symbols, 1):
            symbol = sym_info["symbol"]
            logger.info(f"[{i}/{total}] {symbol} (vol: ${sym_info.get('volume_24h', 0)/1e6:.1f}M, {sym_info.get('liquidity', '?')})")

            try:
                df = self.fetch_klines_for_symbol(symbol, start_ms, now_ms)
                candle_count = len(df) if df is not None and not df.empty else 0
                results[symbol] = candle_count

                progress[symbol] = {
                    "candles": candle_count,
                    "last_fetch": datetime.now(timezone.utc).isoformat(),
                    "status": "done",
                }
                save_progress(progress)

            except Exception as e:
                logger.error(f"  {symbol}: FAILED - {e}")
                results[symbol] = 0
                progress[symbol] = {
                    "candles": 0,
                    "last_fetch": datetime.now(timezone.utc).isoformat(),
                    "status": f"error: {str(e)[:100]}",
                }
                save_progress(progress)

        return results

    def run(self) -> Dict:
        """Full pipeline: fetch symbols → classify → download klines."""
        ensure_dirs()

        # Step 1: Get all USDT perp symbols
        symbols = self.fetch_all_symbols()

        # Step 2: Get 24h volumes and classify
        symbols = self.fetch_24h_volumes(symbols)

        # Step 3: Filter to mid+low liquidity (or subset)
        targets = self.classify_and_save_metadata(symbols)

        # Step 4: Fetch kline data
        results = self.fetch_all_klines(targets)

        # Summary
        total_candles = sum(results.values())
        success = sum(1 for v in results.values() if v > 0)
        logger.info(f"\n{'='*60}")
        logger.info(f"Data Collection Complete")
        logger.info(f"  Symbols fetched: {success}/{len(results)}")
        logger.info(f"  Total candles: {total_candles:,}")
        logger.info(f"  Data directory: {KLINE_DIR}")
        logger.info(f"{'='*60}")

        return {
            "symbols_total": len(symbols),
            "symbols_target": len(targets),
            "symbols_fetched": success,
            "total_candles": total_candles,
            "results": results,
        }


def load_kline_data(symbol: str) -> Optional[pd.DataFrame]:
    """Load kline parquet for a symbol. Convenience function for other modules."""
    path = os.path.join(KLINE_DIR, f"{symbol}.parquet")
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    if df.empty:
        return None
    # Add datetime column
    df["datetime"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    return df


def get_available_symbols() -> List[str]:
    """List symbols with downloaded kline data."""
    if not os.path.exists(KLINE_DIR):
        return []
    return [f.replace(".parquet", "") for f in os.listdir(KLINE_DIR) if f.endswith(".parquet")]


def load_symbol_metadata() -> Optional[Dict]:
    """Load the symbol metadata (with liquidity classification)."""
    if not os.path.exists(META_FILE):
        return None
    with open(META_FILE) as f:
        return json.load(f)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Momentum Scanner - Data Fetcher")
    parser.add_argument("--subset", nargs="*", help="Only fetch these symbols (for testing)")
    parser.add_argument("--list", action="store_true", help="List available kline data")
    args = parser.parse_args()

    if args.list:
        syms = get_available_symbols()
        print(f"Available symbols: {len(syms)}")
        for s in sorted(syms):
            path = os.path.join(KLINE_DIR, f"{s}.parquet")
            df = pd.read_parquet(path)
            print(f"  {s}: {len(df)} candles")
    else:
        fetcher = DataFetcher(subset=args.subset)
        fetcher.run()
