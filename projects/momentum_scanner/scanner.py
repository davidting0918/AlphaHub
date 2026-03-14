"""
Momentum Scanner — Phase 2: Price Jump Detection

Detects price jumps and volume spikes in kline data:
- Single candle: >3% absolute price change
- Consecutive: 3 candles totaling >5%
- Volume spike: >3x average volume

Outputs a DataFrame of all detected jumps with metadata.
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from fetch_data import load_kline_data, get_available_symbols, DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Detection thresholds ────────────────────────────────────────
SINGLE_CANDLE_THRESHOLD = 0.03   # 3% per candle
CONSECUTIVE_THRESHOLD = 0.05     # 5% over 3 candles
CONSECUTIVE_WINDOW = 3           # number of candles
VOLUME_SPIKE_RATIO = 3.0         # 3x average volume
VOLUME_AVG_WINDOW = 48           # 48 candles = 4 hours for volume average

JUMPS_FILE = os.path.join(DATA_DIR, "detected_jumps.parquet")
JUMPS_CSV = os.path.join(DATA_DIR, "detected_jumps.csv")


class JumpScanner:
    """Detects price jumps and volume spikes in kline data."""

    def __init__(
        self,
        single_threshold: float = SINGLE_CANDLE_THRESHOLD,
        consec_threshold: float = CONSECUTIVE_THRESHOLD,
        consec_window: int = CONSECUTIVE_WINDOW,
        volume_ratio: float = VOLUME_SPIKE_RATIO,
        vol_avg_window: int = VOLUME_AVG_WINDOW,
    ):
        self.single_threshold = single_threshold
        self.consec_threshold = consec_threshold
        self.consec_window = consec_window
        self.volume_ratio = volume_ratio
        self.vol_avg_window = vol_avg_window

    def scan_symbol(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Scan a single symbol's kline data for price jumps.

        Returns DataFrame with columns:
            symbol, datetime, open_time_ms, price_change_pct, abs_price_change_pct,
            direction, volume_ratio, jump_type, close_price, volume
        """
        if df is None or len(df) < self.vol_avg_window + self.consec_window:
            return pd.DataFrame()

        df = df.copy()
        df.sort_values("open_time_ms", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Calculate returns
        df["pct_change"] = (df["close"] - df["open"]) / df["open"]
        df["abs_pct_change"] = df["pct_change"].abs()

        # Rolling volume average (exclude current candle)
        df["vol_avg"] = df["volume"].rolling(window=self.vol_avg_window, min_periods=10).mean().shift(1)
        df["vol_ratio"] = df["volume"] / df["vol_avg"].replace(0, np.nan)

        # Rolling 3-candle cumulative return
        df["cum_return_3"] = df["close"].pct_change(periods=self.consec_window)
        df["abs_cum_return_3"] = df["cum_return_3"].abs()

        jumps = []

        for idx in range(self.vol_avg_window, len(df)):
            row = df.iloc[idx]
            jump_types = []

            # Check single candle jump
            is_single_jump = row["abs_pct_change"] >= self.single_threshold
            if is_single_jump:
                jump_types.append("single_candle")

            # Check consecutive candle jump
            is_consec_jump = (
                idx >= self.consec_window
                and row["abs_cum_return_3"] >= self.consec_threshold
            )
            if is_consec_jump:
                jump_types.append("consecutive")

            # Check volume spike
            is_vol_spike = (
                pd.notna(row["vol_ratio"])
                and row["vol_ratio"] >= self.volume_ratio
            )
            if is_vol_spike:
                jump_types.append("volume_spike")

            if not jump_types:
                continue

            # Determine direction
            if "single_candle" in jump_types:
                direction = "up" if row["pct_change"] > 0 else "down"
                price_change = row["pct_change"]
            elif "consecutive" in jump_types:
                direction = "up" if row["cum_return_3"] > 0 else "down"
                price_change = row["cum_return_3"]
            else:
                # Volume spike only — use candle direction
                direction = "up" if row["pct_change"] > 0 else "down"
                price_change = row["pct_change"]

            jumps.append({
                "symbol": symbol,
                "datetime": row.get("datetime", pd.Timestamp(row["open_time_ms"], unit="ms", tz="UTC")),
                "open_time_ms": int(row["open_time_ms"]),
                "price_change_pct": round(price_change * 100, 4),
                "abs_price_change_pct": round(abs(price_change) * 100, 4),
                "direction": direction,
                "volume_ratio": round(row["vol_ratio"], 2) if pd.notna(row["vol_ratio"]) else 0,
                "jump_type": "+".join(jump_types),
                "close_price": row["close"],
                "open_price": row["open"],
                "volume": row["volume"],
                "quote_volume": row["quote_volume"],
                "hour": int(pd.Timestamp(row["open_time_ms"], unit="ms", tz="UTC").hour),
                "day_of_week": int(pd.Timestamp(row["open_time_ms"], unit="ms", tz="UTC").dayofweek),
            })

        return pd.DataFrame(jumps)

    def scan_all(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Scan all available symbols (or a subset) for jumps.

        Returns combined DataFrame of all jumps.
        """
        if symbols is None:
            symbols = get_available_symbols()

        if not symbols:
            logger.warning("No symbols available. Run fetch_data.py first.")
            return pd.DataFrame()

        logger.info(f"Scanning {len(symbols)} symbols for price jumps...")
        all_jumps = []
        stats = {"total_candles": 0, "total_jumps": 0}

        for i, symbol in enumerate(sorted(symbols), 1):
            df = load_kline_data(symbol)
            if df is None or df.empty:
                continue

            stats["total_candles"] += len(df)
            jumps_df = self.scan_symbol(symbol, df)

            if not jumps_df.empty:
                all_jumps.append(jumps_df)
                stats["total_jumps"] += len(jumps_df)
                logger.info(f"  [{i}/{len(symbols)}] {symbol}: {len(jumps_df)} jumps detected "
                            f"(from {len(df)} candles)")
            else:
                logger.debug(f"  [{i}/{len(symbols)}] {symbol}: no jumps")

        if not all_jumps:
            logger.warning("No jumps detected across all symbols!")
            return pd.DataFrame()

        combined = pd.concat(all_jumps, ignore_index=True)
        combined.sort_values("open_time_ms", inplace=True)
        combined.reset_index(drop=True, inplace=True)

        # Save results
        combined.to_parquet(JUMPS_FILE, index=False)
        # Also save CSV for easy inspection
        combined.to_csv(JUMPS_CSV, index=False)

        # Print summary
        self._print_summary(combined, stats)

        return combined

    def _print_summary(self, jumps: pd.DataFrame, stats: dict):
        """Print scan summary statistics."""
        logger.info(f"\n{'='*70}")
        logger.info(f"Jump Detection Summary")
        logger.info(f"{'='*70}")
        logger.info(f"Total candles scanned: {stats['total_candles']:,}")
        logger.info(f"Total jumps detected:  {stats['total_jumps']:,}")
        logger.info(f"Jump rate:             {stats['total_jumps']/max(stats['total_candles'],1)*100:.4f}%")
        logger.info(f"")

        # By type
        type_counts = jumps["jump_type"].value_counts()
        logger.info("By jump type:")
        for jtype, count in type_counts.items():
            logger.info(f"  {jtype}: {count}")

        # By direction
        dir_counts = jumps["direction"].value_counts()
        logger.info(f"\nBy direction:")
        for d, count in dir_counts.items():
            logger.info(f"  {d}: {count} ({count/len(jumps)*100:.1f}%)")

        # Top symbols
        sym_counts = jumps["symbol"].value_counts().head(10)
        logger.info(f"\nTop 10 symbols by jump count:")
        for sym, count in sym_counts.items():
            avg_change = jumps[jumps["symbol"] == sym]["abs_price_change_pct"].mean()
            logger.info(f"  {sym}: {count} jumps (avg {avg_change:.2f}%)")

        # Average metrics
        logger.info(f"\nAverage price change: {jumps['abs_price_change_pct'].mean():.2f}%")
        logger.info(f"Median price change:  {jumps['abs_price_change_pct'].median():.2f}%")
        logger.info(f"Average volume ratio: {jumps['volume_ratio'].mean():.1f}x")
        logger.info(f"{'='*70}")


def load_jumps() -> Optional[pd.DataFrame]:
    """Load previously detected jumps."""
    if os.path.exists(JUMPS_FILE):
        return pd.read_parquet(JUMPS_FILE)
    return None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Momentum Scanner - Jump Detection")
    parser.add_argument("--symbols", nargs="*", help="Only scan these symbols")
    parser.add_argument("--threshold", type=float, default=SINGLE_CANDLE_THRESHOLD,
                        help=f"Single candle threshold (default: {SINGLE_CANDLE_THRESHOLD})")
    args = parser.parse_args()

    scanner = JumpScanner(single_threshold=args.threshold)
    jumps = scanner.scan_all(symbols=args.symbols)
    if not jumps.empty:
        print(f"\nResults saved to:\n  {JUMPS_FILE}\n  {JUMPS_CSV}")
