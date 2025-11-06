"""
Async Test Script - Demonstrates performance improvements with AsyncBinanceAlpha

This script compares the performance of sync vs async implementations
and showcases batch methods for concurrent requests.
"""

import asyncio
import pandas as pd
from datetime import datetime as dt
from adaptor.binance import AsyncBinanceAlpha

async def test_async_batch():
    print("\n" + "="*60)
    print("TEST: Async - Concurrent Multiple Symbols")
    print("="*60)
    
    async with AsyncBinanceAlpha() as alpha:
        # Get token list
        print(f"\n[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Fetching token list...")
        start_time = dt.now()
        token_list = await alpha.get_token_list()
        token_df = pd.DataFrame(token_list)
        elapsed = (dt.now() - start_time).total_seconds()
        print(f"[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Token list fetched: {len(token_df)} tokens ({elapsed:.2f}s)")
        
        # Get top tokens by volume with multiplier > 1
        token_info = token_df.query("multiplier > 1").sort_values(by="volume_24h", ascending=False)
        
        symbol_names = [f"{row['alpha_id']}USDT" for _, row in token_info.iterrows()]
        
        # Test single symbol (still use list)
        print(f"\n[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Fetching single symbol: {symbol_names[0]}...")
        single_start = dt.now()
        single_result = await alpha.get_klines([symbol_names[0]], "15s", 240)
        single_elapsed = (dt.now() - single_start).total_seconds()
        single_klines = single_result[symbol_names[0]]
        print(f"[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Single symbol klines: {len(single_klines) if single_klines else 0} records ({single_elapsed:.2f}s)")
        
        # Test multiple symbols (concurrent)
        print(f"\n[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Fetching klines for {len(symbol_names)} symbols (concurrent)...")
        kline_start = dt.now()
        klines_batch = await alpha.get_klines(symbol_names, "15s", 240)
        kline_elapsed = (dt.now() - kline_start).total_seconds()
        successful = sum(1 for v in klines_batch.values() if v is not None)
        print(f"[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Multiple symbols klines: {successful}/{len(symbol_names)} symbols ({kline_elapsed:.2f}s)")

        # Test multiple symbols for agg trades
        print(f"\n[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Fetching agg trades for {len(symbol_names)} symbols (concurrent)...")
        trade_start = dt.now()
        trades_batch = await alpha.get_agg_trades(symbol_names, 500)
        trade_elapsed = (dt.now() - trade_start).total_seconds()
        successful = sum(1 for v in trades_batch.values() if v is not None)
        print(f"[{dt.now().strftime('%H:%M:%S.%f')[:-3]}] Multiple symbols trades: {successful}/{len(symbol_names)} symbols ({trade_elapsed:.2f}s)")

async def main():
    try:
        await test_async_batch()
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
