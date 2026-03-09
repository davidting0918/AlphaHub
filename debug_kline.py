import asyncio
import os
import sys
import logging

sys.path.append(os.path.join(os.getcwd(), "src"))
from adaptor.binance.client import AsyncBinanceClient

async def debug_kline():
    async with AsyncBinanceClient() as client:
        # Try suffixes
        base_candidates = ["ALPHA_124", "quq", "124"] 
        suffixes = ["", "_USDT", "USDT", "_BNB", "BNB"]
        
        for base in base_candidates:
            for suff in suffixes:
                sym = f"{base}{suff}"
                print(f"Testing: '{sym}'")
                try:
                    data = await client.get_alpha_klines(sym, "1m", 1)
                    if data.get('code') != '-1121': # If not invalid symbol
                        print(f"!!! SUCCESS !!! Symbol is: {sym}")
                        print(data)
                        return
                except Exception as e:
                    pass
        print("All guesses failed.")

if __name__ == "__main__":
    asyncio.run(debug_kline())
