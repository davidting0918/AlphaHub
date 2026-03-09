import asyncio
import os
import sys
import statistics

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

from adaptor.binance.binance_alpha import AsyncBinanceAlpha

async def debug_vol():
    async with AsyncBinanceAlpha() as client:
        # Get list first to find a boosted one
        tokens = await client.get_token_list()
        boosted = [t for t in tokens if t.get('multiplier', 1) > 1]
        if not boosted:
            print("No boosted tokens found.")
            return

        target = boosted[0]
        symbol = target['symbol']
        alpha_id = f"{target['alpha_id']}USDT"
        
        print(f"Debugging Volatility for: {symbol} ({alpha_id})")
        
        # Fetch Klines
        klines = await client.get_klines([alpha_id], "1m", 15)
        data = klines.get(alpha_id)
        
        if not data:
            print("No kline data returned.")
            return
            
        print(f"\nFetched {len(data)} candles.")
        
        close_prices = []
        print("\n--- Raw Data Check ---")
        for i, k in enumerate(data):
            c = k.get('close')
            close_prices.append(c)
            print(f"Candle {i}: Time={k['timestamp']} Close={c}")
            
        # Calc Stats
        if len(close_prices) > 1:
            try:
                mean = statistics.mean(close_prices)
                stdev = statistics.stdev(close_prices)
                vol = stdev / mean if mean else 0
                
                print(f"\n--- Statistics ---")
                print(f"Mean: {mean}")
                print(f"Stdev: {stdev}")
                print(f"Volatility (Ratio): {vol}")
                print(f"Volatility (%): {vol * 100:.4f}%")
            except Exception as e:
                print(f"Calc error: {e}")
        else:
            print("Not enough data points.")

if __name__ == "__main__":
    asyncio.run(debug_vol())
