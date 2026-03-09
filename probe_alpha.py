import asyncio
import sys
import os

# Add src to path so we can import modules
sys.path.append(os.path.join(os.getcwd(), "src"))

from adaptor.binance.binance_alpha import AsyncBinanceAlpha

async def probe_tokens():
    print("Initializing AsyncBinanceAlpha...")
    async with AsyncBinanceAlpha() as client:
        print("Fetching token list...")
        try:
            tokens = await client.get_token_list()
            print(f"Successfully fetched {len(tokens)} tokens.")
            
            print("\n--- First 5 Tokens Sample ---")
            count = 0
            # Ensure tokens is a list and iterate
            if isinstance(tokens, list):
                for token_data in tokens:
                    print(f"Token Data (FULL): {token_data}")
                    count += 1
                    if count >= 3: # Just check 3 records
                        break
                
                # Check for major coins specifically
                print("\n--- Checking for Majors ---")
                majors = ["BTC", "ETH", "SOL", "ADA", "BNB"]
                found = []
                
                for m in majors:
                    # Look for symbol in the token dict (assuming key is 'symbol' or similiar)
                    # We print the whole dict for the first match to understand structure
                    for t in tokens:
                        # Convert to string just in case to search safely
                        s_str = str(t)
                        if f"'{m}" in s_str or f"\"{m}" in s_str: # simple heuristic
                             found.append(f"{m} found: {t}")
                             break
            
                if found:
                    print("Found majors:")
                    for f in found:
                        print(f)
            else:
                print(f"Unexpected data structure: {type(tokens)}")
                
        except Exception as e:
            print(f"Error fetching token list: {e}")

if __name__ == "__main__":
    # Fix for asyncio loop policy if needed, but usually default is fine on Linux
    asyncio.run(probe_tokens())
