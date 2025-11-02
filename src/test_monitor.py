"""
Alpha Stability Monitor - Test & Example Script

Demonstrates batch monitoring functionality with real Binance Alpha tokens.
"""

import asyncio
import json
import sys
import pandas as pd
from datetime import datetime as dt

from adaptor.binance import AsyncBinanceAlpha
from bn_alpha_monitor import StabilityMonitor

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        # Fallback: disable emojis if encoding fails
        EMOJIS_ENABLED = False
else:
    EMOJIS_ENABLED = True


async def main():
    """Main test function"""
    
    print("=" * 70)
    print("Alpha Stability Monitor - Batch Test")
    print("=" * 70)
    
    async with AsyncBinanceAlpha() as alpha:
        # Step 1: Fetch token list
        print(f"\n[{dt.now().strftime('%H:%M:%S')}] Fetching token list...")
        start_time = dt.now()
        token_list = await alpha.get_token_list()
        token_df = pd.DataFrame(token_list)
        elapsed = (dt.now() - start_time).total_seconds()
        print(f"[{dt.now().strftime('%H:%M:%S')}] Found {len(token_df)} tokens ({elapsed:.2f}s)")
        
        # Step 2: Filter and select tokens
        # Select tokens with multiplier > 1 (indicating higher quality)
        # Sort by 24h volume to get most liquid tokens
        filtered_tokens = token_df.query("multiplier > 1 and volume_24h > 0")
        filtered_tokens = filtered_tokens.sort_values(by="volume_24h", ascending=False)
        
        # Select top N tokens
        num_tokens = min(15, len(filtered_tokens))
        selected_tokens = filtered_tokens.head(num_tokens)
        
        symbols = [f"{row['alpha_id']}USDT" for _, row in selected_tokens.iterrows()]
        
        print(f"\n[{dt.now().strftime('%H:%M:%S')}] Selected {len(symbols)} tokens for monitoring:")
        for i, (_, row) in enumerate(selected_tokens.iterrows(), 1):
            print(f"  {i:2d}. {row['alpha_id']:10s} - Volume: ${row['volume_24h']:,.0f}")
        
        # Step 3: Create monitor and run batch analysis
        print(f"\n[{dt.now().strftime('%H:%M:%S')}] Starting batch monitoring...")
        print("-" * 70)
        
        monitor_start = dt.now()
        monitor = StabilityMonitor(alpha)
        batch_result = await monitor.monitor_batch(symbols)
        monitor_elapsed = (dt.now() - monitor_start).total_seconds()
        
        print(f"[{dt.now().strftime('%H:%M:%S')}] Monitoring complete ({monitor_elapsed:.2f}s)")
        print("=" * 70)
        
        # Step 4: Display results
        print("\n[RESULTS] BATCH MONITORING RESULTS")
        print("=" * 70)
        
        # Convert to dict for JSON output
        result_dict = batch_result.to_dict()
        
        # Display summary
        print(f"\n[SUMMARY]")
        print(f"   Total Symbols:    {result_dict['total_symbols']}")
        print(f"   Successful:       {result_dict['successful']}")
        print(f"   Failed:           {result_dict['failed']}")
        print(f"\n[SIGNALS] Signal Distribution:")
        print(f"   [GREEN]  Green Signals:  {result_dict['summary']['green_signals']}")
        print(f"   [YELLOW] Yellow Signals: {result_dict['summary']['yellow_signals']}")
        print(f"   [RED]    Red Signals:    {result_dict['summary']['red_signals']}")
        
        # Display individual results
        print(f"\n[DETAILS] Individual Token Results:")
        print("-" * 70)
        
        for result in result_dict['results']:
            if 'error' in result:
                print(f"\n[ERROR] {result['symbol']}: {result['error']}")
            else:
                signal_prefix = {
                    'green': '[GREEN]',
                    'yellow': '[YELLOW]',
                    'red': '[RED]'
                }.get(result['signal'], '[UNKNOWN]')
                
                print(f"\n{signal_prefix} {result['symbol']}")
                print(f"   Score: {result['composite_score']:.1f}/100")
                print(f"   Signal: {result['signal'].upper()}")
                print(f"   Recommendation: {result['recommendation']}")
                
                # Show top 3 metrics
                metrics = result['metrics']
                print(f"   Key Metrics:")
                for metric_name in ['rolling_volatility', 'atr', 'price_range']:
                    if metric_name in metrics:
                        m = metrics[metric_name]
                        print(f"      - {metric_name}: {m['value']:.4%} (score: {m['score']:.1f})")
        
        # Step 5: Save full JSON output
        output_file = "stability_monitor_results.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        
        print("\n" + "=" * 70)
        print(f"[FILE] Full results saved to: {output_file}")
        print("=" * 70)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[WARNING] Monitoring interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Error: {e}")
        raise

