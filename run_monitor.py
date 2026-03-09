import asyncio
import os
import sys
import logging
import time
from datetime import datetime, timezone, timedelta

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("alpha_monitor.log")
    ]
)
logger = logging.getLogger(__name__)

from adaptor.binance.binance_alpha import AsyncBinanceAlpha
from bn_alpha_monitor import StabilityMonitor, Signal

# --- Configuration ---
def load_secrets():
    secrets = {}
    try:
        with open("/home/ubuntu/clawd/.env.secrets", "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    key = key.replace("export ", "")
                    value = value.strip('"').strip("'")
                    secrets[key] = value
    except Exception as e:
        logger.error(f"Error loading secrets: {e}")
    return secrets

SECRETS = load_secrets()
BOT_TOKEN = SECRETS.get("TELEGRAM_BOT_TOKEN_XIAO")
CHAT_ID = SECRETS.get("TELEGRAM_CHAT_ID_DAVID")

import requests

import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table

def generate_report_image(data, timestamp):
    """Generate a PNG image of the report table with color-coded signals."""
    df = pd.DataFrame(data)
    
    # Extract colors for styling then drop from dataframe
    if 'signal_color' in df.columns:
        colors = df['signal_color'].tolist()
        df = df.drop(columns=['signal_color'])
    else:
        colors = ['WHITE'] * len(df)
    
    # Define columns to show
    df = df[['symbol', 'price', 'mult', 'score', 'vol', 'vol_24h', 'trades']]
    
    # Rename for header
    df.columns = ['Symbol', 'Price', 'Mult', 'Score', 'Volatility', '24h Vol', 'Trades']
    
    # Plot
    fig, ax = plt.subplots(figsize=(11, len(df) * 0.5 + 2)) 
    ax.axis('off')
    
    # Title
    plt.title(f"Boosted Alpha Report - {timestamp}", fontsize=14, fontweight='bold', pad=20)
    
    # Table (No index)
    cell_text = df.values.tolist()
    col_labels = df.columns.tolist()
    
    tbl = plt.table(cellText=cell_text, colLabels=col_labels, loc='center', cellLoc='center', colWidths=[0.15, 0.15, 0.08, 0.1, 0.15, 0.15, 0.15])
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1.2, 1.5)
    
    # Styling
    for (row, col), cell in tbl.get_celld().items():
        if row == 0:
            cell.set_text_props(weight='bold', color='white')
            cell.set_facecolor('#333333') # Dark header
        elif row > 0: # Data rows
             data_row_idx = row - 1
             # Default background
             bg_color = '#ffffff' if row % 2 != 0 else '#f2f2f2'
             
             # Apply Signal Color to SCORE column (index 3)
             if col == 3: 
                 if data_row_idx < len(colors):
                     sig_color = colors[data_row_idx]
                     if sig_color == 'GREEN':
                         bg_color = '#C8E6C9' # Light Green
                     elif sig_color == 'YELLOW':
                         bg_color = '#FFF9C4' # Light Yellow
                     elif sig_color == 'RED':
                         bg_color = '#FFCDD2' # Light Red
             
             cell.set_facecolor(bg_color)
    
    filename = "report.png"
    plt.savefig(filename, bbox_inches='tight', dpi=150)
    plt.close()
    return filename

def send_telegram_alert(results, token_map):
    """
    Send image report for ALL Boosted signals (sorted by Score).
    """
    if not results or not BOT_TOKEN or not CHAT_ID:
        return

    # GMT+8 Time
    tz_gmt8 = timezone(timedelta(hours=8))
    timestamp = datetime.now(tz_gmt8).strftime("%Y-%m-%d %H:%M")

    # Filter: Valid results only
    valid_signals = [r for r in results if not r.error]
    valid_signals.sort(key=lambda x: x.composite_score, reverse=True)

    if not valid_signals:
        logger.info("No valid signals found. Skipping.")
        return

    # Prepare data for image
    table_data = []
    # Show up to 30 tokens
    for r in valid_signals[:30]: 
        score = r.composite_score
        
        # Access MetricResult object safely
        vol_metric = r.metrics.get('rolling_volatility')
        vol_val = vol_metric.value if vol_metric else 0.0
        volatility = vol_val * 100
        
        token_info = token_map.get(r.symbol, {})
        vol_24h = token_info.get('volume_24h', 0)
        trades_24h = token_info.get('trade_count_24h', 0)
        multiplier = token_info.get('multiplier', 1)
        
        # Get Price
        raw_price = token_info.get('high_price_24h', 0)
        
        # Format Price
        if raw_price < 0.0001:
            price_str = f"{raw_price:.8f}"
        elif raw_price < 1:
            price_str = f"{raw_price:.4f}"
        else:
            price_str = f"{raw_price:.2f}"
        
        # Format Volume
        if vol_24h > 1_000_000:
            vol_str = f"${vol_24h/1_000_000:.1f}M"
        elif vol_24h > 1_000:
            vol_str = f"${vol_24h/1_000:.1f}K"
        else:
            vol_str = f"${vol_24h:.0f}"
            
        # Determine Signal Color Code
        if r.signal == Signal.GREEN:
            sig_color = 'GREEN'
        elif r.signal == Signal.YELLOW:
            sig_color = 'YELLOW'
        else:
            sig_color = 'RED'

        table_data.append({
            'symbol': r.symbol,
            'price': price_str,
            'mult': f"x{multiplier}",
            'score': f"{score:.1f}",
            'vol': f"{volatility:.2f}%",
            'vol_24h': vol_str,
            'trades': f"{trades_24h:,}",
            'signal_color': sig_color
        })

    # Generate Image
    try:
        image_path = generate_report_image(table_data, timestamp)
        
        # Send Photo
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
        with open(image_path, 'rb') as f:
            files = {'photo': f}
            data = {
                "chat_id": CHAT_ID,
                "caption": f"🚀 **Boosted Alpha Report** ({timestamp})\nAll {len(valid_signals)} boosted tokens.",
                "parse_mode": "Markdown"
            }
            requests.post(url, data=data, files=files, timeout=30)
            
        logger.info(f"Telegram image sent for {len(valid_signals)} tokens.")
        
    except Exception as e:
        logger.error(f"Failed to generate or send image: {e}")

async def run_cycle():
    async with AsyncBinanceAlpha() as client:
        # 1. Fetch all tokens
        logger.info("Fetching token list...")
        tokens = await client.get_token_list()
        
        if not isinstance(tokens, list):
            logger.error("Invalid token list format.")
            return

        # Map for quick lookup later
        token_map = {t['symbol']: t for t in tokens}

        # 2. Filter: Multiplier > 1 AND Sort by Volume
        # We only care about boosted tokens (Multiplier > 1) for volume farming
        boosted_tokens = [t for t in tokens if t.get('multiplier', 1) > 1]
        
        logger.info(f"Found {len(boosted_tokens)} boosted tokens (Multiplier > 1).")
        
        # Sort by volume_24h descending
        sorted_tokens = sorted(boosted_tokens, key=lambda x: x.get('volume_24h', 0), reverse=True)
        top_50 = sorted_tokens[:50]
        
        if not top_50:
            logger.warning("No boosted tokens found!")
            return
            
        targets = []
        for t in top_50:
            # FIX: Append 'USDT' to alpha_id to get the correct kline symbol
            symbol_for_kline = f"{t['alpha_id']}USDT"
            
            targets.append({
                "symbol": t['symbol'],       
                "alpha_id": symbol_for_kline 
            })
            
        logger.info(f"Selected Top 50 tokens by volume. Top: {targets[0]['symbol']} (ID: {targets[0]['alpha_id']})")

        # 3. Run Stability Monitor
        monitor = StabilityMonitor(client)
        batch_result = await monitor.monitor_batch(targets)
        
        # 4. Process Results
        logger.info(f"Analysis complete. Success: {batch_result.successful}/{batch_result.total_symbols}")
        
        # 5. Send Alert (Pass token_map for extra data)
        send_telegram_alert(batch_result.results, token_map)

if __name__ == "__main__":
    try:
        asyncio.run(run_cycle())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
