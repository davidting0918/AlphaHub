import os
import sys
import requests

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import table
from datetime import datetime, timezone, timedelta

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
        print(f"Error loading secrets: {e}")
    return secrets

SECRETS = load_secrets()
BOT_TOKEN = SECRETS.get("TELEGRAM_BOT_TOKEN_XIAO")
CHAT_ID = SECRETS.get("TELEGRAM_CHAT_ID_DAVID")

def get_top_volume_tickers(limit=20):
    """Fetch all tickers and return top volume USDT pairs."""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        # Filter for USDT pairs only, excluding stablecoins (base asset contains 'USD')
        # e.g. Exclude USDCUSDT, FDUSDUSDT, TUSDUSDT
        usdt_pairs = []
        for t in data:
            symbol = t['symbol']
            if not symbol.endswith("USDT"):
                continue
                
            base_asset = symbol[:-4] # Remove 'USDT' suffix
            if "USD" in base_asset:
                continue
                
            usdt_pairs.append(t)
        
        # Sort by Quote Volume (USDT volume) descending
        # quoteVolume is string in API
        usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        
        return usdt_pairs[:limit]
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []

def generate_market_image(tickers, timestamp):
    """Generate PNG table for top market tickers."""
    rows = []
    for t in tickers:
        symbol = t['symbol'].replace("USDT", "")
        price = float(t['lastPrice'])
        change = float(t['priceChangePercent'])
        vol = float(t['quoteVolume'])
        
        # Format Price
        if price < 1:
            price_str = f"{price:.4f}"
        else:
            price_str = f"{price:,.2f}"
            
        # Format Change
        change_str = f"{change:+.2f}%"
        
        # Format Volume
        if vol > 1_000_000_000:
            vol_str = f"${vol/1_000_000_000:.2f}B"
        elif vol > 1_000_000:
            vol_str = f"${vol/1_000_000:.1f}M"
        else:
            vol_str = f"${vol:,.0f}"
            
        rows.append({
            "Symbol": symbol,
            "Price": price_str,
            "24h Change": change_str,
            "Volume": vol_str
        })
        
    df = pd.DataFrame(rows)
    
    # Plotting
    fig, ax = plt.subplots(figsize=(8, len(df) * 0.4 + 2))
    ax.axis('off')
    
    plt.title(f"Top 20 Crypto Market (Vol) - {timestamp}", fontsize=14, fontweight='bold', pad=20)
    
    # Table using matplotlib directly
    cell_text = df.values.tolist()
    col_labels = df.columns.tolist()
    
    tbl = plt.table(cellText=cell_text, colLabels=col_labels, loc='center', cellLoc='center', colWidths=[0.2, 0.3, 0.25, 0.25])
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1.2, 1.5)
    
    # Styling
    for (row, col), cell in tbl.get_celld().items():
        if row == 0:
            cell.set_text_props(weight='bold', color='white')
            cell.set_facecolor('#2196F3') # Blue header for Market
        elif row > 0:
            # Color code change
            if col == 2: # Change column
                val_text = df.iloc[row-1, col]
                val = float(val_text.strip('%'))
                if val > 0:
                    cell.set_text_props(color='green')
                elif val < 0:
                    cell.set_text_props(color='red')
            
            if row % 2 == 0:
                cell.set_facecolor('#f2f2f2')

    filename = os.path.join(PROJECT_DIR, "market_report.png")
    plt.savefig(filename, bbox_inches='tight', dpi=150)
    plt.close()
    return filename

def send_report():
    if not BOT_TOKEN or not CHAT_ID:
        print("Missing credentials.")
        return

    # GMT+8
    tz_gmt8 = timezone(timedelta(hours=8))
    timestamp = datetime.now(tz_gmt8).strftime("%H:%M")
    
    tickers = get_top_volume_tickers(20)
    if not tickers:
        print("No data.")
        return
        
    try:
        image_path = generate_market_image(tickers, timestamp)
        
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
        with open(image_path, 'rb') as f:
            files = {'photo': f}
            data = {
                "chat_id": CHAT_ID,
                "caption": f"📊 **Hourly Market Report** ({timestamp})\nTop 20 by Volume.",
                "parse_mode": "Markdown"
            }
            requests.post(url, data=data, files=files, timeout=30)
        print("Market report sent.")
    except Exception as e:
        print(f"Failed to send: {e}")

if __name__ == "__main__":
    send_report()
