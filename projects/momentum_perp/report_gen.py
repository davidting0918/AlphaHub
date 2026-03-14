"""Standalone report image generator."""
import asyncio, asyncpg, json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import FancyBboxPatch

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from projects.momentum_perp.config import db_config, trading_config

OUTPUT = str(Path(__file__).parent / "trading_report.png")

async def _fetch():
    pool = await asyncpg.create_pool(db_config.url, min_size=1, max_size=3)
    async with pool.acquire() as c:
        orders = await c.fetch('SELECT * FROM trading_orders WHERE portfolio_name=$1 ORDER BY created_at DESC LIMIT 20', trading_config.portfolio_name)
        snapshots = await c.fetch('SELECT * FROM account_snapshots WHERE portfolio_name=$1 ORDER BY created_at ASC', trading_config.portfolio_name)
    await pool.close()
    return orders, snapshots

def generate_report():
    orders, snapshots = asyncio.run(_fetch())
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(14, 14))
    fig.patch.set_facecolor('#0d1117')
    fig.suptitle('OKXTEST_MAIN_01 — Trading Report', fontsize=20, fontweight='bold', color='#58a6ff', y=0.98)
    fig.text(0.5, 0.96, f'Generated: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}', ha='center', fontsize=10, color='#8b949e')

    # Summary cards
    ax_s = fig.add_axes([0.05, 0.84, 0.9, 0.10])
    ax_s.set_xlim(0, 10); ax_s.set_ylim(0, 3); ax_s.axis('off')
    
    latest = snapshots[-1] if snapshots else None
    initial_eq = float(snapshots[0]['total_equity']) if snapshots else 29490
    current_eq = float(latest['total_equity']) if latest else 29490
    total_pnl = current_eq - initial_eq
    pnl_pct = (total_pnl / initial_eq) * 100 if initial_eq else 0

    boxes = [
        ('Equity', f'${current_eq:,.2f}', '#58a6ff'),
        ('Available', f'${float(latest["available_balance"]):,.2f}' if latest else '-', '#8b949e'),
        ('Total PnL', f'${total_pnl:,.2f}', '#f85149' if total_pnl < 0 else '#3fb950'),
        ('PnL %', f'{pnl_pct:+.3f}%', '#f85149' if total_pnl < 0 else '#3fb950'),
        ('Trades', str(len(orders)), '#d2a8ff'),
    ]
    for i, (label, value, color) in enumerate(boxes):
        x = i * 2.0 + 0.2
        ax_s.add_patch(FancyBboxPatch((x, 0.3), 1.6, 2.2, boxstyle="round,pad=0.1", facecolor='#161b22', edgecolor='#30363d'))
        ax_s.text(x+0.8, 1.8, label, ha='center', va='center', fontsize=9, color='#8b949e')
        ax_s.text(x+0.8, 0.95, value, ha='center', va='center', fontsize=13, fontweight='bold', color=color)

    # Equity curve
    ax_eq = fig.add_axes([0.08, 0.52, 0.87, 0.28])
    ax_eq.set_facecolor('#0d1117')
    if snapshots:
        times = [s['created_at'] for s in snapshots]
        eqs = [float(s['total_equity']) for s in snapshots]
        ax_eq.fill_between(times, eqs, alpha=0.15, color='#58a6ff')
        ax_eq.plot(times, eqs, color='#58a6ff', linewidth=2, marker='o', markersize=4)
        ymin = min(eqs) - 20; ymax = max(eqs) + 20
        ax_eq.set_ylim(ymin, ymax)
    ax_eq.set_title('Equity Curve', fontsize=14, color='#c9d1d9', pad=10)
    ax_eq.set_ylabel('USDT', color='#8b949e')
    ax_eq.tick_params(colors='#8b949e')
    ax_eq.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax_eq.grid(True, alpha=0.1)
    for spine in ['top','right']: ax_eq.spines[spine].set_visible(False)
    for spine in ['bottom','left']: ax_eq.spines[spine].set_color('#30363d')

    # Orders table
    ax_t = fig.add_axes([0.05, 0.05, 0.9, 0.42])
    ax_t.set_facecolor('#0d1117'); ax_t.axis('off')
    ax_t.set_title('Order History', fontsize=14, color='#c9d1d9', pad=10)

    if orders:
        headers = ['Time', 'Strategy', 'Instrument', 'Side', 'Size', 'Price', 'PnL', 'Status']
        rows, cell_colors = [], []
        for o in orders:
            side = str(o['side']).upper()
            pnl_val = float(o['pnl']) if o['pnl'] else 0
            pnl_str = f'${pnl_val:+.2f}' if o['pnl'] else '-'
            price_str = f'${float(o["price"]):.4f}' if o['price'] else '-'
            rows.append([o['created_at'].strftime('%H:%M:%S'), o['strategy_name'], o['instrument'].replace('-USDT-SWAP',''), side, f'{float(o["size"]):.2f}', price_str, pnl_str, o['status']])
            rc = ['#161b22'] * 8
            rc[3] = '#1a3a1a' if side == 'BUY' else '#3a1a1a'
            if pnl_val < 0: rc[6] = '#3a1a1a'
            elif pnl_val > 0: rc[6] = '#1a3a1a'
            cell_colors.append(rc)

        table = ax_t.table(cellText=rows, colLabels=headers, cellColours=cell_colors, colColours=['#21262d']*8, loc='upper center', cellLoc='center')
        table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 1.8)
        for key, cell in table.get_celld().items():
            cell.set_edgecolor('#30363d'); cell.set_text_props(color='#c9d1d9')
            if key[0] == 0: cell.set_text_props(color='#58a6ff', fontweight='bold')

    plt.savefig(OUTPUT, dpi=150, facecolor='#0d1117', bbox_inches='tight')
    plt.close()
    return OUTPUT

if __name__ == "__main__":
    print(generate_report())
