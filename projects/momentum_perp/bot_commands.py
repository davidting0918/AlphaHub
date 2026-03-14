"""
Telegram Bot Command Handler

Polls for commands via getUpdates and responds to user messages.
Supports /status, /report, /orders, /signals, /positions, /balance, /config, /help.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

import httpx
import asyncpg

from .config import telegram_config, trading_config, db_config

logger = logging.getLogger(__name__)

BOT_TOKEN = telegram_config.bot_token
CHAT_ID = telegram_config.chat_id
BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


class BotCommandHandler:
    """Handles Telegram bot commands via polling."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30)
        self.last_update_id = 0
        self._pool: Optional[asyncpg.Pool] = None

    async def connect_db(self):
        if not self._pool:
            self._pool = await asyncpg.create_pool(db_config.url, min_size=1, max_size=3)

    async def close(self):
        if self._pool:
            await self._pool.close()
        await self.client.aclose()

    # ==================== Telegram Helpers ====================

    async def send(self, text: str, parse_mode: str = "HTML"):
        try:
            await self.client.post(f"{BASE_URL}/sendMessage", json={
                "chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode
            })
        except Exception as e:
            logger.error(f"Send failed: {e}")

    async def send_photo(self, photo_path: str, caption: str = ""):
        try:
            with open(photo_path, "rb") as f:
                await self.client.post(f"{BASE_URL}/sendPhoto", data={
                    "chat_id": CHAT_ID, "caption": caption
                }, files={"photo": f})
        except Exception as e:
            logger.error(f"Send photo failed: {e}")

    async def get_updates(self):
        try:
            resp = await self.client.get(f"{BASE_URL}/getUpdates", params={
                "offset": self.last_update_id + 1, "timeout": 5, "limit": 10
            })
            data = resp.json()
            if data.get("ok"):
                return data.get("result", [])
        except Exception as e:
            logger.error(f"getUpdates failed: {e}")
        return []

    # ==================== Command Handlers ====================

    async def handle_command(self, text: str):
        text = text.strip().lower()
        cmd = text.split()[0] if text else ""

        handlers = {
            "/help": self.cmd_help,
            "/status": self.cmd_status,
            "/balance": self.cmd_balance,
            "/orders": self.cmd_orders,
            "/signals": self.cmd_signals,
            "/positions": self.cmd_positions,
            "/config": self.cmd_config,
            "/report": self.cmd_report,
            "/pnl": self.cmd_pnl,
            "/start": self.cmd_help,
        }

        handler = handlers.get(cmd)
        if handler:
            await handler(text)
        elif cmd.startswith("/"):
            await self.send(f"❓ Unknown command: <code>{cmd}</code>\nType /help for available commands.")

    async def cmd_help(self, _=""):
        await self.send("""
🤖 <b>AlphaHub Trading Bot</b>

<b>📋 Commands:</b>
/status — Engine status + scan intervals
/balance — Current account balance
/orders — Recent order history
/signals — Recent strategy signals
/positions — Open positions
/pnl — Today's PnL summary
/config — Current configuration
/report — Generate full visual report
/help — This message

<b>📊 Portfolio:</b> <code>OKXTEST_MAIN_01</code>
""")

    async def cmd_status(self, _=""):
        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            balance = trader.get_balance()
            positions = trader.get_positions()
        except Exception as e:
            await self.send(f"❌ OKX error: {e}")
            return

        # Check if engine process is running
        import subprocess
        result = subprocess.run(["pgrep", "-f", "run.py"], capture_output=True, text=True)
        engine_running = bool(result.stdout.strip())

        intervals = {
            "breakout": "1H", "ema_cross": "15m", "vwap": "5m",
            "multi_tf": "15m", "volume": "1H"
        }
        interval_text = "\n".join(f"  • {k}: every <code>{v}</code>" for k, v in intervals.items())

        await self.send(f"""
⚡ <b>Engine Status</b>

🟢 Engine: <code>{"RUNNING" if engine_running else "STOPPED"}</code>
💰 Equity: <code>${balance['total_equity']:,.2f}</code>
💵 Available: <code>${balance['available_balance']:,.2f}</code>
📈 Positions: <code>{len(positions)}</code>
🏷️ Portfolio: <code>{trading_config.portfolio_name}</code>

<b>⏱ Scan Intervals:</b>
{interval_text}

<b>🎯 Trading Pairs:</b>
<code>{', '.join(i.replace('-USDT-SWAP','') for i in trading_config.instruments)}</code>

🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC
""")

    async def cmd_balance(self, _=""):
        from .okx_trader import OKXTrader
        trader = OKXTrader()
        balance = trader.get_balance()

        await self.send(f"""
💰 <b>Account Balance</b>

💵 Total Equity: <code>${balance['total_equity']:,.2f}</code>
📊 Available: <code>${balance['available_balance']:,.2f}</code>
📈 Unrealized PnL: <code>${balance['unrealized_pnl']:,.2f}</code>

🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC
""")

    async def cmd_orders(self, _=""):
        await self.connect_db()
        async with self._pool.acquire() as conn:
            orders = await conn.fetch("""
                SELECT * FROM trading_orders 
                WHERE portfolio_name = $1 
                ORDER BY created_at DESC LIMIT 10
            """, trading_config.portfolio_name)

        if not orders:
            await self.send("📋 No orders yet.")
            return

        lines = ["📋 <b>Recent Orders</b>\n"]
        for o in orders:
            side_emoji = "🟢" if o['side'] == 'buy' else "🔴"
            pnl = f"${float(o['pnl']):+.2f}" if o['pnl'] else "-"
            inst = o['instrument'].replace('-USDT-SWAP', '')
            lines.append(
                f"{side_emoji} <code>{o['created_at'].strftime('%H:%M:%S')}</code> "
                f"<b>{o['side'].upper()}</b> {float(o['size']):.1f} {inst} "
                f"| {o['strategy_name']} | PnL: {pnl} | {o['status']}"
            )

        await self.send("\n".join(lines))

    async def cmd_signals(self, _=""):
        await self.connect_db()
        async with self._pool.acquire() as conn:
            signals = await conn.fetch("""
                SELECT * FROM strategy_signals 
                WHERE portfolio_name = $1 
                ORDER BY created_at DESC LIMIT 10
            """, trading_config.portfolio_name)

        if not signals:
            await self.send("📡 No signals yet.")
            return

        emoji_map = {
            "entry_long": "🟢", "entry_short": "🔴",
            "exit_long": "⬛", "exit_short": "⬛",
            "take_profit": "🎯", "stop_loss": "🛑",
        }

        lines = ["📡 <b>Recent Signals</b>\n"]
        for s in signals:
            emoji = emoji_map.get(s['signal_type'], "⚪")
            inst = s['instrument'].replace('-USDT-SWAP', '')
            action = s['action_taken'] or '-'
            strength = float(s['signal_strength']) if s['signal_strength'] else 0
            lines.append(
                f"{emoji} <code>{s['created_at'].strftime('%H:%M:%S')}</code> "
                f"{s['signal_type']} {inst} | {s['strategy_name']} "
                f"| str: {strength:.0%} | {action}"
            )

        await self.send("\n".join(lines))

    async def cmd_positions(self, _=""):
        import okx.Account as OkxAccount
        from .config import okx_config
        
        acc = OkxAccount.AccountAPI(
            okx_config.api_key, okx_config.secret_key, okx_config.passphrase,
            False, okx_config.demo_flag
        )
        result = acc.get_positions(instType="SWAP")
        
        if result.get("code") != "0":
            await self.send(f"❌ Error: {result.get('msg')}")
            return
        
        open_pos = [p for p in result["data"] if float(p.get("pos", "0")) != 0]
        
        if not open_pos:
            await self.send("📊 No open positions.")
            return
        
        total_upl = 0
        total_notional = 0
        lines = [f"📊 <b>Open Positions</b> ({len(open_pos)})\n"]
        
        for p in open_pos:
            pos = float(p.get("pos", "0"))
            side = "LONG" if pos > 0 else "SHORT"
            side_emoji = "🟢" if pos > 0 else "🔴"
            inst = p["instId"].replace("-USDT-SWAP", "")
            entry = float(p.get("avgPx", "0"))
            mark = float(p.get("markPx", "0"))
            last = float(p.get("last", "0"))
            upl = float(p.get("upl", "0"))
            upl_pct = float(p.get("uplRatio", "0")) * 100
            notional = float(p.get("notionalUsd", "0"))
            fee = float(p.get("fee", "0"))
            leverage = notional / float(p.get("imr", "1")) if float(p.get("imr", "0")) > 0 else 0
            margin = float(p.get("imr", "0"))
            
            # Duration
            ctime = int(p.get("cTime", "0"))
            if ctime:
                opened = datetime.fromtimestamp(ctime / 1000, tz=timezone.utc)
                duration = datetime.now(timezone.utc) - opened
                hours = duration.total_seconds() / 3600
                if hours < 1:
                    dur_str = f"{int(duration.total_seconds() / 60)}m"
                elif hours < 24:
                    dur_str = f"{hours:.1f}h"
                else:
                    dur_str = f"{hours / 24:.1f}d"
            else:
                dur_str = "?"
            
            total_upl += upl
            total_notional += notional
            
            pnl_emoji = "📈" if upl >= 0 else "📉"
            
            lines.append(
                f"{side_emoji} <b>{inst}</b> {side}\n"
                f"   📏 Size: <code>{abs(pos)}</code> contracts\n"
                f"   💵 Entry: <code>${entry:,.4f}</code>\n"
                f"   📍 Mark: <code>${mark:,.4f}</code>  Last: <code>${last:,.4f}</code>\n"
                f"   {pnl_emoji} UPnL: <code>${upl:+,.2f}</code> (<code>{upl_pct:+.2f}%</code>)\n"
                f"   💰 Notional: <code>${notional:,.2f}</code>  Margin: <code>${margin:,.2f}</code>\n"
                f"   ⚡ Lev: <code>{leverage:.1f}x</code>  Fee: <code>${fee:,.2f}</code>\n"
                f"   ⏱ Open: <code>{dur_str}</code> ago\n"
            )
        
        upl_emoji = "📈" if total_upl >= 0 else "📉"
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"{upl_emoji} <b>Total UPnL:</b> <code>${total_upl:+,.2f}</code>")
        lines.append(f"💰 <b>Total Notional:</b> <code>${total_notional:,.2f}</code>")
        lines.append(f"\n🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
        
        await self.send("\n".join(lines))

    async def cmd_pnl(self, _=""):
        await self.connect_db()
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        async with self._pool.acquire() as conn:
            # Today's orders
            orders = await conn.fetch("""
                SELECT * FROM trading_orders 
                WHERE portfolio_name = $1 AND created_at >= $2
                ORDER BY created_at DESC
            """, trading_config.portfolio_name, today)

            # Today's signals
            signal_count = await conn.fetchval("""
                SELECT COUNT(*) FROM strategy_signals 
                WHERE portfolio_name = $1 AND created_at >= $2
            """, trading_config.portfolio_name, today)

        total_pnl = sum(float(o['pnl']) for o in orders if o['pnl'])
        wins = sum(1 for o in orders if o['pnl'] and float(o['pnl']) > 0)
        losses = sum(1 for o in orders if o['pnl'] and float(o['pnl']) < 0)
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

        await self.send(f"""
📊 <b>Today's PnL Summary</b>

💰 Total PnL: <code>${total_pnl:+.2f}</code>
📋 Trades: <code>{len(orders)}</code>
✅ Wins: <code>{wins}</code> | ❌ Losses: <code>{losses}</code>
🎯 Win Rate: <code>{win_rate:.0f}%</code>
📡 Signals: <code>{signal_count}</code>

🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC
""")

    async def cmd_config(self, _=""):
        await self.send(f"""
⚙️ <b>Configuration</b>

<b>Risk:</b>
  • Max per trade: <code>{trading_config.max_position_pct:.0%}</code>
  • Max exposure: <code>{trading_config.max_exposure_pct:.0%}</code>
  • Max positions: <code>{trading_config.max_concurrent_positions}</code>
  • Daily loss limit: <code>{trading_config.daily_loss_limit_pct:.0%}</code>
  • Default leverage: <code>{trading_config.default_leverage}x</code>

<b>Pairs:</b>
<code>{chr(10).join(trading_config.instruments)}</code>

<b>Portfolio:</b> <code>{trading_config.portfolio_name}</code>
""")

    async def cmd_report(self, _=""):
        await self.send("📊 Generating report...")
        try:
            # Import and run the report generator
            import subprocess
            result = subprocess.run(
                ["python3", "-c", """
import sys; sys.path.insert(0, '.')
from report_gen import generate_report
generate_report()
"""],
                capture_output=True, text=True,
                cwd=str(__import__('pathlib').Path(__file__).parent)
            )
            report_path = str(__import__('pathlib').Path(__file__).parent / "trading_report.png")
            await self.send_photo(report_path, "📊 Trading Report")
        except Exception as e:
            await self.send(f"❌ Report generation failed: {e}")

    # ==================== Main Loop ====================

    async def run(self):
        """Poll for commands."""
        logger.info("Bot command handler started")
        await self.send("🤖 <b>Bot Command Handler Active</b>\nType /help for commands.")

        while True:
            try:
                updates = await self.get_updates()
                for update in updates:
                    self.last_update_id = update["update_id"]
                    msg = update.get("message", {})
                    text = msg.get("text", "")
                    chat_id = str(msg.get("chat", {}).get("id", ""))

                    # Only respond to our authorized chat
                    if chat_id == CHAT_ID and text.startswith("/"):
                        logger.info(f"Command: {text}")
                        await self.handle_command(text)

            except Exception as e:
                logger.error(f"Bot poll error: {e}")

            await asyncio.sleep(2)


async def run_bot():
    bot = BotCommandHandler()
    try:
        await bot.run()
    finally:
        await bot.close()
