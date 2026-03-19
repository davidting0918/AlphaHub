"""
Telegram Bot Command Handler

Polls for commands via getUpdates and responds to user messages.
Supports monitoring, manual trading, and position management commands.
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
        raw_text = text.strip()
        text = raw_text.lower()
        cmd = text.split()[0] if text else ""

        handlers = {
            # Monitoring
            "/help": self.cmd_help,
            "/status": self.cmd_status,
            "/balance": self.cmd_balance,
            "/orders": self.cmd_orders,
            "/signals": self.cmd_signals,
            "/positions": self.cmd_positions,
            "/config": self.cmd_config,
            "/report": self.cmd_report,
            "/pnl": self.cmd_pnl,
            "/price": self.cmd_price,
            "/start": self.cmd_help,
            # Trading (new)
            "/market_trade": self.cmd_market_trade,
            "/close_position": self.cmd_close_position,
            "/get_positions": self.cmd_get_positions,
            # Trading (legacy)
            "/open": self.cmd_open,
            "/close": self.cmd_close,
            "/closeall": self.cmd_closeall,
            "/limit": self.cmd_limit,
            "/cancel": self.cmd_cancel,
        }

        handler = handlers.get(cmd)
        if handler:
            await handler(text)
        elif cmd.startswith("/"):
            await self.send(f"❓ Unknown command: <code>{cmd}</code>\nType /help for available commands.")

    async def cmd_help(self, _=""):
        await self.send("""
🤖 <b>AlphaHub Trading Bot</b>

<b>📈 Trading:</b>
/market_trade <i>INST PORTFOLIO AMOUNT</i>
  Market order in base amount
  <code>/market_trade OKEX_PERP_BTC_USDT OKEXTEST_MAIN_01 3</code>
  <code>/market_trade OKEX_PERP_ETH_USDT OKEXTEST_MAIN_01 -10</code>
  +amount = buy/long, -amount = sell/short

/close_position <i>INST PORTFOLIO</i>
  Close entire position
  <code>/close_position OKEX_PERP_BTC_USDT OKEXTEST_MAIN_01</code>

/get_positions <i>[PORTFOLIO]</i>
  Show open positions
  <code>/get_positions OKEXTEST_MAIN_01</code>

<b>📊 Monitoring:</b>
/status — Engine status
/balance — Account balance
/positions — All positions (detailed)
/orders — Recent orders
/pnl — Today's PnL
/price <i>SYMBOL</i> — Current price
/config — Configuration
/report — Visual report

<b>💡 Instrument format:</b>
<code>OKEX_PERP_BTC_USDT</code> or <code>BTC</code> or <code>BTC-USDT-SWAP</code>

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

    # ==================== Trading Commands ====================

    def _resolve_instrument(self, symbol: str) -> str:
        """Convert various instrument formats to OKX instrument ID.
        
        Supported formats:
            OKEX_PERP_BTC_USDT → BTC-USDT-SWAP
            BTC-USDT-SWAP      → BTC-USDT-SWAP (passthrough)
            BTC-USDT           → BTC-USDT-SWAP
            BTC                → BTC-USDT-SWAP
        """
        symbol = symbol.upper().strip()
        # Handle OKEX_PERP_XXX_USDT format
        if symbol.startswith("OKEX_PERP_"):
            # OKEX_PERP_BTC_USDT → BTC-USDT-SWAP
            parts = symbol.replace("OKEX_PERP_", "").split("_")
            if len(parts) >= 2:
                return f"{parts[0]}-{parts[1]}-SWAP"
        if symbol.endswith("-USDT-SWAP"):
            return symbol
        if symbol.endswith("-USDT"):
            return f"{symbol}-SWAP"
        return f"{symbol}-USDT-SWAP"

    def _format_instrument_display(self, inst_id: str) -> str:
        """Convert OKX instrument ID to OKEX_PERP format for display.
        
        BTC-USDT-SWAP → OKEX_PERP_BTC_USDT
        """
        return "OKEX_PERP_" + inst_id.replace("-SWAP", "").replace("-", "_")

    def _parse_side(self, side_str: str) -> Optional[str]:
        """Parse side string to buy/sell."""
        side_str = side_str.lower().strip()
        if side_str in ("long", "buy", "b", "l"):
            return "buy"
        elif side_str in ("short", "sell", "s", "sh"):
            return "sell"
        return None

    async def cmd_market_trade(self, text: str):
        """Place a market order using base amount.
        
        Usage: /market_trade INSTRUMENT PORTFOLIO AMOUNT
        AMOUNT is in base currency (e.g. 3 = 3 BTC), positive=buy, negative=sell
        
        Examples:
            /market_trade OKEX_PERP_BTC_USDT OKEXTEST_MAIN_01 3      → buy 3 BTC
            /market_trade OKEX_PERP_ETH_USDT OKEXTEST_MAIN_01 -10    → sell 10 ETH
        """
        parts = text.split()
        if len(parts) < 4:
            await self.send(
                "Usage: <code>/market_trade INST PORTFOLIO AMOUNT</code>\n\n"
                "AMOUNT in base currency (+ buy, - sell)\n\n"
                "Examples:\n"
                "<code>/market_trade OKEX_PERP_BTC_USDT OKEXTEST_MAIN_01 3</code>\n"
                "<code>/market_trade OKEX_PERP_ETH_USDT OKEXTEST_MAIN_01 -10</code>"
            )
            return

        inst = self._resolve_instrument(parts[1])
        portfolio = parts[2].upper()
        
        try:
            base_amount = float(parts[3])
        except ValueError:
            await self.send(f"❌ Invalid amount: <code>{parts[3]}</code>")
            return

        if base_amount == 0:
            await self.send("❌ Amount cannot be zero")
            return

        # Determine side from sign
        side = "buy" if base_amount > 0 else "sell"
        abs_amount = abs(base_amount)

        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            # Get instrument info for contract conversion
            inst_info = trader.get_instrument_info(inst)
            contract_value = inst_info["contract_value"]  # e.g. 0.01 BTC per contract
            lot_size = inst_info["lot_size"]
            min_size = inst_info["min_size"]

            # Convert base amount to contracts: 3 BTC / 0.01 = 300 contracts
            size = abs_amount / contract_value

            # Round to lot size
            if lot_size > 0:
                size = round(size / lot_size) * lot_size
                size = round(size, 10)

            if size < min_size:
                await self.send(
                    f"❌ Size too small\n"
                    f"Base amount {abs_amount} → {size} contracts (min: {min_size})\n"
                    f"Contract value: {contract_value}"
                )
                return

            # Get current price for display
            ticker = trader.get_ticker(inst)
            price = ticker["last_price"]
            notional = abs_amount * price

            # Set leverage
            try:
                trader.set_leverage(inst, trading_config.default_leverage)
            except Exception:
                pass

            # Place market order
            result = trader.place_market_order(inst_id=inst, side=side, size=size)

            side_emoji = "🟢 BUY" if side == "buy" else "🔴 SELL"
            inst_display = self._format_instrument_display(inst)
            base_symbol = inst.split("-")[0]

            await self.send(
                f"✅ <b>Market Trade Executed</b>\n\n"
                f"{side_emoji} <b>{inst_display}</b>\n"
                f"📋 Portfolio: <code>{portfolio}</code>\n"
                f"📏 Amount: <code>{abs_amount} {base_symbol}</code>\n"
                f"📦 Contracts: <code>{size}</code>\n"
                f"💵 Price: <code>${price:,.2f}</code>\n"
                f"💰 Notional: <code>${notional:,.2f}</code>\n"
                f"🆔 Order: <code>{result.get('order_id', 'N/A')}</code>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )

            # Save to DB
            await self.connect_db()
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trading_orders 
                    (portfolio_name, strategy_name, instrument, side, order_type, size, price, order_id, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, portfolio, "manual", inst, side, "market",
                    size, price, result.get("order_id", ""), "filled")

        except Exception as e:
            await self.send(f"❌ Market trade failed: {e}")

    async def cmd_close_position(self, text: str):
        """Close entire position for an instrument + portfolio.
        
        Usage: /close_position INSTRUMENT PORTFOLIO
        Example: /close_position OKEX_PERP_BTC_USDT OKEXTEST_MAIN_01
        """
        parts = text.split()
        if len(parts) < 3:
            await self.send(
                "Usage: <code>/close_position INST PORTFOLIO</code>\n\n"
                "Example:\n"
                "<code>/close_position OKEX_PERP_BTC_USDT OKEXTEST_MAIN_01</code>"
            )
            return

        inst = self._resolve_instrument(parts[1])
        portfolio = parts[2].upper()
        inst_display = self._format_instrument_display(inst)

        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            # Check position exists
            position = trader.get_position(inst)
            if not position:
                await self.send(f"❌ No open position for <b>{inst_display}</b>")
                return

            side = position["side"]
            size = position["size"]
            entry = position["avg_entry_price"]
            upl = position["unrealized_pnl"]
            contract_value = 1
            try:
                inst_info = trader.get_instrument_info(inst)
                contract_value = inst_info["contract_value"]
            except Exception:
                pass
            base_amount = size * contract_value
            base_symbol = inst.split("-")[0]

            # Close
            result = trader.close_position(inst)

            side_emoji = "🟢" if side == "long" else "🔴"
            pnl_emoji = "📈" if upl >= 0 else "📉"

            await self.send(
                f"✅ <b>Position Closed</b>\n\n"
                f"{side_emoji} <b>{inst_display}</b> {side.upper()}\n"
                f"📋 Portfolio: <code>{portfolio}</code>\n"
                f"📏 Amount: <code>{base_amount:.4f} {base_symbol}</code> ({size} contracts)\n"
                f"💵 Entry: <code>${entry:,.4f}</code>\n"
                f"{pnl_emoji} PnL: <code>${upl:+,.2f}</code>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )

            # Update DB
            await self.connect_db()
            async with self._pool.acquire() as conn:
                close_side = "sell" if side == "long" else "buy"
                await conn.execute("""
                    INSERT INTO trading_orders 
                    (portfolio_name, strategy_name, instrument, side, order_type, size, price, status, pnl)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, portfolio, "manual_close", inst, close_side,
                    "market", size, entry, "filled", upl)
                await conn.execute("""
                    UPDATE positions SET status = 'closed', closed_at = NOW(), realized_pnl = $1
                    WHERE portfolio_name = $2 AND instrument = $3 AND status = 'open'
                """, upl, portfolio, inst)

        except Exception as e:
            await self.send(f"❌ Close position failed: {e}")

    async def cmd_get_positions(self, text: str):
        """Get all open positions, optionally filtered by portfolio.
        
        Usage: /get_positions [PORTFOLIO]
        Example: /get_positions OKEXTEST_MAIN_01
        """
        parts = text.split()
        portfolio_filter = parts[1].upper() if len(parts) >= 2 else None

        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            positions = trader.get_positions()
            if not positions:
                await self.send("📊 No open positions.")
                return

            # Get contract values for base amount display
            total_upl = 0
            lines = [f"📊 <b>Open Positions</b>"]
            if portfolio_filter:
                lines[0] += f" — <code>{portfolio_filter}</code>"
            lines.append("")

            for p in positions:
                inst = p["instrument"]
                inst_display = self._format_instrument_display(inst)
                side = p["side"]
                size = p["size"]
                entry = p["avg_entry_price"]
                mark = p["current_price"]
                upl = p["unrealized_pnl"]
                notional = p["notional_usd"]
                total_upl += upl

                # Get base amount
                try:
                    inst_info = trader.get_instrument_info(inst)
                    contract_value = inst_info["contract_value"]
                except Exception:
                    contract_value = 1
                base_amount = size * contract_value
                base_symbol = inst.split("-")[0]

                side_emoji = "🟢" if side == "long" else "🔴"
                pnl_emoji = "📈" if upl >= 0 else "📉"

                lines.append(
                    f"{side_emoji} <b>{inst_display}</b> {side.upper()}\n"
                    f"   📏 {base_amount:.4f} {base_symbol} ({size} cts)\n"
                    f"   💵 Entry: <code>${entry:,.4f}</code> → Mark: <code>${mark:,.4f}</code>\n"
                    f"   {pnl_emoji} UPnL: <code>${upl:+,.2f}</code> | 💰 ${notional:,.0f}\n"
                )

            upl_emoji = "📈" if total_upl >= 0 else "📉"
            lines.append(f"━━━━━━━━━━━━━━━━━━━━━━")
            lines.append(f"{upl_emoji} <b>Total UPnL:</b> <code>${total_upl:+,.2f}</code>")
            lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")

            await self.send("\n".join(lines))

        except Exception as e:
            await self.send(f"❌ Get positions failed: {e}")

    async def cmd_price(self, text: str):
        """Get current price for a symbol."""
        parts = text.split()
        if len(parts) < 2:
            await self.send("Usage: <code>/price SYMBOL</code>\ne.g. <code>/price BTC</code>")
            return

        from .okx_trader import OKXTrader
        trader = OKXTrader()
        inst = self._resolve_instrument(parts[1])

        try:
            ticker = trader.get_ticker(inst)
            symbol = parts[1].upper()
            await self.send(
                f"💲 <b>{symbol}</b>\n\n"
                f"📍 Last: <code>${ticker['last_price']:,.4f}</code>\n"
                f"🟢 Bid: <code>${ticker['bid']:,.4f}</code>\n"
                f"🔴 Ask: <code>${ticker['ask']:,.4f}</code>\n"
                f"📊 24h Vol: <code>{ticker['volume_24h']:,.0f}</code>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )
        except Exception as e:
            await self.send(f"❌ Error fetching price for {inst}: {e}")

    async def cmd_open(self, text: str):
        """Open a position with market order.
        
        Usage: /open SYMBOL SIDE [SIZE]
        Examples:
            /open BTC long
            /open ETH short 5
        """
        parts = text.split()
        if len(parts) < 3:
            await self.send(
                "Usage: <code>/open SYMBOL SIDE [SIZE]</code>\n\n"
                "Examples:\n"
                "<code>/open BTC long</code> — auto size\n"
                "<code>/open ETH short 5</code> — 5 contracts"
            )
            return

        inst = self._resolve_instrument(parts[1])
        side = self._parse_side(parts[2])
        if not side:
            await self.send(f"❌ Invalid side: <code>{parts[2]}</code>\nUse: long/buy or short/sell")
            return

        # Parse optional size
        manual_size = None
        if len(parts) >= 4:
            try:
                manual_size = float(parts[3])
            except ValueError:
                await self.send(f"❌ Invalid size: <code>{parts[3]}</code>")
                return

        from .okx_trader import OKXTrader
        from .risk_manager import RiskManager
        trader = OKXTrader()

        try:
            # Get instrument info
            inst_info = trader.get_instrument_info(inst)
            contract_value = inst_info.get("contract_value", 1)
            lot_size = inst_info.get("lot_size", inst_info.get("min_size", 1))
            min_size = inst_info.get("min_size", 1)

            # Get current price
            ticker = trader.get_ticker(inst)
            price = ticker["last_price"]

            if manual_size:
                size = manual_size
            else:
                # Auto-calculate using risk manager
                risk_mgr = RiskManager()
                balance = trader.get_balance()
                risk_mgr.update_balance(balance)
                size = risk_mgr.calculate_position_size(
                    instrument=inst,
                    entry_price=price,
                    stop_loss=None,
                    signal_size_pct=None,
                    contract_value=contract_value
                )

            # Round to lot size
            if lot_size > 0:
                size = round(size / lot_size) * lot_size
                size = round(size, 10)

            if size < min_size:
                await self.send(f"❌ Calculated size {size} below minimum {min_size}")
                return

            # Confirmation message
            side_emoji = "🟢 LONG" if side == "buy" else "🔴 SHORT"
            symbol = parts[1].upper()
            notional = size * contract_value * price

            # Set leverage
            try:
                trader.set_leverage(inst, trading_config.default_leverage)
            except Exception:
                pass

            # Place market order
            result = trader.place_market_order(inst_id=inst, side=side, size=size)

            await self.send(
                f"✅ <b>Order Placed</b>\n\n"
                f"{side_emoji} <b>{symbol}</b>\n"
                f"📏 Size: <code>{size}</code> contracts\n"
                f"💵 Price: <code>${price:,.4f}</code>\n"
                f"💰 Notional: <code>${notional:,.2f}</code>\n"
                f"🆔 Order: <code>{result.get('order_id', 'N/A')}</code>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )

            # Save to DB
            await self.connect_db()
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trading_orders 
                    (portfolio_name, strategy_name, instrument, side, order_type, size, price, order_id, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, trading_config.portfolio_name, "manual", inst, side, "market",
                    size, price, result.get("order_id", ""), "filled")

        except Exception as e:
            await self.send(f"❌ Order failed: {e}")

    async def cmd_close(self, text: str):
        """Close a specific position.
        
        Usage: /close SYMBOL
        Example: /close ETH
        """
        parts = text.split()
        if len(parts) < 2:
            await self.send(
                "Usage: <code>/close SYMBOL</code>\n\n"
                "Example: <code>/close ETH</code>"
            )
            return

        inst = self._resolve_instrument(parts[1])
        symbol = parts[1].upper()

        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            # Check if position exists
            position = trader.get_position(inst)
            if not position:
                await self.send(f"❌ No open position for <b>{symbol}</b>")
                return

            side = position["side"]
            size = position["size"]
            entry = position["avg_entry_price"]
            upl = position["unrealized_pnl"]

            # Close it
            result = trader.close_position(inst)

            side_emoji = "🟢" if side == "long" else "🔴"
            pnl_emoji = "📈" if upl >= 0 else "📉"

            await self.send(
                f"✅ <b>Position Closed</b>\n\n"
                f"{side_emoji} <b>{symbol}</b> {side.upper()}\n"
                f"📏 Size: <code>{size}</code>\n"
                f"💵 Entry: <code>${entry:,.4f}</code>\n"
                f"{pnl_emoji} Realized PnL: <code>${upl:+,.2f}</code>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )

            # Save to DB
            await self.connect_db()
            async with self._pool.acquire() as conn:
                # Record the close order
                close_side = "sell" if side == "long" else "buy"
                await conn.execute("""
                    INSERT INTO trading_orders 
                    (portfolio_name, strategy_name, instrument, side, order_type, size, price, status, pnl)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, trading_config.portfolio_name, "manual_close", inst, close_side,
                    "market", size, entry, "filled", upl)

                # Close in positions table
                await conn.execute("""
                    UPDATE positions SET status = 'closed', closed_at = NOW(), realized_pnl = $1
                    WHERE portfolio_name = $2 AND instrument = $3 AND status = 'open'
                """, upl, trading_config.portfolio_name, inst)

        except Exception as e:
            await self.send(f"❌ Close failed: {e}")

    async def cmd_closeall(self, text: str):
        """Close ALL open positions."""
        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            positions = trader.get_positions()
            if not positions:
                await self.send("📊 No open positions to close.")
                return

            await self.send(f"⏳ Closing {len(positions)} positions...")

            results = []
            total_pnl = 0
            for pos in positions:
                inst = pos["instrument"]
                symbol = inst.replace("-USDT-SWAP", "")
                upl = pos["unrealized_pnl"]
                try:
                    trader.close_position(inst)
                    results.append(f"✅ {symbol}: ${upl:+,.2f}")
                    total_pnl += upl
                except Exception as e:
                    results.append(f"❌ {symbol}: {e}")

            pnl_emoji = "📈" if total_pnl >= 0 else "📉"
            await self.send(
                f"🏁 <b>Close All Results</b>\n\n"
                + "\n".join(results)
                + f"\n\n{pnl_emoji} <b>Total PnL:</b> <code>${total_pnl:+,.2f}</code>"
                + f"\n🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )

            # Update DB
            await self.connect_db()
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    UPDATE positions SET status = 'closed', closed_at = NOW()
                    WHERE portfolio_name = $1 AND status = 'open'
                """, trading_config.portfolio_name)

        except Exception as e:
            await self.send(f"❌ Close all failed: {e}")

    async def cmd_limit(self, text: str):
        """Place a limit order.
        
        Usage: /limit SYMBOL SIDE PRICE [SIZE]
        Examples:
            /limit BTC long 80000
            /limit ETH short 2200 10
        """
        parts = text.split()
        if len(parts) < 4:
            await self.send(
                "Usage: <code>/limit SYMBOL SIDE PRICE [SIZE]</code>\n\n"
                "Examples:\n"
                "<code>/limit BTC long 80000</code>\n"
                "<code>/limit ETH short 2200 10</code>"
            )
            return

        inst = self._resolve_instrument(parts[1])
        side = self._parse_side(parts[2])
        if not side:
            await self.send(f"❌ Invalid side: <code>{parts[2]}</code>")
            return

        try:
            price = float(parts[3])
        except ValueError:
            await self.send(f"❌ Invalid price: <code>{parts[3]}</code>")
            return

        manual_size = None
        if len(parts) >= 5:
            try:
                manual_size = float(parts[4])
            except ValueError:
                await self.send(f"❌ Invalid size: <code>{parts[4]}</code>")
                return

        from .okx_trader import OKXTrader
        from .risk_manager import RiskManager
        trader = OKXTrader()

        try:
            inst_info = trader.get_instrument_info(inst)
            contract_value = inst_info.get("contract_value", 1)
            lot_size = inst_info.get("lot_size", inst_info.get("min_size", 1))
            min_size = inst_info.get("min_size", 1)

            if manual_size:
                size = manual_size
            else:
                risk_mgr = RiskManager()
                balance = trader.get_balance()
                risk_mgr.update_balance(balance)
                size = risk_mgr.calculate_position_size(
                    instrument=inst,
                    entry_price=price,
                    stop_loss=None,
                    signal_size_pct=None,
                    contract_value=contract_value
                )

            if lot_size > 0:
                size = round(size / lot_size) * lot_size
                size = round(size, 10)

            if size < min_size:
                await self.send(f"❌ Calculated size {size} below minimum {min_size}")
                return

            # Set leverage
            try:
                trader.set_leverage(inst, trading_config.default_leverage)
            except Exception:
                pass

            result = trader.place_limit_order(
                inst_id=inst, side=side, size=size, price=price
            )

            side_emoji = "🟢 LONG" if side == "buy" else "🔴 SHORT"
            symbol = parts[1].upper()
            notional = size * contract_value * price

            await self.send(
                f"✅ <b>Limit Order Placed</b>\n\n"
                f"{side_emoji} <b>{symbol}</b>\n"
                f"📏 Size: <code>{size}</code> contracts\n"
                f"💵 Price: <code>${price:,.4f}</code>\n"
                f"💰 Notional: <code>${notional:,.2f}</code>\n"
                f"🆔 Order: <code>{result.get('order_id', 'N/A')}</code>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )

            # Save to DB
            await self.connect_db()
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trading_orders 
                    (portfolio_name, strategy_name, instrument, side, order_type, size, price, order_id, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, trading_config.portfolio_name, "manual", inst, side, "limit",
                    size, price, result.get("order_id", ""), "pending")

        except Exception as e:
            await self.send(f"❌ Limit order failed: {e}")

    async def cmd_cancel(self, text: str):
        """Cancel a pending order.
        
        Usage: /cancel ORDER_ID SYMBOL
        Example: /cancel 12345678 BTC
        """
        parts = text.split()
        if len(parts) < 3:
            await self.send(
                "Usage: <code>/cancel ORDER_ID SYMBOL</code>\n\n"
                "Example: <code>/cancel 12345678 BTC</code>"
            )
            return

        order_id = parts[1]
        inst = self._resolve_instrument(parts[2])
        symbol = parts[2].upper()

        from .okx_trader import OKXTrader
        trader = OKXTrader()

        try:
            result = trader.cancel_order(inst_id=inst, order_id=order_id)
            await self.send(
                f"✅ <b>Order Cancelled</b>\n\n"
                f"🆔 Order: <code>{order_id}</code>\n"
                f"📊 Symbol: <b>{symbol}</b>\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
            )
        except Exception as e:
            await self.send(f"❌ Cancel failed: {e}")

    # ==================== Main Loop ====================

    async def _flush_old_updates(self):
        """Flush any pending updates on startup so we don't re-process old commands."""
        try:
            resp = await self.client.get(f"{BASE_URL}/getUpdates", params={"offset": -1, "limit": 1})
            data = resp.json()
            if data.get("ok") and data.get("result"):
                self.last_update_id = data["result"][-1]["update_id"]
                logger.info(f"Flushed old updates, starting from {self.last_update_id}")
        except Exception as e:
            logger.error(f"Flush failed: {e}")

    async def run(self):
        """Poll for commands."""
        await self._flush_old_updates()
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
