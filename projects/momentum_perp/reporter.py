"""
Telegram Reporter

Sends trade notifications, position updates, and daily summaries.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import httpx

from .config import telegram_config, trading_config

logger = logging.getLogger(__name__)


class TelegramReporter:
    """
    Telegram bot for trading notifications.
    
    Usage:
        reporter = TelegramReporter()
        await reporter.send_trade_notification(order)
        await reporter.send_position_update(position)
        await reporter.send_daily_summary(stats)
    """
    
    def __init__(self):
        self.bot_token = telegram_config.bot_token
        self.chat_id = telegram_config.chat_id
        self.portfolio_name = trading_config.portfolio_name
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        
    async def _send_message(
        self,
        text: str,
        parse_mode: str = "HTML",
        disable_notification: bool = False
    ) -> bool:
        """Send a message via Telegram."""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_notification": disable_notification,
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, timeout=10)
                result = response.json()
                
                if not result.get("ok"):
                    logger.error(f"Telegram error: {result.get('description')}")
                    return False
                    
                return True
                
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    def send_message_sync(
        self,
        text: str,
        parse_mode: str = "HTML"
    ) -> bool:
        """Synchronous message send."""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        
        try:
            import requests
            response = requests.post(url, json=payload, timeout=10)
            result = response.json()
            return result.get("ok", False)
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    # ==================== Trade Notifications ====================
    
    async def send_trade_notification(self, order: Dict[str, Any]):
        """Send notification when a trade is placed/filled."""
        side = order.get("side", "").upper()
        instrument = order.get("instrument", "")
        size = order.get("size", 0)
        price = order.get("filled_price") or order.get("price") or order.get("entry_price", 0)
        status = order.get("status", "")
        strategy = order.get("strategy_name", "unknown")
        
        # Emoji based on side
        emoji = "🟢" if side == "BUY" else "🔴"
        
        message = f"""
{emoji} <b>Trade Executed</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
📈 <b>Instrument:</b> <code>{instrument}</code>
↔️ <b>Side:</b> <code>{side}</code>
📦 <b>Size:</b> <code>{size}</code>
💰 <b>Price:</b> <code>{price:.4f}</code>
🎯 <b>Strategy:</b> <code>{strategy}</code>
✅ <b>Status:</b> <code>{status}</code>
"""
        
        # Add SL/TP if available
        if order.get("stop_loss"):
            message += f"🛑 <b>Stop Loss:</b> <code>{order['stop_loss']:.4f}</code>\n"
        if order.get("take_profit"):
            message += f"🎯 <b>Take Profit:</b> <code>{order['take_profit']:.4f}</code>\n"
        
        message += f"\n🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        await self._send_message(message)
    
    async def send_signal_alert(
        self,
        instrument: str,
        signal_type: str,
        strategy: str,
        strength: float,
        notes: str = ""
    ):
        """Send alert when a strategy generates a signal."""
        emoji_map = {
            "entry_long": "🟢 LONG",
            "entry_short": "🔴 SHORT",
            "exit_long": "⬛ EXIT LONG",
            "exit_short": "⬛ EXIT SHORT",
            "stop_loss": "🛑 STOP LOSS",
            "take_profit": "🎯 TAKE PROFIT",
        }
        
        signal_display = emoji_map.get(signal_type, signal_type)
        
        message = f"""
📡 <b>Signal Alert</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
📈 <b>Instrument:</b> <code>{instrument}</code>
⚡ <b>Signal:</b> <code>{signal_display}</code>
🎯 <b>Strategy:</b> <code>{strategy}</code>
💪 <b>Strength:</b> <code>{strength:.0%}</code>
"""
        
        if notes:
            message += f"📝 <b>Notes:</b> {notes}\n"
        
        message += f"\n🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        await self._send_message(message, disable_notification=True)
    
    # ==================== Position Updates ====================
    
    async def send_position_update(
        self,
        position: Dict[str, Any],
        action: str = "opened"  # "opened", "closed", "updated"
    ):
        """Send notification when position changes."""
        instrument = position.get("instrument", "")
        side = position.get("side", "").upper()
        size = position.get("size", 0)
        entry_price = position.get("avg_entry_price", 0)
        current_price = position.get("current_price", entry_price)
        pnl = position.get("unrealized_pnl", 0)
        
        # Calculate PnL %
        if entry_price > 0 and size > 0:
            pnl_pct = (current_price - entry_price) / entry_price * 100
            if side == "SHORT":
                pnl_pct = -pnl_pct
        else:
            pnl_pct = 0
        
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        action_emoji = {"opened": "📈", "closed": "📉", "updated": "🔄"}.get(action, "📊")
        
        message = f"""
{action_emoji} <b>Position {action.title()}</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
📈 <b>Instrument:</b> <code>{instrument}</code>
↔️ <b>Side:</b> <code>{side}</code>
📦 <b>Size:</b> <code>{size}</code>
💰 <b>Entry:</b> <code>{entry_price:.4f}</code>
📈 <b>Current:</b> <code>{current_price:.4f}</code>
{pnl_emoji} <b>PnL:</b> <code>{pnl:+.2f} USDT ({pnl_pct:+.2f}%)</code>
"""
        
        if action == "closed":
            realized_pnl = position.get("realized_pnl", pnl)
            message += f"\n💵 <b>Realized PnL:</b> <code>{realized_pnl:+.2f} USDT</code>"
        
        message += f"\n🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        await self._send_message(message)
    
    # ==================== Balance & Summary ====================
    
    async def send_balance_snapshot(self, balance: Dict[str, Any]):
        """Send periodic balance snapshot."""
        total_equity = balance.get("total_equity", 0)
        available = balance.get("available_balance", 0)
        unrealized_pnl = balance.get("unrealized_pnl", 0)
        
        pnl_emoji = "🟢" if unrealized_pnl >= 0 else "🔴"
        
        message = f"""
💰 <b>Balance Snapshot</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
💵 <b>Total Equity:</b> <code>{total_equity:,.2f} USDT</code>
💳 <b>Available:</b> <code>{available:,.2f} USDT</code>
{pnl_emoji} <b>Unrealized PnL:</b> <code>{unrealized_pnl:+,.2f} USDT</code>

🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
        
        await self._send_message(message, disable_notification=True)
    
    async def send_daily_summary(self, stats: Dict[str, Any]):
        """Send end-of-day summary."""
        total_equity = stats.get("total_equity", 0)
        daily_pnl = stats.get("daily_pnl", 0)
        daily_pnl_pct = stats.get("daily_pnl_pct", 0)
        trades_count = stats.get("trades_count", 0)
        win_rate = stats.get("win_rate", 0)
        best_trade = stats.get("best_trade", 0)
        worst_trade = stats.get("worst_trade", 0)
        open_positions = stats.get("open_positions", 0)
        
        pnl_emoji = "🟢" if daily_pnl >= 0 else "🔴"
        
        message = f"""
📊 <b>Daily Summary</b>

📋 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
📅 <b>Date:</b> <code>{datetime.utcnow().strftime('%Y-%m-%d')}</code>

💵 <b>Total Equity:</b> <code>{total_equity:,.2f} USDT</code>
{pnl_emoji} <b>Daily PnL:</b> <code>{daily_pnl:+,.2f} USDT ({daily_pnl_pct:+.2f}%)</code>

📈 <b>Trades Today:</b> <code>{trades_count}</code>
🎯 <b>Win Rate:</b> <code>{win_rate:.0%}</code>
🏆 <b>Best Trade:</b> <code>{best_trade:+,.2f} USDT</code>
💥 <b>Worst Trade:</b> <code>{worst_trade:+,.2f} USDT</code>
📂 <b>Open Positions:</b> <code>{open_positions}</code>

🕐 {datetime.utcnow().strftime('%H:%M:%S')} UTC
"""
        
        await self._send_message(message)
    
    async def send_risk_alert(self, alert_type: str, details: str):
        """Send risk management alert."""
        message = f"""
⚠️ <b>Risk Alert</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
🚨 <b>Alert:</b> <code>{alert_type}</code>
📝 <b>Details:</b> {details}

🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
        
        await self._send_message(message)
    
    async def send_error(self, error_type: str, error_msg: str):
        """Send error notification."""
        message = f"""
❌ <b>Error</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
🔥 <b>Type:</b> <code>{error_type}</code>
📝 <b>Message:</b> <code>{error_msg[:500]}</code>

🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
        
        await self._send_message(message)
    
    async def send_startup_message(self):
        """Send message when engine starts."""
        message = f"""
🚀 <b>Trading Engine Started</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
⚙️ <b>Mode:</b> <code>Demo/Testnet</code>
📈 <b>Instruments:</b> <code>{', '.join(trading_config.instruments[:3])}...</code>

🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
        
        await self._send_message(message)
    
    async def send_shutdown_message(self, reason: str = "Manual"):
        """Send message when engine stops."""
        message = f"""
🛑 <b>Trading Engine Stopped</b>

📊 <b>Portfolio:</b> <code>{self.portfolio_name}</code>
📝 <b>Reason:</b> <code>{reason}</code>

🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
        
        await self._send_message(message)


# Sync helper for testing
def send_test_message(text: str) -> bool:
    """Send a test message (sync)."""
    reporter = TelegramReporter()
    return reporter.send_message_sync(text)
