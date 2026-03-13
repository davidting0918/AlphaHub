"""
Telegram Notification Helper

Simple helper for sending Telegram notifications from pipelines.
"""

import requests
import logging
from typing import Optional


logger = logging.getLogger(__name__)


def send_telegram(
    bot_token: str,
    chat_id: str,
    message: str,
    parse_mode: str = "HTML",
    disable_notification: bool = False,
    timeout: int = 10
) -> bool:
    """
    Send a message via Telegram Bot API
    
    Args:
        bot_token: Telegram bot token
        chat_id: Target chat ID
        message: Message text (supports HTML if parse_mode=HTML)
        parse_mode: "HTML" or "Markdown"
        disable_notification: Send silently
        timeout: Request timeout in seconds
        
    Returns:
        True if message was sent successfully, False otherwise
    """
    if not bot_token or not chat_id:
        logger.warning("Telegram credentials not configured, skipping notification")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_notification": disable_notification,
    }
    
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        response_data = response.json()
        
        if response.status_code == 200 and response_data.get('ok'):
            logger.debug(f"Telegram notification sent successfully")
            return True
        else:
            error_desc = response_data.get('description', 'Unknown error')
            logger.error(f"Telegram API error: {error_desc}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Telegram notification: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending Telegram notification: {e}")
        return False
