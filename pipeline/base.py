"""
Base Pipeline Class

Provides common functionality for all data pipelines:
- Database connection management
- Telegram notifications
- Error handling
"""

import os
import psycopg2
import psycopg2.extras
import logging
from abc import ABC, abstractmethod
from typing import Optional
from contextlib import contextmanager

from .notify import send_telegram


logger = logging.getLogger(__name__)


class BasePipeline(ABC):
    """
    Base class for all data pipelines
    
    Handles:
    - Database connection via DATABASE_URL env var
    - Telegram notifications via TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars
    - Common error handling and logging
    """
    
    def __init__(
        self,
        db_url: Optional[str] = None,
        telegram_bot_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None
    ):
        """
        Initialize pipeline with database and notification config
        
        Args:
            db_url: PostgreSQL connection URL (defaults to DATABASE_URL env var)
            telegram_bot_token: Bot token for notifications (defaults to TELEGRAM_BOT_TOKEN env var)
            telegram_chat_id: Chat ID for notifications (defaults to TELEGRAM_CHAT_ID env var)
        """
        self.db_url = db_url or os.environ.get('DATABASE_URL')
        self.telegram_bot_token = telegram_bot_token or os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = telegram_chat_id or os.environ.get('TELEGRAM_CHAT_ID')
        
        self._conn: Optional[psycopg2.extensions.connection] = None
        
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable or db_url parameter required")
    
    @property
    def name(self) -> str:
        """Pipeline name for logging and notifications"""
        return self.__class__.__name__
    
    def connect(self) -> psycopg2.extensions.connection:
        """Establish database connection"""
        if self._conn is None or self._conn.closed:
            logger.info(f"[{self.name}] Connecting to database...")
            self._conn = psycopg2.connect(self.db_url)
            self._conn.autocommit = False
        return self._conn
    
    def close(self):
        """Close database connection"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug(f"[{self.name}] Database connection closed")
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """
        Context manager for database cursor
        
        Usage:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT ...")
        """
        conn = self.connect()
        cursor = conn.cursor(cursor_factory=cursor_factory or psycopg2.extras.DictCursor)
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def notify(self, message: str, silent: bool = False) -> bool:
        """
        Send Telegram notification
        
        Args:
            message: Message text (HTML supported)
            silent: Send without notification sound
            
        Returns:
            True if sent successfully
        """
        return send_telegram(
            bot_token=self.telegram_bot_token,
            chat_id=self.telegram_chat_id,
            message=message,
            disable_notification=silent
        )
    
    def notify_success(self, summary: str):
        """Send success notification with checkmark emoji"""
        message = f"✅ <b>{self.name}</b>\n{summary}"
        self.notify(message, silent=True)
    
    def notify_error(self, error: str):
        """Send error notification with warning emoji"""
        message = f"❌ <b>{self.name} Failed</b>\n<code>{error[:500]}</code>"
        self.notify(message, silent=False)
    
    @abstractmethod
    def run(self):
        """
        Execute the pipeline logic
        
        Must be implemented by subclasses.
        Should handle its own error reporting via notify_error.
        """
        pass
    
    def execute(self):
        """
        Run the pipeline with error handling
        
        Wraps run() with try/except and sends notifications on failure.
        """
        try:
            logger.info(f"[{self.name}] Starting pipeline...")
            self.run()
            logger.info(f"[{self.name}] Pipeline completed successfully")
        except Exception as e:
            logger.exception(f"[{self.name}] Pipeline failed: {e}")
            self.notify_error(str(e))
            raise
        finally:
            self.close()
