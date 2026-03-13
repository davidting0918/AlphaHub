"""
Base Job Class

All pipeline jobs inherit from this. Provides:
- DB connection management
- Portfolio → Exchange client resolution
- Telegram notifications
- Common interface: setup() → run() → teardown()
"""

import os
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from pipeline.notify import send_telegram

logger = logging.getLogger(__name__)


class BaseJob(ABC):
    """
    Base class for all pipeline jobs.

    Every job must implement:
        - run(self) → execute the job logic

    Optional overrides:
        - setup(self)    → called before run()
        - teardown(self) → called after run() (always, even on error)
    """

    # Subclass should set this for logging/notifications
    JOB_NAME: str = "BaseJob"

    def __init__(
        self,
        portfolio_name: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        db_url: Optional[str] = None,
        telegram_bot_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None,
    ):
        self.portfolio_name = portfolio_name
        self.start = start
        self.end = end

        # Config from env or params
        self.db_url = db_url or os.environ.get("DATABASE_URL")
        self.telegram_bot_token = telegram_bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = telegram_chat_id or os.environ.get("TELEGRAM_CHAT_ID")

        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable or db_url parameter required")

        self._conn: Optional[psycopg2.extensions.connection] = None

        # Resolved during setup
        self.portfolio: Optional[Dict[str, Any]] = None
        self.exchange_client = None

    # ==================== DB ====================

    def _connect(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.db_url)
            self._conn.autocommit = False
        return self._conn

    def _close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()

    @contextmanager
    def get_cursor(self):
        conn = self._connect()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    # ==================== Portfolio Resolution ====================

    def _resolve_portfolio(self):
        """Look up portfolio by name → get exchange info"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT p.id AS portfolio_id,
                       p.name AS portfolio_name,
                       p.strategy_id,
                       p.exchange_id,
                       p.config,
                       e.name AS exchange_name
                FROM portfolios p
                JOIN exchanges e ON e.id = p.exchange_id
                WHERE p.name = %s
            """, (self.portfolio_name,))
            row = cursor.fetchone()

        if not row:
            raise ValueError(f"Portfolio '{self.portfolio_name}' not found in database")

        self.portfolio = dict(row)
        logger.info(
            f"[{self.JOB_NAME}] Portfolio: {self.portfolio['portfolio_name']} | "
            f"Exchange: {self.portfolio['exchange_name']} (id={self.portfolio['exchange_id']})"
        )

    def _resolve_exchange_client(self):
        """Resolve exchange client based on portfolio's exchange_id"""
        from pipeline.exchange_registry import get_exchange_client

        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        self.exchange_client = get_exchange_client(exchange_id, exchange_name)
        logger.info(f"[{self.JOB_NAME}] Using exchange client for: {exchange_name} (id={exchange_id})")

    # ==================== Notifications ====================

    def notify(self, message: str, silent: bool = False) -> bool:
        return send_telegram(
            bot_token=self.telegram_bot_token,
            chat_id=self.telegram_chat_id,
            message=message,
            disable_notification=silent,
        )

    def notify_success(self, summary: str):
        message = f"✅ <b>{self.JOB_NAME}</b>\n{summary}"
        self.notify(message, silent=True)

    def notify_error(self, error: str):
        message = f"❌ <b>{self.JOB_NAME} Failed</b>\n<code>{error[:500]}</code>"
        self.notify(message, silent=False)

    # ==================== Lifecycle ====================

    def setup(self):
        """Called before run(). Override for custom setup."""
        self._resolve_portfolio()
        self._resolve_exchange_client()

    @abstractmethod
    def run(self):
        """Main job logic. Must be implemented by subclass."""
        pass

    def teardown(self):
        """Called after run(). Override for custom cleanup."""
        if self.exchange_client and hasattr(self.exchange_client, 'close'):
            self.exchange_client.close()
        self._close()

    def execute(self):
        """Full lifecycle: setup → run → teardown"""
        try:
            logger.info(f"[{self.JOB_NAME}] Starting...")
            self.setup()
            self.run()
            logger.info(f"[{self.JOB_NAME}] Completed successfully")
        except Exception as e:
            logger.exception(f"[{self.JOB_NAME}] Failed: {e}")
            self.notify_error(str(e))
            raise
        finally:
            self.teardown()
