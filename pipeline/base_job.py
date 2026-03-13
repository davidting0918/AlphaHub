"""
Base Job Class

All pipeline jobs inherit from this. Provides:
- DB connection management
- Portfolio → Exchange client resolution
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

logger = logging.getLogger(__name__)


# Exchange name → client class mapping
# Each client class must have EXCHANGE_ID class variable
_EXCHANGE_CLIENT_MAP = {
    "OKX": ("adaptor.okx.client", "OKXClient"),
    "OKXTEST": ("adaptor.okx.client", "OKXClient"),
    # "BINANCE": ("adaptor.binance.client", "BinanceClient"),  # TODO
}


def _get_client_class(exchange_name: str):
    """Dynamically import and return the client class for an exchange."""
    name_upper = exchange_name.upper()
    if name_upper not in _EXCHANGE_CLIENT_MAP:
        raise ValueError(
            f"Exchange '{exchange_name}' not supported. "
            f"Available: {list(_EXCHANGE_CLIENT_MAP.keys())}"
        )
    
    module_path, class_name = _EXCHANGE_CLIENT_MAP[name_upper]
    
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class BaseJob(ABC):
    """
    Base class for all pipeline jobs.

    Every job must implement:
        - run(self) → execute the job logic

    Optional overrides:
        - setup(self)    → called before run()
        - teardown(self) → called after run() (always, even on error)
    """

    # Subclass should set this for logging
    JOB_NAME: str = "BaseJob"

    def __init__(
        self,
        portfolio_name: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        db_url: Optional[str] = None,
    ):
        self.portfolio_name = portfolio_name
        self.start = start
        self.end = end

        # Config from env or params
        self.db_url = db_url or os.environ.get("DATABASE_URL")

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
        exchange_id = self.portfolio["exchange_id"]
        exchange_name = self.portfolio["exchange_name"]

        # Get the client class for this exchange
        client_class = _get_client_class(exchange_name)
        
        # Verify the client class has correct EXCHANGE_ID
        if hasattr(client_class, 'EXCHANGE_ID') and client_class.EXCHANGE_ID != exchange_id:
            logger.warning(
                f"Client {client_class.__name__}.EXCHANGE_ID={client_class.EXCHANGE_ID} "
                f"doesn't match DB exchange_id={exchange_id}"
            )
        
        # Create client instance with exchange_name for instrument_id prefixing
        self.exchange_client = client_class(exchange_name=exchange_name)
        logger.info(f"[{self.JOB_NAME}] Using {client_class.__name__} for: {exchange_name} (id={exchange_id})")

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
            raise
        finally:
            self.teardown()
