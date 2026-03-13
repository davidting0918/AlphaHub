"""
Base Job Class (Async)

All pipeline jobs inherit from this. Provides:
- Async DB via database.PostgresClient
- Portfolio → Exchange → Adaptor resolution (DB-driven, zero hardcode)
- Common interface: setup() → run() → teardown()
"""

import os
import importlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional

from database.client import PostgresClient

logger = logging.getLogger(__name__)


def _get_client_class(adaptor: str):
    """
    Dynamically import and return the Client class for an adaptor.

    Each adaptor package (adaptor/{name}/__init__.py) must export a `Client` alias.
    The adaptor name comes from the `exchanges.adaptor` column in DB.

    Example: adaptor='binance' → from adaptor.binance import Client
    """
    module_path = f"adaptor.{adaptor}"
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ValueError(
            f"Adaptor package '{module_path}' not found. "
            f"Ensure adaptor/{adaptor}/__init__.py exists."
        )

    client_class = getattr(module, "Client", None)
    if client_class is None:
        raise ValueError(
            f"Adaptor '{module_path}' does not export a 'Client' class. "
            f"Add 'Client = YourClient' to adaptor/{adaptor}/__init__.py."
        )

    return client_class


class BaseJob(ABC):
    """
    Async base class for pipeline jobs.

    Subclass must implement:
        async def run(self) → job logic

    Optional overrides:
        async def setup(self)
        async def teardown(self)
    """

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

        self.db_url = db_url or os.environ.get("DATABASE_URL")
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable or db_url parameter required")

        self.db: Optional[PostgresClient] = None
        self.portfolio: Optional[Dict[str, Any]] = None
        self.exchange_client = None

    # ==================== DB ====================

    async def _init_db(self):
        """Initialize async DB client."""
        self.db = PostgresClient()
        await self.db.init_pool()

    async def _close_db(self):
        """Close async DB client."""
        if self.db:
            await self.db.close()
            self.db = None

    # ==================== Portfolio Resolution ====================

    async def _resolve_portfolio(self):
        """Look up portfolio by name → get exchange + adaptor info."""
        row = await self.db.read_one("""
            SELECT p.id AS portfolio_id,
                   p.name AS portfolio_name,
                   p.strategy_id,
                   p.exchange_id,
                   p.config,
                   e.name AS exchange_name,
                   e.adaptor AS adaptor
            FROM portfolios p
            JOIN exchanges e ON e.id = p.exchange_id
            WHERE p.name = $1
        """, self.portfolio_name)

        if not row:
            raise ValueError(f"Portfolio '{self.portfolio_name}' not found in database")

        self.portfolio = row
        logger.info(
            f"[{self.JOB_NAME}] Portfolio: {self.portfolio['portfolio_name']} | "
            f"Exchange: {self.portfolio['exchange_name']} (id={self.portfolio['exchange_id']}) | "
            f"Adaptor: {self.portfolio['adaptor']}"
        )

    def _resolve_exchange_client(self):
        """Resolve exchange client dynamically from DB adaptor field."""
        exchange_name = self.portfolio["exchange_name"]
        adaptor = self.portfolio["adaptor"]

        client_class = _get_client_class(adaptor)
        self.exchange_client = client_class(exchange_name=exchange_name)
        logger.info(
            f"[{self.JOB_NAME}] Using {client_class.__name__} "
            f"(adaptor={adaptor}) for: {exchange_name}"
        )

    # ==================== Lifecycle ====================

    async def setup(self):
        """Called before run(). Override for custom setup."""
        await self._init_db()
        await self._resolve_portfolio()
        self._resolve_exchange_client()

    @abstractmethod
    async def run(self):
        """Main job logic. Must be implemented by subclass."""
        pass

    async def teardown(self):
        """Called after run(). Override for custom cleanup."""
        if self.exchange_client and hasattr(self.exchange_client, 'close'):
            self.exchange_client.close()
        await self._close_db()

    async def execute(self):
        """Full lifecycle: setup → run → teardown."""
        try:
            logger.info(f"[{self.JOB_NAME}] Starting...")
            await self.setup()
            await self.run()
            logger.info(f"[{self.JOB_NAME}] Completed successfully")
        except Exception as e:
            logger.exception(f"[{self.JOB_NAME}] Failed: {e}")
            raise
        finally:
            await self.teardown()
