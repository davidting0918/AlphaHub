"""
PostgreSQL Async Database Client

Async client for CRUD operations using asyncpg connection pool.
Same pattern as VegaExchange's PostgresAsyncClient.

Features:
- Connection pooling via asyncpg
- Simple CRUD: read, read_one, insert_one, insert, upsert_one, execute
- Decimal → float auto-conversion
- Environment-based configuration
"""

import os
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

import asyncpg
from asyncpg import Pool

logger = logging.getLogger(__name__)


class PostgresClient:
    """
    Async PostgreSQL client with connection pooling.

    Usage:
        client = PostgresClient()
        await client.init_pool()

        rows = await client.read("SELECT * FROM instruments WHERE type = $1", "PERP")
        row = await client.read_one("SELECT * FROM instruments WHERE instrument_id = $1", "OKX_PERP_BTC_USDT")

        await client.insert_one("exchanges", {"name": "OKX"})
        await client.execute("UPDATE instruments SET is_active = $1 WHERE id = $2", False, 123)

        await client.close()
    """

    def __init__(self, environment: Optional[str] = None):
        """
        Initialize PostgreSQL client.

        Args:
            environment: 'test', 'staging', or 'prod' (default: from APP_ENV or 'prod')
        """
        if environment is None:
            environment = os.getenv("APP_ENV", "prod")

        self.environment = environment

        if environment == "test":
            self.connection_string = os.getenv("DATABASE_URL_TEST")
        elif environment == "staging":
            self.connection_string = os.getenv("DATABASE_URL_STAGING")
        else:
            self.connection_string = os.getenv("DATABASE_URL")

        if not self.connection_string:
            raise ValueError(
                f"No database URL for environment '{environment}'. "
                f"Set DATABASE_URL (or DATABASE_URL_TEST / DATABASE_URL_STAGING)."
            )

        self._pool: Optional[Pool] = None

    async def init_pool(self, min_size: int = 1, max_size: int = 20):
        """Initialize the connection pool."""
        if self._pool is not None:
            return

        self._pool = await asyncpg.create_pool(
            self.connection_string,
            min_size=min_size,
            max_size=max_size,
            command_timeout=60,
        )
        logger.info(f"Database pool initialized ({self.environment}, min={min_size}, max={max_size})")

    async def close(self):
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database pool closed")

    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool (auto-initializes if needed)."""
        if self._pool is None:
            await self.init_pool()

        async with self._pool.acquire() as conn:
            yield conn

    # ==================== Helpers ====================

    @staticmethod
    def _convert_decimals(obj: Any) -> Any:
        """Recursively convert Decimal → float in nested data structures."""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: PostgresClient._convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            converted = [PostgresClient._convert_decimals(item) for item in obj]
            return type(obj)(converted) if isinstance(obj, tuple) else converted
        return obj

    # ==================== Read ====================

    async def read(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query, return results as list of dicts.

        Args:
            query: SQL with $1, $2, ... placeholders
            *args: Parameters

        Returns:
            List of dicts with Decimal values converted to float
        """
        async with self.get_connection() as conn:
            rows = await conn.fetch(query, *args)
            return self._convert_decimals([dict(row) for row in rows])

    async def read_one(self, query: str, *args: Any) -> Optional[Dict[str, Any]]:
        """
        Execute a SELECT query, return first result as dict (or None).
        """
        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, *args)
            if row:
                return self._convert_decimals(dict(row))
            return None

    # ==================== Insert ====================

    async def insert_one(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a single record. Returns the inserted row.

        Args:
            table: Table name
            data: Dict of column → value
        """
        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        values = list(data.values())

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING *
        """

        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, *values)
            return self._convert_decimals(dict(row))

    async def insert(self, table: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert multiple records. Returns all inserted rows.

        Args:
            table: Table name
            data: List of dicts (all must have same keys)
        """
        if not data:
            return []

        columns = list(data[0].keys())
        n_cols = len(columns)

        value_sets = []
        all_values = []
        for i, record in enumerate(data):
            if set(record.keys()) != set(columns):
                raise ValueError(
                    f"All records must have the same columns. "
                    f"Expected: {columns}, Got: {list(record.keys())}"
                )
            row_placeholders = [f"${j + i * n_cols + 1}" for j in range(n_cols)]
            value_sets.append(f"({', '.join(row_placeholders)})")
            for col in columns:
                all_values.append(record[col])

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES {', '.join(value_sets)}
            RETURNING *
        """

        async with self.get_connection() as conn:
            rows = await conn.fetch(query, *all_values)
            return self._convert_decimals([dict(row) for row in rows])

    async def upsert_one(
        self,
        table: str,
        data: Dict[str, Any],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Insert or update a single record (ON CONFLICT DO UPDATE).

        Args:
            table: Table name
            data: Dict of column → value
            conflict_columns: Columns for ON CONFLICT
            update_columns: Columns to update on conflict (default: all non-conflict)
        """
        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        values = list(data.values())

        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        conflict_clause = ", ".join(conflict_columns)
        update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}
            RETURNING *
        """

        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, *values)
            return self._convert_decimals(dict(row))

    # ==================== Execute ====================

    async def execute(self, query: str, *args: Any) -> str:
        """
        Execute INSERT/UPDATE/DELETE. Returns status string.
        """
        async with self.get_connection() as conn:
            return await conn.execute(query, *args)

    async def execute_returning(self, query: str, *args: Any) -> Optional[Dict[str, Any]]:
        """
        Execute query with RETURNING clause, return first row.
        """
        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, *args)
            if row:
                return self._convert_decimals(dict(row))
            return None

    async def execute_many(self, query: str, data: List[tuple]) -> None:
        """
        Execute a query for multiple parameter sets.
        """
        async with self.get_connection() as conn:
            await conn.executemany(query, data)


# ==================== Singleton Manager ====================

class DatabaseManager:
    """Singleton database manager."""

    _instance: Optional["DatabaseManager"] = None
    _client: Optional[PostgresClient] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def initialize(self, environment: Optional[str] = None):
        if self._client is None:
            self._client = PostgresClient(environment)
            await self._client.init_pool()

    def get_client(self) -> PostgresClient:
        if self._client is None:
            raise RuntimeError("Database not initialized. Call await init_database() first.")
        return self._client

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None


_db_manager = DatabaseManager()


def get_db() -> PostgresClient:
    """Get the global database client."""
    return _db_manager.get_client()


async def init_database(environment: Optional[str] = None):
    """Initialize the global database connection pool."""
    await _db_manager.initialize(environment)


async def close_database():
    """Close the global database connection pool."""
    await _db_manager.close()
