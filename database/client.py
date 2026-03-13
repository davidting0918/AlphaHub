"""
PostgreSQL Database Client

Sync client for CRUD operations on PostgreSQL.
Follows the same pattern as VegaExchange's PostgresAsyncClient but synchronous.

Features:
- Connection pooling via psycopg2 (thread-safe with pool)
- Simple CRUD methods: read, read_one, insert_one, insert, execute
- Upsert support
- Environment-based configuration
- Decimal → float auto-conversion
"""

import os
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)


class PostgresClient:
    """
    Sync PostgreSQL client with connection pooling.

    Usage:
        client = PostgresClient()
        client.init_pool()

        rows = client.read("SELECT * FROM instruments WHERE type = %s", "PERP")
        row = client.read_one("SELECT * FROM instruments WHERE instrument_id = %s", "OKX_PERP_BTC_USDT")

        client.insert_one("exchanges", {"name": "OKX"})
        client.insert("funding_rates", [{"instrument_id": "...", "funding_rate": 0.001, ...}])

        client.execute("UPDATE instruments SET is_active = %s WHERE id = %s", False, 123)

        client.close()
    """

    def __init__(self, environment: Optional[str] = None):
        """
        Initialize PostgreSQL client with environment support.

        Args:
            environment: Environment name (test, staging, prod).
                        If None, auto-detect from APP_ENV.
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
                f"No database URL found for environment '{environment}'. "
                f"Set DATABASE_URL (or DATABASE_URL_TEST/DATABASE_URL_STAGING)."
            )

        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

    def init_pool(self, min_conn: int = 1, max_conn: int = 10):
        """Initialize the connection pool."""
        if self._pool is not None:
            return

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dsn=self.connection_string,
        )
        logger.info(f"Database pool initialized ({self.environment}, min={min_conn}, max={max_conn})")

    def close(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("Database pool closed")

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool (auto-initializes if needed)."""
        if self._pool is None:
            self.init_pool()

        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    @contextmanager
    def get_cursor(self, commit: bool = True):
        """Get a cursor with auto-commit/rollback."""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

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

    def read(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query, return results as list of dicts.

        Args:
            query: SQL query with %s placeholders
            *args: Parameters

        Returns:
            List of dicts with Decimal values converted to float
        """
        with self.get_cursor(commit=False) as cursor:
            cursor.execute(query, args if args else None)
            rows = cursor.fetchall()
            return self._convert_decimals([dict(row) for row in rows])

    def read_one(self, query: str, *args: Any) -> Optional[Dict[str, Any]]:
        """
        Execute a SELECT query, return first result as dict (or None).

        Args:
            query: SQL query with %s placeholders
            *args: Parameters

        Returns:
            Dict or None, with Decimal values converted to float
        """
        with self.get_cursor(commit=False) as cursor:
            cursor.execute(query, args if args else None)
            row = cursor.fetchone()
            if row:
                return self._convert_decimals(dict(row))
            return None

    # ==================== Insert ====================

    def insert_one(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a single record into a table.

        Args:
            table: Table name
            data: Dict of column → value

        Returns:
            The inserted record (via RETURNING *)
        """
        columns = list(data.keys())
        placeholders = ["%s"] * len(columns)
        values = list(data.values())

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING *
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, values)
            result = cursor.fetchone()
            return self._convert_decimals(dict(result))

    def insert(self, table: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert multiple records into a table.

        Args:
            table: Table name
            data: List of dicts (all must have same keys)

        Returns:
            List of inserted records (via RETURNING *)
        """
        if not data:
            return []

        columns = list(data[0].keys())
        placeholders = ["%s"] * len(columns)
        template = f"({', '.join(placeholders)})"

        values_list = []
        for record in data:
            if set(record.keys()) != set(columns):
                raise ValueError(
                    f"All records must have the same columns. "
                    f"Expected: {columns}, Got: {list(record.keys())}"
                )
            values_list.append(tuple(record[col] for col in columns))

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES {', '.join([template] * len(values_list))}
            RETURNING *
        """

        # Flatten values
        flat_values = [v for row in values_list for v in row]

        with self.get_cursor() as cursor:
            cursor.execute(query, flat_values)
            results = cursor.fetchall()
            return self._convert_decimals([dict(row) for row in results])

    def upsert_one(
        self,
        table: str,
        data: Dict[str, Any],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Insert or update a single record.

        Args:
            table: Table name
            data: Dict of column → value
            conflict_columns: Columns for ON CONFLICT clause
            update_columns: Columns to update on conflict (default: all non-conflict columns)

        Returns:
            The upserted record
        """
        columns = list(data.keys())
        placeholders = ["%s"] * len(columns)
        values = list(data.values())

        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        conflict_clause = ", ".join(conflict_columns)
        update_clause = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in update_columns
        )

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({conflict_clause}) DO UPDATE SET
                {update_clause}
            RETURNING *
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, values)
            result = cursor.fetchone()
            return self._convert_decimals(dict(result))

    # ==================== Execute ====================

    def execute(self, query: str, *args: Any) -> str:
        """
        Execute INSERT/UPDATE/DELETE query.

        Args:
            query: SQL query with %s placeholders
            *args: Parameters

        Returns:
            Status message (e.g., "UPDATE 1")
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, args if args else None)
            return cursor.statusmessage

    def execute_returning(self, query: str, *args: Any) -> Optional[Dict[str, Any]]:
        """
        Execute query with RETURNING clause, return first row.

        Args:
            query: SQL query with RETURNING clause
            *args: Parameters

        Returns:
            Dict or None
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, args if args else None)
            row = cursor.fetchone()
            if row:
                return self._convert_decimals(dict(row))
            return None

    def execute_batch(self, query: str, data: List[tuple]) -> None:
        """
        Execute a query for multiple parameter sets (batch).

        Args:
            query: SQL query with %s placeholders
            data: List of tuples, each is one set of parameters
        """
        with self.get_cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, data, page_size=200)


# ==================== Singleton Manager ====================

class DatabaseManager:
    """Singleton database manager (mirrors VegaExchange pattern)."""

    _instance: Optional["DatabaseManager"] = None
    _client: Optional[PostgresClient] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initialize(self, environment: Optional[str] = None):
        """Initialize the database client and pool."""
        if self._client is None:
            self._client = PostgresClient(environment)
            self._client.init_pool()

    def get_client(self) -> PostgresClient:
        """Get the database client (raises if not initialized)."""
        if self._client is None:
            raise RuntimeError("Database not initialized. Call init_database() first.")
        return self._client

    def close(self):
        """Close the database client."""
        if self._client:
            self._client.close()
            self._client = None


# Module-level convenience functions
_db_manager = DatabaseManager()


def get_db() -> PostgresClient:
    """Get the global database client."""
    return _db_manager.get_client()


def init_database(environment: Optional[str] = None):
    """Initialize the global database connection pool."""
    _db_manager.initialize(environment)


def close_database():
    """Close the global database connection pool."""
    _db_manager.close()
