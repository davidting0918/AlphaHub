"""
Database Package — Async PostgreSQL client (asyncpg).

Usage:
    from database import get_db, init_database, close_database

    await init_database()
    db = get_db()

    rows = await db.read("SELECT * FROM instruments WHERE type = $1", "PERP")
    await db.insert_one("exchanges", {"name": "OKX"})

    await close_database()
"""

from .client import PostgresClient, DatabaseManager, get_db, init_database, close_database

__all__ = [
    'PostgresClient',
    'DatabaseManager',
    'get_db',
    'init_database',
    'close_database',
]
