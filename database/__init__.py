"""
Database Package

Provides PostgreSQL client for CRUD operations.

Usage:
    from database import get_db, init_database, close_database

    # Initialize (call once at startup)
    init_database()

    # Use the client
    db = get_db()
    rows = db.read("SELECT * FROM instruments WHERE type = %s", "PERP")
    db.insert_one("exchanges", {"name": "OKX"})
"""

from .client import PostgresClient, DatabaseManager, get_db, init_database, close_database

__all__ = [
    'PostgresClient',
    'DatabaseManager',
    'get_db',
    'init_database',
    'close_database',
]
