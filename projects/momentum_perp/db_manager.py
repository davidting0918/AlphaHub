"""
Database Manager

Handles all database operations for the trading system.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, date
from decimal import Decimal
import json

import asyncpg

from .config import db_config, trading_config

logger = logging.getLogger(__name__)


class DBManager:
    """
    Async database manager for trading operations.
    
    Usage:
        db = DBManager()
        await db.connect()
        
        await db.save_order(order_data)
        await db.save_snapshot(balance)
        
        await db.close()
    """
    
    def __init__(self):
        self.connection_string = db_config.url
        self.portfolio_name = trading_config.portfolio_name
        self._pool: Optional[asyncpg.Pool] = None
        
    async def connect(self):
        """Initialize connection pool."""
        if self._pool is not None:
            return
            
        self._pool = await asyncpg.create_pool(
            self.connection_string,
            min_size=1,
            max_size=10,
            command_timeout=60,
        )
        logger.info("Database pool initialized")
    
    async def close(self):
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database pool closed")
    
    @staticmethod
    def _convert_decimals(obj: Any) -> Any:
        """Recursively convert Decimal to float."""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: DBManager._convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [DBManager._convert_decimals(item) for item in obj]
        return obj
    
    # ==================== Snapshots ====================
    
    async def save_snapshot(self, balance: Dict[str, Any]) -> int:
        """Save account balance snapshot."""
        query = """
            INSERT INTO account_snapshots 
            (portfolio_name, total_equity, available_balance, unrealized_pnl, currency, details)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                self.portfolio_name,
                balance.get("total_equity", 0),
                balance.get("available_balance", 0),
                balance.get("unrealized_pnl", 0),
                balance.get("currency", "USDT"),
                json.dumps(balance.get("details", {}))
            )
            return row["id"]
    
    async def get_snapshots(
        self,
        limit: int = 100,
        start_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get recent balance snapshots."""
        query = """
            SELECT * FROM account_snapshots
            WHERE portfolio_name = $1
        """
        params = [self.portfolio_name]
        
        if start_date:
            query += " AND created_at >= $2"
            params.append(start_date)
            
        query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return self._convert_decimals([dict(row) for row in rows])
    
    # ==================== Positions ====================
    
    async def save_position(self, position: Dict[str, Any]) -> int:
        """Save or update position."""
        query = """
            INSERT INTO positions
            (portfolio_name, instrument, side, size, avg_entry_price, current_price,
             unrealized_pnl, leverage, margin_mode, status, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (portfolio_name, instrument) WHERE status = 'open'
            DO UPDATE SET
                size = EXCLUDED.size,
                avg_entry_price = EXCLUDED.avg_entry_price,
                current_price = EXCLUDED.current_price,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                leverage = EXCLUDED.leverage,
                updated_at = NOW()
            RETURNING id
        """
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                self.portfolio_name,
                position.get("instrument"),
                position.get("side"),
                position.get("size"),
                position.get("avg_entry_price"),
                position.get("current_price"),
                position.get("unrealized_pnl", 0),
                position.get("leverage", 1),
                position.get("margin_mode", "cross"),
                position.get("status", "open"),
                json.dumps(position.get("metadata", {}))
            )
            return row["id"]
    
    async def close_position(
        self,
        instrument: str,
        realized_pnl: float = 0
    ) -> Optional[int]:
        """Mark position as closed."""
        query = """
            UPDATE positions
            SET status = 'closed', closed_at = NOW(), unrealized_pnl = $3, updated_at = NOW()
            WHERE portfolio_name = $1 AND instrument = $2 AND status = 'open'
            RETURNING id
        """
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, self.portfolio_name, instrument, realized_pnl)
            return row["id"] if row else None
    
    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions."""
        query = """
            SELECT * FROM positions
            WHERE portfolio_name = $1 AND status = 'open'
            ORDER BY opened_at DESC
        """
        
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, self.portfolio_name)
            return self._convert_decimals([dict(row) for row in rows])
    
    async def sync_positions(self, positions: List[Dict[str, Any]]):
        """Sync positions from exchange to DB."""
        # Get current DB positions
        db_positions = await self.get_open_positions()
        db_instruments = {p["instrument"] for p in db_positions}
        exchange_instruments = {p["instrument"] for p in positions}
        
        # Close positions that are no longer open on exchange
        for inst in db_instruments - exchange_instruments:
            await self.close_position(inst)
        
        # Update/create positions from exchange
        for pos in positions:
            await self.save_position(pos)
    
    # ==================== Orders ====================
    
    async def save_order(self, order: Dict[str, Any]) -> int:
        """Save trading order."""
        query = """
            INSERT INTO trading_orders
            (portfolio_name, strategy_name, instrument, side, order_type, size,
             price, filled_price, filled_size, fee, pnl, order_id, status, signal_data)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id
        """
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                self.portfolio_name,
                order.get("strategy_name", "unknown"),
                order.get("instrument"),
                order.get("side"),
                order.get("order_type", "market"),
                order.get("size"),
                order.get("price"),
                order.get("filled_price"),
                order.get("filled_size"),
                order.get("fee", 0),
                order.get("pnl"),
                order.get("order_id"),
                order.get("status", "pending"),
                json.dumps(order.get("signal_data", {}))
            )
            return row["id"]
    
    async def update_order(self, order_id: str, updates: Dict[str, Any]):
        """Update order status and fill info."""
        set_clauses = []
        params = []
        i = 1
        
        for key in ["status", "filled_price", "filled_size", "fee", "pnl"]:
            if key in updates:
                set_clauses.append(f"{key} = ${i}")
                params.append(updates[key])
                i += 1
        
        if not set_clauses:
            return
            
        set_clauses.append("updated_at = NOW()")
        
        query = f"""
            UPDATE trading_orders
            SET {', '.join(set_clauses)}
            WHERE order_id = ${i}
        """
        params.append(order_id)
        
        async with self._pool.acquire() as conn:
            await conn.execute(query, *params)
    
    async def get_orders(
        self,
        strategy: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get trading orders."""
        query = "SELECT * FROM trading_orders WHERE portfolio_name = $1"
        params = [self.portfolio_name]
        
        if strategy:
            params.append(strategy)
            query += f" AND strategy_name = ${len(params)}"
        
        if status:
            params.append(status)
            query += f" AND status = ${len(params)}"
        
        params.append(limit)
        query += f" ORDER BY created_at DESC LIMIT ${len(params)}"
        
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return self._convert_decimals([dict(row) for row in rows])
    
    async def get_daily_stats(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """Get daily trading statistics."""
        if target_date is None:
            target_date = date.today()
            
        query = """
            SELECT 
                COUNT(*) as trades_count,
                COUNT(*) FILTER (WHERE pnl > 0) as winning_trades,
                SUM(pnl) as total_pnl,
                MAX(pnl) as best_trade,
                MIN(pnl) as worst_trade,
                SUM(fee) as total_fees
            FROM trading_orders
            WHERE portfolio_name = $1 
            AND DATE(created_at) = $2
            AND status = 'filled'
        """
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, self.portfolio_name, target_date)
            result = self._convert_decimals(dict(row))
            
            trades = result["trades_count"] or 0
            wins = result["winning_trades"] or 0
            result["win_rate"] = wins / trades if trades > 0 else 0
            
            return result
    
    # ==================== Signals ====================
    
    async def save_signal(self, signal: Dict[str, Any]) -> int:
        """Save strategy signal."""
        query = """
            INSERT INTO strategy_signals
            (portfolio_name, strategy_name, instrument, signal_type, signal_strength,
             indicators, action_taken, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
        """
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                self.portfolio_name,
                signal.get("strategy_name"),
                signal.get("instrument"),
                signal.get("signal_type"),
                signal.get("signal_strength"),
                json.dumps(signal.get("indicators", {})),
                signal.get("action_taken"),
                signal.get("notes")
            )
            return row["id"]
    
    async def get_signals(
        self,
        strategy: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent signals."""
        query = "SELECT * FROM strategy_signals WHERE portfolio_name = $1"
        params = [self.portfolio_name]
        
        if strategy:
            params.append(strategy)
            query += f" AND strategy_name = ${len(params)}"
        
        params.append(limit)
        query += f" ORDER BY created_at DESC LIMIT ${len(params)}"
        
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return self._convert_decimals([dict(row) for row in rows])


# Helper to run migrations
async def run_migrations():
    """Create tables if they don't exist."""
    migrations = [
        """
        CREATE TABLE IF NOT EXISTS account_snapshots (
            id SERIAL PRIMARY KEY,
            portfolio_name VARCHAR(100) NOT NULL,
            total_equity DECIMAL NOT NULL,
            available_balance DECIMAL NOT NULL,
            unrealized_pnl DECIMAL DEFAULT 0,
            currency VARCHAR(20) DEFAULT 'USDT',
            details JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_snapshots_portfolio ON account_snapshots(portfolio_name, created_at DESC)",
        """
        CREATE TABLE IF NOT EXISTS positions (
            id SERIAL PRIMARY KEY,
            portfolio_name VARCHAR(100) NOT NULL,
            instrument VARCHAR(100) NOT NULL,
            side VARCHAR(10) NOT NULL,
            size DECIMAL NOT NULL,
            avg_entry_price DECIMAL NOT NULL,
            current_price DECIMAL,
            unrealized_pnl DECIMAL DEFAULT 0,
            leverage DECIMAL DEFAULT 1,
            margin_mode VARCHAR(20) DEFAULT 'cross',
            status VARCHAR(20) DEFAULT 'open',
            opened_at TIMESTAMPTZ DEFAULT NOW(),
            closed_at TIMESTAMPTZ,
            metadata JSONB DEFAULT '{}',
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_positions_portfolio ON positions(portfolio_name, status)",
        """
        CREATE TABLE IF NOT EXISTS trading_orders (
            id SERIAL PRIMARY KEY,
            portfolio_name VARCHAR(100) NOT NULL,
            strategy_name VARCHAR(100) NOT NULL,
            instrument VARCHAR(100) NOT NULL,
            side VARCHAR(10) NOT NULL,
            order_type VARCHAR(20) NOT NULL,
            size DECIMAL NOT NULL,
            price DECIMAL,
            filled_price DECIMAL,
            filled_size DECIMAL,
            fee DECIMAL DEFAULT 0,
            pnl DECIMAL,
            order_id VARCHAR(100),
            status VARCHAR(20) DEFAULT 'pending',
            signal_data JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_orders_portfolio ON trading_orders(portfolio_name, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_orders_strategy ON trading_orders(strategy_name, created_at DESC)",
        """
        CREATE TABLE IF NOT EXISTS strategy_signals (
            id SERIAL PRIMARY KEY,
            portfolio_name VARCHAR(100) NOT NULL,
            strategy_name VARCHAR(100) NOT NULL,
            instrument VARCHAR(100) NOT NULL,
            signal_type VARCHAR(20) NOT NULL,
            signal_strength DECIMAL,
            indicators JSONB DEFAULT '{}',
            action_taken VARCHAR(20),
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_signals_portfolio ON strategy_signals(portfolio_name, created_at DESC)",
    ]
    
    conn = await asyncpg.connect(db_config.url)
    try:
        for migration in migrations:
            await conn.execute(migration)
        logger.info("Database migrations completed")
    finally:
        await conn.close()
