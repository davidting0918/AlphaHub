#!/usr/bin/env python3
"""
Momentum Perp Trading System - Entry Point

Usage:
    python run.py                        # Run all strategies
    python run.py --strategy breakout    # Run single strategy
    python run.py --report               # Send current PnL report
    python run.py --snapshot             # Take balance snapshot
    python run.py --migrate              # Run database migrations
    python run.py --test                 # Run tests (OKX, Telegram, DB)
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path

# Add parent path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from projects.momentum_perp.engine import TradingEngine
from projects.momentum_perp.db_manager import run_migrations
from projects.momentum_perp.okx_trader import OKXTrader
from projects.momentum_perp.reporter import send_test_message
from projects.momentum_perp.config import trading_config
from projects.momentum_perp.bot_commands import run_bot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("momentum_perp.log"),
    ]
)
# Add console only if running interactively
if sys.stdout.isatty():
    logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)


async def run_engine(strategies=None):
    """Run the trading engine."""
    engine = TradingEngine()
    await engine.run(strategies)


async def run_full():
    """Run engine + bot commands concurrently."""
    from projects.momentum_perp.bot_commands import BotCommandHandler
    
    engine = TradingEngine()
    bot = BotCommandHandler()
    
    await asyncio.gather(
        engine.run(),
        bot.run(),
    )


async def send_report():
    """Send PnL report."""
    engine = TradingEngine()
    stats = await engine.send_report()
    print(f"Report sent: {stats}")


async def take_snapshot():
    """Take balance snapshot."""
    engine = TradingEngine()
    balance = await engine.take_snapshot()
    print(f"Snapshot taken: {balance}")


async def run_once(strategy=None):
    """Run strategies once."""
    engine = TradingEngine()
    await engine.run_once(strategy)
    print("Run complete")


async def migrate():
    """Run database migrations."""
    await run_migrations()
    print("Migrations complete")


def test_okx():
    """Test OKX connection."""
    print("Testing OKX connection...")
    trader = OKXTrader()
    
    try:
        # Get balance
        balance = trader.get_balance()
        print(f"✅ Balance: {balance['total_equity']:.2f} USDT")
        print(f"   Available: {balance['available_balance']:.2f} USDT")
        
        # Get positions
        positions = trader.get_positions()
        print(f"✅ Open positions: {len(positions)}")
        
        # Get ticker
        ticker = trader.get_ticker("BTC-USDT-SWAP")
        print(f"✅ BTC price: {ticker['last_price']:.2f}")
        
        # Get klines
        klines = trader.get_klines("BTC-USDT-SWAP", "1H", 10)
        print(f"✅ Got {len(klines)} candles")
        
        print("\n✅ OKX connection OK")
        return True
        
    except Exception as e:
        print(f"❌ OKX error: {e}")
        return False


def test_telegram():
    """Test Telegram connection."""
    print("Testing Telegram...")
    
    try:
        result = send_test_message(
            f"🧪 <b>Test Message</b>\n\n"
            f"Portfolio: <code>{trading_config.portfolio_name}</code>\n"
            f"Status: Connection OK ✅"
        )
        
        if result:
            print("✅ Telegram message sent")
            return True
        else:
            print("❌ Telegram send failed")
            return False
            
    except Exception as e:
        print(f"❌ Telegram error: {e}")
        return False


async def test_db():
    """Test database connection."""
    print("Testing database...")
    
    try:
        from projects.momentum_perp.db_manager import DBManager
        db = DBManager()
        await db.connect()
        
        # Test snapshot
        snapshot_id = await db.save_snapshot({
            "total_equity": 29490.0,
            "available_balance": 29490.0,
            "unrealized_pnl": 0.0,
        })
        print(f"✅ Snapshot saved (id={snapshot_id})")
        
        # Read back
        snapshots = await db.get_snapshots(limit=1)
        if snapshots:
            print(f"✅ Snapshot read: equity={snapshots[0]['total_equity']}")
        
        await db.close()
        print("✅ Database connection OK")
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False


async def run_tests():
    """Run all tests."""
    print("=" * 50)
    print("MOMENTUM PERP TRADING SYSTEM - TESTS")
    print("=" * 50)
    print()
    
    results = []
    
    # Test OKX
    results.append(("OKX", test_okx()))
    print()
    
    # Test Telegram
    results.append(("Telegram", test_telegram()))
    print()
    
    # Test DB
    results.append(("Database", await test_db()))
    print()
    
    # Summary
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name}: {status}")
    
    all_passed = all(r[1] for r in results)
    print()
    print("All tests passed!" if all_passed else "Some tests failed!")
    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Momentum Perp Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--strategy", "-s",
        type=str,
        help="Run specific strategy (breakout, ema_cross, vwap, multi_tf, volume)"
    )
    parser.add_argument(
        "--report", "-r",
        action="store_true",
        help="Send current PnL report"
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Take balance snapshot"
    )
    parser.add_argument(
        "--migrate", "-m",
        action="store_true",
        help="Run database migrations"
    )
    parser.add_argument(
        "--test", "-t",
        action="store_true",
        help="Run connection tests"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run strategies once and exit"
    )
    parser.add_argument(
        "--bot",
        action="store_true",
        help="Run Telegram bot command handler"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run engine + bot commands together"
    )
    
    args = parser.parse_args()
    
    try:
        if args.bot:
            asyncio.run(run_bot())
        elif args.full:
            asyncio.run(run_full())
        elif args.test:
            asyncio.run(run_tests())
        elif args.migrate:
            asyncio.run(migrate())
        elif args.report:
            asyncio.run(send_report())
        elif args.snapshot:
            asyncio.run(take_snapshot())
        elif args.once:
            asyncio.run(run_once(args.strategy))
        else:
            # Run engine
            strategies = [args.strategy] if args.strategy else None
            asyncio.run(run_engine(strategies))
            
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
