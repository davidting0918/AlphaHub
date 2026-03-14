"""
Trading Engine

Main orchestrator for the momentum trading system.
Runs strategies, manages positions, handles execution.
"""

import logging
import asyncio
import signal
from typing import Dict, Any, Optional, List, Type
from datetime import datetime, timedelta

from .config import trading_config
from .okx_trader import OKXTrader
from .risk_manager import RiskManager
from .reporter import TelegramReporter
from .db_manager import DBManager
from .strategies import (
    BaseStrategy, Signal, SignalType,
    BreakoutMomentumStrategy,
    EMACrossRSIStrategy,
    VWAPDeviationStrategy,
    MultiTimeframeTrendStrategy,
    VolumeProfileMomentumStrategy,
    STRATEGIES,
)

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Main trading engine orchestrating all components.
    
    Usage:
        engine = TradingEngine()
        await engine.run()  # Run all strategies
        # or
        await engine.run(strategies=["breakout", "ema_cross"])
    """
    
    # Strategy intervals (seconds) — aggressive for testnet
    STRATEGY_INTERVALS = {
        "breakout_momentum": 300,        # 1H candles -> check every 5 min
        "ema_cross_rsi": 120,            # 15m candles -> check every 2 min
        "vwap_deviation": 60,            # 5m candles -> check every 1 min
        "multi_tf_trend": 120,           # 15m entry TF -> check every 2 min
        "volume_profile": 300,           # 1H candles -> check every 5 min
    }
    
    def __init__(self):
        self.trader = OKXTrader()
        self.risk_manager = RiskManager()
        self.reporter = TelegramReporter()
        self.db = DBManager()
        
        self.strategies: Dict[str, BaseStrategy] = {}
        self.running = False
        self._tasks: List[asyncio.Task] = []
        
        # Last run timestamps
        self._last_runs: Dict[str, datetime] = {}
        
        # Snapshot interval
        self.snapshot_interval = 3600  # 1 hour
        self._last_snapshot = datetime.min
        
    async def initialize(self, strategy_names: Optional[List[str]] = None):
        """Initialize engine components."""
        # Connect to DB
        await self.db.connect()
        
        # Initialize strategies
        if strategy_names is None:
            strategy_names = list(STRATEGIES.keys())
        
        for name in strategy_names:
            if name in STRATEGIES:
                self.strategies[name] = STRATEGIES[name]()
                self._last_runs[name] = datetime.min
                logger.info(f"Loaded strategy: {name}")
            else:
                logger.warning(f"Unknown strategy: {name}")
        
        # Initial state update
        await self._update_state()
        
        logger.info(f"Engine initialized with {len(self.strategies)} strategies")
    
    async def _update_state(self):
        """Update risk manager with current account state."""
        try:
            balance = self.trader.get_balance()
            positions = self.trader.get_positions()
            self.risk_manager.update_state(balance, positions)
            
            # Sync positions to DB
            await self.db.sync_positions(positions)
            
        except Exception as e:
            logger.error(f"Failed to update state: {e}")
    
    async def run(self, strategies: Optional[List[str]] = None):
        """
        Run the trading engine.
        
        Args:
            strategies: List of strategy names to run (None = all)
        """
        await self.initialize(strategies)
        await self.reporter.send_startup_message()
        
        self.running = True
        
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
        
        logger.info("Trading engine starting...")
        
        try:
            while self.running:
                await self._run_cycle()
                await asyncio.sleep(30)  # Main loop interval
                
        except asyncio.CancelledError:
            logger.info("Engine cancelled")
        except Exception as e:
            logger.error(f"Engine error: {e}")
            await self.reporter.send_error("Engine Error", str(e))
        finally:
            await self.shutdown()
    
    async def _run_cycle(self):
        """Run one cycle of strategy checks."""
        now = datetime.utcnow()
        
        # Update account state
        await self._update_state()
        
        # Check if we can trade
        can_trade, reason = self.risk_manager.can_trade()
        if not can_trade:
            logger.info(f"Trading paused: {reason}")
            return
        
        # Run each strategy if interval has passed
        for name, strategy in self.strategies.items():
            interval = self.STRATEGY_INTERVALS.get(name, 3600)
            last_run = self._last_runs.get(name, datetime.min)
            
            if (now - last_run).total_seconds() >= interval:
                await self._run_strategy(name, strategy)
                self._last_runs[name] = now
        
        # Periodic snapshot
        if (now - self._last_snapshot).total_seconds() >= self.snapshot_interval:
            await self._take_snapshot()
            self._last_snapshot = now
    
    async def _run_strategy(self, name: str, strategy: BaseStrategy):
        """Run a single strategy."""
        logger.info(f"Scanning: {name} ({strategy.timeframe})")
        
        signals_found = []
        for instrument in trading_config.instruments:
            try:
                # Get klines
                klines = self.trader.get_klines(
                    instrument,
                    bar=strategy.timeframe,
                    limit=100
                )
                
                # Add instrument to each candle
                for k in klines:
                    k["instrument"] = instrument
                
                # Get secondary timeframe if needed
                secondary_klines = None
                if strategy.secondary_timeframe:
                    secondary_klines = self.trader.get_klines(
                        instrument,
                        bar=strategy.secondary_timeframe,
                        limit=250
                    )
                    for k in secondary_klines:
                        k["instrument"] = instrument
                
                # Get current position
                current_position = self.trader.get_position(instrument)
                
                # Analyze
                signal = strategy.analyze(
                    klines,
                    current_position=current_position,
                    secondary_klines=secondary_klines
                )
                
                # Process signal
                if signal.signal_type != SignalType.NO_ACTION:
                    signals_found.append(f"{instrument}={signal.signal_type.value}({signal.strength:.2f})")
                    await self._process_signal(name, instrument, signal, current_position)
                    
            except Exception as e:
                logger.error(f"Error in {name} for {instrument}: {e}")
        
        if signals_found:
            logger.info(f"  → Signals: {', '.join(signals_found)}")
        else:
            logger.info(f"  → No signals")
    
    async def _process_signal(
        self,
        strategy_name: str,
        instrument: str,
        signal: Signal,
        current_position: Optional[Dict[str, Any]]
    ):
        """Process a trading signal."""
        logger.info(f"Signal: {strategy_name} {instrument} {signal.signal_type.value} (str={signal.strength:.2f})")
        
        # Save signal to DB
        await self.db.save_signal({
            "strategy_name": strategy_name,
            "instrument": instrument,
            "signal_type": signal.signal_type.value,
            "signal_strength": signal.strength,
            "indicators": signal.indicators,
            "notes": signal.notes,
        })
        
        # Alert via Telegram
        try:
            await self.reporter.send_signal_alert(
                instrument=instrument,
                signal_type=signal.signal_type.value,
                strategy=strategy_name,
                strength=signal.strength,
                notes=signal.notes,
            )
        except Exception as e:
            logger.error(f"Failed to send signal alert: {e}")
        
        # Entry signals
        if signal.is_entry:
            await self._handle_entry(strategy_name, instrument, signal)
        
        # Exit signals
        elif signal.is_exit and current_position:
            await self._handle_exit(strategy_name, instrument, signal, current_position)
    
    async def _handle_entry(
        self,
        strategy_name: str,
        instrument: str,
        signal: Signal
    ):
        """Handle entry signal."""
        # Get instrument info for contract sizing
        try:
            inst_info = self.trader.get_instrument_info(instrument)
            contract_value = inst_info.get("contract_value", 1)
            min_size = inst_info.get("min_size", 1)
        except:
            contract_value = 1
            min_size = 1
        
        # Calculate position size
        entry_price = signal.entry_price or self.trader.get_ticker(instrument).get("last_price", 0)
        
        size = self.risk_manager.calculate_position_size(
            instrument=instrument,
            entry_price=entry_price,
            stop_loss=signal.stop_loss,
            signal_size_pct=signal.size_pct,
            contract_value=contract_value
        )
        
        if size < min_size:
            logger.info(f"Position size {size} below minimum {min_size}, skipping")
            await self.db.save_signal({
                "strategy_name": strategy_name,
                "instrument": instrument,
                "signal_type": signal.signal_type.value,
                "signal_strength": signal.strength,
                "action_taken": "skipped",
                "notes": f"Size {size} below min {min_size}"
            })
            return
        
        # Validate order
        side = "buy" if signal.signal_type == SignalType.ENTRY_LONG else "sell"
        valid, reason = self.risk_manager.validate_order(
            instrument, side, size, entry_price, contract_value
        )
        
        if not valid:
            logger.info(f"Order validation failed: {reason}")
            await self.db.save_signal({
                "strategy_name": strategy_name,
                "instrument": instrument,
                "signal_type": signal.signal_type.value,
                "signal_strength": signal.strength,
                "action_taken": "filtered",
                "notes": reason
            })
            return
        
        # Place order
        try:
            # Set leverage (skip for PM accounts which manage leverage differently)
            try:
                self.trader.set_leverage(instrument, trading_config.default_leverage)
            except Exception as lev_err:
                logger.warning(f"Leverage set skipped ({lev_err}), proceeding with default")
            
            # Place market order
            order_result = self.trader.place_market_order(
                inst_id=instrument,
                side=side,
                size=size
            )
            
            # Save order to DB
            order_data = {
                "strategy_name": strategy_name,
                "instrument": instrument,
                "side": side,
                "order_type": "market",
                "size": size,
                "price": entry_price,
                "order_id": order_result.get("order_id"),
                "status": "submitted",
                "signal_data": signal.indicators,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
            }
            await self.db.save_order(order_data)
            
            # Update signal action
            await self.db.save_signal({
                "strategy_name": strategy_name,
                "instrument": instrument,
                "signal_type": signal.signal_type.value,
                "signal_strength": signal.strength,
                "action_taken": "executed",
                "notes": signal.notes,
                "indicators": signal.indicators,
            })
            
            # Send notification
            order_data["filled_price"] = entry_price
            order_data["status"] = "filled"
            await self.reporter.send_trade_notification(order_data)
            
            logger.info(f"Order placed: {side} {size} {instrument}")
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            await self.reporter.send_error("Order Failed", str(e))
    
    async def _handle_exit(
        self,
        strategy_name: str,
        instrument: str,
        signal: Signal,
        position: Dict[str, Any]
    ):
        """Handle exit signal."""
        try:
            # Close position
            result = self.trader.close_position(instrument)
            
            # Calculate realized PnL
            realized_pnl = position.get("unrealized_pnl", 0)
            
            # Update DB
            await self.db.close_position(instrument, realized_pnl)
            
            # Save order
            side = "sell" if position.get("side") == "long" else "buy"
            await self.db.save_order({
                "strategy_name": strategy_name,
                "instrument": instrument,
                "side": side,
                "order_type": "market",
                "size": position.get("size", 0),
                "pnl": realized_pnl,
                "status": "filled",
                "signal_data": signal.indicators,
            })
            
            # Update signal
            await self.db.save_signal({
                "strategy_name": strategy_name,
                "instrument": instrument,
                "signal_type": signal.signal_type.value,
                "signal_strength": signal.strength,
                "action_taken": "executed",
                "notes": signal.notes,
                "indicators": signal.indicators,
            })
            
            # Send notification
            position["realized_pnl"] = realized_pnl
            await self.reporter.send_position_update(position, action="closed")
            
            logger.info(f"Position closed: {instrument} PnL={realized_pnl:.2f}")
            
        except Exception as e:
            logger.error(f"Failed to close position: {e}")
            await self.reporter.send_error("Close Position Failed", str(e))
    
    async def _take_snapshot(self):
        """Take balance snapshot."""
        try:
            balance = self.trader.get_balance()
            await self.db.save_snapshot(balance)
            await self.reporter.send_balance_snapshot(balance)
            logger.debug("Snapshot saved")
        except Exception as e:
            logger.error(f"Failed to save snapshot: {e}")
    
    async def stop(self):
        """Stop the engine gracefully."""
        logger.info("Stopping engine...")
        self.running = False
    
    async def shutdown(self):
        """Clean shutdown."""
        await self.db.close()
        await self.reporter.send_shutdown_message("Graceful shutdown")
        logger.info("Engine shutdown complete")
    
    # ==================== Manual Commands ====================
    
    async def send_report(self):
        """Send current PnL report."""
        await self.db.connect()
        balance = self.trader.get_balance()
        positions = self.trader.get_positions()
        daily_stats = await self.db.get_daily_stats()
        
        stats = {
            "total_equity": balance.get("total_equity", 0),
            "daily_pnl": daily_stats.get("total_pnl", 0) or 0,
            "daily_pnl_pct": (daily_stats.get("total_pnl", 0) or 0) / balance.get("total_equity", 1) * 100,
            "trades_count": daily_stats.get("trades_count", 0),
            "win_rate": daily_stats.get("win_rate", 0),
            "best_trade": daily_stats.get("best_trade", 0) or 0,
            "worst_trade": daily_stats.get("worst_trade", 0) or 0,
            "open_positions": len(positions),
        }
        
        await self.reporter.send_daily_summary(stats)
        await self.db.close()
        return stats
    
    async def take_snapshot(self):
        """Manually take a balance snapshot."""
        await self.initialize([])
        balance = self.trader.get_balance()
        await self.db.save_snapshot(balance)
        await self.reporter.send_balance_snapshot(balance)
        await self.db.close()
        return balance
    
    async def run_once(self, strategy_name: str = None):
        """Run strategies once and exit."""
        strategies = [strategy_name] if strategy_name else None
        await self.initialize(strategies)
        
        await self._update_state()
        
        can_trade, reason = self.risk_manager.can_trade()
        if not can_trade:
            logger.info(f"Cannot trade: {reason}")
            return
        
        for name, strategy in self.strategies.items():
            await self._run_strategy(name, strategy)
        
        await self.db.close()
