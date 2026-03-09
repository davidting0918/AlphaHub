"""
Stability Monitor - Batch Monitoring Coordinator

Orchestrates batch data fetching and stability analysis for multiple symbols.
"""

import asyncio
import time
from typing import List, Dict, Any
import logging

from adaptor.binance import AsyncBinanceAlpha
from .core import AlphaStabilityIndicator
from .models import MonitorBatchResult, StabilityResult, Signal

logger = logging.getLogger(__name__)


class StabilityMonitor:
    """
    Batch stability monitor for multiple trading pairs
    
    Efficiently fetches market data for multiple symbols concurrently
    and performs stability analysis on each.
    """
    
    def __init__(self, async_binance_alpha: AsyncBinanceAlpha):
        """
        Initialize monitor with async Binance client
        
        Args:
            async_binance_alpha: Initialized AsyncBinanceAlpha instance
        """
        self.alpha = async_binance_alpha
    
    async def monitor_batch(self, symbols: List[Dict[str, Any]]) -> MonitorBatchResult:
        """
        Monitor multiple symbols concurrently
        
        Args:
            symbols: List of trading pair symbols (e.g., ["BTCUSDT", "ETHUSDT"])
            
        Returns:
            MonitorBatchResult containing all analysis results
        """
        start_time = time.time()
        timestamp = int(start_time * 1000)
        
        logger.info(f"Starting batch monitoring for {len(symbols)} symbols...")
        
        # Fetch all market data concurrently
        try:
            klines_1m_batch, klines_15s_batch, agg_trades_batch = await asyncio.gather(
                self.alpha.get_klines([i['alpha_id'] for i in symbols], "1m", 15),
                self.alpha.get_klines([i['alpha_id'] for i in symbols], "15s", 20),
                self.alpha.get_agg_trades([i['alpha_id'] for i in symbols], 500)
            )
        except Exception as e:
            logger.error(f"Failed to fetch market data: {str(e)}")
            return MonitorBatchResult(
                timestamp=timestamp,
                total_symbols=len(symbols),
                successful=0,
                failed=len(symbols),
                results=[]
            )
        
        # Process each symbol
        results = []
        for symbol_info in symbols:
            result = await self._analyze_symbol(
                symbol_info['symbol'],
                symbol_info['alpha_id'],
                klines_1m_batch[symbol_info['alpha_id']],
                klines_15s_batch[symbol_info['alpha_id']],
                agg_trades_batch[symbol_info['alpha_id']]
            )
            results.append(result)
        
        # Calculate summary statistics
        successful = sum(1 for r in results if r.error is None)
        failed = len(results) - successful
        
        signal_counts = {
            "green_signals": sum(1 for r in results if r.signal == Signal.GREEN and r.error is None),
            "yellow_signals": sum(1 for r in results if r.signal == Signal.YELLOW and r.error is None),
            "red_signals": sum(1 for r in results if r.signal == Signal.RED and r.error is None)
        }
        
        elapsed = time.time() - start_time
        logger.info(f"Batch monitoring complete: {successful}/{len(symbols)} successful ({elapsed:.2f}s)")
        
        return MonitorBatchResult(
            timestamp=timestamp,
            total_symbols=len(symbols),
            successful=successful,
            failed=failed,
            results=results,
            summary=signal_counts
        )
    
    async def _analyze_symbol(
        self,
        symbol: str,
        alpha_id: str,
        klines_1m: List[Dict[str, Any]],
        klines_15s: List[Dict[str, Any]],
        agg_trades: List[Dict[str, Any]]
    ) -> StabilityResult:
        """
        Analyze a single symbol
        
        Args:
            symbol: Trading pair symbol
            alpha_id: Alpha ID
            klines_1m: 1-minute klines data
            klines_15s: 15-second klines data
            agg_trades: Aggregated trades data
            
        Returns:
            StabilityResult for this symbol
        """
        try:
            # Validate data
            if not klines_1m or not klines_15s or not agg_trades:
                raise ValueError("Incomplete market data")
            
            # Create indicator and analyze
            indicator = AlphaStabilityIndicator(
                symbol=symbol,
                alpha_id=alpha_id,
                klines_1m=klines_1m,
                klines_15s=klines_15s,
                agg_trades=agg_trades
            )
            
            result = indicator.analyze()
            return result
            
        except Exception as e:
            logger.error(f"Failed to analyze {symbol}: {str(e)}")
            return StabilityResult(
                symbol=symbol,
                alpha_id=alpha_id,
                timestamp=int(time.time() * 1000),
                signal=Signal.RED,
                composite_score=0.0,
                metrics={},
                recommendation="data insufficient or calculation error",
                error=str(e)
            )

