"""
Monitoring Service - Integrates Alpha Stability Monitor Logic

Handles token selection, data fetching, and stability analysis
in a reusable service component.
"""

import asyncio
import logging
import pandas as pd
from datetime import datetime as dt
from typing import Optional, Dict, Any, List

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adaptor.binance import AsyncBinanceAlpha
from bn_alpha_monitor import StabilityMonitor

logger = logging.getLogger(__name__)


class MonitoringService:
    """
    Background service that continuously monitors alpha tokens
    
    Integrates the logic from test_monitor.py into a reusable service
    that can be called periodically by the WebSocket server.
    """
    
    def __init__(
        self,
        min_multiplier: float = 1.0,
        min_volume: float = 0.0,
        top_n_tokens: Optional[int] = None
    ):
        """
        Initialize monitoring service
        
        Args:
            min_multiplier: Minimum multiplier for token filtering (default: 1.0)
            min_volume: Minimum 24h volume for token filtering (default: 0.0)
            top_n_tokens: Limit to top N tokens by volume (default: None = all)
        """
        self.min_multiplier = min_multiplier
        self.min_volume = min_volume
        self.top_n_tokens = top_n_tokens
        
        self.alpha_client: Optional[AsyncBinanceAlpha] = None
        self.monitor: Optional[StabilityMonitor] = None
        self.is_running = False
        
        # Cache for token list (to avoid fetching on every cycle)
        self.cached_symbols: Optional[List[Dict[str, str]]] = None
        self.last_token_fetch: Optional[float] = None
        self.token_cache_duration = 300  # Refresh token list every 5 minutes
    
    async def start(self):
        """
        Initialize the monitoring service
        """
        logger.info("🔧 Starting monitoring service...")
        
        # Initialize Binance Alpha client
        self.alpha_client = AsyncBinanceAlpha()
        await self.alpha_client.__aenter__()
        
        # Initialize stability monitor
        self.monitor = StabilityMonitor(self.alpha_client)
        
        # Fetch initial token list
        await self._refresh_token_list()
        
        self.is_running = True
        logger.info("✓ Monitoring service ready")
    
    async def stop(self):
        """
        Gracefully shutdown the monitoring service
        """
        self.is_running = False
        
        if self.alpha_client:
            try:
                await self.alpha_client.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error closing alpha client: {e}")
        
        logger.info("✓ Monitoring service stopped")
    
    async def _refresh_token_list(self):
        """
        Fetch and cache token list from Binance
        """
        logger.info("📥 Fetching token list from Binance...")
        start_time = dt.now()
        
        try:
            token_list = await self.alpha_client.get_token_list()
            token_df = pd.DataFrame(token_list)
            elapsed = (dt.now() - start_time).total_seconds()
            
            logger.info(f"✓ Found {len(token_df)} tokens ({elapsed:.2f}s)")
            
            # Filter tokens based on criteria
            filtered_tokens = token_df.query(
                f"multiplier > {self.min_multiplier} and volume_24h > {self.min_volume}"
            )
            filtered_tokens = filtered_tokens.sort_values(by="volume_24h", ascending=False)
            
            # Limit to top N if specified
            if self.top_n_tokens:
                filtered_tokens = filtered_tokens.head(self.top_n_tokens)
            
            # Convert to symbol list
            self.cached_symbols = [
                {
                    "symbol": row['symbol'],
                    "alpha_id": f"{row['alpha_id']}USDT"
                }
                for _, row in filtered_tokens.iterrows()
            ]
            
            self.last_token_fetch = dt.now().timestamp()
            
            logger.info(f"✓ Selected {len(self.cached_symbols)} tokens for monitoring")
            
            # Log top 5 tokens
            for i, (_, row) in enumerate(filtered_tokens.head(5).iterrows(), 1):
                logger.info(
                    f"   {i}. {row['alpha_id']:10s} - "
                    f"Volume: ${row['volume_24h']:,.0f} | "
                    f"Multiplier: {row['multiplier']:.1f}x"
                )
            
            if len(filtered_tokens) > 5:
                logger.info(f"   ... and {len(filtered_tokens) - 5} more tokens")
        
        except Exception as e:
            logger.error(f"❌ Failed to fetch token list: {e}")
            # Keep using cached symbols if available
            if not self.cached_symbols:
                raise
    
    def _should_refresh_tokens(self) -> bool:
        """
        Check if token list should be refreshed
        
        Returns:
            True if cache is stale or missing
        """
        if not self.cached_symbols or not self.last_token_fetch:
            return True
        
        time_since_fetch = dt.now().timestamp() - self.last_token_fetch
        return time_since_fetch > self.token_cache_duration
    
    async def get_monitoring_results(self) -> Optional[Dict[str, Any]]:
        """
        Perform monitoring analysis and return results
        
        This is the main method called by the WebSocket server to get
        the latest monitoring data.
        
        Returns:
            Dictionary containing batch monitoring results, or None on failure
        """
        if not self.is_running:
            logger.warning("⚠ Monitoring service not running")
            return None
        
        try:
            # Refresh token list if needed
            if self._should_refresh_tokens():
                logger.info("🔄 Token cache expired, refreshing...")
                await self._refresh_token_list()
            
            if not self.cached_symbols:
                logger.error("❌ No symbols available for monitoring")
                return None
            
            # Perform batch monitoring
            logger.info(f"🔍 Monitoring {len(self.cached_symbols)} symbols...")
            monitor_start = dt.now()
            
            batch_result = await self.monitor.monitor_batch(self.cached_symbols)
            
            monitor_elapsed = (dt.now() - monitor_start).total_seconds()
            logger.info(
                f"✓ Monitoring complete ({monitor_elapsed:.2f}s) | "
                f"Success: {batch_result.successful}/{batch_result.total_symbols}"
            )
            
            # Convert to dictionary
            result_dict = batch_result.to_dict()
            
            return result_dict
        
        except Exception as e:
            logger.error(f"❌ Error during monitoring: {e}", exc_info=True)
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current service status
        
        Returns:
            Dictionary containing service status information
        """
        return {
            "is_running": self.is_running,
            "has_cached_symbols": self.cached_symbols is not None,
            "cached_symbol_count": len(self.cached_symbols) if self.cached_symbols else 0,
            "last_token_fetch": self.last_token_fetch,
            "config": {
                "min_multiplier": self.min_multiplier,
                "min_volume": self.min_volume,
                "top_n_tokens": self.top_n_tokens,
                "token_cache_duration": self.token_cache_duration
            }
        }

