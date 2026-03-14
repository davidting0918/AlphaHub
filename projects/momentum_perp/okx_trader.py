"""
OKX Authenticated Trading Client

Handles order placement, position management, and account operations
using the python-okx SDK with demo/testnet support.
"""

import logging
from typing import Optional, Dict, Any, List
from decimal import Decimal

import okx.Account as Account
import okx.Trade as Trade
import okx.PublicData as PublicData
import okx.MarketData as Market

from .config import okx_config, trading_config

logger = logging.getLogger(__name__)


class OKXTrader:
    """
    Authenticated OKX trading client for perpetual futures.
    
    Usage:
        trader = OKXTrader()
        balance = trader.get_balance()
        positions = trader.get_positions()
        order = trader.place_market_order("BTC-USDT-SWAP", "buy", 0.01)
    """
    
    def __init__(self):
        self.api_key = okx_config.api_key
        self.secret_key = okx_config.secret_key
        self.passphrase = okx_config.passphrase
        self.flag = okx_config.demo_flag  # "1" for demo, "0" for live
        
        # Initialize API clients
        self.account_api = Account.AccountAPI(
            self.api_key, self.secret_key, self.passphrase,
            False, self.flag
        )
        self.trade_api = Trade.TradeAPI(
            self.api_key, self.secret_key, self.passphrase,
            False, self.flag
        )
        self.public_api = PublicData.PublicAPI(flag=self.flag)
        self.market_api = Market.MarketAPI(flag=self.flag)
        
        self.portfolio_name = trading_config.portfolio_name
        
    def _check_response(self, result: Dict) -> Dict:
        """Check API response and raise on error."""
        if result.get("code") != "0":
            error_msg = result.get("msg", "Unknown error")
            logger.error(f"OKX API Error: {error_msg}")
            raise Exception(f"OKX API Error: {error_msg}")
        return result
    
    # ==================== Account ====================
    
    def get_balance(self, currency: str = "USDT") -> Dict[str, Any]:
        """
        Get account balance.
        
        Returns:
            Dict with total_equity, available_balance, unrealized_pnl
        """
        result = self.account_api.get_account_balance(ccy=currency)
        self._check_response(result)
        
        data = result.get("data", [{}])[0]
        details = data.get("details", [{}])
        
        # Find USDT balance
        usdt_detail = next(
            (d for d in details if d.get("ccy") == currency),
            {}
        )
        
        return {
            "total_equity": float(data.get("totalEq", 0)),
            "available_balance": float(usdt_detail.get("availBal", 0)),
            "unrealized_pnl": float(data.get("upl", 0)),
            "currency": currency,
            "details": data
        }
    
    def get_positions(self, inst_type: str = "SWAP") -> List[Dict[str, Any]]:
        """
        Get all open positions.
        
        Returns:
            List of position dicts with instrument, side, size, entry_price, pnl
        """
        result = self.account_api.get_positions(instType=inst_type)
        self._check_response(result)
        
        positions = []
        for pos in result.get("data", []):
            if float(pos.get("pos", 0)) == 0:
                continue
                
            positions.append({
                "instrument": pos.get("instId"),
                "side": "long" if pos.get("posSide") == "long" or float(pos.get("pos", 0)) > 0 else "short",
                "size": abs(float(pos.get("pos", 0))),
                "avg_entry_price": float(pos.get("avgPx", 0)),
                "current_price": float(pos.get("markPx", 0)),
                "unrealized_pnl": float(pos.get("upl", 0)),
                "leverage": float(pos.get("lever", 1)),
                "margin_mode": pos.get("mgnMode", "cross"),
                "notional_usd": float(pos.get("notionalUsd", 0)),
                "liq_price": float(pos.get("liqPx", 0)) if pos.get("liqPx") else None,
                "raw": pos
            })
            
        return positions
    
    def get_position(self, inst_id: str) -> Optional[Dict[str, Any]]:
        """Get position for a specific instrument."""
        positions = self.get_positions()
        return next((p for p in positions if p["instrument"] == inst_id), None)
    
    # ==================== Trading ====================
    
    def set_leverage(self, inst_id: str, leverage: int, margin_mode: str = "cross") -> Dict:
        """Set leverage for an instrument."""
        result = self.account_api.set_leverage(
            instId=inst_id,
            lever=str(leverage),
            mgnMode=margin_mode
        )
        return self._check_response(result)
    
    def place_market_order(
        self,
        inst_id: str,
        side: str,  # "buy" or "sell"
        size: float,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Place a market order.
        
        Args:
            inst_id: Instrument ID (e.g., "BTC-USDT-SWAP")
            side: "buy" or "sell"
            size: Order size in contracts
            reduce_only: If True, only reduce existing position
            client_order_id: Optional client order ID
            
        Returns:
            Order result with order_id
        """
        params = {
            "instId": inst_id,
            "tdMode": "cross",  # cross margin
            "side": side,
            "ordType": "market",
            "sz": str(size),
        }
        
        if reduce_only:
            params["reduceOnly"] = True
            
        if client_order_id:
            params["clOrdId"] = client_order_id
            
        result = self.trade_api.place_order(**params)
        self._check_response(result)
        
        order_data = result.get("data", [{}])[0]
        logger.info(f"Market order placed: {side} {size} {inst_id} -> {order_data.get('ordId')}")
        
        return {
            "order_id": order_data.get("ordId"),
            "client_order_id": order_data.get("clOrdId"),
            "instrument": inst_id,
            "side": side,
            "size": size,
            "order_type": "market",
            "status": "submitted"
        }
    
    def place_limit_order(
        self,
        inst_id: str,
        side: str,
        size: float,
        price: float,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a limit order."""
        params = {
            "instId": inst_id,
            "tdMode": "cross",
            "side": side,
            "ordType": "limit",
            "sz": str(size),
            "px": str(price),
        }
        
        if reduce_only:
            params["reduceOnly"] = True
            
        if client_order_id:
            params["clOrdId"] = client_order_id
            
        result = self.trade_api.place_order(**params)
        self._check_response(result)
        
        order_data = result.get("data", [{}])[0]
        logger.info(f"Limit order placed: {side} {size} {inst_id} @ {price} -> {order_data.get('ordId')}")
        
        return {
            "order_id": order_data.get("ordId"),
            "client_order_id": order_data.get("clOrdId"),
            "instrument": inst_id,
            "side": side,
            "size": size,
            "price": price,
            "order_type": "limit",
            "status": "submitted"
        }
    
    def place_order_with_sl_tp(
        self,
        inst_id: str,
        side: str,
        size: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Place a market order with attached stop loss and take profit.
        
        Uses OKX's algo order functionality.
        """
        # First place the market order
        order_result = self.place_market_order(inst_id, side, size)
        
        # Then place SL/TP if specified
        if stop_loss:
            sl_side = "sell" if side == "buy" else "buy"
            self._place_algo_order(
                inst_id, sl_side, size, 
                trigger_price=stop_loss,
                order_type="conditional"
            )
            
        if take_profit:
            tp_side = "sell" if side == "buy" else "buy"
            self._place_algo_order(
                inst_id, tp_side, size,
                trigger_price=take_profit,
                order_type="conditional"
            )
            
        return order_result
    
    def _place_algo_order(
        self,
        inst_id: str,
        side: str,
        size: float,
        trigger_price: float,
        order_type: str = "conditional"
    ) -> Dict:
        """Place an algo/conditional order (SL/TP)."""
        result = self.trade_api.place_algo_order(
            instId=inst_id,
            tdMode="cross",
            side=side,
            ordType=order_type,
            sz=str(size),
            slTriggerPx=str(trigger_price),
            slOrdPx="-1",  # Market price
        )
        return self._check_response(result)
    
    def cancel_order(self, inst_id: str, order_id: str) -> Dict:
        """Cancel an open order."""
        result = self.trade_api.cancel_order(instId=inst_id, ordId=order_id)
        return self._check_response(result)
    
    def get_order(self, inst_id: str, order_id: str) -> Dict[str, Any]:
        """Get order details."""
        result = self.trade_api.get_order(instId=inst_id, ordId=order_id)
        self._check_response(result)
        
        data = result.get("data", [{}])[0]
        return {
            "order_id": data.get("ordId"),
            "instrument": data.get("instId"),
            "side": data.get("side"),
            "size": float(data.get("sz", 0)),
            "price": float(data.get("px", 0)) if data.get("px") else None,
            "filled_size": float(data.get("fillSz", 0)),
            "filled_price": float(data.get("avgPx", 0)) if data.get("avgPx") else None,
            "fee": float(data.get("fee", 0)),
            "status": data.get("state"),
            "order_type": data.get("ordType"),
            "raw": data
        }
    
    def close_position(self, inst_id: str) -> Dict[str, Any]:
        """Close entire position for an instrument."""
        result = self.trade_api.close_positions(
            instId=inst_id,
            mgnMode="cross"
        )
        self._check_response(result)
        
        data = result.get("data", [{}])[0]
        logger.info(f"Position closed: {inst_id}")
        
        return {
            "instrument": inst_id,
            "status": "closed"
        }
    
    # ==================== Market Data ====================
    
    def get_ticker(self, inst_id: str) -> Dict[str, Any]:
        """Get current ticker/price for an instrument."""
        result = self.market_api.get_ticker(instId=inst_id)
        self._check_response(result)
        
        data = result.get("data", [{}])[0]
        return {
            "instrument": inst_id,
            "last_price": float(data.get("last", 0)),
            "bid": float(data.get("bidPx", 0)),
            "ask": float(data.get("askPx", 0)),
            "volume_24h": float(data.get("vol24h", 0)),
            "change_24h_pct": float(data.get("sodUtc8", 0)),
        }
    
    def get_klines(
        self,
        inst_id: str,
        bar: str = "1H",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get candlestick data.
        
        Args:
            inst_id: Instrument ID
            bar: Candle size ("1m", "5m", "15m", "1H", "4H", "1D")
            limit: Number of candles (max 100)
            
        Returns:
            List of candle dicts with ts, open, high, low, close, volume
        """
        result = self.market_api.get_candlesticks(
            instId=inst_id,
            bar=bar,
            limit=str(limit)
        )
        self._check_response(result)
        
        candles = []
        for c in result.get("data", []):
            candles.append({
                "ts": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
                "volume_ccy": float(c[6]),
                "volume_ccy_quote": float(c[7]) if len(c) > 7 else 0,
            })
            
        # Sort by timestamp ascending
        candles.sort(key=lambda x: x["ts"])
        return candles
    
    def get_instrument_info(self, inst_id: str) -> Dict[str, Any]:
        """Get instrument specifications."""
        result = self.public_api.get_instruments(instType="SWAP", instId=inst_id)
        self._check_response(result)
        
        data = result.get("data", [{}])[0]
        return {
            "instrument": inst_id,
            "contract_value": float(data.get("ctVal", 1)),
            "tick_size": float(data.get("tickSz", 0.01)),
            "lot_size": float(data.get("lotSz", 1)),
            "min_size": float(data.get("minSz", 1)),
            "contract_type": data.get("ctType"),
            "settle_currency": data.get("settleCcy"),
        }


# Convenience function
def get_trader() -> OKXTrader:
    """Get a new OKX trader instance."""
    return OKXTrader()
