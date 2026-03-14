"""
Risk Manager

Handles position sizing, exposure limits, and risk controls.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, date
from dataclasses import dataclass, field

from .config import trading_config

logger = logging.getLogger(__name__)


@dataclass
class RiskState:
    """Current risk state for the portfolio."""
    total_equity: float = 0.0
    available_balance: float = 0.0
    unrealized_pnl: float = 0.0
    daily_pnl: float = 0.0
    daily_pnl_start: float = 0.0  # Equity at start of day
    open_positions: int = 0
    total_exposure: float = 0.0  # Notional value of all positions
    positions: List[Dict] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    trading_date: date = field(default_factory=date.today)


class RiskManager:
    """
    Risk management for the trading system.
    
    Enforces:
    - Max position size per trade
    - Max total exposure
    - Max concurrent positions
    - Daily loss limit
    - Required stop losses
    
    Usage:
        rm = RiskManager()
        rm.update_state(balance, positions)
        
        if rm.can_trade():
            size = rm.calculate_position_size(signal)
            if rm.validate_order(inst_id, side, size):
                # proceed with order
    """
    
    def __init__(self):
        self.config = trading_config
        self.state = RiskState()
        
    def update_state(
        self,
        balance: Dict[str, Any],
        positions: List[Dict[str, Any]]
    ):
        """Update risk state with current account info."""
        today = date.today()
        
        # Reset daily PnL tracking on new day
        if self.state.trading_date != today:
            self.state.daily_pnl_start = balance.get("total_equity", 0)
            self.state.daily_pnl = 0
            self.state.trading_date = today
        
        self.state.total_equity = balance.get("total_equity", 0)
        self.state.available_balance = balance.get("available_balance", 0)
        self.state.unrealized_pnl = balance.get("unrealized_pnl", 0)
        
        # Calculate daily PnL
        if self.state.daily_pnl_start == 0:
            self.state.daily_pnl_start = self.state.total_equity
        self.state.daily_pnl = self.state.total_equity - self.state.daily_pnl_start
        
        # Position tracking
        self.state.positions = positions
        self.state.open_positions = len(positions)
        self.state.total_exposure = sum(
            abs(p.get("notional_usd", 0)) for p in positions
        )
        
        self.state.last_updated = datetime.utcnow()
        
        logger.debug(
            f"Risk state updated: equity={self.state.total_equity:.2f}, "
            f"positions={self.state.open_positions}, "
            f"exposure={self.state.total_exposure:.2f}"
        )
    
    def can_trade(self) -> tuple[bool, str]:
        """
        Check if trading is allowed based on risk limits.
        
        Returns:
            (can_trade, reason)
        """
        # Check daily loss limit
        if self.state.daily_pnl_start > 0:
            daily_loss_pct = -self.state.daily_pnl / self.state.daily_pnl_start
            if daily_loss_pct >= self.config.daily_loss_limit_pct:
                return False, f"Daily loss limit reached ({daily_loss_pct:.1%})"
        
        # Check max positions
        if self.state.open_positions >= self.config.max_concurrent_positions:
            return False, f"Max positions reached ({self.state.open_positions})"
        
        # Check total exposure
        exposure_pct = self.state.total_exposure / self.state.total_equity if self.state.total_equity > 0 else 0
        if exposure_pct >= self.config.max_exposure_pct:
            return False, f"Max exposure reached ({exposure_pct:.1%})"
        
        return True, "OK"
    
    def calculate_position_size(
        self,
        instrument: str,
        entry_price: float,
        stop_loss: Optional[float] = None,
        signal_size_pct: float = 1.0,
        contract_value: float = 1.0,
    ) -> float:
        """
        Calculate position size based on risk parameters.
        
        Args:
            instrument: Instrument ID
            entry_price: Entry price
            stop_loss: Stop loss price (optional, uses default risk if None)
            signal_size_pct: Signal's suggested size as fraction (0-1)
            contract_value: Value per contract
            
        Returns:
            Position size in contracts
        """
        equity = self.state.total_equity
        if equity <= 0:
            return 0.0
        
        # Max position value based on config
        max_position_value = equity * self.config.max_position_pct * signal_size_pct
        
        # If stop loss provided, size based on risk
        if stop_loss and stop_loss != entry_price:
            risk_per_contract = abs(entry_price - stop_loss) * contract_value
            if risk_per_contract > 0:
                # Risk 2% of equity per trade
                max_risk = equity * 0.02
                risk_based_size = max_risk / risk_per_contract
                
                # Use the smaller of value-based or risk-based size
                value_based_size = max_position_value / (entry_price * contract_value)
                position_size = min(risk_based_size, value_based_size)
            else:
                position_size = max_position_value / (entry_price * contract_value)
        else:
            position_size = max_position_value / (entry_price * contract_value)
        
        # Round to reasonable precision
        position_size = round(position_size, 3)
        
        logger.debug(
            f"Position size for {instrument}: {position_size} "
            f"(entry={entry_price}, sl={stop_loss}, max_val={max_position_value:.2f})"
        )
        
        return max(0, position_size)
    
    def validate_order(
        self,
        instrument: str,
        side: str,
        size: float,
        price: float,
        contract_value: float = 1.0,
    ) -> tuple[bool, str]:
        """
        Validate if an order can be placed.
        
        Returns:
            (is_valid, reason)
        """
        if size <= 0:
            return False, "Invalid size"
        
        # Check if we can trade
        can_trade, reason = self.can_trade()
        if not can_trade:
            return False, reason
        
        # Check if this would exceed exposure limit
        order_notional = size * price * contract_value
        new_exposure = self.state.total_exposure + order_notional
        new_exposure_pct = new_exposure / self.state.total_equity if self.state.total_equity > 0 else 0
        
        if new_exposure_pct > self.config.max_exposure_pct:
            return False, f"Would exceed max exposure ({new_exposure_pct:.1%})"
        
        # Check if position already exists in opposite direction
        existing_pos = next(
            (p for p in self.state.positions if p.get("instrument") == instrument),
            None
        )
        if existing_pos:
            existing_side = existing_pos.get("side")
            if existing_side == "long" and side == "buy":
                return False, "Already have long position"
            if existing_side == "short" and side == "sell":
                return False, "Already have short position"
        
        return True, "OK"
    
    def get_risk_summary(self) -> Dict[str, Any]:
        """Get current risk summary."""
        equity = self.state.total_equity or 1
        
        return {
            "total_equity": self.state.total_equity,
            "available_balance": self.state.available_balance,
            "unrealized_pnl": self.state.unrealized_pnl,
            "daily_pnl": self.state.daily_pnl,
            "daily_pnl_pct": self.state.daily_pnl / self.state.daily_pnl_start * 100 if self.state.daily_pnl_start else 0,
            "open_positions": self.state.open_positions,
            "max_positions": self.config.max_concurrent_positions,
            "total_exposure": self.state.total_exposure,
            "exposure_pct": self.state.total_exposure / equity * 100,
            "max_exposure_pct": self.config.max_exposure_pct * 100,
            "daily_loss_limit_pct": self.config.daily_loss_limit_pct * 100,
            "can_trade": self.can_trade()[0],
        }
