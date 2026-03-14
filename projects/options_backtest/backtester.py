#!/usr/bin/env python3
"""
Options Backtester — Simulated Backtest for Options Strategies

Strategies:
1. Covered Call: Long BTC + sell OTM call monthly
2. Cash-Secured Put: Sell OTM put monthly
3. IV-RV Spread (Short Strangle): Sell when IV > RV
4. Iron Condor: Sell OTM call+put, buy further OTM protection

Uses data from the PostgreSQL database where available.
Generates simulated historical scenarios for backtest.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import json
import math
import random

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_mHEynLCfk69D@ep-sweet-unit-a1qsqfu5-pooler.ap-southeast-1.aws.neon.tech/trading?sslmode=require&channel_binding=require"
)

# Chart styling (dark theme)
CHART_STYLE = {
    "facecolor": "#1a1a2e",
    "ax_facecolor": "#16213e",
    "text_color": "#e0e0e0",
    "positive_color": "#00d4aa",
    "negative_color": "#ff4757",
    "neutral_color": "#ffd700",
    "grid_color": "#333333",
    "colors": ["#00d4aa", "#ff4757", "#ffd700", "#00bfff", "#ff69b4", "#32cd32"]
}


@dataclass
class BacktestConfig:
    """Backtest configuration."""
    initial_capital: float = 100_000  # USD
    underlying: str = "BTC"
    
    # Option selection
    target_dte: int = 30  # days to expiry
    otm_delta_call: float = 0.30  # for covered call
    otm_delta_put: float = 0.30  # for cash-secured put
    otm_delta_strangle: float = 0.15  # for short strangle
    otm_delta_condor_inner: float = 0.20  # for iron condor inner wings
    otm_delta_condor_outer: float = 0.10  # for iron condor outer wings
    
    # Risk management
    max_position_pct: float = 0.10  # max 10% of capital per trade
    stop_loss_multiple: float = 2.0  # exit if loss > 2x premium
    
    # Trading costs
    fee_rate: float = 0.0003  # 0.03% taker fee
    slippage_pct: float = 0.005  # 0.5% slippage
    
    # IV-RV threshold
    iv_rv_entry_threshold: float = 0.10  # IV - RV > 10%
    iv_rv_exit_threshold: float = 0.02  # IV - RV < 2%


@dataclass
class Trade:
    """Single options trade."""
    entry_date: datetime
    exit_date: Optional[datetime]
    strategy: str
    underlying_entry: float
    underlying_exit: Optional[float]
    strike: float
    option_type: str  # C or P
    position: str  # long or short
    premium_entry: float
    premium_exit: Optional[float]
    contracts: float
    pnl: Optional[float] = None
    fees: float = 0.0
    iv_entry: Optional[float] = None
    iv_exit: Optional[float] = None


@dataclass
class StrategyResult:
    """Results for a single strategy."""
    name: str
    trades: List[Trade]
    equity_curve: List[Tuple[datetime, float]]
    total_pnl: float
    win_rate: float
    max_drawdown: float
    sharpe_ratio: float
    avg_trade_pnl: float
    num_trades: int


class BlackScholes:
    """Black-Scholes option pricing and Greeks."""
    
    @staticmethod
    def d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
        if T <= 0 or sigma <= 0:
            return 0
        return (math.log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    
    @staticmethod
    def d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
        if T <= 0 or sigma <= 0:
            return 0
        return BlackScholes.d1(S, K, T, r, sigma) - sigma * math.sqrt(T)
    
    @staticmethod
    def norm_cdf(x: float) -> float:
        return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0
    
    @staticmethod
    def call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
        if T <= 0:
            return max(S - K, 0)
        d1 = BlackScholes.d1(S, K, T, r, sigma)
        d2 = BlackScholes.d2(S, K, T, r, sigma)
        return S * BlackScholes.norm_cdf(d1) - K * math.exp(-r * T) * BlackScholes.norm_cdf(d2)
    
    @staticmethod
    def put_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
        if T <= 0:
            return max(K - S, 0)
        d1 = BlackScholes.d1(S, K, T, r, sigma)
        d2 = BlackScholes.d2(S, K, T, r, sigma)
        return K * math.exp(-r * T) * BlackScholes.norm_cdf(-d2) - S * BlackScholes.norm_cdf(-d1)
    
    @staticmethod
    def delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
        if T <= 0:
            if option_type == 'C':
                return 1.0 if S > K else 0.0
            else:
                return -1.0 if S < K else 0.0
        d1 = BlackScholes.d1(S, K, T, r, sigma)
        if option_type == 'C':
            return BlackScholes.norm_cdf(d1)
        else:
            return BlackScholes.norm_cdf(d1) - 1


class MarketSimulator:
    """Simulate market scenarios for backtesting."""
    
    def __init__(self, initial_price: float, vol: float = 0.6, days: int = 365):
        self.initial_price = initial_price
        self.vol = vol  # annualized volatility
        self.days = days
        
    def generate_price_path(self, seed: int = None) -> List[Tuple[datetime, float]]:
        """Generate GBM price path."""
        if seed:
            np.random.seed(seed)
        
        dt = 1/365
        prices = [self.initial_price]
        current = self.initial_price
        
        start_date = datetime.now(timezone.utc) - timedelta(days=self.days)
        dates = [start_date]
        
        for i in range(self.days):
            drift = 0.05 * dt  # 5% annual drift
            shock = self.vol * np.sqrt(dt) * np.random.normal()
            current = current * np.exp(drift + shock)
            prices.append(current)
            dates.append(start_date + timedelta(days=i+1))
        
        return list(zip(dates, prices))
    
    def generate_iv_path(self, price_path: List[Tuple[datetime, float]], 
                         base_iv: float = 0.6) -> List[Tuple[datetime, float]]:
        """Generate IV path correlated with price moves."""
        iv_path = []
        prev_price = price_path[0][1]
        current_iv = base_iv
        
        for date, price in price_path:
            # IV tends to increase on down moves (vol smile)
            price_return = (price - prev_price) / prev_price if prev_price > 0 else 0
            iv_change = -price_return * 0.5  # IV increases on down moves
            iv_mean_revert = (base_iv - current_iv) * 0.1  # mean reversion
            iv_noise = np.random.normal(0, 0.02)
            
            current_iv = current_iv + iv_change + iv_mean_revert + iv_noise
            current_iv = max(0.2, min(1.5, current_iv))  # clamp between 20% and 150%
            
            iv_path.append((date, current_iv))
            prev_price = price
        
        return iv_path
    
    def calculate_realized_vol(self, prices: List[float], window: int = 30) -> float:
        """Calculate realized volatility."""
        if len(prices) < window + 1:
            return 0.5
        
        returns = np.diff(np.log(prices[-window-1:]))
        return np.std(returns) * np.sqrt(365)


class OptionsBacktester:
    """Main backtester for options strategies."""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.bs = BlackScholes()
        self.r = 0.05  # risk-free rate
    
    def find_option_by_delta(self, S: float, T: float, sigma: float, 
                             target_delta: float, option_type: str) -> float:
        """Find strike for target delta using bisection."""
        if option_type == 'C':
            low_k, high_k = S * 0.5, S * 2.0
        else:
            low_k, high_k = S * 0.5, S * 1.5
        
        for _ in range(50):
            mid_k = (low_k + high_k) / 2
            d = abs(self.bs.delta(S, mid_k, T, self.r, sigma, option_type))
            
            if abs(d - target_delta) < 0.01:
                return mid_k
            
            if option_type == 'C':
                if d > target_delta:
                    low_k = mid_k
                else:
                    high_k = mid_k
            else:
                if d > target_delta:
                    high_k = mid_k
                else:
                    low_k = mid_k
        
        return mid_k
    
    def run_covered_call(self, price_path: List[Tuple[datetime, float]], 
                         iv_path: List[Tuple[datetime, float]]) -> StrategyResult:
        """Run covered call strategy."""
        trades = []
        capital = self.config.initial_capital
        equity_curve = [(price_path[0][0], capital)]
        
        # Track position
        btc_position = 0
        call_strike = None
        call_expiry = None
        call_premium = 0
        entry_date = None
        entry_price = 0
        
        position_size = capital * self.config.max_position_pct
        
        for i, ((date, price), (_, iv)) in enumerate(zip(price_path, iv_path)):
            # Check if we need to open a position (monthly)
            if btc_position == 0 and i % 30 == 0:
                # Buy underlying
                contracts = position_size / price
                btc_position = contracts
                entry_price = price
                entry_date = date
                
                # Sell OTM call
                T = self.config.target_dte / 365
                call_strike = self.find_option_by_delta(price, T, iv, 
                                                       self.config.otm_delta_call, 'C')
                call_premium = self.bs.call_price(price, call_strike, T, self.r, iv)
                call_expiry = date + timedelta(days=self.config.target_dte)
                
                # Collect premium
                capital += call_premium * contracts * (1 - self.config.fee_rate - self.config.slippage_pct)
            
            # Check expiry
            elif btc_position > 0 and call_expiry and date >= call_expiry:
                # Calculate PnL
                if price >= call_strike:
                    # Called away - sell at strike
                    exit_price = call_strike
                else:
                    # Keep underlying
                    exit_price = price
                
                pnl = (exit_price - entry_price) * btc_position + call_premium * btc_position
                fees = exit_price * btc_position * self.config.fee_rate
                pnl -= fees
                capital += pnl
                
                trade = Trade(
                    entry_date=entry_date,
                    exit_date=date,
                    strategy="covered_call",
                    underlying_entry=entry_price,
                    underlying_exit=exit_price,
                    strike=call_strike,
                    option_type='C',
                    position='short',
                    premium_entry=call_premium,
                    premium_exit=0 if price >= call_strike else call_premium,
                    contracts=btc_position,
                    pnl=pnl,
                    fees=fees,
                    iv_entry=iv
                )
                trades.append(trade)
                
                # Reset position
                btc_position = 0
                call_strike = None
                call_expiry = None
            
            equity_curve.append((date, capital))
        
        return self._calculate_result("Covered Call", trades, equity_curve)
    
    def run_cash_secured_put(self, price_path: List[Tuple[datetime, float]], 
                             iv_path: List[Tuple[datetime, float]]) -> StrategyResult:
        """Run cash-secured put strategy."""
        trades = []
        capital = self.config.initial_capital
        equity_curve = [(price_path[0][0], capital)]
        
        put_strike = None
        put_expiry = None
        put_premium = 0
        entry_date = None
        entry_iv = None
        contracts = 0
        
        position_size = capital * self.config.max_position_pct
        
        for i, ((date, price), (_, iv)) in enumerate(zip(price_path, iv_path)):
            # Open position monthly
            if put_strike is None and i % 30 == 0:
                T = self.config.target_dte / 365
                put_strike = self.find_option_by_delta(price, T, iv, 
                                                      self.config.otm_delta_put, 'P')
                put_premium = self.bs.put_price(price, put_strike, T, self.r, iv)
                put_expiry = date + timedelta(days=self.config.target_dte)
                entry_date = date
                entry_iv = iv
                
                # Size based on strike (cash secured)
                contracts = position_size / put_strike
                
                # Collect premium
                capital += put_premium * contracts * (1 - self.config.fee_rate - self.config.slippage_pct)
            
            # Check expiry
            elif put_strike and date >= put_expiry:
                if price <= put_strike:
                    # Assigned - buy at strike
                    assignment_cost = (put_strike - price) * contracts
                    pnl = put_premium * contracts - assignment_cost
                else:
                    # Expires worthless - keep premium
                    pnl = put_premium * contracts
                
                fees = put_strike * contracts * self.config.fee_rate
                pnl -= fees
                capital += pnl - put_premium * contracts  # Adjust for already-collected premium
                
                trade = Trade(
                    entry_date=entry_date,
                    exit_date=date,
                    strategy="cash_secured_put",
                    underlying_entry=price,
                    underlying_exit=price,
                    strike=put_strike,
                    option_type='P',
                    position='short',
                    premium_entry=put_premium,
                    premium_exit=max(put_strike - price, 0),
                    contracts=contracts,
                    pnl=pnl,
                    fees=fees,
                    iv_entry=entry_iv
                )
                trades.append(trade)
                
                put_strike = None
                put_expiry = None
            
            equity_curve.append((date, capital))
        
        return self._calculate_result("Cash-Secured Put", trades, equity_curve)
    
    def run_short_strangle(self, price_path: List[Tuple[datetime, float]], 
                           iv_path: List[Tuple[datetime, float]]) -> StrategyResult:
        """Run short strangle (IV-RV spread) strategy."""
        trades = []
        capital = self.config.initial_capital
        equity_curve = [(price_path[0][0], capital)]
        
        call_strike = None
        put_strike = None
        expiry = None
        call_premium = 0
        put_premium = 0
        entry_date = None
        entry_price = 0
        entry_iv = None
        contracts = 0
        
        position_size = capital * self.config.max_position_pct
        simulator = MarketSimulator(price_path[0][1])
        
        for i, ((date, price), (_, iv)) in enumerate(zip(price_path, iv_path)):
            # Calculate RV
            recent_prices = [p for _, p in price_path[max(0, i-30):i+1]]
            rv = simulator.calculate_realized_vol(recent_prices)
            
            # Entry condition: IV > RV + threshold
            if call_strike is None and iv - rv > self.config.iv_rv_entry_threshold and i % 7 == 0:
                T = self.config.target_dte / 365
                
                call_strike = self.find_option_by_delta(price, T, iv,
                                                       self.config.otm_delta_strangle, 'C')
                put_strike = self.find_option_by_delta(price, T, iv,
                                                      self.config.otm_delta_strangle, 'P')
                
                call_premium = self.bs.call_price(price, call_strike, T, self.r, iv)
                put_premium = self.bs.put_price(price, put_strike, T, self.r, iv)
                
                expiry = date + timedelta(days=self.config.target_dte)
                entry_date = date
                entry_price = price
                entry_iv = iv
                
                contracts = position_size / price * 0.5  # Half size for strangle
                
                # Collect both premiums
                total_premium = (call_premium + put_premium) * contracts
                capital += total_premium * (1 - self.config.fee_rate - self.config.slippage_pct)
            
            # Check expiry or exit condition
            elif call_strike and (date >= expiry or iv - rv < self.config.iv_rv_exit_threshold):
                T_remaining = max((expiry - date).days / 365, 0)
                
                # Calculate exit values
                call_exit = self.bs.call_price(price, call_strike, T_remaining, self.r, iv)
                put_exit = self.bs.put_price(price, put_strike, T_remaining, self.r, iv)
                
                # PnL = premium collected - cost to close
                pnl = (call_premium + put_premium - call_exit - put_exit) * contracts
                fees = (call_exit + put_exit) * contracts * self.config.fee_rate
                pnl -= fees
                capital += pnl - (call_premium + put_premium) * contracts
                
                trade = Trade(
                    entry_date=entry_date,
                    exit_date=date,
                    strategy="short_strangle",
                    underlying_entry=entry_price,
                    underlying_exit=price,
                    strike=(call_strike + put_strike) / 2,
                    option_type='S',  # Strangle
                    position='short',
                    premium_entry=call_premium + put_premium,
                    premium_exit=call_exit + put_exit,
                    contracts=contracts,
                    pnl=pnl,
                    fees=fees,
                    iv_entry=entry_iv,
                    iv_exit=iv
                )
                trades.append(trade)
                
                call_strike = None
                put_strike = None
                expiry = None
            
            equity_curve.append((date, capital))
        
        return self._calculate_result("Short Strangle (IV-RV)", trades, equity_curve)
    
    def run_iron_condor(self, price_path: List[Tuple[datetime, float]], 
                        iv_path: List[Tuple[datetime, float]]) -> StrategyResult:
        """Run iron condor strategy."""
        trades = []
        capital = self.config.initial_capital
        equity_curve = [(price_path[0][0], capital)]
        
        inner_call = None
        outer_call = None
        inner_put = None
        outer_put = None
        expiry = None
        net_credit = 0
        entry_date = None
        entry_price = 0
        entry_iv = None
        contracts = 0
        
        position_size = capital * self.config.max_position_pct
        
        for i, ((date, price), (_, iv)) in enumerate(zip(price_path, iv_path)):
            # Open monthly
            if inner_call is None and i % 30 == 0:
                T = self.config.target_dte / 365
                
                # Find strikes
                inner_call = self.find_option_by_delta(price, T, iv,
                                                       self.config.otm_delta_condor_inner, 'C')
                outer_call = self.find_option_by_delta(price, T, iv,
                                                       self.config.otm_delta_condor_outer, 'C')
                inner_put = self.find_option_by_delta(price, T, iv,
                                                      self.config.otm_delta_condor_inner, 'P')
                outer_put = self.find_option_by_delta(price, T, iv,
                                                      self.config.otm_delta_condor_outer, 'P')
                
                # Calculate premiums
                inner_call_prem = self.bs.call_price(price, inner_call, T, self.r, iv)
                outer_call_prem = self.bs.call_price(price, outer_call, T, self.r, iv)
                inner_put_prem = self.bs.put_price(price, inner_put, T, self.r, iv)
                outer_put_prem = self.bs.put_price(price, outer_put, T, self.r, iv)
                
                # Net credit = sell inner - buy outer
                net_credit = (inner_call_prem - outer_call_prem + 
                             inner_put_prem - outer_put_prem)
                
                expiry = date + timedelta(days=self.config.target_dte)
                entry_date = date
                entry_price = price
                entry_iv = iv
                
                # Size based on max risk (width of wings)
                wing_width = max(outer_call - inner_call, inner_put - outer_put)
                contracts = position_size / wing_width * 0.5
                
                # Collect net credit
                capital += net_credit * contracts * (1 - self.config.fee_rate - self.config.slippage_pct)
            
            # Check expiry
            elif inner_call and date >= expiry:
                # Calculate payoff at expiry
                if price >= outer_call:
                    # Max loss on call side
                    pnl = net_credit - (outer_call - inner_call)
                elif price >= inner_call:
                    # Partial loss on call side
                    pnl = net_credit - (price - inner_call)
                elif price <= outer_put:
                    # Max loss on put side
                    pnl = net_credit - (inner_put - outer_put)
                elif price <= inner_put:
                    # Partial loss on put side
                    pnl = net_credit - (inner_put - price)
                else:
                    # Price between inner strikes - max profit
                    pnl = net_credit
                
                pnl = pnl * contracts
                fees = net_credit * contracts * self.config.fee_rate
                pnl -= fees
                capital += pnl - net_credit * contracts
                
                trade = Trade(
                    entry_date=entry_date,
                    exit_date=date,
                    strategy="iron_condor",
                    underlying_entry=entry_price,
                    underlying_exit=price,
                    strike=(inner_call + inner_put) / 2,
                    option_type='IC',
                    position='short',
                    premium_entry=net_credit,
                    premium_exit=net_credit - pnl/contracts if contracts > 0 else 0,
                    contracts=contracts,
                    pnl=pnl,
                    fees=fees,
                    iv_entry=entry_iv
                )
                trades.append(trade)
                
                inner_call = None
                outer_call = None
                inner_put = None
                outer_put = None
                expiry = None
            
            equity_curve.append((date, capital))
        
        return self._calculate_result("Iron Condor", trades, equity_curve)
    
    def _calculate_result(self, name: str, trades: List[Trade], 
                         equity_curve: List[Tuple[datetime, float]]) -> StrategyResult:
        """Calculate strategy statistics."""
        if not trades:
            return StrategyResult(
                name=name,
                trades=[],
                equity_curve=equity_curve,
                total_pnl=0,
                win_rate=0,
                max_drawdown=0,
                sharpe_ratio=0,
                avg_trade_pnl=0,
                num_trades=0
            )
        
        pnls = [t.pnl for t in trades if t.pnl is not None]
        total_pnl = sum(pnls)
        wins = len([p for p in pnls if p > 0])
        win_rate = wins / len(pnls) if pnls else 0
        avg_pnl = np.mean(pnls) if pnls else 0
        
        # Max drawdown
        equity_values = [e[1] for e in equity_curve]
        peak = equity_values[0]
        max_dd = 0
        for val in equity_values:
            if val > peak:
                peak = val
            dd = (peak - val) / peak
            max_dd = max(max_dd, dd)
        
        # Sharpe ratio (annualized)
        if len(pnls) > 1:
            returns = np.array(pnls) / self.config.initial_capital
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(12) if np.std(returns) > 0 else 0
        else:
            sharpe = 0
        
        return StrategyResult(
            name=name,
            trades=trades,
            equity_curve=equity_curve,
            total_pnl=total_pnl,
            win_rate=win_rate,
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            avg_trade_pnl=avg_pnl,
            num_trades=len(trades)
        )
    
    def run_all_strategies(self, price_path: List[Tuple[datetime, float]], 
                           iv_path: List[Tuple[datetime, float]]) -> Dict[str, StrategyResult]:
        """Run all strategies and return results."""
        results = {}
        
        logger.info("Running Covered Call...")
        results["covered_call"] = self.run_covered_call(price_path, iv_path)
        
        logger.info("Running Cash-Secured Put...")
        results["cash_secured_put"] = self.run_cash_secured_put(price_path, iv_path)
        
        logger.info("Running Short Strangle...")
        results["short_strangle"] = self.run_short_strangle(price_path, iv_path)
        
        logger.info("Running Iron Condor...")
        results["iron_condor"] = self.run_iron_condor(price_path, iv_path)
        
        return results


class Visualizer:
    """Generate charts for backtest results."""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def plot_equity_curves(self, results: Dict[str, StrategyResult], 
                          initial_capital: float) -> str:
        """Plot all strategy equity curves."""
        fig, ax = plt.subplots(figsize=(14, 8))
        fig.patch.set_facecolor(CHART_STYLE["facecolor"])
        ax.set_facecolor(CHART_STYLE["ax_facecolor"])
        
        for i, (name, result) in enumerate(results.items()):
            dates = [e[0] for e in result.equity_curve]
            values = [e[1] for e in result.equity_curve]
            color = CHART_STYLE["colors"][i % len(CHART_STYLE["colors"])]
            ax.plot(dates, values, label=result.name, color=color, linewidth=2)
        
        # Initial capital line
        ax.axhline(y=initial_capital, color=CHART_STYLE["text_color"], 
                  linestyle='--', alpha=0.5, label='Initial Capital')
        
        ax.set_xlabel('Date', color=CHART_STYLE["text_color"])
        ax.set_ylabel('Portfolio Value ($)', color=CHART_STYLE["text_color"])
        ax.set_title('Options Strategy Equity Curves', color=CHART_STYLE["text_color"], fontsize=14)
        ax.legend(facecolor=CHART_STYLE["ax_facecolor"], edgecolor=CHART_STYLE["grid_color"],
                 labelcolor=CHART_STYLE["text_color"])
        ax.grid(True, color=CHART_STYLE["grid_color"], alpha=0.3)
        ax.tick_params(colors=CHART_STYLE["text_color"])
        
        plt.tight_layout()
        path = os.path.join(self.output_dir, "equity_curves.png")
        plt.savefig(path, facecolor=CHART_STYLE["facecolor"], dpi=150)
        plt.close()
        
        return path
    
    def plot_strategy_comparison(self, results: Dict[str, StrategyResult]) -> str:
        """Plot strategy comparison bar chart."""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.patch.set_facecolor(CHART_STYLE["facecolor"])
        
        strategies = list(results.keys())
        colors = [CHART_STYLE["colors"][i % len(CHART_STYLE["colors"])] 
                 for i in range(len(strategies))]
        
        metrics = [
            ("Total PnL ($)", [r.total_pnl for r in results.values()]),
            ("Win Rate (%)", [r.win_rate * 100 for r in results.values()]),
            ("Max Drawdown (%)", [r.max_drawdown * 100 for r in results.values()]),
            ("Sharpe Ratio", [r.sharpe_ratio for r in results.values()]),
        ]
        
        for ax, (title, values) in zip(axes.flatten(), metrics):
            ax.set_facecolor(CHART_STYLE["ax_facecolor"])
            bars = ax.bar([r.name for r in results.values()], values, color=colors)
            ax.set_title(title, color=CHART_STYLE["text_color"])
            ax.tick_params(colors=CHART_STYLE["text_color"])
            ax.set_xticklabels([r.name for r in results.values()], 
                              rotation=15, ha='right', fontsize=9)
            ax.grid(True, axis='y', color=CHART_STYLE["grid_color"], alpha=0.3)
            
            # Color bars based on value
            for bar, val in zip(bars, values):
                if "Drawdown" in title:
                    bar.set_color(CHART_STYLE["negative_color"] if val > 10 else CHART_STYLE["positive_color"])
                elif val < 0:
                    bar.set_color(CHART_STYLE["negative_color"])
        
        plt.tight_layout()
        path = os.path.join(self.output_dir, "strategy_comparison.png")
        plt.savefig(path, facecolor=CHART_STYLE["facecolor"], dpi=150)
        plt.close()
        
        return path
    
    def plot_iv_rv(self, price_path: List[Tuple[datetime, float]], 
                   iv_path: List[Tuple[datetime, float]]) -> str:
        """Plot IV vs RV time series."""
        fig, ax = plt.subplots(figsize=(14, 6))
        fig.patch.set_facecolor(CHART_STYLE["facecolor"])
        ax.set_facecolor(CHART_STYLE["ax_facecolor"])
        
        dates = [d for d, _ in iv_path]
        ivs = [v * 100 for _, v in iv_path]  # Convert to %
        
        # Calculate RV
        simulator = MarketSimulator(price_path[0][1])
        rvs = []
        for i in range(len(price_path)):
            recent_prices = [p for _, p in price_path[max(0, i-30):i+1]]
            rv = simulator.calculate_realized_vol(recent_prices)
            rvs.append(rv * 100)
        
        ax.plot(dates, ivs, label='Implied Volatility', 
               color=CHART_STYLE["positive_color"], linewidth=2)
        ax.plot(dates, rvs, label='Realized Volatility (30d)', 
               color=CHART_STYLE["neutral_color"], linewidth=2)
        
        # Highlight IV > RV periods
        ax.fill_between(dates, ivs, rvs, where=[i > r for i, r in zip(ivs, rvs)],
                        color=CHART_STYLE["positive_color"], alpha=0.2, 
                        label='IV Premium')
        
        ax.set_xlabel('Date', color=CHART_STYLE["text_color"])
        ax.set_ylabel('Volatility (%)', color=CHART_STYLE["text_color"])
        ax.set_title('Implied vs Realized Volatility', color=CHART_STYLE["text_color"], fontsize=14)
        ax.legend(facecolor=CHART_STYLE["ax_facecolor"], edgecolor=CHART_STYLE["grid_color"],
                 labelcolor=CHART_STYLE["text_color"])
        ax.grid(True, color=CHART_STYLE["grid_color"], alpha=0.3)
        ax.tick_params(colors=CHART_STYLE["text_color"])
        
        plt.tight_layout()
        path = os.path.join(self.output_dir, "iv_rv_spread.png")
        plt.savefig(path, facecolor=CHART_STYLE["facecolor"], dpi=150)
        plt.close()
        
        return path
    
    def plot_pnl_breakdown(self, results: Dict[str, StrategyResult]) -> str:
        """Plot PnL breakdown per strategy."""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.patch.set_facecolor(CHART_STYLE["facecolor"])
        
        for ax, (name, result) in zip(axes.flatten(), results.items()):
            ax.set_facecolor(CHART_STYLE["ax_facecolor"])
            
            if result.trades:
                pnls = [t.pnl for t in result.trades if t.pnl is not None]
                colors = [CHART_STYLE["positive_color"] if p > 0 else CHART_STYLE["negative_color"] 
                         for p in pnls]
                ax.bar(range(len(pnls)), pnls, color=colors)
                ax.axhline(y=0, color=CHART_STYLE["text_color"], linestyle='-', alpha=0.5)
            
            ax.set_title(f'{result.name} Trade PnL', color=CHART_STYLE["text_color"])
            ax.set_xlabel('Trade #', color=CHART_STYLE["text_color"])
            ax.set_ylabel('PnL ($)', color=CHART_STYLE["text_color"])
            ax.tick_params(colors=CHART_STYLE["text_color"])
            ax.grid(True, axis='y', color=CHART_STYLE["grid_color"], alpha=0.3)
        
        plt.tight_layout()
        path = os.path.join(self.output_dir, "pnl_breakdown.png")
        plt.savefig(path, facecolor=CHART_STYLE["facecolor"], dpi=150)
        plt.close()
        
        return path
    
    def plot_volatility_surface(self, strikes: List[float], expiries: List[int], 
                                ivs: List[List[float]], underlying_price: float) -> str:
        """Plot volatility smile/surface."""
        fig = plt.figure(figsize=(14, 8))
        ax = fig.add_subplot(111, projection='3d')
        fig.patch.set_facecolor(CHART_STYLE["facecolor"])
        
        X, Y = np.meshgrid(strikes, expiries)
        Z = np.array(ivs) * 100  # Convert to %
        
        surf = ax.plot_surface(X, Y, Z, cmap='viridis', alpha=0.8)
        
        ax.set_xlabel('Strike', color=CHART_STYLE["text_color"])
        ax.set_ylabel('Days to Expiry', color=CHART_STYLE["text_color"])
        ax.set_zlabel('IV (%)', color=CHART_STYLE["text_color"])
        ax.set_title(f'Volatility Surface (S=${underlying_price:,.0f})', 
                    color=CHART_STYLE["text_color"], fontsize=14)
        
        # Add ATM line
        ax.plot([underlying_price, underlying_price], [min(expiries), max(expiries)], 
               [np.min(Z), np.max(Z)], color=CHART_STYLE["neutral_color"], 
               linewidth=2, label='ATM')
        
        ax.tick_params(colors=CHART_STYLE["text_color"])
        fig.colorbar(surf, shrink=0.5, aspect=5, label='IV (%)')
        
        plt.tight_layout()
        path = os.path.join(self.output_dir, "volatility_surface.png")
        plt.savefig(path, facecolor=CHART_STYLE["facecolor"], dpi=150)
        plt.close()
        
        return path


async def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("OPTIONS BACKTESTER")
    logger.info("=" * 60)
    
    # Configuration
    config = BacktestConfig(
        initial_capital=100_000,
        underlying="BTC",
        target_dte=30,
    )
    
    # Get current BTC price from database
    import asyncpg
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Get latest ticker for reference price (BTC)
    row = await conn.fetchrow("""
        SELECT underlying_price FROM options_tickers 
        WHERE underlying_price IS NOT NULL AND underlying_price > 10000
        ORDER BY timestamp DESC LIMIT 1
    """)
    
    btc_price = float(row["underlying_price"]) if row else 70000
    logger.info(f"Reference BTC price: ${btc_price:,.2f}")
    
    # Get historical volatility from database
    hvol_rows = await conn.fetch("""
        SELECT realized_vol FROM historical_volatility 
        WHERE underlying = 'BTC' ORDER BY timestamp DESC LIMIT 10
    """)
    
    avg_vol = float(np.mean([float(r["realized_vol"]) for r in hvol_rows])) if hvol_rows else 0.6
    logger.info(f"Average historical vol: {avg_vol*100:.1f}%")
    
    await conn.close()
    
    # Generate simulated market data (1 year)
    logger.info("\nGenerating market simulation...")
    simulator = MarketSimulator(initial_price=btc_price, vol=avg_vol, days=365)
    price_path = simulator.generate_price_path(seed=42)
    iv_path = simulator.generate_iv_path(price_path, base_iv=avg_vol)
    
    logger.info(f"Simulated {len(price_path)} days of price data")
    logger.info(f"Price range: ${min(p for _, p in price_path):,.0f} - ${max(p for _, p in price_path):,.0f}")
    
    # Run backtest
    logger.info("\nRunning backtests...")
    backtester = OptionsBacktester(config)
    results = backtester.run_all_strategies(price_path, iv_path)
    
    # Print results
    logger.info("\n" + "=" * 60)
    logger.info("BACKTEST RESULTS")
    logger.info("=" * 60)
    
    for name, result in results.items():
        logger.info(f"\n{result.name}:")
        logger.info(f"  Total PnL: ${result.total_pnl:,.2f}")
        logger.info(f"  Win Rate: {result.win_rate*100:.1f}%")
        logger.info(f"  Max Drawdown: {result.max_drawdown*100:.1f}%")
        logger.info(f"  Sharpe Ratio: {result.sharpe_ratio:.2f}")
        logger.info(f"  Trades: {result.num_trades}")
        logger.info(f"  Avg Trade PnL: ${result.avg_trade_pnl:,.2f}")
    
    # Generate visualizations
    logger.info("\n" + "=" * 60)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("=" * 60)
    
    output_dir = os.path.dirname(os.path.abspath(__file__)) + "/output"
    visualizer = Visualizer(output_dir)
    
    # Equity curves
    path = visualizer.plot_equity_curves(results, config.initial_capital)
    logger.info(f"Created: {path}")
    
    # Strategy comparison
    path = visualizer.plot_strategy_comparison(results)
    logger.info(f"Created: {path}")
    
    # IV vs RV
    path = visualizer.plot_iv_rv(price_path, iv_path)
    logger.info(f"Created: {path}")
    
    # PnL breakdown
    path = visualizer.plot_pnl_breakdown(results)
    logger.info(f"Created: {path}")
    
    # Volatility surface (simulated)
    strikes = [btc_price * m for m in [0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3]]
    expiries = [7, 14, 30, 60, 90]
    ivs = []
    for dte in expiries:
        row_ivs = []
        for strike in strikes:
            moneyness = np.log(strike / btc_price)
            base_iv = avg_vol
            smile = 0.1 * moneyness**2  # Volatility smile
            term = 0.05 * np.sqrt(dte / 30)  # Term structure
            iv = base_iv + smile + term
            row_ivs.append(iv)
        ivs.append(row_ivs)
    
    path = visualizer.plot_volatility_surface(strikes, expiries, ivs, btc_price)
    logger.info(f"Created: {path}")
    
    logger.info("\n" + "=" * 60)
    logger.info("BACKTEST COMPLETE")
    logger.info("=" * 60)
    
    return results


if __name__ == "__main__":
    asyncio.run(main())
