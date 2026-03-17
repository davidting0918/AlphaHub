"""
Options Pricing — Black-Scholes and Greeks Calculations

Provides analytical pricing for European options.
"""

import math
from typing import Tuple, Dict
from dataclasses import dataclass

try:
    from scipy.stats import norm
    from scipy.optimize import brentq
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    # Fallback: simple normal CDF approximation
    def norm_cdf(x):
        """Approximation of standard normal CDF."""
        a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        p = 0.3275911
        sign = 1 if x >= 0 else -1
        x = abs(x) / math.sqrt(2)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
        return 0.5 * (1.0 + sign * y)
    
    def norm_pdf(x):
        """Standard normal PDF."""
        return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


@dataclass
class Greeks:
    """Option greeks container."""
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float


@dataclass
class OptionPrice:
    """Option pricing result."""
    price: float
    intrinsic: float
    time_value: float
    greeks: Greeks


def d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Calculate d1 for Black-Scholes."""
    if T <= 0 or sigma <= 0:
        return 0.0
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))


def d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Calculate d2 for Black-Scholes."""
    if T <= 0 or sigma <= 0:
        return 0.0
    return d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def black_scholes_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """
    Black-Scholes call option price.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate (annualized)
        sigma: Volatility (annualized)
    
    Returns:
        Call option price
    """
    if T <= 0:
        return max(0, S - K)  # Intrinsic value at expiry
    
    if SCIPY_AVAILABLE:
        d1_val = d1(S, K, T, r, sigma)
        d2_val = d2(S, K, T, r, sigma)
        return S * norm.cdf(d1_val) - K * math.exp(-r * T) * norm.cdf(d2_val)
    else:
        d1_val = d1(S, K, T, r, sigma)
        d2_val = d2(S, K, T, r, sigma)
        return S * norm_cdf(d1_val) - K * math.exp(-r * T) * norm_cdf(d2_val)


def black_scholes_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """
    Black-Scholes put option price.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate (annualized)
        sigma: Volatility (annualized)
    
    Returns:
        Put option price
    """
    if T <= 0:
        return max(0, K - S)  # Intrinsic value at expiry
    
    if SCIPY_AVAILABLE:
        d1_val = d1(S, K, T, r, sigma)
        d2_val = d2(S, K, T, r, sigma)
        return K * math.exp(-r * T) * norm.cdf(-d2_val) - S * norm.cdf(-d1_val)
    else:
        d1_val = d1(S, K, T, r, sigma)
        d2_val = d2(S, K, T, r, sigma)
        return K * math.exp(-r * T) * norm_cdf(-d2_val) - S * norm_cdf(-d1_val)


def calculate_greeks(S: float, K: float, T: float, r: float, sigma: float, 
                     option_type: str = "call") -> Greeks:
    """
    Calculate option greeks.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        sigma: Volatility
        option_type: "call" or "put"
    
    Returns:
        Greeks dataclass
    """
    if T <= 0:
        # At expiry
        if option_type == "call":
            delta = 1.0 if S > K else 0.0
        else:
            delta = -1.0 if S < K else 0.0
        return Greeks(delta=delta, gamma=0, theta=0, vega=0, rho=0)
    
    d1_val = d1(S, K, T, r, sigma)
    d2_val = d2(S, K, T, r, sigma)
    sqrt_T = math.sqrt(T)
    
    if SCIPY_AVAILABLE:
        pdf_d1 = norm.pdf(d1_val)
        cdf_d1 = norm.cdf(d1_val)
        cdf_d2 = norm.cdf(d2_val)
        cdf_neg_d1 = norm.cdf(-d1_val)
        cdf_neg_d2 = norm.cdf(-d2_val)
    else:
        pdf_d1 = norm_pdf(d1_val)
        cdf_d1 = norm_cdf(d1_val)
        cdf_d2 = norm_cdf(d2_val)
        cdf_neg_d1 = norm_cdf(-d1_val)
        cdf_neg_d2 = norm_cdf(-d2_val)
    
    # Gamma (same for calls and puts)
    gamma = pdf_d1 / (S * sigma * sqrt_T)
    
    # Vega (same for calls and puts) - per 1% move in vol
    vega = S * sqrt_T * pdf_d1 / 100
    
    if option_type == "call":
        delta = cdf_d1
        theta = (-S * pdf_d1 * sigma / (2 * sqrt_T) 
                 - r * K * math.exp(-r * T) * cdf_d2) / 365
        rho = K * T * math.exp(-r * T) * cdf_d2 / 100
    else:
        delta = cdf_d1 - 1
        theta = (-S * pdf_d1 * sigma / (2 * sqrt_T) 
                 + r * K * math.exp(-r * T) * cdf_neg_d2) / 365
        rho = -K * T * math.exp(-r * T) * cdf_neg_d2 / 100
    
    return Greeks(delta=delta, gamma=gamma, theta=theta, vega=vega, rho=rho)


def price_option(S: float, K: float, T: float, r: float, sigma: float,
                 option_type: str = "call") -> OptionPrice:
    """
    Full option pricing with greeks.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        sigma: Volatility
        option_type: "call" or "put"
    
    Returns:
        OptionPrice with price, intrinsic, time value, and greeks
    """
    if option_type == "call":
        price = black_scholes_call(S, K, T, r, sigma)
        intrinsic = max(0, S - K)
    else:
        price = black_scholes_put(S, K, T, r, sigma)
        intrinsic = max(0, K - S)
    
    time_value = price - intrinsic
    greeks = calculate_greeks(S, K, T, r, sigma, option_type)
    
    return OptionPrice(price=price, intrinsic=intrinsic, 
                       time_value=time_value, greeks=greeks)


def implied_volatility(option_price: float, S: float, K: float, T: float, 
                       r: float, option_type: str = "call",
                       max_iterations: int = 100, tol: float = 1e-5) -> float:
    """
    Calculate implied volatility using Newton-Raphson or Brent's method.
    
    Args:
        option_price: Market price of option
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        option_type: "call" or "put"
    
    Returns:
        Implied volatility (annualized)
    """
    if T <= 0:
        return 0.0
    
    # Handle edge cases
    if option_type == "call":
        intrinsic = max(0, S - K)
        max_price = S
    else:
        intrinsic = max(0, K - S)
        max_price = K * math.exp(-r * T)
    
    if option_price <= intrinsic:
        return 0.0
    if option_price >= max_price:
        return 5.0  # Very high vol
    
    def objective(sigma):
        if option_type == "call":
            return black_scholes_call(S, K, T, r, sigma) - option_price
        else:
            return black_scholes_put(S, K, T, r, sigma) - option_price
    
    if SCIPY_AVAILABLE:
        try:
            return brentq(objective, 0.001, 5.0, xtol=tol)
        except ValueError:
            pass
    
    # Newton-Raphson fallback
    sigma = 0.5  # Initial guess
    for _ in range(max_iterations):
        price = price_option(S, K, T, r, sigma, option_type)
        diff = price.price - option_price
        
        if abs(diff) < tol:
            return sigma
        
        vega = price.greeks.vega * 100  # Convert back from per-1% format
        if abs(vega) < 1e-10:
            break
        
        sigma = sigma - diff / vega
        sigma = max(0.001, min(sigma, 5.0))
    
    return sigma


def calculate_realized_volatility(prices: list, window: int = 30) -> float:
    """
    Calculate realized volatility from price series.
    
    Args:
        prices: List of prices (daily closes)
        window: Rolling window in days
    
    Returns:
        Annualized realized volatility
    """
    if len(prices) < 2:
        return 0.0
    
    # Calculate log returns
    returns = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0 and prices[i] > 0:
            returns.append(math.log(prices[i] / prices[i-1]))
    
    if len(returns) < 2:
        return 0.0
    
    # Use last N returns
    recent_returns = returns[-window:] if len(returns) > window else returns
    
    # Standard deviation of returns
    mean_ret = sum(recent_returns) / len(recent_returns)
    variance = sum((r - mean_ret) ** 2 for r in recent_returns) / len(recent_returns)
    std_ret = math.sqrt(variance)
    
    # Annualize (assuming daily data)
    return std_ret * math.sqrt(365)


def find_strike_by_delta(S: float, T: float, r: float, sigma: float,
                         target_delta: float, option_type: str = "call") -> float:
    """
    Find strike price for a given target delta.
    
    Args:
        S: Spot price
        T: Time to expiry
        r: Risk-free rate
        sigma: Volatility
        target_delta: Target delta (e.g., 0.30 for 30-delta call)
        option_type: "call" or "put"
    
    Returns:
        Strike price
    """
    if T <= 0 or sigma <= 0:
        return S
    
    if SCIPY_AVAILABLE:
        # Use Brent's method
        def objective(K):
            greeks = calculate_greeks(S, K, T, r, sigma, option_type)
            return greeks.delta - target_delta
        
        try:
            # Search range: 0.5x to 2x spot
            return brentq(objective, S * 0.3, S * 3.0)
        except ValueError:
            pass
    
    # Binary search fallback
    low_K, high_K = S * 0.3, S * 3.0
    for _ in range(50):
        mid_K = (low_K + high_K) / 2
        greeks = calculate_greeks(S, mid_K, T, r, sigma, option_type)
        
        if option_type == "call":
            # Call delta decreases as K increases
            if greeks.delta > target_delta:
                low_K = mid_K
            else:
                high_K = mid_K
        else:
            # Put delta (negative) increases (toward 0) as K increases
            if greeks.delta < target_delta:
                low_K = mid_K
            else:
                high_K = mid_K
        
        if high_K - low_K < S * 0.001:
            break
    
    return (low_K + high_K) / 2


def payoff_call(S: float, K: float, premium: float = 0) -> float:
    """Call option payoff at expiry."""
    return max(0, S - K) - premium


def payoff_put(S: float, K: float, premium: float = 0) -> float:
    """Put option payoff at expiry."""
    return max(0, K - S) - premium
