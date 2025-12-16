"""
Black-Scholes Option Pricing.

Implements the Black-Scholes-Merton model for European options pricing.
Used for theoretical pricing and implied volatility calculations.
"""

import math
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional, Tuple

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq


class OptionSide(str, Enum):
    """Option side: call or put."""
    CALL = "call"
    PUT = "put"


@dataclass
class BSInput:
    """Input parameters for Black-Scholes pricing."""
    spot: float          # Current underlying price
    strike: float        # Strike price
    time_to_expiry: float  # Time to expiry in years
    risk_free_rate: float  # Risk-free rate (annualized)
    volatility: float    # Implied volatility (annualized)
    dividend_yield: float = 0.0  # Continuous dividend yield


@dataclass
class BSOutput:
    """Output from Black-Scholes calculation."""
    price: float
    delta: float
    gamma: float
    theta: float  # Per day
    vega: float   # Per 1% IV change
    rho: float    # Per 1% rate change


def _d1(s: float, k: float, t: float, r: float, v: float, q: float = 0) -> float:
    """Calculate d1 parameter."""
    if t <= 0 or v <= 0:
        return 0.0
    return (math.log(s / k) + (r - q + 0.5 * v ** 2) * t) / (v * math.sqrt(t))


def _d2(d1: float, v: float, t: float) -> float:
    """Calculate d2 parameter."""
    if t <= 0:
        return 0.0
    return d1 - v * math.sqrt(t)


def bs_price(
    option_type: OptionSide,
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    dividend_yield: float = 0.0
) -> float:
    """
    Calculate Black-Scholes option price.
    
    Args:
        option_type: CALL or PUT
        spot: Current underlying price
        strike: Strike price
        time_to_expiry: Time to expiry in years
        risk_free_rate: Risk-free rate (annualized, e.g., 0.05 for 5%)
        volatility: Implied volatility (annualized, e.g., 0.20 for 20%)
        dividend_yield: Continuous dividend yield
        
    Returns:
        Theoretical option price
    """
    if time_to_expiry <= 0:
        # At expiration - return intrinsic value
        if option_type == OptionSide.CALL:
            return max(spot - strike, 0)
        else:
            return max(strike - spot, 0)
    
    if volatility <= 0:
        volatility = 0.0001  # Avoid division by zero
    
    d1 = _d1(spot, strike, time_to_expiry, risk_free_rate, volatility, dividend_yield)
    d2 = _d2(d1, volatility, time_to_expiry)
    
    discount = math.exp(-risk_free_rate * time_to_expiry)
    dividend_discount = math.exp(-dividend_yield * time_to_expiry)
    
    if option_type == OptionSide.CALL:
        price = (
            spot * dividend_discount * norm.cdf(d1) - 
            strike * discount * norm.cdf(d2)
        )
    else:
        price = (
            strike * discount * norm.cdf(-d2) - 
            spot * dividend_discount * norm.cdf(-d1)
        )
    
    return max(price, 0)


def bs_greeks(
    option_type: OptionSide,
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    dividend_yield: float = 0.0
) -> BSOutput:
    """
    Calculate Black-Scholes price and Greeks.
    
    Returns:
        BSOutput with price, delta, gamma, theta, vega, rho
    """
    if time_to_expiry <= 0:
        # At expiration
        intrinsic = (
            max(spot - strike, 0) if option_type == OptionSide.CALL 
            else max(strike - spot, 0)
        )
        itm = spot > strike if option_type == OptionSide.CALL else spot < strike
        return BSOutput(
            price=intrinsic,
            delta=1.0 if itm and option_type == OptionSide.CALL else (-1.0 if itm else 0.0),
            gamma=0.0,
            theta=0.0,
            vega=0.0,
            rho=0.0
        )
    
    if volatility <= 0:
        volatility = 0.0001
    
    sqrt_t = math.sqrt(time_to_expiry)
    d1 = _d1(spot, strike, time_to_expiry, risk_free_rate, volatility, dividend_yield)
    d2 = _d2(d1, volatility, time_to_expiry)
    
    discount = math.exp(-risk_free_rate * time_to_expiry)
    dividend_discount = math.exp(-dividend_yield * time_to_expiry)
    
    # Price
    if option_type == OptionSide.CALL:
        price = spot * dividend_discount * norm.cdf(d1) - strike * discount * norm.cdf(d2)
    else:
        price = strike * discount * norm.cdf(-d2) - spot * dividend_discount * norm.cdf(-d1)
    
    # Delta
    if option_type == OptionSide.CALL:
        delta = dividend_discount * norm.cdf(d1)
    else:
        delta = dividend_discount * (norm.cdf(d1) - 1)
    
    # Gamma (same for calls and puts)
    gamma = (dividend_discount * norm.pdf(d1)) / (spot * volatility * sqrt_t)
    
    # Theta (per day)
    term1 = -(spot * dividend_discount * norm.pdf(d1) * volatility) / (2 * sqrt_t)
    if option_type == OptionSide.CALL:
        term2 = -risk_free_rate * strike * discount * norm.cdf(d2)
        term3 = dividend_yield * spot * dividend_discount * norm.cdf(d1)
    else:
        term2 = risk_free_rate * strike * discount * norm.cdf(-d2)
        term3 = -dividend_yield * spot * dividend_discount * norm.cdf(-d1)
    
    theta = (term1 + term2 + term3) / 365  # Per day
    
    # Vega (per 1% IV change = 0.01 vol change)
    vega = spot * dividend_discount * norm.pdf(d1) * sqrt_t / 100
    
    # Rho (per 1% rate change)
    if option_type == OptionSide.CALL:
        rho = strike * time_to_expiry * discount * norm.cdf(d2) / 100
    else:
        rho = -strike * time_to_expiry * discount * norm.cdf(-d2) / 100
    
    return BSOutput(
        price=max(price, 0),
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
        rho=rho
    )


def implied_volatility(
    option_type: OptionSide,
    market_price: float,
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    dividend_yield: float = 0.0,
    initial_guess: float = 0.20,
    max_iterations: int = 100,
    tolerance: float = 1e-6
) -> Optional[float]:
    """
    Calculate implied volatility using Brent's method.
    
    Args:
        option_type: CALL or PUT
        market_price: Observed market price
        spot: Current underlying price
        strike: Strike price
        time_to_expiry: Time to expiry in years
        risk_free_rate: Risk-free rate
        dividend_yield: Dividend yield
        initial_guess: Starting IV guess
        max_iterations: Max iterations for solver
        tolerance: Price tolerance for convergence
        
    Returns:
        Implied volatility or None if cannot solve
    """
    if market_price <= 0 or time_to_expiry <= 0:
        return None
    
    # Check for no extrinsic value
    if option_type == OptionSide.CALL:
        intrinsic = max(spot - strike, 0)
    else:
        intrinsic = max(strike - spot, 0)
    
    if market_price <= intrinsic:
        return None  # No extrinsic value to solve for
    
    def objective(vol):
        return bs_price(
            option_type, spot, strike, time_to_expiry, 
            risk_free_rate, vol, dividend_yield
        ) - market_price
    
    try:
        # Use Brent's method with wide bounds
        iv = brentq(
            objective,
            0.001,  # Min IV (0.1%)
            5.0,    # Max IV (500%)
            xtol=tolerance,
            maxiter=max_iterations
        )
        return iv
    except (ValueError, RuntimeError):
        return None


def time_to_expiry_years(expiration: date, as_of: Optional[date] = None) -> float:
    """
    Calculate time to expiry in years.
    
    Args:
        expiration: Expiration date
        as_of: As-of date (defaults to today)
        
    Returns:
        Time to expiry in years (trading days / 252)
    """
    if as_of is None:
        as_of = date.today()
    
    days = (expiration - as_of).days
    
    if days <= 0:
        return 0.0
    
    # Use calendar days / 365 for simplicity
    # Could use trading days / 252 for more precision
    return days / 365.0


def get_risk_free_rate(config: dict = None) -> float:
    """
    Get current risk-free rate from config or default.
    
    Args:
        config: Optional config dict with market.risk_free_rate
        
    Returns:
        Risk-free rate (annualized, e.g., 0.045 for 4.5%)
    """
    if config and 'market' in config:
        return config['market'].get('risk_free_rate', 0.045)
    return 0.045  # Default 4.5%


def get_dividend_yield(symbol: str, config: dict = None) -> float:
    """
    Get dividend yield for a symbol from config or default.
    
    Args:
        symbol: Underlying symbol (e.g., 'SPY')
        config: Optional config dict with market.dividend_yields
        
    Returns:
        Annualized continuous dividend yield (e.g., 0.013 for 1.3%)
    """
    if config and 'market' in config:
        yields = config['market'].get('dividend_yields', {})
        return yields.get(symbol, yields.get('default', 0.0))
    
    # Hardcoded defaults for common ETFs
    defaults = {
        'SPY': 0.013,
        'QQQ': 0.005,
        'IWM': 0.011,
    }
    return defaults.get(symbol, 0.0)


# ============================================================================
# Convenience Functions
# ============================================================================

def price_option(
    option_type: str,  # "call" or "put"
    spot: float,
    strike: float,
    expiration: date,
    iv: float,
    as_of: Optional[date] = None,
    risk_free_rate: Optional[float] = None,
    dividend_yield: Optional[float] = None,
    symbol: Optional[str] = None,
    config: dict = None,
) -> float:
    """
    Convenience function to price an option.
    
    Args:
        option_type: "call" or "put"
        spot: Current underlying price
        strike: Strike price
        expiration: Expiration date
        iv: Implied volatility (e.g., 0.20 for 20%)
        as_of: As-of date (defaults to today)
        risk_free_rate: Override risk-free rate
        dividend_yield: Override dividend yield
        symbol: Symbol for looking up dividend yield from config
        config: Config dict for market parameters
        
    Returns:
        Theoretical option price
    """
    side = OptionSide.CALL if option_type.lower() == "call" else OptionSide.PUT
    t = time_to_expiry_years(expiration, as_of)
    r = risk_free_rate if risk_free_rate is not None else get_risk_free_rate(config)
    q = dividend_yield if dividend_yield is not None else get_dividend_yield(symbol or "", config)
    
    return bs_price(side, spot, strike, t, r, iv, q)


def calculate_greeks(
    option_type: str,
    spot: float,
    strike: float,
    expiration: date,
    iv: float,
    as_of: Optional[date] = None,
    risk_free_rate: Optional[float] = None,
    dividend_yield: Optional[float] = None,
    symbol: Optional[str] = None,
    config: dict = None,
) -> BSOutput:
    """
    Convenience function to calculate Greeks.
    
    Args:
        option_type: "call" or "put"
        spot: Underlying price
        strike: Strike price
        expiration: Expiration date
        iv: Implied volatility
        as_of: As-of date
        risk_free_rate: Override risk-free rate
        dividend_yield: Override dividend yield
        symbol: Symbol for looking up dividend yield from config
        config: Config dict for market parameters
    
    Returns:
        BSOutput with all Greeks
    """
    side = OptionSide.CALL if option_type.lower() == "call" else OptionSide.PUT
    t = time_to_expiry_years(expiration, as_of)
    r = risk_free_rate if risk_free_rate is not None else get_risk_free_rate(config)
    q = dividend_yield if dividend_yield is not None else get_dividend_yield(symbol or "", config)
    
    return bs_greeks(side, spot, strike, t, r, iv, q)
