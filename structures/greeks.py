"""
Greeks Calculation and Aggregation.

Provides Greeks calculation for individual options and aggregation for structures.
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from structures.pricing import (
    BSOutput,
    OptionSide,
    bs_greeks,
    time_to_expiry_years,
    get_risk_free_rate,
)
from data.schemas import OptionContract, OptionLeg, OptionStructure, Greeks


@dataclass
class PositionGreeks:
    """Greeks for a position (contract * quantity)."""
    delta: float
    gamma: float
    theta: float  # Per day
    vega: float   # Per 1% IV change
    rho: float    # Per 1% rate change
    
    @classmethod
    def from_bs_output(cls, bs: BSOutput, quantity: int) -> 'PositionGreeks':
        """Create position Greeks from BS output and quantity."""
        return cls(
            delta=bs.delta * quantity * 100,  # Per-share delta
            gamma=bs.gamma * quantity * 100,
            theta=bs.theta * quantity * 100,
            vega=bs.vega * quantity * 100,
            rho=bs.rho * quantity * 100,
        )
    
    def __add__(self, other: 'PositionGreeks') -> 'PositionGreeks':
        """Add two PositionGreeks together."""
        return PositionGreeks(
            delta=self.delta + other.delta,
            gamma=self.gamma + other.gamma,
            theta=self.theta + other.theta,
            vega=self.vega + other.vega,
            rho=self.rho + other.rho,
        )


def calculate_contract_greeks(
    contract: OptionContract,
    spot: float,
    as_of: Optional[date] = None,
    risk_free_rate: Optional[float] = None,
    dividend_yield: float = 0.0
) -> Greeks:
    """
    Calculate Greeks for an option contract.
    
    Uses contract's IV if available. If IV is missing but mid price exists,
    attempts to solve for IV using the IV solver.
    
    Args:
        contract: The option contract
        spot: Current underlying price
        as_of: As-of date (defaults to today)
        risk_free_rate: Risk-free rate
        dividend_yield: Dividend yield
        
    Returns:
        Greeks dataclass
    """
    from structures.pricing import implied_volatility
    
    iv = contract.iv
    
    # If no IV, try to solve from mid price
    if iv is None or iv <= 0:
        if contract.mid and contract.mid > 0:
            t = time_to_expiry_years(contract.expiration, as_of)
            if t > 0:
                r = risk_free_rate if risk_free_rate is not None else get_risk_free_rate()
                option_side = (
                    OptionSide.CALL if contract.option_type.value == "call" 
                    else OptionSide.PUT
                )
                solved_iv = implied_volatility(
                    option_side,
                    contract.mid,
                    spot,
                    contract.strike,
                    t,
                    r,
                    dividend_yield
                )
                if solved_iv is not None:
                    iv = solved_iv
    
    # If still no IV, return zeros
    if iv is None or iv <= 0:
        return Greeks(delta=0, gamma=0, theta=0, vega=0)
    
    option_type = (
        OptionSide.CALL if contract.option_type.value == "call" 
        else OptionSide.PUT
    )
    t = time_to_expiry_years(contract.expiration, as_of)
    r = risk_free_rate if risk_free_rate is not None else get_risk_free_rate()
    
    bs = bs_greeks(
        option_type,
        spot,
        contract.strike,
        t,
        r,
        iv,
        dividend_yield
    )
    
    return Greeks(
        delta=bs.delta,
        gamma=bs.gamma,
        theta=bs.theta,
        vega=bs.vega,
        rho=bs.rho
    )


def calculate_leg_greeks(
    leg: OptionLeg,
    spot: float,
    as_of: Optional[date] = None,
    risk_free_rate: Optional[float] = None,
    dividend_yield: float = 0.0
) -> PositionGreeks:
    """
    Calculate position Greeks for an option leg.
    
    Args:
        leg: Option leg with contract and quantity
        spot: Current underlying price
        as_of: As-of date
        risk_free_rate: Risk-free rate
        dividend_yield: Dividend yield
        
    Returns:
        PositionGreeks (scaled by quantity and contract multiplier)
    """
    contract = leg.contract
    
    if contract.iv is None or contract.iv <= 0:
        return PositionGreeks(delta=0, gamma=0, theta=0, vega=0, rho=0)
    
    option_type = (
        OptionSide.CALL if contract.option_type.value == "call" 
        else OptionSide.PUT
    )
    t = time_to_expiry_years(contract.expiration, as_of)
    r = risk_free_rate if risk_free_rate is not None else get_risk_free_rate()
    
    bs = bs_greeks(
        option_type,
        spot,
        contract.strike,
        t,
        r,
        contract.iv,
        dividend_yield
    )
    
    return PositionGreeks.from_bs_output(bs, leg.quantity)


def calculate_structure_greeks(
    structure: OptionStructure,
    spot: float,
    as_of: Optional[date] = None,
    risk_free_rate: Optional[float] = None,
    dividend_yield: float = 0.0
) -> PositionGreeks:
    """
    Calculate aggregate Greeks for an option structure.
    
    Args:
        structure: The option structure with all legs
        spot: Current underlying price
        as_of: As-of date
        risk_free_rate: Risk-free rate
        dividend_yield: Dividend yield
        
    Returns:
        Aggregate PositionGreeks for the entire structure
    """
    total = PositionGreeks(delta=0, gamma=0, theta=0, vega=0, rho=0)
    
    for leg in structure.legs:
        leg_greeks = calculate_leg_greeks(
            leg, spot, as_of, risk_free_rate, dividend_yield
        )
        total = total + leg_greeks
    
    return total


def update_structure_greeks(
    structure: OptionStructure,
    spot: float,
    as_of: Optional[date] = None
) -> OptionStructure:
    """
    Update the Greeks on an OptionStructure in place.
    
    Args:
        structure: The structure to update
        spot: Current underlying price
        as_of: As-of date
        
    Returns:
        The same structure with updated Greeks
    """
    greeks = calculate_structure_greeks(structure, spot, as_of)
    
    structure.net_delta = greeks.delta
    structure.net_gamma = greeks.gamma
    structure.net_theta = greeks.theta
    structure.net_vega = greeks.vega
    
    return structure


# ============================================================================
# Delta Analysis
# ============================================================================

def get_delta_for_strike(
    spot: float,
    strike: float,
    expiration: date,
    iv: float,
    option_type: str = "call",
    as_of: Optional[date] = None
) -> float:
    """
    Get delta for a specific strike.
    
    Useful for finding 25-delta or other specific delta strikes.
    """
    side = OptionSide.CALL if option_type.lower() == "call" else OptionSide.PUT
    t = time_to_expiry_years(expiration, as_of)
    r = get_risk_free_rate()
    
    bs = bs_greeks(side, spot, strike, t, r, iv)
    return bs.delta


def find_strike_for_delta(
    spot: float,
    target_delta: float,
    expiration: date,
    iv: float,
    option_type: str = "call",
    as_of: Optional[date] = None,
    strike_increment: float = 1.0
) -> float:
    """
    Find the strike closest to a target delta.
    
    Args:
        spot: Current underlying price
        target_delta: Target delta (e.g., 0.25 for 25-delta)
        expiration: Expiration date
        iv: Implied volatility
        option_type: "call" or "put"
        as_of: As-of date
        strike_increment: Strike price increment (e.g., 1, 5)
        
    Returns:
        Strike price closest to target delta
    """
    if option_type.lower() == "put":
        # Put delta is negative, so use absolute value for search
        target_delta = -abs(target_delta)
    
    # Search range around spot
    search_range = spot * 0.3  # ±30% of spot
    
    best_strike = spot
    best_delta_diff = float('inf')
    
    strike = spot - search_range
    while strike <= spot + search_range:
        delta = get_delta_for_strike(
            spot, strike, expiration, iv, option_type, as_of
        )
        delta_diff = abs(delta - target_delta)
        
        if delta_diff < best_delta_diff:
            best_delta_diff = delta_diff
            best_strike = strike
        
        strike += strike_increment
    
    # Round to nearest increment
    return round(best_strike / strike_increment) * strike_increment
