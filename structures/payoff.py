"""
Payoff Modeling for Option Structures.

Calculates P&L at expiration and before expiration for various structures.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import numpy as np

from data.schemas import (
    OptionLeg,
    OptionStructure,
    OptionType,
    StructureType,
)
from structures.pricing import (
    price_option,
    time_to_expiry_years,
    get_risk_free_rate,
)


@dataclass
class PayoffResult:
    """Result of payoff calculation."""
    
    # P&L at various underlying prices
    prices: list[float]
    pnl: list[float]
    
    # Key metrics
    max_profit: Optional[float]
    max_loss: float
    breakeven_low: Optional[float]
    breakeven_high: Optional[float]
    
    # At expiration or current
    is_expiration: bool


def calculate_leg_value_at_expiration(
    leg: OptionLeg,
    underlying_price: float
) -> float:
    """
    Calculate the value of a leg at expiration.
    
    Args:
        leg: Option leg
        underlying_price: Price of underlying at expiration
        
    Returns:
        Leg value (positive = value to holder)
    """
    contract = leg.contract
    quantity = leg.quantity
    
    if contract.option_type == OptionType.CALL:
        intrinsic = max(underlying_price - contract.strike, 0)
    else:
        intrinsic = max(contract.strike - underlying_price, 0)
    
    # Value per contract * quantity * multiplier
    return intrinsic * quantity * 100


def calculate_structure_value_at_expiration(
    structure: OptionStructure,
    underlying_price: float
) -> float:
    """
    Calculate total structure value at expiration.
    
    Args:
        structure: Option structure
        underlying_price: Price of underlying
        
    Returns:
        Total structure value
    """
    total = 0
    for leg in structure.legs:
        total += calculate_leg_value_at_expiration(leg, underlying_price)
    return total


def calculate_structure_pnl_at_expiration(
    structure: OptionStructure,
    underlying_price: float
) -> float:
    """
    Calculate P&L of structure at expiration.
    
    Args:
        structure: Option structure
        underlying_price: Underlying price at expiration
        
    Returns:
        P&L including entry cost
    """
    exp_value = calculate_structure_value_at_expiration(structure, underlying_price)
    
    # Entry cost
    entry_cost = 0
    if structure.entry_debit:
        entry_cost = -structure.entry_debit * 100  # Paid debit
    elif structure.entry_credit:
        entry_cost = structure.entry_credit * 100  # Received credit
    
    return exp_value + entry_cost


def calculate_leg_value_before_expiration(
    leg: OptionLeg,
    underlying_price: float,
    new_iv: float,
    as_of: date,
) -> float:
    """
    Calculate leg value before expiration using BS model.
    
    Args:
        leg: Option leg
        underlying_price: Current underlying price
        new_iv: Current implied volatility
        as_of: As-of date
        
    Returns:
        Leg value
    """
    contract = leg.contract
    quantity = leg.quantity
    
    option_price = price_option(
        option_type=contract.option_type.value,
        spot=underlying_price,
        strike=contract.strike,
        expiration=contract.expiration,
        iv=new_iv,
        as_of=as_of,
    )
    
    return option_price * quantity * 100


def calculate_structure_value_before_expiration(
    structure: OptionStructure,
    underlying_price: float,
    new_iv: float,
    as_of: date,
) -> float:
    """
    Calculate structure value before expiration.
    
    Args:
        structure: Option structure
        underlying_price: Current underlying price
        new_iv: Current IV (applied to all legs for simplicity)
        as_of: As-of date
        
    Returns:
        Current structure value
    """
    total = 0
    for leg in structure.legs:
        # Use leg's own IV if available, else use provided IV
        iv = leg.contract.iv if leg.contract.iv else new_iv
        total += calculate_leg_value_before_expiration(
            leg, underlying_price, iv, as_of
        )
    return total


def calculate_payoff_curve(
    structure: OptionStructure,
    current_price: float,
    price_range_pct: float = 0.20,
    num_points: int = 100,
    at_expiration: bool = True,
    as_of: Optional[date] = None,
    new_iv: Optional[float] = None,
) -> PayoffResult:
    """
    Calculate payoff curve for a structure.
    
    Args:
        structure: Option structure
        current_price: Current underlying price
        price_range_pct: Price range as percentage of current (±20% default)
        num_points: Number of price points
        at_expiration: If True, calculate at expiration; else current
        as_of: As-of date (required if not at_expiration)
        new_iv: IV for before-expiration calc
        
    Returns:
        PayoffResult with prices, pnl, and key metrics
    """
    # Generate price range
    low = current_price * (1 - price_range_pct)
    high = current_price * (1 + price_range_pct)
    prices = np.linspace(low, high, num_points).tolist()
    
    # Calculate P&L at each price
    pnl = []
    for price in prices:
        if at_expiration:
            p = calculate_structure_pnl_at_expiration(structure, price)
        else:
            exp_value = calculate_structure_value_before_expiration(
                structure, price, new_iv or 0.20, as_of or date.today()
            )
            # Subtract entry cost
            entry_cost = 0
            if structure.entry_debit:
                entry_cost = -structure.entry_debit * 100
            elif structure.entry_credit:
                entry_cost = structure.entry_credit * 100
            p = exp_value + entry_cost
        pnl.append(p)
    
    # Calculate key metrics
    max_profit = max(pnl)
    max_loss = min(pnl)
    
    # Find breakevens (where P&L crosses zero)
    breakevens = []
    for i in range(len(pnl) - 1):
        if (pnl[i] < 0 and pnl[i+1] >= 0) or (pnl[i] >= 0 and pnl[i+1] < 0):
            # Linear interpolation
            ratio = abs(pnl[i]) / (abs(pnl[i]) + abs(pnl[i+1]))
            be_price = prices[i] + ratio * (prices[i+1] - prices[i])
            breakevens.append(be_price)
    
    breakeven_low = min(breakevens) if breakevens else None
    breakeven_high = max(breakevens) if len(breakevens) > 1 else None
    if breakeven_low and breakeven_high and breakeven_low == breakeven_high:
        breakeven_high = None
    
    return PayoffResult(
        prices=prices,
        pnl=pnl,
        max_profit=max_profit if max_profit > 0 else None,
        max_loss=abs(max_loss),
        breakeven_low=breakeven_low,
        breakeven_high=breakeven_high,
        is_expiration=at_expiration,
    )


def calculate_max_loss(structure: OptionStructure, current_price: float) -> float:
    """
    Calculate the maximum loss for a structure.
    
    This is a critical function for risk management.
    
    Args:
        structure: Option structure
        current_price: Current underlying price
        
    Returns:
        Maximum loss as positive number
    """
    payoff = calculate_payoff_curve(
        structure, current_price, 
        price_range_pct=0.50,  # Wide range
        num_points=500,
        at_expiration=True
    )
    return payoff.max_loss


def calculate_max_profit(structure: OptionStructure, current_price: float) -> Optional[float]:
    """
    Calculate the maximum profit for a structure.
    
    Returns None if profit is unlimited.
    
    Args:
        structure: Option structure
        current_price: Current underlying price
        
    Returns:
        Maximum profit or None if unlimited
    """
    payoff = calculate_payoff_curve(
        structure, current_price,
        price_range_pct=0.50,
        num_points=500,
        at_expiration=True
    )
    
    # Check if profit at extremes is still increasing
    # (indicates unlimited profit)
    if payoff.pnl[-1] >= payoff.pnl[-2] and payoff.pnl[-1] > payoff.max_profit * 0.99:
        return None
    if payoff.pnl[0] >= payoff.pnl[1] and payoff.pnl[0] > payoff.max_profit * 0.99:
        return None
    
    return payoff.max_profit


def calculate_breakevens(structure: OptionStructure, current_price: float) -> list[float]:
    """
    Calculate breakeven prices for a structure.
    
    Args:
        structure: Option structure
        current_price: Current underlying price
        
    Returns:
        List of breakeven prices
    """
    payoff = calculate_payoff_curve(
        structure, current_price,
        price_range_pct=0.30,
        num_points=200,
        at_expiration=True
    )
    
    breakevens = []
    if payoff.breakeven_low:
        breakevens.append(payoff.breakeven_low)
    if payoff.breakeven_high:
        breakevens.append(payoff.breakeven_high)
    
    return breakevens
