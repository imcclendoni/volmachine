"""
Probability Metrics for Trade Candidates.

Computes probability of profit (PoP), expected value, and stress scenarios
using Black-Scholes lognormal assumptions.

IMPORTANT DISCLAIMERS:
- "Model PoP (expiration)" is NOT a win rate or success rate
- Real outcomes depend on exits, gaps, and volatility changes
- These are theoretical values under lognormal assumptions
- EV calculation is a BINARY APPROXIMATION (win full or lose full)
"""

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, List, Dict, Tuple

from scipy.stats import norm

from data.schemas import (
    OptionStructure,
    StructureType,
    OptionType,
)


@dataclass
class ProbabilityMetrics:
    """
    Probability metrics for a trade candidate.
    
    All probabilities are model-derived under lognormal assumptions.
    NOT predictive of actual outcomes - use for relative comparison only.
    
    IMPORTANT: breakevens must be computed using the same pricing assumption
    as max_loss (conservative bid/ask). builders.py uses conservative_credit_points
    for breakeven calculation, ensuring PoP is "honest."
    """
    
    # Core probabilities
    pop_expiry: float           # Model PoP (expiration) - P(profit at expiry)
    
    # Iron condor specific
    p_inside_short_strikes: float  # P(S_T between short put and short call)
    p_between_breakevens: float    # P(BE_low < S_T < BE_high)
    
    # Expected value - BINARY APPROXIMATION (do NOT use for ranking)
    expected_pnl_expiry_binary: float  # EV at expiration (dollars)
    
    # Distance metrics
    breakeven_distance_pct: float  # Distance from spot to nearest breakeven (%)
    
    # Honesty metrics (keep you honest)
    credit_to_width_ratio: float   # credit / width
    reward_to_risk_ratio: float    # max_profit / max_loss
    ev_per_dollar_risk_binary: float  # EV / max_loss (binary approx)
    
    # Stress scenarios (flat IV shift)
    stress_scenarios: Dict[str, float]  # scenario -> PnL
    
    # Assumptions used (for transparency)
    # Using object type to allow str, float, int values
    assumptions: Dict[str, object]
    
    # Warning
    warning: str = (
        "Model PoP assumes lognormal distribution at expiration. "
        "Real outcomes depend on exits, gaps, volatility changes, and execution. "
        "EV is a binary approximation (win full or lose full) - NOT for ranking."
    )


def _clamp_probability(p: float) -> float:
    """Clamp probability to [0, 1]."""
    return max(0.0, min(1.0, p))


def _prob_below(
    spot: float, 
    strike: float, 
    iv: float, 
    t: float, 
    r: float, 
    q: float
) -> float:
    """
    P(S_T < strike) under lognormal dynamics.
    
    Uses -d2 in norm.cdf: P(S_T < K) = N(-d2)
    """
    if t <= 0:
        return 1.0 if spot < strike else 0.0
    if iv <= 0:
        iv = 0.0001
    if strike <= 0:
        return 0.0
    
    d2 = (math.log(spot / strike) + (r - q - 0.5 * iv**2) * t) / (iv * math.sqrt(t))
    return _clamp_probability(norm.cdf(-d2))


def _prob_above(
    spot: float, 
    strike: float, 
    iv: float, 
    t: float, 
    r: float, 
    q: float
) -> float:
    """
    P(S_T > strike) = 1 - P(S_T < strike)
    """
    return _clamp_probability(1.0 - _prob_below(spot, strike, iv, t, r, q))


def _prob_between(
    spot: float,
    low: float,
    high: float,
    iv: float,
    t: float,
    r: float,
    q: float,
) -> float:
    """
    P(low < S_T < high) = P(S_T < high) - P(S_T < low)
    """
    if low >= high:
        return 0.0
    p_below_high = _prob_below(spot, high, iv, t, r, q)
    p_below_low = _prob_below(spot, low, iv, t, r, q)
    return _clamp_probability(p_below_high - p_below_low)


def calculate_probability_metrics(
    structure: OptionStructure,
    spot: float,
    iv: float,
    time_to_expiry: float,
    risk_free_rate: float,
    dividend_yield: float = 0.0,
    as_of_date: Optional[date] = None,
) -> ProbabilityMetrics:
    """
    Calculate probability metrics for a trade structure.
    
    Args:
        structure: The option structure
        spot: Current underlying price
        iv: ATM implied volatility (annualized)
        time_to_expiry: Time to expiry in years
        risk_free_rate: Risk-free rate (annualized)
        dividend_yield: Dividend yield (annualized)
        as_of_date: Reference date for calculations
        
    Returns:
        ProbabilityMetrics with all computed values
    """
    t, r, q = time_to_expiry, risk_free_rate, dividend_yield
    
    # Store assumptions
    assumptions = {
        "spot": spot,
        "iv": iv,
        "time_to_expiry_days": t * 365,
        "risk_free_rate": r,
        "dividend_yield": q,
        "as_of": as_of_date.isoformat() if as_of_date else "unknown",
    }
    
    # Get structure characteristics
    width_points = _get_structure_width(structure)
    max_loss_points = structure.max_loss or 0
    max_profit_points = structure.max_profit or 0
    credit_points = structure.entry_credit or 0
    
    max_loss_dollars = max_loss_points * 100
    max_profit_dollars = max_profit_points * 100
    
    # Get short strikes for iron condor
    short_put_strike, short_call_strike = _get_short_strikes_by_type(structure)
    breakevens = structure.breakevens
    
    # Calculate P(inside short strikes) - for iron condors
    if short_put_strike and short_call_strike and short_put_strike < short_call_strike:
        p_inside_short_strikes = _prob_between(
            spot, short_put_strike, short_call_strike, iv, t, r, q
        )
    else:
        p_inside_short_strikes = 0.5  # Default
    
    # Calculate P(between breakevens)
    if len(breakevens) >= 2:
        be_low, be_high = min(breakevens), max(breakevens)
        p_between_breakevens = _prob_between(spot, be_low, be_high, iv, t, r, q)
    else:
        p_between_breakevens = 0.5
    
    # Calculate PoP at expiration
    pop_expiry = _calculate_pop_expiry(structure, spot, iv, t, r, q)
    
    # Breakeven distance (to nearest)
    if breakevens:
        distances = [abs(spot - be) / spot * 100 for be in breakevens]
        breakeven_distance_pct = min(distances)
    else:
        breakeven_distance_pct = 0
    
    # Honesty metrics
    credit_to_width = credit_points / width_points if width_points > 0 else 0
    reward_to_risk = max_profit_points / max_loss_points if max_loss_points > 0 else 0
    
    # Expected PnL at expiration (BINARY APPROXIMATION - DO NOT USE FOR RANKING)
    # EV = P(win) * max_profit - P(lose) * max_loss
    expected_pnl_binary = (
        pop_expiry * max_profit_dollars - 
        (1 - pop_expiry) * max_loss_dollars
    )
    
    ev_per_dollar_risk_binary = expected_pnl_binary / max_loss_dollars if max_loss_dollars > 0 else 0
    
    # Stress scenarios (FLAT IV shift - same IV for all legs)
    stress = _calculate_stress_scenarios(structure, spot, iv, t, r, q)
    
    return ProbabilityMetrics(
        pop_expiry=pop_expiry,
        p_inside_short_strikes=p_inside_short_strikes,
        p_between_breakevens=p_between_breakevens,
        expected_pnl_expiry_binary=expected_pnl_binary,
        breakeven_distance_pct=breakeven_distance_pct,
        credit_to_width_ratio=credit_to_width,
        reward_to_risk_ratio=reward_to_risk,
        ev_per_dollar_risk_binary=ev_per_dollar_risk_binary,
        stress_scenarios=stress,
        assumptions=assumptions,
    )


def _calculate_pop_expiry(
    structure: OptionStructure,
    spot: float,
    iv: float,
    t: float,
    r: float,
    q: float,
) -> float:
    """
    Calculate probability of profit at expiration.
    
    IRON CONDOR / BUTTERFLY: P(BE_low < S_T < BE_high)
    CREDIT SPREAD: P(S_T stays OTM of breakeven)
    DEBIT SPREAD: P(S_T moves through breakeven)
    """
    structure_type = structure.structure_type
    breakevens = structure.breakevens
    
    # IRON CONDOR: profit if spot stays between both breakevens
    if structure_type == StructureType.IRON_CONDOR:
        if len(breakevens) >= 2:
            be_low, be_high = min(breakevens), max(breakevens)
            return _prob_between(spot, be_low, be_high, iv, t, r, q)
        return 0.5  # No good estimate without both breakevens
    
    # BUTTERFLY / IRON BUTTERFLY: profit in narrow range around center
    if structure_type in (StructureType.BUTTERFLY, StructureType.IRON_BUTTERFLY):
        if len(breakevens) >= 2:
            be_low, be_high = min(breakevens), max(breakevens)
            return _prob_between(spot, be_low, be_high, iv, t, r, q)
        return 0.3  # Conservative default
    
    # CREDIT SPREAD: profit if S_T stays OTM of breakeven
    if structure_type == StructureType.CREDIT_SPREAD:
        breakeven = breakevens[0] if breakevens else spot
        if _is_call_side(structure):
            # Call credit spread: profit if S_T < breakeven
            return _prob_below(spot, breakeven, iv, t, r, q)
        else:
            # Put credit spread: profit if S_T > breakeven
            return _prob_above(spot, breakeven, iv, t, r, q)
    
    # DEBIT SPREAD: profit if S_T moves through breakeven
    if structure_type == StructureType.DEBIT_SPREAD:
        breakeven = breakevens[0] if breakevens else spot
        if _is_call_side(structure):
            # Call debit: profit if S_T > breakeven
            return _prob_above(spot, breakeven, iv, t, r, q)
        else:
            # Put debit: profit if S_T < breakeven
            return _prob_below(spot, breakeven, iv, t, r, q)
    
    return 0.5


def _get_structure_width(structure: OptionStructure) -> float:
    """
    Get the width of the structure in points.
    
    For iron condors: computes put wing and call wing separately by option type,
    returns the max.
    """
    if not structure.legs or len(structure.legs) < 2:
        return 0
    
    structure_type = structure.structure_type
    
    # Iron condor: compute by option type, not by sorted strikes
    if structure_type == StructureType.IRON_CONDOR and len(structure.legs) == 4:
        put_strikes = []
        call_strikes = []
        
        for leg in structure.legs:
            if leg.contract.option_type == OptionType.PUT:
                put_strikes.append(leg.contract.strike)
            else:
                call_strikes.append(leg.contract.strike)
        
        put_width = max(put_strikes) - min(put_strikes) if len(put_strikes) >= 2 else 0
        call_width = max(call_strikes) - min(call_strikes) if len(call_strikes) >= 2 else 0
        
        return max(put_width, call_width)
    
    # For other structures: simple width
    strikes = sorted([leg.contract.strike for leg in structure.legs])
    if len(strikes) == 2:
        return strikes[1] - strikes[0]
    
    return strikes[-1] - strikes[0] if strikes else 0


def _get_short_strikes_by_type(
    structure: OptionStructure
) -> Tuple[Optional[float], Optional[float]]:
    """
    Get short strikes by option type.
    
    Returns:
        (short_put_strike, short_call_strike)
    """
    short_put = None
    short_call = None
    
    if not structure.legs:
        return (None, None)
    
    for leg in structure.legs:
        if leg.quantity < 0:  # Short
            if leg.contract.option_type == OptionType.PUT:
                short_put = leg.contract.strike
            else:
                short_call = leg.contract.strike
    
    return (short_put, short_call)


def _is_call_side(structure: OptionStructure) -> bool:
    """Determine if the structure is on the call side."""
    if not structure.legs:
        return True
    
    for leg in structure.legs:
        if leg.quantity < 0:  # Short leg determines side
            return leg.contract.option_type == OptionType.CALL
    
    return structure.legs[0].contract.option_type == OptionType.CALL


def _calculate_stress_scenarios(
    structure: OptionStructure,
    spot: float,
    iv: float,
    t: float,
    r: float,
    q: float,
) -> Dict[str, float]:
    """
    Calculate PnL under stress scenarios.
    
    FLAT IV SHIFT: Same IV applied to all legs (not per-leg IV).
    
    Scenarios:
    - Spot ±2%, ±5%
    - IV(flat) ±5 points
    - Combined: spot -5% with IV +5pts (panic)
    """
    entry_value = (structure.entry_credit or 0) * 100  # For credit
    if structure.entry_debit:
        entry_value = -(structure.entry_debit * 100)  # For debit
    
    scenarios = {}
    
    # Spot stress only
    spot_moves = [
        ("spot_+2%", 1.02),
        ("spot_-2%", 0.98),
        ("spot_+5%", 1.05),
        ("spot_-5%", 0.95),
    ]
    
    for name, mult in spot_moves:
        stressed_spot = spot * mult
        pnl = _estimate_structure_value(structure, stressed_spot, iv, t, r, q)
        scenarios[name] = pnl - entry_value
    
    # IV stress only (FLAT shift - same for all legs)
    iv_shifts = [
        ("iv(flat)_+5pts", 0.05),
        ("iv(flat)_-5pts", -0.05),
    ]
    
    for name, shift in iv_shifts:
        stressed_iv = max(0.01, iv + shift)
        pnl = _estimate_structure_value(structure, spot, stressed_iv, t, r, q)
        scenarios[name] = pnl - entry_value
    
    # Combined panic: spot -5%, IV(flat) +5pts
    panic_spot = spot * 0.95
    panic_iv = iv + 0.05
    pnl = _estimate_structure_value(structure, panic_spot, panic_iv, t, r, q)
    scenarios["panic_spot-5%_iv(flat)+5pts"] = pnl - entry_value
    
    return scenarios


def _estimate_structure_value(
    structure: OptionStructure,
    spot: float,
    iv: float,
    t: float,
    r: float,
    q: float,
) -> float:
    """Estimate structure value at given spot/IV (flat IV for all legs)."""
    from structures.pricing import bs_price, OptionSide
    
    total = 0
    for leg in structure.legs:
        c = leg.contract
        side = OptionSide.CALL if c.option_type == OptionType.CALL else OptionSide.PUT
        
        price = bs_price(side, spot, c.strike, t, r, iv, q)
        total += price * leg.quantity * 100  # Convert to dollars
    
    return total


def format_probability_metrics(metrics: ProbabilityMetrics) -> str:
    """Format probability metrics for display in report."""
    lines = []
    
    lines.append("### Probability Metrics (Model-Based)")
    lines.append("")
    lines.append(f"⚠️ *{metrics.warning}*")
    lines.append("")
    
    # Core probabilities
    lines.append("**Core Probabilities:**")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Model PoP (expiration) | {metrics.pop_expiry:.1%} |")
    lines.append(f"| P(Inside Short Strikes) | {metrics.p_inside_short_strikes:.1%} |")
    lines.append(f"| P(Between Breakevens) | {metrics.p_between_breakevens:.1%} |")
    lines.append(f"| Breakeven Distance | {metrics.breakeven_distance_pct:.1f}% from spot |")
    lines.append("")
    
    # Expected value
    lines.append("**Expected Value (Binary Approximation - NOT for ranking):**")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| EV at Expiration | ${metrics.expected_pnl_expiry_binary:+.0f} |")
    lines.append(f"| EV per $1 Risk | ${metrics.ev_per_dollar_risk_binary:.3f} |")
    lines.append("")
    
    # Honesty metrics
    lines.append("**Honesty Metrics:**")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Credit / Width | {metrics.credit_to_width_ratio:.1%} |")
    lines.append(f"| Reward / Risk | {metrics.reward_to_risk_ratio:.2f}:1 |")
    lines.append("")
    
    # Stress scenarios
    lines.append("**Stress Scenarios (flat IV shift):**")
    lines.append("| Scenario | PnL |")
    lines.append("|----------|-----|")
    for scenario, pnl in sorted(metrics.stress_scenarios.items()):
        lines.append(f"| {scenario} | ${pnl:+.0f} |")
    lines.append("")
    
    # Assumptions
    lines.append("**Model Assumptions:**")
    a = metrics.assumptions
    lines.append(f"- Spot: ${a.get('spot', 0):,.2f}")
    lines.append(f"- IV: {a.get('iv', 0):.1%}")
    lines.append(f"- Time: {a.get('time_to_expiry_days', 0):.0f} days")
    lines.append(f"- Rate: {a.get('risk_free_rate', 0):.2%}")
    
    return "\n".join(lines)
