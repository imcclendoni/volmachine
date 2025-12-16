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
from typing import Optional, List, Dict

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
    """
    
    # Core probabilities
    pop_expiry: float           # Model PoP (expiration) - P(profit at expiry)
    p_otm_short_strike: float   # P(short strike finishes OTM)
    
    # Expected value
    expected_pnl_expiry: float  # EV at expiration (dollars) - BINARY APPROXIMATION
    
    # Distance metrics
    breakeven_distance_pct: float  # Distance from spot to breakeven (%)
    
    # Honesty metrics (keep you honest)
    credit_to_width_ratio: float   # credit / width
    reward_to_risk_ratio: float    # max_profit / max_loss
    ev_per_dollar_risk: float      # EV / max_loss
    
    # Stress scenarios
    stress_scenarios: Dict[str, float]  # scenario -> PnL
    
    # Assumptions used (for transparency)
    assumptions: Dict[str, float]
    
    # Warning
    warning: str = (
        "Model PoP assumes lognormal distribution at expiration. "
        "Real outcomes depend on exits, gaps, volatility changes, and execution. "
        "EV is a binary approximation (full win or full loss)."
    )


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
    # Store assumptions
    assumptions = {
        "spot": spot,
        "iv": iv,
        "time_to_expiry_days": time_to_expiry * 365,
        "risk_free_rate": risk_free_rate,
        "dividend_yield": dividend_yield,
        "as_of": as_of_date.isoformat() if as_of_date else "unknown",
    }
    
    # Get structure characteristics
    width_points = _get_structure_width(structure)
    max_loss_points = structure.max_loss or 0
    max_profit_points = structure.max_profit or 0
    credit_points = structure.entry_credit or 0
    debit_points = structure.entry_debit or 0
    
    # Width in dollars
    width_dollars = width_points * 100 if width_points else 0
    max_loss_dollars = max_loss_points * 100
    max_profit_dollars = max_profit_points * 100
    
    # Get key strikes
    short_strike = _get_short_strike(structure)
    breakeven = structure.breakevens[0] if structure.breakevens else spot
    
    # Calculate P(OTM) for short strike
    p_otm_short = _prob_otm(
        spot, short_strike, 
        _is_call_side(structure),
        iv, time_to_expiry, 
        risk_free_rate, dividend_yield
    ) if short_strike else 0.5
    
    # Calculate PoP at expiration
    pop_expiry = _calculate_pop_expiry(
        structure, spot, iv, 
        time_to_expiry, risk_free_rate, dividend_yield
    )
    
    # Breakeven distance
    breakeven_distance_pct = abs(spot - breakeven) / spot * 100 if spot > 0 else 0
    
    # Honesty metrics
    credit_to_width = credit_points / width_points if width_points > 0 else 0
    reward_to_risk = max_profit_points / max_loss_points if max_loss_points > 0 else 0
    
    # Expected PnL at expiration (BINARY APPROXIMATION)
    # EV = P(win) * max_profit - P(lose) * max_loss
    # This is simplified - real EV would integrate the payoff function
    expected_pnl = (
        pop_expiry * max_profit_dollars - 
        (1 - pop_expiry) * max_loss_dollars
    )
    
    ev_per_dollar_risk = expected_pnl / max_loss_dollars if max_loss_dollars > 0 else 0
    
    # Stress scenarios
    stress = _calculate_stress_scenarios(
        structure, spot, iv, time_to_expiry, 
        risk_free_rate, dividend_yield
    )
    
    return ProbabilityMetrics(
        pop_expiry=pop_expiry,
        p_otm_short_strike=p_otm_short,
        expected_pnl_expiry=expected_pnl,
        breakeven_distance_pct=breakeven_distance_pct,
        credit_to_width_ratio=credit_to_width,
        reward_to_risk_ratio=reward_to_risk,
        ev_per_dollar_risk=ev_per_dollar_risk,
        stress_scenarios=stress,
        assumptions=assumptions,
    )


def _prob_otm(
    spot: float, 
    strike: float, 
    is_call: bool,
    iv: float, 
    t: float, 
    r: float, 
    q: float
) -> float:
    """
    Calculate probability of finishing OTM.
    
    For calls: P(S_T < K)
    For puts: P(S_T > K)
    """
    if t <= 0:
        if is_call:
            return 1.0 if spot < strike else 0.0
        else:
            return 1.0 if spot > strike else 0.0
    
    if iv <= 0:
        iv = 0.0001
    
    # d2 from Black-Scholes
    d2 = (math.log(spot / strike) + (r - q - 0.5 * iv**2) * t) / (iv * math.sqrt(t))
    
    if is_call:
        # P(S_T < K) = N(-d2)
        return norm.cdf(-d2)
    else:
        # P(S_T > K) = N(d2)
        return norm.cdf(d2)


def _prob_itm(
    spot: float, 
    strike: float, 
    is_call: bool,
    iv: float, 
    t: float, 
    r: float, 
    q: float
) -> float:
    """Probability of finishing ITM."""
    return 1 - _prob_otm(spot, strike, is_call, iv, t, r, q)


def _d2(spot: float, strike: float, iv: float, t: float, r: float, q: float) -> float:
    """Calculate d2 for probability calculations."""
    if t <= 0 or iv <= 0:
        return 0
    return (math.log(spot / strike) + (r - q - 0.5 * iv**2) * t) / (iv * math.sqrt(t))


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
    
    For credit structures: P(S_T stays within profit zone)
    For debit structures: P(S_T moves beyond breakeven)
    
    IRON CONDOR: Uses BOTH breakevens - P(BE_low < S_T < BE_high)
    """
    structure_type = structure.structure_type
    breakevens = structure.breakevens
    
    # IRON CONDOR: profit if spot stays between both breakevens
    if structure_type == StructureType.IRON_CONDOR:
        if len(breakevens) >= 2:
            be_low = min(breakevens)
            be_high = max(breakevens)
            # P(BE_low < S_T < BE_high) = P(S_T < BE_high) - P(S_T < BE_low)
            p_below_high = norm.cdf(_d2(spot, be_high, iv, t, r, q))
            p_below_low = norm.cdf(_d2(spot, be_low, iv, t, r, q))
            return p_below_high - p_below_low
        else:
            # Fallback: use single breakeven
            breakeven = breakevens[0] if breakevens else spot
            return 0.5  # No good estimate without both breakevens
    
    # CREDIT SPREAD: profit if S_T stays OTM of breakeven
    if structure_type == StructureType.CREDIT_SPREAD:
        breakeven = breakevens[0] if breakevens else spot
        if _is_call_side(structure):
            # Call credit spread: profit if S_T < breakeven
            return _prob_otm(spot, breakeven, True, iv, t, r, q)
        else:
            # Put credit spread: profit if S_T > breakeven
            return _prob_otm(spot, breakeven, False, iv, t, r, q)
    
    # DEBIT SPREAD: profit if S_T moves through breakeven
    if structure_type == StructureType.DEBIT_SPREAD:
        breakeven = breakevens[0] if breakevens else spot
        if _is_call_side(structure):
            # Call debit: profit if S_T > breakeven
            return _prob_itm(spot, breakeven, True, iv, t, r, q)
        else:
            # Put debit: profit if S_T < breakeven
            return _prob_itm(spot, breakeven, False, iv, t, r, q)
    
    # BUTTERFLY / IRON BUTTERFLY: profit in narrow range around center
    if structure_type in (StructureType.BUTTERFLY, StructureType.IRON_BUTTERFLY):
        if len(breakevens) >= 2:
            be_low, be_high = min(breakevens), max(breakevens)
            p_below_high = norm.cdf(_d2(spot, be_high, iv, t, r, q))
            p_below_low = norm.cdf(_d2(spot, be_low, iv, t, r, q))
            return p_below_high - p_below_low
        return 0.3  # Conservative default for butterflies
    
    # Default: 50%
    return 0.5


def _get_structure_width(structure: OptionStructure) -> float:
    """Get the width of the structure in points."""
    if not structure.legs or len(structure.legs) < 2:
        return 0
    
    strikes = sorted([leg.contract.strike for leg in structure.legs])
    
    if len(strikes) == 2:
        return strikes[1] - strikes[0]
    elif len(strikes) == 4:  # Iron condor
        # Use the wider wing
        put_width = strikes[1] - strikes[0]
        call_width = strikes[3] - strikes[2]
        return max(put_width, call_width)
    
    return strikes[-1] - strikes[0] if strikes else 0


def _get_short_strike(structure: OptionStructure) -> Optional[float]:
    """Get the primary short strike."""
    if not structure.legs:
        return None
    
    short_legs = [leg for leg in structure.legs if leg.quantity < 0]
    if short_legs:
        return short_legs[0].contract.strike
    return None


def _is_call_side(structure: OptionStructure) -> bool:
    """Determine if the structure is on the call side."""
    if not structure.legs:
        return True
    
    for leg in structure.legs:
        if leg.quantity < 0:  # Short leg determines side
            return leg.contract.option_type == OptionType.CALL
    
    # Default based on first leg
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
    
    Scenarios:
    - Spot ±2%, ±5%
    - IV ±5 points
    - Combined: spot -5% with IV +5pts (panic)
    """
    max_loss_dollars = (structure.max_loss or 0) * 100
    entry_value = (structure.entry_credit or 0) * 100  # For credit
    if structure.entry_debit:
        entry_value = -(structure.entry_debit * 100)  # For debit
    
    scenarios = {}
    
    # Define stress points
    spot_moves = [
        ("spot_+2%", 1.02),
        ("spot_-2%", 0.98),
        ("spot_+5%", 1.05),
        ("spot_-5%", 0.95),
    ]
    
    iv_shifts = [
        ("iv_+5pts", 0.05),
        ("iv_-5pts", -0.05),
    ]
    
    # Spot stress only
    for name, mult in spot_moves:
        stressed_spot = spot * mult
        pnl = _estimate_structure_value(structure, stressed_spot, iv, t, r, q)
        scenarios[name] = pnl - entry_value
    
    # IV stress only
    for name, shift in iv_shifts:
        stressed_iv = max(0.01, iv + shift)
        pnl = _estimate_structure_value(structure, spot, stressed_iv, t, r, q)
        scenarios[name] = pnl - entry_value
    
    # Combined panic: spot -5%, IV +5pts
    panic_spot = spot * 0.95
    panic_iv = iv + 0.05
    pnl = _estimate_structure_value(structure, panic_spot, panic_iv, t, r, q)
    scenarios["panic_spot-5%_iv+5pts"] = pnl - entry_value
    
    return scenarios


def _estimate_structure_value(
    structure: OptionStructure,
    spot: float,
    iv: float,
    t: float,
    r: float,
    q: float,
) -> float:
    """Estimate structure value at given spot/IV."""
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
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Model PoP (expiration) | {metrics.pop_expiry:.1%} |")
    lines.append(f"| P(Short Strike OTM) | {metrics.p_otm_short_strike:.1%} |")
    lines.append(f"| Breakeven Distance | {metrics.breakeven_distance_pct:.1f}% from spot |")
    lines.append("")
    
    # Expected value
    lines.append("**Expected Value (Binary Approximation):**")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| EV at Expiration | ${metrics.expected_pnl_expiry:+.0f} |")
    lines.append(f"| EV per $1 Risk | ${metrics.ev_per_dollar_risk:.3f} |")
    lines.append("")
    
    # Honesty metrics
    lines.append("**Honesty Metrics (keep you honest):**")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Credit / Width | {metrics.credit_to_width_ratio:.1%} |")
    lines.append(f"| Reward / Risk | {metrics.reward_to_risk_ratio:.2f}:1 |")
    lines.append("")
    
    # Stress scenarios
    lines.append("**Stress Scenarios (PnL change):**")
    lines.append(f"| Scenario | PnL |")
    lines.append(f"|----------|-----|")
    for scenario, pnl in sorted(metrics.stress_scenarios.items()):
        lines.append(f"| {scenario} | ${pnl:+.0f} |")
    lines.append("")
    
    # Assumptions
    lines.append("**Model Assumptions:**")
    a = metrics.assumptions
    lines.append(f"- Spot: ${a.get('spot', 0):,.2f}")
    lines.append(f"- IV: {a.get('iv', 0):.1%}")
    lines.append(f"- Time to Expiry: {a.get('time_to_expiry_days', 0):.0f} days")
    lines.append(f"- Risk-Free Rate: {a.get('risk_free_rate', 0):.2%}")
    lines.append(f"- Dividend Yield: {a.get('dividend_yield', 0):.2%}")
    lines.append(f"- As-Of Date: {a.get('as_of', 'N/A')}")
    
    return "\n".join(lines)
