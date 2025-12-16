"""
Vol Surface Sanity Module.

Provides simple, consistent vol surface metrics:
- ATM IV by expiry
- 25-delta skew proxy
- Term structure slope

NOT a full surface model - just enough for edge detection.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, List, Dict
import math

from scipy.stats import norm

from data.schemas import OptionChain, OptionType, OptionContract


@dataclass
class VolSurfaceSummary:
    """
    Simple vol surface summary for edge detection.
    
    Not a full surface model - just the key metrics.
    """
    symbol: str
    timestamp: datetime
    underlying_price: float
    
    # ATM IV by expiration (key = expiry date ISO string)
    atm_iv_by_expiry: Dict[str, float]
    
    # Term structure
    front_month_atm_iv: Optional[float] = None
    second_month_atm_iv: Optional[float] = None
    term_slope: Optional[float] = None  # (back - front) / front
    term_structure: str = "flat"  # contango, backwardation, flat
    
    # Skew metrics (25-delta proxy)
    front_month_skew_25d: Optional[float] = None  # put_iv - call_iv
    skew_percentile: Optional[float] = None  # Historical percentile
    
    # Quality
    data_quality: str = "good"  # good, partial, poor
    expirations_with_data: int = 0


def calculate_vol_surface_summary(
    chain: OptionChain,
    as_of: Optional[datetime] = None,
) -> VolSurfaceSummary:
    """
    Calculate a simple vol surface summary from an option chain.
    
    Args:
        chain: The option chain
        as_of: Reference time (defaults to chain timestamp)
        
    Returns:
        VolSurfaceSummary with key metrics
    """
    as_of = as_of or chain.timestamp
    spot = chain.underlying_price
    
    # Get ATM IV for each expiration
    atm_iv_by_expiry = {}
    front_month_iv = None
    second_month_iv = None
    front_month_exp = None
    second_month_exp = None
    
    sorted_expirations = sorted(chain.expirations)
    
    for exp in sorted_expirations:
        atm_strike = chain.get_atm_strike(exp)
        atm_iv = _get_atm_iv(chain, exp, atm_strike)
        
        if atm_iv:
            atm_iv_by_expiry[exp.isoformat()] = atm_iv
            
            # Identify front and second month
            dte = (exp - as_of.date()).days
            if dte >= 7:  # Skip weeklies under 7 DTE
                if front_month_exp is None:
                    front_month_exp = exp
                    front_month_iv = atm_iv
                elif second_month_exp is None:
                    second_month_exp = exp
                    second_month_iv = atm_iv
    
    # Calculate term structure
    term_slope = None
    term_structure = "flat"
    if front_month_iv and second_month_iv:
        term_slope = (second_month_iv - front_month_iv) / front_month_iv
        if term_slope > 0.02:
            term_structure = "contango"
        elif term_slope < -0.02:
            term_structure = "backwardation"
    
    # Calculate 25-delta skew for front month
    front_month_skew = None
    if front_month_exp:
        front_month_skew = _calculate_25d_skew(
            chain, front_month_exp, spot, front_month_iv or 0.20
        )
    
    # Determine data quality
    data_quality = "good"
    if len(atm_iv_by_expiry) < 2:
        data_quality = "poor"
    elif len(atm_iv_by_expiry) < 4:
        data_quality = "partial"
    
    return VolSurfaceSummary(
        symbol=chain.symbol,
        timestamp=as_of,
        underlying_price=spot,
        atm_iv_by_expiry=atm_iv_by_expiry,
        front_month_atm_iv=front_month_iv,
        second_month_atm_iv=second_month_iv,
        term_slope=term_slope,
        term_structure=term_structure,
        front_month_skew_25d=front_month_skew,
        data_quality=data_quality,
        expirations_with_data=len(atm_iv_by_expiry),
    )


def _get_atm_iv(
    chain: OptionChain, 
    expiration: date, 
    atm_strike: float
) -> Optional[float]:
    """Get ATM IV for an expiration."""
    # Try to get both call and put, average them
    call = chain.get_contract(expiration, atm_strike, OptionType.CALL)
    put = chain.get_contract(expiration, atm_strike, OptionType.PUT)
    
    call_iv = call.iv if call and call.iv else None
    put_iv = put.iv if put and put.iv else None
    
    if call_iv and put_iv:
        return (call_iv + put_iv) / 2
    elif call_iv:
        return call_iv
    elif put_iv:
        return put_iv
    
    return None


def _calculate_25d_skew(
    chain: OptionChain,
    expiration: date,
    spot: float,
    atm_iv: float,
) -> Optional[float]:
    """
    Calculate 25-delta skew proxy.
    
    Uses simple delta approximation to find 25-delta strikes,
    then compares put IV to call IV.
    
    Returns:
        put_25d_iv - call_25d_iv (positive = puts expensive)
    """
    # Get contracts for this expiration
    exp_contracts = chain.get_expiration(expiration)
    if not exp_contracts:
        return None
    
    calls = [c for c in exp_contracts if c.option_type == OptionType.CALL]
    puts = [c for c in exp_contracts if c.option_type == OptionType.PUT]
    
    if not calls or not puts:
        return None
    
    # Find 25-delta call (OTM call with delta ~0.25)
    # Approximate: OTM call delta decreases as strike increases
    call_25d = _find_delta_strike(calls, spot, 0.25, atm_iv, is_call=True)
    
    # Find 25-delta put (OTM put with delta ~-0.25)
    put_25d = _find_delta_strike(puts, spot, -0.25, atm_iv, is_call=False)
    
    if call_25d and put_25d and call_25d.iv and put_25d.iv:
        return put_25d.iv - call_25d.iv
    
    return None


def _find_delta_strike(
    contracts: List[OptionContract],
    spot: float,
    target_delta: float,
    atm_iv: float,
    is_call: bool,
) -> Optional[OptionContract]:
    """Find contract closest to target delta."""
    best_contract = None
    best_delta_diff = float('inf')
    
    for c in contracts:
        if not c.iv:
            continue
        
        # Calculate approximate delta
        t = max((c.expiration - date.today()).days / 365, 0.01)
        d1 = (math.log(spot / c.strike) + 0.5 * c.iv**2 * t) / (c.iv * math.sqrt(t))
        
        if is_call:
            delta = norm.cdf(d1)
        else:
            delta = norm.cdf(d1) - 1
        
        delta_diff = abs(delta - target_delta)
        if delta_diff < best_delta_diff:
            best_delta_diff = delta_diff
            best_contract = c
    
    return best_contract


def format_vol_surface_summary(summary: VolSurfaceSummary) -> str:
    """Format vol surface summary for display."""
    lines = [
        f"## Vol Surface: {summary.symbol}",
        f"**Data Quality: {summary.data_quality.upper()}** ({summary.expirations_with_data} expirations)",
        "",
    ]
    
    # Term structure
    lines.append("### Term Structure")
    lines.append(f"- **Front Month ATM IV**: {summary.front_month_atm_iv:.1%}" if summary.front_month_atm_iv else "- Front Month: N/A")
    lines.append(f"- **Second Month ATM IV**: {summary.second_month_atm_iv:.1%}" if summary.second_month_atm_iv else "- Second Month: N/A")
    lines.append(f"- **Structure**: {summary.term_structure.upper()}")
    if summary.term_slope:
        lines.append(f"- **Slope**: {summary.term_slope:+.1%}")
    lines.append("")
    
    # Skew
    lines.append("### Skew")
    if summary.front_month_skew_25d:
        lines.append(f"- **25-Delta Skew**: {summary.front_month_skew_25d:.1%}")
        if summary.front_month_skew_25d > 0.05:
            lines.append("- ⚠️ Puts are significantly expensive (fear)")
        elif summary.front_month_skew_25d < -0.02:
            lines.append("- ⚠️ Calls are expensive (greed)")
    else:
        lines.append("- 25-Delta Skew: N/A")
    lines.append("")
    
    # ATM IV curve
    lines.append("### ATM IV Curve")
    lines.append("| Expiry | ATM IV |")
    lines.append("|--------|--------|")
    for exp_str, iv in sorted(summary.atm_iv_by_expiry.items())[:6]:
        lines.append(f"| {exp_str} | {iv:.1%} |")
    
    return "\n".join(lines)
