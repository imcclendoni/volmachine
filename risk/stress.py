"""
Stress Testing.

Scenarios for testing portfolio resilience:
- Underlying price gaps
- IV shifts
- Liquidity shocks (spread widening)
"""

from dataclasses import dataclass
from typing import Optional

from data.schemas import OptionStructure, Position
from structures.payoff import (
    calculate_structure_value_before_expiration,
    calculate_leg_value_before_expiration,
)
from datetime import date


@dataclass
class StressConfig:
    """Configuration for stress testing."""
    
    # Gap scenarios
    gap_scenarios: list[float] = None  # [0.02, 0.05] = ±2%, ±5%
    
    # IV shift scenarios
    iv_shift_points: float = 5.0  # ±5 vol points
    
    # Spread widening
    spread_widen_factor: float = 2.0  # Bid-ask doubles
    
    # Combined scenarios
    run_combined: bool = True
    
    def __post_init__(self):
        if self.gap_scenarios is None:
            self.gap_scenarios = [0.02, 0.05]


@dataclass
class ScenarioResult:
    """Result of a stress scenario."""
    
    scenario_name: str
    underlying_change: float
    iv_change: float
    
    # Before stress
    original_value: float
    
    # After stress
    stressed_value: float
    pnl_impact: float
    pnl_impact_pct: float
    
    # Risk assessment
    max_loss_exceeded: bool = False
    notes: str = ""


@dataclass
class StressTestResult:
    """Complete stress test result."""
    
    # Individual scenarios
    scenarios: list[ScenarioResult]
    
    # Worst case
    worst_case_pnl: float
    worst_case_scenario: str
    
    # Summary
    passes_stress_test: bool
    concerns: list[str]


def stress_position(
    position: Position,
    underlying_price: float,
    price_shock_pct: float,
    iv_shock_points: float,
    as_of: Optional[date] = None,
) -> ScenarioResult:
    """
    Apply stress scenario to a position.
    
    Args:
        position: Position to stress
        underlying_price: Current underlying price
        price_shock_pct: Price change (e.g., -0.05 for -5%)
        iv_shock_points: IV change in points (e.g., 5 for +5 vols)
        as_of: As-of date
        
    Returns:
        ScenarioResult
    """
    if as_of is None:
        as_of = date.today()
    
    structure = position.structure
    
    # Original value (approximate)
    original_iv = 0.20  # Default
    for leg in structure.legs:
        if leg.contract.iv:
            original_iv = leg.contract.iv
            break
    
    original_value = 0
    for leg in structure.legs:
        iv = leg.contract.iv or original_iv
        original_value += calculate_leg_value_before_expiration(
            leg, underlying_price, iv, as_of
        )
    
    # Stressed values
    new_price = underlying_price * (1 + price_shock_pct)
    new_iv = original_iv + iv_shock_points / 100  # Convert points to decimal
    new_iv = max(new_iv, 0.01)  # Floor IV
    
    stressed_value = 0
    for leg in structure.legs:
        stressed_value += calculate_leg_value_before_expiration(
            leg, new_price, new_iv, as_of
        )
    
    # Adjust for position size
    original_value *= position.contracts
    stressed_value *= position.contracts
    
    pnl_impact = stressed_value - original_value
    pnl_impact_pct = pnl_impact / abs(original_value) * 100 if original_value != 0 else 0
    
    # Check if max loss exceeded
    max_loss = position.entry_max_loss * position.contracts * 100
    max_loss_exceeded = -pnl_impact > max_loss
    
    scenario_name = f"Price {price_shock_pct:+.1%}, IV {iv_shock_points:+.0f}pts"
    
    return ScenarioResult(
        scenario_name=scenario_name,
        underlying_change=price_shock_pct,
        iv_change=iv_shock_points / 100,
        original_value=original_value,
        stressed_value=stressed_value,
        pnl_impact=pnl_impact,
        pnl_impact_pct=pnl_impact_pct,
        max_loss_exceeded=max_loss_exceeded,
        notes=f"Max loss breach" if max_loss_exceeded else "",
    )


def stress_structure(
    structure: OptionStructure,
    underlying_price: float,
    price_shock_pct: float,
    iv_shock_points: float,
    contracts: int = 1,
    as_of: Optional[date] = None,
) -> ScenarioResult:
    """
    Apply stress scenario to a structure (not yet a position).
    
    Args:
        structure: Structure to stress
        underlying_price: Current underlying price
        price_shock_pct: Price change
        iv_shock_points: IV change in points
        contracts: Number of contracts
        as_of: As-of date
        
    Returns:
        ScenarioResult
    """
    if as_of is None:
        as_of = date.today()
    
    # Get original IV
    original_iv = 0.20
    for leg in structure.legs:
        if leg.contract.iv:
            original_iv = leg.contract.iv
            break
    
    # Original value
    original_value = 0
    for leg in structure.legs:
        iv = leg.contract.iv or original_iv
        original_value += calculate_leg_value_before_expiration(
            leg, underlying_price, iv, as_of
        )
    
    # Stressed value
    new_price = underlying_price * (1 + price_shock_pct)
    new_iv = max(original_iv + iv_shock_points / 100, 0.01)
    
    stressed_value = 0
    for leg in structure.legs:
        stressed_value += calculate_leg_value_before_expiration(
            leg, new_price, new_iv, as_of
        )
    
    original_value *= contracts
    stressed_value *= contracts
    
    pnl_impact = stressed_value - original_value
    pnl_impact_pct = pnl_impact / abs(original_value) * 100 if original_value != 0 else 0
    
    max_loss = structure.max_loss * contracts * 100
    max_loss_exceeded = -pnl_impact > max_loss
    
    return ScenarioResult(
        scenario_name=f"Price {price_shock_pct:+.1%}, IV {iv_shock_points:+.0f}pts",
        underlying_change=price_shock_pct,
        iv_change=iv_shock_points / 100,
        original_value=original_value,
        stressed_value=stressed_value,
        pnl_impact=pnl_impact,
        pnl_impact_pct=pnl_impact_pct,
        max_loss_exceeded=max_loss_exceeded,
    )


def run_stress_test(
    positions: list[Position],
    underlying_prices: dict[str, float],  # symbol -> price
    config: Optional[StressConfig] = None,
    as_of: Optional[date] = None,
) -> StressTestResult:
    """
    Run full stress test on a portfolio of positions.
    
    Args:
        positions: List of positions
        underlying_prices: Current prices by symbol
        config: Stress test configuration
        as_of: As-of date
        
    Returns:
        StressTestResult
    """
    if config is None:
        config = StressConfig()
    
    if as_of is None:
        as_of = date.today()
    
    scenarios = []
    concerns = []
    
    # Generate scenario matrix
    for gap in config.gap_scenarios:
        # Price down, IV up (typical crash)
        for pos in positions:
            price = underlying_prices.get(pos.symbol, pos.structure.legs[0].contract.strike)
            result = stress_position(pos, price, -gap, config.iv_shift_points, as_of)
            scenarios.append(result)
            if result.max_loss_exceeded:
                concerns.append(f"{pos.symbol}: Max loss exceeded in {result.scenario_name}")
        
        # Price up
        for pos in positions:
            price = underlying_prices.get(pos.symbol, pos.structure.legs[0].contract.strike)
            result = stress_position(pos, price, gap, -config.iv_shift_points/2, as_of)
            scenarios.append(result)
    
    # IV-only shocks
    for pos in positions:
        price = underlying_prices.get(pos.symbol, pos.structure.legs[0].contract.strike)
        # Vol spike
        result = stress_position(pos, price, 0, config.iv_shift_points * 2, as_of)
        scenarios.append(result)
        # Vol crush
        result = stress_position(pos, price, 0, -config.iv_shift_points, as_of)
        scenarios.append(result)
    
    # Find worst case
    if scenarios:
        worst = min(scenarios, key=lambda s: s.pnl_impact)
        worst_case_pnl = worst.pnl_impact
        worst_case_scenario = worst.scenario_name
    else:
        worst_case_pnl = 0
        worst_case_scenario = "N/A"
    
    passes = len(concerns) == 0
    
    return StressTestResult(
        scenarios=scenarios,
        worst_case_pnl=worst_case_pnl,
        worst_case_scenario=worst_case_scenario,
        passes_stress_test=passes,
        concerns=concerns,
    )


def stress_summary(result: StressTestResult, account_equity: float) -> dict:
    """Create summary of stress test for reporting."""
    return {
        'worst_case_pnl': result.worst_case_pnl,
        'worst_case_pnl_pct': result.worst_case_pnl / account_equity * 100,
        'worst_scenario': result.worst_case_scenario,
        'passes': result.passes_stress_test,
        'num_scenarios': len(result.scenarios),
        'num_concerns': len(result.concerns),
        'concerns': result.concerns,
    }
