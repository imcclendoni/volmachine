"""
Structures package - Option structure building and analysis.
"""

from structures.pricing import (
    bs_price,
    bs_greeks,
    BSOutput,
    OptionSide,
    implied_volatility,
    time_to_expiry_years,
    get_risk_free_rate,
    price_option,
    calculate_greeks,
)
from structures.greeks import (
    PositionGreeks,
    calculate_contract_greeks,
    calculate_leg_greeks,
    calculate_structure_greeks,
    update_structure_greeks,
    find_strike_for_delta,
)
from structures.payoff import (
    PayoffResult,
    calculate_payoff_curve,
    calculate_max_loss,
    calculate_max_profit,
    calculate_breakevens,
)
from structures.builders import (
    BuilderConfig,
    build_credit_spread,
    build_debit_spread,
    build_iron_condor,
    build_butterfly,
    build_calendar,
    build_diagonal,
)
from structures.validation import (
    ValidationResult,
    ValidationConfig,
    validate_structure,
    validate_defined_risk,
    validate_liquidity,
    estimate_margin_requirement,
)


__all__ = [
    # Pricing
    'bs_price',
    'bs_greeks',
    'BSOutput',
    'OptionSide',
    'implied_volatility',
    'time_to_expiry_years',
    'get_risk_free_rate',
    'price_option',
    'calculate_greeks',
    # Greeks
    'PositionGreeks',
    'calculate_contract_greeks',
    'calculate_leg_greeks',
    'calculate_structure_greeks',
    'update_structure_greeks',
    'find_strike_for_delta',
    # Payoff
    'PayoffResult',
    'calculate_payoff_curve',
    'calculate_max_loss',
    'calculate_max_profit',
    'calculate_breakevens',
    # Builders
    'BuilderConfig',
    'build_credit_spread',
    'build_debit_spread',
    'build_iron_condor',
    'build_butterfly',
    'build_calendar',
    'build_diagonal',
    # Validation
    'ValidationResult',
    'ValidationConfig',
    'validate_structure',
    'validate_defined_risk',
    'validate_liquidity',
    'estimate_margin_requirement',
]
