"""
FLAT Call Debit Edge Module (Edge Module #2)

Detects FLAT skew signals and maps them to call debit spreads.

FROZEN LOGIC - Do not modify signal generation.

Thesis: FLAT skew (≤10th percentile) indicates bullish continuation.
        Forward returns are +1.23% at 20D with 67.6% win rate on call debits.

Research Validation:
- 111 signals (2022-2024)
- PF 4.82, Expectancy $156
- Consistent across all years and IV buckets
- All 4 realism audits passed

Excluded: EEM (strike increment mismatch - $2.50 vs $5.00)
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, List, Dict, Any

from data.schemas import EdgeSignal, EdgeType, TradeDirection


@dataclass
class FlatCallDebitConfig:
    """Configuration for FLAT call debit edge module."""
    
    # Enabled symbols (EEM excluded due to strike increment issues)
    enabled_symbols: List[str] = field(default_factory=lambda: [
        "SPY", "QQQ", "IWM", "XLF", "GLD", "TLT", "DIA"
    ])
    
    # Signal thresholds (FROZEN - validated in research)
    min_strength: float = 0.40
    
    # Structure parameters
    default_width: float = 5.0  # $5 width
    min_dte: int = 21
    max_dte: int = 45
    
    # Exit rules (from ablation: 14-day hold optimal)
    holding_period_days: int = 14
    take_profit_pct: Optional[float] = 80.0  # Exit at 80% max profit
    stop_loss_pct: Optional[float] = 50.0     # Exit at 50% of max loss
    time_stop_dte: int = 7                    # Exit at 7 DTE
    
    # Strike increment map (for width normalization)
    strike_increments: Dict[str, float] = field(default_factory=lambda: {
        "SPY": 1.0,
        "QQQ": 1.0,
        "IWM": 1.0,
        "XLF": 0.5,
        "GLD": 1.0,
        "TLT": 1.0,
        "DIA": 1.0,
        "EEM": 0.5,  # Excluded but documented
    })


def is_flat_signal(edge: EdgeSignal) -> bool:
    """
    Check if an edge signal is a FLAT signal.
    
    FROZEN LOGIC - do not modify.
    """
    if edge.edge_type != EdgeType.SKEW_EXTREME:
        return False
    
    # FLAT detection: is_flat == 1.0 OR direction == LONG
    metrics = edge.metrics or {}
    is_flat = metrics.get('is_flat', 0.0) == 1.0
    is_long = edge.direction == TradeDirection.LONG
    
    return is_flat or is_long


def compute_width_dollars(legs: List[Dict]) -> Optional[float]:
    """
    Compute width from legs (source of truth).
    
    Do NOT trust structure['width'] - compute from strikes.
    """
    if len(legs) != 2:
        return None
    
    strikes = [leg.get('strike') for leg in legs if leg.get('strike')]
    if len(strikes) != 2:
        return None
    
    return abs(strikes[0] - strikes[1])


def should_trade_flat(
    edge: EdgeSignal,
    config: Optional[FlatCallDebitConfig] = None,
) -> bool:
    """
    Determine if a FLAT signal should be traded.
    
    Args:
        edge: Edge signal to evaluate
        config: Module configuration
    
    Returns:
        True if signal passes all filters
    """
    if config is None:
        config = FlatCallDebitConfig()
    
    # Check symbol is enabled
    if edge.symbol not in config.enabled_symbols:
        return False
    
    # Check is FLAT signal
    if not is_flat_signal(edge):
        return False
    
    # Check minimum strength
    if edge.strength < config.min_strength:
        return False
    
    return True


def get_call_debit_structure_params(
    edge: EdgeSignal,
    underlying_price: float,
    config: Optional[FlatCallDebitConfig] = None,
) -> Dict[str, Any]:
    """
    Get parameters for call debit spread structure.
    
    Args:
        edge: FLAT edge signal
        underlying_price: Current underlying price
        config: Module configuration
    
    Returns:
        Dict with structure parameters for builder
    """
    if config is None:
        config = FlatCallDebitConfig()
    
    # Get strike increment for symbol
    increment = config.strike_increments.get(edge.symbol, 1.0)
    
    # Calculate ATM strike (rounded to increment)
    atm_strike = round(underlying_price / increment) * increment
    
    # Long call at ATM, short call at ATM + width
    long_strike = atm_strike
    short_strike = atm_strike + config.default_width
    
    return {
        'structure_type': 'call_debit_spread',
        'option_type': 'CALL',
        'long_strike': long_strike,
        'short_strike': short_strike,
        'width_dollars': config.default_width,
        'min_dte': config.min_dte,
        'max_dte': config.max_dte,
    }


# Module metadata
MODULE_INFO = {
    'name': 'FLAT Call Debit',
    'version': '1.0.0',
    'edge_number': 2,
    'status': 'validated',
    'research_date': '2025-12-19',
    'signals_tested': 111,
    'profit_factor': 4.82,
    'expectancy_usd': 155.92,
    'win_rate_pct': 67.6,
    'excluded_symbols': ['EEM'],
    'frozen': True,
}
