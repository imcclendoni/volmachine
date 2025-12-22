"""
IV Carry Mean Reversion Edge Module.

Detects premium selling opportunities when ATM IV is significantly
elevated with favorable regime conditions.
"""

from .config import IVCarryMRConfig
from .signal import (
    IVCarryMRSignal,
    IVCarryMRDetector,
    compute_atm_iv_for_date,
    calculate_realized_volatility,
)

__all__ = [
    'IVCarryMRConfig',
    'IVCarryMRSignal',
    'IVCarryMRDetector',
    'compute_atm_iv_for_date',
    'calculate_realized_volatility',
]
