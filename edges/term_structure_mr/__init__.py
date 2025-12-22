"""
Term Structure Mean-Reversion Edge Module.

Detects front-vs-back IV dislocation for mean-reversion trades.
"""

from edges.term_structure_mr.config import TermStructureMRConfig
from edges.term_structure_mr.signal import (
    TermStructureMRSignal,
    TermStructureMRDetector,
    compute_atm_iv_for_expiry,
)

__all__ = [
    'TermStructureMRConfig',
    'TermStructureMRSignal',
    'TermStructureMRDetector',
    'compute_atm_iv_for_expiry',
]
