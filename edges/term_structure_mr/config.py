"""
Configuration for Term Structure Mean-Reversion Edge.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class TermStructureMRConfig:
    """Configuration for term structure mean-reversion edge detection."""
    
    # Target DTE ranges for front/back comparison
    front_dte_range: tuple[int, int] = (20, 35)
    back_dte_range: tuple[int, int] = (60, 90)
    
    # Z-score lookback and threshold
    lookback_days: int = 120
    z_threshold: float = 2.0
    
    # Regime gates
    max_atm_iv_pctl: int = 85
    max_vix: float = 30.0
    
    # Universe: FLAT Tier-1 only (17 symbols)
    # XLV excluded until Phase-1 validated
    enabled_symbols: List[str] = field(default_factory=lambda: [
        "SPY", "QQQ", "IWM", "DIA",
        "XLF", "XLE", "XLK", "XLI", "XLY", "XLP", "XLU",
        "TLT", "IEF",
        "GLD", "SLV", "USO", "EEM"
    ])
    
    # Minimum history required before generating signals
    min_history_days: int = 60
    
    # Width cascade for structure building
    width_cascade: List[int] = field(default_factory=lambda: [5, 10])
    
    # Exit rules
    take_profit_pct: float = 50.0  # 50% of max profit
    time_stop_dte: int = 14
