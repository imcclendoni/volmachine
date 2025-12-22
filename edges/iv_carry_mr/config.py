"""
IV Carry Mean Reversion Edge - Configuration.

Configurable parameters for the IV Carry MR edge detector.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class IVCarryMRConfig:
    """Configuration for IV Carry Mean Reversion edge."""
    
    # Signal parameters
    lookback_days: int = 120           # Rolling window for z-score
    iv_zscore_threshold: float = 2.0   # Minimum |z| to trigger
    
    # DTE range for structure
    min_dte: int = 30
    max_dte: int = 45
    
    # Regime gates
    rv_iv_max: float = 1.0             # Reject if RV/IV > this
    rv_window: int = 20                # Window for realized vol
    
    # Trend filter
    trend_fast_ma: int = 20            # Fast MA period
    trend_slow_ma: int = 60            # Slow MA period
    
    # Structure
    width_cascade: List[int] = field(default_factory=lambda: [5, 10])
    
    # Exit rules (for backtest)
    take_profit_pct: float = 50.0      # Close at 50% of credit
    time_stop_dte: int = 7             # Close at DTE <= 7
    
    # Universe
    enabled_symbols: List[str] = field(default_factory=lambda: [
        "SPY", "QQQ", "IWM", "DIA",
        "XLF", "XLE", "XLK", "XLI", "XLY", "XLP", "XLU",
        "TLT", "IEF",
        "GLD", "SLV", "USO", "EEM"
    ])
