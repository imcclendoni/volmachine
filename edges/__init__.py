"""
Edges package - Edge detection for volatility trading.
"""

from edges.vol_risk_premium import VRPDetector, VRPConfig, VRPMetrics
from edges.term_structure import TermStructureDetector, TermStructureConfig
from edges.skew_extremes import SkewDetector, SkewConfig
from edges.event_vol import EventVolDetector, EventVolConfig, EventInfo, EventCalendar
from edges.gamma_pressure import GammaPressureDetector, GammaConfig


__all__ = [
    # VRP
    'VRPDetector',
    'VRPConfig',
    'VRPMetrics',
    # Term Structure
    'TermStructureDetector',
    'TermStructureConfig',
    # Skew
    'SkewDetector',
    'SkewConfig',
    # Event Vol
    'EventVolDetector',
    'EventVolConfig',
    'EventInfo',
    'EventCalendar',
    # Gamma
    'GammaPressureDetector',
    'GammaConfig',
]
