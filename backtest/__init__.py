"""
Backtest package - Paper simulation, walk-forward, and metrics.
"""

from backtest.paper_simulator import PaperSimulator, PaperConfig, FillResult
from backtest.metrics import (
    PerformanceMetrics,
    calculate_returns_metrics,
    calculate_drawdown,
    calculate_trade_metrics,
    calculate_metrics_by_regime,
    create_performance_metrics,
    format_metrics_report,
)
from backtest.walk_forward import (
    WalkForwardConfig,
    WalkForwardPeriod,
    WalkForwardResult,
    run_walk_forward,
    format_walk_forward_report,
)
from backtest.event_study import (
    EventStudyResult,
    analyze_iv_events,
    format_event_study_report,
)


__all__ = [
    # Paper
    'PaperSimulator',
    'PaperConfig',
    'FillResult',
    # Metrics
    'PerformanceMetrics',
    'calculate_returns_metrics',
    'calculate_drawdown',
    'calculate_trade_metrics',
    'calculate_metrics_by_regime',
    'create_performance_metrics',
    'format_metrics_report',
    # Walk Forward
    'WalkForwardConfig',
    'WalkForwardPeriod',
    'WalkForwardResult',
    'run_walk_forward',
    'format_walk_forward_report',
    # Event Study
    'EventStudyResult',
    'analyze_iv_events',
    'format_event_study_report',
]
