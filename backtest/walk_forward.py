"""
Walk-Forward Evaluation.

Implements walk-forward testing with in-sample/out-of-sample separation.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Callable
import copy

from backtest.metrics import calculate_trade_metrics, calculate_metrics_by_regime


@dataclass
class WalkForwardConfig:
    """Configuration for walk-forward testing."""
    
    # Window sizes
    in_sample_days: int = 180  # 6 months in-sample
    out_of_sample_days: int = 30  # 1 month out-of-sample
    
    # Step size
    step_days: int = 30  # Move forward 1 month each step
    
    # Minimum trades
    min_trades_in_sample: int = 10


@dataclass
class WalkForwardPeriod:
    """A single walk-forward period."""
    
    # Dates
    is_start: date
    is_end: date
    oos_start: date
    oos_end: date
    
    # Results
    is_trades: list
    oos_trades: list
    
    # Metrics
    is_metrics: dict
    oos_metrics: dict


@dataclass
class WalkForwardResult:
    """Complete walk-forward test result."""
    
    periods: list[WalkForwardPeriod]
    
    # Aggregate OOS performance
    total_oos_trades: int
    total_oos_pnl: float
    oos_win_rate: float
    oos_profit_factor: float
    
    # In-sample vs out-of-sample comparison
    is_vs_oos_correlation: float
    
    # By regime
    oos_by_regime: dict


def run_walk_forward(
    trades: list[dict],  # trades with 'date', 'pnl', 'regime'
    config: Optional[WalkForwardConfig] = None,
) -> WalkForwardResult:
    """
    Run walk-forward evaluation.
    
    Args:
        trades: List of historical trades
        config: Walk-forward configuration
        
    Returns:
        WalkForwardResult
    """
    if config is None:
        config = WalkForwardConfig()
    
    if not trades:
        return WalkForwardResult(
            periods=[],
            total_oos_trades=0,
            total_oos_pnl=0,
            oos_win_rate=0,
            oos_profit_factor=0,
            is_vs_oos_correlation=0,
            oos_by_regime={},
        )
    
    # Sort trades by date
    sorted_trades = sorted(trades, key=lambda t: t.get('date', date.min))
    
    # Find date range
    start_date = sorted_trades[0].get('date', date.today())
    end_date = sorted_trades[-1].get('date', date.today())
    
    periods = []
    all_oos_trades = []
    
    # Walk forward
    current_start = start_date
    
    while True:
        is_end = current_start + timedelta(days=config.in_sample_days)
        oos_start = is_end + timedelta(days=1)
        oos_end = oos_start + timedelta(days=config.out_of_sample_days)
        
        if oos_end > end_date:
            break
        
        # Get trades for each period
        is_trades = [
            t for t in sorted_trades
            if current_start <= t.get('date', date.min) <= is_end
        ]
        
        oos_trades = [
            t for t in sorted_trades
            if oos_start <= t.get('date', date.min) <= oos_end
        ]
        
        if len(is_trades) >= config.min_trades_in_sample:
            # Calculate metrics
            is_metrics = calculate_trade_metrics(is_trades)
            oos_metrics = calculate_trade_metrics(oos_trades)
            
            period = WalkForwardPeriod(
                is_start=current_start,
                is_end=is_end,
                oos_start=oos_start,
                oos_end=oos_end,
                is_trades=is_trades,
                oos_trades=oos_trades,
                is_metrics=is_metrics,
                oos_metrics=oos_metrics,
            )
            
            periods.append(period)
            all_oos_trades.extend(oos_trades)
        
        # Step forward
        current_start += timedelta(days=config.step_days)
    
    # Calculate aggregate OOS metrics
    agg_oos = calculate_trade_metrics(all_oos_trades)
    
    # Calculate IS vs OOS correlation
    # Compare win rates between IS and OOS periods
    is_win_rates = [p.is_metrics.get('win_rate', 0) for p in periods]
    oos_win_rates = [p.oos_metrics.get('win_rate', 0) for p in periods]
    
    correlation = 0
    if len(is_win_rates) > 1:
        import numpy as np
        if np.std(is_win_rates) > 0 and np.std(oos_win_rates) > 0:
            correlation = np.corrcoef(is_win_rates, oos_win_rates)[0, 1]
    
    # OOS by regime
    oos_by_regime = calculate_metrics_by_regime(all_oos_trades)
    
    return WalkForwardResult(
        periods=periods,
        total_oos_trades=agg_oos.get('total_trades', 0),
        total_oos_pnl=sum(t.get('pnl', 0) for t in all_oos_trades),
        oos_win_rate=agg_oos.get('win_rate', 0),
        oos_profit_factor=agg_oos.get('profit_factor', 0),
        is_vs_oos_correlation=correlation if not np.isnan(correlation) else 0,
        oos_by_regime=oos_by_regime,
    )


def format_walk_forward_report(result: WalkForwardResult) -> str:
    """Format walk-forward result as a report."""
    lines = [
        "=" * 60,
        "WALK-FORWARD EVALUATION",
        "=" * 60,
        "",
        f"Total Periods: {len(result.periods)}",
        "",
        "OUT-OF-SAMPLE AGGREGATE",
        f"  Total Trades:    {result.total_oos_trades}",
        f"  Total P&L:       ${result.total_oos_pnl:.2f}",
        f"  Win Rate:        {result.oos_win_rate:.1%}",
        f"  Profit Factor:   {result.oos_profit_factor:.2f}",
        "",
        "VALIDATION",
        f"  IS vs OOS Correlation: {result.is_vs_oos_correlation:.2f}",
        "",
    ]
    
    if result.oos_by_regime:
        lines.append("OUT-OF-SAMPLE BY REGIME")
        for regime, metrics in result.oos_by_regime.items():
            lines.append(f"  {regime}:")
            lines.append(f"    Trades: {metrics.get('total_trades', 0)}")
            lines.append(f"    Win Rate: {metrics.get('win_rate', 0):.1%}")
    
    lines.append("")
    lines.append("=" * 60)
    
    return "\n".join(lines)
