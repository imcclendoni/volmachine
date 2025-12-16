"""
Backtest Metrics.

Calculates performance metrics for backtesting:
- Sharpe, Sortino, Calmar ratios
- Win rate, profit factor
- Max drawdown
- Metrics by regime
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass
class PerformanceMetrics:
    """Performance metrics for a strategy."""
    
    # Returns
    total_return: float
    annualized_return: float
    
    # Risk
    volatility: float
    max_drawdown: float
    max_drawdown_duration: int  # Days
    
    # Ratios
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # Trade stats
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    
    # Profit
    gross_profit: float
    gross_loss: float
    profit_factor: float
    
    # Average trade
    avg_win: float
    avg_loss: float
    avg_trade: float
    
    # Expectancy
    expectancy: float


def calculate_returns_metrics(
    daily_returns: list[float],
    risk_free_rate: float = 0.05,
) -> dict:
    """
    Calculate return-based metrics.
    
    Args:
        daily_returns: List of daily returns
        risk_free_rate: Annual risk-free rate
        
    Returns:
        Dictionary of metrics
    """
    if not daily_returns:
        return {}
    
    returns = np.array(daily_returns)
    
    # Total return
    total_return = np.prod(1 + returns) - 1
    
    # Annualized return
    n_days = len(returns)
    annualized_return = (1 + total_return) ** (252 / n_days) - 1 if n_days > 0 else 0
    
    # Volatility
    volatility = np.std(returns) * np.sqrt(252)
    
    # Sharpe ratio
    excess_returns = returns - risk_free_rate / 252
    sharpe = np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252) if np.std(excess_returns) > 0 else 0
    
    # Sortino ratio (downside deviation)
    negative_returns = returns[returns < 0]
    downside_std = np.std(negative_returns) if len(negative_returns) > 0 else 0
    sortino = np.mean(excess_returns) / downside_std * np.sqrt(252) if downside_std > 0 else 0
    
    return {
        'total_return': total_return,
        'annualized_return': annualized_return,
        'volatility': volatility,
        'sharpe_ratio': sharpe,
        'sortino_ratio': sortino,
    }


def calculate_drawdown(equity_curve: list[float]) -> tuple[float, int]:
    """
    Calculate maximum drawdown.
    
    Args:
        equity_curve: List of equity values
        
    Returns:
        Tuple of (max_drawdown, max_duration_days)
    """
    if not equity_curve:
        return 0.0, 0
    
    equity = np.array(equity_curve)
    peak = np.maximum.accumulate(equity)
    drawdown = (equity - peak) / peak
    
    max_dd = np.min(drawdown)
    
    # Calculate max duration
    in_drawdown = drawdown < 0
    max_duration = 0
    current_duration = 0
    
    for dd in in_drawdown:
        if dd:
            current_duration += 1
            max_duration = max(max_duration, current_duration)
        else:
            current_duration = 0
    
    return max_dd, max_duration


def calculate_trade_metrics(trades: list[dict]) -> dict:
    """
    Calculate trade-based metrics.
    
    Args:
        trades: List of trade dicts with 'pnl' key
        
    Returns:
        Dictionary of metrics
    """
    if not trades:
        return {}
    
    pnls = [t.get('pnl', 0) for t in trades]
    
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    
    total_trades = len(pnls)
    winning_trades = len(wins)
    losing_trades = len(losses)
    
    win_rate = winning_trades / total_trades if total_trades > 0 else 0
    
    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0
    
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    
    avg_win = np.mean(wins) if wins else 0
    avg_loss = abs(np.mean(losses)) if losses else 0
    avg_trade = np.mean(pnls) if pnls else 0
    
    # Expectancy = (Win% * Avg Win) - (Loss% * Avg Loss)
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    
    return {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': win_rate,
        'gross_profit': gross_profit,
        'gross_loss': gross_loss,
        'profit_factor': profit_factor,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'avg_trade': avg_trade,
        'expectancy': expectancy,
    }


def calculate_metrics_by_regime(
    trades: list[dict],  # trades with 'pnl' and 'regime' keys
) -> dict:
    """
    Calculate metrics grouped by regime.
    
    Args:
        trades: List of trades with regime info
        
    Returns:
        Dictionary of metrics by regime
    """
    from collections import defaultdict
    
    trades_by_regime = defaultdict(list)
    
    for trade in trades:
        regime = trade.get('regime', 'unknown')
        trades_by_regime[regime].append(trade)
    
    results = {}
    for regime, regime_trades in trades_by_regime.items():
        results[regime] = calculate_trade_metrics(regime_trades)
    
    return results


def create_performance_metrics(
    equity_curve: list[float],
    trades: list[dict],
    risk_free_rate: float = 0.05,
) -> PerformanceMetrics:
    """
    Create complete performance metrics.
    
    Args:
        equity_curve: Daily equity values
        trades: List of trades with 'pnl' key
        risk_free_rate: Annual risk-free rate
        
    Returns:
        PerformanceMetrics
    """
    # Calculate daily returns from equity
    daily_returns = []
    for i in range(1, len(equity_curve)):
        if equity_curve[i-1] > 0:
            ret = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1]
            daily_returns.append(ret)
    
    returns_metrics = calculate_returns_metrics(daily_returns, risk_free_rate)
    max_dd, max_dd_duration = calculate_drawdown(equity_curve)
    trade_metrics = calculate_trade_metrics(trades)
    
    # Calmar ratio
    calmar = returns_metrics.get('annualized_return', 0) / abs(max_dd) if max_dd != 0 else 0
    
    return PerformanceMetrics(
        total_return=returns_metrics.get('total_return', 0),
        annualized_return=returns_metrics.get('annualized_return', 0),
        volatility=returns_metrics.get('volatility', 0),
        max_drawdown=max_dd,
        max_drawdown_duration=max_dd_duration,
        sharpe_ratio=returns_metrics.get('sharpe_ratio', 0),
        sortino_ratio=returns_metrics.get('sortino_ratio', 0),
        calmar_ratio=calmar,
        total_trades=trade_metrics.get('total_trades', 0),
        winning_trades=trade_metrics.get('winning_trades', 0),
        losing_trades=trade_metrics.get('losing_trades', 0),
        win_rate=trade_metrics.get('win_rate', 0),
        gross_profit=trade_metrics.get('gross_profit', 0),
        gross_loss=trade_metrics.get('gross_loss', 0),
        profit_factor=trade_metrics.get('profit_factor', 0),
        avg_win=trade_metrics.get('avg_win', 0),
        avg_loss=trade_metrics.get('avg_loss', 0),
        avg_trade=trade_metrics.get('avg_trade', 0),
        expectancy=trade_metrics.get('expectancy', 0),
    )


def format_metrics_report(metrics: PerformanceMetrics) -> str:
    """Format metrics as a readable report."""
    lines = [
        "=" * 50,
        "PERFORMANCE METRICS",
        "=" * 50,
        "",
        "RETURNS",
        f"  Total Return:       {metrics.total_return:>10.2%}",
        f"  Annualized Return:  {metrics.annualized_return:>10.2%}",
        f"  Volatility (Ann):   {metrics.volatility:>10.2%}",
        "",
        "RISK",
        f"  Max Drawdown:       {metrics.max_drawdown:>10.2%}",
        f"  Max DD Duration:    {metrics.max_drawdown_duration:>10} days",
        "",
        "RISK-ADJUSTED",
        f"  Sharpe Ratio:       {metrics.sharpe_ratio:>10.2f}",
        f"  Sortino Ratio:      {metrics.sortino_ratio:>10.2f}",
        f"  Calmar Ratio:       {metrics.calmar_ratio:>10.2f}",
        "",
        "TRADES",
        f"  Total Trades:       {metrics.total_trades:>10}",
        f"  Win Rate:           {metrics.win_rate:>10.1%}",
        f"  Profit Factor:      {metrics.profit_factor:>10.2f}",
        "",
        f"  Avg Win:            ${metrics.avg_win:>9.2f}",
        f"  Avg Loss:           ${metrics.avg_loss:>9.2f}",
        f"  Avg Trade:          ${metrics.avg_trade:>9.2f}",
        "",
        f"  Expectancy:         ${metrics.expectancy:>9.2f}",
        "=" * 50,
    ]
    return "\n".join(lines)
