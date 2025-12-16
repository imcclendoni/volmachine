"""
Event Study Analysis.

Analyzes historical events for edge validation.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import numpy as np


@dataclass
class EventStudyResult:
    """Result of event study analysis."""
    
    event_type: str
    num_events: int
    
    # IV before/after
    avg_iv_before: float
    avg_iv_after: float
    avg_iv_crush: float  # pct reduction
    
    # Returns around event
    avg_return_1d: float  # 1 day after
    avg_return_5d: float  # 5 days after
    avg_abs_move: float   # Absolute move
    
    # Strategy performance
    avg_trade_pnl: float
    win_rate: float
    
    # Consistency
    crush_occurred_pct: float  # % of events with IV crush


def analyze_iv_events(
    events: list[dict],  # {'date': date, 'iv_before': float, 'iv_after': float}
    event_type: str = "unknown",
) -> EventStudyResult:
    """
    Analyze IV behavior around events.
    
    Args:
        events: List of event data
        event_type: Type of event for labeling
        
    Returns:
        EventStudyResult
    """
    if not events:
        return EventStudyResult(
            event_type=event_type,
            num_events=0,
            avg_iv_before=0,
            avg_iv_after=0,
            avg_iv_crush=0,
            avg_return_1d=0,
            avg_return_5d=0,
            avg_abs_move=0,
            avg_trade_pnl=0,
            win_rate=0,
            crush_occurred_pct=0,
        )
    
    iv_befores = [e.get('iv_before', 0) for e in events if e.get('iv_before')]
    iv_afters = [e.get('iv_after', 0) for e in events if e.get('iv_after')]
    
    avg_iv_before = np.mean(iv_befores) if iv_befores else 0
    avg_iv_after = np.mean(iv_afters) if iv_afters else 0
    
    # IV crush
    crushes = []
    crush_count = 0
    for e in events:
        before = e.get('iv_before', 0)
        after = e.get('iv_after', 0)
        if before > 0 and after > 0:
            crush = (before - after) / before
            crushes.append(crush)
            if crush > 0:
                crush_count += 1
    
    avg_iv_crush = np.mean(crushes) if crushes else 0
    crush_occurred_pct = crush_count / len(crushes) * 100 if crushes else 0
    
    # Returns
    returns_1d = [e.get('return_1d', 0) for e in events if 'return_1d' in e]
    returns_5d = [e.get('return_5d', 0) for e in events if 'return_5d' in e]
    abs_moves = [abs(r) for r in returns_1d]
    
    # Trade results
    pnls = [e.get('trade_pnl', 0) for e in events if 'trade_pnl' in e]
    wins = sum(1 for p in pnls if p > 0)
    
    return EventStudyResult(
        event_type=event_type,
        num_events=len(events),
        avg_iv_before=avg_iv_before,
        avg_iv_after=avg_iv_after,
        avg_iv_crush=avg_iv_crush,
        avg_return_1d=np.mean(returns_1d) if returns_1d else 0,
        avg_return_5d=np.mean(returns_5d) if returns_5d else 0,
        avg_abs_move=np.mean(abs_moves) if abs_moves else 0,
        avg_trade_pnl=np.mean(pnls) if pnls else 0,
        win_rate=wins / len(pnls) if pnls else 0,
        crush_occurred_pct=crush_occurred_pct,
    )


def format_event_study_report(result: EventStudyResult) -> str:
    """Format event study as a report."""
    lines = [
        f"EVENT STUDY: {result.event_type}",
        "=" * 40,
        f"Events Analyzed: {result.num_events}",
        "",
        "IMPLIED VOLATILITY",
        f"  Avg IV Before:    {result.avg_iv_before:.1%}",
        f"  Avg IV After:     {result.avg_iv_after:.1%}",
        f"  Avg IV Crush:     {result.avg_iv_crush:.1%}",
        f"  Crush Occurred:   {result.crush_occurred_pct:.0f}% of events",
        "",
        "PRICE MOVES",
        f"  Avg 1-Day Return: {result.avg_return_1d:.2%}",
        f"  Avg 5-Day Return: {result.avg_return_5d:.2%}",
        f"  Avg Absolute Move: {result.avg_abs_move:.2%}",
        "",
        "STRATEGY RESULTS",
        f"  Avg Trade P&L:    ${result.avg_trade_pnl:.2f}",
        f"  Win Rate:         {result.win_rate:.1%}",
    ]
    return "\n".join(lines)
