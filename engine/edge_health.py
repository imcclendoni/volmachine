"""
Edge Health Monitoring.

Tracks edge performance over time to detect degradation.
NOT for optimization - for monitoring edge health and auto-suspension.

Philosophy:
- We do NOT optimize for highest win rate
- We do NOT grid-search parameters
- We DO track whether structural edges persist
- We DO suspend edges when performance degrades
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict
from collections import defaultdict

from data.schemas import EdgeType, RegimeState


class EdgeStatus(str, Enum):
    """Edge health status."""
    ACTIVE = "active"           # Edge is healthy, normal trading
    WATCHLIST = "watchlist"     # Underperforming, monitor closely
    SUSPENDED = "suspended"     # Auto-suspended due to degradation
    MANUAL_HOLD = "manual_hold" # Manually paused by user


@dataclass
class TradeOutcome:
    """Record of a single trade outcome for an edge."""
    trade_id: str
    edge_type: EdgeType
    regime_at_entry: RegimeState
    entry_date: date
    exit_date: Optional[date]
    
    # Sizing
    contracts: int
    max_loss_dollars: float
    
    # Outcome
    pnl_dollars: Optional[float] = None  # None if still open
    is_winner: Optional[bool] = None
    
    # Context
    entry_iv: Optional[float] = None
    exit_iv: Optional[float] = None
    underlying_move_pct: Optional[float] = None


@dataclass
class EdgePerformanceWindow:
    """Performance metrics for a rolling window."""
    window_days: int
    trade_count: int
    
    # Hit rate (NOT "win rate" - just tracking)
    hits: int  # Trades that were profitable
    hit_rate: float
    
    # Expected value metrics
    total_pnl: float
    avg_pnl_per_trade: float
    avg_pnl_per_dollar_risked: float  # EV per $ risked
    
    # Risk metrics
    max_drawdown: float
    largest_loss: float
    largest_win: float
    
    # Distribution
    pnl_std_dev: float
    sharpe_like_ratio: float  # avg_pnl / std_dev


@dataclass
class RegimePerformance:
    """Performance breakdown by regime."""
    regime: RegimeState
    trade_count: int
    hit_rate: float
    avg_pnl: float
    total_pnl: float


@dataclass
class EdgeHealthReport:
    """Complete health report for an edge type."""
    edge_type: EdgeType
    status: EdgeStatus
    status_reason: str
    
    # Current windows
    last_7d: Optional[EdgePerformanceWindow]
    last_30d: Optional[EdgePerformanceWindow]
    last_90d: Optional[EdgePerformanceWindow]
    lifetime: Optional[EdgePerformanceWindow]
    
    # Regime breakdown
    by_regime: Dict[str, RegimePerformance]
    
    # Suspension info
    suspended_since: Optional[date] = None
    suspension_reason: Optional[str] = None
    
    # Warnings
    warnings: List[str] = field(default_factory=list)


class EdgePerformanceTracker:
    """
    Tracks edge performance over time.
    
    Purpose:
    - Monitor if structural edges persist
    - Auto-suspend edges when performance degrades
    - Provide regime-conditional performance summaries
    
    NOT for:
    - Optimizing parameters
    - Grid searching for "best" settings
    - Chasing high win rates
    """
    
    # Suspension thresholds (conservative, not optimized)
    SUSPENSION_THRESHOLDS = {
        "min_trades_for_evaluation": 10,       # Need N trades to evaluate
        "max_consecutive_losses": 5,            # Suspend after N losses in a row
        "min_hit_rate_30d": 0.35,              # Below this = watchlist
        "suspend_hit_rate_30d": 0.25,          # Below this = suspend
        "max_drawdown_pct": 0.50,              # 50% of rolling gains
        "min_ev_per_dollar": -0.10,            # Suspend if losing >10% per $ risked
        "watchlist_ev_per_dollar": 0.0,        # Watchlist if EV is negative
    }
    
    def __init__(self, storage_path: Optional[Path] = None):
        """
        Initialize tracker.
        
        Args:
            storage_path: Path to store trade outcomes (for persistence)
        """
        self.storage_path = storage_path or Path("./logs/edge_health")
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # In-memory storage
        self.outcomes: Dict[EdgeType, List[TradeOutcome]] = defaultdict(list)
        self.edge_status: Dict[EdgeType, EdgeStatus] = {}
        self.suspension_dates: Dict[EdgeType, date] = {}
        self.suspension_reasons: Dict[EdgeType, str] = {}
        
        # Load existing data
        self._load_state()
    
    def record_trade(self, outcome: TradeOutcome) -> None:
        """Record a completed trade outcome."""
        self.outcomes[outcome.edge_type].append(outcome)
        self._save_state()
        
        # Check if we need to update status
        self._evaluate_edge_health(outcome.edge_type)
    
    def record_entry(
        self,
        trade_id: str,
        edge_type: EdgeType,
        regime: RegimeState,
        entry_date: date,
        contracts: int,
        max_loss_dollars: float,
        entry_iv: Optional[float] = None,
    ) -> None:
        """Record a trade entry (outcome TBD)."""
        outcome = TradeOutcome(
            trade_id=trade_id,
            edge_type=edge_type,
            regime_at_entry=regime,
            entry_date=entry_date,
            exit_date=None,
            contracts=contracts,
            max_loss_dollars=max_loss_dollars,
            pnl_dollars=None,
            is_winner=None,
            entry_iv=entry_iv,
        )
        self.outcomes[edge_type].append(outcome)
        self._save_state()
    
    def record_exit(
        self,
        trade_id: str,
        edge_type: EdgeType,
        exit_date: date,
        pnl_dollars: float,
        exit_iv: Optional[float] = None,
        underlying_move_pct: Optional[float] = None,
    ) -> None:
        """Record a trade exit with outcome."""
        for outcome in self.outcomes[edge_type]:
            if outcome.trade_id == trade_id and outcome.exit_date is None:
                outcome.exit_date = exit_date
                outcome.pnl_dollars = pnl_dollars
                outcome.is_winner = pnl_dollars > 0
                outcome.exit_iv = exit_iv
                outcome.underlying_move_pct = underlying_move_pct
                break
        
        self._save_state()
        self._evaluate_edge_health(edge_type)
    
    def is_edge_tradeable(self, edge_type: EdgeType) -> tuple[bool, str]:
        """
        Check if an edge is currently tradeable.
        
        Returns:
            (is_tradeable, reason)
        """
        status = self.edge_status.get(edge_type, EdgeStatus.ACTIVE)
        
        if status == EdgeStatus.ACTIVE:
            return True, "Edge is healthy"
        elif status == EdgeStatus.WATCHLIST:
            return True, "Edge on watchlist - trade with caution"
        elif status == EdgeStatus.SUSPENDED:
            reason = self.suspension_reasons.get(edge_type, "Performance degradation")
            return False, f"Edge suspended: {reason}"
        elif status == EdgeStatus.MANUAL_HOLD:
            return False, "Edge manually paused"
        
        return True, "Unknown status - defaulting to tradeable"
    
    def get_health_report(self, edge_type: EdgeType) -> EdgeHealthReport:
        """Generate a complete health report for an edge."""
        outcomes = self._get_closed_outcomes(edge_type)
        
        # Calculate windows
        today = date.today()
        last_7d = self._calculate_window(outcomes, today - timedelta(days=7), today)
        last_30d = self._calculate_window(outcomes, today - timedelta(days=30), today)
        last_90d = self._calculate_window(outcomes, today - timedelta(days=90), today)
        lifetime = self._calculate_window(outcomes, date(2000, 1, 1), today)
        
        # Calculate by regime
        by_regime = self._calculate_by_regime(outcomes)
        
        # Get current status
        status = self.edge_status.get(edge_type, EdgeStatus.ACTIVE)
        status_reason = self._get_status_reason(edge_type, last_30d)
        
        # Generate warnings
        warnings = self._generate_warnings(edge_type, last_7d, last_30d)
        
        return EdgeHealthReport(
            edge_type=edge_type,
            status=status,
            status_reason=status_reason,
            last_7d=last_7d,
            last_30d=last_30d,
            last_90d=last_90d,
            lifetime=lifetime,
            by_regime={r.regime.value: r for r in by_regime},
            suspended_since=self.suspension_dates.get(edge_type),
            suspension_reason=self.suspension_reasons.get(edge_type),
            warnings=warnings,
        )
    
    def suspend_edge(self, edge_type: EdgeType, reason: str) -> None:
        """Manually or automatically suspend an edge."""
        self.edge_status[edge_type] = EdgeStatus.SUSPENDED
        self.suspension_dates[edge_type] = date.today()
        self.suspension_reasons[edge_type] = reason
        self._save_state()
    
    def reinstate_edge(self, edge_type: EdgeType) -> None:
        """Reinstate a suspended edge."""
        self.edge_status[edge_type] = EdgeStatus.ACTIVE
        if edge_type in self.suspension_dates:
            del self.suspension_dates[edge_type]
        if edge_type in self.suspension_reasons:
            del self.suspension_reasons[edge_type]
        self._save_state()
    
    def _evaluate_edge_health(self, edge_type: EdgeType) -> None:
        """Evaluate edge health and update status."""
        outcomes = self._get_closed_outcomes(edge_type)
        
        # Need minimum trades to evaluate
        if len(outcomes) < self.SUSPENSION_THRESHOLDS["min_trades_for_evaluation"]:
            return
        
        # Calculate 30-day window
        today = date.today()
        window_30d = self._calculate_window(
            outcomes, 
            today - timedelta(days=30), 
            today
        )
        
        if not window_30d:
            return
        
        current_status = self.edge_status.get(edge_type, EdgeStatus.ACTIVE)
        new_status = EdgeStatus.ACTIVE
        reason = ""
        
        # Check consecutive losses
        recent_outcomes = sorted(outcomes, key=lambda x: x.exit_date or date.min, reverse=True)
        consecutive_losses = 0
        for o in recent_outcomes:
            if o.is_winner:
                break
            consecutive_losses += 1
        
        if consecutive_losses >= self.SUSPENSION_THRESHOLDS["max_consecutive_losses"]:
            new_status = EdgeStatus.SUSPENDED
            reason = f"{consecutive_losses} consecutive losses"
        
        # Check hit rate
        elif window_30d.hit_rate < self.SUSPENSION_THRESHOLDS["suspend_hit_rate_30d"]:
            new_status = EdgeStatus.SUSPENDED
            reason = f"Hit rate {window_30d.hit_rate:.0%} below threshold"
        
        elif window_30d.hit_rate < self.SUSPENSION_THRESHOLDS["min_hit_rate_30d"]:
            new_status = EdgeStatus.WATCHLIST
            reason = f"Hit rate {window_30d.hit_rate:.0%} declining"
        
        # Check EV per dollar
        elif window_30d.avg_pnl_per_dollar_risked < self.SUSPENSION_THRESHOLDS["min_ev_per_dollar"]:
            new_status = EdgeStatus.SUSPENDED
            reason = f"EV per $ risked is {window_30d.avg_pnl_per_dollar_risked:.1%}"
        
        elif window_30d.avg_pnl_per_dollar_risked < self.SUSPENSION_THRESHOLDS["watchlist_ev_per_dollar"]:
            new_status = EdgeStatus.WATCHLIST
            reason = f"EV per $ risked is negative at {window_30d.avg_pnl_per_dollar_risked:.1%}"
        
        # Update status if changed
        if new_status != current_status:
            if new_status == EdgeStatus.SUSPENDED:
                self.suspend_edge(edge_type, reason)
            else:
                self.edge_status[edge_type] = new_status
                self._save_state()
    
    def _get_closed_outcomes(self, edge_type: EdgeType) -> List[TradeOutcome]:
        """Get all closed trades for an edge."""
        return [o for o in self.outcomes[edge_type] if o.pnl_dollars is not None]
    
    def _calculate_window(
        self, 
        outcomes: List[TradeOutcome], 
        start_date: date, 
        end_date: date
    ) -> Optional[EdgePerformanceWindow]:
        """Calculate performance metrics for a time window."""
        window_outcomes = [
            o for o in outcomes 
            if o.exit_date and start_date <= o.exit_date <= end_date
        ]
        
        if not window_outcomes:
            return None
        
        # CRITICAL: Sort by exit_date for correct consecutive loss and drawdown computation
        window_outcomes = sorted(window_outcomes, key=lambda x: x.exit_date or date.min)
        
        pnls = [o.pnl_dollars for o in window_outcomes]
        risks = [o.max_loss_dollars for o in window_outcomes]
        
        total_pnl = sum(pnls)
        total_risk = sum(risks)
        hits = sum(1 for p in pnls if p > 0)
        
        # Calculate std dev
        avg_pnl = total_pnl / len(pnls) if pnls else 0
        variance = sum((p - avg_pnl) ** 2 for p in pnls) / len(pnls) if pnls else 0
        std_dev = variance ** 0.5
        
        # Rolling drawdown (computed on chronologically sorted PnLs)
        cumulative = 0
        peak = 0
        max_dd = 0
        for p in pnls:
            cumulative += p
            peak = max(peak, cumulative)
            dd = peak - cumulative
            max_dd = max(max_dd, dd)
        
        return EdgePerformanceWindow(
            window_days=(end_date - start_date).days,
            trade_count=len(window_outcomes),
            hits=hits,
            hit_rate=hits / len(window_outcomes) if window_outcomes else 0,
            total_pnl=total_pnl,
            avg_pnl_per_trade=avg_pnl,
            avg_pnl_per_dollar_risked=total_pnl / total_risk if total_risk > 0 else 0,
            max_drawdown=max_dd,
            largest_loss=min(pnls) if pnls else 0,
            largest_win=max(pnls) if pnls else 0,
            pnl_std_dev=std_dev,
            sharpe_like_ratio=avg_pnl / std_dev if std_dev > 0 else 0,
        )
    
    def _calculate_by_regime(self, outcomes: List[TradeOutcome]) -> List[RegimePerformance]:
        """Calculate performance breakdown by regime."""
        by_regime = defaultdict(list)
        for o in outcomes:
            by_regime[o.regime_at_entry].append(o)
        
        results = []
        for regime, regime_outcomes in by_regime.items():
            pnls = [o.pnl_dollars for o in regime_outcomes]
            hits = sum(1 for p in pnls if p > 0)
            
            results.append(RegimePerformance(
                regime=regime,
                trade_count=len(regime_outcomes),
                hit_rate=hits / len(regime_outcomes) if regime_outcomes else 0,
                avg_pnl=sum(pnls) / len(pnls) if pnls else 0,
                total_pnl=sum(pnls),
            ))
        
        return results
    
    def _get_status_reason(
        self, 
        edge_type: EdgeType, 
        window_30d: Optional[EdgePerformanceWindow]
    ) -> str:
        """Get human-readable status reason."""
        status = self.edge_status.get(edge_type, EdgeStatus.ACTIVE)
        
        if status == EdgeStatus.SUSPENDED:
            return self.suspension_reasons.get(edge_type, "Performance degradation")
        elif status == EdgeStatus.WATCHLIST:
            if window_30d:
                return f"Monitoring: {window_30d.hit_rate:.0%} hit rate, ${window_30d.avg_pnl_per_trade:.0f} avg PnL"
            return "Under observation"
        elif status == EdgeStatus.MANUAL_HOLD:
            return "Manually paused"
        else:
            if window_30d and window_30d.avg_pnl_per_dollar_risked > 0.05:
                return f"Healthy: +{window_30d.avg_pnl_per_dollar_risked:.1%} EV per $ risked"
            return "Active"
    
    def _generate_warnings(
        self,
        edge_type: EdgeType,
        last_7d: Optional[EdgePerformanceWindow],
        last_30d: Optional[EdgePerformanceWindow],
    ) -> List[str]:
        """Generate warning messages."""
        warnings = []
        
        if last_7d and last_7d.trade_count > 0:
            if last_7d.hit_rate < 0.3:
                warnings.append(f"⚠️ Last 7 days: {last_7d.hit_rate:.0%} hit rate ({last_7d.trade_count} trades)")
            if last_7d.total_pnl < 0:
                warnings.append(f"⚠️ Last 7 days: ${last_7d.total_pnl:.0f} total PnL")
        
        if last_30d and last_30d.trade_count > 0:
            if last_30d.avg_pnl_per_dollar_risked < 0:
                warnings.append(f"⚠️ Negative EV: {last_30d.avg_pnl_per_dollar_risked:.1%} per $ risked")
        
        return warnings
    
    def _save_state(self) -> None:
        """Persist state to disk."""
        # Save outcomes
        for edge_type, outcomes in self.outcomes.items():
            path = self.storage_path / f"{edge_type.value}_outcomes.jsonl"
            with open(path, "w") as f:
                for o in outcomes:
                    data = {
                        "trade_id": o.trade_id,
                        "edge_type": o.edge_type.value,
                        "regime_at_entry": o.regime_at_entry.value,
                        "entry_date": o.entry_date.isoformat() if o.entry_date else None,
                        "exit_date": o.exit_date.isoformat() if o.exit_date else None,
                        "contracts": o.contracts,
                        "max_loss_dollars": o.max_loss_dollars,
                        "pnl_dollars": o.pnl_dollars,
                        "is_winner": o.is_winner,
                    }
                    f.write(json.dumps(data) + "\n")
        
        # Save status
        status_path = self.storage_path / "edge_status.json"
        status_data = {
            "statuses": {e.value: s.value for e, s in self.edge_status.items()},
            "suspension_dates": {e.value: d.isoformat() for e, d in self.suspension_dates.items()},
            "suspension_reasons": {e.value: r for e, r in self.suspension_reasons.items()},
        }
        with open(status_path, "w") as f:
            json.dump(status_data, f, indent=2)
    
    def _load_state(self) -> None:
        """Load state from disk."""
        # Load status
        status_path = self.storage_path / "edge_status.json"
        if status_path.exists():
            with open(status_path) as f:
                data = json.load(f)
                for edge_str, status_str in data.get("statuses", {}).items():
                    try:
                        edge = EdgeType(edge_str)
                        self.edge_status[edge] = EdgeStatus(status_str)
                    except ValueError:
                        pass
                for edge_str, date_str in data.get("suspension_dates", {}).items():
                    try:
                        edge = EdgeType(edge_str)
                        self.suspension_dates[edge] = date.fromisoformat(date_str)
                    except ValueError:
                        pass
                self.suspension_reasons = {
                    EdgeType(k): v 
                    for k, v in data.get("suspension_reasons", {}).items()
                    if k in [e.value for e in EdgeType]
                }
        
        # Load outcomes
        for edge_type in EdgeType:
            path = self.storage_path / f"{edge_type.value}_outcomes.jsonl"
            if path.exists():
                with open(path) as f:
                    for line in f:
                        data = json.loads(line)
                        self.outcomes[edge_type].append(TradeOutcome(
                            trade_id=data["trade_id"],
                            edge_type=EdgeType(data["edge_type"]),
                            regime_at_entry=RegimeState(data["regime_at_entry"]),
                            entry_date=date.fromisoformat(data["entry_date"]) if data.get("entry_date") else None,
                            exit_date=date.fromisoformat(data["exit_date"]) if data.get("exit_date") else None,
                            contracts=data["contracts"],
                            max_loss_dollars=data["max_loss_dollars"],
                            pnl_dollars=data.get("pnl_dollars"),
                            is_winner=data.get("is_winner"),
                        ))


def format_edge_health_report(report: EdgeHealthReport) -> str:
    """Format an edge health report for display."""
    lines = [
        f"## Edge Health: {report.edge_type.value.upper()}",
        "",
        f"**Status: {report.status.value.upper()}**",
        f"*{report.status_reason}*",
        "",
    ]
    
    # Warnings
    if report.warnings:
        for w in report.warnings:
            lines.append(w)
        lines.append("")
    
    # Performance windows
    lines.append("### Performance Summary")
    lines.append("| Window | Trades | Hit Rate | Total PnL | EV/$ Risked |")
    lines.append("|--------|--------|----------|-----------|-------------|")
    
    for name, window in [
        ("7 days", report.last_7d),
        ("30 days", report.last_30d),
        ("90 days", report.last_90d),
        ("Lifetime", report.lifetime),
    ]:
        if window:
            lines.append(
                f"| {name} | {window.trade_count} | {window.hit_rate:.0%} | "
                f"${window.total_pnl:+,.0f} | {window.avg_pnl_per_dollar_risked:+.1%} |"
            )
    
    lines.append("")
    
    # Regime breakdown
    if report.by_regime:
        lines.append("### Performance by Regime")
        lines.append("| Regime | Trades | Hit Rate | Avg PnL |")
        lines.append("|--------|--------|----------|---------|")
        for regime_name, perf in report.by_regime.items():
            lines.append(
                f"| {regime_name} | {perf.trade_count} | {perf.hit_rate:.0%} | ${perf.avg_pnl:+.0f} |"
            )
        lines.append("")
    
    # Suspension info
    if report.status == EdgeStatus.SUSPENDED:
        lines.append(f"⛔ **Suspended since**: {report.suspended_since}")
        lines.append(f"**Reason**: {report.suspension_reason}")
        lines.append("")
    
    return "\n".join(lines)
