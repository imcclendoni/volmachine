"""
Portfolio Risk Engine

Converts trade signals into sized orders with:
- Correlation-aware clustering
- Drawdown kill-switch
- Position and risk caps
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum


class RejectionReason(Enum):
    """Reasons for trade rejection."""
    SYMBOL_COOLDOWN = "symbol_cooldown"
    CLUSTER_COOLDOWN = "cluster_cooldown"
    DRAWDOWN_KILL_SWITCH = "dd_kill_switch"
    MAX_POSITIONS = "max_positions"
    MAX_TOTAL_RISK = "max_total_risk"
    MAX_RISK_PER_TRADE_EXCEEDED = "max_risk_per_trade_exceeded"  # NEW: one-lot exceeds cap
    CLUSTER_POSITION_CAP = "cluster_position_cap"
    CLUSTER_RISK_CAP = "cluster_risk_cap"
    SAME_DAY_CLUSTER_DEDUP = "same_day_cluster_dedup"
    COVERAGE_INVALID = "coverage_invalid"
    UNEXECUTABLE = "unexecutable"


@dataclass
class RiskConfig:
    """Configuration for the risk engine."""
    
    # Equity-based sizing (new)
    initial_equity: float = 10000  # Starting equity for backtests
    risk_per_trade_pct: Optional[float] = None  # e.g., 0.02 for 2% (null = use fixed USD)
    max_total_risk_pct: Optional[float] = None  # e.g., 0.06 for 6% (null = use fixed USD)
    
    # Base sizing (fallback when pct is null)
    risk_per_trade_usd: float = 500
    max_open_positions: int = 3
    max_total_risk_usd: float = 1500
    
    # Correlation clustering
    clusters: Dict[str, List[str]] = field(default_factory=lambda: {
        'equity_etf': ['SPY', 'QQQ', 'IWM']
    })
    max_cluster_risk_usd: float = 750
    max_cluster_positions: int = 1
    cluster_dedup_mode: str = 'best_edge'  # 'best_edge' or 'reduce_size'
    cluster_risk_multiplier: float = 0.5  # Only used if cluster_dedup_mode='reduce_size'
    
    # Drawdown protection
    dd_kill_pct: float = 0.10  # 10% from peak triggers kill-switch
    
    # Cooldowns
    symbol_cooldown_after_sl_days: int = 10
    cluster_cooldown_after_sl_days: int = 5


@dataclass
class Position:
    """Represents an open position."""
    symbol: str
    cluster: Optional[str]
    entry_date: date
    exit_date: date  # Expected exit
    risk_usd: float
    contracts: int


@dataclass
class TradeCandidate:
    """A candidate trade to evaluate."""
    symbol: str
    signal_date: date
    execution_date: date
    edge_strength: float
    edge_type: str
    structure_type: str
    max_loss_usd: float
    credit_usd: Optional[float] = None
    regime: Optional[str] = None
    expiry: Optional[date] = None


@dataclass
class ApprovedTrade:
    """An approved trade with final sizing."""
    candidate: TradeCandidate
    contracts: int
    risk_usd: float
    cluster: Optional[str]


@dataclass
class RejectedTrade:
    """A rejected trade with reason."""
    candidate: TradeCandidate
    reason: RejectionReason
    details: str = ""


class RiskEngine:
    """
    Portfolio Risk Engine.
    
    Evaluates trade candidates and applies risk rules:
    1. Symbol cooldown after stop-loss
    2. Cluster cooldown after stop-loss
    3. Drawdown kill-switch (block all if DD > threshold)
    4. Max open positions
    5. Max total risk
    6. Cluster position cap
    7. Cluster risk cap
    8. Same-day cluster deduplication
    """
    
    def __init__(self, config: RiskConfig):
        self.config = config
        self.open_positions: List[Position] = []
        
        # Initialize equity from config for compounding
        self.initial_equity: float = config.initial_equity
        self.current_equity: float = config.initial_equity
        self.peak_equity: float = config.initial_equity
        
        self.symbol_cooldowns: Dict[str, date] = {}  # symbol -> cooldown_until
        self.cluster_cooldowns: Dict[str, date] = {}  # cluster -> cooldown_until
        
        # Stats tracking
        self.stats = {
            'approved': 0,
            'rejected_symbol_cooldown': 0,
            'rejected_cluster_cooldown': 0,
            'rejected_dd_kill_switch': 0,
            'rejected_max_positions': 0,
            'rejected_max_total_risk': 0,
            'rejected_max_risk_per_trade': 0,
            'rejected_cluster_position_cap': 0,
            'rejected_cluster_risk_cap': 0,
            'rejected_same_day_dedup': 0,
        }
    
    def get_cluster(self, symbol: str) -> Optional[str]:
        """Get the cluster a symbol belongs to."""
        for cluster_name, symbols in self.config.clusters.items():
            if symbol in symbols:
                return cluster_name
        return None
    
    def update_equity(self, pnl: float):
        """Update equity tracking for drawdown calculation."""
        self.current_equity += pnl
        self.peak_equity = max(self.peak_equity, self.current_equity)
    
    def get_risk_per_trade(self) -> float:
        """
        Get current risk per trade in USD.
        Uses percentage-based if configured, otherwise fixed USD.
        """
        if self.config.risk_per_trade_pct is not None:
            return self.current_equity * self.config.risk_per_trade_pct
        return self.config.risk_per_trade_usd
    
    def get_max_total_risk(self) -> float:
        """
        Get current max total risk in USD.
        Uses percentage-based if configured, otherwise fixed USD.
        """
        if self.config.max_total_risk_pct is not None:
            return self.current_equity * self.config.max_total_risk_pct
        return self.config.max_total_risk_usd
    
    def check_dd_kill_switch(self) -> bool:
        """Check if drawdown kill-switch is triggered."""
        if self.peak_equity <= 0:
            return False
        dd_pct = (self.peak_equity - self.current_equity) / self.peak_equity
        return dd_pct >= self.config.dd_kill_pct
    
    def expire_positions(self, current_date: date):
        """Remove positions that have exited."""
        self.open_positions = [
            p for p in self.open_positions
            if current_date < p.exit_date
        ]
    
    def get_open_position_count(self) -> int:
        """Get count of open positions."""
        return len(self.open_positions)
    
    def get_total_risk(self) -> float:
        """Get total risk across all open positions."""
        return sum(p.risk_usd for p in self.open_positions)
    
    def get_cluster_positions(self, cluster: str) -> int:
        """Get count of positions in a cluster."""
        return sum(1 for p in self.open_positions if p.cluster == cluster)
    
    def get_cluster_risk(self, cluster: str) -> float:
        """Get total risk in a cluster."""
        return sum(p.risk_usd for p in self.open_positions if p.cluster == cluster)
    
    def set_symbol_cooldown(self, symbol: str, from_date: date):
        """Set cooldown for a symbol after stop-loss."""
        cooldown_until = from_date + timedelta(days=self.config.symbol_cooldown_after_sl_days)
        self.symbol_cooldowns[symbol] = cooldown_until
        
        # Also set cluster cooldown
        cluster = self.get_cluster(symbol)
        if cluster:
            cluster_cooldown_until = from_date + timedelta(days=self.config.cluster_cooldown_after_sl_days)
            self.cluster_cooldowns[cluster] = cluster_cooldown_until
    
    def add_position(self, trade: ApprovedTrade, expected_exit: date):
        """Add an approved position."""
        self.open_positions.append(Position(
            symbol=trade.candidate.symbol,
            cluster=trade.cluster,
            entry_date=trade.candidate.execution_date,
            exit_date=expected_exit,
            risk_usd=trade.risk_usd,
            contracts=trade.contracts,
        ))
    
    def close_position(self, symbol: str, actual_exit_date: date):
        """
        Update position's exit_date to actual exit after trade simulation.
        
        This is critical for backtesting: without this, positions would be counted
        as open until expiry even if they hit TP/SL earlier.
        """
        for pos in self.open_positions:
            if pos.symbol == symbol and pos.exit_date > actual_exit_date:
                pos.exit_date = actual_exit_date
                break
    
    def evaluate_candidates(
        self,
        candidates: List[TradeCandidate],
        current_date: date,
    ) -> Tuple[List[ApprovedTrade], List[RejectedTrade]]:
        """
        Evaluate a batch of candidates for a given date.
        
        Returns (approved, rejected) lists.
        """
        # Expire old positions
        self.expire_positions(current_date)
        
        approved: List[ApprovedTrade] = []
        rejected: List[RejectedTrade] = []
        
        # Step 1: Pre-filter by cooldowns and kill-switch (per-candidate)
        pre_filtered = []
        for candidate in candidates:
            cluster = self.get_cluster(candidate.symbol)
            
            # Check symbol cooldown
            if candidate.symbol in self.symbol_cooldowns:
                if current_date < self.symbol_cooldowns[candidate.symbol]:
                    rejected.append(RejectedTrade(
                        candidate=candidate,
                        reason=RejectionReason.SYMBOL_COOLDOWN,
                        details=f"Until {self.symbol_cooldowns[candidate.symbol]}"
                    ))
                    self.stats['rejected_symbol_cooldown'] += 1
                    continue
            
            # Check cluster cooldown
            if cluster and cluster in self.cluster_cooldowns:
                if current_date < self.cluster_cooldowns[cluster]:
                    rejected.append(RejectedTrade(
                        candidate=candidate,
                        reason=RejectionReason.CLUSTER_COOLDOWN,
                        details=f"Cluster {cluster} until {self.cluster_cooldowns[cluster]}"
                    ))
                    self.stats['rejected_cluster_cooldown'] += 1
                    continue
            
            # Check drawdown kill-switch
            if self.check_dd_kill_switch():
                rejected.append(RejectedTrade(
                    candidate=candidate,
                    reason=RejectionReason.DRAWDOWN_KILL_SWITCH,
                    details=f"DD {((self.peak_equity - self.current_equity) / self.peak_equity * 100):.1f}%"
                ))
                self.stats['rejected_dd_kill_switch'] += 1
                continue
            
            pre_filtered.append((candidate, cluster))
        
        # Step 2: Same-day cluster deduplication (best edge wins)
        if self.config.cluster_dedup_mode == 'best_edge':
            # Group by cluster
            by_cluster: Dict[str, List[Tuple[TradeCandidate, str]]] = {}
            no_cluster = []
            
            for candidate, cluster in pre_filtered:
                if cluster:
                    by_cluster.setdefault(cluster, []).append((candidate, cluster))
                else:
                    no_cluster.append((candidate, cluster))
            
            # Take best from each cluster
            deduped = list(no_cluster)
            for cluster, group in by_cluster.items():
                if len(group) == 1:
                    deduped.append(group[0])
                else:
                    # Best edge wins
                    best = max(group, key=lambda x: x[0].edge_strength)
                    deduped.append(best)
                    # Reject others
                    for item in group:
                        if item != best:
                            rejected.append(RejectedTrade(
                                candidate=item[0],
                                reason=RejectionReason.SAME_DAY_CLUSTER_DEDUP,
                                details=f"Better edge in cluster: {best[0].symbol}"
                            ))
                            self.stats['rejected_same_day_dedup'] += 1
            
            pre_filtered = deduped
        
        # Step 3: Apply position and risk caps (in order of edge strength)
        sorted_candidates = sorted(pre_filtered, key=lambda x: x[0].edge_strength, reverse=True)
        
        for candidate, cluster in sorted_candidates:
            # Check max open positions
            if self.get_open_position_count() >= self.config.max_open_positions:
                rejected.append(RejectedTrade(
                    candidate=candidate,
                    reason=RejectionReason.MAX_POSITIONS,
                    details=f"At limit: {self.config.max_open_positions}"
                ))
                self.stats['rejected_max_positions'] += 1
                continue
            
            # Get dynamic risk budget based on current equity
            risk_budget = self.get_risk_per_trade()
            max_total_risk = self.get_max_total_risk()
            
            # Use TRUE max_loss (no capping - we need honest risk accounting)
            max_loss_per_contract = candidate.max_loss_usd
            
            # Compute contracts based on risk budget
            contracts = max(1, int(risk_budget / max_loss_per_contract))
            risk_usd = max_loss_per_contract * contracts
            
            # Policy A: REJECT if even one-lot exceeds risk-per-trade cap
            if max_loss_per_contract > risk_budget:
                rejected.append(RejectedTrade(
                    candidate=candidate,
                    reason=RejectionReason.MAX_RISK_PER_TRADE_EXCEEDED,
                    details=f"${max_loss_per_contract:.0f} > ${risk_budget:.0f} cap"
                ))
                self.stats['rejected_max_risk_per_trade'] += 1
                continue
            
            # Check max total risk (dynamic)
            if self.get_total_risk() + risk_usd > max_total_risk:
                rejected.append(RejectedTrade(
                    candidate=candidate,
                    reason=RejectionReason.MAX_TOTAL_RISK,
                    details=f"Would exceed ${max_total_risk:.0f}"
                ))
                self.stats['rejected_max_total_risk'] += 1
                continue
            
            # Check cluster position cap
            if cluster:
                if self.get_cluster_positions(cluster) >= self.config.max_cluster_positions:
                    rejected.append(RejectedTrade(
                        candidate=candidate,
                        reason=RejectionReason.CLUSTER_POSITION_CAP,
                        details=f"Cluster {cluster} at limit: {self.config.max_cluster_positions}"
                    ))
                    self.stats['rejected_cluster_position_cap'] += 1
                    continue
                
                # Check cluster risk cap
                if self.get_cluster_risk(cluster) + risk_usd > self.config.max_cluster_risk_usd:
                    rejected.append(RejectedTrade(
                        candidate=candidate,
                        reason=RejectionReason.CLUSTER_RISK_CAP,
                        details=f"Cluster {cluster} would exceed ${self.config.max_cluster_risk_usd}"
                    ))
                    self.stats['rejected_cluster_risk_cap'] += 1
                    continue
            
            # Approved!
            trade = ApprovedTrade(
                candidate=candidate,
                contracts=contracts,
                risk_usd=risk_usd,
                cluster=cluster,
            )
            approved.append(trade)
            self.stats['approved'] += 1
            
            # Add to open positions (expected exit = expiry or 30 days)
            expected_exit = candidate.expiry or (candidate.execution_date + timedelta(days=30))
            self.add_position(trade, expected_exit)
        
        return approved, rejected
    
    def print_stats(self):
        """Print rejection statistics."""
        print("\n📊 RISK ENGINE STATS")
        print(f"  Approved: {self.stats['approved']}")
        print(f"  Rejected - Symbol cooldown: {self.stats['rejected_symbol_cooldown']}")
        print(f"  Rejected - Cluster cooldown: {self.stats['rejected_cluster_cooldown']}")
        print(f"  Rejected - DD kill-switch: {self.stats['rejected_dd_kill_switch']}")
        print(f"  Rejected - Max positions: {self.stats['rejected_max_positions']}")
        print(f"  Rejected - Max total risk: {self.stats['rejected_max_total_risk']}")
        print(f"  Rejected - Risk per trade exceeded: {self.stats['rejected_max_risk_per_trade']}")
        print(f"  Rejected - Cluster position cap: {self.stats['rejected_cluster_position_cap']}")
        print(f"  Rejected - Cluster risk cap: {self.stats['rejected_cluster_risk_cap']}")
        print(f"  Rejected - Same-day dedup: {self.stats['rejected_same_day_dedup']}")
