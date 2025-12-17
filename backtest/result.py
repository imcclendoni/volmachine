"""
Backtest Result Dataclasses.

Structures for storing backtest trades and results.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, List, Dict, Any
from enum import Enum


class ExitReason(Enum):
    """Reason for exiting a trade."""
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    TIME_STOP = "time_stop"
    EXPIRY = "expiry"
    MAX_HOLD = "max_hold"
    MANUAL = "manual"


@dataclass
class BacktestTrade:
    """Single trade result from backtest."""
    # Identification
    trade_id: str = ""
    symbol: str = ""
    
    # Edge info
    edge_type: str = ""
    edge_strength: float = 0.0
    edge_percentile: float = 0.0
    regime: str = ""
    
    # Structure info
    structure_type: str = ""
    spread_type: str = ""  # "credit" or "debit"
    dte_at_entry: int = 0
    
    # Timing
    signal_date: str = ""       # Date signal was generated
    entry_date: str = ""        # Date position entered (signal_date close)
    exit_date: str = ""         # Date position exited
    
    # Pricing
    entry_price: float = 0.0    # Net premium (+ credit, - debit)
    exit_price: float = 0.0     # Net premium at close
    max_loss_theoretical: float = 0.0
    max_profit_theoretical: float = 0.0
    
    # PnL
    gross_pnl: float = 0.0
    commissions: float = 0.0
    net_pnl: float = 0.0
    pnl_pct: float = 0.0        # % of max risk
    
    # MFE/MAE (Maximum Favorable/Adverse Excursion)
    mfe: float = 0.0            # Best PnL during trade
    mae: float = 0.0            # Worst PnL during trade
    
    # Exit
    exit_reason: ExitReason = ExitReason.EXPIRY
    hold_days: int = 0
    
    # Contracts
    contracts: int = 1
    
    # Legs detail (for audit)
    legs: List[Dict[str, Any]] = field(default_factory=list)
    
    # Data source
    data_source: str = "polygon"  # "polygon" or "ibkr"
    
    def is_winner(self) -> bool:
        """Check if trade was profitable."""
        return self.net_pnl > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'trade_id': self.trade_id,
            'symbol': self.symbol,
            'edge_type': self.edge_type,
            'edge_strength': self.edge_strength,
            'edge_percentile': self.edge_percentile,
            'regime': self.regime,
            'structure_type': self.structure_type,
            'spread_type': self.spread_type,
            'dte_at_entry': self.dte_at_entry,
            'signal_date': self.signal_date,
            'entry_date': self.entry_date,
            'exit_date': self.exit_date,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'max_loss_theoretical': self.max_loss_theoretical,
            'max_profit_theoretical': self.max_profit_theoretical,
            'gross_pnl': self.gross_pnl,
            'commissions': self.commissions,
            'net_pnl': self.net_pnl,
            'pnl_pct': self.pnl_pct,
            'mfe': self.mfe,
            'mae': self.mae,
            'exit_reason': self.exit_reason.value,
            'hold_days': self.hold_days,
            'contracts': self.contracts,
            'legs': self.legs,
            'data_source': self.data_source,
        }


@dataclass
class BacktestMetrics:
    """Aggregate metrics for backtest."""
    # Trade counts
    total_trades: int = 0
    winners: int = 0
    losers: int = 0
    
    # Win rate
    win_rate: float = 0.0
    
    # PnL
    total_pnl: float = 0.0
    total_commissions: float = 0.0
    avg_pnl: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    
    # Profit factor
    profit_factor: float = 0.0  # Gross wins / Gross losses
    
    # Expectancy
    expectancy: float = 0.0     # (Win% * AvgWin) - (Loss% * AvgLoss)
    expectancy_per_dollar: float = 0.0  # Expectancy / AvgRisk
    
    # Drawdown
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    
    # Sharpe-like
    sharpe_ratio: float = 0.0
    
    # Exposure
    avg_hold_days: float = 0.0
    total_exposure_days: int = 0
    
    # Breakdowns
    by_edge_type: Dict[str, Dict] = field(default_factory=dict)
    by_regime: Dict[str, Dict] = field(default_factory=dict)
    by_structure: Dict[str, Dict] = field(default_factory=dict)
    by_symbol: Dict[str, Dict] = field(default_factory=dict)


@dataclass
class BacktestResult:
    """Complete backtest result."""
    # Config
    start_date: str = ""
    end_date: str = ""
    config_hash: str = ""       # Hash of config for reproducibility
    
    # Results
    trades: List[BacktestTrade] = field(default_factory=list)
    metrics: BacktestMetrics = field(default_factory=BacktestMetrics)
    
    # Metadata
    generated_at: str = ""
    signals_source: str = ""    # "saved_reports" or "regenerated"
    data_source: str = "polygon"
    
    # Reproducibility
    config_used: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'start_date': self.start_date,
            'end_date': self.end_date,
            'config_hash': self.config_hash,
            'trades': [t.to_dict() for t in self.trades],
            'metrics': {
                'total_trades': self.metrics.total_trades,
                'winners': self.metrics.winners,
                'losers': self.metrics.losers,
                'win_rate': self.metrics.win_rate,
                'total_pnl': self.metrics.total_pnl,
                'avg_pnl': self.metrics.avg_pnl,
                'profit_factor': self.metrics.profit_factor,
                'expectancy': self.metrics.expectancy,
                'max_drawdown': self.metrics.max_drawdown,
                'sharpe_ratio': self.metrics.sharpe_ratio,
                'by_edge_type': self.metrics.by_edge_type,
                'by_regime': self.metrics.by_regime,
                'by_structure': self.metrics.by_structure,
                'by_symbol': self.metrics.by_symbol,
            },
            'generated_at': self.generated_at,
            'signals_source': self.signals_source,
            'data_source': self.data_source,
            'config_used': self.config_used,
        }
