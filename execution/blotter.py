"""
Paper Trading Blotter for VolMachine.

Tracks all paper trades with full PnL attribution for edge analysis.

Schema per trade:
- trade_id: unique identifier
- timestamp: entry time
- symbol: underlying
- edge_type: skew_extremes, vol_risk_premium, etc.
- edge_percentile: percentile at entry (0-100)
- regime: TREND/CHOP/REVERSAL
- structure: credit_spread, iron_condor, etc.
- legs: list of {strike, expiry, side, quantity, price}
- entry_credit/debit: net premium
- exit_credit/debit: net premium at close
- realized_pnl: dollars
- exit_reason: expiry, stop_loss, profit_target, manual
- diagnostics: delta_proxy_used, strike_distance, etc.
"""

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import uuid


@dataclass
class TradeLeg:
    """Single leg of an option trade."""
    strike: float
    expiry: str  # YYYYMMDD
    right: str  # P or C
    side: str  # BUY or SELL
    quantity: int
    entry_price: float
    exit_price: Optional[float] = None


@dataclass
class PaperTrade:
    """Full paper trade record for PnL attribution."""
    # Core identification
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Symbol and edge
    symbol: str = ""
    edge_type: str = ""
    edge_percentile: float = 0.0
    
    # Market context
    regime: str = ""  # TREND, CHOP, REVERSAL
    vol_regime: str = ""  # LOW, NORMAL, HIGH, EXTREME
    spot_price: float = 0.0
    
    # Structure
    structure: str = ""  # credit_spread, iron_condor, butterfly, etc.
    spread_width: float = 0.0
    dte: int = 0
    
    # Legs
    legs: List[Dict[str, Any]] = field(default_factory=list)
    
    # Entry pricing
    entry_credit: float = 0.0  # Positive = credit received
    entry_debit: float = 0.0   # Positive = debit paid
    
    # Exit (filled when closed)
    exit_timestamp: Optional[str] = None
    exit_credit: float = 0.0
    exit_debit: float = 0.0
    exit_reason: str = ""  # expiry, stop_loss, profit_target, manual, time_decay
    
    # PnL
    realized_pnl: float = 0.0  # Dollars
    realized_pnl_pct: float = 0.0  # Percentage of max risk
    
    # Status
    status: str = "open"  # open, closed, cancelled
    
    # Diagnostics
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PaperTrade':
        return cls(**data)


class Blotter:
    """
    Paper trading blotter - persists all trades to JSONL for analysis.
    
    File: logs/blotter/trades.jsonl (append-only)
    """
    
    def __init__(self, path: str = "./logs/blotter/trades.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._trades: Dict[str, PaperTrade] = {}
        self._load()
    
    def _load(self):
        """Load existing trades from JSONL."""
        if self.path.exists():
            with open(self.path, 'r') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        trade = PaperTrade.from_dict(data)
                        self._trades[trade.trade_id] = trade
    
    def _append(self, trade: PaperTrade):
        """Append trade to JSONL file."""
        with open(self.path, 'a') as f:
            f.write(json.dumps(trade.to_dict()) + '\n')
    
    def record_entry(self, trade: PaperTrade) -> str:
        """Record a new trade entry. Returns trade_id."""
        trade.status = "open"
        self._trades[trade.trade_id] = trade
        self._append(trade)
        return trade.trade_id
    
    def record_exit(
        self, 
        trade_id: str, 
        exit_credit: float = 0.0,
        exit_debit: float = 0.0,
        exit_reason: str = "manual",
    ) -> Optional[PaperTrade]:
        """Record trade exit and calculate PnL."""
        if trade_id not in self._trades:
            return None
        
        trade = self._trades[trade_id]
        trade.exit_timestamp = datetime.now().isoformat()
        trade.exit_credit = exit_credit
        trade.exit_debit = exit_debit
        trade.exit_reason = exit_reason
        trade.status = "closed"
        
        # Calculate PnL
        # For credit spread: PnL = entry_credit - exit_debit
        # For debit spread: PnL = exit_credit - entry_debit
        if trade.entry_credit > 0:
            # Credit spread
            trade.realized_pnl = (trade.entry_credit - trade.exit_debit) * 100  # Per contract
        else:
            # Debit spread
            trade.realized_pnl = (trade.exit_credit - trade.entry_debit) * 100
        
        # PnL as percentage of max risk
        max_risk = trade.spread_width * 100 - abs(trade.entry_credit - trade.entry_debit) * 100
        if max_risk > 0:
            trade.realized_pnl_pct = trade.realized_pnl / max_risk * 100
        
        self._append(trade)
        return trade
    
    def get_open_trades(self) -> List[PaperTrade]:
        """Get all open trades."""
        return [t for t in self._trades.values() if t.status == "open"]
    
    def get_closed_trades(self) -> List[PaperTrade]:
        """Get all closed trades."""
        return [t for t in self._trades.values() if t.status == "closed"]
    
    def get_trade(self, trade_id: str) -> Optional[PaperTrade]:
        """Get trade by ID."""
        return self._trades.get(trade_id)
    
    def get_summary(self) -> dict:
        """Get blotter summary statistics."""
        closed = self.get_closed_trades()
        
        if not closed:
            return {
                "total_trades": 0,
                "open_trades": len(self.get_open_trades()),
                "winners": 0,
                "losers": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "by_symbol": {},
                "by_edge": {},
            }
        
        winners = [t for t in closed if t.realized_pnl > 0]
        losers = [t for t in closed if t.realized_pnl <= 0]
        total_pnl = sum(t.realized_pnl for t in closed)
        
        # By symbol
        by_symbol = {}
        for t in closed:
            if t.symbol not in by_symbol:
                by_symbol[t.symbol] = {"trades": 0, "pnl": 0.0}
            by_symbol[t.symbol]["trades"] += 1
            by_symbol[t.symbol]["pnl"] += t.realized_pnl
        
        # By edge type
        by_edge = {}
        for t in closed:
            if t.edge_type not in by_edge:
                by_edge[t.edge_type] = {"trades": 0, "pnl": 0.0, "avg_percentile": 0.0}
            by_edge[t.edge_type]["trades"] += 1
            by_edge[t.edge_type]["pnl"] += t.realized_pnl
        
        # Calculate avg percentile per edge
        for edge in by_edge:
            edge_trades = [t for t in closed if t.edge_type == edge]
            by_edge[edge]["avg_percentile"] = sum(t.edge_percentile for t in edge_trades) / len(edge_trades)
        
        return {
            "total_trades": len(closed),
            "open_trades": len(self.get_open_trades()),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": len(winners) / len(closed) * 100 if closed else 0,
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / len(closed), 2) if closed else 0,
            "by_symbol": by_symbol,
            "by_edge": by_edge,
        }


def create_trade_from_candidate(
    candidate: dict,
    entry_price: float,
    spread_type: str = "credit",
) -> PaperTrade:
    """
    Create a PaperTrade from an engine candidate.
    
    Args:
        candidate: Engine trade candidate dict
        entry_price: Net credit/debit at entry
        spread_type: "credit" or "debit"
    """
    trade = PaperTrade(
        symbol=candidate.get("symbol", ""),
        edge_type=candidate.get("edge_type", ""),
        edge_percentile=candidate.get("edge_percentile", 0),
        regime=candidate.get("regime", ""),
        structure=candidate.get("structure", ""),
        spread_width=candidate.get("width", 0),
        dte=candidate.get("dte", 0),
        spot_price=candidate.get("spot", 0),
    )
    
    if spread_type == "credit":
        trade.entry_credit = entry_price
    else:
        trade.entry_debit = entry_price
    
    # Copy diagnostics
    trade.diagnostics = {
        "delta_proxy_used": candidate.get("delta_proxy_used", False),
        "strike_distance": candidate.get("strike_distance", 0),
        "skew_value": candidate.get("skew_value", 0),
    }
    
    return trade


# Module-level singleton
_blotter: Optional[Blotter] = None


def get_blotter() -> Blotter:
    """Get or create the global blotter instance."""
    global _blotter
    if _blotter is None:
        _blotter = Blotter()
    return _blotter
