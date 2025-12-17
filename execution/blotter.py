"""
Paper Trading Blotter for VolMachine.

Tracks all paper trades with full PnL attribution for edge analysis.

Schema per trade (v2):
- trade_id: unique identifier
- timestamp: entry time
- symbol, edge_type, edge_percentile, regime, structure, dte
- entry_price: signed net (positive=credit, negative=debit)
- exit_price: signed net at close
- max_loss_dollars, max_profit_dollars
- legs: list with conId, localSymbol, strike, side, price
- ibkr_order_id, ibkr_perm_id
- realized_pnl, exit_reason
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
    con_id: int = 0              # IBKR conId
    local_symbol: str = ""       # IBKR localSymbol (e.g., "SPY   251220P00680000")
    strike: float = 0.0
    expiry: str = ""             # YYYYMMDD
    right: str = ""              # P or C
    side: str = ""               # BUY or SELL
    quantity: int = 1
    entry_price: float = 0.0
    exit_price: Optional[float] = None
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PaperTrade:
    """Full paper trade record for PnL attribution."""
    # Core identification
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Symbol and edge
    symbol: str = ""
    edge_type: str = ""
    edge_percentile: float = 0.0  # 0-100
    
    # Market context
    regime: str = ""              # TREND, CHOP, REVERSAL
    vol_regime: str = ""          # LOW, NORMAL, HIGH, EXTREME
    spot_price: float = 0.0
    
    # Structure
    structure: str = ""           # credit_spread, debit_spread, iron_condor, etc.
    spread_type: str = ""         # "credit" or "debit"
    spread_width: float = 0.0     # Width in dollars
    dte: int = 0
    
    # Legs (list of TradeLeg dicts)
    legs: List[Dict[str, Any]] = field(default_factory=list)
    
    # Entry pricing (SIGNED: positive=credit received, negative=debit paid)
    entry_price: float = 0.0
    
    # Risk metrics
    max_loss_dollars: float = 0.0
    max_profit_dollars: float = 0.0
    
    # IBKR order tracking
    ibkr_order_id: Optional[int] = None
    ibkr_perm_id: Optional[int] = None
    
    # Exit (filled when closed)
    exit_timestamp: Optional[str] = None
    exit_price: float = 0.0       # SIGNED: positive=credit, negative=debit
    exit_reason: str = ""         # expiry, stop_loss, profit_target, manual
    
    # PnL
    realized_pnl: float = 0.0     # Dollars (per contract * 100)
    realized_pnl_pct: float = 0.0 # Percentage of max risk
    
    # Status
    status: str = "open"          # open, closed, cancelled
    
    # Diagnostics
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PaperTrade':
        # Handle legacy fields
        if 'entry_credit' in data:
            data['entry_price'] = data.pop('entry_credit', 0) or -data.pop('entry_debit', 0)
        if 'exit_credit' in data:
            data['exit_price'] = data.pop('exit_credit', 0) or -data.pop('exit_debit', 0)
        # Remove unknown fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        data = {k: v for k, v in data.items() if k in known_fields}
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
                        try:
                            data = json.loads(line)
                            trade = PaperTrade.from_dict(data)
                            self._trades[trade.trade_id] = trade
                        except Exception:
                            pass  # Skip malformed lines
    
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
        exit_price: float = 0.0,
        exit_reason: str = "manual",
    ) -> Optional[PaperTrade]:
        """
        Record trade exit and calculate PnL.
        
        exit_price: SIGNED (positive=credit, negative=debit)
        """
        if trade_id not in self._trades:
            return None
        
        trade = self._trades[trade_id]
        trade.exit_timestamp = datetime.now().isoformat()
        trade.exit_price = exit_price
        trade.exit_reason = exit_reason
        trade.status = "closed"
        
        # Calculate PnL
        # PnL = entry_price - exit_price (both signed)
        # Credit spread: entry_price=+0.50, exit_price=-0.10 → PnL = 0.50 - (-0.10) = 0.60
        # Debit spread: entry_price=-1.00, exit_price=+1.50 → PnL = -1.00 - 1.50 = 0.50
        trade.realized_pnl = (trade.entry_price - trade.exit_price) * 100  # Per contract
        
        # PnL as percentage of max risk
        if trade.max_loss_dollars > 0:
            trade.realized_pnl_pct = (trade.realized_pnl / trade.max_loss_dollars) * 100
        
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


def create_trade_from_ibkr_order(
    symbol: str,
    spread_type: str,
    entry_price: float,
    legs: List[Dict[str, Any]],
    ibkr_order_id: int,
    ibkr_perm_id: Optional[int] = None,
    edge_type: str = "",
    edge_percentile: float = 0.0,
    regime: str = "",
    structure: str = "",
    spread_width: float = 0.0,
    dte: int = 0,
    spot_price: float = 0.0,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> PaperTrade:
    """
    Create a PaperTrade from an IBKR order submission.
    
    Called only when transmit=True succeeds.
    
    entry_price: SIGNED (positive=credit, negative=debit)
    """
    trade = PaperTrade(
        symbol=symbol,
        edge_type=edge_type,
        edge_percentile=edge_percentile,
        regime=regime,
        structure=structure,
        spread_type=spread_type,
        spread_width=spread_width,
        dte=dte,
        spot_price=spot_price,
        legs=legs,
        entry_price=entry_price,
        ibkr_order_id=ibkr_order_id,
        ibkr_perm_id=ibkr_perm_id,
        diagnostics=diagnostics or {},
    )
    
    # Calculate max profit/loss
    if spread_type == "credit":
        trade.max_profit_dollars = abs(entry_price) * 100
        trade.max_loss_dollars = (spread_width - abs(entry_price)) * 100
    else:
        trade.max_profit_dollars = (spread_width - abs(entry_price)) * 100
        trade.max_loss_dollars = abs(entry_price) * 100
    
    return trade


# Module-level singleton
_blotter: Optional[Blotter] = None


def get_blotter() -> Blotter:
    """Get or create the global blotter instance."""
    global _blotter
    if _blotter is None:
        _blotter = Blotter()
    return _blotter
