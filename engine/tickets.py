"""
Trade Ticket Generation.

Generates structured trade tickets for execution:
- Semi-automated workflow: engine generates → human approves
- JSON output for programmatic use
- Clipboard-ready format for manual entry
- Later: wire to IBKR API
"""

import json
from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Optional, List
from pathlib import Path

from data.schemas import TradeCandidate, OptionStructure, OptionLeg


@dataclass
class TradeLeg:
    """A single leg of a trade ticket."""
    action: str  # "BUY" or "SELL"
    quantity: int
    symbol: str  # Underlying
    contract_symbol: str  # OCC symbol
    option_type: str  # "CALL" or "PUT"
    strike: float
    expiration: str  # ISO date
    limit_price: float
    
    # For display
    description: str


@dataclass
class TradeTicket:
    """
    Complete trade ticket for execution.
    
    Workflow:
    1. Engine generates ticket from candidate
    2. Human reviews and approves
    3. Execute via broker API or manual entry
    """
    ticket_id: str
    generated_at: str
    status: str  # "pending", "approved", "rejected", "executed"
    
    # Trade details
    symbol: str
    structure_type: str
    legs: List[TradeLeg]
    
    # Pricing
    net_credit: Optional[float]  # Dollars, credit received
    net_debit: Optional[float]   # Dollars, debit paid
    max_loss: float              # Dollars
    max_profit: Optional[float]  # Dollars
    
    # Risk
    contracts: int
    total_risk: float  # Dollars
    risk_pct_of_equity: float
    
    # Context
    edge_type: str
    edge_strength: float
    regime: str
    quality_score: Optional[int]
    
    # Execution instructions
    order_type: str  # "limit", "market"
    limit_price: Optional[float]  # Net price
    time_in_force: str  # "DAY", "GTC"
    
    # Notes
    rationale: str
    warnings: List[str]


def generate_trade_ticket(
    candidate: TradeCandidate,
    account_equity: float = 100000,
) -> TradeTicket:
    """
    Generate a trade ticket from a trade candidate.
    
    Args:
        candidate: The approved trade candidate
        account_equity: Account equity for risk % calculation
        
    Returns:
        TradeTicket ready for execution
    """
    structure = candidate.structure
    
    # Build legs
    legs = []
    for leg in structure.legs:
        contract = leg.contract
        action = "BUY" if leg.quantity > 0 else "SELL"
        quantity = abs(leg.quantity) * candidate.recommended_contracts
        
        # Use conservative pricing for limit
        if leg.quantity > 0:  # Buying
            limit_price = contract.ask  # Pay ask
        else:  # Selling
            limit_price = contract.bid  # Receive bid
        
        description = (
            f"{action} {quantity} {contract.symbol} "
            f"{contract.expiration.isoformat()} "
            f"${contract.strike:.0f} {contract.option_type.value.upper()}"
        )
        
        legs.append(TradeLeg(
            action=action,
            quantity=quantity,
            symbol=contract.symbol,
            contract_symbol=contract.contract_symbol,
            option_type=contract.option_type.value.upper(),
            strike=contract.strike,
            expiration=contract.expiration.isoformat(),
            limit_price=limit_price,
            description=description,
        ))
    
    # Calculate net price
    net_credit = None
    net_debit = None
    
    if structure.entry_credit:
        net_credit = structure.entry_credit * 100  # Points to dollars
        limit_price = structure.entry_credit  # Credit as positive
    elif structure.entry_debit:
        net_debit = structure.entry_debit * 100
        limit_price = structure.entry_debit  # Debit as positive
    else:
        limit_price = None
    
    # Max loss/profit in dollars
    max_loss = (structure.max_loss or 0) * 100 * candidate.recommended_contracts
    max_profit = (structure.max_profit or 0) * 100 * candidate.recommended_contracts if structure.max_profit else None
    
    # Warnings
    warnings = []
    if candidate.recommendation == "REVIEW":
        warnings.append("Trade marked REVIEW - check validation messages")
    if candidate.validation_messages:
        warnings.extend(candidate.validation_messages)
    
    # Quality score
    quality_score = None
    if candidate.quality_score:
        quality_score = candidate.quality_score.get("total")
    
    return TradeTicket(
        ticket_id=candidate.id,
        generated_at=datetime.now().isoformat(),
        status="pending",
        symbol=candidate.symbol,
        structure_type=structure.structure_type.value,
        legs=legs,
        net_credit=net_credit,
        net_debit=net_debit,
        max_loss=max_loss,
        max_profit=max_profit,
        contracts=candidate.recommended_contracts,
        total_risk=candidate.total_risk,
        risk_pct_of_equity=candidate.total_risk / account_equity * 100,
        edge_type=candidate.edge.edge_type.value,
        edge_strength=candidate.edge.strength,
        regime=candidate.regime.regime.value,
        quality_score=quality_score,
        order_type="limit",
        limit_price=limit_price,
        time_in_force="DAY",
        rationale=candidate.rationale,
        warnings=warnings,
    )


def ticket_to_json(ticket: TradeTicket) -> str:
    """Convert ticket to JSON string."""
    data = asdict(ticket)
    data["legs"] = [asdict(leg) for leg in ticket.legs]
    return json.dumps(data, indent=2)


def ticket_to_clipboard_format(ticket: TradeTicket) -> str:
    """
    Generate clipboard-ready format for manual entry.
    
    Format designed for quick copy-paste into broker.
    """
    lines = [
        "=" * 60,
        f"TRADE TICKET: {ticket.ticket_id[:8]}",
        "=" * 60,
        "",
        f"Symbol: {ticket.symbol}",
        f"Structure: {ticket.structure_type.upper()}",
        f"Edge: {ticket.edge_type} ({ticket.edge_strength:.0%})",
        f"Regime: {ticket.regime}",
        "",
        "-" * 40,
        "LEGS:",
        "-" * 40,
    ]
    
    for leg in ticket.legs:
        lines.append(f"  {leg.description} @ ${leg.limit_price:.2f}")
    
    lines.extend([
        "",
        "-" * 40,
        "PRICING:",
        "-" * 40,
    ])
    
    if ticket.net_credit:
        lines.append(f"  Net Credit: ${ticket.net_credit:.0f}")
    if ticket.net_debit:
        lines.append(f"  Net Debit: ${ticket.net_debit:.0f}")
    
    lines.extend([
        f"  Limit Price: ${ticket.limit_price:.2f}" if ticket.limit_price else "",
        f"  Order: {ticket.order_type.upper()} / {ticket.time_in_force}",
        "",
        "-" * 40,
        "RISK:",
        "-" * 40,
        f"  Contracts: {ticket.contracts}",
        f"  Max Loss: ${ticket.max_loss:.0f}",
        f"  Total Risk: ${ticket.total_risk:.0f} ({ticket.risk_pct_of_equity:.2f}% of equity)",
    ])
    
    if ticket.max_profit:
        lines.append(f"  Max Profit: ${ticket.max_profit:.0f}")
    
    lines.extend([
        "",
        "-" * 40,
        "RATIONALE:",
        "-" * 40,
        f"  {ticket.rationale}",
    ])
    
    if ticket.warnings:
        lines.extend([
            "",
            "⚠️ WARNINGS:",
        ])
        for w in ticket.warnings:
            lines.append(f"  - {w}")
    
    lines.extend([
        "",
        "=" * 60,
        f"Status: {ticket.status.upper()}",
        f"Generated: {ticket.generated_at}",
        "=" * 60,
    ])
    
    return "\n".join(lines)


class TicketStore:
    """
    Stores and manages trade tickets.
    
    Persists tickets for audit trail and approval workflow.
    """
    
    def __init__(self, storage_path: str = "./logs/tickets"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    def save_ticket(self, ticket: TradeTicket) -> None:
        """Save a ticket to disk."""
        file_path = self.storage_path / f"{ticket.ticket_id}.json"
        with open(file_path, "w") as f:
            f.write(ticket_to_json(ticket))
    
    def load_ticket(self, ticket_id: str) -> Optional[TradeTicket]:
        """Load a ticket by ID."""
        file_path = self.storage_path / f"{ticket_id}.json"
        if not file_path.exists():
            return None
        
        with open(file_path) as f:
            data = json.load(f)
        
        # Reconstruct legs
        legs = [TradeLeg(**leg) for leg in data.pop("legs")]
        return TradeTicket(legs=legs, **data)
    
    def update_status(
        self, 
        ticket_id: str, 
        status: str,
        notes: str = None,
    ) -> bool:
        """Update ticket status."""
        ticket = self.load_ticket(ticket_id)
        if not ticket:
            return False
        
        ticket.status = status
        if notes:
            ticket.warnings.append(f"[{status}] {notes}")
        
        self.save_ticket(ticket)
        return True
    
    def get_pending_tickets(self) -> List[TradeTicket]:
        """Get all pending tickets."""
        tickets = []
        for file_path in self.storage_path.glob("*.json"):
            ticket = self.load_ticket(file_path.stem)
            if ticket and ticket.status == "pending":
                tickets.append(ticket)
        return tickets
    
    def approve_ticket(self, ticket_id: str) -> bool:
        """Approve a ticket for execution."""
        return self.update_status(ticket_id, "approved")
    
    def reject_ticket(self, ticket_id: str, reason: str) -> bool:
        """Reject a ticket."""
        return self.update_status(ticket_id, "rejected", reason)
