"""
Execution Plan Module.

ExecutionPlan is a PREVIEW ONLY - NOT an IBKR order.
It represents what WOULD be submitted if the user confirms.
"""

from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from typing import Optional
from uuid import uuid4


@dataclass
class ExecutionLeg:
    """Single leg of the execution plan."""
    action: str  # BUY | SELL
    option_type: str  # PUT | CALL
    strike: float
    expiration: date
    quantity: int
    
    def to_dict(self) -> dict:
        return {
            'action': self.action,
            'type': self.option_type,
            'strike': self.strike,
            'expiry': self.expiration.isoformat(),
            'qty': self.quantity,
        }


@dataclass
class ExecutionPlan:
    """
    Execution Plan - PREVIEW ONLY.
    
    This is NOT an IBKR order and must NEVER be submitted automatically.
    It represents what the trade would look like if confirmed.
    """
    id: str = field(default_factory=lambda: str(uuid4()))
    symbol: str = ""
    structure_type: str = ""  # iron_condor, credit_spread, butterfly, etc.
    legs: list[ExecutionLeg] = field(default_factory=list)
    
    # Order parameters (for preview)
    order_type: str = "LMT"  # LMT only
    limit_price: float = 0.0  # Net credit (positive) or debit (negative)
    tif: str = "DAY"  # Time in force
    exchange: str = "SMART"
    currency: str = "USD"
    
    # Risk metrics
    expected_credit_dollars: float = 0.0
    max_loss_dollars: float = 0.0
    pop_estimate: float = 0.0
    
    # Status - NEVER auto-submitted
    status: str = "PENDING_CONFIRMATION"  # PENDING_CONFIRMATION | CONFIRMED | SUBMITTED | CANCELLED
    
    created_at: datetime = field(default_factory=datetime.now)
    confirmed_at: Optional[datetime] = None
    submitted_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dict for logging/display."""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'structure': self.structure_type,
            'legs': [leg.to_dict() for leg in self.legs],
            'order_type': self.order_type,
            'limit_price': self.limit_price,
            'tif': self.tif,
            'expected_credit': self.expected_credit_dollars,
            'max_loss': self.max_loss_dollars,
            'pop_estimate': self.pop_estimate,
            'status': self.status,
            'created_at': self.created_at.isoformat(),
            'confirmed_at': self.confirmed_at.isoformat() if self.confirmed_at else None,
        }
    
    @property
    def is_confirmed(self) -> bool:
        """Check if user has confirmed this trade."""
        return self.status in ("CONFIRMED", "SUBMITTED")
    
    @property
    def is_pending(self) -> bool:
        """Check if awaiting user confirmation."""
        return self.status == "PENDING_CONFIRMATION"


def create_execution_plan_from_candidate(candidate, spot_price: float = None) -> ExecutionPlan:
    """
    Create an ExecutionPlan from a TradeCandidate.
    
    This is a PREVIEW ONLY - not an order.
    """
    structure = candidate.structure
    
    legs = []
    if structure and structure.legs:
        for leg in structure.legs:
            exec_leg = ExecutionLeg(
                action="SELL" if leg.quantity < 0 else "BUY",
                option_type=leg.contract.option_type.value.upper(),
                strike=leg.contract.strike,
                expiration=leg.contract.expiration,
                quantity=abs(leg.quantity),
            )
            legs.append(exec_leg)
    
    # Calculate expected credit (in dollars)
    expected_credit = 0.0
    if structure and structure.entry_credit:
        expected_credit = structure.entry_credit * 100 * candidate.recommended_contracts
    
    # Max loss (in dollars)
    max_loss = 0.0
    if structure and structure.max_loss:
        max_loss = structure.max_loss * 100 * candidate.recommended_contracts
    
    # POP estimate from probability metrics if available
    pop_estimate = 0.0
    if candidate.probability_metrics:
        pop_estimate = candidate.probability_metrics.get('pop_expiry', 0.0)
    
    return ExecutionPlan(
        symbol=candidate.symbol,
        structure_type=structure.structure_type.value if structure else "unknown",
        legs=legs,
        limit_price=structure.entry_credit if structure and structure.entry_credit else 0.0,
        expected_credit_dollars=expected_credit,
        max_loss_dollars=max_loss,
        pop_estimate=pop_estimate,
        status="PENDING_CONFIRMATION",
    )
