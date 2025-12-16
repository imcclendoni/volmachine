"""
Paper Trading Simulator.

Simulates trade execution with configurable slippage.
Tracks positions with mark-to-market using IV surface.

FIXES:
- Close slippage direction: credit=pay more, debit=receive less
- Implement fill_at config (mid/bid/ask)
- Check is_valid and sizing.allowed before executing
- Proper cashflow convention throughout
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from uuid import uuid4

from data.schemas import (
    Position,
    OptionStructure,
    TradeCandidate,
)
from risk import Portfolio, LimitTracker


CONTRACT_MULTIPLIER = 100


@dataclass
class FillResult:
    """Result of a simulated fill."""
    
    success: bool
    fill_price_dollars: float  # Absolute value in dollars
    slippage_dollars: float
    message: str
    
    # Created position if successful
    position_id: Optional[str] = None


@dataclass
class PaperConfig:
    """Configuration for paper trading."""
    
    enabled: bool = True
    slippage_pct: float = 0.5  # 0.5% slippage
    fill_at: str = "conservative"  # "mid", "bid", "ask", "conservative"
    
    # Position limits
    max_positions: int = 20
    
    # Logging
    log_fills: bool = True


class PaperSimulator:
    """
    Paper trading simulator.
    
    Simulates fills, tracks positions, and enforces risk rules.
    """
    
    def __init__(
        self,
        portfolio: Portfolio,
        limit_tracker: LimitTracker,
        config: Optional[PaperConfig] = None,
    ):
        self.portfolio = portfolio
        self.limit_tracker = limit_tracker
        self.config = config or PaperConfig()
        
        # Fill history
        self.fills: list[dict] = []
    
    def execute_candidate(
        self,
        candidate: TradeCandidate,
    ) -> FillResult:
        """
        Execute a trade candidate in paper mode.
        
        REQUIRES:
        - candidate.recommendation == "TRADE"
        - candidate.is_valid == True
        - limit_tracker allows trading
        
        Args:
            candidate: Trade candidate to execute
            
        Returns:
            FillResult with outcome
        """
        # Check recommendation
        if candidate.recommendation != "TRADE":
            return FillResult(
                success=False,
                fill_price_dollars=0,
                slippage_dollars=0,
                message=f"Candidate not recommended: {candidate.recommendation}",
            )
        
        # Check is_valid (sizing passed + validation passed)
        if not candidate.is_valid:
            return FillResult(
                success=False,
                fill_price_dollars=0,
                slippage_dollars=0,
                message=f"Candidate not valid: {'; '.join(candidate.validation_messages)}",
            )
        
        # Check risk limits
        if not self.limit_tracker.is_trading_allowed():
            return FillResult(
                success=False,
                fill_price_dollars=0,
                slippage_dollars=0,
                message="Trading not allowed by risk limits",
            )
        
        structure = candidate.structure
        
        # Calculate entry price based on fill_at config
        base_price_points = self._calculate_entry_price(structure)
        base_price_dollars = base_price_points * CONTRACT_MULTIPLIER
        slippage_dollars = base_price_dollars * self.config.slippage_pct / 100
        
        # Apply slippage (against us)
        # For credits: entry_cashflow = +credit received (but less due to slippage)
        # For debits: entry_cashflow = -debit paid (more due to slippage)
        if structure.entry_credit:
            # Credit spread: we receive less
            fill_cashflow_dollars = base_price_dollars - slippage_dollars  # positive (received)
        else:
            # Debit spread: we pay more
            fill_cashflow_dollars = -(base_price_dollars + slippage_dollars)  # negative (paid)
        
        # Convert to per-contract points for portfolio (which stores points)
        fill_cashflow_per_contract_points = fill_cashflow_dollars / CONTRACT_MULTIPLIER
        
        # Add to portfolio using cashflow convention
        position = self.portfolio.add_position(
            structure=structure,
            contracts=candidate.recommended_contracts,
            entry_cashflow_per_contract=fill_cashflow_per_contract_points,
            trade_candidate_id=candidate.id,
        )
        
        # Log fill
        fill_record = {
            'timestamp': datetime.now().isoformat(),
            'position_id': position.id,
            'symbol': candidate.symbol,
            'structure_type': structure.structure_type.value,
            'is_credit': structure.entry_credit is not None,
            'contracts': candidate.recommended_contracts,
            'fill_cashflow_dollars': fill_cashflow_dollars * candidate.recommended_contracts,
            'slippage_dollars': slippage_dollars * candidate.recommended_contracts,
            'max_loss_dollars': structure.max_loss * candidate.recommended_contracts * CONTRACT_MULTIPLIER,
        }
        self.fills.append(fill_record)
        
        return FillResult(
            success=True,
            fill_price_dollars=abs(fill_cashflow_dollars),
            slippage_dollars=slippage_dollars,
            message=f"Filled {candidate.recommended_contracts} contracts at ${abs(fill_cashflow_dollars):.2f}",
            position_id=position.id,
        )
    
    def _calculate_entry_price(self, structure: OptionStructure) -> float:
        """
        Calculate entry price from structure based on fill_at config.
        
        Returns points (not dollars).
        """
        fill_at = self.config.fill_at
        
        # If structure has pre-calculated credit/debit, use that for mid
        if fill_at == "mid":
            if structure.entry_credit:
                return structure.entry_credit
            elif structure.entry_debit:
                return structure.entry_debit
        
        # Calculate from legs based on fill_at
        if not structure.legs:
            if structure.entry_credit:
                return structure.entry_credit
            elif structure.entry_debit:
                return structure.entry_debit
            return 0
        
        total = 0
        for leg in structure.legs:
            contract = leg.contract
            
            if fill_at == "conservative":
                # Conservative = worst-case for us
                if leg.quantity < 0:  # Selling
                    price = contract.bid  # Receive less
                else:  # Buying
                    price = contract.ask  # Pay more
            elif fill_at == "bid":
                price = contract.bid
            elif fill_at == "ask":
                price = contract.ask
            else:  # mid
                price = (contract.bid + contract.ask) / 2
            
            total += price * leg.quantity
        
        return abs(total)
    
    def close_position(
        self,
        position_id: str,
        exit_price_points: Optional[float] = None,
    ) -> FillResult:
        """
        Close a position in paper mode.
        
        SLIPPAGE DIRECTION:
        - Closing credit position = BUY BACK = pay debit = slippage makes us pay MORE
        - Closing debit position = SELL = receive credit = slippage makes us receive LESS
        
        Args:
            position_id: Position to close
            exit_price_points: Exit price in points (absolute value)
            
        Returns:
            FillResult with P&L
        """
        positions = {p.id: p for p in self.portfolio.positions}
        
        if position_id not in positions:
            return FillResult(
                success=False,
                fill_price_dollars=0,
                slippage_dollars=0,
                message=f"Position {position_id} not found",
            )
        
        position = positions[position_id]
        
        # Determine if this was a credit or debit position
        was_credit = position.entry_price > 0  # Positive entry = received credit
        
        if exit_price_points is None:
            # Estimate exit price (would need current option prices)
            # Use current_value if available, else estimate 50% decay
            if position.current_value is not None:
                exit_price_points = abs(position.current_value)
            else:
                exit_price_points = abs(position.entry_price) * 0.5
        
        exit_price_dollars = exit_price_points * CONTRACT_MULTIPLIER
        slippage_dollars = exit_price_dollars * self.config.slippage_pct / 100
        
        # Apply slippage based on position type
        if was_credit:
            # Closing credit = BUY BACK = pay debit
            # Slippage means we PAY MORE (price goes up)
            exit_cashflow_dollars = -(exit_price_dollars + slippage_dollars)  # negative (paying)
        else:
            # Closing debit = SELL = receive credit
            # Slippage means we RECEIVE LESS (price goes down)
            exit_cashflow_dollars = exit_price_dollars - slippage_dollars  # positive (receiving)
        
        # Convert to points for portfolio
        exit_cashflow_per_contract_points = exit_cashflow_dollars / CONTRACT_MULTIPLIER
        
        # Close in portfolio
        pnl_dollars = self.portfolio.close_position(position_id, exit_cashflow_per_contract_points)
        
        # Update limit tracker
        self.limit_tracker.update_pnl(pnl_dollars)
        
        return FillResult(
            success=True,
            fill_price_dollars=abs(exit_cashflow_dollars),
            slippage_dollars=slippage_dollars,
            message=f"Closed for P&L: ${pnl_dollars:.2f}",
        )
    
    def mark_to_market(
        self,
        current_exit_prices: dict[str, float],  # position_id -> current exit price in points
    ):
        """
        Update position values with current prices.
        
        Args:
            current_exit_prices: Current exit price (points) per contract by position ID
        """
        self.portfolio.update_position_values(current_exit_prices)
    
    def get_summary(self) -> dict:
        """Get simulator summary."""
        return {
            'positions_open': self.portfolio.position_count,
            'total_max_loss_dollars': self.portfolio.get_total_max_loss_dollars(),
            'unrealized_pnl_dollars': self.portfolio.get_total_unrealized_pnl_dollars(),
            'realized_pnl_dollars': self.portfolio._realized_pnl_dollars,
            'realized_pnl_today_dollars': self.portfolio._realized_pnl_today_dollars,
            'fills_count': len(self.fills),
            'trading_allowed': self.limit_tracker.is_trading_allowed(),
        }
