"""
Portfolio Management.

Tracks open positions and aggregate portfolio risk.

FIXES:
- Cashflow-based PnL: store entry_cashflow (positive=received, negative=paid)
- PnL = exit_cashflow - entry_cashflow (works for both credits and debits)
- Use run_date instead of date.today() where applicable
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from uuid import uuid4

from data.schemas import (
    Position,
    OptionStructure,
    PortfolioState,
    TradeCandidate,
)
from structures.greeks import PositionGreeks, calculate_structure_greeks
from risk.sizing import SizingConfig, get_portfolio_risk_summary


class Portfolio:
    """
    Portfolio manager for option positions.
    
    Tracks open positions, aggregate Greeks, and risk metrics.
    
    PnL Convention:
    - entry_cashflow: positive = credit received, negative = debit paid
    - exit_cashflow: positive = credit received, negative = debit paid
    - pnl = exit_cashflow - entry_cashflow (correct for both directions)
    """
    
    def __init__(
        self,
        account_equity: float = 100000,
        sizing_config: Optional[SizingConfig] = None,
    ):
        self.account_equity = account_equity
        self.sizing_config = sizing_config or SizingConfig(account_equity=account_equity)
        
        # Positions
        self._positions: dict[str, Position] = {}
        
        # P&L tracking (all in dollars)
        self._realized_pnl_dollars: float = 0
        self._realized_pnl_today_dollars: float = 0
        self._last_pnl_date: Optional[date] = None
    
    @property
    def positions(self) -> list[Position]:
        """Get list of open positions."""
        return list(self._positions.values())
    
    @property
    def position_count(self) -> int:
        """Get number of open positions."""
        return len(self._positions)
    
    def add_position(
        self,
        structure: OptionStructure,
        contracts: int,
        entry_cashflow_per_contract: float,
        trade_candidate_id: str = "",
        as_of_date: Optional[date] = None,
    ) -> Position:
        """
        Add a new position to the portfolio.
        
        Args:
            structure: Option structure
            contracts: Number of contracts
            entry_cashflow_per_contract: Cashflow at entry per contract
                - Positive = credit received (e.g., +1.50 for credit spread)
                - Negative = debit paid (e.g., -2.00 for debit spread)
            trade_candidate_id: ID of trade candidate that generated this
            as_of_date: Reference date (default: today)
            
        Returns:
            Created Position
        """
        ref_date = as_of_date or date.today()
        
        position = Position(
            id=str(uuid4()),
            entry_timestamp=datetime.now(),
            symbol=structure.symbol,
            structure=structure,
            contracts=contracts,
            entry_price=entry_cashflow_per_contract,  # Cashflow-based
            entry_max_loss=structure.max_loss,
            trade_candidate_id=trade_candidate_id,
        )
        
        self._positions[position.id] = position
        return position
    
    def close_position(
        self,
        position_id: str,
        exit_cashflow_per_contract: float,
        as_of_date: Optional[date] = None,
    ) -> float:
        """
        Close a position.
        
        Args:
            position_id: ID of position to close
            exit_cashflow_per_contract: Cashflow at exit per contract
                - For credits: typically negative (pay to close)
                - For debits: typically positive (sell to close)
            as_of_date: Reference date for PnL tracking
            
        Returns:
            Realized P&L in dollars
        """
        if position_id not in self._positions:
            raise ValueError(f"Position {position_id} not found")
        
        ref_date = as_of_date or date.today()
        position = self._positions[position_id]
        
        # Calculate P&L using cashflow convention
        # entry_price is entry_cashflow (positive = received, negative = paid)
        # exit_cashflow is exit cashflow (positive = received, negative = paid)
        # PnL = exit - entry (works correctly for both directions)
        #
        # Credit spread example:
        #   Entry: received $1.50 -> entry_cashflow = +1.50
        #   Exit: paid $0.50 to close -> exit_cashflow = -0.50
        #   PnL = -0.50 - 1.50 = -2.00? NO, that's wrong
        #   
        # Actually for credit spreads:
        #   Entry cashflow = +$1.50 (received)
        #   Exit cashflow = -$0.50 (paid to buy back)
        #   Net cashflow = +1.50 + (-0.50) = +$1.00 profit
        #   PnL = entry + exit = 1.50 + (-0.50) = 1.00 ✓
        #
        # For debit spreads:
        #   Entry cashflow = -$2.00 (paid)
        #   Exit cashflow = +$3.00 (received)
        #   Net cashflow = -2.00 + 3.00 = +$1.00 profit
        #   PnL = entry + exit = -2.00 + 3.00 = 1.00 ✓
        #
        # So PnL = entry_cashflow + exit_cashflow
        
        pnl_per_contract = position.entry_price + exit_cashflow_per_contract
        pnl_dollars = pnl_per_contract * position.contracts * 100
        
        # Update realized P&L
        self._update_realized_pnl(pnl_dollars, ref_date)
        
        # Remove position
        del self._positions[position_id]
        
        return pnl_dollars
    
    def _update_realized_pnl(self, pnl_dollars: float, as_of_date: Optional[date] = None):
        """Update realized P&L tracking."""
        ref_date = as_of_date or date.today()
        
        if self._last_pnl_date != ref_date:
            self._realized_pnl_today_dollars = 0
            self._last_pnl_date = ref_date
        
        self._realized_pnl_dollars += pnl_dollars
        self._realized_pnl_today_dollars += pnl_dollars
    
    def update_position_values(
        self,
        current_cashflows: dict[str, float],  # position_id -> current exit cashflow per contract
        as_of_date: Optional[date] = None,
    ):
        """
        Update mark-to-market values for positions.
        
        Args:
            current_cashflows: Current exit cashflow per contract by position ID
                - Positive = would receive if closed
                - Negative = would pay if closed
            as_of_date: Reference date
        """
        ref_date = as_of_date or date.today()
        
        for pos_id, current_exit_cashflow in current_cashflows.items():
            if pos_id in self._positions:
                pos = self._positions[pos_id]
                pos.current_value = current_exit_cashflow
                
                # Unrealized PnL using cashflow convention
                unrealized_per_contract = pos.entry_price + current_exit_cashflow
                pos.unrealized_pnl = unrealized_per_contract * pos.contracts * 100
                pos.days_held = (ref_date - pos.entry_timestamp.date()).days
    
    def get_total_max_loss_dollars(self) -> float:
        """Get total max loss across all positions (in dollars)."""
        return sum(
            p.entry_max_loss * p.contracts * 100
            for p in self._positions.values()
        )
    
    # Alias for compatibility
    def get_total_max_loss(self) -> float:
        """Alias for get_total_max_loss_dollars."""
        return self.get_total_max_loss_dollars()
    
    def get_total_unrealized_pnl_dollars(self) -> float:
        """Get total unrealized P&L in dollars."""
        return sum(
            p.unrealized_pnl or 0
            for p in self._positions.values()
        )
    
    def get_aggregate_greeks(
        self,
        underlying_prices: dict[str, float],
    ) -> PositionGreeks:
        """
        Get aggregate Greeks across all positions.
        
        Args:
            underlying_prices: Current prices by symbol
            
        Returns:
            Aggregate PositionGreeks
        """
        total = PositionGreeks(delta=0, gamma=0, theta=0, vega=0, rho=0)
        
        for pos in self._positions.values():
            price = underlying_prices.get(pos.symbol, 0)
            if price > 0:
                greeks = calculate_structure_greeks(pos.structure, price)
                # Scale by contracts
                total = PositionGreeks(
                    delta=total.delta + greeks.delta * pos.contracts,
                    gamma=total.gamma + greeks.gamma * pos.contracts,
                    theta=total.theta + greeks.theta * pos.contracts,
                    vega=total.vega + greeks.vega * pos.contracts,
                    rho=total.rho + greeks.rho * pos.contracts,
                )
        
        return total
    
    def get_state(
        self,
        underlying_prices: Optional[dict[str, float]] = None,
    ) -> PortfolioState:
        """
        Get current portfolio state snapshot.
        
        Args:
            underlying_prices: Current prices for Greeks calculation
            
        Returns:
            PortfolioState
        """
        if underlying_prices is None:
            underlying_prices = {}
        
        # Aggregate Greeks
        greeks = self.get_aggregate_greeks(underlying_prices)
        
        return PortfolioState(
            timestamp=datetime.now(),
            open_positions=self.positions,
            total_max_loss=self.get_total_max_loss_dollars(),
            total_current_risk=self.get_total_max_loss_dollars(),
            portfolio_delta=greeks.delta,
            portfolio_gamma=greeks.gamma,
            portfolio_theta=greeks.theta,
            portfolio_vega=greeks.vega,
            realized_pnl_today=self._realized_pnl_today_dollars,
            unrealized_pnl=self.get_total_unrealized_pnl_dollars(),
            trades_open=self.position_count,
        )
    
    def get_risk_summary(self) -> dict:
        """Get portfolio risk summary."""
        return get_portfolio_risk_summary(
            self.positions,
            self.sizing_config,
        )
    
    def has_position_for_symbol(self, symbol: str) -> bool:
        """Check if there's already a position for a symbol."""
        return any(p.symbol == symbol for p in self._positions.values())
    
    def get_positions_for_symbol(self, symbol: str) -> list[Position]:
        """Get all positions for a symbol."""
        return [p for p in self._positions.values() if p.symbol == symbol]
    
    def to_dict(self) -> dict:
        """Export portfolio as dictionary."""
        return {
            'account_equity_dollars': self.account_equity,
            'position_count': self.position_count,
            'positions': [
                {
                    'id': p.id,
                    'symbol': p.symbol,
                    'structure_type': p.structure.structure_type.value,
                    'contracts': p.contracts,
                    'entry_cashflow': p.entry_price,
                    'entry_max_loss_points': p.entry_max_loss,
                    'days_held': p.days_held,
                    'unrealized_pnl_dollars': p.unrealized_pnl,
                }
                for p in self._positions.values()
            ],
            'total_max_loss_dollars': self.get_total_max_loss_dollars(),
            'unrealized_pnl_dollars': self.get_total_unrealized_pnl_dollars(),
            'realized_pnl_dollars': self._realized_pnl_dollars,
            'realized_pnl_today_dollars': self._realized_pnl_today_dollars,
            'risk_summary': self.get_risk_summary(),
        }
