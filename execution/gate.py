"""
Execution Gate Module.

Human-in-the-loop execution control.
NO timers. NO background submits. NO silent execution.

All trades require EXPLICIT user confirmation.
"""

from datetime import datetime
from typing import Dict, Optional, Set

from execution.config import (
    ExecutionConfig,
    ExecutionBlocked,
    LiveTradingAttempted,
    validate_execution_config,
)
from execution.plan import ExecutionPlan


class ExecutionGate:
    """
    Human-in-the-loop execution control.
    
    HARD RULES:
    - NO timers
    - NO background submits
    - NO silent execution
    - ALL trades require explicit confirmation
    """
    
    def __init__(self, config: Optional[ExecutionConfig] = None):
        """
        Initialize gate with config.
        
        Kill switch is validated on creation.
        """
        self.config = config or ExecutionConfig()
        validate_execution_config(self.config)  # Kill switch
        
        self._confirmed_trades: Set[str] = set()
        self._pending_plans: Dict[str, ExecutionPlan] = {}
        self._confirmation_log: list[dict] = []
    
    def register_plan(self, plan: ExecutionPlan) -> None:
        """
        Register an execution plan for potential confirmation.
        
        Does NOT submit anything - just tracks for UI display.
        """
        # Kill switch check
        if self.config.mode != "paper":
            raise LiveTradingAttempted("LIVE TRADING DISABLED")
        
        plan.status = "PENDING_CONFIRMATION"
        self._pending_plans[plan.id] = plan
    
    def confirm_trade(self, trade_id: str, user_action: str) -> ExecutionPlan:
        """
        User explicitly confirms a trade in UI.
        
        Args:
            trade_id: ID of the execution plan to confirm
            user_action: Must be exactly "CONFIRM"
        
        Returns:
            The confirmed ExecutionPlan
            
        Raises:
            ExecutionBlocked: If confirmation requirements not met
            LiveTradingAttempted: If live mode detected
        """
        # Kill switch - always check
        if self.config.mode != "paper":
            raise LiveTradingAttempted("LIVE TRADING DISABLED")
        
        if self.config.auto_execute:
            raise LiveTradingAttempted("AUTO-EXECUTION DISABLED")
        
        # Require explicit CONFIRM action
        if user_action != "CONFIRM":
            raise ExecutionBlocked(
                f"Manual confirmation required. Got action='{user_action}', expected 'CONFIRM'"
            )
        
        # Find the plan
        if trade_id not in self._pending_plans:
            raise ExecutionBlocked(f"Trade ID '{trade_id}' not found in pending plans")
        
        plan = self._pending_plans[trade_id]
        
        # Update status
        plan.status = "CONFIRMED"
        plan.confirmed_at = datetime.now()
        self._confirmed_trades.add(trade_id)
        
        # Log the confirmation
        self._confirmation_log.append({
            'timestamp': datetime.now().isoformat(),
            'trade_id': trade_id,
            'action': 'CONFIRMED',
            'symbol': plan.symbol,
            'structure': plan.structure_type,
        })
        
        return plan
    
    def cancel_trade(self, trade_id: str) -> None:
        """Cancel a pending trade."""
        if trade_id in self._pending_plans:
            self._pending_plans[trade_id].status = "CANCELLED"
            self._confirmed_trades.discard(trade_id)
            
            self._confirmation_log.append({
                'timestamp': datetime.now().isoformat(),
                'trade_id': trade_id,
                'action': 'CANCELLED',
            })
    
    def can_submit(self, trade_id: str) -> bool:
        """
        Check if trade is confirmed and ready for submission.
        
        Returns:
            True if trade is confirmed and mode is paper
            
        Raises:
            LiveTradingAttempted: If live mode detected
        """
        # Kill switch - ALWAYS check
        if self.config.mode == "live":
            raise LiveTradingAttempted("LIVE TRADING DISABLED - PAPER MODE ONLY")
        
        if trade_id not in self._confirmed_trades:
            return False
        
        plan = self._pending_plans.get(trade_id)
        if not plan:
            return False
        
        return plan.status == "CONFIRMED"
    
    def get_pending_plans(self) -> list[ExecutionPlan]:
        """Get all pending execution plans awaiting confirmation."""
        return [
            p for p in self._pending_plans.values() 
            if p.status == "PENDING_CONFIRMATION"
        ]
    
    def get_confirmed_plans(self) -> list[ExecutionPlan]:
        """Get all confirmed plans ready for submission."""
        return [
            p for p in self._pending_plans.values()
            if p.status == "CONFIRMED"
        ]
    
    def get_plan(self, trade_id: str) -> Optional[ExecutionPlan]:
        """Get a specific execution plan by ID."""
        return self._pending_plans.get(trade_id)
