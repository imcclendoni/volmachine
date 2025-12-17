"""
VolMachine Execution Module.

Paper trading execution layer with human-in-the-loop control.

HARD RULES (NON-NEGOTIABLE):
1. PAPER MODE ONLY
2. NO AUTO-EXECUTION
3. ALL TRADES REQUIRE MANUAL CONFIRMATION
4. SYSTEM TERMINATES IF LIVE MODE DETECTED
"""

from execution.config import (
    ExecutionConfig,
    ExecutionBlocked,
    LiveTradingAttempted,
    validate_execution_config,
    get_execution_config,
)

from execution.plan import (
    ExecutionPlan,
    ExecutionLeg,
    create_execution_plan_from_candidate,
)

from execution.gate import ExecutionGate

from execution.audit import (
    log_execution_attempt,
    log_confirmation_attempt,
    log_kill_switch_triggered,
    get_audit_log_entries,
)

from execution.ibkr_validator import (
    IBKRValidationResult,
    validate_for_ibkr,
    validate_account_is_paper,
)

from execution.ibkr_order_client import (
    IBKROrderClient,
    OrderTicket,
    ResolvedLeg,
    OrderStatus,
    LiveTradingBlocked,
    get_ibkr_client,
    reset_ibkr_client,
)

from execution.blotter import (
    PaperTrade,
    TradeLeg,
    Blotter,
    get_blotter,
    create_trade_from_candidate,
)


__all__ = [
    # Config
    'ExecutionConfig',
    'ExecutionBlocked',
    'LiveTradingAttempted',
    'validate_execution_config',
    'get_execution_config',
    
    # Plan
    'ExecutionPlan',
    'ExecutionLeg',
    'create_execution_plan_from_candidate',
    
    # Gate
    'ExecutionGate',
    
    # Audit
    'log_execution_attempt',
    'log_confirmation_attempt',
    'log_kill_switch_triggered',
    'get_audit_log_entries',
    
    # IBKR Validation
    'IBKRValidationResult',
    'validate_for_ibkr',
    'validate_account_is_paper',
    
    # Blotter
    'PaperTrade',
    'TradeLeg',
    'Blotter',
    'get_blotter',
    'create_trade_from_candidate',
]

