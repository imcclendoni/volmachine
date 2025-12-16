"""
IBKR Validator Module.

Dry-run validation - does NOT submit orders.
Validates that execution plans are compatible with IBKR before confirmation.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from execution.config import LiveTradingAttempted
from execution.plan import ExecutionPlan


@dataclass
class IBKRValidationResult:
    """Result of IBKR dry-run validation."""
    is_valid: bool
    messages: list[str]
    account_type: Optional[str] = None
    margin_requirement: Optional[float] = None


def validate_for_ibkr(
    plan: ExecutionPlan,
    ibkr_client = None,
) -> IBKRValidationResult:
    """
    Pre-submission validation - does NOT submit orders.
    
    Checks:
    1. Account type must be PAPER
    2. Combo legs are valid
    3. Margin <= max_loss
    4. Expirations and strikes exist
    
    Args:
        plan: The execution plan to validate
        ibkr_client: Optional IBKR client for live validation
    
    Returns:
        IBKRValidationResult with validation status and messages
    """
    checks = []
    account_type = None
    margin_req = None
    
    # If no client provided, do static validation only
    if ibkr_client is None:
        return _static_validation(plan)
    
    try:
        # 1. Account type must be PAPER
        account_type = ibkr_client.get_account_type()
        if account_type != "PAPER":
            raise LiveTradingAttempted(
                f"LIVE ACCOUNT DETECTED ({account_type}) - TERMINATING"
            )
        
        # 2. Validate combo legs exist in option chain
        for leg in plan.legs:
            contract = ibkr_client.qualify_contract(
                symbol=plan.symbol,
                option_type=leg.option_type,
                strike=leg.strike,
                expiration=leg.expiration,
            )
            if not contract:
                checks.append(f"Leg not found: {leg.strike} {leg.option_type} {leg.expiration}")
        
        # 3. Check margin requirement
        margin_req = ibkr_client.check_margin(plan) if hasattr(ibkr_client, 'check_margin') else None
        if margin_req and margin_req > plan.max_loss_dollars:
            checks.append(f"Margin {margin_req:.0f} > max_loss {plan.max_loss_dollars:.0f}")
        
    except LiveTradingAttempted:
        raise  # Re-raise kill switch
    except Exception as e:
        checks.append(f"IBKR validation error: {str(e)}")
    
    return IBKRValidationResult(
        is_valid=len(checks) == 0,
        messages=checks,
        account_type=account_type,
        margin_requirement=margin_req,
    )


def _static_validation(plan: ExecutionPlan) -> IBKRValidationResult:
    """
    Static validation without IBKR connection.
    
    Validates plan structure and parameters.
    """
    checks = []
    
    # Check required fields
    if not plan.symbol:
        checks.append("Symbol is required")
    
    if not plan.legs:
        checks.append("At least one leg is required")
    
    # Validate each leg
    for i, leg in enumerate(plan.legs):
        if leg.strike <= 0:
            checks.append(f"Leg {i+1}: Invalid strike {leg.strike}")
        if leg.quantity <= 0:
            checks.append(f"Leg {i+1}: Invalid quantity {leg.quantity}")
        if leg.option_type not in ("PUT", "CALL"):
            checks.append(f"Leg {i+1}: Invalid option_type {leg.option_type}")
        if leg.action not in ("BUY", "SELL"):
            checks.append(f"Leg {i+1}: Invalid action {leg.action}")
    
    # Check risk parameters
    if plan.max_loss_dollars <= 0:
        checks.append("Max loss must be positive")
    
    if plan.order_type != "LMT":
        checks.append(f"Only LMT orders allowed, got {plan.order_type}")
    
    if plan.exchange != "SMART":
        checks.append(f"Exchange must be SMART, got {plan.exchange}")
    
    if plan.currency != "USD":
        checks.append(f"Currency must be USD, got {plan.currency}")
    
    return IBKRValidationResult(
        is_valid=len(checks) == 0,
        messages=checks,
        account_type="PAPER",  # Assume paper for static validation
        margin_requirement=None,
    )


def validate_account_is_paper(ibkr_client) -> bool:
    """
    Validate that the connected IBKR account is PAPER.
    
    Kill switch - terminates if live account detected.
    """
    if ibkr_client is None:
        return True  # Assume paper if no client
    
    try:
        account_type = ibkr_client.get_account_type()
        if account_type != "PAPER":
            raise LiveTradingAttempted(
                f"LIVE ACCOUNT DETECTED ({account_type}) - TERMINATING"
            )
        return True
    except LiveTradingAttempted:
        raise
    except Exception:
        # Connection issues - assume paper but log warning
        print("WARNING: Could not verify account type")
        return True
