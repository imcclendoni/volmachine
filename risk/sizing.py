"""
Position Sizing Calculator.

Calculates appropriate position size based on account equity,
risk parameters, and structure max loss.
"""

from dataclasses import dataclass
from typing import Optional
import math

from data.schemas import OptionStructure


@dataclass
class SizingConfig:
    """Configuration for position sizing."""
    
    # Account
    account_equity: float = 100000
    
    # Per-trade risk (in dollars)
    max_risk_per_trade_pct: float = 1.0  # 1% of equity
    max_contracts: int = 50  # Hard cap on contracts
    
    # What-if risk percentages for decision support
    what_if_risk_pcts: list[float] = None  # e.g., [2.0, 5.0, 10.0]
    
    # Portfolio-level
    max_total_risk_pct: float = 10.0  # Total portfolio risk cap
    max_trades_open: int = 10
    
    # Contract multiplier
    contract_multiplier: int = 100
    
    def __post_init__(self):
        if self.what_if_risk_pcts is None:
            self.what_if_risk_pcts = [1.0, 2.0, 5.0, 10.0]  # Full sizing ladder


@dataclass
class SizingResult:
    """Result of position sizing calculation."""
    
    recommended_contracts: int
    risk_per_contract_dollars: float  # In dollars per contract
    total_risk_dollars: float  # In dollars
    risk_pct_of_equity: float
    
    # Limits
    capped: bool = False
    cap_reason: Optional[str] = None
    
    # Approval
    allowed: bool = True
    rejection_reason: Optional[str] = None
    
    # What-if sizing at 2%, 5%, 10% for decision support
    # Format: {"2%": {"contracts": N, "risk_dollars": X, "allowed": bool, "reason": str}}
    what_if_sizes: Optional[dict] = None


def calculate_size(
    structure: OptionStructure,
    config: Optional[SizingConfig] = None,
    current_portfolio_risk_dollars: float = 0,
    current_open_trades: int = 0,
) -> SizingResult:
    """
    Calculate recommended position size for a structure.
    
    Formula: contracts = floor((equity * risk_pct) / max_loss_per_contract)
    
    CRITICAL: If max_loss_per_contract exceeds per-trade risk cap, reject entirely.
    Do NOT force min 1 contract.
    
    Args:
        structure: Option structure with max_loss defined (in points per contract)
        config: Sizing configuration
        current_portfolio_risk_dollars: Current total portfolio risk in $
        current_open_trades: Number of currently open trades
        
    Returns:
        SizingResult with recommendation and limits info
    """
    if config is None:
        config = SizingConfig()
    
    # Get max loss per contract (in dollars)
    # structure.max_loss is in points (e.g., $5 width spread = 5 points)
    if structure.max_loss is None or structure.max_loss <= 0:
        return SizingResult(
            recommended_contracts=0,
            risk_per_contract_dollars=0,
            total_risk_dollars=0,
            risk_pct_of_equity=0,
            allowed=False,
            rejection_reason="Max loss not defined"
        )
    
    max_loss_per_contract_dollars = structure.max_loss * config.contract_multiplier
    
    # Calculate max allowable risk for this trade in dollars
    max_risk_per_trade_dollars = config.account_equity * config.max_risk_per_trade_pct / 100
    
    # CRITICAL FIX: If even 1 contract exceeds per-trade risk cap, reject
    if max_loss_per_contract_dollars > max_risk_per_trade_dollars:
        return SizingResult(
            recommended_contracts=0,
            risk_per_contract_dollars=max_loss_per_contract_dollars,
            total_risk_dollars=0,
            risk_pct_of_equity=0,
            allowed=False,
            rejection_reason=f"Single contract risk ${max_loss_per_contract_dollars:.2f} exceeds per-trade cap ${max_risk_per_trade_dollars:.2f}"
        )
    
    # Calculate base size
    raw_contracts = max_risk_per_trade_dollars / max_loss_per_contract_dollars
    
    # Floor to get whole contracts - do NOT force min 1
    contracts = int(math.floor(raw_contracts))
    
    if contracts <= 0:
        return SizingResult(
            recommended_contracts=0,
            risk_per_contract_dollars=max_loss_per_contract_dollars,
            total_risk_dollars=0,
            risk_pct_of_equity=0,
            allowed=False,
            rejection_reason="Trade too large for risk allocation"
        )
    
    # Apply caps
    capped = False
    cap_reason = None
    
    # Max contracts cap
    if contracts > config.max_contracts:
        contracts = config.max_contracts
        capped = True
        cap_reason = f"Capped at max {config.max_contracts} contracts"
    
    # Check portfolio risk capacity
    max_portfolio_risk_dollars = config.account_equity * config.max_total_risk_pct / 100
    available_risk_dollars = max_portfolio_risk_dollars - current_portfolio_risk_dollars
    
    if available_risk_dollars <= 0:
        return SizingResult(
            recommended_contracts=0,
            risk_per_contract_dollars=max_loss_per_contract_dollars,
            total_risk_dollars=0,
            risk_pct_of_equity=0,
            allowed=False,
            rejection_reason="Portfolio risk limit reached"
        )
    
    new_trade_risk_dollars = contracts * max_loss_per_contract_dollars
    
    if new_trade_risk_dollars > available_risk_dollars:
        # Reduce size to fit
        contracts = int(math.floor(available_risk_dollars / max_loss_per_contract_dollars))
        capped = True
        cap_reason = f"Reduced to fit portfolio risk limit (${max_portfolio_risk_dollars:.0f})"
        
        if contracts <= 0:
            return SizingResult(
                recommended_contracts=0,
                risk_per_contract_dollars=max_loss_per_contract_dollars,
                total_risk_dollars=0,
                risk_pct_of_equity=0,
                allowed=False,
                rejection_reason="Insufficient portfolio risk capacity"
            )
    
    # Check trade count
    if current_open_trades >= config.max_trades_open:
        return SizingResult(
            recommended_contracts=0,
            risk_per_contract_dollars=max_loss_per_contract_dollars,
            total_risk_dollars=0,
            risk_pct_of_equity=0,
            allowed=False,
            rejection_reason=f"Max open trades ({config.max_trades_open}) reached"
        )
    
    # Calculate final values
    total_risk_dollars = contracts * max_loss_per_contract_dollars
    risk_pct = total_risk_dollars / config.account_equity * 100
    
    # Compute what-if sizing at alternative risk levels
    what_if_sizes = _compute_what_if_sizes(
        max_loss_per_contract_dollars=max_loss_per_contract_dollars,
        config=config,
        current_portfolio_risk_dollars=current_portfolio_risk_dollars,
    )
    
    return SizingResult(
        recommended_contracts=contracts,
        risk_per_contract_dollars=max_loss_per_contract_dollars,
        total_risk_dollars=total_risk_dollars,
        risk_pct_of_equity=risk_pct,
        capped=capped,
        cap_reason=cap_reason,
        allowed=True,
        what_if_sizes=what_if_sizes,
    )


def _compute_what_if_sizes(
    max_loss_per_contract_dollars: float,
    config: SizingConfig,
    current_portfolio_risk_dollars: float = 0,
) -> dict:
    """
    Compute what-if sizing at alternative risk percentages.
    
    This is for decision support only - shows what sizing would look like
    at 2%, 5%, 10% risk caps.
    
    Returns:
        Dict like {"2%": {"contracts": N, "risk_dollars": X, "allowed": bool, "reason": str}}
    """
    what_if = {}
    
    for risk_pct in config.what_if_risk_pcts:
        label = f"{risk_pct:.0f}%"
        
        # Calculate max allowable risk for this what-if level
        max_risk_dollars = config.account_equity * risk_pct / 100
        
        # Check if even 1 contract exceeds this cap
        if max_loss_per_contract_dollars > max_risk_dollars:
            what_if[label] = {
                "contracts": 0,
                "risk_dollars": 0.0,
                "allowed": False,
                "reason": f"Single contract ${max_loss_per_contract_dollars:.0f} exceeds {label} cap ${max_risk_dollars:.0f}",
            }
            continue
        
        # Calculate how many contracts fit
        raw_contracts = max_risk_dollars / max_loss_per_contract_dollars
        contracts = int(math.floor(raw_contracts))
        
        if contracts <= 0:
            what_if[label] = {
                "contracts": 0,
                "risk_dollars": 0.0,
                "allowed": False,
                "reason": "Trade too large for risk allocation",
            }
            continue
        
        # Apply max contracts cap
        if contracts > config.max_contracts:
            contracts = config.max_contracts
        
        # Check portfolio risk capacity (use same max_total_risk_pct)
        max_portfolio_risk_dollars = config.account_equity * config.max_total_risk_pct / 100
        available_risk_dollars = max_portfolio_risk_dollars - current_portfolio_risk_dollars
        
        total_trade_risk = contracts * max_loss_per_contract_dollars
        
        if total_trade_risk > available_risk_dollars:
            # Reduce to fit
            contracts = int(math.floor(available_risk_dollars / max_loss_per_contract_dollars))
            if contracts <= 0:
                what_if[label] = {
                    "contracts": 0,
                    "risk_dollars": 0.0,
                    "allowed": False,
                    "reason": "Insufficient portfolio risk capacity",
                }
                continue
            total_trade_risk = contracts * max_loss_per_contract_dollars
        
        what_if[label] = {
            "contracts": contracts,
            "risk_dollars": total_trade_risk,
            "allowed": True,
            "reason": "",
        }
    
    return what_if


def calculate_kelly_size(
    win_rate: float,
    avg_win_dollars: float,
    avg_loss_dollars: float,
    max_kelly_fraction: float = 0.25,
) -> float:
    """
    Calculate Kelly criterion position size.
    
    Kelly % = W - [(1-W) / R]
    Where W = win rate, R = win/loss ratio
    
    Args:
        win_rate: Historical win rate (0-1)
        avg_win_dollars: Average winning trade in dollars
        avg_loss_dollars: Average losing trade in dollars
        max_kelly_fraction: Maximum fraction of Kelly to use (default 25%)
        
    Returns:
        Recommended position size as fraction of bankroll
    """
    if avg_loss_dollars <= 0 or win_rate <= 0 or win_rate >= 1:
        return 0.0
    
    win_loss_ratio = avg_win_dollars / avg_loss_dollars
    
    kelly = win_rate - ((1 - win_rate) / win_loss_ratio)
    
    # Cap at fraction of Kelly (full Kelly is aggressive)
    kelly = max(min(kelly * max_kelly_fraction, 0.25), 0)
    
    return kelly


def get_portfolio_risk_summary(
    positions: list,  # List of Position objects
    config: Optional[SizingConfig] = None,
) -> dict:
    """
    Get summary of portfolio risk utilization.
    
    Args:
        positions: List of open positions
        config: Sizing configuration
        
    Returns:
        Dictionary with risk metrics (all dollar values explicit)
    """
    if config is None:
        config = SizingConfig()
    
    total_risk_dollars = sum(
        p.entry_max_loss * p.contracts * config.contract_multiplier
        for p in positions
    )
    
    max_portfolio_risk_dollars = config.account_equity * config.max_total_risk_pct / 100
    
    return {
        'total_positions': len(positions),
        'max_positions': config.max_trades_open,
        'positions_remaining': config.max_trades_open - len(positions),
        'total_risk_dollars': total_risk_dollars,
        'max_risk_dollars': max_portfolio_risk_dollars,
        'risk_used_pct': total_risk_dollars / max_portfolio_risk_dollars * 100 if max_portfolio_risk_dollars > 0 else 0,
        'risk_remaining_dollars': max_portfolio_risk_dollars - total_risk_dollars,
    }
