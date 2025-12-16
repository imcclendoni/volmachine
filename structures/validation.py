"""
Structure Validation.

Validates option structures for:
- Defined risk (max loss computable)
- Margin sanity
- Liquidity requirements
- Other sanity checks
"""

from dataclasses import dataclass
from typing import Optional

from data.schemas import (
    OptionStructure,
    StructureType,
)


@dataclass
class ValidationResult:
    """Result of structure validation."""
    
    is_valid: bool
    messages: list[str]
    warnings: list[str]
    
    # Specific checks
    is_defined_risk: bool = True
    passes_liquidity: bool = True
    passes_margin: bool = True
    passes_sanity: bool = True


@dataclass
class ValidationConfig:
    """Configuration for structure validation."""
    
    # Liquidity requirements
    min_volume: int = 10
    min_open_interest: int = 100
    max_bid_ask_pct: float = 10.0  # 10% max spread
    
    # CRITICAL: If True, liquidity failure = FAIL (not warn)
    # Otherwise report may say TRADE but builder would never fill live
    enforce_liquidity: bool = True
    
    # Risk requirements
    max_loss_required: bool = True  # Must have defined max loss
    max_loss_cap: float = 10000  # Max loss per contract cap
    
    # Margin
    estimate_margin: bool = True
    max_margin_multiplier: float = 5.0  # Max margin vs max loss
    
    # Other
    allow_negative_credit: bool = False
    min_reward_risk_ratio: float = 0.0  # 0 = no minimum


def validate_defined_risk(
    structure: OptionStructure,
    config: Optional[ValidationConfig] = None,
) -> tuple[bool, list[str]]:
    """
    Validate that a structure has defined risk.
    
    Returns:
        Tuple of (is_valid, messages)
    """
    if config is None:
        config = ValidationConfig()
    
    messages = []
    
    # Check max loss is defined
    if structure.max_loss is None or structure.max_loss <= 0:
        messages.append("Max loss is not defined or invalid")
        return False, messages
    
    # Check max loss is reasonable
    if structure.max_loss > config.max_loss_cap:
        messages.append(f"Max loss ${structure.max_loss:.2f} exceeds cap ${config.max_loss_cap:.2f}")
        return False, messages
    
    # For certain structure types, verify max loss calculation
    if structure.structure_type == StructureType.CREDIT_SPREAD:
        # Credit spread: max loss = width - credit
        if structure.entry_credit and len(structure.legs) == 2:
            strikes = [leg.contract.strike for leg in structure.legs]
            width = abs(strikes[0] - strikes[1])
            expected_max_loss = width - structure.entry_credit
            if abs(structure.max_loss - expected_max_loss) > 0.01:
                messages.append(
                    f"Max loss mismatch: calculated {expected_max_loss:.2f}, "
                    f"stated {structure.max_loss:.2f}"
                )
    
    if structure.structure_type == StructureType.IRON_CONDOR:
        # Iron condor: max loss = wing width - credit
        # All wings should be same width
        pass  # Complex validation
    
    return True, messages


def validate_liquidity(
    structure: OptionStructure,
    config: Optional[ValidationConfig] = None,
) -> tuple[bool, list[str]]:
    """
    Validate that all legs meet liquidity requirements.
    
    Returns:
        Tuple of (is_valid, messages)
    """
    if config is None:
        config = ValidationConfig()
    
    messages = []
    
    for i, leg in enumerate(structure.legs):
        contract = leg.contract
        
        # Check volume
        if contract.volume < config.min_volume:
            messages.append(
                f"Leg {i+1} ({contract.strike} {contract.option_type.value}): "
                f"volume {contract.volume} < min {config.min_volume}"
            )
        
        # Check OI
        if contract.open_interest < config.min_open_interest:
            messages.append(
                f"Leg {i+1} ({contract.strike} {contract.option_type.value}): "
                f"OI {contract.open_interest} < min {config.min_open_interest}"
            )
        
        # Check bid-ask spread
        if contract.bid_ask_pct > config.max_bid_ask_pct:
            messages.append(
                f"Leg {i+1} ({contract.strike} {contract.option_type.value}): "
                f"bid-ask {contract.bid_ask_pct:.1f}% > max {config.max_bid_ask_pct}%"
            )
    
    return len(messages) == 0, messages


def estimate_margin_requirement(
    structure: OptionStructure,
) -> float:
    """
    Estimate margin requirement for a structure.
    
    This is a rough approximation. Actual margin depends on broker.
    
    Returns:
        Estimated margin requirement per contract
    """
    # For defined-risk spreads, margin ≈ max loss
    if structure.structure_type in [
        StructureType.CREDIT_SPREAD,
        StructureType.DEBIT_SPREAD,
        StructureType.IRON_CONDOR,
        StructureType.IRON_BUTTERFLY,
        StructureType.BUTTERFLY,
    ]:
        return structure.max_loss
    
    # For calendars/diagonals, margin ≈ debit paid
    if structure.structure_type in [StructureType.CALENDAR, StructureType.DIAGONAL]:
        if structure.entry_debit:
            return structure.entry_debit
        return structure.max_loss
    
    # Default: max loss
    return structure.max_loss


def validate_margin(
    structure: OptionStructure,
    account_equity: float,
    max_margin_pct: float = 0.50,
    config: Optional[ValidationConfig] = None,
) -> tuple[bool, list[str]]:
    """
    Validate that margin requirement is reasonable.
    
    Args:
        structure: Option structure
        account_equity: Account equity for context
        max_margin_pct: Max margin as percent of equity
        config: Validation config
        
    Returns:
        Tuple of (is_valid, messages)
    """
    if config is None:
        config = ValidationConfig()
    
    messages = []
    
    margin = estimate_margin_requirement(structure) * 100  # Per contract in $
    max_margin = account_equity * max_margin_pct
    
    if margin > max_margin:
        messages.append(
            f"Margin ${margin:.2f} exceeds {max_margin_pct:.0%} "
            f"of equity (${max_margin:.2f})"
        )
        return False, messages
    
    # Check margin vs max loss ratio
    if structure.max_loss and margin > structure.max_loss * config.max_margin_multiplier * 100:
        messages.append(
            f"Margin ${margin:.2f} is {margin / (structure.max_loss * 100):.1f}x max loss"
        )
    
    return True, messages


def validate_sanity(
    structure: OptionStructure,
    config: Optional[ValidationConfig] = None,
) -> tuple[bool, list[str]]:
    """
    Perform sanity checks on structure.
    
    Returns:
        Tuple of (is_valid, messages)
    """
    if config is None:
        config = ValidationConfig()
    
    messages = []
    
    # Check we have legs
    if not structure.legs:
        messages.append("Structure has no legs")
        return False, messages
    
    # Check all legs are for same symbol
    symbols = set(leg.contract.symbol for leg in structure.legs)
    if len(symbols) > 1:
        messages.append(f"Mixed symbols in structure: {symbols}")
        return False, messages
    
    # Check expirations make sense
    expirations = sorted(set(leg.contract.expiration for leg in structure.legs))
    if structure.structure_type not in [StructureType.CALENDAR, StructureType.DIAGONAL]:
        # Non-calendar should have same expiration
        if len(expirations) > 1:
            messages.append(f"Multiple expirations in non-calendar: {expirations}")
            return False, messages
    
    # Check credit/debit makes sense
    if structure.entry_credit and structure.entry_credit < 0:
        messages.append("Negative credit (should be debit?)")
        return False, messages
    
    if structure.entry_debit and structure.entry_debit < 0:
        messages.append("Negative debit (should be credit?)")
        return False, messages
    
    # Check reward/risk if configured
    if config.min_reward_risk_ratio > 0 and structure.max_profit:
        ratio = structure.max_profit / structure.max_loss if structure.max_loss > 0 else 0
        if ratio < config.min_reward_risk_ratio:
            messages.append(
                f"Reward/risk ratio {ratio:.2f} below minimum {config.min_reward_risk_ratio}"
            )
    
    return len(messages) == 0, messages


def validate_structure(
    structure: OptionStructure,
    account_equity: float = 100000,
    config: Optional[ValidationConfig] = None,
) -> ValidationResult:
    """
    Perform full validation of a structure.
    
    Args:
        structure: Option structure to validate
        account_equity: Account equity
        config: Validation configuration
        
    Returns:
        ValidationResult with detailed feedback
    """
    if config is None:
        config = ValidationConfig()
    
    all_messages = []
    warnings = []
    
    # Defined risk check
    is_defined_risk, dr_messages = validate_defined_risk(structure, config)
    all_messages.extend(dr_messages)
    
    # Liquidity check
    # CRITICAL: If enforce_liquidity=True, liquidity failure = FAIL (not warn)
    # Otherwise report may say TRADE but builder would never fill live
    passes_liquidity, liq_messages = validate_liquidity(structure, config)
    if config.enforce_liquidity and liq_messages:
        all_messages.extend(liq_messages)  # Hard failure
    elif liq_messages:
        warnings.extend(liq_messages)  # Soft warning only
    
    # Margin check
    passes_margin, margin_messages = validate_margin(structure, account_equity, config=config)
    all_messages.extend(margin_messages)
    
    # Sanity check
    passes_sanity, sanity_messages = validate_sanity(structure, config)
    all_messages.extend(sanity_messages)
    
    # Overall validity
    # Include liquidity in validity check when enforced
    liquidity_ok = passes_liquidity or not config.enforce_liquidity
    is_valid = is_defined_risk and passes_margin and passes_sanity and liquidity_ok
    
    return ValidationResult(
        is_valid=is_valid,
        messages=all_messages,
        warnings=warnings,
        is_defined_risk=is_defined_risk,
        passes_liquidity=passes_liquidity,
        passes_margin=passes_margin,
        passes_sanity=passes_sanity,
    )
