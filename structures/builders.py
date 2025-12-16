"""
Option Structure Builders.

Creates defined-risk option structures from option chains.
All structures have known max loss at entry.

FIXES:
- Compute actual_width from chosen strikes (not assumed)
- Use conservative bid/ask for credit/debit (worst-case fills)
- Enforce strict liquidity in live mode
- Diagonals disabled (complex max loss)
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional

from data.schemas import (
    OptionChain,
    OptionContract,
    OptionLeg,
    OptionStructure,
    OptionType,
    StructureType,
)
from structures.greeks import update_structure_greeks


@dataclass
class BuilderConfig:
    """Configuration for structure builders."""
    
    # Strike selection
    preferred_width_points: int = 5  # $5 wide spreads
    min_width_points: int = 1
    max_width_points: int = 20
    
    # Expiration preference
    min_dte: int = 7
    max_dte: int = 60
    target_dte: int = 30
    
    # Liquidity filters
    min_volume: int = 10
    min_open_interest: int = 100
    max_bid_ask_pct: float = 10.0  # 10% max spread
    
    # Mode
    enforce_liquidity: bool = True  # Set False for paper/backtest


def find_best_expiration(
    option_chain: OptionChain,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[date]:
    """
    Find the best expiration date matching criteria.
    
    Args:
        option_chain: Option chain
        as_of_date: Reference date (default: chain's as_of or today)
        config: Builder configuration
        
    Returns:
        Best expiration date or None
    """
    if config is None:
        config = BuilderConfig()
    
    # Use chain's as_of_date if available, else provided, else today
    ref_date = as_of_date or getattr(option_chain, 'as_of_date', None) or date.today()
    
    best_exp = None
    best_dte_diff = float('inf')
    
    for exp in option_chain.expirations:
        dte = (exp - ref_date).days
        
        if dte < config.min_dte or dte > config.max_dte:
            continue
        
        dte_diff = abs(dte - config.target_dte)
        if dte_diff < best_dte_diff:
            best_dte_diff = dte_diff
            best_exp = exp
    
    return best_exp


def is_liquid(contract: OptionContract, config: BuilderConfig) -> bool:
    """Check if a contract meets liquidity requirements."""
    if contract.volume < config.min_volume:
        return False
    if contract.open_interest < config.min_open_interest:
        return False
    if contract.bid_ask_pct > config.max_bid_ask_pct:
        return False
    return True


def find_contract(
    option_chain: OptionChain,
    expiration: date,
    strike: float,
    option_type: OptionType,
    config: Optional[BuilderConfig] = None,
    require_liquidity: bool = True,
) -> Optional[OptionContract]:
    """
    Find a contract with optional liquidity check.
    
    If exact strike not found, returns closest available.
    """
    if config is None:
        config = BuilderConfig()
    
    contract = option_chain.get_contract(expiration, strike, option_type)
    
    if contract is None:
        # Find closest
        candidates = [
            c for c in option_chain.contracts
            if c.expiration == expiration and c.option_type == option_type
        ]
        
        if not candidates:
            return None
        
        contract = min(candidates, key=lambda c: abs(c.strike - strike))
    
    # Check liquidity if required
    if require_liquidity and config.enforce_liquidity:
        if not is_liquid(contract, config):
            return None
    
    return contract


def conservative_credit_points(short_contract: OptionContract, long_contract: OptionContract) -> float:
    """
    Calculate credit using conservative pricing.
    Sell at bid (worst case), buy at ask (worst case).
    
    Returns:
        Credit in points (may be negative if not viable)
    """
    credit = short_contract.bid - long_contract.ask
    return credit


def conservative_debit_points(long_contract: OptionContract, short_contract: OptionContract) -> float:
    """
    Calculate debit using conservative pricing.
    Buy at ask (worst case), sell at bid (worst case).
    
    Returns:
        Debit in points (positive = we pay)
    """
    debit = long_contract.ask - short_contract.bid
    return debit


# ============================================================================
# Vertical Spreads
# ============================================================================

def build_credit_spread(
    option_chain: OptionChain,
    option_type: OptionType,  # PUT for bull put, CALL for bear call
    short_strike: float,
    width_points: Optional[int] = None,
    expiration: Optional[date] = None,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[OptionStructure]:
    """
    Build a credit spread.
    
    Credit Put Spread (Bull Put): Sell higher put, buy lower put
    Credit Call Spread (Bear Call): Sell lower call, buy higher call
    
    FIXES:
    - Compute actual_width from chosen strikes
    - Use conservative bid/ask pricing
    
    Args:
        option_chain: Option chain
        option_type: PUT or CALL
        short_strike: Strike to sell
        width_points: Width of spread in strike points
        expiration: Expiration date (default: find best)
        as_of_date: Reference date for DTE calculation
        config: Builder configuration
        
    Returns:
        OptionStructure or None
    """
    if config is None:
        config = BuilderConfig()
    
    if width_points is None:
        width_points = config.preferred_width_points
    
    if expiration is None:
        expiration = find_best_expiration(option_chain, as_of_date, config)
        if expiration is None:
            return None
    
    # Determine long strike based on type
    if option_type == OptionType.PUT:
        # Bull put spread: sell high, buy low
        long_strike = short_strike - width_points
    else:
        # Bear call spread: sell low, buy high
        long_strike = short_strike + width_points
    
    # Get contracts with liquidity check
    require_liq = config.enforce_liquidity
    short_contract = find_contract(option_chain, expiration, short_strike, option_type, config, require_liq)
    long_contract = find_contract(option_chain, expiration, long_strike, option_type, config, require_liq)
    
    if short_contract is None or long_contract is None:
        return None
    
    # Compute ACTUAL width from chosen strikes
    actual_width_points = abs(short_contract.strike - long_contract.strike)
    
    # Build legs
    short_leg = OptionLeg(contract=short_contract, quantity=-1)
    long_leg = OptionLeg(contract=long_contract, quantity=1)
    
    # Calculate entry credit using CONSERVATIVE pricing
    entry_credit_points = conservative_credit_points(short_contract, long_contract)
    
    if entry_credit_points <= 0:
        # Not a credit spread at worst-case fills
        return None
    
    # Max loss = width - credit (in points)
    max_loss_points = actual_width_points - entry_credit_points
    
    if max_loss_points <= 0:
        # Free trade - suspicious, reject
        return None
    
    # Build structure
    structure = OptionStructure(
        structure_type=StructureType.CREDIT_SPREAD,
        symbol=option_chain.symbol,
        legs=[short_leg, long_leg],
        entry_credit=entry_credit_points,
        max_loss=max_loss_points,
        max_profit=entry_credit_points,
        breakevens=[short_contract.strike - entry_credit_points] if option_type == OptionType.PUT 
                   else [short_contract.strike + entry_credit_points],
    )
    
    # Update Greeks
    update_structure_greeks(structure, option_chain.underlying_price)
    
    return structure


def build_debit_spread(
    option_chain: OptionChain,
    option_type: OptionType,  # CALL for bull call, PUT for bear put
    long_strike: float,
    width_points: Optional[int] = None,
    expiration: Optional[date] = None,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[OptionStructure]:
    """
    Build a debit spread.
    
    Debit Call Spread (Bull Call): Buy lower call, sell higher call
    Debit Put Spread (Bear Put): Buy higher put, sell lower put
    
    Args:
        option_chain: Option chain
        option_type: CALL or PUT
        long_strike: Strike to buy
        width_points: Width of spread in points
        expiration: Expiration date
        as_of_date: Reference date for DTE
        config: Builder configuration
        
    Returns:
        OptionStructure or None
    """
    if config is None:
        config = BuilderConfig()
    
    if width_points is None:
        width_points = config.preferred_width_points
    
    if expiration is None:
        expiration = find_best_expiration(option_chain, as_of_date, config)
        if expiration is None:
            return None
    
    # Determine short strike
    if option_type == OptionType.CALL:
        short_strike = long_strike + width_points
    else:
        short_strike = long_strike - width_points
    
    # Get contracts
    require_liq = config.enforce_liquidity
    long_contract = find_contract(option_chain, expiration, long_strike, option_type, config, require_liq)
    short_contract = find_contract(option_chain, expiration, short_strike, option_type, config, require_liq)
    
    if long_contract is None or short_contract is None:
        return None
    
    # Compute ACTUAL width
    actual_width_points = abs(long_contract.strike - short_contract.strike)
    
    # Build legs
    long_leg = OptionLeg(contract=long_contract, quantity=1)
    short_leg = OptionLeg(contract=short_contract, quantity=-1)
    
    # Calculate entry debit using CONSERVATIVE pricing
    entry_debit_points = conservative_debit_points(long_contract, short_contract)
    
    if entry_debit_points <= 0:
        return None
    
    # Max loss = debit paid
    max_loss_points = entry_debit_points
    max_profit_points = actual_width_points - entry_debit_points
    
    if max_profit_points <= 0:
        return None
    
    structure = OptionStructure(
        structure_type=StructureType.DEBIT_SPREAD,
        symbol=option_chain.symbol,
        legs=[long_leg, short_leg],
        entry_debit=entry_debit_points,
        max_loss=max_loss_points,
        max_profit=max_profit_points,
        breakevens=[long_contract.strike + entry_debit_points] if option_type == OptionType.CALL 
                   else [long_contract.strike - entry_debit_points],
    )
    
    update_structure_greeks(structure, option_chain.underlying_price)
    
    return structure


# ============================================================================
# Iron Condor
# ============================================================================

def build_iron_condor(
    option_chain: OptionChain,
    put_short_strike: float,
    call_short_strike: float,
    wing_width_points: Optional[int] = None,
    expiration: Optional[date] = None,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[OptionStructure]:
    """
    Build an iron condor.
    
    Combines a bull put spread and bear call spread.
    Profits if price stays between short strikes.
    
    Args:
        option_chain: Option chain
        put_short_strike: Short put strike (lower)
        call_short_strike: Short call strike (upper)
        wing_width_points: Width of each wing in points
        expiration: Expiration date
        as_of_date: Reference date
        config: Builder configuration
        
    Returns:
        OptionStructure or None
    """
    if config is None:
        config = BuilderConfig()
    
    if wing_width_points is None:
        wing_width_points = config.preferred_width_points
    
    if expiration is None:
        expiration = find_best_expiration(option_chain, as_of_date, config)
        if expiration is None:
            return None
    
    # Calculate target strikes
    put_long_strike = put_short_strike - wing_width_points
    call_long_strike = call_short_strike + wing_width_points
    
    # Get all contracts
    require_liq = config.enforce_liquidity
    put_short = find_contract(option_chain, expiration, put_short_strike, OptionType.PUT, config, require_liq)
    put_long = find_contract(option_chain, expiration, put_long_strike, OptionType.PUT, config, require_liq)
    call_short = find_contract(option_chain, expiration, call_short_strike, OptionType.CALL, config, require_liq)
    call_long = find_contract(option_chain, expiration, call_long_strike, OptionType.CALL, config, require_liq)
    
    if None in [put_short, put_long, call_short, call_long]:
        return None
    
    # Compute ACTUAL widths
    put_width_points = abs(put_short.strike - put_long.strike)
    call_width_points = abs(call_long.strike - call_short.strike)
    max_wing_width_points = max(put_width_points, call_width_points)
    
    # Build legs
    legs = [
        OptionLeg(contract=put_long, quantity=1),
        OptionLeg(contract=put_short, quantity=-1),
        OptionLeg(contract=call_short, quantity=-1),
        OptionLeg(contract=call_long, quantity=1),
    ]
    
    # Calculate entry credit using CONSERVATIVE pricing
    put_credit = conservative_credit_points(put_short, put_long)
    call_credit = conservative_credit_points(call_short, call_long)
    total_credit_points = put_credit + call_credit
    
    if total_credit_points <= 0:
        return None
    
    # Max loss is max wing width minus credit (whichever side loses)
    max_loss_points = max_wing_width_points - total_credit_points
    
    if max_loss_points <= 0:
        return None
    
    structure = OptionStructure(
        structure_type=StructureType.IRON_CONDOR,
        symbol=option_chain.symbol,
        legs=legs,
        entry_credit=total_credit_points,
        max_loss=max_loss_points,
        max_profit=total_credit_points,
        breakevens=[put_short.strike - total_credit_points, call_short.strike + total_credit_points],
    )
    
    update_structure_greeks(structure, option_chain.underlying_price)
    
    return structure


# ============================================================================
# Butterfly
# ============================================================================

def build_butterfly(
    option_chain: OptionChain,
    center_strike: float,
    option_type: OptionType = OptionType.CALL,
    wing_width_points: Optional[int] = None,
    expiration: Optional[date] = None,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[OptionStructure]:
    """
    Build a long butterfly.
    
    Buy 1 lower, sell 2 middle, buy 1 upper.
    Max profit at center strike, defined risk on wings.
    
    Args:
        option_chain: Option chain
        center_strike: Center strike (short 2)
        option_type: CALL or PUT (both work)
        wing_width_points: Width of each wing in points
        expiration: Expiration date
        as_of_date: Reference date
        config: Builder configuration
        
    Returns:
        OptionStructure or None
    """
    if config is None:
        config = BuilderConfig()
    
    if wing_width_points is None:
        wing_width_points = config.preferred_width_points
    
    if expiration is None:
        expiration = find_best_expiration(option_chain, as_of_date, config)
        if expiration is None:
            return None
    
    lower_strike = center_strike - wing_width_points
    upper_strike = center_strike + wing_width_points
    
    # Get contracts
    require_liq = config.enforce_liquidity
    lower = find_contract(option_chain, expiration, lower_strike, option_type, config, require_liq)
    center = find_contract(option_chain, expiration, center_strike, option_type, config, require_liq)
    upper = find_contract(option_chain, expiration, upper_strike, option_type, config, require_liq)
    
    if None in [lower, center, upper]:
        return None
    
    # Compute ACTUAL width (should be symmetric)
    lower_width = abs(center.strike - lower.strike)
    upper_width = abs(upper.strike - center.strike)
    actual_wing_width_points = min(lower_width, upper_width)
    
    # Build legs: buy 1 lower, sell 2 center, buy 1 upper
    legs = [
        OptionLeg(contract=lower, quantity=1),
        OptionLeg(contract=center, quantity=-2),
        OptionLeg(contract=upper, quantity=1),
    ]
    
    # Calculate entry debit using CONSERVATIVE pricing
    # Buy wings at ask, sell center at bid
    entry_debit_points = lower.ask + upper.ask - 2 * center.bid
    
    if entry_debit_points < 0:
        # Credit butterfly - unusual
        entry_debit_points = abs(entry_debit_points)
    
    # Max loss = debit paid
    max_loss_points = entry_debit_points
    
    # Max profit = wing width - debit (at center strike)
    max_profit_points = actual_wing_width_points - entry_debit_points
    
    if max_profit_points <= 0:
        return None
    
    structure = OptionStructure(
        structure_type=StructureType.BUTTERFLY,
        symbol=option_chain.symbol,
        legs=legs,
        entry_debit=entry_debit_points,
        max_loss=max_loss_points,
        max_profit=max_profit_points,
        breakevens=[center.strike - max_profit_points, center.strike + max_profit_points],
    )
    
    update_structure_greeks(structure, option_chain.underlying_price)
    
    return structure


# ============================================================================
# Calendar Spread
# ============================================================================

def build_calendar(
    option_chain: OptionChain,
    strike: float,
    option_type: OptionType = OptionType.CALL,
    front_expiration: Optional[date] = None,
    back_expiration: Optional[date] = None,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[OptionStructure]:
    """
    Build a calendar spread (time spread).
    
    Sell front-month, buy back-month at same strike.
    Profits from time decay if price stays near strike.
    
    Args:
        option_chain: Option chain
        strike: Strike price (same for both)
        option_type: CALL or PUT
        front_expiration: Near-term expiration to sell
        back_expiration: Far-term expiration to buy
        as_of_date: Reference date
        config: Builder configuration
        
    Returns:
        OptionStructure or None
    """
    if config is None:
        config = BuilderConfig()
    
    # Use reference date
    ref_date = as_of_date or getattr(option_chain, 'as_of_date', None) or date.today()
    sorted_exps = sorted(option_chain.expirations)
    
    if front_expiration is None:
        # Find first expiration >= min_dte
        for exp in sorted_exps:
            dte = (exp - ref_date).days
            if dte >= config.min_dte:
                front_expiration = exp
                break
    
    if back_expiration is None:
        # Find expiration 20-40 days after front
        if front_expiration:
            front_dte = (front_expiration - ref_date).days
            for exp in sorted_exps:
                dte = (exp - ref_date).days
                if dte >= front_dte + 20:
                    back_expiration = exp
                    break
    
    if front_expiration is None or back_expiration is None:
        return None
    
    if front_expiration >= back_expiration:
        return None
    
    # Get contracts
    require_liq = config.enforce_liquidity
    front = find_contract(option_chain, front_expiration, strike, option_type, config, require_liq)
    back = find_contract(option_chain, back_expiration, strike, option_type, config, require_liq)
    
    if front is None or back is None:
        return None
    
    # Build legs: sell front, buy back
    legs = [
        OptionLeg(contract=front, quantity=-1),
        OptionLeg(contract=back, quantity=1),
    ]
    
    # Entry debit using CONSERVATIVE pricing
    # Sell front at bid, buy back at ask
    entry_debit_points = back.ask - front.bid
    
    if entry_debit_points <= 0:
        return None  # Should be a debit for long calendar
    
    # Max loss is the debit paid
    max_loss_points = entry_debit_points
    
    structure = OptionStructure(
        structure_type=StructureType.CALENDAR,
        symbol=option_chain.symbol,
        legs=legs,
        entry_debit=entry_debit_points,
        max_loss=max_loss_points,  # Approximate
        max_profit=None,  # Undefined - depends on IV
        breakevens=[],  # Depends on time and IV
    )
    
    update_structure_greeks(structure, option_chain.underlying_price)
    
    return structure


# ============================================================================
# Diagonal Spread - DISABLED
# ============================================================================

def build_diagonal(
    option_chain: OptionChain,
    front_strike: float,
    back_strike: float,
    option_type: OptionType = OptionType.CALL,
    front_expiration: Optional[date] = None,
    back_expiration: Optional[date] = None,
    as_of_date: Optional[date] = None,
    config: Optional[BuilderConfig] = None,
) -> Optional[OptionStructure]:
    """
    Build a diagonal spread.
    
    DISABLED: Diagonal spreads have complex max loss characteristics
    that depend on IV and cannot be reliably bounded at entry.
    
    Returns:
        None (disabled)
    """
    # Diagonals disabled - max loss is complex and path-dependent
    return None
