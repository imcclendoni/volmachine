"""
Fill Model for Backtesting.

Handles slippage, commissions, bid/ask modeling, and fill price calculations.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
import yaml
from pathlib import Path


@dataclass
class FillConfig:
    """Configuration for fill model with realistic execution modeling."""
    slippage_per_leg: float = 0.02      # Dollars per leg (base slippage)
    commission_per_contract: float = 0.65
    min_commission: float = 0.00
    
    # Bid/Ask modeling (for when only close is available)
    bid_ask_spread_pct: float = 0.02    # 2% of close price = half-spread
    liquidity_stress_mult: float = 1.0   # Multiply spread during stress (e.g., 2x)
    
    # High-vol detection threshold (ATM IV or VIX proxy)
    high_vol_threshold: float = 0.30     # 30% IV = high vol
    
    @classmethod
    def from_yaml(cls, path: str = './config/backtest.yaml') -> 'FillConfig':
        """Load from YAML config."""
        config_path = Path(path)
        if not config_path.exists():
            return cls()
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        slip = config.get('slippage', {})
        comm = config.get('commissions', {})
        
        return cls(
            slippage_per_leg=slip.get('per_leg', 0.02),
            commission_per_contract=comm.get('per_contract', 0.65),
            min_commission=comm.get('min_per_order', 0.00),
            bid_ask_spread_pct=slip.get('bid_ask_spread_pct', 0.02),
            liquidity_stress_mult=slip.get('liquidity_stress_mult', 1.0),
            high_vol_threshold=slip.get('high_vol_threshold', 0.30),
        )
    
    def get_bid_ask(self, close: float, is_high_vol: bool = False) -> tuple:
        """
        Model bid/ask from close price.
        
        Returns (bid, ask) where:
        - bid = close - half_spread
        - ask = close + half_spread
        """
        half_spread = close * self.bid_ask_spread_pct
        if is_high_vol:
            half_spread *= self.liquidity_stress_mult
        
        bid = max(0.01, close - half_spread)
        ask = close + half_spread
        return bid, ask


def calculate_entry_fill(
    leg_closes: Dict[str, float],
    leg_sides: Dict[str, str],  # "BUY" or "SELL"
    config: FillConfig,
) -> Dict[str, Any]:
    """
    Calculate entry fill prices with slippage.
    
    For SELL legs: we receive close - slippage (worse fill on credit)
    For BUY legs: we pay close + slippage (worse fill on debit)
    
    Args:
        leg_closes: Dict of leg_id -> close price
        leg_sides: Dict of leg_id -> "BUY" or "SELL"
        config: Fill configuration
        
    Returns:
        Dict with fill_prices, net_premium, commissions
    """
    fill_prices = {}
    net_premium = 0.0
    
    for leg_id, close in leg_closes.items():
        side = leg_sides.get(leg_id, 'BUY')
        
        if side == 'SELL':
            # Selling: we receive less (close - slippage)
            fill = close - config.slippage_per_leg
            net_premium += fill  # Receive credit
        else:
            # Buying: we pay more (close + slippage)
            fill = close + config.slippage_per_leg
            net_premium -= fill  # Pay debit
        
        fill_prices[leg_id] = max(0.01, fill)  # Floor at $0.01
    
    # Calculate commissions
    num_legs = len(leg_closes)
    commissions = max(
        num_legs * config.commission_per_contract,
        config.min_commission
    )
    
    return {
        'fill_prices': fill_prices,
        'net_premium': net_premium,  # Positive = credit, Negative = debit
        'commissions': commissions,
    }


def calculate_exit_fill(
    leg_closes: Dict[str, float],
    leg_sides: Dict[str, str],  # Original entry sides
    config: FillConfig,
) -> Dict[str, Any]:
    """
    Calculate exit fill prices with slippage.
    
    Exit is opposite of entry:
    - If we SOLD at entry, we BUY to close: pay close + slippage
    - If we BOUGHT at entry, we SELL to close: receive close - slippage
    
    Args:
        leg_closes: Dict of leg_id -> close price at exit
        leg_sides: Dict of leg_id -> original entry side ("BUY" or "SELL")
        config: Fill configuration
        
    Returns:
        Dict with fill_prices, net_premium, commissions
    """
    fill_prices = {}
    net_premium = 0.0
    
    for leg_id, close in leg_closes.items():
        entry_side = leg_sides.get(leg_id, 'BUY')
        
        if entry_side == 'SELL':
            # We sold at entry, now buy to close: pay more
            fill = close + config.slippage_per_leg
            net_premium -= fill  # Pay to close
        else:
            # We bought at entry, now sell to close: receive less
            fill = close - config.slippage_per_leg
            net_premium += fill  # Receive on close
        
        fill_prices[leg_id] = max(0.01, fill)
    
    # Commissions on exit
    num_legs = len(leg_closes)
    commissions = max(
        num_legs * config.commission_per_contract,
        config.min_commission
    )
    
    return {
        'fill_prices': fill_prices,
        'net_premium': net_premium,
        'commissions': commissions,
    }


def calculate_realized_pnl(
    entry_net: float,       # Net premium at entry (+ credit, - debit)
    exit_net: float,        # Net premium at exit
    entry_commissions: float,
    exit_commissions: float,
    contracts: int = 1,
) -> Dict[str, float]:
    """
    Calculate realized PnL including all costs.
    
    PnL = (entry_net + exit_net) * 100 * contracts - commissions
    
    For credit spread:
        entry_net = +$0.50 (received credit)
        exit_net = -$0.20 (paid to close)
        PnL = (0.50 - 0.20) * 100 - commissions = $30 - commissions
        
    For debit spread:
        entry_net = -$1.00 (paid debit)
        exit_net = +$1.50 (received on close)
        PnL = (-1.00 + 1.50) * 100 - commissions = $50 - commissions
    """
    gross_pnl = (entry_net + exit_net) * 100 * contracts
    total_commissions = (entry_commissions + exit_commissions) * contracts
    net_pnl = gross_pnl - total_commissions
    
    return {
        'gross_pnl': gross_pnl,
        'commissions': total_commissions,
        'net_pnl': net_pnl,
    }


def calculate_strict_entry_fill(
    leg_closes: Dict[str, float],
    leg_sides: Dict[str, str],
    config: FillConfig,
    is_high_vol: bool = False,
) -> Dict[str, Any]:
    """
    Calculate entry fill using strict bid/ask bounds.
    
    For credit spreads: credit = short_bid - long_ask
    For debit spreads: debit = long_ask - short_bid
    
    Returns 'unexecutable': True if fill would be worse than limiting case.
    """
    fill_prices = {}
    short_credit = 0.0
    long_debit = 0.0
    
    for leg_id, close in leg_closes.items():
        bid, ask = config.get_bid_ask(close, is_high_vol)
        side = leg_sides.get(leg_id, 'BUY')
        
        if side == 'SELL':
            # Selling at bid (conservative)
            fill = bid
            short_credit += fill
        else:
            # Buying at ask (conservative)
            fill = ask
            long_debit += fill
        
        fill_prices[leg_id] = fill
    
    net_premium = short_credit - long_debit
    
    # Determine if this is a credit or debit structure from the computed cashflow.
    # net_premium > 0 => credit; net_premium < 0 => debit
    unexecutable = False
    
    # Basic sanity checks: must have at least one buy and one sell leg
    has_sell = any(side == 'SELL' for side in leg_sides.values())
    has_buy = any(side != 'SELL' for side in leg_sides.values())
    if not (has_sell and has_buy):
        unexecutable = True
    
    # Credit spread: must produce positive credit
    if net_premium >= 0:
        # Net credit must be strictly positive to be executable
        if net_premium <= 0:
            unexecutable = True
    else:
        # Debit spread: must produce a strictly positive debit (i.e., pay something)
        debit = -net_premium
        if debit <= 0:
            unexecutable = True
    
    num_legs = len(leg_closes)
    commissions = max(num_legs * config.commission_per_contract, config.min_commission)
    
    return {
        'fill_prices': fill_prices,
        'net_premium': net_premium,
        'commissions': commissions,
        'unexecutable': unexecutable,
        'short_credit': short_credit,
        'long_debit': long_debit,
    }


def calculate_strict_exit_fill(
    leg_closes: Dict[str, float],
    leg_sides: Dict[str, str],  # Original ENTRY sides
    config: FillConfig,
    is_high_vol: bool = False,
) -> Dict[str, Any]:
    """
    Calculate exit fill using strict bid/ask bounds.
    
    Exit is opposite of entry:
    - SELL at entry -> BUY at ask to close
    - BUY at entry -> SELL at bid to close
    """
    fill_prices = {}
    exit_debit = 0.0     # Paid to close short
    exit_credit = 0.0    # Received to close long
    
    for leg_id, close in leg_closes.items():
        bid, ask = config.get_bid_ask(close, is_high_vol)
        entry_side = leg_sides.get(leg_id, 'BUY')
        
        if entry_side == 'SELL':
            # We sold, now buy to close at ask
            fill = ask
            exit_debit += fill
        else:
            # We bought, now sell to close at bid
            fill = bid
            exit_credit += fill
        
        fill_prices[leg_id] = fill
    
    net_premium = exit_credit - exit_debit  # Negative for credit spread exit
    
    num_legs = len(leg_closes)
    commissions = max(num_legs * config.commission_per_contract, config.min_commission)
    
    return {
        'fill_prices': fill_prices,
        'net_premium': net_premium,
        'commissions': commissions,
        'exit_debit': exit_debit,
        'exit_credit': exit_credit,
    }
