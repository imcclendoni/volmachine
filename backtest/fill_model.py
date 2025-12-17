"""
Fill Model for Backtesting.

Handles slippage, commissions, and fill price calculations.
"""

from dataclasses import dataclass
from typing import Dict, Any
import yaml
from pathlib import Path


@dataclass
class FillConfig:
    """Configuration for fill model."""
    slippage_per_leg: float = 0.02      # Dollars per leg
    commission_per_contract: float = 0.65
    min_commission: float = 0.00
    
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
        )


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
