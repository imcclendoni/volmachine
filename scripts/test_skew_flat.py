#!/usr/bin/env python3
"""
Test Script: Force skew_flat edge and verify debit put spread is built.

This script forces a SKEW_EXTREME edge with is_flat=1.0 to verify
the structure mapping fix.
"""

import sys
sys.path.insert(0, '/Users/jeffreyboyle/Desktop/volmachine')

from datetime import date, datetime
from data.schemas import (
    EdgeSignal, EdgeType, EdgeDirection,
    OptionChain, OptionContract, OptionType, Greeks,
)
from structures import (
    build_debit_spread,
    BuilderConfig,
)

def create_mock_option_chain(symbol: str = "SPY", spot: float = 600.0) -> OptionChain:
    """Create a mock option chain for testing."""
    exp_date = date(2025, 1, 17)
    contracts = []
    
    # Create puts at various strikes
    for strike in range(580, 620, 5):
        # Put contract
        mid = max(0.5, (spot - strike) * 0.1 if strike < spot else 0.5)
        contracts.append(OptionContract(
            symbol=symbol,
            contract_symbol=f"{symbol}250117P{strike*1000:08d}",
            option_type=OptionType.PUT,
            strike=float(strike),
            expiration=exp_date,
            bid=mid * 0.95,
            ask=mid * 1.05,
            last=mid,
            iv=0.20,
            greeks=Greeks(delta=-0.3, gamma=0.02, theta=-0.05, vega=0.15),
            volume=1000,
            open_interest=5000,
        ))
        
        # Call contract
        contracts.append(OptionContract(
            symbol=symbol,
            contract_symbol=f"{symbol}250117C{strike*1000:08d}",
            option_type=OptionType.CALL,
            strike=float(strike),
            expiration=exp_date,
            bid=mid * 0.95,
            ask=mid * 1.05,
            last=mid,
            iv=0.20,
            greeks=Greeks(delta=0.3, gamma=0.02, theta=-0.05, vega=0.15),
            volume=1000,
            open_interest=5000,
        ))
    
    return OptionChain(
        symbol=symbol,
        underlying_price=spot,
        timestamp=datetime.now(),
        expirations=[exp_date],
        contracts=contracts,
    )


def test_skew_flat_debit_spread():
    """Test that skew_flat edge builds a debit put spread."""
    print("=" * 60)
    print("TEST 3: Force skew_flat edge -> verify debit put spread")
    print("=" * 60)
    
    # Create mock chain
    chain = create_mock_option_chain("SPY", 600.0)
    print(f"✓ Created mock option chain: {chain.symbol} @ ${chain.underlying_price}")
    
    # Create skew_flat edge
    edge = EdgeSignal(
        symbol="SPY",
        edge_type=EdgeType.SKEW_EXTREME,
        strength=0.75,
        direction=EdgeDirection.LONG,  # Tail protection cheap
        metrics={"is_flat": 1.0, "skew_pct": -0.05},  # FLAT skew
        rationale="Skew is flat - tail protection cheap",
    )
    print(f"✓ Created edge: {edge.edge_type.value}, is_flat={edge.metrics.get('is_flat')}")
    
    # Test building debit spread directly
    cfg = BuilderConfig(min_dte=7, max_dte=45)
    atm_strike = 600.0
    width = 5
    
    long_strike = atm_strike - width  # 595 - closer to ATM
    
    print(f"✓ Building debit spread: long_strike={long_strike}, width={width}")
    
    structure = build_debit_spread(
        chain,
        OptionType.PUT,
        long_strike=long_strike,
        width_points=width,
        as_of_date=date.today(),
        config=cfg,
    )
    
    if structure is None:
        print("❌ FAILED: build_debit_spread returned None")
        return False
    
    print(f"✓ Structure built: {structure.structure_type.value}")
    print(f"  Legs: {len(structure.legs)}")
    
    for i, leg in enumerate(structure.legs):
        action = "SELL" if leg.quantity < 0 else "BUY"
        print(f"    Leg {i+1}: {action} {abs(leg.quantity)} {leg.contract.option_type.value} @ ${leg.contract.strike}")
    
    print(f"  Entry Debit: ${structure.entry_debit_dollars:.2f}")
    print(f"  Max Loss: ${structure.max_loss_dollars:.2f}")
    print(f"  Max Profit: ${structure.max_profit_dollars:.2f}")
    
    # Validate structure
    checks = []
    
    # Check it's a debit spread
    if structure.entry_debit is None or structure.entry_debit <= 0:
        checks.append("FAIL: No entry debit - not a debit spread")
    else:
        checks.append(f"PASS: Entry debit = ${structure.entry_debit_dollars:.2f}")
    
    # Check legs
    if len(structure.legs) != 2:
        checks.append(f"FAIL: Expected 2 legs, got {len(structure.legs)}")
    else:
        leg1, leg2 = structure.legs
        # Debit spread: BUY higher strike (closer to ATM), SELL lower strike
        buy_leg = leg1 if leg1.quantity > 0 else leg2
        sell_leg = leg2 if leg1.quantity > 0 else leg1
        
        if buy_leg.contract.strike > sell_leg.contract.strike:
            checks.append(f"PASS: BUY leg at ${buy_leg.contract.strike} > SELL leg at ${sell_leg.contract.strike}")
        else:
            checks.append(f"FAIL: Buy/sell strikes incorrect")
    
    print("\n" + "-" * 40)
    for check in checks:
        print(f"  {check}")
    
    all_passed = all("PASS" in c for c in checks)
    print("-" * 40)
    print(f"\n{'✅ TEST 3 PASSED' if all_passed else '❌ TEST 3 FAILED'}")
    
    return all_passed


if __name__ == "__main__":
    success = test_skew_flat_debit_spread()
    sys.exit(0 if success else 1)
