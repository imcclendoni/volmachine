#!/usr/bin/env python3
"""
Submit Test Order to IBKR Paper.

End-to-end test of IBKR order flow:
1. Connect to IBKR paper account
2. Use reqSecDefOptParams to get valid expiry/strikes
3. Build a small put spread (SPY)
4. Submit with transmit=True
5. Track status changes

Usage:
    python3 scripts/submit_test_order.py --paper
    python3 scripts/submit_test_order.py --paper --dry-run  # Preview only
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ib_insync import IB, Stock, Option, Contract, ComboLeg, Order


def connect_ibkr(host: str, port: int, client_id: int) -> IB:
    """Connect to IBKR and verify paper account."""
    ib = IB()
    
    print(f"Connecting to {host}:{port} (clientId={client_id})...")
    ib.connect(host, port, clientId=client_id)
    
    if not ib.isConnected():
        raise ConnectionError("Failed to connect to IBKR")
    
    # Get account info
    accounts = ib.managedAccounts()
    if not accounts:
        raise ValueError("No accounts found")
    
    account_id = accounts[0]
    server_time = ib.reqCurrentTime()
    
    print(f"✅ Connected!")
    print(f"   Account: {account_id}")
    print(f"   Server time: {server_time}")
    
    # Verify paper account
    if not account_id.startswith('D'):
        raise ValueError(f"SAFETY BLOCK: Account {account_id} is not paper (must start with 'D')")
    
    print(f"   ✅ Paper account confirmed")
    
    return ib


def get_valid_spy_spread(ib: IB) -> tuple[Option, Option, float]:
    """
    Get a valid SPY put spread using live chain data.
    
    Returns: (long_put, short_put, strike_width)
    """
    print("\n📊 Finding valid SPY options...")
    
    # Get SPY underlying
    spy = Stock('SPY', 'SMART', 'USD')
    ib.qualifyContracts(spy)
    
    # Get current price (use close if no live data subscription)
    ticker = ib.reqTickers(spy)[0]
    ib.sleep(1)  # Wait for data
    
    spot = ticker.marketPrice()
    if spot is None or spot != spot:  # NaN check
        spot = ticker.close
    if spot is None or spot != spot:
        spot = ticker.last
    if spot is None or spot != spot:
        # Fallback: use approximate current SPY price
        spot = 595.0
        print(f"   ⚠️ Using fallback spot: ${spot:.2f} (no market data subscription)")
    else:
        print(f"   SPY spot: ${spot:.2f}")
    
    # Get option chain parameters
    chains = ib.reqSecDefOptParams('SPY', '', 'STK', spy.conId)
    
    if not chains:
        raise ValueError("No option chains found for SPY")
    
    # Collect all expirations and strikes across exchanges
    all_expirations = set()
    all_strikes = set()
    for chain in chains:
        all_expirations.update(chain.expirations)
        all_strikes.update(chain.strikes)
    
    # Get today's date in YYYYMMDD format
    today = datetime.now().strftime('%Y%m%d')
    
    # Filter to future expirations (at least 7 DTE)
    min_exp = (datetime.now().replace(hour=0, minute=0) + __import__('datetime').timedelta(days=7)).strftime('%Y%m%d')
    valid_expirations = sorted([e for e in all_expirations if e >= min_exp])
    
    print(f"   Found {len(valid_expirations)} valid expirations (>= {min_exp})")
    
    if len(valid_expirations) < 1:
        raise ValueError(f"Not enough expirations available. All: {sorted(all_expirations)[:10]}")
    
    # Use first valid expiration
    target_exp = valid_expirations[0]
    print(f"   Target expiration: {target_exp}")
    
    # Find strikes near spot (5% OTM puts)
    otm_factor = 0.95
    target_short = round(spot * otm_factor)
    
    # Find nearest valid strikes from all_strikes
    available_strikes = sorted(all_strikes)
    short_strike = min(available_strikes, key=lambda x: abs(x - target_short))
    
    # Find next lower strike for long leg (SPY often has $5 increments)
    short_idx = available_strikes.index(short_strike)
    if short_idx > 0:
        long_strike = available_strikes[short_idx - 1]
    else:
        long_strike = short_strike - 5  # Fallback $5 width
    
    print(f"   Target short: {short_strike}P, long: {long_strike}P")
    
    # Create and VERIFY both option contracts
    short_put = Option('SPY', target_exp, short_strike, 'P', 'SMART', currency='USD')
    long_put = Option('SPY', target_exp, long_strike, 'P', 'SMART', currency='USD')
    
    # Try to qualify both
    ib.qualifyContracts(short_put, long_put)
    
    # If long leg failed, try $5 increments
    if not long_put.conId:
        print(f"   ⚠️ {long_strike}P not found, trying $5 lower...")
        for offset in [5, 10, 15]:
            test_strike = short_strike - offset
            if test_strike in available_strikes:
                long_put = Option('SPY', target_exp, test_strike, 'P', 'SMART', currency='USD')
                ib.qualifyContracts(long_put)
                if long_put.conId:
                    long_strike = test_strike
                    break
    
    if not short_put.conId or not long_put.conId:
        raise ValueError(f"Failed to qualify contracts. Short: {short_put.conId}, Long: {long_put.conId}")
    
    print(f"   ✅ Short put conId: {short_put.conId}")
    print(f"   ✅ Long put conId: {long_put.conId}")
    
    return long_put, short_put, short_strike - long_strike


def create_combo_order(
    ib: IB,
    long_leg: Option,
    short_leg: Option,
    quantity: int = 1,
    limit_price: float = None,
    width: float = 5.0,
) -> tuple[Contract, Order]:
    """
    Create a BAG contract and limit order for a spread.
    
    For debit spread (buy long, sell short):
    - Action: BUY
    - Limit: positive (debit paid)
    """
    print("\n📦 Creating BAG combo order...")
    
    # Create BAG contract
    bag = Contract()
    bag.symbol = 'SPY'
    bag.secType = 'BAG'
    bag.exchange = 'SMART'
    bag.currency = 'USD'
    
    # Add combo legs
    # For debit spread: buy long put (lower strike), sell short put (higher strike)
    long_combo_leg = ComboLeg()
    long_combo_leg.conId = long_leg.conId
    long_combo_leg.ratio = 1
    long_combo_leg.action = 'BUY'
    long_combo_leg.exchange = 'SMART'
    
    short_combo_leg = ComboLeg()
    short_combo_leg.conId = short_leg.conId
    short_combo_leg.ratio = 1
    short_combo_leg.action = 'SELL'
    short_combo_leg.exchange = 'SMART'
    
    bag.comboLegs = [long_combo_leg, short_combo_leg]
    
    print(f"   BAG legs: {len(bag.comboLegs)}")
    print(f"   Long leg conId: {long_leg.conId} (BUY)")
    print(f"   Short leg conId: {short_leg.conId} (SELL)")
    
    # Calculate realistic limit price if not specified
    # For a far OTM put debit spread, use ~10-20% of width as a reasonable debit
    if limit_price is None:
        limit_price = round(width * 0.15, 2)  # 15% of width
        print(f"   Calculated limit: ${limit_price:.2f} (15% of ${width:.0f} width)")
    
    # Create limit order
    order = Order()
    order.action = 'BUY'  # Buying the spread (debit)
    order.orderType = 'LMT'
    order.totalQuantity = quantity
    order.lmtPrice = limit_price
    order.tif = 'DAY'
    
    print(f"   Order: {order.action} {order.totalQuantity} @ ${order.lmtPrice:.2f} LMT")
    
    return bag, order


def submit_order(ib: IB, contract: Contract, order: Order, transmit: bool = True) -> int:
    """
    Submit order to IBKR.
    
    Args:
        ib: IB connection
        contract: BAG contract
        order: Order object
        transmit: True to submit, False for preview
        
    Returns:
        Order ID
    """
    order.transmit = transmit
    
    if transmit:
        print("\n🚀 Submitting order (transmit=True)...")
    else:
        print("\n👁️ Preview order (transmit=False)...")
    
    trade = ib.placeOrder(contract, order)
    
    # Wait for order to be acknowledged
    time.sleep(1)
    ib.sleep(1)
    
    order_id = trade.order.orderId
    status = trade.orderStatus.status
    
    print(f"   Order ID: {order_id}")
    print(f"   Status: {status}")
    
    if transmit:
        # Poll for status changes
        print("\n📡 Monitoring order status (10 seconds)...")
        for i in range(10):
            ib.sleep(1)
            current_status = trade.orderStatus.status
            if current_status != status:
                print(f"   ➡️ Status changed: {status} → {current_status}")
                status = current_status
            
            if status in ['Filled', 'Cancelled', 'Inactive']:
                break
        
        # Final status
        print(f"\n📋 Final Status:")
        print(f"   Status: {trade.orderStatus.status}")
        print(f"   Filled: {trade.orderStatus.filled}")
        print(f"   Remaining: {trade.orderStatus.remaining}")
        print(f"   Avg Fill Price: {trade.orderStatus.avgFillPrice}")
    
    return order_id


def main():
    parser = argparse.ArgumentParser(description="Submit test order to IBKR paper")
    parser.add_argument('--paper', action='store_true', required=True, help='Confirm paper trading')
    parser.add_argument('--dry-run', action='store_true', help='Preview only (transmit=False)')
    parser.add_argument('--host', default='127.0.0.1', help='IBKR host')
    parser.add_argument('--port', type=int, default=4002, help='IBKR port (default: 4002 paper)')
    parser.add_argument('--client-id', type=int, default=99, help='Client ID')
    parser.add_argument('--quantity', type=int, default=1, help='Number of spreads')
    parser.add_argument('--limit', type=float, default=None, help='Limit price (debit) - auto-calculated if not set')
    args = parser.parse_args()
    
    print("=" * 60)
    print("IBKR Paper Test Order")
    print("=" * 60)
    print(f"Mode: {'DRY RUN (preview)' if args.dry_run else 'LIVE SUBMIT'}")
    print(f"Port: {args.port}")
    print()
    
    ib = None
    try:
        # Connect
        ib = connect_ibkr(args.host, args.port, args.client_id)
        
        # Get valid spread
        long_put, short_put, width = get_valid_spy_spread(ib)
        
        # Create combo order
        bag, order = create_combo_order(
            ib, long_put, short_put,
            quantity=args.quantity,
            limit_price=args.limit,
            width=width,
        )
        
        # Submit
        order_id = submit_order(ib, bag, order, transmit=not args.dry_run)
        
        print("\n" + "=" * 60)
        print("TEST COMPLETE")
        print("=" * 60)
        print(f"Order ID: {order_id}")
        print(f"Check IBKR Desktop → Activity → Orders")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1
        
    finally:
        if ib and ib.isConnected():
            print("\nDisconnecting...")
            ib.disconnect()


if __name__ == "__main__":
    sys.exit(main())
