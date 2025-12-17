#!/usr/bin/env python3
"""
Submit Test Order to IBKR Paper.

End-to-end test of IBKR order flow:
1. Connect to IBKR paper account
2. Get spot price from Polygon (reliable source)
3. Select options with verified Polygon pricing data
4. Build put spread using legs that can be priced
5. Preview order (DRY RUN only until reliable quotes available)

Usage:
    python3 scripts/submit_test_order.py --paper --dry-run
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ib_insync import IB, Stock, Option, Contract, ComboLeg, Order


def get_polygon_api_key() -> str:
    """Load Polygon API key from secrets."""
    import toml
    secrets_path = Path('./.streamlit/secrets.toml')
    if secrets_path.exists():
        secrets = toml.load(secrets_path)
        if 'POLYGON_API_KEY' in secrets:
            return secrets['POLYGON_API_KEY']
    import os
    return os.environ.get('POLYGON_API_KEY', '')


def polygon_get_spot(symbol: str = 'SPY') -> float:
    """Get underlying spot price from Polygon stock aggregates."""
    import requests
    
    api_key = get_polygon_api_key()
    if not api_key:
        raise ValueError("No Polygon API key configured")
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {'apiKey': api_key, 'adjusted': 'true', 'sort': 'desc', 'limit': 1}
    
    r = requests.get(url, params=params, timeout=10)
    data = r.json()
    
    if data.get('status') in ['OK', 'DELAYED'] and data.get('resultsCount', 0) > 0:
        result = data['results'][0]
        spot = result.get('c') or result.get('vw')
        if spot and spot > 0:
            return float(spot)
    
    raise ValueError(f"Could not get spot price for {symbol} from Polygon")


def polygon_get_option_price(ticker: str, days_back: int = 45) -> float:
    """
    Get option last close from Polygon aggregates.
    
    ticker: O:SPY241220P00680000 format
    Returns close price or None if no data.
    """
    import requests
    
    api_key = get_polygon_api_key()
    if not api_key:
        return None
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    params = {'apiKey': api_key, 'adjusted': 'true', 'sort': 'desc', 'limit': 1}
    
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        
        if data.get('status') in ['OK', 'DELAYED'] and data.get('resultsCount', 0) > 0:
            result = data['results'][0]
            close = result.get('c')
            if close and close > 0:
                return float(close)
    except Exception:
        pass
    
    return None


def build_polygon_ticker(symbol: str, exp: str, strike: float, right: str) -> str:
    """Build Polygon option ticker from components."""
    # exp should be YYYYMMDD, convert to YYMMDD
    if len(exp) == 8:
        exp_short = exp[2:]
    else:
        exp_short = exp
    
    strike_padded = f"{int(strike * 1000):08d}"
    return f"O:{symbol}{exp_short}{right}{strike_padded}"


def connect_ibkr(host: str, port: int, client_id: int) -> IB:
    """Connect to IBKR and verify paper account."""
    ib = IB()
    
    print(f"Connecting to {host}:{port} (clientId={client_id})...")
    ib.connect(host, port, clientId=client_id)
    
    if not ib.isConnected():
        raise ConnectionError("Failed to connect to IBKR")
    
    accounts = ib.managedAccounts()
    if not accounts:
        raise ValueError("No accounts found")
    
    account_id = accounts[0]
    server_time = ib.reqCurrentTime()
    
    print(f"✅ Connected!")
    print(f"   Account: {account_id}")
    print(f"   Server time: {server_time}")
    
    if not account_id.startswith('D'):
        raise ValueError(f"SAFETY BLOCK: Account {account_id} is not paper (must start with 'D')")
    
    print(f"   ✅ Paper account confirmed")
    
    return ib


def find_priced_spread(ib: IB) -> tuple[Option, Option, float, float, float]:
    """
    Find a put spread where BOTH legs have Polygon pricing data.
    
    Returns: (long_put, short_put, width, long_price, short_price)
    """
    print("\n📊 Finding priced SPY options...")
    
    # 1. Get spot from Polygon
    spot = polygon_get_spot('SPY')
    print(f"   SPY spot (Polygon): ${spot:.2f}")
    
    # 2. Get option chain from IBKR
    spy = Stock('SPY', 'SMART', 'USD')
    ib.qualifyContracts(spy)
    
    chains = ib.reqSecDefOptParams('SPY', '', 'STK', spy.conId)
    if not chains:
        raise ValueError("No option chains found")
    
    # Pick exchange with near-term expirations
    best_chain = max(chains, key=lambda c: len([e for e in c.expirations 
                     if e <= (datetime.now() + timedelta(days=30)).strftime('%Y%m%d')]))
    
    print(f"   Exchange: {best_chain.exchange}")
    
    # Convert strikes to float
    available_strikes = sorted([float(s) for s in best_chain.strikes])
    available_expirations = sorted(best_chain.expirations)
    
    # 3. Filter to 7-21 DTE
    today = datetime.now()
    min_dte = (today + timedelta(days=7)).strftime('%Y%m%d')
    max_dte = (today + timedelta(days=21)).strftime('%Y%m%d')
    
    valid_exps = [e for e in available_expirations if min_dte <= e <= max_dte]
    if not valid_exps:
        valid_exps = [e for e in available_expirations if e >= min_dte][:3]
    
    if not valid_exps:
        raise ValueError("No valid expirations in 7-21 DTE range")
    
    print(f"   Candidate expirations: {valid_exps[:3]}")
    
    # 4. Find ATM strike
    atm_strike = min(available_strikes, key=lambda x: abs(x - spot))
    atm_idx = available_strikes.index(atm_strike)
    
    print(f"   ATM strike: {atm_strike}")
    
    # 5. Search for priced spread: walk through expirations and strike combinations
    widths_to_try = [1, 2, 5]  # $1, $2, $5 wide spreads
    offsets_to_try = [0, -1, 1, -2, 2, -3, 3, -5, 5]  # Near ATM first
    
    for exp in valid_exps[:3]:
        print(f"\n   Trying expiration {exp}...")
        
        for offset in offsets_to_try:
            short_idx = atm_idx + offset
            if short_idx < 0 or short_idx >= len(available_strikes):
                continue
            
            short_strike = available_strikes[short_idx]
            
            for width in widths_to_try:
                # Find long strike (lower for put spread)
                long_strike = short_strike - width
                if long_strike not in available_strikes:
                    # Try to find nearest available
                    candidates = [s for s in available_strikes if s < short_strike]
                    if not candidates:
                        continue
                    long_strike = max(candidates)
                
                # Build Polygon tickers
                short_ticker = build_polygon_ticker('SPY', exp, short_strike, 'P')
                long_ticker = build_polygon_ticker('SPY', exp, long_strike, 'P')
                
                # Check if both have pricing
                short_price = polygon_get_option_price(short_ticker)
                if short_price is None:
                    continue
                
                long_price = polygon_get_option_price(long_ticker)
                if long_price is None:
                    continue
                
                # Found priced spread!
                print(f"   ✅ Found priced spread: {short_strike}P/${long_strike}P")
                print(f"      Short: {short_ticker} = ${short_price:.2f}")
                print(f"      Long: {long_ticker} = ${long_price:.2f}")
                
                # Qualify contracts with IBKR
                short_put = Option('SPY', exp, short_strike, 'P', 'SMART', currency='USD')
                long_put = Option('SPY', exp, long_strike, 'P', 'SMART', currency='USD')
                
                ib.qualifyContracts(short_put, long_put)
                
                if not short_put.conId or not long_put.conId:
                    print(f"      ⚠️ IBKR contract qualification failed, trying next...")
                    continue
                
                print(f"      Short conId: {short_put.conId}")
                print(f"      Long conId: {long_put.conId}")
                
                actual_width = short_strike - long_strike
                return long_put, short_put, actual_width, long_price, short_price
    
    raise ValueError("Could not find a spread with Polygon pricing data")


def determine_spread_type(long_price: float, short_price: float) -> tuple[str, float]:
    """
    Determine if spread is credit or debit based on leg prices.
    
    Returns: (spread_type, net_premium)
    - 'credit': short_price > long_price, we receive premium
    - 'debit': long_price > short_price, we pay premium
    """
    if short_price > long_price:
        net_credit = short_price - long_price
        return 'credit', net_credit
    else:
        net_debit = long_price - short_price
        return 'debit', net_debit


def calculate_combo_limit(long_price: float, short_price: float, width: float, 
                          slippage: float = 0.10) -> tuple[str, float]:
    """
    Calculate combo limit price from leg closes.
    
    Returns: (spread_type, limit_price)
    - spread_type: 'credit' or 'debit'
    - limit_price: always positive
    """
    print("\n💰 Calculating combo limit price...")
    
    # Determine spread type
    spread_type, net_premium = determine_spread_type(long_price, short_price)
    
    print(f"   Spread type: {spread_type.upper()}")
    print(f"   Long price: ${long_price:.2f}")
    print(f"   Short price: ${short_price:.2f}")
    print(f"   Net premium: ${net_premium:.2f}")
    
    # Guardrail: reject if net is invalid
    if net_premium <= 0:
        raise ValueError(f"Invalid net premium ${net_premium:.2f} for {spread_type} spread")
    
    if net_premium != net_premium:  # NaN check
        raise ValueError("Net premium is NaN - cannot price")
    
    # Apply slippage
    if spread_type == 'credit':
        # Credit spread: we receive less (worse fill)
        limit = round(net_premium * (1 - slippage), 2)
        print(f"   Credit limit: ${limit:.2f} (accepting {slippage*100:.0f}% less)")
    else:
        # Debit spread: we pay more (worse fill)
        limit = round(net_premium * (1 + slippage), 2)
        print(f"   Debit limit: ${limit:.2f} (paying {slippage*100:.0f}% more)")
    
    # Guardrail: limit can't exceed width
    if limit > width:
        print(f"   ⚠️ Limit ${limit:.2f} exceeds width ${width:.2f}, capping to 90%...")
        limit = round(width * 0.9, 2)
    
    # Guardrail: limit must be positive
    if limit <= 0:
        raise ValueError(f"Invalid limit ${limit:.2f} - must be positive")
    
    print(f"   ✅ Order: {spread_type.upper()} @ ${limit:.2f}")
    
    return spread_type, limit


def create_combo_order(long_leg: Option, short_leg: Option, quantity: int, 
                       spread_type: str, limit_price: float) -> tuple[Contract, Order]:
    """
    Create BAG contract and limit order.
    
    For CREDIT spread: order.action = 'SELL' (sell the spread to receive credit)
    For DEBIT spread: order.action = 'BUY' (buy the spread, pay debit)
    """
    print("\n📦 Creating BAG combo...")
    
    if not long_leg.conId or not short_leg.conId:
        raise ValueError(f"Invalid conIds: long={long_leg.conId}, short={short_leg.conId}")
    
    if limit_price <= 0:
        raise ValueError(f"Invalid limit price: ${limit_price}")
    
    # Create BAG contract
    bag = Contract()
    bag.symbol = 'SPY'
    bag.secType = 'BAG'
    bag.exchange = 'SMART'
    bag.currency = 'USD'
    
    # Long leg (always BUY the lower strike)
    long_combo = ComboLeg()
    long_combo.conId = long_leg.conId
    long_combo.ratio = 1
    long_combo.action = 'BUY'
    long_combo.exchange = 'SMART'
    
    # Short leg (always SELL the higher strike)
    short_combo = ComboLeg()
    short_combo.conId = short_leg.conId
    short_combo.ratio = 1
    short_combo.action = 'SELL'
    short_combo.exchange = 'SMART'
    
    bag.comboLegs = [long_combo, short_combo]
    
    # Create order with correct action
    order = Order()
    
    if spread_type == 'credit':
        # SELL the spread to receive credit
        order.action = 'SELL'
    else:
        # BUY the spread, pay debit
        order.action = 'BUY'
    
    order.orderType = 'LMT'
    order.totalQuantity = quantity
    order.lmtPrice = limit_price
    order.tif = 'DAY'
    
    print(f"   Long leg: conId={long_leg.conId} (BUY)")
    print(f"   Short leg: conId={short_leg.conId} (SELL)")
    print(f"   Order: {order.action} {quantity} @ ${limit_price:.2f} LMT ({spread_type})")
    
    return bag, order


def preview_order(ib: IB, contract: Contract, order: Order) -> dict:
    """Preview order (transmit=False) and return status. Does NOT record to blotter."""
    order.transmit = False
    
    print("\n👁️ Preview order (transmit=False)...")
    
    trade = ib.placeOrder(contract, order)
    ib.sleep(2)
    
    result = {
        'orderId': trade.order.orderId,
        'status': trade.orderStatus.status,
    }
    
    print(f"   Order ID: {result['orderId']}")
    print(f"   Status: {result['status']}")
    
    return result


def submit_order(
    ib: IB, 
    contract: Contract, 
    order: Order,
    symbol: str,
    spread_type: str,
    entry_price: float,
    spread_width: float,
    dte: int,
    long_leg: Option,
    short_leg: Option,
    spot_price: float,
) -> dict:
    """
    Submit order with transmit=True and record to blotter.
    
    Only records to blotter on successful submission.
    """
    from execution.blotter import get_blotter, create_trade_from_ibkr_order
    
    order.transmit = True
    
    print("\n🚀 Submitting order (transmit=True)...")
    
    trade = ib.placeOrder(contract, order)
    ib.sleep(3)
    
    result = {
        'orderId': trade.order.orderId,
        'permId': trade.order.permId,
        'status': trade.orderStatus.status,
    }
    
    print(f"   Order ID: {result['orderId']}")
    print(f"   Perm ID: {result['permId']}")
    print(f"   Status: {result['status']}")
    
    # Only record to blotter if order was accepted (not rejected)
    if result['status'] not in ['Inactive', 'Cancelled', 'ApiCancelled']:
        print("\n📝 Recording trade to blotter...")
        
        # Build leg info
        legs = [
            {
                'con_id': long_leg.conId,
                'local_symbol': long_leg.localSymbol,
                'strike': long_leg.strike,
                'expiry': long_leg.lastTradeDateOrContractMonth,
                'right': long_leg.right,
                'side': 'BUY',
                'quantity': 1,
            },
            {
                'con_id': short_leg.conId,
                'local_symbol': short_leg.localSymbol,
                'strike': short_leg.strike,
                'expiry': short_leg.lastTradeDateOrContractMonth,
                'right': short_leg.right,
                'side': 'SELL',
                'quantity': 1,
            },
        ]
        
        # Signed entry price (positive=credit, negative=debit)
        signed_entry = entry_price if spread_type == 'credit' else -entry_price
        
        paper_trade = create_trade_from_ibkr_order(
            symbol=symbol,
            spread_type=spread_type,
            entry_price=signed_entry,
            legs=legs,
            ibkr_order_id=result['orderId'],
            ibkr_perm_id=result['permId'],
            structure=f"{spread_type}_spread",
            spread_width=spread_width,
            dte=dte,
            spot_price=spot_price,
        )
        
        blotter = get_blotter()
        trade_id = blotter.record_entry(paper_trade)
        result['trade_id'] = trade_id
        
        print(f"   Trade ID: {trade_id}")
        print(f"   Max Profit: ${paper_trade.max_profit_dollars:.2f}")
        print(f"   Max Loss: ${paper_trade.max_loss_dollars:.2f}")
    else:
        print(f"\n⚠️ Order rejected - NOT recording to blotter")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Submit test order to IBKR paper")
    parser.add_argument('--paper', action='store_true', required=True, help='Confirm paper trading')
    parser.add_argument('--dry-run', action='store_true', 
                        help='Preview only (transmit=False, no blotter)')
    parser.add_argument('--submit', action='store_true',
                        help='Submit order (transmit=True, records to blotter)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show full stack traces')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=4002)
    parser.add_argument('--client-id', type=int, default=99)
    parser.add_argument('--quantity', type=int, default=1)
    parser.add_argument('--slippage', type=float, default=0.10)
    args = parser.parse_args()
    
    # Require exactly one of --dry-run or --submit
    if not args.dry_run and not args.submit:
        print("❌ Must specify --dry-run or --submit")
        return 1
    if args.dry_run and args.submit:
        print("❌ Cannot specify both --dry-run and --submit")
        return 1
    
    mode = "DRY RUN (preview only)" if args.dry_run else "LIVE SUBMIT (records to blotter)"
    
    print("=" * 60)
    print("IBKR Paper Test Order — Polygon Priced")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Port: {args.port}")
    print()
    
    ib = None
    spot_price = 0.0
    
    try:
        # Connect
        ib = connect_ibkr(args.host, args.port, args.client_id)
        
        # Find spread with Polygon pricing
        long_put, short_put, width, long_price, short_price = find_priced_spread(ib)
        spot_price = polygon_get_spot('SPY')
        
        # Calculate DTE from expiry
        expiry_str = long_put.lastTradeDateOrContractMonth
        expiry_date = datetime.strptime(expiry_str, '%Y%m%d')
        dte = (expiry_date - datetime.now()).days
        
        # Calculate limit (returns spread_type and limit)
        spread_type, limit = calculate_combo_limit(long_price, short_price, width, 
                                                    slippage=args.slippage)
        
        # Create order with correct action for spread type
        bag, order = create_combo_order(long_put, short_put, args.quantity, 
                                         spread_type, limit)
        
        if args.dry_run:
            # Preview only - no blotter
            result = preview_order(ib, bag, order)
            
            print("\n" + "=" * 60)
            print("DRY RUN COMPLETE")
            print("=" * 60)
            print(f"Order ID: {result['orderId']}")
            print(f"Status: {result['status']}")
            print()
            print("⚠️ Order NOT submitted (transmit=False)")
            print("⚠️ NOT recorded to blotter")
            print()
            print("To submit for real: python3 scripts/submit_test_order.py --paper --submit")
        else:
            # Live submit - records to blotter
            result = submit_order(
                ib, bag, order,
                symbol='SPY',
                spread_type=spread_type,
                entry_price=limit,
                spread_width=width,
                dte=dte,
                long_leg=long_put,
                short_leg=short_put,
                spot_price=spot_price,
            )
            
            print("\n" + "=" * 60)
            print("ORDER SUBMITTED")
            print("=" * 60)
            print(f"Order ID: {result['orderId']}")
            print(f"Perm ID: {result.get('permId', 'N/A')}")
            print(f"Status: {result['status']}")
            if 'trade_id' in result:
                print(f"Trade ID: {result['trade_id']}")
                print()
                print("✅ Recorded to blotter: logs/blotter/trades.jsonl")
        
        return 0
        
    except ValueError as e:
        # Friendly error for validation/pricing issues
        error_msg = str(e)
        print(f"\n❌ Rejected: {error_msg}")
        
        # Add helpful hints
        if "limit" in error_msg.lower() and "positive" in error_msg.lower():
            print("   💡 Hint: Slippage too high for credit spread. Try --slippage 0.05")
        elif "net premium" in error_msg.lower():
            print("   💡 Hint: Spread has zero/negative premium. Choose different strikes.")
        elif "polygon" in error_msg.lower() or "pricing" in error_msg.lower():
            print("   💡 Hint: No trading data for these options. Try nearer-term expiration.")
        
        return 1
        
    except ConnectionError as e:
        print(f"\n❌ Connection failed: {e}")
        print("   💡 Hint: Ensure IBKR TWS/Gateway is running on port 4002")
        return 1
        
    except Exception as e:
        # Unexpected errors - show brief message, suggest --verbose
        print(f"\n❌ Unexpected error: {e}")
        print("   Run with --verbose for full traceback")
        if args.verbose if hasattr(args, 'verbose') else False:
            import traceback
            traceback.print_exc()
        return 1
        
    finally:
        if ib and ib.isConnected():
            print("\nDisconnecting...")
            ib.disconnect()


if __name__ == "__main__":
    sys.exit(main())

