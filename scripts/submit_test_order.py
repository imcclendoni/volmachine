#!/usr/bin/env python3
"""
Submit Test Order to IBKR Paper.

End-to-end test of IBKR order flow:
1. Connect to IBKR paper account
2. Use reqSecDefOptParams to get valid expiry/strikes
3. Build a small put spread (SPY)
4. Price from leg quotes (not arbitrary limit)
5. Submit with transmit=True
6. Track status changes

Usage:
    python3 scripts/submit_test_order.py --paper
    python3 scripts/submit_test_order.py --paper --dry-run  # Preview only
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
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


def get_polygon_option_quote(option: Option) -> dict:
    """
    Get option quote from Polygon API using aggregates endpoint.
    Synthesizes bid/ask from close with 2.5% spread.
    """
    import requests
    from datetime import datetime, timedelta
    
    api_key = get_polygon_api_key()
    if not api_key:
        print("      ⚠️ No Polygon API key")
        return {'bid': None, 'ask': None, 'mid': None, 'last': None, 'close': None}
    
    # Convert IBKR localSymbol to Polygon ticker
    # IBKR: "SPY   251230P00565000" -> Polygon: "O:SPY251230P00565000"
    local_sym = option.localSymbol if hasattr(option, 'localSymbol') and option.localSymbol else None
    
    if local_sym:
        ticker = "O:" + local_sym.replace(" ", "")
    else:
        exp = option.lastTradeDateOrContractMonth
        if len(exp) == 8:
            exp = exp[2:]
        strike_str = f"{int(option.strike * 1000):08d}"
        right = 'P' if option.right == 'P' else 'C'
        ticker = f"O:{option.symbol}{exp}{right}{strike_str}"
    
    print(f"      Polygon ticker: {ticker}")
    
    # Try with 14-day window first, then 30-day fallback
    for days_back in [14, 30]:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {'apiKey': api_key, 'adjusted': 'true', 'sort': 'desc', 'limit': 1}
        
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            
            if data.get('status') == 'OK' and data.get('resultsCount', 0) > 0:
                result = data['results'][0]
                
                # Use close, fallback to vwap, then open
                mid = result.get('c') or result.get('vw') or result.get('o')
                
                if mid:
                    # Synthesize bid/ask with 2.5% half-spread
                    half_spread = 0.025
                    bid = round(mid * (1 - half_spread), 2)
                    ask = round(mid * (1 + half_spread), 2)
                    
                    print(f"      ✅ close=${result.get('c')}, vwap=${result.get('vw')}")
                    print(f"      Synthesized: bid=${bid}, mid=${mid}, ask=${ask}")
                    
                    return {
                        'bid': bid,
                        'ask': ask,
                        'mid': mid,
                        'last': result.get('c'),
                        'close': result.get('c'),
                    }
            
            # If no results with this window, try wider
            if days_back == 14:
                print(f"      No data in {days_back}d window, trying 30d...")
                continue
            
        except Exception as e:
            print(f"      Polygon error: {e}")
            break
    
    print(f"      ❌ No Polygon data for {ticker}")
    return {'bid': None, 'ask': None, 'mid': None, 'last': None, 'close': None}


def get_option_quote(ib: IB, option: Option) -> dict:
    """
    Get bid/ask/mid for an option contract.
    Tries IBKR first, falls back to Polygon if no data.
    """
    ticker = ib.reqTickers(option)[0]
    ib.sleep(1)
    
    bid = ticker.bid if ticker.bid and ticker.bid > 0 else None
    ask = ticker.ask if ticker.ask and ticker.ask > 0 else None
    last = ticker.last if ticker.last and ticker.last > 0 else None
    close = ticker.close if ticker.close and ticker.close > 0 else None
    
    # If no IBKR data, try Polygon
    if not bid and not ask and not last:
        print(f"      No IBKR data, trying Polygon...")
        return get_polygon_option_quote(option)
    
    # Calculate mid
    if bid and ask:
        mid = (bid + ask) / 2
    elif last:
        mid = last
    elif close:
        mid = close
    else:
        mid = None
    
    return {
        'bid': bid,
        'ask': ask,
        'last': last,
        'close': close,
        'mid': mid,
    }


def find_valid_spread(ib: IB, spot: float = None) -> tuple[Option, Option, float, dict, dict]:
    """
    Find a valid SPY put spread using IBKR chain data.
    
    Returns: (long_put, short_put, width, long_quote, short_quote)
    """
    print("\n📊 Finding valid SPY options...")
    
    # Get SPY underlying
    spy = Stock('SPY', 'SMART', 'USD')
    ib.qualifyContracts(spy)
    
    # Get spot price
    if spot is None:
        ticker = ib.reqTickers(spy)[0]
        ib.sleep(1)
        
        spot = ticker.marketPrice()
        if spot is None or spot != spot:
            spot = ticker.close
        if spot is None or spot != spot:
            spot = ticker.last
        if spot is None or spot != spot:
            spot = 595.0  # Fallback
            print(f"   ⚠️ Using fallback spot: ${spot:.2f}")
        else:
            print(f"   SPY spot: ${spot:.2f}")
    
    # Ensure spot is reasonable (sanity check)
    if spot < 100 or spot > 1000:
        print(f"   ⚠️ Spot ${spot} seems wrong, using 595.0")
        spot = 595.0
    
    print(f"   DEBUG: spot = {spot} (type: {type(spot).__name__})")
    
    # Get option chain
    chains = ib.reqSecDefOptParams('SPY', '', 'STK', spy.conId)
    if not chains:
        raise ValueError("No option chains found")
    
    print(f"   DEBUG: Found {len(chains)} exchange chains")
    
    # Pick a good exchange (NASDAQOM or AMEX have weekly/monthly, avoid CBOE2 which has only far expiries)
    # Prefer exchanges with many near-term expirations
    min_exp_cutoff = (datetime.now() + timedelta(days=30)).strftime('%Y%m%d')
    
    best_chain = None
    best_score = 0
    
    for chain in chains:
        # Count near-term expirations
        near_exps = [e for e in chain.expirations if e <= min_exp_cutoff]
        score = len(near_exps)
        
        if score > best_score:
            best_score = score
            best_chain = chain
    
    if not best_chain:
        # Fallback: use first chain with most strikes
        best_chain = max(chains, key=lambda c: len(c.strikes))
    
    print(f"   DEBUG: Selected exchange: {best_chain.exchange}")
    print(f"   DEBUG: Chain has {len(best_chain.expirations)} expirations, {len(best_chain.strikes)} strikes")
    
    # Convert strikes to float EXPLICITLY and sort
    available_strikes = sorted([float(s) for s in best_chain.strikes])
    available_expirations = sorted(best_chain.expirations)
    
    # Show first 10 strikes near spot
    near_strikes = sorted(available_strikes, key=lambda x: abs(x - spot))[:10]
    print(f"   DEBUG: 10 strikes near ${spot:.0f}: {near_strikes}")
    
    # Filter to future expirations (>= 14 DTE for better liquidity)
    min_exp = (datetime.now() + timedelta(days=14)).strftime('%Y%m%d')
    valid_exps = [e for e in available_expirations if e >= min_exp]
    
    if not valid_exps:
        min_exp = (datetime.now() + timedelta(days=7)).strftime('%Y%m%d')
        valid_exps = [e for e in available_expirations if e >= min_exp]
    
    if not valid_exps:
        raise ValueError("No valid expirations found")
    
    target_exp = valid_exps[0]
    print(f"   Expiration: {target_exp}")
    
    # Select strikes NEAR ATM (±2% of spot) for better Polygon liquidity
    # Short strike: highest strike <= spot (slightly ITM put)
    strikes_at_or_below = [s for s in available_strikes if s <= spot]
    if not strikes_at_or_below:
        strikes_at_or_below = available_strikes[:10]
    
    # Target ATM or 2% OTM for guaranteed trading volume
    target_short = spot * 0.98  # 2% OTM instead of 5%
    short_strike = min(strikes_at_or_below, key=lambda x: abs(x - target_short))
    
    print(f"   DEBUG: target_short = {target_short:.2f}, selected short_strike = {short_strike}")
    
    # Find short strike index in the sorted list
    try:
        short_idx = available_strikes.index(short_strike)
    except ValueError:
        raise ValueError(f"Short strike {short_strike} not in available_strikes list")
    
    print(f"   DEBUG: short_strike {short_strike} is at index {short_idx} in strikes list")
    
    # Long strike: next lower strike in the list (not short - 1)
    if short_idx <= 0:
        raise ValueError(f"No strikes below {short_strike}")
    
    # Try to qualify both legs
    short_put = None
    long_put = None
    width = 0
    
    # First, qualify short leg
    test_short = Option('SPY', target_exp, short_strike, 'P', 'SMART', currency='USD')
    ib.qualifyContracts(test_short)
    
    if not test_short.conId:
        # Try next strike up if short doesn't qualify
        for offset in range(1, 5):
            if short_idx + offset < len(available_strikes):
                alt_strike = available_strikes[short_idx + offset]
                test_short = Option('SPY', target_exp, alt_strike, 'P', 'SMART', currency='USD')
                ib.qualifyContracts(test_short)
                if test_short.conId:
                    short_strike = alt_strike
                    short_idx = available_strikes.index(short_strike)
                    break
    
    if not test_short.conId:
        raise ValueError(f"Could not qualify short put near {short_strike}")
    
    short_put = test_short
    print(f"   ✅ Short: {short_strike}P conId={short_put.conId}")
    
    # Now find long leg: iterate down from short_idx - 1
    for offset in range(1, min(10, short_idx + 1)):
        long_strike = available_strikes[short_idx - offset]
        test_long = Option('SPY', target_exp, long_strike, 'P', 'SMART', currency='USD')
        ib.qualifyContracts(test_long)
        
        if test_long.conId:
            long_put = test_long
            width = short_strike - long_strike
            print(f"   ✅ Long: {long_strike}P conId={long_put.conId}")
            print(f"   Spread width: ${width}")
            break
    
    if not long_put:
        raise ValueError(f"Could not qualify any long put below {short_strike}")
    
    # Get quotes for both legs
    print("\n📈 Fetching leg quotes...")
    short_quote = get_option_quote(ib, short_put)
    long_quote = get_option_quote(ib, long_put)
    
    print(f"   Short {short_strike}P: bid={short_quote['bid']}, ask={short_quote['ask']}, mid={short_quote['mid']}")
    print(f"   Long {long_strike}P: bid={long_quote['bid']}, ask={long_quote['ask']}, mid={long_quote['mid']}")
    
    return long_put, short_put, width, long_quote, short_quote


def calculate_debit_limit(
    long_quote: dict, 
    short_quote: dict, 
    width: float, 
    slippage: float = 0.10,
    allow_estimated: bool = False,
) -> float:
    """
    Calculate limit price for debit spread from leg quotes.
    
    Debit spread: BUY long (pay ask), SELL short (receive bid)
    Conservative debit = long.ask - short.bid
    Mid debit = long.mid - short.mid
    
    Returns limit price or raises error if invalid.
    """
    print("\n💰 Calculating combo limit price...")
    
    # Try to calculate from bid/ask
    if long_quote['ask'] and short_quote['bid']:
        conservative_debit = long_quote['ask'] - short_quote['bid']
        print(f"   Conservative debit: ${conservative_debit:.2f} (long ask - short bid)")
    else:
        conservative_debit = None
    
    # Mid-based pricing
    if long_quote['mid'] and short_quote['mid']:
        mid_debit = long_quote['mid'] - short_quote['mid']
        print(f"   Mid debit: ${mid_debit:.2f} (long mid - short mid)")
    else:
        mid_debit = None
    
    # Choose best available
    if conservative_debit is not None:
        limit = round(conservative_debit * (1 + slippage), 2)
    elif mid_debit is not None:
        limit = round(mid_debit * (1 + slippage), 2)
    else:
        # Last resort: use last/close prices
        long_price = long_quote['last'] or long_quote['close'] or 0
        short_price = short_quote['last'] or short_quote['close'] or 0
        if long_price and short_price:
            limit = round((long_price - short_price) * (1 + slippage), 2)
            print(f"   Using last/close prices: ${limit:.2f}")
        elif allow_estimated:
            # Estimated price for after-hours testing: ~20% of width for far OTM put spreads
            limit = round(width * 0.20, 2)
            print(f"   ⚠️ Using ESTIMATED price: ${limit:.2f} (20% of ${width} width)")
            print(f"   ⚠️ This is for testing only - will likely be rejected or need adjustment")
        else:
            raise ValueError("No valid quotes to price combo - cannot submit (use --test-price for estimated)")
    
    
    # Sanity checks
    if limit <= 0:
        raise ValueError(f"Invalid debit ${limit:.2f} - combo would be a credit, not debit")
    
    if limit > width:
        raise ValueError(f"Debit ${limit:.2f} exceeds width ${width:.2f} - guaranteed loss")
    
    if limit > width * 0.8:
        print(f"   ⚠️ WARNING: Debit is {limit/width*100:.0f}% of max profit - poor risk/reward")
    
    print(f"   ✅ Limit price: ${limit:.2f} (with {slippage*100:.0f}% slippage buffer)")
    
    return limit


def create_combo_order(
    long_leg: Option,
    short_leg: Option,
    quantity: int,
    limit_price: float,
) -> tuple[Contract, Order]:
    """Create BAG contract and limit order for debit spread."""
    print("\n📦 Creating BAG combo...")
    
    # Sanity checks
    if not long_leg.conId or not short_leg.conId:
        raise ValueError(f"Invalid conIds: long={long_leg.conId}, short={short_leg.conId}")
    
    if limit_price <= 0:
        raise ValueError(f"Invalid limit price: ${limit_price}")
    
    # Create BAG
    bag = Contract()
    bag.symbol = 'SPY'
    bag.secType = 'BAG'
    bag.exchange = 'SMART'
    bag.currency = 'USD'
    
    # Long leg (buy)
    long_combo = ComboLeg()
    long_combo.conId = long_leg.conId
    long_combo.ratio = 1
    long_combo.action = 'BUY'
    long_combo.exchange = 'SMART'
    
    # Short leg (sell)
    short_combo = ComboLeg()
    short_combo.conId = short_leg.conId
    short_combo.ratio = 1
    short_combo.action = 'SELL'
    short_combo.exchange = 'SMART'
    
    bag.comboLegs = [long_combo, short_combo]
    
    # Create order
    order = Order()
    order.action = 'BUY'
    order.orderType = 'LMT'
    order.totalQuantity = quantity
    order.lmtPrice = limit_price
    order.tif = 'DAY'
    
    print(f"   Long leg: conId={long_leg.conId} (BUY)")
    print(f"   Short leg: conId={short_leg.conId} (SELL)")
    print(f"   Order: BUY {quantity} @ ${limit_price:.2f} LMT")
    
    return bag, order


def submit_order(ib: IB, contract: Contract, order: Order, transmit: bool) -> dict:
    """Submit order and monitor status."""
    order.transmit = transmit
    
    if transmit:
        print("\n🚀 Submitting order (transmit=True)...")
    else:
        print("\n👁️ Preview order (transmit=False)...")
    
    trade = ib.placeOrder(contract, order)
    ib.sleep(2)
    
    result = {
        'orderId': trade.order.orderId,
        'status': trade.orderStatus.status,
        'filled': trade.orderStatus.filled,
        'remaining': trade.orderStatus.remaining,
        'avgFillPrice': trade.orderStatus.avgFillPrice,
    }
    
    print(f"   Order ID: {result['orderId']}")
    print(f"   Status: {result['status']}")
    
    if transmit:
        # Monitor for changes
        print("\n📡 Monitoring (10 seconds)...")
        for _ in range(10):
            ib.sleep(1)
            new_status = trade.orderStatus.status
            if new_status != result['status']:
                print(f"   ➡️ {result['status']} → {new_status}")
                result['status'] = new_status
            
            if new_status in ['Filled', 'Cancelled', 'Inactive']:
                break
        
        result['filled'] = trade.orderStatus.filled
        result['avgFillPrice'] = trade.orderStatus.avgFillPrice
        
        print(f"\n📋 Final:")
        print(f"   Status: {result['status']}")
        print(f"   Filled: {result['filled']}")
        print(f"   Avg Price: {result['avgFillPrice']}")
        
        # Check for errors
        if trade.log:
            for entry in trade.log:
                if entry.errorCode:
                    print(f"   ⚠️ Error {entry.errorCode}: {entry.message}")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Submit test order to IBKR paper")
    parser.add_argument('--paper', action='store_true', required=True, help='Confirm paper trading')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--test-price', action='store_true', help='Use estimated price when no quotes available')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=4002)
    parser.add_argument('--client-id', type=int, default=99)
    parser.add_argument('--quantity', type=int, default=1)
    parser.add_argument('--slippage', type=float, default=0.10, help='Slippage buffer (default: 10%)')
    args = parser.parse_args()
    
    print("=" * 60)
    print("IBKR Paper Test Order")
    print("=" * 60)
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE SUBMIT'}")
    if args.test_price:
        print("Test pricing: ENABLED (will use estimated price if no quotes)")
    print(f"Port: {args.port}")
    print()
    
    ib = None
    try:
        # Connect
        ib = connect_ibkr(args.host, args.port, args.client_id)
        
        # Find valid spread
        long_put, short_put, width, long_quote, short_quote = find_valid_spread(ib)
        
        # Calculate limit from quotes
        limit = calculate_debit_limit(
            long_quote, short_quote, width, 
            slippage=args.slippage,
            allow_estimated=args.test_price,
        )
        
        # Create order
        bag, order = create_combo_order(long_put, short_put, args.quantity, limit)
        
        # Submit
        result = submit_order(ib, bag, order, transmit=not args.dry_run)
        
        print("\n" + "=" * 60)
        print("TEST COMPLETE")
        print("=" * 60)
        print(f"Order ID: {result['orderId']}")
        print(f"Status: {result['status']}")
        
        if result['status'] in ['Cancelled', 'Inactive']:
            print("\n⚠️ Order was rejected/cancelled")
            return 1
        
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
