#!/usr/bin/env python3
"""
Backfill Historical Signals - Version 2.

Uses Polygon option daily bars (not live snapshot) to detect historical edges.

Approach:
1. For each historical date, get underlying price
2. Build ATM option OCC symbols for that date
3. Fetch put and call daily bars
4. Compare prices as skew proxy
5. If skew extreme detected, save signal

This uses REAL historical data from Polygon aggs endpoint.
"""

import argparse
import sys
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_polygon_api_key() -> str:
    """Get Polygon API key."""
    import os
    key = os.environ.get('POLYGON_API_KEY')
    if key:
        return key
    
    secrets_path = Path(__file__).parent.parent / '.streamlit' / 'secrets.toml'
    if secrets_path.exists():
        import toml
        return toml.load(secrets_path).get('POLYGON_API_KEY', '')
    return ''


def get_underlying_price(symbol: str, as_of_date: date, api_key: str) -> Optional[float]:
    """Get underlying close price for a specific date."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{as_of_date.isoformat()}/{as_of_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'true'}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        results = data.get('results', [])
        if results:
            return results[0].get('c')  # Close price
    except:
        pass
    return None


def build_occ_symbol(symbol: str, expiry: date, strike: float, right: str) -> str:
    """Build OCC option symbol."""
    exp_str = expiry.strftime('%y%m%d')
    strike_int = int(strike * 1000)
    return f"{symbol.ljust(6)}{exp_str}{right}{strike_int:08d}"


def get_option_price(occ: str, target_date: date, api_key: str) -> Optional[float]:
    """Get option close price for a specific date."""
    ticker = f"O:{occ.replace(' ', '')}"
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date.isoformat()}/{target_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'true'}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        results = data.get('results', [])
        if results:
            return results[0].get('c')  # Close price
    except:
        pass
    return None


def find_nearest_monthly_expiry(as_of: date) -> date:
    """Find the next monthly options expiry (3rd Friday)."""
    # Start from as_of, find 3rd Friday at least 14 days out
    target = as_of + timedelta(days=14)
    
    # Find the month's third Friday
    first_of_month = target.replace(day=1)
    first_friday = first_of_month + timedelta(days=(4 - first_of_month.weekday()) % 7)
    third_friday = first_friday + timedelta(days=14)
    
    if third_friday < as_of + timedelta(days=7):
        # Move to next month
        next_month = first_of_month + timedelta(days=32)
        first_of_month = next_month.replace(day=1)
        first_friday = first_of_month + timedelta(days=(4 - first_of_month.weekday()) % 7)
        third_friday = first_friday + timedelta(days=14)
    
    return third_friday


def detect_skew_from_prices(
    put_price: float,
    call_price: float,
    underlying_price: float,
    strike: float,
    skew_history: List[float],
) -> Optional[Dict]:
    """
    Detect skew extreme from ATM put vs call prices.
    
    Skew = (Put Price / Call Price) ratio
    Higher ratio = more expensive puts = higher skew
    """
    if not put_price or not call_price or call_price < 0.05:
        return None
    
    skew_ratio = put_price / call_price
    
    # Calculate percentile if we have history
    if skew_history:
        below = sum(1 for s in skew_history if s < skew_ratio)
        percentile = (below / len(skew_history)) * 100
    else:
        # No history - use absolute thresholds
        # Typical ATM put/call ratio is around 1.0-1.2
        if skew_ratio > 1.5:
            percentile = 90  # High skew
        elif skew_ratio < 0.8:
            percentile = 10  # Low skew
        else:
            percentile = 50  # Normal
    
    # Check extremes
    if percentile > 90:
        return {
            'type': 'skew_extreme',
            'direction': 'BEARISH',
            'strength': min(1.0, (percentile - 90) / 10 + 0.5),
            'metrics': {
                'put_price': put_price,
                'call_price': call_price,
                'skew_ratio': skew_ratio,
                'skew_percentile': percentile,
            },
            'strike': strike,
            'underlying_price': underlying_price,
        }
    elif percentile < 10:
        return {
            'type': 'skew_extreme',
            'direction': 'BULLISH',
            'strength': min(1.0, (10 - percentile) / 10 + 0.5),
            'metrics': {
                'put_price': put_price,
                'call_price': call_price,
                'skew_ratio': skew_ratio,
                'skew_percentile': percentile,
            },
            'strike': strike,
            'underlying_price': underlying_price,
        }
    
    return None


def build_spread_structure(edge: Dict, symbol: str, expiry: date) -> Dict:
    """Build spread structure from edge."""
    direction = edge.get('direction', 'BULLISH')
    strike = edge.get('strike', 0)
    
    spread_width = 5
    
    if direction == 'BULLISH':
        short_strike = strike - 5
        long_strike = strike - 10
        legs = [
            {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat()},
            {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat()},
        ]
    else:
        short_strike = strike + 5
        long_strike = strike + 10
        legs = [
            {'strike': short_strike, 'right': 'C', 'side': 'SELL', 'expiry': expiry.isoformat()},
            {'strike': long_strike, 'right': 'C', 'side': 'BUY', 'expiry': expiry.isoformat()},
        ]
    
    return {
        'type': 'credit_spread',
        'spread_type': 'credit',
        'legs': legs,
        'max_loss_dollars': spread_width * 100,
        'max_profit_dollars': spread_width * 100 * 0.3,
    }


def save_backfill_report(
    report_date: date,
    symbol: str,
    edge: Dict,
    structure: Dict,
    output_dir: Path,
) -> Path:
    """Save backfilled signal as report JSON."""
    report = {
        'report_date': report_date.isoformat(),
        'generated_at': datetime.now().isoformat(),
        'session': 'backfill',
        'trading_allowed': True,
        'do_not_trade_reasons': [],
        'regime': {'state': 'unknown', 'confidence': 0.5},
        'edges': [{
            'symbol': symbol,
            'edge_type': edge['type'],
            'strength': edge['strength'],
            'direction': edge['direction'],
            'metrics': edge['metrics'],
        }],
        'candidates': [{
            'symbol': symbol,
            'recommendation': 'TRADE',
            'edge': edge,
            'structure': structure,
        }],
    }
    
    filename = f"{report_date.isoformat()}_backfill.json"
    path = output_dir / filename
    
    with open(path, 'w') as f:
        json.dump(report, f, indent=2)
    
    return path


def load_skew_history(symbol: str) -> List[float]:
    """Load historical skew ratios."""
    cache_path = Path(__file__).parent.parent / 'cache' / 'edges' / f'{symbol}_skew_ratios.json'
    if cache_path.exists():
        with open(cache_path) as f:
            return json.load(f)
    return []


def save_skew_history(symbol: str, ratios: List[float]):
    """Save skew ratios for future percentile calculations."""
    cache_dir = Path(__file__).parent.parent / 'cache' / 'edges'
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f'{symbol}_skew_ratios.json'
    with open(path, 'w') as f:
        json.dump(ratios[-252:], f)  # Keep last 252 trading days


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical signals using Polygon option bars"
    )
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "IWM", "TLT"])
    parser.add_argument("--output", default="./logs/reports")
    parser.add_argument("--delay", type=float, default=0.3)
    parser.add_argument("--build-history", action="store_true", help="Build skew history only, no signals")
    
    args = parser.parse_args()
    
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return 1
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    end_date = date.today() - timedelta(days=1)  # Yesterday (need settled data)
    start_date = end_date - timedelta(days=args.days)
    
    print(f"=== Signal Backfill v2: {start_date} to {end_date} ===")
    print(f"Symbols: {', '.join(args.symbols)}")
    print()
    
    signals_found = 0
    dates_processed = 0
    
    # Track skew ratios for history building
    skew_histories = {sym: load_skew_history(sym) for sym in args.symbols}
    
    current = start_date
    while current <= end_date:
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        dates_processed += 1
        print(f"Processing {current}...", end=" ", flush=True)
        
        day_signals = []
        
        for symbol in args.symbols:
            # Get underlying price
            underlying = get_underlying_price(symbol, current, api_key)
            if not underlying:
                continue
            
            # Find expiry and ATM strike
            expiry = find_nearest_monthly_expiry(current)
            atm_strike = round(underlying / 5) * 5  # Round to nearest $5
            
            # Build OCC symbols
            put_occ = build_occ_symbol(symbol, expiry, atm_strike, 'P')
            call_occ = build_occ_symbol(symbol, expiry, atm_strike, 'C')
            
            # Get prices
            put_price = get_option_price(put_occ, current, api_key)
            call_price = get_option_price(call_occ, current, api_key)
            
            time.sleep(args.delay)  # Rate limit
            
            if put_price and call_price and call_price > 0.05:
                skew_ratio = put_price / call_price
                skew_histories[symbol].append(skew_ratio)
                
                if not args.build_history:
                    # Detect edge
                    edge = detect_skew_from_prices(
                        put_price, call_price, underlying, atm_strike,
                        skew_histories[symbol][:-1]  # Exclude current for percentile
                    )
                    
                    if edge and edge['strength'] >= 0.5:
                        structure = build_spread_structure(edge, symbol, expiry)
                        save_backfill_report(current, symbol, edge, structure, output_dir)
                        signals_found += 1
                        day_signals.append(f"{symbol}: {edge['direction']} (ratio={skew_ratio:.2f})")
        
        if day_signals:
            print(f"✅ {', '.join(day_signals)}")
        else:
            print("scanned")
        
        current += timedelta(days=1)
    
    # Save skew histories
    for symbol in args.symbols:
        save_skew_history(symbol, skew_histories[symbol])
    
    print()
    print(f"=== Backfill Complete ===")
    print(f"Dates processed: {dates_processed}")
    print(f"Signals found: {signals_found}")
    print(f"Skew history saved for percentile calculations")
    print()
    
    if signals_found > 0:
        print(f"Now run: python3 scripts/run_backtest.py --days {args.days}")
    else:
        print("No extreme skew detected. Try --build-history first to build baseline.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
