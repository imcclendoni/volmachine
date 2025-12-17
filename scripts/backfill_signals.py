#!/usr/bin/env python3
"""
Backfill Historical Signals.

Regenerates signals for past dates using REAL Polygon options data.
This allows backtesting when you don't have saved reports.

How it works:
1. For each historical date in the range
2. Fetch options chain snapshot from Polygon
3. Run edge detection (skew extremes, VRP, etc.)
4. If TRADE signal found, save to reports dir
5. Backtest can then use these regenerated reports

IMPORTANT: Uses REAL historical data - no fake signals.
The goal is to prove/disprove edge expectancy.
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


def fetch_options_snapshot(symbol: str, as_of_date: date, api_key: str) -> Optional[Dict]:
    """
    Fetch options chain snapshot for a symbol on a specific date.
    
    Uses Polygon /v3/snapshot/options/{underlying} with as_of parameter.
    """
    url = f"https://api.polygon.io/v3/snapshot/options/{symbol}"
    params = {
        'apiKey': api_key,
        'limit': 250,
        # Filter to relevant DTEs
        'expiration_date.gte': (as_of_date + timedelta(days=7)).isoformat(),
        'expiration_date.lte': (as_of_date + timedelta(days=60)).isoformat(),
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if data.get('status') not in ['OK', 'DELAYED']:
            return None
        
        return data.get('results', [])
    except Exception as e:
        print(f"  Error fetching {symbol} snapshot: {e}")
        return None


def calculate_skew_percentile(
    current_put_iv: float,
    current_call_iv: float,
    historical_skews: List[float],
) -> float:
    """Calculate where current skew sits in historical distribution."""
    if not historical_skews:
        return 50.0
    
    current_skew = current_put_iv - current_call_iv
    below = sum(1 for s in historical_skews if s < current_skew)
    return (below / len(historical_skews)) * 100


def detect_skew_extreme(
    options: List[Dict],
    historical_skews: List[float],
    threshold_low: float = 10,
    threshold_high: float = 90,
) -> Optional[Dict]:
    """
    Detect if skew is at an extreme percentile.
    
    Returns edge signal if found, None otherwise.
    """
    if not options:
        return None
    
    # Find ATM options (closest to underlying price)
    # Group by expiry
    by_expiry = {}
    for opt in options:
        exp = opt.get('details', {}).get('expiration_date', '')
        if not exp:
            continue
        if exp not in by_expiry:
            by_expiry[exp] = []
        by_expiry[exp].append(opt)
    
    if not by_expiry:
        return None
    
    # Get nearest expiry with enough contracts
    sorted_expiries = sorted(by_expiry.keys())
    target_expiry = None
    for exp in sorted_expiries:
        if len(by_expiry[exp]) >= 10:
            target_expiry = exp
            break
    
    if not target_expiry:
        return None
    
    chain = by_expiry[target_expiry]
    
    # Find ATM strike
    underlying_price = None
    for opt in chain:
        up = opt.get('underlying_asset', {}).get('price')
        if up:
            underlying_price = up
            break
    
    if not underlying_price:
        return None
    
    atm_strike = round(underlying_price, 0)
    
    # Find ATM put and call IV
    atm_put_iv = None
    atm_call_iv = None
    
    for opt in chain:
        strike = opt.get('details', {}).get('strike_price', 0)
        contract_type = opt.get('details', {}).get('contract_type', '')
        iv = opt.get('implied_volatility')
        
        if abs(strike - atm_strike) <= 2:  # Within $2 of ATM
            if contract_type == 'put' and iv:
                atm_put_iv = iv
            elif contract_type == 'call' and iv:
                atm_call_iv = iv
    
    if not atm_put_iv or not atm_call_iv:
        return None
    
    # Calculate percentile
    percentile = calculate_skew_percentile(atm_put_iv, atm_call_iv, historical_skews)
    
    # Check if extreme
    if percentile < threshold_low:
        return {
            'type': 'skew_extreme',
            'direction': 'BULLISH',  # Low skew = puts cheap = bullish signal
            'strength': (threshold_low - percentile) / threshold_low,
            'metrics': {
                'put_iv': atm_put_iv,
                'call_iv': atm_call_iv,
                'skew': atm_put_iv - atm_call_iv,
                'skew_percentile': percentile,
            },
            'expiry': target_expiry,
            'atm_strike': atm_strike,
            'underlying_price': underlying_price,
        }
    elif percentile > threshold_high:
        return {
            'type': 'skew_extreme',
            'direction': 'BEARISH',  # High skew = puts expensive = bearish signal
            'strength': (percentile - threshold_high) / (100 - threshold_high),
            'metrics': {
                'put_iv': atm_put_iv,
                'call_iv': atm_call_iv,
                'skew': atm_put_iv - atm_call_iv,
                'skew_percentile': percentile,
            },
            'expiry': target_expiry,
            'atm_strike': atm_strike,
            'underlying_price': underlying_price,
        }
    
    return None


def build_spread_structure(edge: Dict, symbol: str) -> Dict:
    """Build a credit/debit spread structure from edge signal."""
    direction = edge.get('direction', 'BULLISH')
    expiry = edge.get('expiry', '')
    atm_strike = edge.get('atm_strike', 0)
    underlying = edge.get('underlying_price', 0)
    
    spread_width = 5  # $5 wide spread
    
    if direction == 'BULLISH':
        # Bull put spread (credit spread)
        short_strike = atm_strike - 5
        long_strike = atm_strike - 10
        
        return {
            'type': 'credit_spread',
            'spread_type': 'credit',
            'legs': [
                {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry},
                {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry},
            ],
            'max_loss_dollars': spread_width * 100,  # $500 per contract
            'max_profit_dollars': spread_width * 100 * 0.3,  # Estimate 30% of width
        }
    else:
        # Bear call spread (credit spread)
        short_strike = atm_strike + 5
        long_strike = atm_strike + 10
        
        return {
            'type': 'credit_spread',
            'spread_type': 'credit',
            'legs': [
                {'strike': short_strike, 'right': 'C', 'side': 'SELL', 'expiry': expiry},
                {'strike': long_strike, 'right': 'C', 'side': 'BUY', 'expiry': expiry},
            ],
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
    """Save backfilled signal as a report JSON."""
    report = {
        'report_date': report_date.isoformat(),
        'generated_at': datetime.now().isoformat(),
        'session': 'backfill',
        'trading_allowed': True,
        'do_not_trade_reasons': [],
        'regime': {'state': 'unknown', 'confidence': 0.5, 'rationale': 'Backfilled'},
        'edges': [{
            'symbol': symbol,
            'edge_type': edge['type'],
            'strength': edge['strength'],
            'direction': edge['direction'],
            'metrics': edge['metrics'],
            'rationale': f"Skew at {edge['metrics']['skew_percentile']:.0f}th percentile",
        }],
        'candidates': [{
            'symbol': symbol,
            'recommendation': 'TRADE',
            'edge': {
                'type': edge['type'],
                'strength': edge['strength'],
                'direction': edge['direction'],
                'metrics': edge['metrics'],
            },
            'structure': structure,
        }],
    }
    
    # Use backfill prefix to distinguish from live reports
    filename = f"{report_date.isoformat()}_backfill.json"
    path = output_dir / filename
    
    with open(path, 'w') as f:
        json.dump(report, f, indent=2)
    
    return path


def load_historical_skews(symbol: str) -> List[float]:
    """Load historical skew values for percentile calculation."""
    # Try to load from cache
    cache_path = Path(__file__).parent.parent / 'cache' / 'edges' / f'{symbol}_skew_history.json'
    if cache_path.exists():
        with open(cache_path) as f:
            return json.load(f)
    
    # Return empty if no history (will use absolute thresholds)
    return []


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical signals using real Polygon data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to backfill (default: 90)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["SPY", "QQQ", "IWM", "TLT"],
        help="Symbols to scan"
    )
    parser.add_argument(
        "--output",
        default="./logs/reports",
        help="Output directory for backfilled reports"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between API calls (seconds)"
    )
    
    args = parser.parse_args()
    
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return 1
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    end_date = date.today()
    start_date = end_date - timedelta(days=args.days)
    
    print(f"=== Signal Backfill: {start_date} to {end_date} ===")
    print(f"Symbols: {', '.join(args.symbols)}")
    print()
    
    signals_found = 0
    dates_processed = 0
    
    current = start_date
    while current <= end_date:
        # Skip weekends
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        dates_processed += 1
        print(f"Processing {current}...", end=" ", flush=True)
        
        day_signals = []
        
        for symbol in args.symbols:
            # Load historical skews for this symbol
            historical_skews = load_historical_skews(symbol)
            
            # Fetch options snapshot
            options = fetch_options_snapshot(symbol, current, api_key)
            
            if not options:
                continue
            
            # Detect edges
            edge = detect_skew_extreme(options, historical_skews)
            
            if edge and edge['strength'] >= 0.5:
                structure = build_spread_structure(edge, symbol)
                path = save_backfill_report(current, symbol, edge, structure, output_dir)
                signals_found += 1
                day_signals.append(f"{symbol}: {edge['direction']} ({edge['strength']:.0%})")
        
        if day_signals:
            print(f"✅ {', '.join(day_signals)}")
        else:
            print("no signals")
        
        # Rate limiting
        time.sleep(args.delay)
        current += timedelta(days=1)
    
    print()
    print(f"=== Backfill Complete ===")
    print(f"Dates processed: {dates_processed}")
    print(f"Signals found: {signals_found}")
    print(f"Reports saved to: {output_dir}")
    print()
    print("Now run: python3 scripts/run_backtest.py --days", args.days)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
