#!/usr/bin/env python3
"""
Chain Availability Audit Script.

For a given symbol and date, deterministically proves what option chains exist:
- Lists all expiries available
- For each relevant expiry: strike count, min/max, detected increment, nearest strikes to spot
"""

import sys
import gzip
import csv
from datetime import date, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

FLATFILE_DIR = Path("cache/flatfiles/options_aggs")


def load_day_options(target_date: date) -> Dict[str, Dict]:
    """Load all options from flat file for a date."""
    file_path = FLATFILE_DIR / f"{target_date.isoformat()}.csv.gz"
    if not file_path.exists():
        print(f"ERROR: No flat file for {target_date}")
        return {}
    
    options = {}
    with gzip.open(file_path, 'rt') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6:
                continue
            ticker = row[0]
            if not ticker.startswith('O:'):
                continue
            try:
                options[ticker] = {
                    'volume': int(row[1]),
                    'open': float(row[2]),
                    'close': float(row[3]),
                    'high': float(row[4]),
                    'low': float(row[5]),
                }
            except:
                continue
    return options


def parse_option_ticker(ticker: str) -> Tuple[str, date, str, float]:
    """Parse OCC ticker to (symbol, expiry, right, strike)."""
    # O:XLK220121C00083000
    ticker = ticker.replace('O:', '')
    
    # Find where the date starts (first digit after letters)
    i = 0
    while i < len(ticker) and ticker[i].isalpha():
        i += 1
    
    symbol = ticker[:i]
    rest = ticker[i:]
    
    # Date is 6 chars, then right (C/P), then strike
    expiry_str = rest[:6]
    right = rest[6]
    strike = int(rest[7:]) / 1000
    
    expiry = date(2000 + int(expiry_str[:2]), int(expiry_str[2:4]), int(expiry_str[4:6]))
    
    return symbol, expiry, right, strike


def compute_increment(strikes: List[float]) -> float:
    """Compute modal strike increment from a list of strikes."""
    if len(strikes) < 2:
        return 5.0
    
    sorted_strikes = sorted(set(strikes))
    diffs = []
    for i in range(1, len(sorted_strikes)):
        diff = round(sorted_strikes[i] - sorted_strikes[i-1], 2)
        if diff > 0:
            diffs.append(diff)
    
    if not diffs:
        return 5.0
    
    # Return the most common (modal) increment
    from collections import Counter
    counter = Counter(diffs)
    modal_increment = counter.most_common(1)[0][0]
    
    # Clamp to valid increments
    valid = [0.5, 1.0, 2.5, 5.0]
    return min(valid, key=lambda x: abs(x - modal_increment))


def audit_symbol(symbol: str, target_date: date, underlying_price: float = None):
    """Audit chain availability for a symbol on a specific date."""
    print("=" * 80)
    print(f"CHAIN AVAILABILITY AUDIT: {symbol} on {target_date}")
    print("=" * 80)
    
    options = load_day_options(target_date)
    print(f"\nLoaded {len(options):,} total options from flat file")
    
    # Filter for this symbol
    symbol_options = {k: v for k, v in options.items() if symbol in k}
    print(f"Options matching {symbol}: {len(symbol_options):,}")
    
    if not symbol_options:
        print(f"\n❌ NO OPTIONS FOUND for {symbol}")
        return
    
    # Group by expiry
    by_expiry = defaultdict(lambda: {'puts': [], 'calls': []})
    for ticker, data in symbol_options.items():
        try:
            sym, expiry, right, strike = parse_option_ticker(ticker)
            if sym != symbol:
                continue
            if right == 'P':
                by_expiry[expiry]['puts'].append((strike, data))
            else:
                by_expiry[expiry]['calls'].append((strike, data))
        except Exception as e:
            continue
    
    print(f"\nExpiries available: {len(by_expiry)}")
    
    # Sort by expiry
    for expiry in sorted(by_expiry.keys()):
        chains = by_expiry[expiry]
        puts = chains['puts']
        calls = chains['calls']
        
        put_strikes = [s for s, _ in puts]
        call_strikes = [s for s, _ in calls]
        all_strikes = sorted(set(put_strikes + call_strikes))
        
        dte = (expiry - target_date).days
        increment = compute_increment(all_strikes)
        
        print(f"\n{'─' * 60}")
        print(f"EXPIRY: {expiry} (DTE={dte})")
        print(f"  Puts: {len(puts)} | Calls: {len(calls)} | Total: {len(puts)+len(calls)}")
        print(f"  Strikes: {len(all_strikes)} unique | Min: ${min(all_strikes):.1f} | Max: ${max(all_strikes):.1f}")
        print(f"  Detected Increment: ${increment}")
        
        if underlying_price:
            # Find nearest strikes to spot
            nearest = sorted(all_strikes, key=lambda x: abs(x - underlying_price))[:10]
            print(f"  Nearest to spot (${underlying_price:.2f}): {nearest}")
            
            # Check ATM availability
            atm_approx = round(underlying_price / increment) * increment
            atm_candidates = [atm_approx + i * increment for i in range(-3, 4)]
            available_atm = [s for s in atm_candidates if s in all_strikes]
            missing_atm = [s for s in atm_candidates if s not in all_strikes]
            
            print(f"  ATM strikes expected (±3 from {atm_approx}): {atm_candidates}")
            print(f"  ATM strikes FOUND: {available_atm}")
            print(f"  ATM strikes MISSING: {missing_atm}")
            
            # Check for both call and put at ATM
            if available_atm:
                best_atm = min(available_atm, key=lambda x: abs(x - underlying_price))
                has_call = best_atm in call_strikes
                has_put = best_atm in put_strikes
                print(f"  Best ATM (${best_atm}): Call={'✓' if has_call else '✗'} Put={'✓' if has_put else '✗'}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python audit_chain.py <symbol> <date> [underlying_price]")
        print("Example: python audit_chain.py XLK 2021-12-20 84.88")
        sys.exit(1)
    
    symbol = sys.argv[1].upper()
    target_date = date.fromisoformat(sys.argv[2])
    underlying = float(sys.argv[3]) if len(sys.argv) > 3 else None
    
    audit_symbol(symbol, target_date, underlying)


if __name__ == "__main__":
    main()
