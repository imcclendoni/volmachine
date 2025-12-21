#!/usr/bin/env python3
"""
Coverage Check for Universe Expansion

Checks data quality for each candidate symbol before adding to universe.
Symbols must have >= 90% coverage and stable option data.
"""

import json
import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Dict, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.option_bar_store import OptionBarStore

FLATFILES_DIR = Path(__file__).parent.parent / 'cache' / 'flatfiles'
BAR_STORE = OptionBarStore(cache_dir=FLATFILES_DIR, mode='thin')

# Candidate symbols by cluster
CLUSTERS = {
    'us_broad': ['SPY', 'QQQ', 'IWM', 'DIA'],
    'sectors': ['XLF', 'XLE', 'XLK', 'XLI', 'XLY', 'XLP', 'XLU'],
    'rates': ['TLT', 'IEF', 'HYG', 'LQD'],
    'commodities': ['GLD', 'SLV', 'USO', 'UUP'],
    'intl': ['EEM', 'EFA'],
}

# Minimum coverage threshold
MIN_COVERAGE = 0.90


def check_symbol_coverage(symbol: str, start_date: date, end_date: date) -> Dict:
    """Check if symbol has adequate option data coverage."""
    total_days = 0
    covered_days = 0
    sample_strikes_found = 0
    
    current = start_date
    while current <= end_date:
        # Skip weekends
        if current.weekday() < 5:
            total_days += 1
            
            # Check if we have flat file for this date
            if BAR_STORE.has_date(current):
                # Try to load and check for symbol options
                BAR_STORE.load_day(current)
                
                # Look for any options for this symbol
                day_data = BAR_STORE._day_cache.get(current.isoformat(), {})
                symbol_options = [k for k in day_data.keys() if symbol in k]
                
                if symbol_options:
                    covered_days += 1
                    sample_strikes_found = max(sample_strikes_found, len(symbol_options))
                
                BAR_STORE.evict_day(current)
        
        current += timedelta(days=1)
    
    coverage = covered_days / total_days if total_days > 0 else 0
    
    return {
        'symbol': symbol,
        'total_days': total_days,
        'covered_days': covered_days,
        'coverage': coverage,
        'pass': coverage >= MIN_COVERAGE,
        'sample_strikes': sample_strikes_found,
    }


def main():
    # Date range (4 years)
    end_date = date.today()
    start_date = end_date - timedelta(days=4*365)
    
    print("=" * 70)
    print("COVERAGE CHECK FOR UNIVERSE EXPANSION")
    print("=" * 70)
    print(f"\nPeriod: {start_date} to {end_date}")
    print(f"Minimum coverage required: {MIN_COVERAGE*100:.0f}%\n")
    
    all_results = []
    
    for cluster, symbols in CLUSTERS.items():
        print(f"\n--- {cluster.upper()} ---")
        
        for symbol in symbols:
            print(f"Checking {symbol}...", end=" ", flush=True)
            result = check_symbol_coverage(symbol, start_date, end_date)
            all_results.append({'cluster': cluster, **result})
            
            status = "✓ PASS" if result['pass'] else "✗ FAIL"
            print(f"{result['coverage']*100:.1f}% ({result['covered_days']}/{result['total_days']}) {status}")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    passed = [r for r in all_results if r['pass']]
    failed = [r for r in all_results if not r['pass']]
    
    print(f"\n✓ Passed ({len(passed)}):")
    for r in passed:
        print(f"  {r['symbol']}: {r['coverage']*100:.1f}% ({r['cluster']})")
    
    if failed:
        print(f"\n✗ Failed ({len(failed)}):")
        for r in failed:
            print(f"  {r['symbol']}: {r['coverage']*100:.1f}% ({r['cluster']})")
    
    # Recommended universe
    print("\n" + "-" * 70)
    print("RECOMMENDED UNIVERSE")
    print("-" * 70)
    
    by_cluster = {}
    for r in passed:
        cluster = r['cluster']
        if cluster not in by_cluster:
            by_cluster[cluster] = []
        by_cluster[cluster].append(r['symbol'])
    
    for cluster, symbols in by_cluster.items():
        print(f"  {cluster}: {', '.join(symbols)}")
    
    print(f"\nTotal qualified: {len(passed)} symbols")


if __name__ == '__main__':
    main()
