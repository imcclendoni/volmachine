#!/usr/bin/env python3
"""
Debug gating thresholds.

Prints top 20 extreme-percentile days with their skew_delta and pctl_change
to help pick empirical thresholds.
"""

import json
import sys
from pathlib import Path
from datetime import date, timedelta

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    symbols = ["SPY", "QQQ", "IWM"]
    delta_window = 5
    
    print("=" * 70)
    print("GATING DEBUG: Skew Delta Analysis")
    print("=" * 70)
    print()
    
    for symbol in symbols:
        cache_path = Path(__file__).parent.parent / 'cache' / 'edges' / f'{symbol}_skew_history_v4.json'
        
        if not cache_path.exists():
            print(f"{symbol}: No v4 history found")
            continue
        
        with open(cache_path) as f:
            data = json.load(f)
        
        skews = data.get('skew', [])
        pctls = data.get('percentile', [])
        
        print(f"{symbol}: {len(skews)} skew values, {len(pctls)} percentile values")
        
        # Find extreme days
        extremes = []
        for i in range(delta_window, len(pctls)):
            pctl = pctls[i]
            if pctl >= 90 or pctl <= 10:
                skew = skews[i] if i < len(skews) else 0
                skew_delta = skews[i] - skews[i - delta_window] if i < len(skews) else 0
                pctl_delta = pctl - pctls[i - delta_window]
                
                extremes.append({
                    'idx': i,
                    'pctl': pctl,
                    'skew': skew,
                    'skew_delta': skew_delta,
                    'pctl_delta': pctl_delta,
                    'is_steep': pctl >= 90,
                })
        
        print(f"  Extreme days (pctl <= 10 or >= 90): {len(extremes)}")
        
        if not extremes:
            print()
            continue
        
        # Analyze gating pass rates at different thresholds
        thresholds = [0.001, 0.002, 0.005, 0.01, 0.02]
        
        print(f"\n  Gating analysis (delta_window={delta_window}):")
        print(f"  {'Threshold':>10} | {'Pass Count':>10} | {'Pass Rate':>10}")
        print(f"  {'-'*10}-+-{'-'*10}-+-{'-'*10}")
        
        for thresh in thresholds:
            passed = 0
            for e in extremes:
                if e['is_steep']:
                    # Steep: skew falling (negative delta) OR pctl falling
                    if e['skew_delta'] < -thresh or e['pctl_delta'] < -10:
                        passed += 1
                else:
                    # Flat: skew rising (positive delta) OR pctl rising
                    if e['skew_delta'] > thresh or e['pctl_delta'] > 10:
                        passed += 1
            
            rate = passed / len(extremes) * 100 if extremes else 0
            print(f"  {thresh:>10.3f} | {passed:>10} | {rate:>9.1f}%")
        
        # Show last 10 extreme days
        print(f"\n  Last 10 extreme days:")
        print(f"  {'idx':>5} | {'pctl':>6} | {'skew':>8} | {'Δskew':>8} | {'Δpctl':>7} | {'Type':>6} | Pass@0.005")
        print(f"  {'-'*5}-+-{'-'*6}-+-{'-'*8}-+-{'-'*8}-+-{'-'*7}-+-{'-'*6}-+-{'-'*10}")
        
        for e in extremes[-10:]:
            thresh = 0.005
            if e['is_steep']:
                passed = e['skew_delta'] < -thresh or e['pctl_delta'] < -10
            else:
                passed = e['skew_delta'] > thresh or e['pctl_delta'] > 10
            
            pass_str = "✓" if passed else "✗"
            type_str = "STEEP" if e['is_steep'] else "FLAT"
            
            print(f"  {e['idx']:>5} | {e['pctl']:>6.0f} | {e['skew']:>8.4f} | {e['skew_delta']:>+8.4f} | {e['pctl_delta']:>+7.1f} | {type_str:>6} | {pass_str:>10}")
        
        print()
    
    print("=" * 70)
    print("RECOMMENDATION:")
    print("  Use min_skew_delta = 0.005 (0.5 vol points)")
    print("  This should keep ~40-60% of extreme signals")
    print("=" * 70)


if __name__ == "__main__":
    main()
