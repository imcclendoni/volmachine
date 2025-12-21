#!/usr/bin/env python3
"""
Leave-One-Year-Out Validation

For each year, calibrate threshold on other years, evaluate on held-out year.
This is the minimum standard to claim "not delusion / not lucky."
"""

import subprocess
import re
import sys
from pathlib import Path
from datetime import date, timedelta

YEARS = [2022, 2023, 2024, 2025]
THRESHOLDS = [70, 75, 80, 85, 90]

def run_backtest(start_year, end_year, pctl_threshold):
    """Run STEEP-only backtest for a specific year range."""
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    cmd = [
        'python3', 'scripts/run_backtest.py',
        '--input-dir', 'logs/backfill/v6/reports',
        '--start', start_date,
        '--end', end_date,
        '--phase', 'phase1',
        '--symbols', 'SPY', 'QQQ', 'IWM', 'XLF', 'GLD', 'TLT', 'DIA',
        '--edge-slice', 'steep',
    ]
    
    if pctl_threshold:
        cmd.extend(['--max-atm-iv-pctl', str(pctl_threshold)])
    
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
    return result.stdout + result.stderr


def parse_results(output):
    """Extract key metrics from backtest output."""
    trades_match = re.search(r'TRADES: (\d+) total \((\d+)W / (\d+)L\)', output)
    pf_match = re.search(r'PROFIT FACTOR: ([\d.]+)', output)
    pnl_match = re.search(r'TOTAL PnL: \$([-\d.]+)', output)
    
    if trades_match:
        return {
            'trades': int(trades_match.group(1)),
            'pf': float(pf_match.group(1)) if pf_match else 0,
            'pnl': float(pnl_match.group(1)) if pnl_match else 0,
        }
    return {'trades': 0, 'pf': 0, 'pnl': 0}


def main():
    print("=" * 70)
    print("LEAVE-ONE-YEAR-OUT (LOYO) VALIDATION")
    print("=" * 70)
    print("\nFor each year: calibrate on other years, evaluate on held-out year.")
    print("Dataset: v6, 7 symbols, STEEP-only\n")
    
    results = []
    
    for holdout_year in YEARS:
        print(f"\n--- Holdout Year: {holdout_year} ---")
        
        # Training years (all except holdout)
        train_years = [y for y in YEARS if y != holdout_year]
        print(f"Training on: {train_years}")
        
        # Find best threshold on training data
        best_threshold = None
        best_pf = 0
        
        for pctl in THRESHOLDS:
            # Combine results across training years
            total_pnl = 0
            total_trades = 0
            
            for train_year in train_years:
                output = run_backtest(train_year, train_year, pctl)
                metrics = parse_results(output)
                total_pnl += metrics['pnl']
                total_trades += metrics['trades']
            
            # Calculate combined PF approximation
            if total_trades > 0:
                avg_pf = total_pnl / max(1, total_trades)  # Using average as proxy
                if avg_pf > best_pf:
                    best_pf = avg_pf
                    best_threshold = pctl
        
        if best_threshold:
            print(f"Best threshold on training: {best_threshold}th percentile")
        else:
            best_threshold = 90  # Default
            print("No positive threshold found, using 90th percentile")
        
        # Evaluate on holdout year
        holdout_output = run_backtest(holdout_year, holdout_year, best_threshold)
        holdout_metrics = parse_results(holdout_output)
        
        result = {
            'holdout_year': holdout_year,
            'threshold': best_threshold,
            **holdout_metrics
        }
        results.append(result)
        
        status = "✓ PASS" if holdout_metrics['pnl'] > 0 else "✗ FAIL"
        print(f"Holdout {holdout_year} with <= {best_threshold}th pctl: {status}")
        print(f"  Trades: {holdout_metrics['trades']}, PF: {holdout_metrics['pf']:.2f}, PnL: ${holdout_metrics['pnl']:.2f}")
    
    print("\n" + "=" * 70)
    print("LOYO VALIDATION RESULTS")
    print("=" * 70)
    print(f"\n{'Holdout':<10} {'Threshold':<12} {'Trades':<8} {'PF':<8} {'PnL':<12} {'Status':<8}")
    print("-" * 60)
    
    passed = 0
    for r in results:
        status = "PASS" if r['pnl'] > 0 else "FAIL"
        if r['pnl'] > 0:
            passed += 1
        print(f"{r['holdout_year']:<10} <= {r['threshold']:<5}th {r['trades']:<8} {r['pf']:<8.2f} ${r['pnl']:<11.2f} {status:<8}")
    
    print("-" * 60)
    print(f"\n{'✓' if passed >= 3 else '✗'} {passed}/{len(YEARS)} holdout years positive")
    
    if passed >= 3:
        print("✓ LOYO VALIDATION PASSED - Edge appears robust across time")
    else:
        print("✗ LOYO VALIDATION FAILED - Edge may be regime-dependent or overfit")


if __name__ == '__main__':
    main()
