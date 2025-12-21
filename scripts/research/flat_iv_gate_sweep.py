#!/usr/bin/env python3
"""
FLAT IV Gate Sweep - Find optimal ATM IV percentile threshold.

Sweeps max_atm_iv_pctl from 50 to 100 and reports:
- Trades, WR, PF, PnL, MaxDD, CAGR

Only the gate parameter moves - no signal/structure changes.
"""

import subprocess
import re
import sys
from datetime import date, timedelta

# Configuration
THRESHOLDS = [50, 60, 70, 75, 80, 85, 90, 95, 100]
SYMBOLS = "SPY QQQ IWM DIA XLF XLE XLK XLI XLY XLP XLU TLT IEF GLD SLV USO EEM"
INPUT_DIR = "logs/backfill/v7/reports"
YEARS = 4

def run_backtest(max_iv_pctl):
    """Run backtest with given IV percentile threshold."""
    cmd = [
        "python3", "scripts/run_backtest.py",
        "--input-dir", INPUT_DIR,
        "--years", str(YEARS),
        "--phase", "phase1",
        "--symbols", *SYMBOLS.split(),
        "--edge-slice", "flat",
    ]
    
    if max_iv_pctl < 100:
        cmd.extend(["--max-atm-iv-pctl", str(max_iv_pctl)])
    
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
    return result.stdout + result.stderr

def parse_results(output):
    """Extract key metrics from backtest output."""
    metrics = {
        'trades': 0,
        'wr': 0.0,
        'pf': 0.0,
        'pnl': 0.0,
        'max_dd': 0.0,
        'cagr': 0.0,
    }
    
    # Parse trades
    match = re.search(r'TRADES: (\d+) total', output)
    if match:
        metrics['trades'] = int(match.group(1))
    
    # Parse WR
    match = re.search(r'WIN RATE: ([\d.]+)%', output)
    if match:
        metrics['wr'] = float(match.group(1))
    
    # Parse PF
    match = re.search(r'PROFIT FACTOR: ([\d.]+|inf)', output)
    if match:
        pf = match.group(1)
        metrics['pf'] = 999.0 if pf == 'inf' else float(pf)
    
    # Parse PnL
    match = re.search(r'TOTAL PnL: \$([\d.-]+)', output)
    if match:
        metrics['pnl'] = float(match.group(1))
    
    # Parse Max DD
    match = re.search(r'MAX DRAWDOWN: \$([\d.]+)', output)
    if match:
        metrics['max_dd'] = float(match.group(1))
    
    # Parse CAGR
    match = re.search(r'CAGR \(on \$10k\): ([\d.]+)%', output)
    if match:
        metrics['cagr'] = float(match.group(1))
    
    return metrics

def main():
    print("=" * 80)
    print("FLAT v1.1 IV Gate Sweep")
    print("=" * 80)
    print()
    print("Testing ATM IV percentile thresholds: reject signals where IVp > threshold")
    print()
    
    results = []
    
    for threshold in THRESHOLDS:
        label = f"IVp≤{threshold}" if threshold < 100 else "No filter"
        print(f"Running {label}...", end=" ", flush=True)
        
        output = run_backtest(threshold)
        metrics = parse_results(output)
        metrics['threshold'] = threshold
        results.append(metrics)
        
        print(f"Done ({metrics['trades']} trades, PF {metrics['pf']:.2f})")
    
    # Print results table
    print()
    print("=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    print()
    print(f"{'Threshold':<12} {'Trades':>7} {'WR':>7} {'PF':>7} {'PnL':>10} {'MaxDD':>10} {'CAGR':>7}")
    print("-" * 80)
    
    for r in results:
        threshold_str = f"IVp≤{r['threshold']}" if r['threshold'] < 100 else "No filter"
        pf_str = "∞" if r['pf'] >= 999 else f"{r['pf']:.2f}"
        print(f"{threshold_str:<12} {r['trades']:>7} {r['wr']:>6.1f}% {pf_str:>7} ${r['pnl']:>9.0f} ${r['max_dd']:>9.0f} {r['cagr']:>6.1f}%")
    
    # Find best by PF (excluding < 20 trades)
    valid_results = [r for r in results if r['trades'] >= 10]
    if valid_results:
        best_pf = max(valid_results, key=lambda x: x['pf'])
        best_wr = max(valid_results, key=lambda x: x['wr'])
        print()
        print(f"Best PF (min 10 trades): IVp≤{best_pf['threshold']} (PF {best_pf['pf']:.2f}, {best_pf['trades']} trades)")
        print(f"Best WR (min 10 trades): IVp≤{best_wr['threshold']} ({best_wr['wr']:.1f}% WR, {best_wr['trades']} trades)")

if __name__ == "__main__":
    main()
