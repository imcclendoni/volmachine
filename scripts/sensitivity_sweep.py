#!/usr/bin/env python3
"""
STEEP Sensitivity Sweep

Runs STEEP-only backtest across multiple IV percentile thresholds.
Prevents overfitting by showing stability across thresholds.
"""

import subprocess
import re
import sys
from pathlib import Path

THRESHOLDS = [70, 75, 80, 85, 90, 100]  # 100 = no filter (baseline)

def run_backtest(pctl_threshold):
    """Run STEEP-only backtest with given IV percentile threshold."""
    cmd = [
        'python3', 'scripts/run_backtest.py',
        '--input-dir', 'logs/backfill/v6/reports',
        '--years', '4',
        '--phase', 'phase1',
        '--symbols', 'SPY', 'QQQ', 'IWM', 'XLF', 'GLD', 'TLT', 'DIA',
        '--edge-slice', 'steep',
    ]
    
    if pctl_threshold < 100:
        cmd.extend(['--max-atm-iv-pctl', str(pctl_threshold)])
    
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
    return result.stdout + result.stderr


def parse_results(output):
    """Extract key metrics from backtest output."""
    trades_match = re.search(r'TRADES: (\d+) total \((\d+)W / (\d+)L\)', output)
    wr_match = re.search(r'WIN RATE: ([\d.]+)%', output)
    pf_match = re.search(r'PROFIT FACTOR: ([\d.]+)', output)
    pnl_match = re.search(r'TOTAL PnL: \$([-\d.]+)', output)
    dd_match = re.search(r'MAX DRAWDOWN: \$([\d.]+)', output)
    
    if trades_match:
        return {
            'trades': int(trades_match.group(1)),
            'wins': int(trades_match.group(2)),
            'losses': int(trades_match.group(3)),
            'wr': float(wr_match.group(1)) if wr_match else 0,
            'pf': float(pf_match.group(1)) if pf_match else 0,
            'pnl': float(pnl_match.group(1)) if pnl_match else 0,
            'max_dd': float(dd_match.group(1)) if dd_match else 0,
        }
    return None


def main():
    print("=" * 70)
    print("STEEP SENSITIVITY SWEEP - IV Percentile Threshold")
    print("=" * 70)
    print("\nRunning STEEP-only Phase 1 across different IV percentile thresholds...")
    print("Dataset: v6, 4 years, 7 symbols\n")
    
    results = []
    
    for threshold in THRESHOLDS:
        label = f"pctl <= {threshold}" if threshold < 100 else "No filter"
        print(f"Running {label}...", end=" ", flush=True)
        
        output = run_backtest(threshold)
        metrics = parse_results(output)
        
        if metrics:
            results.append({'threshold': threshold, **metrics})
            print(f"✓ {metrics['trades']} trades, PF {metrics['pf']:.2f}, PnL ${metrics['pnl']:.0f}")
        else:
            print("✗ Failed to parse")
    
    print("\n" + "=" * 70)
    print("SENSITIVITY SWEEP RESULTS")
    print("=" * 70)
    print(f"\n{'Threshold':<12} {'Trades':<8} {'WR':<8} {'PF':<8} {'PnL':<12} {'MaxDD':<10} {'Exp/Trade':<10}")
    print("-" * 70)
    
    for r in results:
        label = f"<= {r['threshold']}th" if r['threshold'] < 100 else "baseline"
        exp_per_trade = r['pnl'] / r['trades'] if r['trades'] > 0 else 0
        print(f"{label:<12} {r['trades']:<8} {r['wr']:<7.1f}% {r['pf']:<8.2f} ${r['pnl']:<11.2f} ${r['max_dd']:<9.2f} ${exp_per_trade:<9.2f}")
    
    print("-" * 70)
    
    # Stability check
    stable_thresholds = [r for r in results if r['pf'] > 1.0 and r['pnl'] > 0]
    print(f"\n✓ {len(stable_thresholds)}/{len(results)} thresholds have PF > 1.0 and positive PnL")
    
    if stable_thresholds:
        best = max(stable_thresholds, key=lambda x: x['pf'])
        print(f"✓ Best threshold: <= {best['threshold']}th percentile (PF={best['pf']:.2f}, {best['trades']} trades)")
    else:
        print("✗ No stable threshold found - edge may not be robust")


if __name__ == '__main__':
    main()
