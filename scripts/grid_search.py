#!/usr/bin/env python3
"""
Grid search for parameter sensitivity analysis.
Tests combinations of SL, TP, and credit/width thresholds.
"""

import argparse
import itertools
import json
import subprocess
import sys
from pathlib import Path
from datetime import date, timedelta
import yaml


def run_backtest_with_params(sl_mult: float, tp_pct: int, credit_min: float, years: int):
    """Run backtest with specific parameters and return metrics."""
    
    # Temporarily modify config
    config_path = Path('./config/backtest.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Backup original values
    orig_sl = config['exit_rules']['credit_spread'].get('stop_loss_mult', 1.25)
    orig_tp = config['exit_rules']['credit_spread'].get('take_profit_pct', 50)
    
    # Update values
    config['exit_rules']['credit_spread']['stop_loss_mult'] = sl_mult
    config['exit_rules']['credit_spread']['take_profit_pct'] = tp_pct
    
    # Save temp config
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    try:
        # Run backtest
        result = subprocess.run(
            ['python3', 'scripts/run_backtest.py', 
             '--input-dir', 'logs/backfill/v4/reports',
             '--years', str(years),
             '--output', 'logs/backtest/grid'],
            capture_output=True,
            text=True,
            cwd='.'
        )
        
        output = result.stdout + result.stderr
        
        # Parse results
        metrics = {
            'sl_mult': sl_mult,
            'tp_pct': tp_pct,
            'credit_min': credit_min,
            'pf': 0.0,
            'cagr': 0.0,
            'max_dd_pct': 0.0,
            'trades_per_year': 0.0,
            'total_pnl': 0.0,
            'win_rate': 0.0
        }
        
        for line in output.split('\n'):
            if 'PROFIT FACTOR:' in line:
                try:
                    metrics['pf'] = float(line.split(':')[1].strip())
                except:
                    pass
            if 'CAGR (on $10k):' in line:
                try:
                    metrics['cagr'] = float(line.split(':')[1].strip().replace('%', ''))
                except:
                    pass
            if 'Max DD %:' in line:
                try:
                    metrics['max_dd_pct'] = float(line.split(':')[1].strip().replace('%', ''))
                except:
                    pass
            if 'Trades per year:' in line:
                try:
                    metrics['trades_per_year'] = float(line.split(':')[1].strip())
                except:
                    pass
            if 'TOTAL PnL:' in line:
                try:
                    val = line.split('$')[1].split()[0]
                    metrics['total_pnl'] = float(val)
                except:
                    pass
            if 'WIN RATE:' in line:
                try:
                    metrics['win_rate'] = float(line.split(':')[1].strip().replace('%', ''))
                except:
                    pass
        
        return metrics
        
    finally:
        # Restore original values
        config['exit_rules']['credit_spread']['stop_loss_mult'] = orig_sl
        config['exit_rules']['credit_spread']['take_profit_pct'] = orig_tp
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description="Parameter sensitivity grid search")
    parser.add_argument('--years', type=int, default=1, help="Backtest period in years")
    parser.add_argument('--output', type=str, default='logs/grid_results.csv')
    args = parser.parse_args()
    
    # Parameter grid
    sl_values = [1.0, 1.25, 1.5]
    tp_values = [35, 50, 65]
    credit_values = [0.15, 0.20, 0.25]  # Not implemented in grid yet
    
    results = []
    total = len(sl_values) * len(tp_values)
    
    print(f"Running grid search: {total} combinations")
    print("=" * 60)
    
    for i, (sl, tp) in enumerate(itertools.product(sl_values, tp_values)):
        print(f"\n[{i+1}/{total}] SL={sl}x, TP={tp}%...")
        
        metrics = run_backtest_with_params(sl, tp, 0.20, args.years)
        results.append(metrics)
        
        print(f"   PF={metrics['pf']:.2f}, CAGR={metrics['cagr']:.1f}%, "
              f"DD={metrics['max_dd_pct']:.1f}%, Trades/yr={metrics['trades_per_year']:.0f}")
    
    # Save results
    print("\n" + "=" * 60)
    print("GRID SEARCH RESULTS")
    print("=" * 60)
    
    # Sort by PF
    results.sort(key=lambda x: x['pf'], reverse=True)
    
    print(f"\n{'SL':>6} {'TP':>6} {'PF':>8} {'CAGR':>8} {'DD%':>8} {'Trades/Y':>10} {'PnL':>10}")
    print("-" * 60)
    for r in results:
        print(f"{r['sl_mult']:>6.2f} {r['tp_pct']:>6}% {r['pf']:>8.2f} {r['cagr']:>7.1f}% "
              f"{r['max_dd_pct']:>7.1f}% {r['trades_per_year']:>10.1f} ${r['total_pnl']:>9.2f}")
    
    # Save CSV
    import csv
    with open(args.output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults saved: {args.output}")
    
    # Best parameters
    best = results[0]
    print(f"\n✅ BEST: SL={best['sl_mult']}x, TP={best['tp_pct']}%")
    print(f"   PF={best['pf']:.2f}, CAGR={best['cagr']:.1f}%, DD={best['max_dd_pct']:.1f}%")


if __name__ == '__main__':
    main()
