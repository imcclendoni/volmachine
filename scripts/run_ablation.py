#!/usr/bin/env python3
"""
Backtest Ablation Report.

Runs multiple backtest configurations to isolate what's driving results:
- A) All symbols, all structures
- B) Exclude TLT
- C) Credit spreads only (no debit)
- D) Exclude TLT + Credit only (recommended)

Prints comparison table for quick decision making.
"""

import argparse
import sys
import json
import yaml
import copy
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.deterministic import DeterministicBacktester


def run_single_backtest(
    start_date: date,
    end_date: date,
    config_overrides: Dict[str, Any],
    base_config_path: str = './config/backtest.yaml',
) -> Dict[str, Any]:
    """Run a single backtest with config overrides."""
    
    # Load base config
    with open(base_config_path) as f:
        config = yaml.safe_load(f)
    
    # Apply overrides
    if 'strategies' not in config:
        config['strategies'] = {}
    if 'skew_extreme' not in config['strategies']:
        config['strategies']['skew_extreme'] = {}
    
    for key, value in config_overrides.items():
        if '.' in key:
            parts = key.split('.')
            target = config
            for part in parts[:-1]:
                if part not in target:
                    target[part] = {}
                target = target[part]
            target[parts[-1]] = value
        else:
            config['strategies']['skew_extreme'][key] = value
    
    # Write temp config
    temp_config_path = Path('./config/backtest_ablation_temp.yaml')
    with open(temp_config_path, 'w') as f:
        yaml.dump(config, f)
    
    # Run backtest
    backtester = DeterministicBacktester(config_path=str(temp_config_path))
    result = backtester.run_range(start_date, end_date, symbols=['SPY', 'QQQ', 'IWM', 'TLT'])
    
    # Clean up
    temp_config_path.unlink()
    
    return {
        'total_trades': result.metrics.total_trades,
        'winners': result.metrics.winners,
        'losers': result.metrics.losers,
        'win_rate': result.metrics.win_rate,
        'total_pnl': result.metrics.total_pnl,
        'profit_factor': result.metrics.profit_factor,
        'max_drawdown': result.metrics.max_drawdown,
        'avg_pnl': result.metrics.avg_pnl,
        'by_structure': result.metrics.by_structure,
        'by_symbol': result.metrics.by_symbol,
    }


def print_ablation_table(results: Dict[str, Dict[str, Any]]):
    """Print formatted ablation comparison table."""
    print()
    print("=" * 80)
    print("ABLATION REPORT - Backtest Configuration Comparison")
    print("=" * 80)
    print()
    
    # Header
    print(f"{'Config':<30} {'Trades':>8} {'Win%':>8} {'PnL':>12} {'PF':>8}")
    print("-" * 80)
    
    for name, data in results.items():
        trades = data['total_trades']
        win_rate = data['win_rate']
        pnl = data['total_pnl']
        pf = data['profit_factor']
        
        # Highlight positive PnL
        pnl_str = f"${pnl:>+10.2f}"
        if pnl > 0:
            pnl_str = f"✅ {pnl_str}"
        else:
            pnl_str = f"   {pnl_str}"
        
        pf_str = f"{pf:>7.2f}"
        if pf >= 1.0:
            pf_str = f"✅{pf_str}"
        else:
            pf_str = f"  {pf_str}"
        
        print(f"{name:<30} {trades:>8} {win_rate:>7.1f}% {pnl_str} {pf_str}")
    
    print("-" * 80)
    print()
    
    # Find best config
    best = max(results.items(), key=lambda x: x[1]['profit_factor'] if x[1]['total_trades'] > 0 else 0)
    print(f"📊 BEST CONFIG: {best[0]}")
    print(f"   PF={best[1]['profit_factor']:.2f}, PnL=${best[1]['total_pnl']:.2f}, {best[1]['total_trades']} trades")
    print()
    
    # Recommendations
    print("RECOMMENDATIONS:")
    for name, data in results.items():
        if data['profit_factor'] >= 1.0 and data['total_trades'] >= 10:
            print(f"  ✅ {name}: Profitable (PF={data['profit_factor']:.2f})")
        elif data['profit_factor'] >= 0.8 and data['total_trades'] >= 10:
            print(f"  ⚠️ {name}: Near breakeven (PF={data['profit_factor']:.2f})")
        elif data['total_trades'] >= 5:
            print(f"  ❌ {name}: Unprofitable (PF={data['profit_factor']:.2f})")
    
    print()
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Run ablation tests to find optimal backtest configuration"
    )
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    
    args = parser.parse_args()
    
    end_date = date.today()
    if args.end:
        end_date = date.fromisoformat(args.end)
    
    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        start_date = end_date - timedelta(days=args.days)
    
    print(f"Running ablation tests: {start_date} to {end_date}")
    print()
    
    # Define test configurations
    configs = {
        'A) All symbols + All structures': {
            'enabled_symbols': ['SPY', 'QQQ', 'IWM', 'TLT'],
            'disabled_symbols': [],
            'enable_credit_spread': True,
            'enable_debit_spread': True,
        },
        'B) Exclude TLT': {
            'enabled_symbols': ['SPY', 'QQQ', 'IWM'],
            'disabled_symbols': ['TLT'],
            'enable_credit_spread': True,
            'enable_debit_spread': True,
        },
        'C) Credit spreads only': {
            'enabled_symbols': ['SPY', 'QQQ', 'IWM', 'TLT'],
            'disabled_symbols': [],
            'enable_credit_spread': True,
            'enable_debit_spread': False,
        },
        'D) No TLT + Credit only': {
            'enabled_symbols': ['SPY', 'QQQ', 'IWM'],
            'disabled_symbols': ['TLT'],
            'enable_credit_spread': True,
            'enable_debit_spread': False,
        },
    }
    
    results = {}
    
    for name, overrides in configs.items():
        print(f"Testing: {name}...")
        try:
            result = run_single_backtest(start_date, end_date, overrides)
            results[name] = result
            print(f"  -> {result['total_trades']} trades, PF={result['profit_factor']:.2f}, PnL=${result['total_pnl']:.2f}")
        except Exception as e:
            print(f"  -> ERROR: {e}")
            results[name] = {
                'total_trades': 0, 'winners': 0, 'losers': 0,
                'win_rate': 0, 'total_pnl': 0, 'profit_factor': 0,
                'max_drawdown': 0, 'avg_pnl': 0, 'by_structure': {}, 'by_symbol': {},
            }
    
    # Print comparison
    print_ablation_table(results)
    
    # Save results
    output_path = Path('./logs/backtest/ablation_report.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump({
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'results': results,
        }, f, indent=2)
    
    print(f"Results saved: {output_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
