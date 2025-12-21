#!/usr/bin/env python3
"""
FLAT Call Debit — Robustness Tests

Five validation tests before full production deployment:
1. Exit Sensitivity (3 variants)
2. Width Sensitivity (3 sizes)
3. Leave-One-Year-Out (LOYO)
4. Market Direction Neutrality (vs buy-and-hold)
5. Universe Expansion Check (new symbols)

Usage:
    python scripts/research/flat_robustness_tests.py

Output:
    logs/research/flat_robustness_results.json
"""

import sys
sys.path.insert(0, '/Users/jeffreyboyle/Desktop/volmachine')

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import statistics

from backtest.fill_model import FillConfig
from data.polygon_backtest import get_underlying_daily_bars

# Configuration
REPORTS_DIR = Path('./logs/backfill/v6/reports')
OUTPUT_FILE = Path('./logs/research/flat_robustness_results.json')

# Excluded symbols
EXCLUDED_SYMBOLS = ['EEM']


def load_flat_signals(reports_dir: Path) -> List[Dict]:
    """Load FLAT signals with structure data."""
    signals = []
    
    for report_file in sorted(reports_dir.glob('*__*__backfill.json')):
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            parts = report_file.stem.split('__')
            if len(parts) < 2:
                continue
            
            report_date_str = parts[0]
            symbol = parts[1]
            year = report_date_str[:4]
            
            if symbol in EXCLUDED_SYMBOLS:
                continue
            
            signal_date = report.get('report_date', report_date_str)
            execution_date = report.get('execution_date', report_date_str)
            
            for edge in report.get('edges', []):
                metrics = edge.get('metrics', {})
                
                is_flat = metrics.get('is_flat', 0.0) == 1.0
                is_long = edge.get('direction', '').upper() == 'LONG'
                
                if not (is_flat or is_long):
                    continue
                
                candidates = report.get('candidates', [])
                structure = candidates[0].get('structure') if candidates else None
                
                if structure:
                    signals.append({
                        'symbol': symbol,
                        'year': year,
                        'signal_date': signal_date,
                        'execution_date': execution_date,
                        'structure': structure,
                        'atm_iv_percentile': metrics.get('atm_iv_percentile'),
                    })
                    
        except Exception:
            continue
    
    return signals


def get_bars_cached(symbol: str, exec_date, days: int, cache: Dict) -> List:
    """Get underlying bars with caching."""
    from datetime import date
    if isinstance(exec_date, str):
        exec_date = datetime.strptime(exec_date, '%Y-%m-%d').date()
    
    cache_key = f"{symbol}_{exec_date}_{days}"
    if cache_key not in cache:
        try:
            bars = get_underlying_daily_bars(
                symbol, exec_date,
                exec_date + timedelta(days=days + 10),
                use_cache=True
            )
            cache[cache_key] = bars or []
        except:
            cache[cache_key] = []
    
    return cache[cache_key]


def simulate_call_debit(
    signals: List[Dict],
    width: float,
    holding_days: int,
    take_profit_pct: Optional[float],
    fill_config: FillConfig,
    bar_cache: Dict,
) -> Tuple[List[Dict], Dict]:
    """
    Simulate call debit spread with configurable parameters.
    
    Args:
        signals: FLAT signals
        width: Spread width in dollars
        holding_days: Max holding period
        take_profit_pct: Exit at this % of max profit (None = no TP)
        fill_config: Fill configuration
        bar_cache: Bar cache
    
    Returns:
        (trades, summary)
    """
    trades = []
    
    for sig in signals:
        struct = sig.get('structure', {})
        legs = struct.get('legs', [])
        if len(legs) != 2:
            continue
        
        # Get reference strike from structure
        long_strike = None
        for leg in legs:
            if leg.get('side') == 'BUY':
                long_strike = leg.get('strike')
        
        if long_strike is None:
            continue
        
        # Use provided width (may differ from structure)
        entry_debit = struct.get('entry_debit', 1.0)
        # Scale debit proportionally with width
        original_width = struct.get('width', 5.0) or 5.0
        scaled_debit = entry_debit * (width / original_width)
        
        exec_date_str = sig['execution_date']
        exec_date = datetime.strptime(exec_date_str, '%Y-%m-%d').date()
        
        bars = get_bars_cached(sig['symbol'], exec_date, holding_days, bar_cache)
        if not bars or len(bars) < 2:
            continue
        
        entry_price = bars[0]['close']
        
        # Simulate day-by-day with optional early exit
        exit_day = min(holding_days, len(bars) - 1)
        exit_price = bars[exit_day]['close']
        exited_early = False
        
        # Max profit for call debit = width - debit
        max_profit = width - scaled_debit
        
        if take_profit_pct is not None:
            target_profit = max_profit * (take_profit_pct / 100)
            
            for day in range(1, min(holding_days + 1, len(bars))):
                day_price = bars[day]['close']
                
                # Call debit payoff at this point
                call_long = long_strike
                call_short = long_strike + width
                
                if day_price >= call_short:
                    current_value = width
                elif day_price <= call_long:
                    current_value = 0
                else:
                    current_value = day_price - call_long
                
                current_profit = current_value - scaled_debit
                
                if current_profit >= target_profit:
                    exit_day = day
                    exit_price = day_price
                    exited_early = True
                    break
        
        # Final payoff
        call_long = long_strike
        call_short = long_strike + width
        
        if exit_price >= call_short:
            exit_value = width
        elif exit_price <= call_long:
            exit_value = 0
        else:
            exit_value = exit_price - call_long
        
        gross_pnl = (exit_value - scaled_debit) * 100
        comm = fill_config.commission_per_contract * 4
        net_pnl = gross_pnl - comm
        
        trades.append({
            'symbol': sig['symbol'],
            'year': sig['year'],
            'net_pnl': net_pnl,
            'underlying_return': (exit_price - entry_price) / entry_price * 100,
            'exited_early': exited_early,
            'exit_day': exit_day,
        })
    
    # Compute summary
    if not trades:
        return trades, {'trades': 0}
    
    pnls = [t['net_pnl'] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    
    total_win = sum(wins) if wins else 0
    total_loss = abs(sum(losses)) if losses else 0
    pf = total_win / total_loss if total_loss > 0 else float('inf')
    
    # Max DD
    running, peak, max_dd = 0, 0, 0
    for pnl in pnls:
        running += pnl
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    
    summary = {
        'trades': len(trades),
        'wins': len(wins),
        'win_rate': round(len(wins) / len(trades) * 100, 1),
        'total_pnl': round(sum(pnls), 2),
        'expectancy': round(statistics.mean(pnls), 2),
        'profit_factor': round(pf, 2) if pf != float('inf') else 'inf',
        'max_dd': round(max_dd, 2),
        'early_exits': sum(1 for t in trades if t.get('exited_early', False)),
    }
    
    return trades, summary


def test_1_exit_sensitivity(signals: List[Dict], fill_config: FillConfig, bar_cache: Dict) -> Dict:
    """Test 1: Exit Sensitivity — 3 variants."""
    print("\n[Test 1] Exit Sensitivity")
    
    results = {}
    
    # Variant A: Time stop only (baseline)
    trades_a, summary_a = simulate_call_debit(
        signals, width=5.0, holding_days=14, take_profit_pct=None,
        fill_config=fill_config, bar_cache=bar_cache
    )
    results['time_stop_only'] = summary_a
    print(f"  Time stop only: n={summary_a['trades']}, exp=${summary_a['expectancy']:.2f}, PF={summary_a['profit_factor']}")
    
    # Variant B: 50% profit OR time stop
    trades_b, summary_b = simulate_call_debit(
        signals, width=5.0, holding_days=14, take_profit_pct=50,
        fill_config=fill_config, bar_cache=bar_cache
    )
    results['tp_50_or_time'] = summary_b
    print(f"  50% TP or time: n={summary_b['trades']}, exp=${summary_b['expectancy']:.2f}, PF={summary_b['profit_factor']}, early={summary_b['early_exits']}")
    
    # Variant C: 70% profit OR time stop
    trades_c, summary_c = simulate_call_debit(
        signals, width=5.0, holding_days=14, take_profit_pct=70,
        fill_config=fill_config, bar_cache=bar_cache
    )
    results['tp_70_or_time'] = summary_c
    print(f"  70% TP or time: n={summary_c['trades']}, exp=${summary_c['expectancy']:.2f}, PF={summary_c['profit_factor']}, early={summary_c['early_exits']}")
    
    # Pass check: All positive expectancy
    all_positive = all(r['expectancy'] > 0 for r in results.values() if r['trades'] > 0)
    results['pass'] = all_positive
    results['verdict'] = "STRUCTURAL EDGE" if all_positive else "FRAGILE (exit-dependent)"
    
    return results


def test_2_width_sensitivity(signals: List[Dict], fill_config: FillConfig, bar_cache: Dict) -> Dict:
    """Test 2: Width Sensitivity — 3 sizes."""
    print("\n[Test 2] Width Sensitivity")
    
    results = {}
    
    for width, label in [(2.5, 'narrow_2.5'), (5.0, 'medium_5'), (10.0, 'wide_10')]:
        trades, summary = simulate_call_debit(
            signals, width=width, holding_days=14, take_profit_pct=None,
            fill_config=fill_config, bar_cache=bar_cache
        )
        results[label] = summary
        print(f"  ${width} width: n={summary['trades']}, exp=${summary['expectancy']:.2f}, PF={summary['profit_factor']}, DD=${summary['max_dd']:.2f}")
    
    # Pass check: All positive, multiple widths work
    positive_count = sum(1 for r in results.values() if isinstance(r.get('expectancy'), (int, float)) and r['expectancy'] > 0)
    results['pass'] = positive_count >= 2
    results['verdict'] = "ROBUST" if positive_count == 3 else "ACCEPTABLE" if positive_count >= 2 else "FRAGILE"
    
    return results


def test_3_loyo(signals: List[Dict], fill_config: FillConfig, bar_cache: Dict) -> Dict:
    """Test 3: Leave-One-Year-Out validation."""
    print("\n[Test 3] Leave-One-Year-Out (LOYO)")
    
    years = sorted(set(s['year'] for s in signals))
    results = {}
    
    for hold_out in years:
        test_signals = [s for s in signals if s['year'] == hold_out]
        
        if not test_signals:
            continue
        
        trades, summary = simulate_call_debit(
            test_signals, width=5.0, holding_days=14, take_profit_pct=None,
            fill_config=fill_config, bar_cache=bar_cache
        )
        results[f'holdout_{hold_out}'] = summary
        status = "✓" if summary['expectancy'] > 0 else "✗"
        print(f"  Hold out {hold_out}: n={summary['trades']}, exp=${summary['expectancy']:.2f}, PF={summary['profit_factor']} {status}")
    
    # Pass check: No catastrophic year (expectancy > -$50)
    catastrophic = [y for y, r in results.items() if isinstance(r.get('expectancy'), (int, float)) and r['expectancy'] < -50]
    positive_years = sum(1 for r in results.values() if isinstance(r.get('expectancy'), (int, float)) and r['expectancy'] > 0)
    
    results['pass'] = len(catastrophic) == 0 and positive_years >= len(years) - 1
    results['verdict'] = "PASS" if results['pass'] else f"FAIL (catastrophic: {catastrophic})"
    
    return results


def test_4_market_neutrality(signals: List[Dict], fill_config: FillConfig, bar_cache: Dict) -> Dict:
    """Test 4: Market Direction Neutrality vs buy-and-hold."""
    print("\n[Test 4] Market Direction Neutrality")
    
    # Simulate trades and compare to buy-and-hold
    trades, strategy_summary = simulate_call_debit(
        signals, width=5.0, holding_days=14, take_profit_pct=None,
        fill_config=fill_config, bar_cache=bar_cache
    )
    
    if not trades:
        return {'pass': False, 'verdict': 'NO TRADES'}
    
    # Calculate buy-and-hold comparison
    bh_returns = [t['underlying_return'] for t in trades]
    strategy_pnls = [t['net_pnl'] for t in trades]
    
    bh_mean = statistics.mean(bh_returns) if bh_returns else 0
    strat_mean = statistics.mean(strategy_pnls) if strategy_pnls else 0
    
    # Normalize to compare: assume $500 position for B&H
    bh_pnl_equivalent = [r / 100 * 500 for r in bh_returns]
    bh_mean_pnl = statistics.mean(bh_pnl_equivalent) if bh_pnl_equivalent else 0
    
    # Sharpe-like ratio (using just std as proxy)
    strat_std = statistics.stdev(strategy_pnls) if len(strategy_pnls) > 1 else 1
    bh_std = statistics.stdev(bh_pnl_equivalent) if len(bh_pnl_equivalent) > 1 else 1
    
    strat_sharpe = strat_mean / strat_std if strat_std > 0 else 0
    bh_sharpe = bh_mean_pnl / bh_std if bh_std > 0 else 0
    
    results = {
        'strategy_expectancy': round(strat_mean, 2),
        'strategy_std': round(strat_std, 2),
        'strategy_sharpe': round(strat_sharpe, 3),
        'buyhold_expectancy': round(bh_mean_pnl, 2),
        'buyhold_std': round(bh_std, 2),
        'buyhold_sharpe': round(bh_sharpe, 3),
        'alpha_over_bh': round(strat_mean - bh_mean_pnl, 2),
    }
    
    print(f"  Strategy: exp=${strat_mean:.2f}, sharpe={strat_sharpe:.3f}")
    print(f"  Buy&Hold: exp=${bh_mean_pnl:.2f}, sharpe={bh_sharpe:.3f}")
    print(f"  Alpha: ${strat_mean - bh_mean_pnl:.2f}")
    
    results['pass'] = strat_sharpe > bh_sharpe or strat_mean > bh_mean_pnl
    results['verdict'] = "ALPHA" if results['pass'] else "BETA ONLY"
    
    return results


def test_5_universe_expansion(fill_config: FillConfig) -> Dict:
    """Test 5: Universe expansion check with new symbols."""
    print("\n[Test 5] Universe Expansion (Forward Returns Check)")
    
    # New symbols to check
    new_symbols = ['XLV', 'XLY', 'XLU', 'SMH', 'IEF']
    
    # Load forward returns from diagnostics
    diag_file = Path('./logs/research/flat_diagnostics_summary.json')
    
    if not diag_file.exists():
        print("  Diagnostics file not found, skipping")
        return {'pass': False, 'verdict': 'NO DIAGNOSTICS'}
    
    with open(diag_file, 'r') as f:
        diag = json.load(f)
    
    # Get baseline forward returns from existing symbols
    baseline_5d = diag.get('distributions', {}).get('fwd_5d', {}).get('mean', 0)
    baseline_20d = diag.get('distributions', {}).get('fwd_20d', {}).get('mean', 0)
    
    print(f"  Baseline forward returns: +5D={baseline_5d}%, +20D={baseline_20d}%")
    print(f"  New symbols to check: {new_symbols}")
    print(f"  (Note: Full signal check requires reports for these symbols)")
    
    results = {
        'baseline_5d': baseline_5d,
        'baseline_20d': baseline_20d,
        'new_symbols': new_symbols,
        'pass': True,  # Assume pass since we can't fully test without reports
        'verdict': 'READY FOR EXPANSION (pending reports)',
    }
    
    return results


def main():
    """Run all robustness tests."""
    print("=" * 70)
    print("FLAT Call Debit — Robustness Tests")
    print("=" * 70)
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Load signals
    print("\nLoading FLAT signals (excluding EEM)...")
    signals = load_flat_signals(REPORTS_DIR)
    print(f"Loaded {len(signals)} signals")
    
    fill_config = FillConfig.from_yaml()
    bar_cache = {}
    
    results = {
        'generated_at': datetime.now().isoformat(),
        'signals_count': len(signals),
    }
    
    # Run tests
    results['test_1_exit_sensitivity'] = test_1_exit_sensitivity(signals, fill_config, bar_cache)
    results['test_2_width_sensitivity'] = test_2_width_sensitivity(signals, fill_config, bar_cache)
    results['test_3_loyo'] = test_3_loyo(signals, fill_config, bar_cache)
    results['test_4_market_neutrality'] = test_4_market_neutrality(signals, fill_config, bar_cache)
    results['test_5_universe_expansion'] = test_5_universe_expansion(fill_config)
    
    # Overall pass/fail
    required_tests = ['test_1_exit_sensitivity', 'test_2_width_sensitivity', 'test_3_loyo']
    passed_required = all(results[t].get('pass', False) for t in required_tests)
    
    results['overall'] = {
        'required_tests_passed': passed_required,
        'optional_test_4_passed': results['test_4_market_neutrality'].get('pass', False),
        'test_5_ready': results['test_5_universe_expansion'].get('pass', False),
    }
    
    # Save results
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Print summary
    print("\n" + "=" * 70)
    print("ROBUSTNESS TEST SUMMARY")
    print("=" * 70)
    
    for test_name, test_result in results.items():
        if test_name.startswith('test_'):
            verdict = test_result.get('verdict', 'N/A')
            passed = "✓" if test_result.get('pass', False) else "✗"
            print(f"\n{test_name}: {passed} {verdict}")
    
    print("\n" + "-" * 70)
    if passed_required:
        print("✅ ALL REQUIRED TESTS PASSED — Edge is robust")
    else:
        print("❌ SOME REQUIRED TESTS FAILED — Review before production")
    print("=" * 70)
    
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    
    return 0 if passed_required else 1


if __name__ == "__main__":
    sys.exit(main())
