#!/usr/bin/env python3
"""
FLAT Structure Ablation (Agent #2 Research) — AUDIT-GRADE

Simulates candidate structures for FLAT signals with full coverage tracking.

Structures tested:
1. Debit put spread (original mapping)
2. Call debit spread (bullish continuation)
3. Put credit spread (bullish; monetize muted fear)
4. Call calendar (vol expansion thesis) — SIMPLIFIED

Coverage metrics:
- signals_total, signals_simulated, signals_skipped
- Skip reasons: missing_underlying, missing_structure, missing_strikes, etc.
- Stratification by year, symbol, IV bucket

Usage:
    python scripts/research/flat_structure_ablation.py

Output:
    logs/research/flat_ablation_results.json
"""

import sys
sys.path.insert(0, '/Users/jeffreyboyle/Desktop/volmachine')

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Tuple
import statistics

from backtest.fill_model import FillConfig
from data.polygon_backtest import get_underlying_daily_bars

# Configuration
REPORTS_DIR = Path('./logs/backfill/v6/reports')
OUTPUT_FILE = Path('./logs/research/flat_ablation_results.json')

# Simulation parameters
HOLDING_PERIOD_DAYS = 14
SPREAD_WIDTH = 5  # $5 width for synthetic structures


class CoverageTracker:
    """Track simulation coverage and skip reasons."""
    
    def __init__(self):
        self.total = 0
        self.simulated = 0
        self.skipped = defaultdict(list)  # reason -> [(symbol, date), ...]
    
    def record_skip(self, reason: str, symbol: str, date: str):
        self.skipped[reason].append({'symbol': symbol, 'date': date})
    
    def record_success(self):
        self.simulated += 1
    
    def set_total(self, n: int):
        self.total = n
    
    def summary(self) -> Dict:
        skip_counts = {reason: len(items) for reason, items in self.skipped.items()}
        return {
            'signals_total': self.total,
            'signals_simulated': self.simulated,
            'signals_skipped': self.total - self.simulated,
            'skip_reasons': skip_counts,
            'skip_details': dict(self.skipped),
        }


def load_flat_signals(reports_dir: Path) -> Tuple[List[Dict], Dict]:
    """
    Load ALL FLAT signals with metadata about missing data.
    
    Returns:
        (signals, load_stats)
    """
    signals = []
    load_stats = {
        'files_scanned': 0,
        'flat_edges_found': 0,
        'signals_with_structure': 0,
        'signals_without_structure': 0,
    }
    
    for report_file in sorted(reports_dir.glob('*__*__backfill.json')):
        load_stats['files_scanned'] += 1
        
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            parts = report_file.stem.split('__')
            if len(parts) < 2:
                continue
            
            report_date_str = parts[0]
            symbol = parts[1]
            year = report_date_str[:4]
            
            signal_date = report.get('report_date', report_date_str)
            execution_date = report.get('execution_date', report_date_str)
            
            for edge in report.get('edges', []):
                metrics = edge.get('metrics', {})
                
                is_flat = metrics.get('is_flat', 0.0) == 1.0
                is_long = edge.get('direction', '').upper() == 'LONG'
                
                if not (is_flat or is_long):
                    continue
                
                load_stats['flat_edges_found'] += 1
                
                # Get structure if available
                candidates = report.get('candidates', [])
                structure = candidates[0].get('structure') if candidates else None
                
                if structure:
                    load_stats['signals_with_structure'] += 1
                else:
                    load_stats['signals_without_structure'] += 1
                
                signals.append({
                    'symbol': symbol,
                    'year': year,
                    'signal_date': signal_date,
                    'execution_date': execution_date,
                    'edge': edge,
                    'structure': structure,
                    'atm_iv_percentile': metrics.get('atm_iv_percentile'),
                    'has_structure': structure is not None,
                })
                
        except Exception as e:
            print(f"Error loading {report_file}: {e}")
            continue
    
    return signals, load_stats


def get_underlying_bars_cached(symbol: str, exec_date, cache: Dict) -> List:
    """Get underlying bars with caching to reduce API calls."""
    cache_key = f"{symbol}_{exec_date}"
    if cache_key not in cache:
        from datetime import date
        if isinstance(exec_date, str):
            exec_date = datetime.strptime(exec_date, '%Y-%m-%d').date()
        
        try:
            bars = get_underlying_daily_bars(
                symbol,
                exec_date,
                exec_date + timedelta(days=HOLDING_PERIOD_DAYS + 10),
                use_cache=True
            )
            cache[cache_key] = bars or []
        except Exception:
            cache[cache_key] = []
    
    return cache[cache_key]


def simulate_structure(
    signals: List[Dict],
    structure_name: str,
    payoff_func,
    fill_config: FillConfig,
    bar_cache: Dict,
) -> Tuple[List[Dict], CoverageTracker]:
    """
    Generic structure simulation with full coverage tracking.
    
    Args:
        signals: List of FLAT signals
        structure_name: Name of structure
        payoff_func: Function(sig, entry_price, exit_price, width) -> (exit_value, entry_cost)
        fill_config: Fill configuration
        bar_cache: Cache for underlying bars
    
    Returns:
        (trades, coverage_tracker)
    """
    tracker = CoverageTracker()
    tracker.set_total(len(signals))
    trades = []
    
    for sig in signals:
        symbol = sig['symbol']
        exec_date_str = sig['execution_date']
        
        # Check for structure data
        structure = sig.get('structure')
        if not structure:
            tracker.record_skip('missing_structure', symbol, exec_date_str)
            continue
        
        # Get strikes from structure
        legs = structure.get('legs', [])
        if len(legs) != 2:
            tracker.record_skip('invalid_leg_count', symbol, exec_date_str)
            continue
        
        long_strike = None
        short_strike = None
        for leg in legs:
            if leg.get('side') == 'BUY':
                long_strike = leg.get('strike')
            else:
                short_strike = leg.get('strike')
        
        if long_strike is None or short_strike is None:
            tracker.record_skip('missing_strikes', symbol, exec_date_str)
            continue
        
        width = abs(long_strike - short_strike)
        entry_debit = structure.get('entry_debit', 1.0)
        
        # Get underlying bars
        exec_date = datetime.strptime(exec_date_str, '%Y-%m-%d').date()
        bars = get_underlying_bars_cached(symbol, exec_date, bar_cache)
        
        if not bars:
            tracker.record_skip('missing_underlying_bars', symbol, exec_date_str)
            continue
        
        if len(bars) < HOLDING_PERIOD_DAYS:
            tracker.record_skip('insufficient_bars', symbol, exec_date_str)
            continue
        
        entry_price = bars[0]['close']
        exit_bar = bars[min(HOLDING_PERIOD_DAYS, len(bars) - 1)]
        exit_price = exit_bar['close']
        
        # Calculate payoff using structure-specific function
        try:
            exit_value, entry_cost = payoff_func(
                sig, entry_price, exit_price, width, long_strike, short_strike, entry_debit
            )
        except Exception as e:
            tracker.record_skip('payoff_error', symbol, exec_date_str)
            continue
        
        # Calculate PnL
        gross_pnl = (exit_value - entry_cost) * 100
        total_comm = fill_config.commission_per_contract * 4  # 2 legs x entry/exit
        net_pnl = gross_pnl - total_comm
        
        trades.append({
            'symbol': symbol,
            'year': sig['year'],
            'signal_date': sig['signal_date'],
            'execution_date': exec_date_str,
            'structure_type': structure_name,
            'entry_cost': entry_cost,
            'exit_value': exit_value,
            'gross_pnl': gross_pnl,
            'commissions': total_comm,
            'net_pnl': net_pnl,
            'underlying_return_pct': round((exit_price - entry_price) / entry_price * 100, 3),
            'atm_iv_percentile': sig.get('atm_iv_percentile'),
        })
        tracker.record_success()
    
    return trades, tracker


# ============================================================================
# Payoff Functions for Each Structure
# ============================================================================

def payoff_debit_put_spread(sig, entry_price, exit_price, width, long_strike, short_strike, entry_debit):
    """Debit put spread: profit if underlying drops."""
    if exit_price <= short_strike:
        exit_value = width
    elif exit_price >= long_strike:
        exit_value = 0
    else:
        exit_value = (long_strike - exit_price)
    return exit_value, entry_debit


def payoff_call_debit_spread(sig, entry_price, exit_price, width, long_strike, short_strike, entry_debit):
    """Call debit spread: profit if underlying rises."""
    call_long_strike = long_strike
    call_short_strike = long_strike + width
    
    if exit_price >= call_short_strike:
        exit_value = width
    elif exit_price <= call_long_strike:
        exit_value = 0
    else:
        exit_value = (exit_price - call_long_strike)
    return exit_value, entry_debit


def payoff_put_credit_spread(sig, entry_price, exit_price, width, long_strike, short_strike, entry_debit):
    """
    Put credit spread (bullish): sell put spread below market.
    
    Thesis: FLAT = muted fear, monetize the lack of premium.
    Entry: receive credit
    Max profit if underlying stays above short strike
    """
    # Use same strikes as the debit put spread structure
    # Short the higher strike put, long the lower strike put
    # Credit spread: short_strike > long_strike for puts
    put_short_strike = long_strike  # Closer to ATM
    put_long_strike = short_strike   # Further OTM
    
    # Approximate credit received (inverse of debit)
    entry_credit = entry_debit * 0.8  # Slightly less due to wider spreads
    
    if exit_price >= put_short_strike:
        # Full profit: keep the credit
        exit_cost = 0
    elif exit_price <= put_long_strike:
        # Max loss: width - credit
        exit_cost = width
    else:
        # Partial loss
        exit_cost = (put_short_strike - exit_price)
    
    # For credit spread: entry_cost is negative (receive premium)
    # exit_value is what we have to pay back
    # PnL = credit received - exit cost
    return entry_credit - exit_cost, 0  # Return as (net_value, cost=0)


def payoff_call_calendar(sig, entry_price, exit_price, width, long_strike, short_strike, entry_debit):
    """
    Call calendar spread (simplified): long back-month, short front-month at same strike.
    
    Thesis: FLAT skew = vol too cheap, position for vol expansion + time decay.
    
    Simplified model: profit from ATM straddle behavior without full term structure.
    """
    # Use ATM strike
    atm_strike = long_strike  # Approximate ATM from structure
    
    # Calendar profit peaks when underlying stays near strike
    # Model as inverted V around ATM
    distance_from_strike = abs(exit_price - atm_strike)
    max_calendar_value = width * 0.6  # Calendars typically have lower max profit
    
    # Entry cost for calendar is typically the debit to establish
    entry_cost = entry_debit * 0.5  # Calendars usually cheaper than verticals
    
    # Profit peaks at strike, decays as underlying moves away
    if distance_from_strike < width:
        exit_value = max_calendar_value * (1 - distance_from_strike / width)
    else:
        exit_value = 0
    
    return exit_value, entry_cost


# ============================================================================
# Statistics Functions
# ============================================================================

def compute_stratified_stats(trades: List[Dict]) -> Dict:
    """Compute stats stratified by year, symbol, and IV bucket."""
    
    # By Year
    by_year = defaultdict(list)
    for t in trades:
        by_year[t['year']].append(t['net_pnl'])
    
    year_stats = {}
    for year, pnls in sorted(by_year.items()):
        wins = [p for p in pnls if p > 0]
        year_stats[year] = {
            'trades': len(pnls),
            'win_rate': round(len(wins) / len(pnls) * 100, 1) if pnls else 0,
            'total_pnl': round(sum(pnls), 2),
            'expectancy': round(statistics.mean(pnls), 2) if pnls else 0,
        }
    
    # By Symbol
    by_symbol = defaultdict(list)
    for t in trades:
        by_symbol[t['symbol']].append(t['net_pnl'])
    
    symbol_stats = {}
    for symbol, pnls in sorted(by_symbol.items()):
        wins = [p for p in pnls if p > 0]
        symbol_stats[symbol] = {
            'trades': len(pnls),
            'win_rate': round(len(wins) / len(pnls) * 100, 1) if pnls else 0,
            'total_pnl': round(sum(pnls), 2),
            'expectancy': round(statistics.mean(pnls), 2) if pnls else 0,
        }
    
    # By IV Bucket
    by_iv = {'low_iv_0_33': [], 'mid_iv_33_66': [], 'high_iv_66_100': []}
    for t in trades:
        pctl = t.get('atm_iv_percentile')
        if pctl is None:
            continue
        if pctl < 33:
            by_iv['low_iv_0_33'].append(t['net_pnl'])
        elif pctl < 66:
            by_iv['mid_iv_33_66'].append(t['net_pnl'])
        else:
            by_iv['high_iv_66_100'].append(t['net_pnl'])
    
    iv_stats = {}
    for bucket, pnls in by_iv.items():
        if pnls:
            wins = [p for p in pnls if p > 0]
            iv_stats[bucket] = {
                'trades': len(pnls),
                'win_rate': round(len(wins) / len(pnls) * 100, 1),
                'total_pnl': round(sum(pnls), 2),
                'expectancy': round(statistics.mean(pnls), 2),
            }
        else:
            iv_stats[bucket] = {'trades': 0}
    
    return {
        'by_year': year_stats,
        'by_symbol': symbol_stats,
        'by_iv_bucket': iv_stats,
    }


def compute_summary(structure_name: str, trades: List[Dict], coverage: CoverageTracker) -> Dict:
    """Compute full summary with coverage metrics."""
    if not trades:
        return {
            'structure': structure_name,
            'coverage': coverage.summary(),
            'trades': 0,
            'aggregate': {},
            'stratified': {},
        }
    
    pnls = [t['net_pnl'] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    
    total_win = sum(wins) if wins else 0
    total_loss = abs(sum(losses)) if losses else 0
    pf = total_win / total_loss if total_loss > 0 else float('inf') if total_win > 0 else 0
    
    # Max drawdown
    running, peak, max_dd = 0, 0, 0
    for pnl in pnls:
        running += pnl
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    
    # Tail ratio
    sorted_pnls = sorted(pnls)
    n = len(sorted_pnls)
    p5 = sorted_pnls[max(0, int(n * 0.05))]
    p95 = sorted_pnls[min(n - 1, int(n * 0.95))]
    tail_ratio = p95 / abs(p5) if p5 < 0 else float('inf') if p95 > 0 else 0
    
    return {
        'structure': structure_name,
        'coverage': coverage.summary(),
        'trades': len(trades),
        'aggregate': {
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': round(len(wins) / len(trades) * 100, 1),
            'total_pnl': round(sum(pnls), 2),
            'expectancy': round(statistics.mean(pnls), 2),
            'profit_factor': round(pf, 2) if pf != float('inf') else 'inf',
            'max_dd': round(max_dd, 2),
            'tail_ratio': round(tail_ratio, 2) if tail_ratio != float('inf') else 'inf',
        },
        'stratified': compute_stratified_stats(trades),
        'trade_details': trades,
    }


def main():
    """Run FLAT structure ablations with full coverage tracking."""
    print("=" * 70)
    print("FLAT Structure Ablation Study (AUDIT-GRADE)")
    print("=" * 70)
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    fill_config = FillConfig.from_yaml()
    print(f"\nFill config: slippage=${fill_config.slippage_per_leg}/leg, "
          f"commission=${fill_config.commission_per_contract}/contract")
    
    # Load signals
    print(f"\n[1/5] Loading FLAT signals...")
    signals, load_stats = load_flat_signals(REPORTS_DIR)
    print(f"  Files scanned: {load_stats['files_scanned']}")
    print(f"  FLAT edges found: {load_stats['flat_edges_found']}")
    print(f"  With structure: {load_stats['signals_with_structure']}")
    print(f"  Without structure: {load_stats['signals_without_structure']}")
    
    if not signals:
        print("No signals found. Exiting.")
        return
    
    bar_cache = {}  # Share cache across structures
    results = {}
    
    structures = [
        ('debit_put_spread', payoff_debit_put_spread),
        ('call_debit_spread', payoff_call_debit_spread),
        ('put_credit_spread', payoff_put_credit_spread),
        ('call_calendar', payoff_call_calendar),
    ]
    
    for i, (name, payoff_func) in enumerate(structures, 2):
        print(f"\n[{i}/5] Simulating {name}...")
        trades, coverage = simulate_structure(signals, name, payoff_func, fill_config, bar_cache)
        results[name] = compute_summary(name, trades, coverage)
        cov = results[name]['coverage']
        print(f"  Total: {cov['signals_total']}, Simulated: {cov['signals_simulated']}, "
              f"Skipped: {cov['signals_skipped']}")
        for reason, count in cov['skip_reasons'].items():
            print(f"    - {reason}: {count}")
    
    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'reports_directory': str(REPORTS_DIR),
        'holding_period_days': HOLDING_PERIOD_DAYS,
        'fill_config': {
            'slippage_per_leg': fill_config.slippage_per_leg,
            'commission_per_contract': fill_config.commission_per_contract,
        },
        'load_stats': load_stats,
        'results': results,
    }
    
    # Save (remove trade details for cleaner JSON)
    output_clean = json.loads(json.dumps(output, default=str))
    for name in output_clean['results']:
        output_clean['results'][name].pop('trade_details', None)
        output_clean['results'][name]['coverage'].pop('skip_details', None)
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_clean, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 70)
    print("ABLATION RESULTS SUMMARY")
    print("=" * 70)
    
    for name, stats in results.items():
        agg = stats.get('aggregate', {})
        cov = stats.get('coverage', {})
        print(f"\n{name.upper()}")
        print(f"  Coverage: {cov.get('signals_simulated', 0)}/{cov.get('signals_total', 0)} simulated")
        if agg:
            print(f"  Win Rate: {agg.get('win_rate', 0)}%")
            print(f"  Total PnL: ${agg.get('total_pnl', 0):.2f}")
            print(f"  Expectancy: ${agg.get('expectancy', 0):.2f}")
            print(f"  Profit Factor: {agg.get('profit_factor', 0)}")
            print(f"  Max DD: ${agg.get('max_dd', 0):.2f}")
        
        strat = stats.get('stratified', {})
        if strat.get('by_year'):
            print(f"  By Year:")
            for year, ys in strat['by_year'].items():
                print(f"    {year}: n={ys['trades']}, exp=${ys['expectancy']:.2f}, wr={ys['win_rate']}%")
        
        if strat.get('by_iv_bucket'):
            print(f"  By IV Bucket:")
            for bucket, bs in strat['by_iv_bucket'].items():
                if bs.get('trades', 0) > 0:
                    print(f"    {bucket}: n={bs['trades']}, exp=${bs['expectancy']:.2f}, wr={bs['win_rate']}%")
    
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
