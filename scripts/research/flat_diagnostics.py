#!/usr/bin/env python3
"""
FLAT Signal Diagnostics (Agent #2 Research)

Scans backfill reports for FLAT skew signals and computes:
- Counts by symbol / year
- Distribution of atm_iv, skew_percentile, skew_delta, percentile_delta
- Forward underlying returns: +1D/+5D/+10D/+20D

Usage:
    python scripts/research/flat_diagnostics.py

Output:
    logs/research/flat_diagnostics_summary.json
"""

import sys
sys.path.insert(0, '/Users/jeffreyboyle/Desktop/volmachine')

import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import statistics

from data.polygon_backtest import get_underlying_daily_bars

# Configuration
REPORTS_DIR = Path('./logs/backfill/v6/reports')
OUTPUT_FILE = Path('./logs/research/flat_diagnostics_summary.json')


def load_flat_signals(reports_dir: Path) -> List[Dict]:
    """
    Scan reports directory for FLAT signals.
    
    FLAT signals are identified by:
    - edge.metrics.is_flat == 1.0, OR
    - edge.direction == "LONG" (for skew_extreme edges)
    """
    flat_signals = []
    
    for report_file in sorted(reports_dir.glob('*__*__backfill.json')):
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            # Parse filename: 2022-04-21__SPY__backfill.json
            parts = report_file.stem.split('__')
            if len(parts) < 2:
                continue
            
            report_date_str = parts[0]
            symbol = parts[1]
            
            # Get execution date (trade date = day after signal)
            execution_date = report.get('execution_date', report_date_str)
            signal_date = report.get('report_date', report_date_str)
            
            # Check edges for FLAT signals
            for edge in report.get('edges', []):
                metrics = edge.get('metrics', {})
                
                # FLAT detection: is_flat == 1.0 OR direction == LONG
                is_flat = metrics.get('is_flat', 0.0) == 1.0
                is_long = edge.get('direction', '').upper() == 'LONG'
                
                if is_flat or is_long:
                    flat_signals.append({
                        'symbol': symbol,
                        'signal_date': signal_date,
                        'execution_date': execution_date,
                        'edge_type': edge.get('edge_type'),
                        'strength': edge.get('strength'),
                        'direction': edge.get('direction'),
                        'atm_iv': metrics.get('atm_iv'),
                        'atm_iv_percentile': metrics.get('atm_iv_percentile'),
                        'skew_percentile': metrics.get('skew_percentile'),
                        'put_call_skew': metrics.get('put_call_skew'),
                        'skew_delta': metrics.get('skew_delta'),
                        'percentile_delta': metrics.get('percentile_delta'),
                        'is_flat': metrics.get('is_flat'),
                        'history_mode': metrics.get('history_mode'),
                        'structure': report.get('candidates', [{}])[0].get('structure'),
                    })
                    
        except Exception as e:
            print(f"Error loading {report_file}: {e}")
            continue
    
    return flat_signals


def compute_forward_returns(signals: List[Dict]) -> List[Dict]:
    """
    Compute forward underlying returns for each signal.
    
    Returns: +1D, +5D, +10D, +20D returns from execution date.
    """
    # Group signals by symbol to batch API calls
    by_symbol = defaultdict(list)
    for sig in signals:
        by_symbol[sig['symbol']].append(sig)
    
    enriched = []
    
    for symbol, symbol_signals in by_symbol.items():
        print(f"[FLAT] Computing forward returns for {symbol} ({len(symbol_signals)} signals)...")
        
        # Get date range for underlying bars
        dates = [datetime.strptime(s['execution_date'], '%Y-%m-%d').date() 
                 for s in symbol_signals]
        min_date = min(dates) - timedelta(days=5)
        max_date = max(dates) + timedelta(days=30)  # Need +20D forward
        
        try:
            bars = get_underlying_daily_bars(symbol, min_date, max_date, use_cache=True)
            
            if not bars:
                print(f"  No underlying bars for {symbol}")
                for sig in symbol_signals:
                    sig['fwd_1d'] = None
                    sig['fwd_5d'] = None
                    sig['fwd_10d'] = None
                    sig['fwd_20d'] = None
                    enriched.append(sig)
                continue
            
            # Build date -> close price lookup
            prices = {bar['date']: bar['close'] for bar in bars}
            date_list = sorted(prices.keys())
            
            for sig in symbol_signals:
                exec_date = datetime.strptime(sig['execution_date'], '%Y-%m-%d').date()
                exec_str = exec_date.isoformat()
                
                # Find execution day price
                entry_price = prices.get(exec_str)
                if entry_price is None:
                    # Try to find next available trading day
                    for d in date_list:
                        if d >= exec_str:
                            entry_price = prices.get(d)
                            break
                
                if entry_price is None:
                    sig['fwd_1d'] = None
                    sig['fwd_5d'] = None
                    sig['fwd_10d'] = None
                    sig['fwd_20d'] = None
                    enriched.append(sig)
                    continue
                
                # Compute forward returns
                for horizon, key in [(1, 'fwd_1d'), (5, 'fwd_5d'), (10, 'fwd_10d'), (20, 'fwd_20d')]:
                    target_date = exec_date + timedelta(days=horizon)
                    target_str = target_date.isoformat()
                    
                    # Find next available trading day on or after target
                    exit_price = None
                    for d in date_list:
                        if d >= target_str:
                            exit_price = prices.get(d)
                            break
                    
                    if exit_price:
                        sig[key] = round((exit_price - entry_price) / entry_price * 100, 3)
                    else:
                        sig[key] = None
                
                enriched.append(sig)
                
        except Exception as e:
            print(f"  Error fetching bars for {symbol}: {e}")
            for sig in symbol_signals:
                sig['fwd_1d'] = None
                sig['fwd_5d'] = None
                sig['fwd_10d'] = None
                sig['fwd_20d'] = None
                enriched.append(sig)
    
    return enriched


def compute_distributions(signals: List[Dict]) -> Dict:
    """Compute distributions of key metrics."""
    
    def safe_stats(values: List[float]) -> Dict:
        """Compute stats for a list of values."""
        clean = [v for v in values if v is not None]
        if not clean:
            return {'count': 0, 'mean': None, 'median': None, 'std': None, 'min': None, 'max': None}
        
        return {
            'count': len(clean),
            'mean': round(statistics.mean(clean), 4),
            'median': round(statistics.median(clean), 4),
            'std': round(statistics.stdev(clean), 4) if len(clean) > 1 else 0,
            'min': round(min(clean), 4),
            'max': round(max(clean), 4),
        }
    
    return {
        'atm_iv': safe_stats([s.get('atm_iv') for s in signals]),
        'atm_iv_percentile': safe_stats([s.get('atm_iv_percentile') for s in signals]),
        'skew_percentile': safe_stats([s.get('skew_percentile') for s in signals]),
        'put_call_skew': safe_stats([s.get('put_call_skew') for s in signals]),
        'skew_delta': safe_stats([s.get('skew_delta') for s in signals]),
        'percentile_delta': safe_stats([s.get('percentile_delta') for s in signals]),
        'fwd_1d': safe_stats([s.get('fwd_1d') for s in signals]),
        'fwd_5d': safe_stats([s.get('fwd_5d') for s in signals]),
        'fwd_10d': safe_stats([s.get('fwd_10d') for s in signals]),
        'fwd_20d': safe_stats([s.get('fwd_20d') for s in signals]),
    }


def compute_counts_by_symbol_year(signals: List[Dict]) -> Dict:
    """Compute signal counts by symbol and year."""
    counts = defaultdict(lambda: defaultdict(int))
    
    for sig in signals:
        symbol = sig['symbol']
        year = sig['signal_date'][:4]
        counts[symbol][year] += 1
    
    # Convert to regular dict for JSON
    return {sym: dict(years) for sym, years in counts.items()}


def compute_iv_bucket_analysis(signals: List[Dict]) -> Dict:
    """Analyze forward returns by ATM IV percentile buckets."""
    buckets = {
        'low_iv_0_33': [],
        'mid_iv_33_66': [],
        'high_iv_66_100': [],
    }
    
    for sig in signals:
        pctl = sig.get('atm_iv_percentile')
        if pctl is None:
            continue
        
        if pctl < 33:
            buckets['low_iv_0_33'].append(sig)
        elif pctl < 66:
            buckets['mid_iv_33_66'].append(sig)
        else:
            buckets['high_iv_66_100'].append(sig)
    
    analysis = {}
    for bucket_name, bucket_signals in buckets.items():
        if not bucket_signals:
            analysis[bucket_name] = {'count': 0}
            continue
        
        fwd_returns = {
            'fwd_1d': [s['fwd_1d'] for s in bucket_signals if s.get('fwd_1d') is not None],
            'fwd_5d': [s['fwd_5d'] for s in bucket_signals if s.get('fwd_5d') is not None],
            'fwd_10d': [s['fwd_10d'] for s in bucket_signals if s.get('fwd_10d') is not None],
            'fwd_20d': [s['fwd_20d'] for s in bucket_signals if s.get('fwd_20d') is not None],
        }
        
        analysis[bucket_name] = {
            'count': len(bucket_signals),
            'fwd_1d_mean': round(statistics.mean(fwd_returns['fwd_1d']), 3) if fwd_returns['fwd_1d'] else None,
            'fwd_5d_mean': round(statistics.mean(fwd_returns['fwd_5d']), 3) if fwd_returns['fwd_5d'] else None,
            'fwd_10d_mean': round(statistics.mean(fwd_returns['fwd_10d']), 3) if fwd_returns['fwd_10d'] else None,
            'fwd_20d_mean': round(statistics.mean(fwd_returns['fwd_20d']), 3) if fwd_returns['fwd_20d'] else None,
        }
    
    return analysis


def main():
    """Run FLAT diagnostics."""
    print("=" * 60)
    print("FLAT Signal Diagnostics")
    print("=" * 60)
    
    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Load FLAT signals
    print(f"\n[1/4] Scanning {REPORTS_DIR} for FLAT signals...")
    signals = load_flat_signals(REPORTS_DIR)
    print(f"  Found {len(signals)} FLAT signals")
    
    if not signals:
        print("No FLAT signals found. Exiting.")
        return
    
    # Compute forward returns
    print(f"\n[2/4] Computing forward underlying returns...")
    signals = compute_forward_returns(signals)
    
    # Compute statistics
    print(f"\n[3/4] Computing distributions...")
    distributions = compute_distributions(signals)
    counts = compute_counts_by_symbol_year(signals)
    iv_buckets = compute_iv_bucket_analysis(signals)
    
    # Build summary
    summary = {
        'generated_at': datetime.now().isoformat(),
        'reports_directory': str(REPORTS_DIR),
        'total_flat_signals': len(signals),
        'counts_by_symbol_year': counts,
        'distributions': distributions,
        'iv_bucket_analysis': iv_buckets,
        'signals': signals,  # Full detail for downstream analysis
    }
    
    # Save output
    print(f"\n[4/4] Saving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total FLAT signals: {len(signals)}")
    print(f"\nSignals by symbol:")
    for sym, years in sorted(counts.items()):
        total = sum(years.values())
        print(f"  {sym}: {total} ({', '.join(f'{y}:{c}' for y, c in sorted(years.items()))})")
    
    print(f"\nForward Returns (all signals):")
    for horizon in ['fwd_1d', 'fwd_5d', 'fwd_10d', 'fwd_20d']:
        d = distributions[horizon]
        if d['mean'] is not None:
            print(f"  {horizon}: mean={d['mean']:.2f}%, median={d['median']:.2f}% (n={d['count']})")
    
    print(f"\nIV Bucket Analysis:")
    for bucket, stats in iv_buckets.items():
        if stats['count'] > 0:
            print(f"  {bucket}: n={stats['count']}, "
                  f"+5D={stats['fwd_5d_mean']}%, +20D={stats['fwd_20d_mean']}%")
    
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
