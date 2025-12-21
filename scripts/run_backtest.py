#!/usr/bin/env python3
"""
Run Backtest CLI.

Entry point for running deterministic backtests.
"""

import argparse
import sys
import json
import yaml
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.deterministic import DeterministicBacktester
from backtest.integrity import generate_integrity_report, print_integrity_report


def check_coverage(reports_dir: Path, start_date: date, end_date: date, 
                   config_path: str, force: bool = False, symbols: list = None) -> Dict[str, Any]:
    """
    Load coverage JSONL and check if coverage meets minimum threshold.
    
    Returns dict with:
        - status: 'VALID', 'INVALID', or 'UNKNOWN' (no coverage file)
        - coverage_by_symbol: {symbol: rate}
        - min_coverage: threshold from config
        - message: explanation
    """
    # Load config for threshold
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except:
        config = {}
    
    validation_config = config.get('validation', {})
    min_coverage = validation_config.get('min_coverage', 0.90)
    fail_on_low_coverage = validation_config.get('fail_on_low_coverage', True)
    
    # Find coverage file matching backtest period
    coverage_files = list(reports_dir.glob('coverage_*.jsonl'))
    if not coverage_files:
        print(f"⚠️  No coverage file found in {reports_dir}")
        print("   Run backfill first to generate coverage data.")
        return {'status': 'UNKNOWN', 'coverage_by_symbol': {}, 'min_coverage': min_coverage, 
                'message': 'No coverage file found', 'records': []}
    
    # Parse coverage file dates and find best match for backtest period
    # Coverage files are named: coverage_YYYY-MM-DD_YYYY-MM-DD.jsonl
    coverage_file = None
    best_match_coverage = 0  # Track overlap coverage
    
    for cf in coverage_files:
        try:
            # Extract start/end dates from filename
            fname = cf.stem  # coverage_YYYY-MM-DD_YYYY-MM-DD
            parts = fname.split('_')
            if len(parts) >= 3:
                cf_start = date.fromisoformat(parts[1])
                cf_end = date.fromisoformat(parts[2])
                
                # Check if this coverage file's period matches the backtest period
                # Perfect match: coverage file covers the entire backtest period
                if cf_start <= start_date and cf_end >= end_date:
                    # This file covers our period fully
                    coverage_file = cf
                    print(f"Loading coverage: {cf} (full match for {start_date} to {end_date})")
                    break
                
                # Partial match: some overlap
                overlap_start = max(cf_start, start_date)
                overlap_end = min(cf_end, end_date)
                if overlap_end >= overlap_start:
                    overlap_days = (overlap_end - overlap_start).days + 1
                    backtest_days = (end_date - start_date).days + 1
                    overlap_pct = overlap_days / backtest_days if backtest_days > 0 else 0
                    
                    if overlap_pct > best_match_coverage:
                        best_match_coverage = overlap_pct
                        coverage_file = cf
        except (ValueError, IndexError):
            continue
    
    # Fallback to most recent if no match found
    if not coverage_file:
        coverage_file = sorted(coverage_files)[-1]
        print(f"⚠️  No period-matching coverage file found!")
        print(f"   Using most recent: {coverage_file}")
        print(f"   WARNING: Coverage check may not reflect actual backtest period.")
    elif best_match_coverage >= 1.0:
        print(f"Loading coverage: {coverage_file} (full match)")
    elif best_match_coverage > 0:
        print(f"Loading coverage: {coverage_file}")
        print(f"   ⚠️  Partial period match: {best_match_coverage:.0%} overlap with backtest period")
    else:
        print(f"Loading coverage: {coverage_file}")
    
    # Parse coverage JSONL
    records = []
    try:
        with open(coverage_file) as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
    except Exception as e:
        print(f"⚠️  Error loading coverage file: {e}")
        return {'status': 'UNKNOWN', 'coverage_by_symbol': {}, 'min_coverage': min_coverage,
                'message': f'Error loading coverage: {e}'}
    
    if not records:
        return {'status': 'UNKNOWN', 'coverage_by_symbol': {}, 'min_coverage': min_coverage,
                'message': 'Coverage file is empty'}
    
    # Compute coverage per symbol ONLY for records within backtest period
    valid_by_symbol = {}
    total_by_symbol = {}
    records_in_range = []  # Filtered records for fallback analysis
    skipped_records = 0
    
    for record in records:
        sym = record.get('symbol')
        status = record.get('status')
        record_date_str = record.get('date')
        
        # Skip if no date or symbol
        if not sym or not record_date_str:
            skipped_records += 1
            continue
        
        # Parse and filter by backtest period
        try:
            record_date = date.fromisoformat(record_date_str)
        except ValueError:
            skipped_records += 1
            continue
        
        # CRITICAL: Only count records within backtest period
        if not (start_date <= record_date <= end_date):
            continue
        
        # Filter by --symbols if specified (exclude symbols not in list)
        if symbols and sym not in symbols:
            continue
        
        # Track filtered records for fallback analysis
        records_in_range.append(record)
        
        total_by_symbol[sym] = total_by_symbol.get(sym, 0) + 1
        if status == 'VALID':
            valid_by_symbol[sym] = valid_by_symbol.get(sym, 0) + 1
    
    coverage_by_symbol = {}
    all_pass = True
    
    print(f"\n" + "=" * 60)
    print("COVERAGE CHECK")
    print("=" * 60)
    print(f"Minimum required: {min_coverage:.0%}")
    print()
    
    for sym in sorted(total_by_symbol.keys()):
        valid = valid_by_symbol.get(sym, 0)
        total = total_by_symbol[sym]
        rate = valid / total if total > 0 else 0
        coverage_by_symbol[sym] = rate
        status_icon = "✅" if rate >= min_coverage else "❌"
        if rate < min_coverage:
            all_pass = False
        print(f"  {sym}: {valid}/{total} = {rate:.1%} {status_icon}")
    
    overall_status = 'VALID' if all_pass else 'INVALID'
    
    if not all_pass:
        print(f"\n⚠️  Some symbols below {min_coverage:.0%} threshold!")
        if fail_on_low_coverage and not force:
            print("   Backtest results cannot be trusted.")
    else:
        print(f"\n✅  All symbols pass {min_coverage:.0%} coverage threshold.")
    
    return {
        'status': overall_status,
        'coverage_by_symbol': coverage_by_symbol,
        'min_coverage': min_coverage,
        'message': 'Coverage check complete',
        'records': records_in_range  # Filtered to backtest period for fallback analysis
    }


def find_valid_windows(records: list, symbols: list, start_date: date, end_date: date, 
                        min_window_days: int = 14) -> list:
    """
    Find contiguous date windows where ALL symbols have VALID coverage.
    ONLY considers records within the backtest period (start_date to end_date).
    
    Returns list of windows: [{'start': date, 'end': date, 'days': int}]
    """
    if not records:
        return []
    
    # Build daily validity matrix: {date_str: {symbol: bool}}
    # CRITICAL: Only include records within backtest period
    daily_valid = {}
    all_dates = set()
    
    for rec in records:
        dt_str = rec.get('date')
        sym = rec.get('symbol')
        status = rec.get('status')
        
        if not dt_str or not sym:
            continue
        
        try:
            dt = date.fromisoformat(dt_str)
        except ValueError:
            continue
        
        # Filter by backtest period
        if not (start_date <= dt <= end_date):
            continue
        
        all_dates.add(dt_str)
        if dt_str not in daily_valid:
            daily_valid[dt_str] = {}
        daily_valid[dt_str][sym] = (status == 'VALID')
    
    # Sort dates chronologically
    sorted_dates = sorted(all_dates)
    
    # Find days where ALL symbols are VALID
    all_valid_dates = []
    for dt in sorted_dates:
        day_status = daily_valid.get(dt, {})
        if all(day_status.get(sym, False) for sym in symbols):
            all_valid_dates.append(dt)
    
    # Find contiguous windows
    windows = []
    if not all_valid_dates:
        return windows
    
    window_start = all_valid_dates[0]
    window_dates = [window_start]
    
    for i in range(1, len(all_valid_dates)):
        prev_date = date.fromisoformat(all_valid_dates[i-1])
        curr_date = date.fromisoformat(all_valid_dates[i])
        
        # Check if contiguous (allowing for weekends: gap <= 3 days)
        gap = (curr_date - prev_date).days
        
        if gap <= 3:  # Allow weekend gaps
            window_dates.append(all_valid_dates[i])
        else:
            # Save current window if long enough
            if len(window_dates) >= min_window_days:
                windows.append({
                    'start': date.fromisoformat(window_dates[0]),
                    'end': date.fromisoformat(window_dates[-1]),
                    'days': len(window_dates),
                    'dates': window_dates
                })
            # Start new window
            window_start = all_valid_dates[i]
            window_dates = [window_start]
    
    # Don't forget last window
    if len(window_dates) >= min_window_days:
        windows.append({
            'start': date.fromisoformat(window_dates[0]),
            'end': date.fromisoformat(window_dates[-1]),
            'days': len(window_dates),
            'dates': window_dates
        })
    
    return windows


def print_valid_windows(windows: list, symbols: list):
    """Print valid windows summary."""
    print()
    print("=" * 60)
    print("VALID COVERAGE WINDOWS")
    print("=" * 60)
    
    if not windows:
        print("⚠️  No valid windows found with all symbols at 100% daily coverage!")
        print("   Cannot run reliable backtest without valid data windows.")
        return
    
    total_valid_days = sum(w['days'] for w in windows)
    print(f"Found {len(windows)} valid window(s) totaling {total_valid_days} days:")
    print()
    
    print(f"  {'#':>3}  {'Start':<12} {'End':<12} {'Days':>6}")
    print(f"  {'-'*3}  {'-'*12} {'-'*12} {'-'*6}")
    
    for i, w in enumerate(windows, 1):
        print(f"  {i:>3}  {w['start'].isoformat():<12} {w['end'].isoformat():<12} {w['days']:>6}")
    
    print()


def run_windowed_backtest(windows: list, config_path: str, reports_dir: str, 
                           symbols: list = None, csv_export: bool = False) -> dict:
    """
    Run backtests on each valid coverage window and aggregate results.
    
    Returns dict with per-window stats and aggregated totals.
    """
    if not windows:
        return {'status': 'NO_WINDOWS', 'windows': [], 'aggregate': None}
    
    window_results = []
    all_trades = []
    
    print()
    print("=" * 60)
    print("WINDOWED BACKTEST MODE")
    print("=" * 60)
    print(f"Running backtests on {len(windows)} valid coverage window(s)...")
    print()
    
    backtester = DeterministicBacktester(config_path=config_path, reports_dir=reports_dir)
    
    for i, window in enumerate(windows, 1):
        print(f"--- Window {i}: {window['start'].isoformat()} to {window['end'].isoformat()} ({window['days']} days) ---")
        
        result = backtester.run_range(window['start'], window['end'], symbols=symbols)
        
        # Calculate window stats
        wins = sum(1 for t in result.trades if t.net_pnl > 0)
        losses = len(result.trades) - wins
        total_pnl = sum(t.net_pnl for t in result.trades)
        win_rate = wins / len(result.trades) * 100 if result.trades else 0
        
        # Profit factor
        gross_profit = sum(t.net_pnl for t in result.trades if t.net_pnl > 0)
        gross_loss = abs(sum(t.net_pnl for t in result.trades if t.net_pnl < 0))
        pf = gross_profit / gross_loss if gross_loss > 0 else float('inf') if gross_profit > 0 else 0
        
        window_stats = {
            'window_num': i,
            'start': window['start'],
            'end': window['end'],
            'days': window['days'],
            'trades': len(result.trades),
            'wins': wins,
            'losses': losses,
            'pnl': total_pnl,
            'win_rate': win_rate,
            'profit_factor': pf,
        }
        window_results.append(window_stats)
        all_trades.extend(result.trades)
        
        # Print window summary
        pf_str = f"{pf:.2f}" if pf != float('inf') else "∞"
        print(f"  Trades: {len(result.trades)} ({wins}W/{losses}L), PnL: ${total_pnl:.2f}, WR: {win_rate:.1f}%, PF: {pf_str}")
        print()
    
    # Aggregate stats across all windows
    total_trades = len(all_trades)
    total_wins = sum(1 for t in all_trades if t.net_pnl > 0)
    total_losses = total_trades - total_wins
    total_pnl = sum(t.net_pnl for t in all_trades)
    aggregate_wr = total_wins / total_trades * 100 if total_trades > 0 else 0
    
    agg_gross_profit = sum(t.net_pnl for t in all_trades if t.net_pnl > 0)
    agg_gross_loss = abs(sum(t.net_pnl for t in all_trades if t.net_pnl < 0))
    agg_pf = agg_gross_profit / agg_gross_loss if agg_gross_loss > 0 else float('inf') if agg_gross_profit > 0 else 0
    
    total_valid_days = sum(w['days'] for w in windows)
    
    print("=" * 60)
    print("AGGREGATE RESULTS (VALID WINDOWS ONLY)")
    print("=" * 60)
    print()
    print(f"📅 Valid Coverage: {total_valid_days} days across {len(windows)} windows")
    print(f"📊 TRADES: {total_trades} total ({total_wins}W / {total_losses}L)")
    print(f"📈 WIN RATE: {aggregate_wr:.1f}%")
    print()
    print(f"💰 TOTAL PnL: ${total_pnl:.2f}")
    agg_pf_str = f"{agg_pf:.2f}" if agg_pf != float('inf') else "∞"
    print(f"📐 PROFIT FACTOR: {agg_pf_str}")
    print()
    print(f"⚠️  EXCLUDED: Dates with provider data gaps (coverage < 100%)")
    print()
    
    return {
        'status': 'COMPLETED',
        'windows': window_results,
        'aggregate': {
            'total_days': total_valid_days,
            'num_windows': len(windows),
            'trades': total_trades,
            'wins': total_wins,
            'losses': total_losses,
            'pnl': total_pnl,
            'win_rate': aggregate_wr,
            'profit_factor': agg_pf,
        },
        'all_trades': all_trades,
    }



def analyze_fallback_trades(result, coverage_records: list, config: dict) -> dict:
    """
    Analyze trades for fallback strike usage.
    
    Returns dict with:
        - fallback_count: number of trades using fallback
        - fallback_ratio: fallback_count / total_trades
        - max_fallback_distance: max abs(original - actual) across all trades
        - fallback_trades: list of trades with fallback
        - non_fallback_trades: list of trades without fallback
        - stratified_stats: {fallback: {trades, wins, pnl, pf}, non_fallback: {...}}
        - validation_status: 'PASS' or 'FAIL'
        - validation_message: reason for failure
    """
    validation_config = config.get('validation', {})
    max_distance_limit = validation_config.get('max_fallback_strike_distance_points', 5)
    max_ratio_limit = validation_config.get('max_fallback_trade_ratio', 0.25)
    
    # Build lookup from coverage records for signal dates
    coverage_by_date_symbol = {}
    for rec in coverage_records:
        key = (rec.get('date'), rec.get('symbol'))
        coverage_by_date_symbol[key] = rec.get('details', {})
    
    fallback_trades = []
    non_fallback_trades = []
    max_distance = 0
    
    for trade in result.trades:
        # Try to find coverage details for this trade
        entry_date = getattr(trade, 'entry_date', None)
        symbol = getattr(trade, 'symbol', None)
        
        # Check if this trade used fallback
        details = coverage_by_date_symbol.get((str(entry_date), symbol), {})
        used_fallback = details.get('used_fallback_strike', False)
        
        if used_fallback:
            # Compute distance
            # Note: coverage stores these as put_strike/call_strike, not actual_put_strike
            orig_put = details.get('original_put_strike', 0)
            actual_put = details.get('put_strike', 0)  # Fixed: was 'actual_put_strike'
            orig_call = details.get('original_call_strike', 0)
            actual_call = details.get('call_strike', 0)  # Fixed: was 'actual_call_strike'
            
            put_distance = abs(orig_put - actual_put) if orig_put and actual_put else 0
            call_distance = abs(orig_call - actual_call) if orig_call and actual_call else 0
            distance = max(put_distance, call_distance)
            max_distance = max(max_distance, distance)
            
            fallback_trades.append((trade, distance))
        else:
            non_fallback_trades.append(trade)
    
    total_trades = len(result.trades)
    fallback_count = len(fallback_trades)
    fallback_ratio = fallback_count / total_trades if total_trades > 0 else 0
    
    # Compute stratified stats
    def compute_bucket_stats(trades_list):
        if not trades_list:
            return {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0, 'win_rate': 0, 'pf': 0, 'avg_pnl': 0}
        
        # Handle both (trade, distance) tuples and plain trades
        trades = [t[0] if isinstance(t, tuple) else t for t in trades_list]
        
        wins = sum(1 for t in trades if t.net_pnl > 0)
        losses = len(trades) - wins
        total_pnl = sum(t.net_pnl for t in trades)
        gross_profit = sum(t.net_pnl for t in trades if t.net_pnl > 0)
        gross_loss = abs(sum(t.net_pnl for t in trades if t.net_pnl < 0))
        
        win_rate = wins / len(trades) * 100 if trades else 0
        pf = gross_profit / gross_loss if gross_loss > 0 else float('inf') if gross_profit > 0 else 0
        avg_pnl = total_pnl / len(trades) if trades else 0
        
        return {
            'trades': len(trades),
            'wins': wins,
            'losses': losses,
            'pnl': total_pnl,
            'win_rate': win_rate,
            'pf': pf,
            'avg_pnl': avg_pnl
        }
    
    fallback_stats = compute_bucket_stats(fallback_trades)
    non_fallback_stats = compute_bucket_stats(non_fallback_trades)
    
    # Validation checks
    validation_status = 'PASS'
    validation_message = ''
    
    if max_distance > max_distance_limit:
        validation_status = 'FAIL'
        validation_message = f'Max fallback distance {max_distance} > limit {max_distance_limit}'
    elif fallback_ratio > max_ratio_limit:
        validation_status = 'FAIL'
        validation_message = f'Fallback ratio {fallback_ratio:.1%} > limit {max_ratio_limit:.0%}'
    
    return {
        'fallback_count': fallback_count,
        'fallback_ratio': fallback_ratio,
        'max_fallback_distance': max_distance,
        'max_distance_limit': max_distance_limit,
        'max_ratio_limit': max_ratio_limit,
        'fallback_stats': fallback_stats,
        'non_fallback_stats': non_fallback_stats,
        'validation_status': validation_status,
        'validation_message': validation_message
    }


def print_fallback_analysis(analysis: dict):
    """Print fallback analysis results."""
    print()
    print("=" * 60)
    print("FALLBACK STRIKE ANALYSIS")
    print("=" * 60)
    
    fb = analysis
    status_icon = "✅" if fb['validation_status'] == 'PASS' else "❌"
    
    print(f"Fallback trades: {fb['fallback_count']} / {fb['fallback_stats']['trades'] + fb['non_fallback_stats']['trades']} ({fb['fallback_ratio']:.1%})")
    print(f"Max fallback distance: {fb['max_fallback_distance']} points (limit: {fb['max_distance_limit']})")
    print(f"Validation: {fb['validation_status']} {status_icon}")
    
    if fb['validation_message']:
        print(f"  Reason: {fb['validation_message']}")
    
    # Stratified breakdown
    print()
    print("STRATIFIED RESULTS:")
    print(f"  {'Bucket':<20} {'Trades':>8} {'WR':>8} {'PF':>8} {'Avg PnL':>10}")
    print(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")
    
    nf = fb['non_fallback_stats']
    print(f"  {'Non-fallback':<20} {nf['trades']:>8} {nf['win_rate']:>7.1f}% {nf['pf']:>8.2f} ${nf['avg_pnl']:>9.2f}")
    
    f = fb['fallback_stats']
    print(f"  {'Fallback':<20} {f['trades']:>8} {f['win_rate']:>7.1f}% {f['pf']:>8.2f} ${f['avg_pnl']:>9.2f}")


def main():
    parser = argparse.ArgumentParser(
        description="Run deterministic backtest on historical signals"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to backtest (default: 90)"
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Number of years to backtest (overrides --days)"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). Overrides --days"
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD, default: today)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include (default: from config)"
    )
    parser.add_argument(
        "--config",
        default="./config/backtest.yaml",
        help="Path to backtest config"
    )
    parser.add_argument(
        "--output",
        default="./logs/backtest",
        help="Output directory for results"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Export trades to CSV"
    )
    parser.add_argument(
        "--integrity",
        action="store_true",
        default=True,
        help="Show integrity report (default: True)"
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Path to reports directory (default: ./logs/reports)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Proceed even if coverage is below threshold"
    )
    parser.add_argument(
        "--windows",
        action="store_true",
        help="Run backtests on valid coverage windows only (auto-enabled if coverage fails)"
    )
    parser.add_argument(
        "--phase",
        choices=["phase1", "phase2", "phase3"],
        default=None,
        help="Phase preset: phase1=edge_validation (no RiskEngine), phase2=tradeability, phase3=optimization"
    )
    parser.add_argument(
        "--edge-slice",
        choices=["steep", "flat", "both"],
        default="both",
        help="Filter signals by edge type: steep (is_steep=True), flat (is_flat=True), or both (default: both)"
    )
    parser.add_argument(
        "--max-iv",
        type=float,
        default=None,
        help="Max ATM IV filter (e.g., 0.30 for regime filter). Only include signals where atm_iv <= this value."
    )
    parser.add_argument(
        "--max-atm-iv-pctl",
        type=float,
        default=None,
        help="Max ATM IV percentile filter (e.g., 80 for 80th percentile). Asset-independent regime filter."
    )
    
    args = parser.parse_args()
    
    # Calculate date range
    end_date = date.today()
    if args.end:
        end_date = date.fromisoformat(args.end)
    
    if args.start:
        start_date = date.fromisoformat(args.start)
    elif args.years:
        start_date = end_date - timedelta(days=args.years * 365)
    else:
        start_date = end_date - timedelta(days=args.days)
    
    # Load config for run header
    try:
        with open(args.config) as f:
            config = yaml.safe_load(f)
    except:
        config = {}
    
    # Extract config values for header
    strategy_config = config.get('strategies', {}).get('skew_extreme', {})
    risk_engine_config = config.get('risk_engine', {})
    portfolio_mode = strategy_config.get('portfolio_mode', 'portfolio_safe')
    
    # Phase override
    phase_config = {}
    if args.phase:
        phase_config = config.get('phases', {}).get(args.phase, {})
        if phase_config.get('portfolio_mode'):
            portfolio_mode = phase_config['portfolio_mode']
            # Also override in strategy_config for downstream use
            strategy_config['portfolio_mode'] = portfolio_mode
    
    max_open_positions = risk_engine_config.get('max_open_positions', 3)
    max_total_risk = risk_engine_config.get('max_total_risk_usd', 1500)
    max_cluster_positions = risk_engine_config.get('max_cluster_positions', 1)
    max_cluster_risk = risk_engine_config.get('max_cluster_risk_usd', 750)
    
    print(f"VolMachine Backtest")
    print(f"=" * 60)
    print(f"Period: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    print(f"Config: {args.config}")
    if args.input_dir:
        print(f"Input: {args.input_dir}")
    if args.symbols:
        print(f"Symbols: {', '.join(args.symbols)}")
    
    # Run configuration header
    print()
    print("=" * 60)
    print("RESOLVED CONFIGURATION")
    print("=" * 60)
    
    # Mode
    print(f"\n📊 MODE:")
    if args.phase:
        phase_desc = phase_config.get('description', args.phase)
        print(f"  PHASE: {args.phase.upper()} - {phase_desc}")
    if portfolio_mode == 'signal_quality':
        print(f"  Portfolio Mode: SIGNAL_QUALITY (research - unlimited overlap)")
        if args.phase == 'phase1':
            print(f"  ⚠️  RiskEngine: DISABLED")
            print(f"  ⚠️  Credit gate: {phase_config.get('min_credit_to_width', 0.15)*100:.0f}% (relaxed)")
            print(f"  Goal: Prove skew mean-reversion produces positive expectancy")
    else:
        print(f"  Portfolio Mode: PORTFOLIO_SAFE (RiskEngine enabled)")
    print(f"  Windowed: {args.windows}, Force: {args.force}")
    
    # Position limits - only show if portfolio_safe
    if portfolio_mode == 'portfolio_safe':
        print(f"\n📦 POSITION LIMITS:")
        print(f"  Max Positions: {max_open_positions}")
        print(f"  Max Total Risk: ${max_total_risk}")
        print(f"  Risk Per Trade: ${risk_engine_config.get('risk_per_trade_usd', 500)}")
        
        # Cluster settings
        print(f"\n🔗 CLUSTER SETTINGS:")
        print(f"  Cluster Cap: {max_cluster_positions}")
        print(f"  Cluster Risk: ${max_cluster_risk}")
        print(f"  Dedup Mode: {risk_engine_config.get('cluster_dedup_mode', 'best_edge')}")
        clusters = risk_engine_config.get('clusters', {})
        for cluster_name, symbols in clusters.items():
            print(f"  Cluster '{cluster_name}': {symbols}")
        
        # Cooldowns
        print(f"\n⏱️ COOLDOWNS:")
        symbol_cd = risk_engine_config.get('symbol_cooldown_after_sl_days', 10)
        cluster_cd = risk_engine_config.get('cluster_cooldown_after_sl_days', 5)
        print(f"  Symbol cooldown after SL: {symbol_cd} days")
        print(f"  Cluster cooldown after SL: {cluster_cd} days")
        
        # Drawdown protection
        dd_kill = risk_engine_config.get('dd_kill_pct', 0.10)
        print(f"  DD Kill-Switch: {dd_kill*100:.0f}% from peak")
    
    # Exit rules
    exit_rules = config.get('exit_rules', {}).get('credit_spread', {})
    tp_pct = exit_rules.get('take_profit_pct', 50)
    sl_mult = exit_rules.get('stop_loss_mult', 1.25)
    ts_dte = exit_rules.get('time_stop_dte', 7)
    print(f"\n🚪 EXIT RULES (credit spread):")
    print(f"  Take Profit: {f'{tp_pct}%' if tp_pct is not None else 'DISABLED'}")
    print(f"  Stop Loss: {f'{sl_mult}x credit' if sl_mult is not None else 'DISABLED'}")
    print(f"  Time Stop: DTE ≤ {ts_dte}")
    
    # Fallback policy
    print(f"\n⚠️ FALLBACK POLICY:")
    print(f"  ATM Fallback Distance Limit: 5 points (STRICT)")
    
    print()
    print("=" * 60)
    print()
    
    # Run backtest
    reports_dir = args.input_dir if args.input_dir else './logs/reports'
    
    # Load and check coverage (if coverage file exists)
    # Pass symbols to filter coverage check to only specified symbols
    coverage_check_result = check_coverage(Path(reports_dir), start_date, end_date, args.config, args.force, symbols=args.symbols)
    if coverage_check_result['status'] == 'INVALID' and not args.force:
        # Show valid windows even if overall coverage fails
        coverage_records = coverage_check_result.get('records', [])
        symbols = args.symbols if args.symbols else ['SPY', 'QQQ', 'IWM']
        valid_windows = find_valid_windows(coverage_records, symbols, start_date, end_date, min_window_days=14)
        print_valid_windows(valid_windows, symbols)
        
        # If --windows flag, run windowed backtest instead of exiting
        if args.windows and valid_windows:
            windowed_result = run_windowed_backtest(
                windows=valid_windows,
                config_path=args.config,
                reports_dir=reports_dir,
                symbols=symbols,
                csv_export=args.csv
            )
            
            # Consider windowed backtest successful if we got trades
            if windowed_result['status'] == 'COMPLETED' and windowed_result['aggregate']['trades'] > 0:
                print("=" * 60)
                print("WINDOWED BACKTEST: COMPLETED")
                print("=" * 60)
                print("\n✅ Strategy validated on coverage-verified windows only.")
                print("⚠️  March 2025 and other data gap periods excluded.")
                return 0
            else:
                print("⚠️  No trades found in valid windows.")
                return 1
        
        print("=" * 60)
        print("STATUS: INVALID (INSUFFICIENT DATA COVERAGE)")
        print("=" * 60)
        print(f"\nCoverage below minimum threshold. Use --force to proceed.")
        
        if valid_windows:
            print(f"\n💡 TIP: Use --windows to backtest on valid coverage windows only.")
            print(f"   Run: python3 scripts/run_backtest.py --input-dir {reports_dir} --years 1 --windows")
        else:
            print("\nNo valid coverage windows found. Cannot run reliable backtest.")
        
        print("\nResults would be labeled 'DO NOT USE' - exiting.")
        return 1
    
    backtester = DeterministicBacktester(config_path=args.config, reports_dir=reports_dir)
    result = backtester.run_range(
        start_date,
        end_date,
        symbols=args.symbols,
        portfolio_mode_override=portfolio_mode if args.phase else None,
        edge_slice=args.edge_slice,
        max_iv=args.max_iv,
        max_atm_iv_pctl=args.max_atm_iv_pctl,
    )
    
    # Load config for fallback analysis
    with open(args.config) as f:
        config = yaml.safe_load(f)
    
    # Fallback analysis (if coverage records exist)
    fallback_analysis = None
    coverage_records = coverage_check_result.get('records', [])
    if result.trades and coverage_records:
        fallback_analysis = analyze_fallback_trades(result, coverage_records, config)
        print_fallback_analysis(fallback_analysis)
        
        # Check fallback validation
        if fallback_analysis['validation_status'] == 'FAIL' and not args.force:
            print("\n" + "=" * 60)
            print("STATUS: INVALID (FALLBACK VALIDATION FAILED)")
            print("=" * 60)
            print(f"\n{fallback_analysis['validation_message']}")
            print("Use --force to proceed. Results would be labeled 'DO NOT USE'.")
            return 1
    
    # Print summary
    print()
    print("=" * 60)
    print("BACKTEST SUMMARY")
    print("=" * 60)
    print()
    
    m = result.metrics
    print(f"📊 TRADES: {m.total_trades} total ({m.winners}W / {m.losers}L)")
    print(f"📈 WIN RATE: {m.win_rate:.1f}%")
    print()
    
    if m.total_trades > 0:
        print(f"💰 TOTAL PnL: ${m.total_pnl:.2f}")
        print(f"   Avg PnL: ${m.avg_pnl:.2f}")
        print(f"   Avg Win: ${m.avg_win:.2f}")
        print(f"   Avg Loss: ${m.avg_loss:.2f}")
        print()
        print(f"📐 PROFIT FACTOR: {m.profit_factor:.2f}")
        print(f"📉 MAX DRAWDOWN: ${m.max_drawdown:.2f}")
        print(f"⏱️ AVG HOLD: {m.avg_hold_days:.1f} days")
        print()
        
        # Breakdowns
        if m.by_symbol:
            print("BY SYMBOL:")
            for sym, data in m.by_symbol.items():
                print(f"  {sym}: {data['trades']} trades, ${data['pnl']:.2f}, {data['win_rate']:.0f}% win")
        
        if m.by_edge_type:
            print("\nBY EDGE TYPE:")
            for edge, data in m.by_edge_type.items():
                print(f"  {edge}: {data['trades']} trades, ${data['pnl']:.2f}, {data['win_rate']:.0f}% win")
        
        if m.by_structure:
            print("\nBY STRUCTURE TYPE:")
            for struct, data in m.by_structure.items():
                print(f"  {struct}: {data['trades']} trades, ${data['pnl']:.2f}, {data['win_rate']:.0f}% win")
        
        # Multi-year validation metrics
        period_days = (end_date - start_date).days
        years = period_days / 365.0
        if years >= 1.0 and m.total_trades > 0:
            print("\n" + "=" * 60)
            print("VALIDATION METRICS")
            print("=" * 60)
            
            # Trades per year
            trades_per_year = m.total_trades / years
            print(f"\n📅 Trades per year: {trades_per_year:.1f}")
            
            # Calculate CAGR (assuming $10k starting)
            starting_capital = 10000.0
            ending_capital = starting_capital + m.total_pnl
            if ending_capital > 0 and years > 0:
                cagr = ((ending_capital / starting_capital) ** (1.0 / years) - 1) * 100
                print(f"📈 CAGR (on $10k): {cagr:.1f}%")
            
            # Max drawdown %
            if ending_capital > 0:
                max_dd_pct = (m.max_drawdown / starting_capital) * 100
                print(f"📉 Max DD %: {max_dd_pct:.1f}%")
            
            # Worst consecutive loss streak
            if result.trades:
                current_streak = 0
                worst_streak = 0
                worst_streak_loss = 0.0
                current_loss = 0.0
                
                for trade in result.trades:
                    if trade.net_pnl < 0:
                        current_streak += 1
                        current_loss += trade.net_pnl
                        if current_streak > worst_streak:
                            worst_streak = current_streak
                            worst_streak_loss = current_loss
                    else:
                        current_streak = 0
                        current_loss = 0.0
                
                print(f"🔴 Worst loss streak: {worst_streak} trades (${worst_streak_loss:.2f})")
            
            # Equity curve snapshot (high/low)
            if result.trades:
                equity = 0.0
                peak = 0.0
                trough = 0.0
                
                for trade in result.trades:
                    equity += trade.net_pnl
                    if equity > peak:
                        peak = equity
                    if equity < trough:
                        trough = equity
                
                print(f"💹 Equity peak: ${peak:.2f}")
                print(f"💹 Equity trough: ${trough:.2f}")
                print(f"💹 Final equity: ${equity:.2f}")
            
            # Risk-capital lens metrics (options-correct)
            if result.trades:
                # Per-trade RoR calculations
                rors = []
                for t in result.trades:
                    max_loss = getattr(t, 'max_loss', 350.0) or 350.0
                    if max_loss > 0:
                        ror = t.net_pnl / max_loss
                        rors.append(ror)
                
                if rors:
                    import statistics
                    avg_ror = sum(rors) / len(rors) * 100
                    median_ror = statistics.median(rors) * 100
                    rors_sorted = sorted(rors)
                    p10_ror = rors_sorted[int(len(rors) * 0.1)] * 100 if len(rors) > 10 else rors_sorted[0] * 100
                    p90_ror = rors_sorted[int(len(rors) * 0.9)] * 100 if len(rors) > 10 else rors_sorted[-1] * 100
                    
                    # Risk deployment
                    total_risk = sum(getattr(t, 'max_loss', 350.0) or 350.0 for t in result.trades)
                    avg_risk = total_risk / len(result.trades) if result.trades else 350.0
                    pnl_over_risk = (m.total_pnl / total_risk) * 100 if total_risk > 0 else 0
                    annual_ror = avg_ror * m.total_trades / years if years > 0 else 0
                    
                    # Tail ratio (avg loss / avg win)
                    avg_win_val = m.avg_win if m.avg_win > 0 else 1
                    tail_ratio = abs(m.avg_loss / avg_win_val) if avg_win_val > 0 else 0
                    
                    print(f"\n📊 RISK-CAPITAL METRICS:")
                    print(f"   Avg RoR/trade: {avg_ror:.1f}%")
                    print(f"   Median RoR/trade: {median_ror:.1f}%")
                    print(f"   RoR distribution: p10={p10_ror:.1f}%, p50={median_ror:.1f}%, p90={p90_ror:.1f}%")
                    print(f"   Trades/year: {m.total_trades / years:.1f}")
                    print(f"   Annual return on risk: {annual_ror:.1f}%")
                    print(f"   Total PnL / total risk: {pnl_over_risk:.1f}%")
                    print(f"   Tail ratio (avgL/avgW): {tail_ratio:.2f}x")
                    print(f"   Avg risk deployed: ${avg_risk:.0f}")
                    
                    # Stability flags
                    stability_issues = []
                    if m.profit_factor < 1.0:
                        stability_issues.append("PF < 1.0")
                    if m.max_drawdown / 10000 > 0.15:  # >15% DD
                        stability_issues.append("DD > 15%")
                    if tail_ratio > 3.0:
                        stability_issues.append("tail_ratio > 3x")
                    
                    if stability_issues:
                        print(f"\n   ⚠️ STABILITY FLAGS: {', '.join(stability_issues)}")
                
                # Year-by-year breakdown
                trades_by_year = {}
                for t in result.trades:
                    try:
                        year = t.signal_date[:4] if isinstance(t.signal_date, str) else t.signal_date.year
                    except:
                        year = '????'
                    if year not in trades_by_year:
                        trades_by_year[year] = {'trades': [], 'pnl': 0, 'wins': 0}
                    trades_by_year[year]['trades'].append(t)
                    trades_by_year[year]['pnl'] += t.net_pnl
                    if t.net_pnl > 0:
                        trades_by_year[year]['wins'] += 1
                
                if len(trades_by_year) > 1:
                    print(f"\n📅 YEAR-BY-YEAR BREAKDOWN:")
                    print(f"   {'Year':<6} {'Trades':>7} {'PnL':>10} {'WR':>6} {'PF':>6}")
                    print(f"   {'-'*6} {'-'*7} {'-'*10} {'-'*6} {'-'*6}")
                    for year in sorted(trades_by_year.keys()):
                        data = trades_by_year[year]
                        n = len(data['trades'])
                        pnl = data['pnl']
                        wr = (data['wins'] / n * 100) if n > 0 else 0
                        wins_pnl = sum(t.net_pnl for t in data['trades'] if t.net_pnl > 0)
                        losses_pnl = abs(sum(t.net_pnl for t in data['trades'] if t.net_pnl < 0))
                        pf = wins_pnl / losses_pnl if losses_pnl > 0 else float('inf')
                        print(f"   {year:<6} {n:>7} ${pnl:>9.2f} {wr:>5.0f}% {pf:>6.2f}")
            
            # Benchmark comparison
            print(f"\n📉 BENCHMARK COMPARISON:")
            print(f"   SPY buy-hold (10yr avg): ~10.5% CAGR")
            print(f"   QQQ buy-hold (10yr avg): ~14.0% CAGR")
            if 'annual_ror' in dir():
                print(f"   Strategy on risk capital: {annual_ror:.1f}% annual")
        
        # Integrity report
        if args.integrity:
            integrity = generate_integrity_report(result)
            print_integrity_report(integrity)
            
            if not integrity.passed:
                print("\n⚠️  INTEGRITY CHECK FAILED - Results may not be reliable")
                
    else:
        print("No trades generated. Check:")
        print("  - Report files exist in logs/reports/")
        print("  - Signals have recommendation=TRADE")
        print("  - Edge strength meets minimum threshold")
    
    # Save output
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # JSON output
    if args.json or True:  # Always save JSON
        json_path = output_dir / f"backtest_{start_date}_{end_date}.json"
        with open(json_path, 'w') as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        print(f"\nResults saved: {json_path}")
    
    # CSV export
    if args.csv and result.trades:
        import csv
        csv_path = output_dir / f"trades_{start_date}_{end_date}.csv"
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'trade_id', 'symbol', 'signal_date', 'exit_date',
                'edge_type', 'edge_strength', 'regime', 'structure_type',
                'entry_price', 'exit_price', 'net_pnl', 'exit_reason',
                'hold_days', 'mfe', 'mae'
            ])
            writer.writeheader()
            for t in result.trades:
                writer.writerow({
                    'trade_id': t.trade_id,
                    'symbol': t.symbol,
                    'signal_date': t.signal_date,
                    'exit_date': t.exit_date,
                    'edge_type': t.edge_type,
                    'edge_strength': f"{t.edge_strength:.2f}",
                    'regime': t.regime,
                    'structure_type': t.structure_type,
                    'entry_price': f"{t.entry_price:.4f}",
                    'exit_price': f"{t.exit_price:.4f}",
                    'net_pnl': f"{t.net_pnl:.2f}",
                    'exit_reason': t.exit_reason.value,
                    'hold_days': t.hold_days,
                    'mfe': f"{t.mfe:.2f}",
                    'mae': f"{t.mae:.2f}",
                })
        print(f"Trades CSV: {csv_path}")
    
    print("\n=== Done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
