#!/usr/bin/env python3
"""
Reconciliation Check for Backtest.

Verifies that backfill produces identical signals to live engine.
Must pass before interpreting backtest P&L results.
"""

import argparse
import sys
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def load_live_report(report_date: date, reports_dir: Path) -> Optional[Dict]:
    """Load live engine report for a date."""
    patterns = [
        reports_dir / f'{report_date.isoformat()}.json',
        reports_dir / f'{report_date.isoformat()}_open.json',
        reports_dir / f'{report_date.isoformat()}_close.json',
    ]
    
    for path in patterns:
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except:
                pass
    
    return None


def load_backfill_report(report_date: date, reports_dir: Path) -> Optional[Dict]:
    """Load backfilled report for a date."""
    path = reports_dir / f'{report_date.isoformat()}_backfill.json'
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except:
            pass
    return None


def compare_candidates(live_candidates: List[Dict], backfill_candidates: List[Dict]) -> Dict:
    """
    Compare candidates from live and backfill reports.
    
    Returns comparison result with matches and mismatches.
    """
    result = {
        'matches': [],
        'live_only': [],
        'backfill_only': [],
        'field_mismatches': [],
    }
    
    # Build lookup by symbol
    live_by_symbol = {c.get('symbol', ''): c for c in live_candidates}
    backfill_by_symbol = {c.get('symbol', ''): c for c in backfill_candidates}
    
    all_symbols = set(live_by_symbol.keys()) | set(backfill_by_symbol.keys())
    
    for symbol in all_symbols:
        live = live_by_symbol.get(symbol)
        backfill = backfill_by_symbol.get(symbol)
        
        if live and not backfill:
            result['live_only'].append({
                'symbol': symbol,
                'live_edge': live.get('edge', {}).get('type', ''),
                'live_direction': live.get('edge', {}).get('direction', ''),
            })
        elif backfill and not live:
            result['backfill_only'].append({
                'symbol': symbol,
                'backfill_edge': backfill.get('edge', {}).get('type', ''),
                'backfill_direction': backfill.get('edge', {}).get('direction', ''),
            })
        else:
            # Both exist - compare fields
            mismatches = []
            
            live_edge = live.get('edge', {})
            backfill_edge = backfill.get('edge', {})
            
            # Compare key fields
            fields_to_compare = [
                ('edge_type', live_edge.get('type', ''), backfill_edge.get('type', '')),
                ('direction', live_edge.get('direction', ''), backfill_edge.get('direction', '')),
            ]
            
            live_structure = live.get('structure', {})
            backfill_structure = backfill.get('structure', {})
            
            fields_to_compare.extend([
                ('structure_type', live_structure.get('type', ''), backfill_structure.get('type', '')),
                ('spread_type', live_structure.get('spread_type', ''), backfill_structure.get('spread_type', '')),
            ])
            
            # Compare legs (strikes only)
            live_legs = live_structure.get('legs', [])
            backfill_legs = backfill_structure.get('legs', [])
            
            live_strikes = sorted([l.get('strike', 0) for l in live_legs])
            backfill_strikes = sorted([l.get('strike', 0) for l in backfill_legs])
            
            if live_strikes != backfill_strikes:
                mismatches.append({
                    'field': 'strikes',
                    'live': live_strikes,
                    'backfill': backfill_strikes,
                })
            
            # Check expiration
            live_expiry = live_legs[0].get('expiry', '') if live_legs else ''
            backfill_expiry = backfill_legs[0].get('expiry', '') if backfill_legs else ''
            
            if live_expiry != backfill_expiry:
                mismatches.append({
                    'field': 'expiration',
                    'live': live_expiry,
                    'backfill': backfill_expiry,
                })
            
            # Check core fields
            for field, live_val, backfill_val in fields_to_compare:
                if live_val != backfill_val:
                    mismatches.append({
                        'field': field,
                        'live': live_val,
                        'backfill': backfill_val,
                    })
            
            if mismatches:
                result['field_mismatches'].append({
                    'symbol': symbol,
                    'mismatches': mismatches,
                })
            else:
                result['matches'].append({
                    'symbol': symbol,
                    'edge_type': live_edge.get('type', ''),
                    'direction': live_edge.get('direction', ''),
                    'structure_type': live_structure.get('type', ''),
                })
    
    return result


def run_reconciliation(
    dates: List[date],
    reports_dir: Path,
) -> Dict:
    """
    Run reconciliation check for given dates.
    
    Returns overall result with all comparisons.
    """
    results = {
        'passed': True,
        'dates_checked': len(dates),
        'dates_with_signals': 0,
        'total_matches': 0,
        'total_mismatches': 0,
        'details': [],
    }
    
    for check_date in dates:
        live_report = load_live_report(check_date, reports_dir)
        backfill_report = load_backfill_report(check_date, reports_dir)
        
        date_result = {
            'date': check_date.isoformat(),
            'live_found': live_report is not None,
            'backfill_found': backfill_report is not None,
            'comparison': None,
        }
        
        if not live_report:
            date_result['status'] = 'skipped_no_live'
        elif not backfill_report:
            date_result['status'] = 'skipped_no_backfill'
        else:
            live_candidates = live_report.get('candidates', [])
            backfill_candidates = backfill_report.get('candidates', [])
            
            if not live_candidates and not backfill_candidates:
                date_result['status'] = 'no_signals_both'
            else:
                results['dates_with_signals'] += 1
                comparison = compare_candidates(live_candidates, backfill_candidates)
                date_result['comparison'] = comparison
                
                results['total_matches'] += len(comparison['matches'])
                
                has_issues = (
                    len(comparison['live_only']) > 0 or
                    len(comparison['backfill_only']) > 0 or
                    len(comparison['field_mismatches']) > 0
                )
                
                if has_issues:
                    results['passed'] = False
                    results['total_mismatches'] += (
                        len(comparison['live_only']) +
                        len(comparison['backfill_only']) +
                        len(comparison['field_mismatches'])
                    )
                    date_result['status'] = 'mismatch'
                else:
                    date_result['status'] = 'match'
        
        results['details'].append(date_result)
    
    return results


def print_reconciliation_results(results: Dict):
    """Print formatted reconciliation results."""
    print()
    print("=" * 60)
    print("RECONCILIATION CHECK")
    print("=" * 60)
    print()
    
    status = "✅ PASSED" if results['passed'] else "❌ FAILED"
    print(f"Status: {status}")
    print(f"Dates checked: {results['dates_checked']}")
    print(f"Dates with signals: {results['dates_with_signals']}")
    print(f"Total matches: {results['total_matches']}")
    print(f"Total mismatches: {results['total_mismatches']}")
    print()
    
    for detail in results['details']:
        date_str = detail['date']
        status = detail['status']
        
        if status == 'match':
            print(f"  {date_str}: ✅ MATCH")
            if detail.get('comparison'):
                for m in detail['comparison']['matches']:
                    print(f"    {m['symbol']}: {m['edge_type']}, {m['direction']}, {m['structure_type']}")
        
        elif status == 'mismatch':
            print(f"  {date_str}: ❌ MISMATCH")
            comp = detail['comparison']
            
            if comp['live_only']:
                for item in comp['live_only']:
                    print(f"    {item['symbol']}: live only - {item['live_edge']}, {item['live_direction']}")
            
            if comp['backfill_only']:
                for item in comp['backfill_only']:
                    print(f"    {item['symbol']}: backfill only - {item['backfill_edge']}, {item['backfill_direction']}")
            
            if comp['field_mismatches']:
                for item in comp['field_mismatches']:
                    print(f"    {item['symbol']}: field differences")
                    for mm in item['mismatches']:
                        print(f"      {mm['field']}: live={mm['live']} vs backfill={mm['backfill']}")
        
        elif status == 'skipped_no_live':
            print(f"  {date_str}: ⚪ skipped (no live report)")
        
        elif status == 'skipped_no_backfill':
            print(f"  {date_str}: ⚪ skipped (no backfill report)")
        
        else:
            print(f"  {date_str}: ⚪ no signals")
    
    print()
    print("=" * 60)
    
    if not results['passed']:
        print()
        print("⚠️  RECONCILIATION FAILED")
        print("Backfill signals do not match live engine output.")
        print("Do NOT interpret backtest P&L until this is fixed.")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Verify backfill produces identical signals to live engine"
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        type=str,
        default=None,
        help="Specific dates to check (YYYY-MM-DD format)"
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=5,
        help="Check most recent N dates with live reports (default: 5)"
    )
    parser.add_argument(
        "--reports-dir",
        default="./logs/reports",
        help="Reports directory"
    )
    
    args = parser.parse_args()
    reports_dir = Path(args.reports_dir)
    
    if args.dates:
        dates = [date.fromisoformat(d) for d in args.dates]
    else:
        # Find most recent dates with live reports
        dates = []
        current = date.today()
        while len(dates) < args.recent and current > date.today() - timedelta(days=90):
            live = load_live_report(current, reports_dir)
            if live:
                dates.append(current)
            current -= timedelta(days=1)
    
    if not dates:
        print("No dates to check. Run live engine to create reports first.")
        return 1
    
    print(f"Checking reconciliation for {len(dates)} dates...")
    results = run_reconciliation(dates, reports_dir)
    print_reconciliation_results(results)
    
    return 0 if results['passed'] else 1


if __name__ == "__main__":
    sys.exit(main())
