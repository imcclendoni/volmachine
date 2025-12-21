#!/usr/bin/env python3
"""
Generate Daily FLAT Signals

Produces a JSON artifact for dashboard integration.
Reads signal reports for a target date and outputs qualifying FLAT candidates.

Usage:
    python3 scripts/generate_daily_signals.py --edge flat --date 2025-12-20
    python3 scripts/generate_daily_signals.py --edge flat  # Uses today's date
"""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def load_config() -> Dict[str, Any]:
    """Load backtest config for universe and gate settings."""
    import yaml
    config_path = Path(__file__).parent.parent / "config" / "backtest.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_flat_universe(config: Dict[str, Any]) -> List[str]:
    """Get the FLAT production universe from config."""
    return config.get('strategies', {}).get('skew_extreme', {}).get('enabled_symbols', [])


def get_ivp_gate(config: Dict[str, Any]) -> Optional[float]:
    """Get the IVp gate for FLAT from config."""
    return config.get('regime_gate', {}).get('flat', {}).get('max_atm_iv_pctl')


def find_reports_for_date(reports_dir: Path, target_date: date) -> List[Path]:
    """
    Find ALL signal reports for a given date.
    
    Report filename format: YYYY-MM-DD__SYMBOL__backfill.json
    The date in filename is execution_date (N+1), report_date is N.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    
    # Match format: YYYY-MM-DD__*__backfill.json
    pattern = f"{date_str}__*__backfill.json"
    matches = list(reports_dir.glob(pattern))
    
    # Also scan all files and match by report_date field
    if not matches:
        for f in reports_dir.glob("*__backfill.json"):
            try:
                with open(f) as fp:
                    report = json.load(fp)
                if report.get('report_date') == date_str or report.get('execution_date') == date_str:
                    matches.append(f)
            except:
                continue
    
    return matches


def extract_flat_candidates(
    report: Dict[str, Any],
    universe: List[str],
    ivp_gate: Optional[float]
) -> List[Dict[str, Any]]:
    """Extract FLAT candidates from a report that pass all gates."""
    candidates = []
    
    for cand in report.get('candidates', []):
        symbol = cand.get('symbol')
        if symbol not in universe:
            continue
        
        # Check is_flat
        metrics = cand.get('edge', {}).get('metrics', {})
        is_flat = metrics.get('is_flat', 0)
        if is_flat != 1.0:
            continue
        
        # Check IVp gate
        atm_iv_pctl = metrics.get('atm_iv_percentile')
        if atm_iv_pctl is None or atm_iv_pctl <= 0 or atm_iv_pctl > 100:
            continue  # Invalid IVp
        if ivp_gate and atm_iv_pctl > ivp_gate:
            continue  # Exceeds gate
        
        # Check recommendation
        if cand.get('recommendation') != 'TRADE':
            continue
        
        # Extract structure info (correct field mappings)
        structure = cand.get('structure', {})
        legs = structure.get('legs', [])
        
        # Get expiry from first leg (not top-level)
        expiry = legs[0].get('expiry') if legs else None
        
        # Build candidate summary
        candidates.append({
            'symbol': symbol,
            'report_date': report.get('report_date'),
            'execution_date': report.get('execution_date'),
            'skew_percentile': metrics.get('skew_percentile'),
            'atm_iv_percentile': atm_iv_pctl,
            'structure': {
                'type': structure.get('type'),  # Correct field name
                'expiry': expiry,
                'entry_debit': structure.get('entry_debit'),
                'max_loss': structure.get('max_loss_dollars'),
                'max_profit': structure.get('max_profit_dollars'),
                'legs': [
                    {
                        'strike': leg.get('strike'),
                        'right': leg.get('right'),
                        'side': leg.get('side'),
                        'expiry': leg.get('expiry')
                    }
                    for leg in legs
                ]
            },
            'edge_strength': cand.get('edge', {}).get('strength'),
            'rationale': f"FLAT signal: skew at {metrics.get('skew_percentile', 0):.0f}th pctl, reverting. IVp {atm_iv_pctl:.0f} (gate: {ivp_gate})"
        })
    
    return candidates


def generate_signals(
    edge: str,
    target_date: date,
    reports_dir: Path,
    output_dir: Path
) -> Dict[str, Any]:
    """Generate signals JSON for a given edge and date."""
    
    if edge != 'flat':
        raise ValueError(f"Unsupported edge: {edge}. Only 'flat' is currently supported.")
    
    config = load_config()
    universe = get_flat_universe(config)
    ivp_gate = get_ivp_gate(config)
    
    # Find ALL reports for this date
    report_paths = find_reports_for_date(reports_dir, target_date)
    
    # Extract candidates from all reports
    candidates = []
    reports_processed = 0
    for report_path in report_paths:
        try:
            with open(report_path) as f:
                report = json.load(f)
            reports_processed += 1
            candidates.extend(extract_flat_candidates(report, universe, ivp_gate))
        except Exception as e:
            print(f"  Warning: Failed to process {report_path}: {e}")
    
    # Build output (dashboard contract schema)
    output = {
        'edge_id': edge,
        'edge_version': 'v1.0',
        'generated_at': datetime.now().isoformat(),
        'report_date': None,  # Will be set from first candidate
        'execution_date': target_date.isoformat(),
        'universe': universe,
        'regime_gate': {
            'max_atm_iv_pctl': ivp_gate
        },
        'reports_found': len(report_paths),
        'reports_processed': reports_processed,
        'candidates': candidates,
        'candidate_count': len(candidates)
    }
    
    # Set report_date from first candidate if available
    if candidates:
        output['report_date'] = candidates[0].get('report_date')
    
    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "latest_signals.json"
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    # Also write dated version
    dated_path = output_dir / f"signals_{target_date.isoformat()}.json"
    with open(dated_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"Generated {len(candidates)} FLAT signals for {target_date}")
    print(f"  Reports processed: {reports_processed}")
    print(f"  Output: {output_path}")
    
    return output


def main():
    parser = argparse.ArgumentParser(description="Generate daily edge signals")
    parser.add_argument('--edge', required=True, choices=['flat'], 
                        help="Edge type (only 'flat' supported)")
    parser.add_argument('--date', type=str, default=None,
                        help="Target date (YYYY-MM-DD), defaults to today")
    parser.add_argument('--reports-dir', type=str, default='logs/backfill/v7/reports',
                        help="Reports directory")
    parser.add_argument('--output-dir', type=str, default=None,
                        help="Output directory (defaults to logs/edges/{edge})")
    
    args = parser.parse_args()
    
    # Parse date
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        target_date = date.today()
    
    # Set paths
    reports_dir = Path(args.reports_dir)
    output_dir = Path(args.output_dir) if args.output_dir else Path(f'logs/edges/{args.edge}')
    
    # Generate
    result = generate_signals(args.edge, target_date, reports_dir, output_dir)
    
    # Print summary
    print(f"\n=== FLAT Signal Summary for {target_date} ===")
    print(f"Universe: {len(result['universe'])} symbols")
    ivp_gate = result.get('regime_gate', {}).get('max_atm_iv_pctl', 'N/A')
    print(f"IVp Gate: <= {ivp_gate}")
    print(f"Candidates found: {result['candidate_count']}")
    
    if result['candidates']:
        print("\nCandidates:")
        for c in result['candidates']:
            debit = c['structure'].get('entry_debit', 0) or 0
            print(f"  {c['symbol']}: IVp={c['atm_iv_percentile']:.0f}, "
                  f"skew_pctl={c['skew_percentile']:.0f}, "
                  f"debit=${debit:.2f}")


if __name__ == '__main__':
    main()
