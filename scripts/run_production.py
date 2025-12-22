#!/usr/bin/env python3
"""
Production Runner

Runs all edges for the effective trading date (derived from data watermark).
Creates auditable run output with UUID, timestamps, and diagnostic fields.

Usage:
    python scripts/run_production.py
    python scripts/run_production.py --dry-run
"""

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.data_watermark import get_data_watermark, get_symbols_with_data


def get_git_sha() -> str:
    """Get current git commit SHA."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True, cwd=Path(__file__).parent.parent
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except:
        return "unknown"


def load_flat_config() -> Dict[str, Any]:
    """Load FLAT edge configuration."""
    import yaml
    config_path = Path(__file__).parent.parent / "config" / "backtest.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return {
        'universe': config.get('strategies', {}).get('skew_extreme', {}).get('enabled_symbols', []),
        'ivp_gate': config.get('regime_gate', {}).get('flat', {}).get('max_atm_iv_pctl', 75)
    }


def get_gate_samples(effective_date: date, universe: List[str]) -> List[Dict[str, Any]]:
    """
    Get gate pass/fail samples for top symbols.
    
    This is a placeholder - in production, this would query the signal detector
    to get actual IVp values for each symbol.
    """
    # For now, return placeholder indicating we need real IVp computation
    top_symbols = ['SPY', 'QQQ', 'DIA', 'IWM', 'XLF'][:5]
    samples = []
    for sym in top_symbols:
        if sym in universe:
            samples.append({
                'symbol': sym,
                'ivp': None,  # Would be computed from OptionBarStore
                'gate_pass': None,
                'note': 'IVp computation requires OptionBarStore load'
            })
    return samples


def run_edge_signals(
    edge: str,
    effective_date: date,
    output_dir: Path
) -> Dict[str, Any]:
    """
    Run signal generation for a specific edge.
    
    NOTE: Currently reads from pre-computed backfill reports.
    For "live" signals, you must first run the backfill up to effective_date:
      - FLAT: python scripts/backfill_signals.py --end-date {effective_date}
      - IV Carry MR: python scripts/backfill_iv_carry_signals.py --end-date {effective_date}
    
    Returns the generated signals JSON.
    """
    from scripts.generate_daily_signals import generate_signals
    
    if edge == 'flat':
        reports_dir = Path('logs/backfill/v7/reports')
    elif edge == 'iv_carry_mr':
        reports_dir = Path('logs/backfill/iv_carry_mr/reports')
    else:
        raise ValueError(f"Unknown edge: {edge}")
    
    result = generate_signals(edge, effective_date, reports_dir, output_dir)
    
    # Add diagnostic info
    print(f"  Reports dir: {reports_dir}")
    print(f"  Reports found: {result.get('reports_found', 0)}")
    print(f"  Reports processed: {result.get('reports_processed', 0)}")
    print(f"  Candidates: {result.get('candidate_count', 0)}")
    
    if result.get('reports_found', 0) == 0:
        signal_date = effective_date - timedelta(days=1)
        print(f"  ⚠️ No reports for signal_date {signal_date}")
        print(f"     Run backfill to generate: backfill_*_signals.py --end-date {effective_date}")
    
    return result


def run_production(dry_run: bool = False) -> Dict[str, Any]:
    """
    Execute production run for all edges.
    
    Returns run metadata and results.
    """
    project_root = Path(__file__).parent.parent
    flatfiles_dir = project_root / "cache" / "flatfiles"
    
    # 1. Compute data watermark
    watermark = get_data_watermark(flatfiles_dir)
    
    print("=" * 60)
    print("PRODUCTION RUN")
    print("=" * 60)
    print(f"  Reference date: {date.today()}")
    print(f"  Data max date: {watermark.data_max_date}")
    print(f"  Effective date: {watermark.effective_date}")
    print(f"  Is stale: {watermark.is_stale}")
    if watermark.stale_reason:
        print(f"  Stale reason: {watermark.stale_reason}")
    print()
    
    # 2. Generate run metadata
    run_id = str(uuid.uuid4())[:8]
    run_ts_utc = datetime.utcnow().isoformat() + "Z"
    git_sha = get_git_sha()
    
    # 3. Create run directory
    run_date_str = date.today().isoformat()
    run_dir = project_root / "logs" / "runs" / run_date_str / f"run_{run_id}"
    
    if not dry_run:
        run_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Run ID: {run_id}")
    print(f"Run dir: {run_dir}")
    print()
    
    # 4. Build data snapshot
    symbols_with_data = get_symbols_with_data(flatfiles_dir, watermark.effective_date)
    
    data_snapshot = {
        'flatfiles_dir': str(flatfiles_dir),
        'effective_date': watermark.effective_date.isoformat(),
        'files_found': watermark.files_found,
        'symbols_with_data': len(symbols_with_data),
        'symbol_list': symbols_with_data[:20],  # First 20 for brevity
    }
    
    # 5. Get gate samples (placeholder)
    flat_config = load_flat_config()
    gate_samples = get_gate_samples(watermark.effective_date, flat_config['universe'])
    
    # 6. Determine trading_allowed
    trading_allowed = not watermark.is_stale
    do_not_trade_reasons = []
    if watermark.is_stale:
        do_not_trade_reasons.append(f"DATA_STALE: {watermark.stale_reason}")
    
    # 7. Run edges
    edges_results = []
    all_candidates = []
    
    for edge in ['flat', 'iv_carry_mr']:
        print(f"--- Running {edge.upper()} ---")
        
        if dry_run:
            print(f"  [DRY RUN] Would generate signals for {edge}")
            edges_results.append({
                'edge_id': edge,
                'candidate_count': 0,
                'status': 'dry_run'
            })
        else:
            try:
                edge_output_dir = run_dir / edge
                edge_output_dir.mkdir(parents=True, exist_ok=True)
                
                result = run_edge_signals(edge, watermark.effective_date, edge_output_dir)
                
                edges_results.append({
                    'edge_id': edge,
                    'candidate_count': result.get('candidate_count', 0),
                    'status': 'success'
                })
                all_candidates.extend(result.get('candidates', []))
                
                # Copy to edge-specific latest
                edge_latest_dir = project_root / "logs" / "edges" / edge
                edge_latest_dir.mkdir(parents=True, exist_ok=True)
                latest_path = edge_latest_dir / "latest_signals.json"
                with open(latest_path, 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                print(f"  Updated: {latest_path}")
                
            except Exception as e:
                print(f"  ERROR: {e}")
                edges_results.append({
                    'edge_id': edge,
                    'candidate_count': 0,
                    'status': f'error: {e}'
                })
    
    # 8. Build run summary
    run_summary = {
        'run_id': run_id,
        'run_ts_utc': run_ts_utc,
        'engine_version': 'v1.0',
        'git_sha': git_sha,
        'effective_date': watermark.effective_date.isoformat(),
        'data_max_date': watermark.data_max_date.isoformat(),
        'is_stale': watermark.is_stale,
        'stale_reason': watermark.stale_reason,
        'trading_allowed': trading_allowed,
        'do_not_trade_reasons': do_not_trade_reasons,
        'data_snapshot': data_snapshot,
        'gate_samples': gate_samples,
        'edges': edges_results,
        'total_candidates': len(all_candidates),
        'candidates': all_candidates,
    }
    
    # 9. Write outputs
    if not dry_run:
        # Write run_meta.json
        meta_path = run_dir / "run_meta.json"
        with open(meta_path, 'w') as f:
            json.dump(run_summary, f, indent=2, default=str)
        print(f"\nWritten: {meta_path}")
        
        # Update logs/reports/latest.json
        reports_dir = project_root / "logs" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        latest_report = reports_dir / "latest.json"
        with open(latest_report, 'w') as f:
            json.dump(run_summary, f, indent=2, default=str)
        print(f"Updated: {latest_report}")
    
    # 10. Print summary
    print()
    print("=" * 60)
    print("RUN SUMMARY")
    print("=" * 60)
    print(f"  Run ID: {run_id}")
    print(f"  Effective date: {watermark.effective_date}")
    print(f"  Trading allowed: {trading_allowed}")
    print(f"  Total candidates: {len(all_candidates)}")
    for edge_result in edges_results:
        print(f"    {edge_result['edge_id']}: {edge_result['candidate_count']} candidates")
    
    return run_summary


def main():
    parser = argparse.ArgumentParser(description="Run production signal generation")
    parser.add_argument('--dry-run', action='store_true', help="Don't write outputs")
    args = parser.parse_args()
    
    run_production(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
