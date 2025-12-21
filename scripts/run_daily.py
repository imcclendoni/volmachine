#!/usr/bin/env python3
"""
Run Daily Analysis - PRODUCTION EDGES ONLY

Main entry point for daily signal generation.
Only runs production-ready, locked edges (currently: FLAT v1).

Usage:
    python3 scripts/run_daily.py
    python3 scripts/run_daily.py --date 2025-12-21
"""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_flat_v1_signals(run_date: date, output_dir: Path) -> dict:
    """
    Run FLAT v1 signal generator.
    
    Returns signal summary for dashboard display.
    """
    from scripts.generate_daily_signals import generate_signals
    
    reports_dir = Path("logs/backfill/v7/reports")
    edge_output_dir = Path("logs/edges/flat")
    
    print(f"[FLAT v1] Generating signals for {run_date}...")
    
    result = generate_signals(
        edge="flat",
        target_date=run_date,
        reports_dir=reports_dir,
        output_dir=edge_output_dir
    )
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run daily production edge analysis (FLAT v1 only)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to run for (YYYY-MM-DD, default: today)"
    )
    parser.add_argument(
        "--output",
        default="./logs/reports",
        help="Output directory for reports"
    )
    
    args = parser.parse_args()
    
    # Parse date
    run_date = date.today()
    if args.date:
        run_date = date.fromisoformat(args.date)
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print(f"  VOLMACHINE PRODUCTION RUN: {run_date}")
    print("  Active Edges: FLAT v1")
    print("=" * 60)
    print()
    
    # Run FLAT v1 signals
    flat_result = run_flat_v1_signals(run_date, output_dir)
    
    # Build combined report for dashboard
    report = {
        "report_date": run_date.isoformat(),
        "generated_at": datetime.now().isoformat(),
        "trading_allowed": True,
        "do_not_trade_reasons": [],
        "provider_status": {
            "connected": True,
            "source": "FlatfilesProvider",
            "last_run": datetime.now().isoformat()
        },
        "universe_scan": {
            "symbols_scanned": len(flat_result.get('universe', [])),
            "symbols_with_data": len(flat_result.get('universe', [])),
            "symbols_with_edges": flat_result.get('candidate_count', 0),
            "symbols_with_trades": flat_result.get('candidate_count', 0),
            "symbol_list": flat_result.get('universe', [])
        },
        "vrp_metrics": [],
        "regime": {
            "state": "production",
            "confidence": 1.0,
            "rationale": "FLAT v1 locked production mode - IVp≤75 gate active"
        },
        "edges": [
            {
                "edge_id": "flat",
                "edge_version": "v1.0",
                "candidate_count": flat_result.get('candidate_count', 0)
            }
        ],
        "candidates": flat_result.get('candidates', []),
        "portfolio": {
            "positions_open": 0,
            "total_max_loss_dollars": 0.0,
            "realized_pnl_today_dollars": 0.0,
            "unrealized_pnl_dollars": 0.0,
            "kill_switch_active": False,
            "kill_switch_reason": None
        }
    }
    
    # Print summary
    print()
    print("=" * 60)
    print("                     RUN SUMMARY")
    print("=" * 60)
    print()
    print(f"📊 FLAT v1 Edge: {flat_result.get('candidate_count', 0)} candidates")
    print(f"   Universe: {len(flat_result.get('universe', []))} symbols")
    print(f"   Regime Gate: IVp ≤ {flat_result.get('regime_gate', {}).get('max_atm_iv_pctl', 75)}")
    print()
    
    if flat_result.get('candidates'):
        print("🎯 TRADE CANDIDATES:")
        for c in flat_result['candidates']:
            structure = c.get('structure', {})
            print(f"   {c['symbol']}: IVp={c.get('atm_iv_percentile', 0):.0f}, "
                  f"debit=${structure.get('entry_debit', 0):.2f}, "
                  f"max_loss=${structure.get('max_loss', 0):.0f}")
    else:
        print("💤 No FLAT signals today")
    
    print()
    print("✅ Trading Allowed" if report['trading_allowed'] else "⛔ Trading Blocked")
    
    # Save report
    latest_path = output_dir / "latest.json"
    with open(latest_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved: {latest_path}")
    
    print("\n=== Done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
