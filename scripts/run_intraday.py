#!/usr/bin/env python3
"""
Run Intraday Checks.

Lighter-weight intraday monitoring for:
- Regime shifts
- Risk limit checks  
- Position updates
- Kill switch status
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine import VolMachineEngine, get_logger


def run_single_check(engine: VolMachineEngine, export_json: bool = True):
    """Run a single intraday check with optional full report export."""
    logger = get_logger()
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Intraday Check")
    
    # Check risk limits
    status = engine.limit_tracker.check_limits()
    
    if status.kill_switch_active:
        print("🚨 KILL SWITCH ACTIVE")
        logger.log_kill_switch("Intraday check", status=status.to_dict() if hasattr(status, 'to_dict') else str(status))
        return False
    
    if not status.trading_allowed:
        print(f"⚠️  Trading blocked: {status.blocked_reason}")
    else:
        print("✅ Trading OK")
    
    # Show current levels
    print(f"   Daily Loss:  {status.daily_loss_pct:.1f}%")
    print(f"   Weekly Loss: {status.weekly_loss_pct:.1f}%")
    print(f"   Drawdown:    {status.max_drawdown_pct:.1f}%")
    
    for warning in status.warnings:
        print(f"   ⚠️ {warning}")
    
    # Portfolio summary
    summary = engine.portfolio.get_risk_summary()
    print(f"\n   Positions: {summary['total_positions']}/{summary['max_positions']}")
    print(f"   Risk Used: ${summary['total_risk_dollars']:.0f} ({summary['risk_used_pct']:.0f}%)")
    
    # Run full engine scan and export JSON for UI
    if export_json:
        try:
            from datetime import date
            from engine.report_json import export_report_json
            
            run_date = date.today()
            report = engine.run_daily(run_date=run_date)
            
            if report:
                # Build provider status
                provider_status = {
                    'connected': engine.provider is not None,
                    'source': engine.provider.__class__.__name__ if engine.provider else 'none',
                    'last_run': datetime.now().isoformat(),
                }
                
                # Build universe scan summary  
                symbols_scanned = list(engine.get_enabled_symbols())
                symbols_with_edges = list(set(e.symbol for e in report.edges))
                symbols_with_trades = list(set(c.symbol for c in report.candidates if c.recommendation == 'TRADE'))
                
                universe_scan = {
                    'symbols_scanned': len(symbols_scanned),
                    'symbols_with_data': len(symbols_scanned),
                    'symbols_with_edges': len(symbols_with_edges),
                    'symbols_with_trades': len(symbols_with_trades),
                }
                
                # Export JSON to update latest.json for UI
                json_path = export_report_json(
                    report_date=run_date,
                    regime=report.regime,
                    edges=report.edges,
                    candidates=report.candidates,
                    trading_allowed=report.trading_allowed,
                    do_not_trade_reasons=report.do_not_trade_reasons,
                    output_dir='./logs/reports',
                    provider_status=provider_status,
                    universe_scan=universe_scan,
                )
                
                print(f"\n   📊 Report updated: {json_path}")
                print(f"   📈 Edges: {len(report.edges)}, Candidates: {len(report.candidates)}")
        except Exception as e:
            print(f"   ⚠️ Report export failed: {e}")
    
    return status.trading_allowed


def main():
    parser = argparse.ArgumentParser(
        description="Run intraday monitoring checks"
    )
    parser.add_argument(
        "--config",
        default="./config/settings.yaml",
        help="Path to settings.yaml"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Check interval in minutes (0 for single run)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    print("=== VolMachine Intraday Monitor ===")
    
    # Initialize engine
    engine = VolMachineEngine(config_path=args.config)
    
    if not engine.connect():
        print("Warning: Could not connect to data provider")
    
    if args.interval == 0:
        # Single check
        run_single_check(engine)
    else:
        # Continuous monitoring
        print(f"Monitoring every {args.interval} minutes (Ctrl+C to stop)")
        
        try:
            while True:
                trading_ok = run_single_check(engine)
                
                if not trading_ok:
                    print("\n⚠️  Trading blocked - continuing to monitor...")
                
                time.sleep(args.interval * 60)
                
        except KeyboardInterrupt:
            print("\n\nStopped by user")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
