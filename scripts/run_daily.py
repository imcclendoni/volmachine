#!/usr/bin/env python3
"""
Run Daily Analysis.

Main entry point for daily volatility desk analysis.
Generates regime classification, edge signals, and trade candidates.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine import VolMachineEngine


def main():
    parser = argparse.ArgumentParser(
        description="Run daily volatility desk analysis"
    )
    parser.add_argument(
        "--config",
        default="./config/settings.yaml",
        help="Path to settings.yaml"
    )
    parser.add_argument(
        "--universe",
        default="./config/universe.yaml",
        help="Path to universe.yaml"
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
    parser.add_argument(
        "--format",
        nargs="+",
        default=["markdown", "html"],
        help="Report formats (markdown, html)"
    )
    parser.add_argument(
        "--paper",
        action="store_true",
        help="Execute candidates in paper mode"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate report without saving"
    )
    
    args = parser.parse_args()
    
    # Parse date
    run_date = date.today()
    if args.date:
        run_date = date.fromisoformat(args.date)
    
    print(f"=== VolMachine Daily Run: {run_date} ===")
    print()
    
    # Initialize engine
    engine = VolMachineEngine(
        config_path=args.config,
        universe_path=args.universe,
    )
    
    # Connect to data provider
    if not engine.connect():
        print("Warning: Could not connect to data provider")
        print("Running with cached/simulated data...")
    
    # Run daily analysis
    report = engine.run_daily(run_date)
    
    # Get enabled symbols for display
    enabled_symbols = engine.get_enabled_symbols()
    
    print()
    print("=" * 60)
    print("                     RUN SUMMARY")
    print("=" * 60)
    print()
    
    # Regime
    print(f"📊 REGIME: {report.regime.regime.value.upper()} ({report.regime.confidence:.0%} confidence)")
    print(f"   {report.regime.rationale}")
    print()
    
    # Universe Summary
    symbols_with_edges = list(set(e.symbol for e in report.edges))
    symbols_with_trades = list(set(c.symbol for c in report.candidates if c.recommendation == 'TRADE'))
    
    print(f"🔍 UNIVERSE: {len(enabled_symbols)} symbols scanned")
    print(f"   Edges Found: {len(report.edges)} across {len(symbols_with_edges)} symbols")
    print(f"   Trade Candidates: {len(symbols_with_trades)} symbols")
    print()
    
    # Symbol-by-Symbol Breakdown
    print("📋 SYMBOL BREAKDOWN:")
    print("-" * 60)
    print(f"{'SYMBOL':<8} {'EDGE':<12} {'STRENGTH':<10} {'OUTCOME':<15} {'REASON':<15}")
    print("-" * 60)
    
    for symbol in enabled_symbols:
        # Find edges for this symbol
        sym_edges = [e for e in report.edges if e.symbol == symbol]
        sym_candidates = [c for c in report.candidates if c.symbol == symbol]
        
        if not sym_edges:
            print(f"{symbol:<8} {'--':<12} {'--':<10} {'NO EDGE':<15}")
            continue
        
        for i, edge in enumerate(sym_edges):
            edge_type = edge.edge_type.value if hasattr(edge.edge_type, 'value') else str(edge.edge_type)
            strength = f"{edge.strength:.0%}"
            
            # Find candidate for this edge
            matching_candidates = [c for c in sym_candidates if c.edge.edge_type == edge.edge_type]
            
            if matching_candidates:
                cand = matching_candidates[0]
                outcome = cand.recommendation
                # Get failure reason for PASS
                if outcome == 'PASS':
                    reason = cand.validation_messages[0][:15] if cand.validation_messages else "structure fail"
                else:
                    reason = ""
            else:
                outcome = "NO STRUCTURE"
                reason = ""
            
            print(f"{symbol:<8} {edge_type:<12} {strength:<10} {outcome:<15} {reason}")
    
    print("-" * 60)
    print()
    
    # Trading Status
    if not report.trading_allowed:
        print("⛔ TRADING BLOCKED")
        for reason in report.do_not_trade_reasons:
            print(f"  - {reason}")
        print()
    else:
        print("✅ Trading Allowed")
    
    # Show top candidates
    if report.candidates:
        print("\n=== Trade Candidates ===")
        for i, c in enumerate(report.candidates[:5], 1):
            emoji = "✅" if c.recommendation == "TRADE" else "⚠️" if c.recommendation == "REVIEW" else "❌"
            print(f"{i}. {emoji} {c.symbol} - {c.structure.structure_type.value}")
            print(f"   Edge: {c.edge.edge_type.value} ({c.edge.strength:.0%})")
            # IMPORTANT: Use max_loss_dollars not max_loss (points vs dollars)
            max_loss_dollars = c.structure.max_loss_dollars if c.structure.max_loss_dollars else 0
            max_loss_str = f"${max_loss_dollars:.2f}" if max_loss_dollars else "N/A"
            print(f"   Max Loss: {max_loss_str}, Contracts: {c.recommended_contracts}")
    
    # Save report
    if not args.dry_run:
        saved = engine.export_report(report, args.output)
        print(f"\nReports saved:")
        for path in saved:
            print(f"  - {path}")
        
        # Also export JSON for Desk UI with diagnostics
        from engine.report_json import export_report_json
        from datetime import datetime
        
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
            'symbols_with_data': len(symbols_scanned),  # Assume all scanned have data for now
            'symbols_with_edges': len(symbols_with_edges),
            'symbols_with_trades': len(symbols_with_trades),
            'symbol_list': symbols_scanned,
        }
        
        # Build VRP metrics from edge signals
        vrp_metrics = []
        for edge in report.edges:
            if edge.edge_type == 'vrp':
                vrp_metrics.append({
                    'symbol': edge.symbol,
                    'atm_iv': edge.metrics.get('atm_iv', 0),
                    'rv_20': edge.metrics.get('rv_20', 0),
                    'iv_rv_ratio': edge.metrics.get('iv_rv_ratio', 0),
                    'threshold': 1.12,  # Default VRP threshold
                    'status': 'ABOVE_THRESHOLD' if edge.metrics.get('iv_rv_ratio', 0) >= 1.12 else 'BELOW_THRESHOLD',
                })
        
        json_path = export_report_json(
            report_date=run_date,
            regime=report.regime,
            edges=report.edges,
            candidates=report.candidates,
            trading_allowed=report.trading_allowed,
            do_not_trade_reasons=report.do_not_trade_reasons,
            output_dir=args.output,
            provider_status=provider_status,
            universe_scan=universe_scan,
            vrp_metrics=vrp_metrics,
        )
        print(f"  - {json_path}")
    
    # Paper trading execution
    if args.paper and report.trading_allowed:
        print("\n=== Paper Trading ===")
        from backtest import PaperSimulator, PaperConfig
        from risk import Portfolio, LimitTracker, LimitConfig
        
        # Use config equity from engine, not hardcoded
        account_equity = engine.sizing_config.account_equity
        
        portfolio = Portfolio(account_equity=account_equity)
        limit_tracker = LimitTracker(LimitConfig(account_equity=account_equity))
        simulator = PaperSimulator(portfolio, limit_tracker, PaperConfig())
        
        for candidate in report.candidates:
            if candidate.recommendation == "TRADE":
                result = simulator.execute_candidate(candidate)
                if result.success:
                    print(f"📈 Executed: {candidate.symbol} - {result.message}")
                else:
                    print(f"❌ Skipped: {candidate.symbol} - {result.message}")
        
        summary = simulator.get_summary()
        print(f"\nPortfolio: {summary['positions_open']} positions, ${summary['total_max_loss_dollars']:.0f} max loss")
    
    print("\n=== Done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
