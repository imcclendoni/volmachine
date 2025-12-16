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
    
    # Print summary
    print(f"Regime: {report.regime.regime.value} ({report.regime.confidence:.0%})")
    print(f"Edges Found: {len(report.edges)}")
    print(f"Trade Candidates: {len(report.candidates)}")
    print()
    
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
            max_loss_str = f"${c.structure.max_loss:.2f}" if c.structure.max_loss else "N/A"
            print(f"   Max Loss: {max_loss_str}, Contracts: {c.recommended_contracts}")
    
    # Save report
    if not args.dry_run:
        saved = engine.export_report(report, args.output)
        print(f"\nReports saved:")
        for path in saved:
            print(f"  - {path}")
    
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
