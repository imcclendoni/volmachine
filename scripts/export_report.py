#!/usr/bin/env python3
"""
Export Historical Report.

Generate reports from historical data or saved state.
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.report import generate_markdown_report, generate_html_report, save_report
from data.schemas import DailyReport, RegimeClassification, PortfolioState, RegimeState


def main():
    parser = argparse.ArgumentParser(
        description="Export volatility desk report"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Report date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--output",
        default="./logs/reports",
        help="Output directory"
    )
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["markdown", "html"],
        default=["markdown", "html"],
        help="Output formats"
    )
    parser.add_argument(
        "--template",
        action="store_true",
        help="Generate template/example report"
    )
    
    args = parser.parse_args()
    
    report_date = date.fromisoformat(args.date)
    
    if args.template:
        # Generate example report
        print(f"Generating template report for {report_date}")
        
        report = DailyReport(
            report_date=report_date,
            generated_at=datetime.now(),
            regime=RegimeClassification(
                timestamp=datetime.now(),
                regime=RegimeState.LOW_VOL_GRIND,
                confidence=0.75,
                features={},
                rationale="Example regime classification"
            ),
            vol_state={"SPY": 0.15, "QQQ": 0.18},
            term_structure={"SPY": "contango", "QQQ": "contango"},
            edges=[],
            candidates=[],
            portfolio=PortfolioState(
                timestamp=datetime.now(),
                open_positions=[],
                total_max_loss=0,
                total_current_risk=0,
                portfolio_delta=0,
                portfolio_gamma=0,
                portfolio_theta=0,
                portfolio_vega=0,
                realized_pnl_today=0,
                unrealized_pnl=0,
                trades_open=0,
            ),
            trading_allowed=True,
            do_not_trade_reasons=[],
        )
        
        saved = save_report(report, args.output, args.format)
        print(f"Template report saved to:")
        for path in saved:
            print(f"  - {path}")
    else:
        print(f"Export for {report_date} requires saved state (not yet implemented)")
        print("Use --template to generate an example report")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
