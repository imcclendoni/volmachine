#!/usr/bin/env python3
"""
Run Backtest.

Historical walk-forward evaluation of the volatility strategy.
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest import (
    run_walk_forward,
    format_walk_forward_report,
    create_performance_metrics,
    format_metrics_report,
    WalkForwardConfig,
)


def main():
    parser = argparse.ArgumentParser(
        description="Run historical backtest with walk-forward evaluation"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=(date.today() - timedelta(days=365)).isoformat(),
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end",
        type=str,
        default=date.today().isoformat(),
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["SPY", "QQQ"],
        help="Symbols to backtest"
    )
    parser.add_argument(
        "--in-sample",
        type=int,
        default=180,
        help="In-sample window (days)"
    )
    parser.add_argument(
        "--out-sample",
        type=int,
        default=30,
        help="Out-of-sample window (days)"
    )
    parser.add_argument(
        "--output",
        default="./logs/backtest",
        help="Output directory"
    )
    
    args = parser.parse_args()
    
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    
    print("=== VolMachine Backtest ===")
    print(f"Period: {start_date} to {end_date}")
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"Walk-Forward: {args.in_sample}d IS / {args.out_sample}d OOS")
    print()
    
    # Note: Full backtest requires historical option data
    # This is a framework demonstration
    
    print("⚠️  Full historical backtest requires option chain history")
    print("   Showing framework structure...")
    print()
    
    # Example walk-forward with mock trades
    example_trades = [
        {'date': start_date + timedelta(days=i), 'pnl': 100 if i % 3 != 0 else -150, 'regime': 'LOW_VOL_GRIND'}
        for i in range(0, 300, 2)
    ]
    
    config = WalkForwardConfig(
        in_sample_days=args.in_sample,
        out_of_sample_days=args.out_sample,
        step_days=30,
    )
    
    result = run_walk_forward(example_trades, config)
    
    print(format_walk_forward_report(result))
    
    # Performance metrics
    initial_equity = 100000
    equity_curve = [initial_equity]
    for trade in example_trades:
        equity_curve.append(equity_curve[-1] + trade['pnl'])
    
    metrics = create_performance_metrics(equity_curve, example_trades)
    print()
    print(format_metrics_report(metrics))
    
    # Save output
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / f"backtest_{start_date}_{end_date}.txt"
    with open(report_file, 'w') as f:
        f.write("=== BACKTEST RESULTS ===\n\n")
        f.write(format_walk_forward_report(result))
        f.write("\n\n")
        f.write(format_metrics_report(metrics))
    
    print(f"\nResults saved to: {report_file}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
