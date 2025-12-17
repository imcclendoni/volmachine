#!/usr/bin/env python3
"""
Run Backtest CLI.

Entry point for running deterministic backtests.
"""

import argparse
import sys
import json
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.deterministic import DeterministicBacktester


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
    
    args = parser.parse_args()
    
    # Calculate date range
    end_date = date.today()
    if args.end:
        end_date = date.fromisoformat(args.end)
    
    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        start_date = end_date - timedelta(days=args.days)
    
    print(f"VolMachine Backtest")
    print(f"=" * 60)
    print(f"Period: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    print(f"Config: {args.config}")
    if args.symbols:
        print(f"Symbols: {', '.join(args.symbols)}")
    print()
    
    # Run backtest
    backtester = DeterministicBacktester(config_path=args.config)
    result = backtester.run_range(start_date, end_date, symbols=args.symbols)
    
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
