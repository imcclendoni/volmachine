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
from backtest.integrity import generate_integrity_report, print_integrity_report


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
        "--years",
        type=int,
        default=None,
        help="Number of years to backtest (overrides --days)"
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
    parser.add_argument(
        "--integrity",
        action="store_true",
        default=True,
        help="Show integrity report (default: True)"
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Path to reports directory (default: ./logs/reports)"
    )
    
    args = parser.parse_args()
    
    # Calculate date range
    end_date = date.today()
    if args.end:
        end_date = date.fromisoformat(args.end)
    
    if args.start:
        start_date = date.fromisoformat(args.start)
    elif args.years:
        start_date = end_date - timedelta(days=args.years * 365)
    else:
        start_date = end_date - timedelta(days=args.days)
    
    print(f"VolMachine Backtest")
    print(f"=" * 60)
    print(f"Period: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    print(f"Config: {args.config}")
    if args.input_dir:
        print(f"Input: {args.input_dir}")
    if args.symbols:
        print(f"Symbols: {', '.join(args.symbols)}")
    print()
    
    # Run backtest
    reports_dir = args.input_dir if args.input_dir else './logs/reports'
    backtester = DeterministicBacktester(config_path=args.config, reports_dir=reports_dir)
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
        
        if m.by_structure:
            print("\nBY STRUCTURE TYPE:")
            for struct, data in m.by_structure.items():
                print(f"  {struct}: {data['trades']} trades, ${data['pnl']:.2f}, {data['win_rate']:.0f}% win")
        
        # Multi-year validation metrics
        period_days = (end_date - start_date).days
        years = period_days / 365.0
        if years >= 1.0 and m.total_trades > 0:
            print("\n" + "=" * 60)
            print("VALIDATION METRICS")
            print("=" * 60)
            
            # Trades per year
            trades_per_year = m.total_trades / years
            print(f"\n📅 Trades per year: {trades_per_year:.1f}")
            
            # Calculate CAGR (assuming $10k starting)
            starting_capital = 10000.0
            ending_capital = starting_capital + m.total_pnl
            if ending_capital > 0 and years > 0:
                cagr = ((ending_capital / starting_capital) ** (1.0 / years) - 1) * 100
                print(f"📈 CAGR (on $10k): {cagr:.1f}%")
            
            # Max drawdown %
            if ending_capital > 0:
                max_dd_pct = (m.max_drawdown / starting_capital) * 100
                print(f"📉 Max DD %: {max_dd_pct:.1f}%")
            
            # Worst consecutive loss streak
            if result.trades:
                current_streak = 0
                worst_streak = 0
                worst_streak_loss = 0.0
                current_loss = 0.0
                
                for trade in result.trades:
                    if trade.net_pnl < 0:
                        current_streak += 1
                        current_loss += trade.net_pnl
                        if current_streak > worst_streak:
                            worst_streak = current_streak
                            worst_streak_loss = current_loss
                    else:
                        current_streak = 0
                        current_loss = 0.0
                
                print(f"🔴 Worst loss streak: {worst_streak} trades (${worst_streak_loss:.2f})")
            
            # Equity curve snapshot (high/low)
            if result.trades:
                equity = 0.0
                peak = 0.0
                trough = 0.0
                
                for trade in result.trades:
                    equity += trade.net_pnl
                    if equity > peak:
                        peak = equity
                    if equity < trough:
                        trough = equity
                
                print(f"💹 Equity peak: ${peak:.2f}")
                print(f"💹 Equity trough: ${trough:.2f}")
                print(f"💹 Final equity: ${equity:.2f}")
        
        # Integrity report
        if args.integrity:
            integrity = generate_integrity_report(result)
            print_integrity_report(integrity)
            
            if not integrity.passed:
                print("\n⚠️  INTEGRITY CHECK FAILED - Results may not be reliable")
                
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
