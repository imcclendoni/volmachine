#!/usr/bin/env python3
"""
Loss Attribution Report.

Analyze backtest results to understand losses:
- Top 10 worst trades
- Top 10 best trades
- Loss bucket histogram
- Compare winners vs losers (credit, width, DTE, regime)
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict


def load_backtest_results(results_path: Path) -> Dict:
    """Load backtest results JSON."""
    with open(results_path) as f:
        return json.load(f)


def analyze_trades(trades: List[Dict]) -> Dict[str, Any]:
    """Analyze trade distribution and attribution."""
    if not trades:
        return {'error': 'No trades to analyze'}
    
    # Sort by PnL
    sorted_trades = sorted(trades, key=lambda x: x.get('pnl', 0))
    
    # Top 10 worst and best
    worst_10 = sorted_trades[:10]
    best_10 = sorted_trades[-10:][::-1]
    
    # Separate winners and losers
    winners = [t for t in trades if t.get('pnl', 0) > 0]
    losers = [t for t in trades if t.get('pnl', 0) <= 0]
    
    # Loss buckets
    loss_buckets = defaultdict(int)
    for t in losers:
        pnl = abs(t.get('pnl', 0))
        if pnl < 50:
            loss_buckets['$0-50'] += 1
        elif pnl < 100:
            loss_buckets['$50-100'] += 1
        elif pnl < 200:
            loss_buckets['$100-200'] += 1
        elif pnl < 300:
            loss_buckets['$200-300'] += 1
        else:
            loss_buckets['$300+'] += 1
    
    # Win buckets
    win_buckets = defaultdict(int)
    for t in winners:
        pnl = t.get('pnl', 0)
        if pnl < 25:
            win_buckets['$0-25'] += 1
        elif pnl < 50:
            win_buckets['$25-50'] += 1
        elif pnl < 75:
            win_buckets['$50-75'] += 1
        elif pnl < 100:
            win_buckets['$75-100'] += 1
        else:
            win_buckets['$100+'] += 1
    
    # Compare winner vs loser characteristics
    def avg_field(trades_list: List[Dict], field: str) -> float:
        values = [t.get(field, 0) for t in trades_list if t.get(field) is not None]
        return sum(values) / len(values) if values else 0
    
    winner_stats = {
        'count': len(winners),
        'avg_pnl': avg_field(winners, 'pnl'),
        'avg_entry_credit': avg_field(winners, 'entry_credit'),
        'avg_max_loss': avg_field(winners, 'max_loss'),
        'avg_dte': avg_field(winners, 'dte_at_entry'),
    }
    
    loser_stats = {
        'count': len(losers),
        'avg_pnl': avg_field(losers, 'pnl'),
        'avg_entry_credit': avg_field(losers, 'entry_credit'),
        'avg_max_loss': avg_field(losers, 'max_loss'),
        'avg_dte': avg_field(losers, 'dte_at_entry'),
    }
    
    # Exit reason breakdown
    exit_reasons = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0})
    for t in trades:
        reason = t.get('exit_reason', 'unknown')
        pnl = t.get('pnl', 0)
        if pnl > 0:
            exit_reasons[reason]['wins'] += 1
        else:
            exit_reasons[reason]['losses'] += 1
        exit_reasons[reason]['total_pnl'] += pnl
    
    # Symbol breakdown
    symbol_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0, 'trades': []})
    for t in trades:
        sym = t.get('symbol', 'unknown')
        pnl = t.get('pnl', 0)
        if pnl > 0:
            symbol_stats[sym]['wins'] += 1
        else:
            symbol_stats[sym]['losses'] += 1
        symbol_stats[sym]['total_pnl'] += pnl
        symbol_stats[sym]['trades'].append(t)
    
    return {
        'worst_10': worst_10,
        'best_10': best_10,
        'loss_buckets': dict(loss_buckets),
        'win_buckets': dict(win_buckets),
        'winner_stats': winner_stats,
        'loser_stats': loser_stats,
        'exit_reasons': {k: dict(v) for k, v in exit_reasons.items()},
        'symbol_stats': {k: {kk: vv for kk, vv in v.items() if kk != 'trades'} 
                         for k, v in symbol_stats.items()},
        'tail_ratio': loser_stats['avg_pnl'] / winner_stats['avg_pnl'] if winner_stats['avg_pnl'] != 0 else 0
    }


def print_report(analysis: Dict[str, Any]):
    """Print formatted loss attribution report."""
    print()
    print("=" * 70)
    print("LOSS ATTRIBUTION REPORT")
    print("=" * 70)
    
    # Top 10 worst trades
    print("\n📉 TOP 10 WORST TRADES:")
    print("-" * 70)
    print(f"{'Date':<12} {'Symbol':<6} {'PnL':>10} {'Exit':>12} {'Credit':>8} {'MaxLoss':>10}")
    print("-" * 70)
    for t in analysis.get('worst_10', []):
        print(f"{t.get('entry_date', 'N/A'):<12} {t.get('symbol', 'N/A'):<6} "
              f"${t.get('pnl', 0):>9.2f} {t.get('exit_reason', 'N/A'):>12} "
              f"${t.get('entry_credit', 0):>7.2f} ${t.get('max_loss', 0):>9.2f}")
    
    # Top 10 best trades
    print("\n📈 TOP 10 BEST TRADES:")
    print("-" * 70)
    print(f"{'Date':<12} {'Symbol':<6} {'PnL':>10} {'Exit':>12} {'Credit':>8} {'MaxLoss':>10}")
    print("-" * 70)
    for t in analysis.get('best_10', []):
        print(f"{t.get('entry_date', 'N/A'):<12} {t.get('symbol', 'N/A'):<6} "
              f"${t.get('pnl', 0):>9.2f} {t.get('exit_reason', 'N/A'):>12} "
              f"${t.get('entry_credit', 0):>7.2f} ${t.get('max_loss', 0):>9.2f}")
    
    # Loss buckets
    print("\n📊 LOSS SIZE DISTRIBUTION:")
    for bucket, count in sorted(analysis.get('loss_buckets', {}).items()):
        bar = "█" * count
        print(f"  {bucket:<10}: {count:>3} {bar}")
    
    # Win buckets
    print("\n📊 WIN SIZE DISTRIBUTION:")
    for bucket, count in sorted(analysis.get('win_buckets', {}).items()):
        bar = "█" * count
        print(f"  {bucket:<10}: {count:>3} {bar}")
    
    # Winner vs Loser comparison
    print("\n🔍 WINNER VS LOSER COMPARISON:")
    ws = analysis.get('winner_stats', {})
    ls = analysis.get('loser_stats', {})
    print(f"  {'Metric':<20} {'Winners':>12} {'Losers':>12}")
    print(f"  {'-'*20} {'-'*12} {'-'*12}")
    print(f"  {'Count':<20} {ws.get('count', 0):>12} {ls.get('count', 0):>12}")
    print(f"  {'Avg PnL':<20} ${ws.get('avg_pnl', 0):>11.2f} ${ls.get('avg_pnl', 0):>11.2f}")
    print(f"  {'Avg Entry Credit':<20} ${ws.get('avg_entry_credit', 0):>11.2f} ${ls.get('avg_entry_credit', 0):>11.2f}")
    print(f"  {'Avg Max Loss':<20} ${ws.get('avg_max_loss', 0):>11.2f} ${ls.get('avg_max_loss', 0):>11.2f}")
    print(f"  {'Avg DTE':<20} {ws.get('avg_dte', 0):>12.1f} {ls.get('avg_dte', 0):>12.1f}")
    
    # Tail ratio
    tail_ratio = analysis.get('tail_ratio', 0)
    print(f"\n  Tail Ratio (|AvgLoss|/AvgWin): {abs(tail_ratio):.2f}x")
    if abs(tail_ratio) > 2:
        print("  ⚠️  High tail ratio - losses are significantly larger than wins")
    
    # Exit reason breakdown
    print("\n📋 EXIT REASON BREAKDOWN:")
    for reason, stats in analysis.get('exit_reasons', {}).items():
        total = stats['wins'] + stats['losses']
        wr = stats['wins'] / total * 100 if total > 0 else 0
        print(f"  {reason:<15}: {total:>3} trades, {wr:>5.1f}% WR, ${stats['total_pnl']:>8.2f} PnL")
    
    # Symbol breakdown
    print("\n🏷️ SYMBOL BREAKDOWN:")
    for sym, stats in analysis.get('symbol_stats', {}).items():
        total = stats['wins'] + stats['losses']
        wr = stats['wins'] / total * 100 if total > 0 else 0
        print(f"  {sym:<6}: {total:>3} trades, {wr:>5.1f}% WR, ${stats['total_pnl']:>8.2f} PnL")
    
    print()


def main():
    parser = argparse.ArgumentParser(description="Generate loss attribution report")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to backtest results JSON file"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path for JSON report (optional)"
    )
    
    args = parser.parse_args()
    
    results_path = Path(args.input)
    if not results_path.exists():
        print(f"Error: File not found: {results_path}")
        return 1
    
    # Load results
    results = load_backtest_results(results_path)
    trades = results.get('trades', [])
    
    if not trades:
        print("No trades found in results file.")
        return 1
    
    # Analyze
    analysis = analyze_trades(trades)
    
    # Print report
    print_report(analysis)
    
    # Save JSON if requested
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"Report saved: {output_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
