#!/usr/bin/env python3
"""
Conditional Expectancy Analysis

For each STEEP trade, compute regime metrics and bin by atm_iv_percentile.
Reports per-bin: trade count, PF, expectancy, avg win/loss, tail ratio.

This is the key deliverable for proving/disproving edge existence.
"""

import json
import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Dict, List, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_backtest import get_option_daily_bars, get_underlying_daily_bars

# IV percentile bins
IV_BINS = [
    (0, 60, "0-60"),
    (60, 70, "60-70"),
    (70, 80, "70-80"),
    (80, 90, "80-90"),
    (90, 100, "90-100"),
]


def load_steep_trades(reports_dir: str) -> List[Dict]:
    """Load all STEEP (credit spread) trades from v6 reports."""
    reports_path = Path(reports_dir)
    trades = []
    
    for report_file in reports_path.glob("*.json"):
        try:
            with open(report_file) as f:
                report = json.load(f)
            
            report_date = report.get('report_date', report.get('execution_date', ''))
            
            for candidate in report.get('candidates', []):
                structure = candidate.get('structure', {})
                edge = candidate.get('edge', {})
                
                # Only STEEP = credit spread / direction SHORT
                if structure.get('spread_type') != 'credit':
                    continue
                if edge.get('direction') != 'SHORT':
                    continue
                
                metrics = edge.get('metrics', {})
                
                trades.append({
                    'symbol': candidate.get('symbol'),
                    'date': report_date,
                    'candidate': candidate,
                    'metrics': {
                        'skew_percentile': metrics.get('skew_percentile'),
                        'skew_delta': metrics.get('skew_delta'),
                        'percentile_delta': metrics.get('percentile_delta'),
                        'atm_iv': metrics.get('atm_iv'),
                        'atm_iv_percentile': metrics.get('atm_iv_percentile'),
                        'is_steep': metrics.get('is_steep'),
                    },
                    'structure': {
                        'entry_credit': structure.get('entry_credit'),
                        'max_loss_dollars': structure.get('max_loss_dollars'),
                        'width': structure.get('width_dollars', structure.get('width')),
                    }
                })
        except Exception as e:
            print(f"Error loading {report_file}: {e}")
    
    return trades


def simulate_trade(trade: Dict) -> Dict:
    """
    Simulate trade outcome using backtester logic.
    Returns PnL, exit reason, MFE, MAE.
    """
    candidate = trade['candidate']
    structure = candidate.get('structure', {})
    symbol = candidate.get('symbol')
    
    # Parse dates
    try:
        signal_date = date.fromisoformat(str(trade['date'])[:10])
    except:
        return None
    
    legs = structure.get('legs', [])
    if len(legs) < 2:
        return None
    
    expiry_str = structure.get('expiry') or legs[0].get('expiry', '')
    try:
        expiry = date.fromisoformat(str(expiry_str)[:10])
    except:
        return None
    
    entry_credit = structure.get('entry_credit', 0)
    max_loss = structure.get('max_loss_dollars', 500)
    
    # Get underlying bars
    underlying_bars = get_underlying_daily_bars(symbol, signal_date, expiry)
    if not underlying_bars:
        return None
    
    # Simulate: time-stop at DTE <= 7 or expiry
    exit_date = expiry - timedelta(days=7)
    if exit_date <= signal_date:
        exit_date = expiry
    
    # Get option bars for exit
    short_leg = [l for l in legs if l.get('side') == 'SELL'][0] if legs else None
    long_leg = [l for l in legs if l.get('side') == 'BUY'][0] if legs else None
    
    if not short_leg or not long_leg:
        return None
    
    # Build OCC symbols
    short_strike = short_leg.get('strike')
    long_strike = long_leg.get('strike')
    right = short_leg.get('right', 'P')
    
    exp_str = expiry.strftime('%y%m%d')
    short_occ = f"{symbol.ljust(6)}{exp_str}{right}{int(short_strike*1000):08d}"
    long_occ = f"{symbol.ljust(6)}{exp_str}{right}{int(long_strike*1000):08d}"
    
    # Get exit prices
    short_bars = get_option_daily_bars(short_occ, signal_date, expiry)
    long_bars = get_option_daily_bars(long_occ, signal_date, expiry)
    
    if not short_bars or not long_bars:
        return None
    
    # Find exit bar (time-stop or expiry)
    exit_short_price = None
    exit_long_price = None
    actual_exit_date = None
    exit_reason = 'expiry'
    
    # Track MFE/MAE
    mfe = 0  # Max favorable excursion
    mae = 0  # Max adverse excursion
    
    for i, bar in enumerate(short_bars):
        bar_date = date.fromisoformat(bar['date'])
        if bar_date <= signal_date:
            continue
        
        # Get corresponding long bar
        long_bar = None
        for lb in long_bars:
            if lb['date'] == bar['date']:
                long_bar = lb
                break
        
        if not long_bar:
            continue
        
        # Calculate spread value
        spread_value = bar['close'] - long_bar['close']
        pnl_at_point = (entry_credit - spread_value) * 100
        
        # Track MFE/MAE
        if pnl_at_point > mfe:
            mfe = pnl_at_point
        if pnl_at_point < mae:
            mae = pnl_at_point
        
        # Check time-stop (DTE <= 7)
        dte = (expiry - bar_date).days
        if dte <= 7 and actual_exit_date is None:
            exit_short_price = bar['close']
            exit_long_price = long_bar['close']
            actual_exit_date = bar_date
            exit_reason = 'time_stop'
    
    # If no time-stop, use expiry
    if actual_exit_date is None and short_bars:
        exit_short_price = short_bars[-1]['close']
        exit_long_price = long_bars[-1]['close'] if long_bars else 0
        actual_exit_date = date.fromisoformat(short_bars[-1]['date'])
    
    if exit_short_price is None:
        return None
    
    # Calculate final PnL
    exit_spread = exit_short_price - exit_long_price
    pnl = (entry_credit - exit_spread) * 100
    
    return {
        'pnl': pnl,
        'exit_reason': exit_reason,
        'mfe': mfe,
        'mae': mae,
        'entry_credit': entry_credit,
        'exit_spread': exit_spread,
        'hold_days': (actual_exit_date - signal_date).days if actual_exit_date else 0,
    }


def analyze_bins(trades: List[Dict]) -> Dict:
    """Analyze trades binned by IV percentile."""
    bins = {label: [] for _, _, label in IV_BINS}
    
    for trade in trades:
        iv_pctl = trade['metrics'].get('atm_iv_percentile', 0) or 0
        
        for low, high, label in IV_BINS:
            if low <= iv_pctl < high or (high == 100 and iv_pctl >= 90):
                bins[label].append(trade)
                break
    
    return bins


def compute_bin_stats(trades: List[Dict]) -> Dict:
    """Compute statistics for a bin of trades."""
    if not trades:
        return {
            'count': 0,
            'pf': 0,
            'expectancy': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'tail_ratio': 0,
            'max_dd': 0,
        }
    
    pnls = [t.get('result', {}).get('pnl', 0) for t in trades if t.get('result')]
    
    if not pnls:
        return {
            'count': len(trades),
            'pf': 0,
            'expectancy': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'tail_ratio': 0,
            'max_dd': 0,
        }
    
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    
    total_wins = sum(wins) if wins else 0
    total_losses = abs(sum(losses)) if losses else 0
    
    pf = total_wins / total_losses if total_losses > 0 else (float('inf') if total_wins > 0 else 0)
    expectancy = sum(pnls) / len(pnls)
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    tail_ratio = abs(avg_loss / avg_win) if avg_win != 0 else 0
    
    # Compute max drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for pnl in pnls:
        equity += pnl
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    
    return {
        'count': len(pnls),
        'pf': pf,
        'expectancy': expectancy,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'tail_ratio': tail_ratio,
        'max_dd': max_dd,
        'win_rate': len(wins) / len(pnls) * 100 if pnls else 0,
        'total_pnl': sum(pnls),
    }


def main():
    reports_dir = sys.argv[1] if len(sys.argv) > 1 else 'logs/backfill/v6/reports'
    
    print("=" * 80)
    print("CONDITIONAL EXPECTANCY ANALYSIS - STEEP Trades by IV Regime")
    print("=" * 80)
    print(f"\nLoading STEEP trades from: {reports_dir}\n")
    
    # Load trades
    trades = load_steep_trades(reports_dir)
    print(f"Found {len(trades)} STEEP (credit spread) signals\n")
    
    if not trades:
        print("No STEEP trades found!")
        return
    
    # Print individual trade details
    print("-" * 80)
    print("INDIVIDUAL TRADE DETAILS")
    print("-" * 80)
    print(f"{'Symbol':<8} {'Date':<12} {'ATM_IV':<8} {'IV_Pctl':<8} {'Skew_Pctl':<10} {'Outcome':<12}")
    print("-" * 80)
    
    simulated_trades = []
    for trade in trades:
        # Simulate trade
        result = simulate_trade(trade)
        trade['result'] = result
        
        if result:
            simulated_trades.append(trade)
            
            m = trade['metrics']
            outcome = f"${result['pnl']:.0f} ({result['exit_reason']})"
            print(f"{trade['symbol']:<8} {trade['date']:<12} "
                  f"{m.get('atm_iv', 0)*100 if m.get('atm_iv') else 0:.1f}%   "
                  f"{m.get('atm_iv_percentile', 0) or 0:<8.1f} "
                  f"{m.get('skew_percentile', 0) or 0:<10.1f} "
                  f"{outcome:<12}")
    
    print(f"\nSimulated {len(simulated_trades)}/{len(trades)} trades\n")
    
    # Bin analysis
    print("=" * 80)
    print("REGIME-BINNED ANALYSIS")
    print("=" * 80)
    
    bins = analyze_bins(simulated_trades)
    
    print(f"\n{'IV Bin':<12} {'Count':<8} {'WR':<8} {'PF':<8} {'Exp/Trade':<12} {'AvgWin':<10} {'AvgLoss':<10} {'TailR':<8} {'MaxDD':<10}")
    print("-" * 96)
    
    all_stats = []
    for _, _, label in IV_BINS:
        bin_trades = bins.get(label, [])
        stats = compute_bin_stats(bin_trades)
        all_stats.append((label, stats))
        
        if stats['count'] > 0:
            print(f"{label:<12} {stats['count']:<8} {stats['win_rate']:<7.1f}% {stats['pf']:<8.2f} "
                  f"${stats['expectancy']:<11.2f} ${stats['avg_win']:<9.2f} ${stats['avg_loss']:<9.2f} "
                  f"{stats['tail_ratio']:<8.2f} ${stats['max_dd']:<9.2f}")
        else:
            print(f"{label:<12} {stats['count']:<8} -       -        -            -          -          -        -")
    
    print("-" * 96)
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    positive_bins = [(l, s) for l, s in all_stats if s['count'] >= 2 and s['pf'] > 1.0]
    negative_bins = [(l, s) for l, s in all_stats if s['count'] >= 2 and s['pf'] < 1.0]
    
    if positive_bins:
        print(f"\n✓ Positive expectancy bins (count >= 2, PF > 1.0):")
        for label, stats in positive_bins:
            print(f"  - {label}: {stats['count']} trades, PF={stats['pf']:.2f}, Exp=${stats['expectancy']:.2f}/trade")
    else:
        print("\n✗ No bins with positive expectancy and adequate sample (count >= 2)")
    
    if negative_bins:
        print(f"\n✗ Negative expectancy bins:")
        for label, stats in negative_bins:
            print(f"  - {label}: {stats['count']} trades, PF={stats['pf']:.2f}, Exp=${stats['expectancy']:.2f}/trade")
    
    # Overall
    overall = compute_bin_stats(simulated_trades)
    print(f"\nOverall: {overall['count']} trades, PF={overall['pf']:.2f}, "
          f"Exp=${overall['expectancy']:.2f}/trade, MaxDD=${overall['max_dd']:.2f}")


if __name__ == '__main__':
    main()
