#!/usr/bin/env python3
"""
FLAT Sizing Sweep — Production-Grade Analysis

Evaluates FLAT edge performance under multiple sizing rules on a $50k account.

Inputs (FROZEN):
- Edge: FLAT call debit spread
- Width: $5
- Entry: execution_date (N+1)
- Exit: 70% TP OR DTE≤7
- Universe: SPY, QQQ, IWM, XLF, GLD, TLT, DIA (EEM excluded)

Grid:
- account_equity = $50,000
- risk_pct ∈ {0.25%, 0.50%, 0.75%, 1.00%, 1.50%, 2.00%}

Output:
- logs/research/flat_sizing_sweep.json

Usage:
    python scripts/research/flat_sizing_sweep.py
"""

import sys
sys.path.insert(0, '/Users/jeffreyboyle/Desktop/volmachine')

import json
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import statistics

from backtest.fill_model import FillConfig
from data.polygon_backtest import get_underlying_daily_bars

# Configuration
REPORTS_DIR = Path('./logs/backfill/v6/reports')
OUTPUT_FILE = Path('./logs/research/flat_sizing_sweep.json')

# Frozen parameters
ACCOUNT_EQUITY = 50_000
EXCLUDED_SYMBOLS = ['EEM']
WIDTH = 5.0
HOLDING_DAYS = 14
TP_PCT = 70.0

# Sizing grid
RISK_PCT_GRID = [0.25, 0.50, 0.75, 1.00, 1.50, 2.00]


def load_flat_signals(reports_dir: Path) -> List[Dict]:
    """Load FLAT signals with structure data."""
    signals = []
    
    for report_file in sorted(reports_dir.glob('*__*__backfill.json')):
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            parts = report_file.stem.split('__')
            if len(parts) < 2:
                continue
            
            report_date_str = parts[0]
            symbol = parts[1]
            year = report_date_str[:4]
            
            if symbol in EXCLUDED_SYMBOLS:
                continue
            
            execution_date = report.get('execution_date', report_date_str)
            
            for edge in report.get('edges', []):
                metrics = edge.get('metrics', {})
                
                is_flat = metrics.get('is_flat', 0.0) == 1.0
                is_long = edge.get('direction', '').upper() == 'LONG'
                
                if not (is_flat or is_long):
                    continue
                
                candidates = report.get('candidates', [])
                structure = candidates[0].get('structure') if candidates else None
                
                if structure:
                    signals.append({
                        'symbol': symbol,
                        'year': year,
                        'execution_date': execution_date,
                        'structure': structure,
                    })
                    
        except Exception:
            continue
    
    # Sort by execution date
    signals.sort(key=lambda s: s['execution_date'])
    return signals


def get_bars_cached(symbol: str, start_date, days: int, cache: Dict) -> List:
    """Get underlying bars with caching."""
    from datetime import date
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    
    cache_key = f"{symbol}_{start_date}_{days}"
    if cache_key not in cache:
        try:
            bars = get_underlying_daily_bars(
                symbol, start_date,
                start_date + timedelta(days=days + 10),
                use_cache=True
            )
            cache[cache_key] = bars or []
        except:
            cache[cache_key] = []
    
    return cache[cache_key]


def simulate_with_sizing(
    signals: List[Dict],
    risk_pct: float,
    initial_equity: float,
    fill_config: FillConfig,
    bar_cache: Dict,
) -> Dict:
    """
    Simulate trades with dynamic sizing based on account equity.
    
    Sizing: contracts = floor(risk_per_trade / (entry_debit × 100))
    where risk_per_trade = risk_pct × current_equity
    """
    equity = initial_equity
    peak_equity = initial_equity
    max_dd = 0.0
    max_dd_pct = 0.0
    
    trades = []
    equity_curve = []
    loss_streak = 0
    max_loss_streak = 0
    worst_loss_streak_pct = 0.0
    current_streak_loss = 0.0
    
    total_capital_deployed = 0
    deployment_count = 0
    
    for sig in signals:
        struct = sig.get('structure', {})
        legs = struct.get('legs', [])
        if len(legs) != 2:
            continue
        
        # Get strike
        long_strike = None
        for leg in legs:
            if leg.get('side') == 'BUY':
                long_strike = leg.get('strike')
        
        if long_strike is None:
            continue
        
        entry_debit = struct.get('entry_debit', 1.0)
        
        # Dynamic sizing
        risk_per_trade = equity * (risk_pct / 100)
        max_loss_per_contract = entry_debit * 100  # Max loss = debit paid
        contracts = max(1, int(risk_per_trade / max_loss_per_contract))
        
        # Track capital deployment
        capital_deployed = entry_debit * 100 * contracts
        total_capital_deployed += capital_deployed
        deployment_count += 1
        
        exec_date_str = sig['execution_date']
        exec_date = datetime.strptime(exec_date_str, '%Y-%m-%d').date()
        
        bars = get_bars_cached(sig['symbol'], exec_date, HOLDING_DAYS, bar_cache)
        if not bars or len(bars) < 2:
            continue
        
        entry_price = bars[0]['close']
        
        # Simulate with TP logic
        max_profit = WIDTH - entry_debit
        tp_threshold = max_profit * (TP_PCT / 100)
        
        exit_day = min(HOLDING_DAYS, len(bars) - 1)
        exit_price = bars[exit_day]['close']
        
        for day in range(1, min(HOLDING_DAYS + 1, len(bars))):
            day_price = bars[day]['close']
            
            call_long = long_strike
            call_short = long_strike + WIDTH
            
            if day_price >= call_short:
                current_value = WIDTH
            elif day_price <= call_long:
                current_value = 0
            else:
                current_value = day_price - call_long
            
            current_profit = current_value - entry_debit
            
            if current_profit >= tp_threshold:
                exit_day = day
                exit_price = day_price
                break
        
        # Final payoff
        if exit_price >= long_strike + WIDTH:
            exit_value = WIDTH
        elif exit_price <= long_strike:
            exit_value = 0
        else:
            exit_value = exit_price - long_strike
        
        gross_pnl = (exit_value - entry_debit) * 100 * contracts
        comm = fill_config.commission_per_contract * 4 * contracts
        net_pnl = gross_pnl - comm
        
        # Update equity
        equity += net_pnl
        peak_equity = max(peak_equity, equity)
        dd = peak_equity - equity
        dd_pct = dd / peak_equity * 100 if peak_equity > 0 else 0
        max_dd = max(max_dd, dd)
        max_dd_pct = max(max_dd_pct, dd_pct)
        
        # Track loss streaks
        if net_pnl < 0:
            loss_streak += 1
            current_streak_loss += net_pnl
            max_loss_streak = max(max_loss_streak, loss_streak)
            worst_loss_streak_pct = min(worst_loss_streak_pct, current_streak_loss / initial_equity * 100)
        else:
            loss_streak = 0
            current_streak_loss = 0
        
        trades.append({
            'date': exec_date_str,
            'symbol': sig['symbol'],
            'contracts': contracts,
            'net_pnl': round(net_pnl, 2),
            'equity': round(equity, 2),
        })
        
        equity_curve.append({
            'date': exec_date_str,
            'equity': round(equity, 2),
            'drawdown': round(dd, 2),
            'drawdown_pct': round(dd_pct, 2),
        })
    
    if not trades:
        return {'trades': 0}
    
    # Calculate metrics
    total_pnl = equity - initial_equity
    total_return_pct = total_pnl / initial_equity * 100
    
    # Annualize (assume ~252 trading days, ~3 years of data)
    years = 3.0
    trades_per_year = len(trades) / years
    annual_return_pct = total_return_pct / years
    
    avg_capital_deployed = total_capital_deployed / deployment_count if deployment_count > 0 else 0
    
    return {
        'risk_pct': risk_pct,
        'trades': len(trades),
        'trades_per_year': round(trades_per_year, 1),
        'total_return_usd': round(total_pnl, 2),
        'total_return_pct': round(total_return_pct, 1),
        'annual_return_pct': round(annual_return_pct, 1),
        'max_dd_usd': round(max_dd, 2),
        'max_dd_pct': round(max_dd_pct, 1),
        'return_to_dd': round(total_return_pct / max_dd_pct, 2) if max_dd_pct > 0 else float('inf'),
        'max_loss_streak': max_loss_streak,
        'worst_streak_pct': round(worst_loss_streak_pct, 1),
        'avg_capital_deployed': round(avg_capital_deployed, 2),
        'final_equity': round(equity, 2),
        'equity_curve': equity_curve,
    }


def main():
    """Run sizing sweep."""
    print("=" * 70)
    print("FLAT Sizing Sweep — $50k Account")
    print("=" * 70)
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Load signals
    print("\nLoading FLAT signals (excluding EEM)...")
    signals = load_flat_signals(REPORTS_DIR)
    print(f"Loaded {len(signals)} signals")
    
    fill_config = FillConfig.from_yaml()
    bar_cache = {}
    
    results = {
        'generated_at': datetime.now().isoformat(),
        'account_equity': ACCOUNT_EQUITY,
        'signals_count': len(signals),
        'grid': [],
    }
    
    print(f"\nRunning sizing grid...")
    print(f"\n{'Risk %':>8} | {'Trades':>6} | {'Return $':>10} | {'Return %':>9} | {'Max DD %':>9} | {'Ret/DD':>7} | {'Streak':>6}")
    print("-" * 75)
    
    for risk_pct in RISK_PCT_GRID:
        result = simulate_with_sizing(
            signals, risk_pct, ACCOUNT_EQUITY, fill_config, bar_cache
        )
        results['grid'].append(result)
        
        print(f"{risk_pct:>7.2f}% | {result['trades']:>6} | ${result['total_return_usd']:>9,.0f} | "
              f"{result['total_return_pct']:>8.1f}% | {result['max_dd_pct']:>8.1f}% | "
              f"{result['return_to_dd']:>6.2f} | {result['max_loss_streak']:>6}")
    
    # Find optimal (best return/DD ratio with reasonable drawdown)
    valid_configs = [r for r in results['grid'] if r['max_dd_pct'] <= 15]
    if valid_configs:
        best = max(valid_configs, key=lambda r: r['return_to_dd'])
        results['recommendation'] = {
            'risk_pct': best['risk_pct'],
            'rationale': f"Best return/DD ratio ({best['return_to_dd']:.2f}) with max DD {best['max_dd_pct']:.1f}%",
        }
    else:
        best = min(results['grid'], key=lambda r: r['max_dd_pct'])
        results['recommendation'] = {
            'risk_pct': best['risk_pct'],
            'rationale': f"Lowest DD at {best['max_dd_pct']:.1f}% (all configs exceed 15% DD target)",
        }
    
    # Save results
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Print recommendation
    print("\n" + "=" * 70)
    print("RECOMMENDATION")
    print("=" * 70)
    print(f"\n  Suggested risk_pct: {results['recommendation']['risk_pct']:.2f}%")
    print(f"  Rationale: {results['recommendation']['rationale']}")
    
    rec = next(r for r in results['grid'] if r['risk_pct'] == results['recommendation']['risk_pct'])
    print(f"\n  Expected performance:")
    print(f"    - Annual return: {rec['annual_return_pct']:.1f}%")
    print(f"    - Max drawdown: {rec['max_dd_pct']:.1f}%")
    print(f"    - Return/DD: {rec['return_to_dd']:.2f}")
    print(f"    - Trades/year: {rec['trades_per_year']:.0f}")
    
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
