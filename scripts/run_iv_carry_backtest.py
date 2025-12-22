#!/usr/bin/env python3
"""
Phase 1 Backtest for IV Carry MR Edge.

Simulates credit spread trades from IV Carry signals using the existing
deterministic backtester fill logic.

Usage:
    python scripts/run_iv_carry_backtest.py --input-dir logs/backfill/iv_carry_mr/reports
"""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.option_bar_store import OptionBarStore
from data.polygon_backtest import get_underlying_daily_bars
from backtest.fill_model import (
    FillConfig,
    calculate_entry_fill,
    calculate_exit_fill,
    calculate_realized_pnl,
)

FLATFILE_CACHE = Path("cache/flatfiles")


@dataclass
class TradeResult:
    """Result of a single simulated trade."""
    signal_date: date
    symbol: str
    direction: str
    iv_zscore: float
    entry_date: date
    exit_date: date
    exit_reason: str
    entry_price: float
    exit_price: float
    pnl: float
    holding_days: int


def load_signals(input_dir: Path, symbols: List[str] = None) -> List[Dict]:
    """Load signals from JSON files."""
    reports_dir = input_dir / "reports"
    if not reports_dir.exists():
        reports_dir = input_dir  # Try input_dir directly
    
    signals = []
    for f in sorted(reports_dir.glob("*.json")):
        if "summary" in f.name:
            continue
        
        with open(f) as fp:
            data = json.load(fp)
        
        signal = data.get('signal', data)
        
        # Filter by symbol if specified
        if symbols and signal.get('symbol') not in symbols:
            continue
        
        signals.append(data)
    
    return signals


def build_credit_spread(
    bar_store,
    signal: Dict,
    width: int = 5,
) -> Optional[Dict]:
    """
    Build a credit spread structure from an IV Carry signal.
    
    SELL_PUTS -> Put credit spread (sell higher strike, buy lower)
    SELL_CALLS -> Call credit spread (sell lower strike, buy higher)
    """
    sig = signal.get('signal', signal)
    
    exec_date = signal.get('execution_date')
    if isinstance(exec_date, str):
        exec_date = date.fromisoformat(exec_date)
    
    symbol = sig['symbol']
    direction = sig['direction']
    underlying_price = sig['underlying_price']
    target_expiry = sig.get('target_expiry')
    
    if target_expiry:
        if isinstance(target_expiry, str):
            target_expiry = date.fromisoformat(target_expiry)
    else:
        # Find a 30-45 DTE expiry
        expiries_data = bar_store.get_available_expiries(exec_date, symbol)
        for exp_date, dte in expiries_data:
            if 30 <= dte <= 45:
                target_expiry = exp_date
                break
        
        if not target_expiry:
            return None
    
    # Find ATM strike
    atm_strike = round(underlying_price / 5) * 5  # Round to nearest $5
    
    # Get available strikes
    strikes_data = bar_store.get_available_strikes(exec_date, symbol, target_expiry)
    if not strikes_data:
        return None
    
    available_strikes = sorted(strikes_data.keys())
    
    # Find closest ATM
    closest_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
    
    if direction == "SELL_PUTS":
        # Put credit spread: sell ATM put, buy OTM put
        short_strike = closest_strike
        long_strike = short_strike - width
        option_type = "put"
        spread_type = "credit"
    else:  # SELL_CALLS
        # Call credit spread: sell ATM call, buy OTM call  
        short_strike = closest_strike
        long_strike = short_strike + width
        option_type = "call"
        spread_type = "credit"
    
    # Check strikes exist
    if long_strike not in available_strikes:
        # Try larger width
        if direction == "SELL_PUTS":
            long_strike = short_strike - 10
        else:
            long_strike = short_strike + 10
        
        if long_strike not in available_strikes:
            return None
    
    return {
        'symbol': symbol,
        'expiry': target_expiry.isoformat() if isinstance(target_expiry, date) else target_expiry,
        'option_type': option_type,
        'short_strike': short_strike,
        'long_strike': long_strike,
        'spread_type': spread_type,
        'direction': direction,
    }


def simulate_trade(
    bar_store,
    signal: Dict,
    structure: Dict,
    config: Dict,
) -> Optional[TradeResult]:
    """Simulate a single trade from entry to exit."""
    sig = signal.get('signal', signal)
    
    exec_date = signal.get('execution_date')
    if isinstance(exec_date, str):
        exec_date = date.fromisoformat(exec_date)
    
    signal_date = signal.get('signal_date')
    if isinstance(signal_date, str):
        signal_date = date.fromisoformat(signal_date)
    
    expiry = structure['expiry']
    if isinstance(expiry, str):
        expiry = date.fromisoformat(expiry)
    
    symbol = sig['symbol']
    
    # Load entry day data
    try:
        bar_store.load_day(exec_date)
    except Exception as e:
        return None
    
    # Get leg data for entry
    short_strike = structure['short_strike']
    long_strike = structure['long_strike']
    opt_type = structure['option_type']
    
    # Build tickers
    exp_str = expiry.strftime("%y%m%d")
    opt_char = 'P' if opt_type == 'put' else 'C'
    
    short_ticker = f"O:{symbol}{exp_str}{opt_char}{int(short_strike * 1000):08d}"
    long_ticker = f"O:{symbol}{exp_str}{opt_char}{int(long_strike * 1000):08d}"
    
    # Get bars
    short_bar = bar_store.get_bar(exec_date, short_ticker)
    long_bar = bar_store.get_bar(exec_date, long_ticker)
    
    if not short_bar or not long_bar:
        return None
    
    # Calculate entry credit (sell short at bid, buy long at ask)
    short_bid = short_bar.get('bid', short_bar.get('close', 0))
    long_ask = long_bar.get('ask', long_bar.get('close', 0))
    
    if short_bid <= 0 or long_ask < 0:
        return None
    
    entry_credit = (short_bid - long_ask) * 100  # Per contract
    
    if entry_credit <= 0:
        return None  # No positive credit
    
    # Simulate holding period
    take_profit_pct = config.get('take_profit_pct', 50)
    time_stop_dte = config.get('time_stop_dte', 7)
    
    exit_date = exec_date
    exit_reason = None
    exit_debit = None
    
    current_date = exec_date + timedelta(days=1)
    
    while current_date <= expiry:
        # Time stop
        dte = (expiry - current_date).days
        if dte <= time_stop_dte:
            exit_date = current_date
            exit_reason = "time_stop"
            break
        
        # Try to load day
        try:
            bar_store.load_day(current_date)
            
            short_bar_now = bar_store.get_bar(current_date, short_ticker)
            long_bar_now = bar_store.get_bar(current_date, long_ticker)
            
            if short_bar_now and long_bar_now:
                short_ask = short_bar_now.get('ask', short_bar_now.get('close', 0))
                long_bid = long_bar_now.get('bid', long_bar_now.get('close', 0))
                
                current_debit = (short_ask - long_bid) * 100
                
                # Check take profit
                profit_pct = (entry_credit - current_debit) / entry_credit * 100
                
                if profit_pct >= take_profit_pct:
                    exit_date = current_date
                    exit_reason = "take_profit"
                    exit_debit = current_debit
                    break
        except:
            pass
        
        current_date += timedelta(days=1)
    
    # If no exit found, exit at last date before expiry
    if not exit_reason:
        exit_date = expiry - timedelta(days=1)
        exit_reason = "expiry"
    
    # Calculate exit debit if not already done
    if exit_debit is None:
        try:
            bar_store.load_day(exit_date)
            short_bar_exit = bar_store.get_bar(exit_date, short_ticker)
            long_bar_exit = bar_store.get_bar(exit_date, long_ticker)
            
            if short_bar_exit and long_bar_exit:
                short_ask = short_bar_exit.get('ask', short_bar_exit.get('close', 0))
                long_bid = long_bar_exit.get('bid', long_bar_exit.get('close', 0))
                exit_debit = (short_ask - long_bid) * 100
            else:
                exit_debit = entry_credit  # Assume scratch
        except:
            exit_debit = entry_credit
    
    pnl = entry_credit - exit_debit
    holding_days = (exit_date - exec_date).days
    
    return TradeResult(
        signal_date=signal_date,
        symbol=symbol,
        direction=sig['direction'],
        iv_zscore=sig['iv_zscore'],
        entry_date=exec_date,
        exit_date=exit_date,
        exit_reason=exit_reason,
        entry_price=entry_credit,
        exit_price=exit_debit,
        pnl=pnl,
        holding_days=holding_days,
    )


def main():
    parser = argparse.ArgumentParser(description="Backtest for IV Carry MR Edge")
    parser.add_argument("--input-dir", type=str, default="logs/backfill/iv_carry_mr",
                        help="Input directory with signals")
    parser.add_argument("--symbols", type=str, nargs="+", default=None,
                        help="Symbols to backtest")
    parser.add_argument("--phase", type=str, default="phase1", choices=["phase1", "phase2"],
                        help="Backtest phase (phase1=edge quality, phase2=portfolio)")
    parser.add_argument("--equity", type=float, default=25000,
                        help="Initial equity for Phase 2")
    parser.add_argument("--risk-pct", type=float, default=0.02,
                        help="Risk per trade as decimal (0.02 = 2%)")
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    is_phase2 = args.phase == "phase2"
    
    print("=" * 60)
    print(f"IV CARRY MR {'PHASE 2 PORTFOLIO' if is_phase2 else 'PHASE 1 EDGE'} BACKTEST")
    print("=" * 60)
    print(f"Input: {input_dir}")
    if is_phase2:
        print(f"Initial Equity: ${args.equity:,.0f}")
        print(f"Risk per Trade: {args.risk_pct*100:.1f}%")
    print()
    
    # Load signals
    signals = load_signals(input_dir, args.symbols)
    print(f"Loaded {len(signals)} signals")
    
    if not signals:
        print("No signals found!")
        return 1
    
    # Sort signals by execution date for Phase 2
    signals = sorted(signals, key=lambda x: x.get('execution_date', x.get('signal_date', '')))
    
    # Initialize
    bar_store = OptionBarStore(FLATFILE_CACHE, mode='thin')
    
    config = {
        'take_profit_pct': 50,
        'time_stop_dte': 7,
    }
    
    trades = []
    skipped = 0
    
    # Phase 2 equity tracking
    if is_phase2:
        equity = args.equity
        peak_equity = equity
        max_dd = 0.0
        max_dd_pct = 0.0
        equity_curve = [(None, equity)]  # (date, equity)
    
    print("\nRunning simulation...")
    print("-" * 40)
    
    for signal in signals:
        sig = signal.get('signal', signal)
        exec_date = signal.get('execution_date')
        if isinstance(exec_date, str):
            exec_date = date.fromisoformat(exec_date)
        
        # Load data
        try:
            bar_store.load_day(exec_date)
        except:
            skipped += 1
            continue
        
        # Build structure
        structure = build_credit_spread(bar_store, signal)
        if not structure:
            skipped += 1
            continue
        
        # Simulate trade
        result = simulate_trade(bar_store, signal, structure, config)
        
        if result:
            # Phase 2: Scale PnL based on risk sizing
            if is_phase2:
                # Calculate position size based on risk budget
                risk_budget = equity * args.risk_pct
                # Max loss on credit spread = width - credit received
                width = abs(structure['short_strike'] - structure['long_strike'])
                max_loss_per_contract = width * 100 - result.entry_price
                
                if max_loss_per_contract > 0:
                    contracts = max(1, int(risk_budget / max_loss_per_contract))
                else:
                    contracts = 1
                
                scaled_pnl = result.pnl * contracts
                
                # Update equity
                equity += scaled_pnl
                peak_equity = max(peak_equity, equity)
                current_dd = peak_equity - equity
                current_dd_pct = current_dd / peak_equity if peak_equity > 0 else 0
                max_dd = max(max_dd, current_dd)
                max_dd_pct = max(max_dd_pct, current_dd_pct)
                
                equity_curve.append((result.exit_date, equity))
                
                print(f"  {result.signal_date} {result.symbol}: ${scaled_pnl:.0f} ({contracts}x) -> Eq: ${equity:,.0f} ({result.exit_reason})")
            else:
                print(f"  {result.signal_date} {result.symbol}: ${result.pnl:.2f} ({result.exit_reason})")
            
            trades.append(result)
        else:
            skipped += 1
    
    # Calculate metrics
    print("\n" + "=" * 60)
    print(f"{'PHASE 2 PORTFOLIO' if is_phase2 else 'PHASE 1'} RESULTS")
    print("=" * 60)
    
    if not trades:
        print("No trades executed!")
        return 1
    
    total_pnl = sum(t.pnl for t in trades)
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]
    
    gross_profit = sum(t.pnl for t in wins) if wins else 0
    gross_loss = abs(sum(t.pnl for t in losses)) if losses else 1
    
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999
    win_rate = len(wins) / len(trades) * 100
    expectancy = total_pnl / len(trades)
    
    avg_win = gross_profit / len(wins) if wins else 0
    avg_loss = gross_loss / len(losses) if losses else 0
    
    print(f"\nTrades: {len(trades)} ({skipped} skipped)")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"Expectancy (per contract): ${expectancy:.2f}")
    
    if is_phase2:
        total_return = (equity - args.equity) / args.equity * 100
        years = 4.0  # Approximate backtest period
        cagr = ((equity / args.equity) ** (1/years) - 1) * 100 if equity > 0 else 0
        
        print(f"\n--- Portfolio Metrics ---")
        print(f"Initial Equity: ${args.equity:,.0f}")
        print(f"Final Equity: ${equity:,.0f}")
        print(f"Total Return: {total_return:.1f}%")
        print(f"CAGR: {cagr:.1f}%")
        print(f"Max Drawdown: ${max_dd:,.0f} ({max_dd_pct*100:.1f}%)")
    else:
        print(f"Total PnL (per contract): ${total_pnl:.2f}")
    
    # Breakdown by exit reason
    print("\nExit Reasons:")
    reasons = {}
    for t in trades:
        reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
    for reason, count in sorted(reasons.items()):
        print(f"  {reason}: {count}")
    
    # Breakdown by direction
    print("\nBy Direction:")
    for direction in ["SELL_PUTS", "SELL_CALLS"]:
        dir_trades = [t for t in trades if t.direction == direction]
        if dir_trades:
            dir_pnl = sum(t.pnl for t in dir_trades)
            dir_wins = len([t for t in dir_trades if t.pnl > 0])
            print(f"  {direction}: {len(dir_trades)} trades, ${dir_pnl:.2f}, {dir_wins/len(dir_trades)*100:.0f}% WR")
    
    # Kill criteria check
    print("\n" + "=" * 60)
    print("KILL CRITERIA CHECK")
    print("=" * 60)
    
    if is_phase2:
        if max_dd_pct < 0.20:
            print(f"✅ Max DD {max_dd_pct*100:.1f}% < 20% - PASS")
        elif max_dd_pct < 0.25:
            print(f"⚠️ Max DD {max_dd_pct*100:.1f}% < 25% but >= 20% - MARGINAL")
        else:
            print(f"❌ Max DD {max_dd_pct*100:.1f}% >= 25% - FAIL")
    else:
        if profit_factor >= 1.3:
            print(f"✅ Profit Factor {profit_factor:.2f} >= 1.3 - PASS")
        elif profit_factor >= 1.1:
            print(f"⚠️ Profit Factor {profit_factor:.2f} >= 1.1 but < 1.3 - MARGINAL")
        else:
            print(f"❌ Profit Factor {profit_factor:.2f} < 1.1 - FAIL")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

