#!/usr/bin/env python3
"""
TS-MR v2 Calendar Spread Backtester.

Standalone backtester for calendar spread structures.
Uses flatfile data via OptionBarStore.

Key differences from vertical backtester:
- Tracks two legs with DIFFERENT expiries
- Exit on z-score reversion OR front leg time stop
- PnL = net_exit_value - net_entry_value
"""

import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.option_bar_store import OptionBarStore
from edges.term_structure_mr.signal import TermStructureMRDetector, TermStructureMRConfig


FLATFILE_CACHE = Path("cache/flatfiles")


@dataclass
class CalendarTradeResult:
    """Result of a single calendar trade."""
    trade_id: str = ""
    symbol: str = ""
    signal_date: str = ""
    entry_date: str = ""
    exit_date: str = ""
    
    # Entry
    front_expiry: str = ""
    back_expiry: str = ""
    strike: float = 0.0
    entry_debit: float = 0.0
    front_dte_at_entry: int = 0
    back_dte_at_entry: int = 0
    
    # Signal info
    entry_z: float = 0.0
    direction: str = ""  # long_compression or short_compression
    
    # Exit
    exit_z: float = 0.0
    exit_credit: float = 0.0
    exit_reason: str = ""  # z_reverted, time_stop, front_expiry
    
    # PnL
    gross_pnl: float = 0.0
    commissions: float = 0.0
    net_pnl: float = 0.0
    hold_days: int = 0
    
    def to_dict(self) -> Dict:
        return {
            'trade_id': self.trade_id,
            'symbol': self.symbol,
            'signal_date': self.signal_date,
            'entry_date': self.entry_date,
            'exit_date': self.exit_date,
            'front_expiry': self.front_expiry,
            'back_expiry': self.back_expiry,
            'strike': self.strike,
            'entry_debit': self.entry_debit,
            'front_dte_at_entry': self.front_dte_at_entry,
            'back_dte_at_entry': self.back_dte_at_entry,
            'entry_z': self.entry_z,
            'direction': self.direction,
            'exit_z': self.exit_z,
            'exit_credit': self.exit_credit,
            'exit_reason': self.exit_reason,
            'gross_pnl': self.gross_pnl,
            'commissions': self.commissions,
            'net_pnl': self.net_pnl,
            'hold_days': self.hold_days,
        }


@dataclass
class CalendarBacktestConfig:
    """Configuration for calendar backtest."""
    # DTE requirements
    front_dte_min: int = 25
    front_dte_max: int = 45
    back_dte_min: int = 55
    back_dte_max: int = 90
    dte_gap_min: int = 21
    
    # Exit rules
    time_stop_front_dte: int = 7
    z_reversion_threshold: float = 0.5  # Close when z crosses this toward 0
    
    # Costs
    commission_per_contract: float = 0.65
    slippage_pct: float = 0.02


class CalendarBacktester:
    """
    Backtester for calendar spread structures.
    
    Entry: Sell front-month, buy back-month at ATM strike
    Exit: Time stop OR z-score reversion
    """
    
    def __init__(
        self,
        bar_store: OptionBarStore,
        detector: TermStructureMRDetector,
        config: Optional[CalendarBacktestConfig] = None,
    ):
        self.bar_store = bar_store
        self.detector = detector
        self.config = config or CalendarBacktestConfig()
    
    def simulate_trade(
        self,
        signal: Dict[str, Any],
    ) -> Optional[CalendarTradeResult]:
        """
        Simulate a calendar trade from signal.
        
        Args:
            signal: Dict with signal info including front/back expiry, strike, z-score
            
        Returns:
            CalendarTradeResult or None if can't simulate
        """
        sig = signal.get("signal", signal)
        symbol = sig.get("symbol", "")
        signal_date_str = sig.get("signal_date", "")
        execution_date_str = signal.get("execution_date", signal_date_str)
        
        if not symbol or not signal_date_str:
            return None
        
        try:
            signal_date = date.fromisoformat(signal_date_str)
            exec_date = date.fromisoformat(execution_date_str)
        except:
            return None
        
        front_expiry_str = sig.get("front_expiry", "")
        back_expiry_str = sig.get("back_expiry", "")
        underlying_price = sig.get("underlying_price", 0)
        entry_z = sig.get("term_z", 0)
        direction = sig.get("signal_type", "long_compression")
        
        if not front_expiry_str or not back_expiry_str or underlying_price <= 0:
            return None
        
        try:
            front_expiry = date.fromisoformat(front_expiry_str)
            back_expiry = date.fromisoformat(back_expiry_str)
        except:
            return None
        
        # Get ATM strike
        strike = round(underlying_price)
        
        # Load entry day data
        try:
            self.bar_store.load_day(exec_date)
        except Exception as e:
            return None
        
        # Get entry prices for both legs
        front_close = self._get_option_close(exec_date, symbol, front_expiry, strike, "P")
        back_close = self._get_option_close(exec_date, symbol, back_expiry, strike, "P")
        
        if front_close is None or back_close is None:
            return None
        
        # Calculate entry debit (buy back, sell front)
        # Long calendar: net_debit = back_price - front_price (should be positive)
        slippage = self.config.slippage_pct
        front_bid = front_close * (1 - slippage)
        back_ask = back_close * (1 + slippage)
        
        entry_debit = back_ask - front_bid
        
        if entry_debit <= 0:
            # Entry not viable
            return None
        
        entry_commissions = 2 * self.config.commission_per_contract
        
        front_dte_at_entry = (front_expiry - exec_date).days
        back_dte_at_entry = (back_expiry - exec_date).days
        
        # Simulate daily until exit
        current_date = exec_date + timedelta(days=1)
        exit_date = None
        exit_reason = None
        exit_z = entry_z
        
        while current_date <= front_expiry:
            # Load day data
            try:
                self.bar_store.load_day(current_date)
            except:
                current_date += timedelta(days=1)
                continue
            
            # Get current term_z
            # Use detector to get current z-score
            try:
                underlying_bar = self.bar_store.get_bar(
                    current_date, 
                    f"O:{symbol}"  # This won't work, need underlying data
                )
                # We need underlying price - for now use approximate from option prices
                current_z = self._estimate_current_z(current_date, symbol, front_expiry, back_expiry, strike)
            except:
                current_z = None
            
            # Check exit conditions
            front_dte_now = (front_expiry - current_date).days
            
            # 1. Time stop: front DTE <= threshold
            if front_dte_now <= self.config.time_stop_front_dte:
                exit_date = current_date
                exit_reason = "time_stop"
                exit_z = current_z if current_z else entry_z
                break
            
            # 2. Z-score reversion: DISABLED for now
            # The price-ratio approximation is unreliable and triggers false exits.
            # Proper implementation needs full detector with rolling history.
            # TODO: Implement proper z-score tracking by maintaining detector state
            #
            # if current_z is not None:
            #     if direction == "long_compression":
            #         if current_z <= self.config.z_reversion_threshold:
            #             exit_date = current_date
            #             exit_reason = "z_reverted"
            #             exit_z = current_z
            #             break
            #     else:
            #         if current_z >= -self.config.z_reversion_threshold:
            #             exit_date = current_date
            #             exit_reason = "z_reverted"
            #             exit_z = current_z
            #             break
            
            current_date += timedelta(days=1)
        
        # If no exit, use front expiry
        if exit_date is None:
            exit_date = front_expiry
            exit_reason = "front_expiry"
        
        # Get exit prices
        try:
            self.bar_store.load_day(exit_date)
        except:
            return None
        
        front_exit = self._get_option_close(exit_date, symbol, front_expiry, strike, "P")
        back_exit = self._get_option_close(exit_date, symbol, back_expiry, strike, "P")
        
        if front_exit is None:
            front_exit = 0.01  # Near-worthless at expiry
        if back_exit is None:
            return None
        
        # Calculate exit credit (sell back, buy front)
        front_ask = front_exit * (1 + slippage)
        back_bid = back_exit * (1 - slippage)
        
        exit_credit = back_bid - front_ask
        
        exit_commissions = 2 * self.config.commission_per_contract
        
        # Calculate PnL
        gross_pnl = (exit_credit - entry_debit) * 100
        total_commissions = entry_commissions + exit_commissions
        net_pnl = gross_pnl - total_commissions
        
        hold_days = (exit_date - exec_date).days
        
        return CalendarTradeResult(
            trade_id=f"{signal_date_str}_{symbol}_calendar",
            symbol=symbol,
            signal_date=signal_date_str,
            entry_date=exec_date.isoformat(),
            exit_date=exit_date.isoformat(),
            front_expiry=front_expiry.isoformat(),
            back_expiry=back_expiry.isoformat(),
            strike=strike,
            entry_debit=entry_debit,
            front_dte_at_entry=front_dte_at_entry,
            back_dte_at_entry=back_dte_at_entry,
            entry_z=entry_z,
            direction=direction,
            exit_z=exit_z,
            exit_credit=exit_credit,
            exit_reason=exit_reason,
            gross_pnl=gross_pnl,
            commissions=total_commissions,
            net_pnl=net_pnl,
            hold_days=hold_days,
        )
    
    def _get_option_close(
        self,
        target_date: date,
        symbol: str,
        expiry: date,
        strike: float,
        right: str,
    ) -> Optional[float]:
        """Get close price for a specific option."""
        strikes_data = self.bar_store.get_available_strikes(target_date, symbol, expiry, right=right)
        
        if not strikes_data:
            return None
        
        # Find closest strike
        available = list(strikes_data.keys())
        actual_strike = min(available, key=lambda s: abs(s - strike))
        
        bar = strikes_data.get(actual_strike, {}).get(right)
        if bar:
            return bar.get("close", 0)
        return None
    
    def _estimate_current_z(
        self,
        current_date: date,
        symbol: str,
        front_expiry: date,
        back_expiry: date,
        strike: float,
    ) -> Optional[float]:
        """
        Estimate current term z-score (simplified).
        
        In production, this would use the full detector with history.
        For now, we approximate based on IV ratio.
        """
        # Get current IVs (approximated from option prices)
        front_close = self._get_option_close(current_date, symbol, front_expiry, strike, "P")
        back_close = self._get_option_close(current_date, symbol, back_expiry, strike, "P")
        
        if front_close is None or back_close is None:
            return None
        
        # Very rough approximation: if front/back ratio is near 1, z is near 0
        # This is a placeholder - real implementation needs full IV calculation
        ratio = front_close / back_close if back_close > 0 else 1.0
        
        # Map ratio to approximate z-score (very rough)
        # ratio > 1.5 → z > 2 (front rich)
        # ratio ≈ 1.0 → z ≈ 0
        # ratio < 0.7 → z < -2 (back rich)
        if ratio > 1.5:
            return 2.0 + (ratio - 1.5) * 2
        elif ratio < 0.7:
            return -2.0 - (0.7 - ratio) * 2
        else:
            # Linear interpolation between 0.7 and 1.5
            return (ratio - 1.0) * 5  # rough mapping
    
    def run_backtest(
        self,
        signals: List[Dict],
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        Run backtest on a list of signals.
        
        Returns summary statistics.
        """
        trades = []
        skipped = 0
        
        for sig in signals:
            result = self.simulate_trade(sig)
            if result:
                trades.append(result)
                if verbose:
                    print(f"  {result.entry_date} {result.symbol}: ${result.net_pnl:.2f} ({result.exit_reason})")
            else:
                skipped += 1
        
        # Calculate metrics
        if not trades:
            return {
                "total_trades": 0,
                "skipped": skipped,
                "message": "No trades executed",
            }
        
        total_pnl = sum(t.net_pnl for t in trades)
        winners = [t for t in trades if t.net_pnl > 0]
        losers = [t for t in trades if t.net_pnl <= 0]
        
        win_rate = len(winners) / len(trades) * 100
        avg_win = sum(t.net_pnl for t in winners) / len(winners) if winners else 0
        avg_loss = sum(t.net_pnl for t in losers) / len(losers) if losers else 0
        
        gross_wins = sum(t.net_pnl for t in winners) if winners else 0
        gross_losses = abs(sum(t.net_pnl for t in losers)) if losers else 1
        profit_factor = gross_wins / gross_losses if gross_losses > 0 else float('inf')
        
        expectancy = total_pnl / len(trades)
        
        # Exit reason breakdown
        exit_reasons = defaultdict(int)
        for t in trades:
            exit_reasons[t.exit_reason] += 1
        
        return {
            "total_trades": len(trades),
            "skipped": skipped,
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "expectancy": expectancy,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "winners": len(winners),
            "losers": len(losers),
            "exit_reasons": dict(exit_reasons),
            "trades": [t.to_dict() for t in trades],
        }


def main():
    """Run calendar backtest on existing TS-MR signals."""
    import argparse
    
    parser = argparse.ArgumentParser(description="TS-MR v2 Calendar Backtest")
    parser.add_argument("--input-dir", type=str, default="logs/backfill/termstructure_mr_4yr/reports",
                        help="Directory with signal files")
    parser.add_argument("--symbols", type=str, nargs="+", default=None,
                        help="Symbols to include")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON file")
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    
    if not input_dir.exists():
        print(f"❌ Input directory not found: {input_dir}")
        return 1
    
    print("=" * 60)
    print("TS-MR v2 CALENDAR BACKTEST")
    print("=" * 60)
    print(f"Input: {input_dir}")
    print()
    
    # Load signals
    signals = []
    for f in input_dir.glob("*_TSMR.json"):
        try:
            with open(f) as fp:
                sig = json.load(fp)
            symbol = sig.get("signal", {}).get("symbol") or sig.get("symbol")
            if args.symbols and symbol not in args.symbols:
                continue
            signals.append(sig)
        except Exception as e:
            print(f"Error loading {f}: {e}")
    
    print(f"Loaded {len(signals)} signals")
    
    if not signals:
        print("No signals to backtest!")
        return 1
    
    # Initialize backtester
    bar_store = OptionBarStore(FLATFILE_CACHE, mode='thin')
    detector = TermStructureMRDetector(TermStructureMRConfig())
    config = CalendarBacktestConfig()
    
    backtester = CalendarBacktester(bar_store, detector, config)
    
    print("\nRunning calendar simulation...")
    print("-" * 40)
    
    results = backtester.run_backtest(signals, verbose=True)
    
    # Print summary
    print()
    print("=" * 60)
    print("CALENDAR BACKTEST SUMMARY")
    print("=" * 60)
    print(f"Trades: {results['total_trades']} ({results['skipped']} skipped)")
    print(f"Total PnL: ${results.get('total_pnl', 0):.2f}")
    print(f"Win Rate: {results.get('win_rate', 0):.1f}%")
    print(f"Profit Factor: {results.get('profit_factor', 0):.2f}")
    print(f"Expectancy: ${results.get('expectancy', 0):.2f}/trade")
    print()
    print("Exit Reasons:")
    for reason, count in results.get("exit_reasons", {}).items():
        print(f"  {reason}: {count}")
    
    # Save results
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved: {output_path}")
    
    # Kill criteria check
    print()
    print("=" * 60)
    print("KILL CRITERIA CHECK")
    print("=" * 60)
    pf = results.get('profit_factor', 0)
    if pf >= 1.1:
        print(f"✅ Profit Factor {pf:.2f} >= 1.1 - PASS Phase 1")
    else:
        print(f"❌ Profit Factor {pf:.2f} < 1.1 - FAIL Phase 1")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
