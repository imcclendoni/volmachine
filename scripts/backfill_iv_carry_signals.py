#!/usr/bin/env python3
"""
IV Carry MR Signal Backfill Script (Optimized).

Uses Polygon API for underlying prices and OptionBarStore for IV calculation.
Designed for fast signal generation across multiple years.

Usage:
    python scripts/backfill_iv_carry_signals.py \
        --symbols SPY QQQ IWM DIA XLF XLE \
        --years 4 \
        --output-dir logs/backfill/iv_carry_mr
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.option_bar_store import OptionBarStore
from data.polygon_backtest import get_underlying_daily_bars
from edges.iv_carry_mr import IVCarryMRConfig, IVCarryMRDetector

# Configuration
FLATFILE_CACHE = Path("cache/flatfiles")

DEFAULT_SYMBOLS = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLF", "XLE", "XLK", "XLI", "XLY", "XLP", "XLU",
    "TLT", "IEF",
    "GLD", "SLV", "USO", "EEM"
]


def get_price_history(
    symbol: str,
    start_date: date,
    end_date: date,
) -> Dict[date, float]:
    """
    Fetch underlying price history from Polygon (cached).
    
    Returns dict of date -> close price.
    """
    bars = get_underlying_daily_bars(symbol, start_date, end_date, use_cache=True)
    
    if not bars:
        return {}
    
    prices = {}
    for bar in bars:
        try:
            d = date.fromisoformat(bar['date'][:10])
            prices[d] = bar['close']
        except:
            pass
    
    return prices


def get_trading_days_from_prices(prices: Dict[date, float]) -> List[date]:
    """Get sorted list of trading days from price dict."""
    return sorted(prices.keys())


def backfill_symbol(
    bar_store: OptionBarStore,
    detector: IVCarryMRDetector,
    symbol: str,
    price_history: Dict[date, float],
    start_date: date,
    end_date: date,
    output_dir: Path,
) -> Dict[str, Any]:
    """Backfill signals for a single symbol."""
    stats = {
        'symbol': symbol,
        'signals': 0,
        'sell_puts': 0,
        'sell_calls': 0,
        'days_checked': 0,
        'skipped': 0,
    }
    
    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Get sorted dates
    all_dates = sorted(price_history.keys())
    
    for target_date in all_dates:
        if target_date < start_date or target_date > end_date:
            continue
        
        stats['days_checked'] += 1
        
        # Get price history up to this date
        prices_to_date = [price_history[d] for d in all_dates if d <= target_date]
        
        if len(prices_to_date) < 60:
            stats['skipped'] += 1
            continue
        
        underlying_price = prices_to_date[-1]
        
        # Try to load options data
        try:
            bar_store.load_day(target_date)
        except Exception as e:
            stats['skipped'] += 1
            continue
        
        # Detect signal
        signal = detector.detect(
            bar_store=bar_store,
            target_date=target_date,
            symbol=symbol,
            underlying_price=underlying_price,
            price_history=prices_to_date,
        )
        
        if signal and signal.is_triggered:
            stats['signals'] += 1
            
            if signal.direction == "SELL_PUTS":
                stats['sell_puts'] += 1
            else:
                stats['sell_calls'] += 1
            
            # Save signal
            exec_date = target_date + timedelta(days=1)
            report = {
                'signal_date': target_date.isoformat(),
                'execution_date': exec_date.isoformat(),
                'signal': signal.to_dict(),
            }
            
            filename = f"{target_date.isoformat()}_{symbol}_IVCMR.json"
            with open(reports_dir / filename, 'w') as f:
                json.dump(report, f, indent=2)
            
            print(f"  {target_date} {symbol}: z={signal.iv_zscore:.2f}, "
                  f"RV/IV={signal.rv_iv_ratio:.2f}, {signal.direction}")
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill IV Carry MR signals")
    parser.add_argument("--symbols", type=str, nargs="+", default=DEFAULT_SYMBOLS,
                        help="Symbols to backfill")
    parser.add_argument("--years", type=int, default=4,
                        help="Number of years to backfill")
    parser.add_argument("--output-dir", type=str, 
                        default="logs/backfill/iv_carry_mr",
                        help="Output directory")
    parser.add_argument("--z-threshold", type=float, default=2.0,
                        help="IV z-score threshold")
    parser.add_argument("--rv-iv-max", type=float, default=1.0,
                        help="Max RV/IV ratio")
    args = parser.parse_args()
    
    # Calculate date range
    end_date = date.today()
    start_date = date(end_date.year - args.years, end_date.month, end_date.day)
    warmup_start = start_date - timedelta(days=150)  # For IV history warmup
    
    print("=" * 60)
    print("IV CARRY MR SIGNAL BACKFILL")
    print("=" * 60)
    print(f"Period: {start_date} to {end_date}")
    print(f"Symbols: {args.symbols}")
    print(f"IV z-score threshold: {args.z_threshold}")
    print(f"RV/IV max: {args.rv_iv_max}")
    print()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize bar store once
    bar_store = OptionBarStore(FLATFILE_CACHE, mode='thin')
    
    config = IVCarryMRConfig(
        iv_zscore_threshold=args.z_threshold,
        rv_iv_max=args.rv_iv_max,
    )
    
    all_stats = []
    
    for symbol in args.symbols:
        print(f"\n{'='*40}")
        print(f"Processing {symbol}")
        print(f"{'='*40}")
        
        # Create fresh detector for each symbol
        detector = IVCarryMRDetector(config)
        
        # Fetch price history from Polygon
        print(f"Fetching price history from Polygon...")
        price_history = get_price_history(symbol, warmup_start, end_date)
        
        if len(price_history) < 120:
            print(f"  ⚠️ Insufficient price history ({len(price_history)} days), skipping")
            continue
        
        print(f"  Loaded {len(price_history)} price points")
        
        # Warm up IV history (before start_date)
        warmup_dates = [d for d in sorted(price_history.keys()) if d < start_date]
        print(f"Warming up IV history ({len(warmup_dates)} days)...")
        
        for warmup_date in warmup_dates:
            prices_to_date = [price_history[d] for d in sorted(price_history.keys()) if d <= warmup_date]
            if len(prices_to_date) < 60:
                continue
            
            try:
                bar_store.load_day(warmup_date)
            except:
                continue
            
            underlying_price = prices_to_date[-1]
            
            # Run detect to build IV history (ignore result)
            detector.detect(
                bar_store=bar_store,
                target_date=warmup_date,
                symbol=symbol,
                underlying_price=underlying_price,
                price_history=prices_to_date,
            )
        
        iv_history_len = len(detector.get_iv_history(symbol))
        print(f"  IV history size: {iv_history_len}")
        
        # Now backfill actual signals
        trading_days = [d for d in sorted(price_history.keys()) if start_date <= d <= end_date]
        print(f"\nScanning {len(trading_days)} trading days for signals...")
        
        stats = backfill_symbol(
            bar_store, detector, symbol, price_history, start_date, end_date, output_dir
        )
        all_stats.append(stats)
    
    # Summary
    print("\n" + "=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    
    total_signals = 0
    total_puts = 0
    total_calls = 0
    
    for stats in all_stats:
        print(f"\n{stats['symbol']}:")
        print(f"  Days checked: {stats['days_checked']}")
        print(f"  Signals: {stats['signals']} ({stats['sell_puts']} puts, {stats['sell_calls']} calls)")
        
        total_signals += stats['signals']
        total_puts += stats['sell_puts']
        total_calls += stats['sell_calls']
    
    years = args.years
    print(f"\n{'='*40}")
    print(f"TOTAL: {total_signals} signals ({total_signals/years:.1f}/year)")
    print(f"  SELL_PUTS: {total_puts}")
    print(f"  SELL_CALLS: {total_calls}")
    print(f"Output: {output_dir}")
    
    # Save summary
    summary = {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'z_threshold': args.z_threshold,
        'rv_iv_max': args.rv_iv_max,
        'symbols': args.symbols,
        'total_signals': total_signals,
        'sell_puts': total_puts,
        'sell_calls': total_calls,
        'by_symbol': all_stats,
    }
    
    with open(output_dir / "backfill_summary.json", 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nSummary saved: {output_dir / 'backfill_summary.json'}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
