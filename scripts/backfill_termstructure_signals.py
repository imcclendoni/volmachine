#!/usr/bin/env python3
"""
Backfill Term Structure Mean-Reversion Signals.

Computes historical term_z signals from Polygon flat files.
Uses rolling 120-day z-score of term_slope (front IV - back IV).

Usage:
    python3 scripts/backfill_termstructure_signals.py \
        --symbols SPY QQQ IWM \
        --years 4 \
        --output-dir logs/backfill/termstructure_mr
"""

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.option_bar_store import OptionBarStore
from edges.term_structure_mr import TermStructureMRConfig, TermStructureMRDetector


# ============================================================
# CONSTANTS
# ============================================================

FLATFILE_CACHE = Path("cache/flatfiles")

# Default Tier-1 universe (FLAT v1 exact match - 17 symbols)
# XLV excluded until Phase-1 validated
DEFAULT_SYMBOLS = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLF", "XLE", "XLK", "XLI", "XLY", "XLP", "XLU",
    "TLT", "IEF",
    "GLD", "SLV", "USO", "EEM"
]


# ============================================================
# HELPERS
# ============================================================

def get_polygon_api_key() -> str:
    """Get Polygon API key."""
    import os
    key = os.environ.get('POLYGON_API_KEY')
    if key:
        return key
    
    secrets_path = Path(__file__).parent.parent / '.streamlit' / 'secrets.toml'
    if secrets_path.exists():
        import toml
        return toml.load(secrets_path).get('POLYGON_API_KEY', '')
    return ''


def get_underlying_price_from_cache(symbol: str, target_date: date) -> Optional[float]:
    """Get underlying close price from local cache."""
    cache_path = Path(__file__).parent.parent / 'cache' / 'ohlcv' / f'{symbol}_daily.json'
    
    if not cache_path.exists():
        return None
    
    try:
        with open(cache_path) as f:
            data = json.load(f)
            bars = data.get('bars', [])
            date_str = target_date.isoformat()
            for bar in bars:
                bar_date = datetime.fromtimestamp(bar['t'] / 1000).date().isoformat()
                if bar_date == date_str:
                    return bar.get('c')
    except Exception:
        pass
    
    return None


def get_underlying_price_from_api(symbol: str, target_date: date, api_key: str) -> Optional[float]:
    """Get underlying close price from Polygon API."""
    import requests
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{target_date.isoformat()}/{target_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'false'}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        results = data.get('results', [])
        if results:
            return results[0].get('c')
    except Exception:
        pass
    
    return None


def get_trading_days(start_date: date, end_date: date, bar_store: OptionBarStore) -> List[date]:
    """Get list of trading days with flat file data."""
    trading_days = []
    current = start_date
    
    while current <= end_date:
        if bar_store.has_date(current):
            trading_days.append(current)
        current += timedelta(days=1)
    
    return trading_days


# ============================================================
# MAIN BACKFILL
# ============================================================

def backfill_symbol(
    symbol: str,
    trading_days: List[date],
    bar_store: OptionBarStore,
    detector: TermStructureMRDetector,
    api_key: str,
    output_dir: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Backfill signals for a single symbol.
    
    Returns summary statistics.
    """
    stats = {
        'symbol': symbol,
        'days_processed': 0,
        'days_with_data': 0,
        'signals_triggered': 0,
        'long_compression': 0,
        'short_compression': 0,
        'errors': 0,
    }
    
    signals_saved = []
    
    for target_date in trading_days:
        stats['days_processed'] += 1
        
        try:
            # Load day into bar store
            bar_store.load_day(target_date)
            
            # Get underlying price
            price = get_underlying_price_from_cache(symbol, target_date)
            if price is None:
                price = get_underlying_price_from_api(symbol, target_date, api_key)
            
            if price is None or price <= 0:
                continue
            
            # Detect signal
            signal = detector.detect(
                bar_store=bar_store,
                target_date=target_date,
                symbol=symbol,
                underlying_price=price,
                atm_iv_pctl=None,  # Skip IV percentile gate for backfill (not yet computed)
                vix_level=None,   # Skip VIX gate for backfill research
            )
            
            if signal is None:
                continue
            
            stats['days_with_data'] += 1
            
            if signal.is_triggered:
                stats['signals_triggered'] += 1
                
                if signal.signal_type == 'long_compression':
                    stats['long_compression'] += 1
                else:
                    stats['short_compression'] += 1
                
                # Save signal report
                report = save_signal_report(signal, output_dir)
                signals_saved.append(report)
                
                if verbose:
                    print(f"  ✅ {target_date} | {signal.signal_type} | z={signal.term_z:.2f} | "
                          f"front={signal.front_iv:.1%} ({signal.front_dte}d) | "
                          f"back={signal.back_iv:.1%} ({signal.back_dte}d)")
            
            # Evict day to free memory
            bar_store.evict_day(target_date)
            
        except Exception as e:
            stats['errors'] += 1
            if verbose:
                print(f"  ⚠ {target_date} | Error: {e}")
    
    return stats


def save_signal_report(signal, output_dir: Path) -> Path:
    """Save signal as JSON report."""
    reports_dir = output_dir / 'reports'
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Execution date is next trading day (simplified: +1 day)
    execution_date = signal.signal_date + timedelta(days=1)
    
    report = {
        'edge_type': 'term_structure_mr',
        'version': 'v1',
        'signal_date': signal.signal_date.isoformat(),
        'execution_date': execution_date.isoformat(),
        'symbol': signal.symbol,
        'signal': signal.to_dict(),
        'structure': None,  # To be filled by structure builder
        'generated_at': datetime.now().isoformat(),
    }
    
    filename = f"{execution_date.isoformat()}_{signal.symbol}_TSMR.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w') as f:
        json.dump(report, f, indent=2)
    
    return filepath


def main():
    parser = argparse.ArgumentParser(description='Backfill Term Structure MR signals')
    parser.add_argument('--symbols', nargs='+', default=DEFAULT_SYMBOLS,
                        help='Symbols to backfill')
    parser.add_argument('--years', type=int, default=4,
                        help='Number of years to backfill')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--output-dir', type=str, 
                        default='logs/backfill/termstructure_mr',
                        help='Output directory for signals')
    parser.add_argument('--z-threshold', type=float, default=2.0,
                        help='Z-score threshold for signals')
    parser.add_argument('--lookback', type=int, default=120,
                        help='Rolling lookback days for z-score')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    
    args = parser.parse_args()
    
    # Set up dates
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    else:
        end_date = date.today() - timedelta(days=1)  # Yesterday
    
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = end_date - timedelta(days=args.years * 365)
    
    # Initialize
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    bar_store = OptionBarStore(FLATFILE_CACHE, mode='thin')
    api_key = get_polygon_api_key()
    
    config = TermStructureMRConfig(
        z_threshold=args.z_threshold,
        lookback_days=args.lookback,
    )
    
    print("=" * 60)
    print("TERM STRUCTURE MR BACKFILL")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Symbols: {len(args.symbols)}")
    print(f"Z-threshold: {args.z_threshold}")
    print(f"Lookback: {args.lookback} days")
    print(f"Output: {output_dir}")
    print("=" * 60)
    
    # Get trading days
    trading_days = get_trading_days(start_date, end_date, bar_store)
    print(f"Trading days with flat files: {len(trading_days)}")
    
    if not trading_days:
        print("❌ No trading days found. Check flat file cache.")
        return
    
    # Process each symbol
    all_stats = []
    
    for symbol in args.symbols:
        print(f"\n📊 Processing {symbol}...")
        
        # Fresh detector per symbol (independent histories)
        detector = TermStructureMRDetector(config)
        
        stats = backfill_symbol(
            symbol=symbol,
            trading_days=trading_days,
            bar_store=bar_store,
            detector=detector,
            api_key=api_key,
            output_dir=output_dir,
            verbose=args.verbose,
        )
        
        all_stats.append(stats)
        
        print(f"  Days: {stats['days_with_data']}/{stats['days_processed']} | "
              f"Signals: {stats['signals_triggered']} "
              f"(⬆{stats['long_compression']} ⬇{stats['short_compression']})")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total_signals = sum(s['signals_triggered'] for s in all_stats)
    total_long = sum(s['long_compression'] for s in all_stats)
    total_short = sum(s['short_compression'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)
    
    print(f"Total signals: {total_signals}")
    print(f"  Long compression: {total_long}")
    print(f"  Short compression: {total_short}")
    print(f"Errors: {total_errors}")
    print(f"Signals per year: {total_signals / max(1, args.years):.1f}")
    
    # Save summary
    summary_path = output_dir / 'backfill_summary.json'
    summary = {
        'run_date': datetime.now().isoformat(),
        'date_range': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
        'config': {
            'z_threshold': args.z_threshold,
            'lookback_days': args.lookback,
        },
        'symbols': args.symbols,
        'trading_days': len(trading_days),
        'total_signals': total_signals,
        'long_compression': total_long,
        'short_compression': total_short,
        'per_symbol': all_stats,
    }
    
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n✅ Summary saved to: {summary_path}")


if __name__ == '__main__':
    main()
