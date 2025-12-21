#!/usr/bin/env python3
"""
Structure Regeneration Tool v2 (Flat File Edition)

Rebuilds structure legs with correct strike selection (dollar width semantics).
Uses FLAT FILES for option prices - fast, deterministic, no API calls.

Key fix: width=5 now means $5 wide, not 5 increments.
"""

import json
import sys
from pathlib import Path
from datetime import date
from typing import Dict, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.option_bar_store import OptionBarStore

# Initialize BarStore with flat files location
FLATFILES_DIR = Path(__file__).parent.parent / 'cache' / 'flatfiles'
BAR_STORE = OptionBarStore(cache_dir=FLATFILES_DIR, mode='thin')

# Per-symbol strike increments
# Tier 1 ETFs: equity, rates, commodities, intl
STRIKE_INCREMENT = {
    # Equity
    'SPY': 5.0,
    'QQQ': 5.0,
    'IWM': 1.0,
    'DIA': 1.0,
    'XLF': 1.0,
    'XLK': 1.0,
    'XLE': 1.0,
    'XLU': 1.0,
    'XLP': 1.0,
    # Rates
    'TLT': 1.0,
    'IEF': 0.5,
    # Commodities
    'GLD': 1.0,
    'SLV': 0.5,
    'USO': 0.5,
    # Intl
    'EEM': 0.5,
    'FXI': 1.0,
    'EWZ': 1.0,
    'EWJ': 0.5,
}

# Target dollar widths
TARGET_WIDTHS = [5, 10]


def get_strike_increment(symbol: str) -> float:
    return STRIKE_INCREMENT.get(symbol, 5.0)


def build_polygon_ticker(symbol: str, expiry: date, strike: float, right: str) -> str:
    """Build Polygon option ticker (O:SYMBOL...)."""
    exp_str = expiry.strftime('%y%m%d')
    return f"O:{symbol}{exp_str}{right}{int(strike*1000):08d}"


def fetch_option_close(symbol: str, expiry: date, strike: float, right: str, target_date: date) -> Optional[float]:
    """Fetch option close price from flat files."""
    ticker = build_polygon_ticker(symbol, expiry, strike, right)
    bar = BAR_STORE.get_bar(target_date, ticker)
    
    if bar:
        return bar.get('close')
    return None


def rebuild_structure(
    symbol: str,
    signal_date: date,
    atm_strike: float,
    expiry: date,
    direction: str,  # 'SHORT' or 'LONG'
    target_width: int = 5,
) -> Optional[Dict]:
    """
    Rebuild spread structure with correct dollar-width semantics.
    Uses flat files for option prices.
    """
    increment = get_strike_increment(symbol)
    
    # Compute number of increments for target dollar width
    if target_width < increment:
        return None  # Can't build spread narrower than strike increment
    
    num_increments = int(target_width / increment)
    actual_width_dollars = num_increments * increment
    
    if direction == 'SHORT':
        # Credit put spread: sell OTM put, buy further OTM
        short_strike = atm_strike - increment
        long_strike = short_strike - (num_increments * increment)
    else:
        # Debit put spread: buy closer to ATM, sell further OTM
        long_strike = atm_strike - increment
        short_strike = long_strike - (num_increments * increment)
    
    # Fetch prices from flat files
    short_price = fetch_option_close(symbol, expiry, short_strike, 'P', signal_date)
    long_price = fetch_option_close(symbol, expiry, long_strike, 'P', signal_date)
    
    if short_price is None or long_price is None:
        return None  # Can't find option data in flat files
    
    # Calculate entry credit/debit and max loss
    if direction == 'SHORT':
        entry_credit = short_price - long_price
        if entry_credit <= 0:
            return None
        max_loss_dollars = (actual_width_dollars - entry_credit) * 100
        max_profit_dollars = entry_credit * 100
        
        return {
            'type': 'credit_spread',
            'spread_type': 'credit',
            'legs': [
                {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat(), 'price': short_price},
                {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat(), 'price': long_price},
            ],
            'width': actual_width_dollars,
            'width_dollars': actual_width_dollars,
            'entry_credit': entry_credit,
            'max_loss_dollars': max_loss_dollars,
            'max_profit_dollars': max_profit_dollars,
            'expiry': expiry.isoformat(),
            '_regenerated_v2': True,
        }
    else:
        entry_debit = long_price - short_price
        if entry_debit <= 0:
            return None
        max_loss_dollars = entry_debit * 100
        max_profit_dollars = (actual_width_dollars - entry_debit) * 100
        
        return {
            'type': 'debit_spread',
            'spread_type': 'debit',
            'legs': [
                {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat(), 'price': long_price},
                {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat(), 'price': short_price},
            ],
            'width': actual_width_dollars,
            'width_dollars': actual_width_dollars,
            'entry_debit': entry_debit,
            'max_loss_dollars': max_loss_dollars,
            'max_profit_dollars': max_profit_dollars,
            'expiry': expiry.isoformat(),
            '_regenerated_v2': True,
        }


def regenerate_report(report: Dict, target_width: int = 5) -> Dict:
    """Regenerate all structures in a report with correct widths."""
    candidates = report.get('candidates', [])
    
    # Get signal date from report - check multiple possible field names
    signal_date_str = (
        report.get('report_date') or 
        report.get('execution_date') or 
        report.get('date') or 
        report.get('signal_date', '')
    )
    try:
        signal_date = date.fromisoformat(str(signal_date_str)[:10])
    except:
        print(f"  ⚠ Could not parse date: {signal_date_str}")
        return report
    
    # Pre-load the day's flat file for fast lookups
    loaded = BAR_STORE.load_day(signal_date)
    if loaded == 0:
        print(f"  ⚠ No flat file for {signal_date}")
        return report
    
    for candidate in candidates:
        symbol = candidate.get('symbol', '')
        structure = candidate.get('structure', {})
        edge = candidate.get('edge', {})
        
        if not structure or not structure.get('legs'):
            continue
        
        # Extract key info from existing structure
        legs = structure.get('legs', [])
        expiry_str = structure.get('expiry') or legs[0].get('expiry', '')
        
        try:
            expiry = date.fromisoformat(str(expiry_str)[:10])
        except:
            continue
        
        # Get ATM strike from skew metrics if available
        atm_strike = candidate.get('skew_metrics', {}).get('atm_strike')
        if not atm_strike:
            # Fallback: estimate from leg strikes
            strikes = [l.get('strike', 0) for l in legs]
            atm_strike = max(strikes) + get_strike_increment(symbol)
        
        # Get direction from edge
        direction = edge.get('direction', 'SHORT')
        
        # Try to rebuild structure with cascading widths
        for width in TARGET_WIDTHS:
            new_structure = rebuild_structure(
                symbol=symbol,
                signal_date=signal_date,
                atm_strike=atm_strike,
                expiry=expiry,
                direction=direction,
                target_width=width,
            )
            if new_structure:
                candidate['structure'] = new_structure
                break
    
    # Evict day from cache to free memory
    BAR_STORE.evict_day(signal_date)
    
    return report


def regenerate_reports(input_dir: str, output_dir: str):
    """Load v5 reports, rebuild structures, save to v6."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Copy coverage files as-is
    for coverage_file in input_path.glob('coverage_*.jsonl'):
        dest = output_path / coverage_file.name
        dest.write_text(coverage_file.read_text())
        print(f"Copied: {coverage_file.name}")
    
    report_files = list(input_path.glob('*.json'))
    print(f"\nFound {len(report_files)} report files to process")
    print(f"Target widths: {TARGET_WIDTHS}")
    
    regenerated_count = 0
    success_count = 0
    fail_count = 0
    
    for report_file in report_files:
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            # Regenerate structures
            report = regenerate_report(report)
            
            # Count results
            for c in report.get('candidates', []):
                if c.get('structure', {}).get('_regenerated_v2'):
                    success_count += 1
                    legs = c.get('structure', {}).get('legs', [])
                    if legs:
                        strikes = [l['strike'] for l in legs]
                        width = abs(max(strikes) - min(strikes))
                        max_loss = c.get('structure', {}).get('max_loss_dollars', 0)
                        print(f"  ✓ {c['symbol']}: ${width:.0f} wide, max_loss=${max_loss:.0f}")
                else:
                    fail_count += 1
            
            # Save to output
            output_file = output_path / report_file.name
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            regenerated_count += 1
            
        except Exception as e:
            print(f"Error processing {report_file}: {e}")
    
    print(f"\n{'='*60}")
    print(f"✅ Regenerated {regenerated_count} reports")
    print(f"   Successful structures: {success_count}")
    print(f"   Failed structures: {fail_count}")
    print(f"   Output: {output_path}")
    print(f"\n📊 BarStore stats: {BAR_STORE.stats()}")


if __name__ == '__main__':
    input_dir = sys.argv[1] if len(sys.argv) > 1 else 'logs/backfill/v5/reports'
    output_dir = sys.argv[2] if len(sys.argv) > 2 else 'logs/backfill/v6/reports'
    
    print("=" * 60)
    print("STRUCTURE REGENERATION v2 (Flat File Edition)")
    print("=" * 60)
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Widths: {TARGET_WIDTHS} (dollar widths)")
    print(f"\nUsing flat files from: {FLATFILES_DIR}")
    print("=" * 60)
    
    regenerate_reports(input_dir, output_dir)
