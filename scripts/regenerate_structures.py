#!/usr/bin/env python3
"""
Structure-Only Regeneration Script

Reads v5 reports and regenerates structures with corrected width semantics.
No data re-ingestion - just fixes the width bug.

Bug fixed: width was interpreted as "increment count" not "dollar width"
- Old: width=5 with $5 increment → $25 spread
- New: width=5 with $5 increment → $5 spread (1 increment apart)
"""

import json
import os
from pathlib import Path
from datetime import date
from typing import Dict, Optional

# Per-symbol strike increments
STRIKE_INCREMENT = {
    'SPY': 5.0,
    'QQQ': 5.0,
    'IWM': 1.0,
    'XLF': 0.50,
    'XLE': 0.50,
    'XLK': 1.0,
    'EEM': 0.50,
    'GLD': 1.0,
    'TLT': 1.0,
    'DIA': 1.0,
}

def get_strike_increment(symbol: str) -> float:
    return STRIKE_INCREMENT.get(symbol, 5.0)


def rebuild_structure(structure: Dict, symbol: str) -> Dict:
    """Rebuild structure with corrected width semantics."""
    legs = structure.get('legs', [])
    if len(legs) < 2:
        return structure  # Can't fix without legs
    
    # Extract strikes from legs
    strikes = [leg.get('strike', 0) for leg in legs if leg.get('strike')]
    if len(strikes) < 2:
        return structure
    
    # Calculate actual width in dollars
    short_strike = max(strikes)
    long_strike = min(strikes)
    actual_width_dollars = abs(short_strike - long_strike)
    
    # Get prices from legs
    short_leg = next((l for l in legs if l.get('strike') == short_strike), None)
    long_leg = next((l for l in legs if l.get('strike') == long_strike), None)
    
    if not short_leg or not long_leg:
        return structure
    
    short_price = short_leg.get('price', 0)
    long_price = long_leg.get('price', 0)
    
    spread_type = structure.get('spread_type', 'credit')
    
    if spread_type == 'credit':
        entry_credit = short_price - long_price
        max_loss_dollars = (actual_width_dollars - entry_credit) * 100
        max_profit_dollars = entry_credit * 100
        
        return {
            **structure,
            'width': actual_width_dollars,  # Now in dollars
            'width_dollars': actual_width_dollars,
            'entry_credit': entry_credit,
            'max_loss_dollars': max_loss_dollars,
            'max_profit_dollars': max_profit_dollars,
            '_regenerated': True,
        }
    else:
        # Debit spread
        entry_debit = long_price - short_price
        max_loss_dollars = entry_debit * 100
        max_profit_dollars = (actual_width_dollars - entry_debit) * 100
        
        return {
            **structure,
            'width': actual_width_dollars,
            'width_dollars': actual_width_dollars,
            'entry_debit': entry_debit,
            'max_loss_dollars': max_loss_dollars,
            'max_profit_dollars': max_profit_dollars,
            '_regenerated': True,
        }


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
    
    regenerated_count = 0
    candidate_count = 0
    
    for report_file in report_files:
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            candidates = report.get('candidates', [])
            
            for candidate in candidates:
                symbol = candidate.get('symbol', '')
                structure = candidate.get('structure', {})
                
                if structure and structure.get('legs'):
                    candidate['structure'] = rebuild_structure(structure, symbol)
                    candidate_count += 1
            
            # Save to output
            output_file = output_path / report_file.name
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            regenerated_count += 1
            
        except Exception as e:
            print(f"Error processing {report_file}: {e}")
    
    print(f"\n✅ Regenerated {regenerated_count} reports")
    print(f"   Rebuilt {candidate_count} candidate structures")
    print(f"   Output: {output_path}")


if __name__ == '__main__':
    import sys
    
    input_dir = sys.argv[1] if len(sys.argv) > 1 else 'logs/backfill/v5/reports'
    output_dir = sys.argv[2] if len(sys.argv) > 2 else 'logs/backfill/v6/reports'
    
    print("=" * 60)
    print("STRUCTURE-ONLY REGENERATION")
    print("=" * 60)
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"\nFix: width is now DOLLAR width, not increment count")
    print("=" * 60)
    
    regenerate_reports(input_dir, output_dir)
