#!/usr/bin/env python3
"""
Daily Edge Processor

Runs all registered edges and generates their artifacts.
This is the single entry point for daily signal generation.

Usage:
    python3 scripts/run_daily_processor.py --date 2025-12-21
    python3 scripts/run_daily_processor.py  # Uses today's date
"""

import argparse
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent))

# Registered edges (add new edges here)
REGISTERED_EDGES = [
    'flat',
    # Future edges: 'steep', 'termstructure', etc.
]


def run_edge_processor(edge_id: str, target_date: date) -> bool:
    """Run the signal generator for a single edge."""
    print(f"\n=== Processing edge: {edge_id.upper()} ===")
    
    cmd = [
        sys.executable,
        'scripts/generate_daily_signals.py',
        '--edge', edge_id,
        '--date', target_date.isoformat(),
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
        print(result.stdout)
        if result.stderr:
            print(f"Warnings: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        print(f"Error running {edge_id}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run daily edge processor")
    parser.add_argument('--date', type=str, default=None,
                        help="Target date (YYYY-MM-DD), defaults to today")
    parser.add_argument('--edges', nargs='+', default=None,
                        help="Specific edges to process (default: all)")
    
    args = parser.parse_args()
    
    # Parse date
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        target_date = date.today()
    
    # Determine edges to process
    edges = args.edges if args.edges else REGISTERED_EDGES
    
    print(f"Daily Edge Processor - {target_date}")
    print(f"Edges to process: {edges}")
    
    # Process each edge
    results = {}
    for edge_id in edges:
        results[edge_id] = run_edge_processor(edge_id, target_date)
    
    # Summary
    print("\n=== Summary ===")
    for edge_id, success in results.items():
        status = "✅ OK" if success else "❌ FAILED"
        print(f"  {edge_id}: {status}")
    
    # Return non-zero if any failed
    if not all(results.values()):
        sys.exit(1)


if __name__ == '__main__':
    main()
