#!/usr/bin/env python3
"""
Daily Runner - One-Click Operation

Downloads latest flatfiles and runs production signals.
This is the single command to run each trading morning.

Usage:
    python scripts/run_daily.py
    python scripts/run_daily.py --days-back 3  # Download extra history
    python scripts/run_daily.py --skip-download  # Just run signals
"""

import argparse
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


def get_last_trading_day(d: date) -> date:
    """Get the most recent trading day (skip weekends)."""
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d -= timedelta(days=1)
    return d


def download_flatfiles(start_date: date, end_date: date) -> bool:
    """Download flatfiles for date range."""
    print("=" * 60)
    print("STEP 1: DOWNLOAD FLATFILES")
    print("=" * 60)
    print(f"  Period: {start_date} to {end_date}")
    print()
    
    result = subprocess.run(
        [
            sys.executable,
            "scripts/download_flatfiles.py",
            "--start", start_date.isoformat(),
            "--end", end_date.isoformat(),
        ],
        cwd=Path(__file__).parent.parent,
    )
    
    return result.returncode == 0


def run_production() -> bool:
    """Run production signal generation."""
    print()
    print("=" * 60)
    print("STEP 2: RUN PRODUCTION SIGNALS (LIVE MODE)")
    print("=" * 60)
    print()
    
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_production.py",
            "--source", "live",
        ],
        cwd=Path(__file__).parent.parent,
    )
    
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(
        description="One-click daily runner: download flatfiles + run production"
    )
    parser.add_argument(
        "--days-back", type=int, default=5,
        help="Days of history to download (default: 5)"
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Skip flatfile download, just run production"
    )
    args = parser.parse_args()
    
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║              VOLMACHINE DAILY RUNNER                     ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    
    # Determine date range - download up to today, tolerate 403s for unpublished dates
    today = date.today()
    last_trading = get_last_trading_day(today)
    start_date = last_trading - timedelta(days=args.days_back)
    
    print(f"Today: {today}")
    print(f"Target end date: {last_trading}")
    print(f"(Note: 403 errors for future dates are expected and handled)")
    print()
    
    # Step 1: Download flatfiles
    if not args.skip_download:
        if not download_flatfiles(start_date, last_trading):
            print("\n❌ Flatfile download failed!")
            return 1
        print("\n✅ Flatfiles updated")
    else:
        print("⏭️ Skipping flatfile download")
    
    # Step 2: Run production
    if not run_production():
        print("\n❌ Production run failed!")
        return 1
    
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║              ✅ DAILY RUN COMPLETE                       ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    print("Outputs:")
    print("  logs/reports/latest.json")
    print("  logs/edges/flat/latest_signals.json")
    print("  logs/edges/iv_carry_mr/latest_signals.json")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
