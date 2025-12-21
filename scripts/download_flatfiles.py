#!/usr/bin/env python3
"""
Download Polygon Flat Files for options day aggregates.

Downloads daily CSV files from S3 to local cache for fast backfill processing.
"""

import boto3
from datetime import date, timedelta
from pathlib import Path
import argparse
import sys


S3_ENDPOINT = "https://files.massive.com"
BUCKET = "flatfiles"
ACCESS_KEY = "894817e1-bb1b-441f-9501-2a27f7f77890"
SECRET_KEY = "lrpYXeKqUp8pBGDlbz1BdJwsmpnpiKzu"
CACHE_DIR = Path("cache/flatfiles/options_aggs")


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


def download_day(client, target_date: date) -> bool:
    """Download single day's flat file. Returns True if successful."""
    date_str = target_date.isoformat()
    key = f"us_options_opra/day_aggs_v1/{date_str[:4]}/{date_str[5:7]}/{date_str}.csv.gz"
    local = CACHE_DIR / f"{date_str}.csv.gz"
    
    if local.exists():
        return True  # Already cached
    
    try:
        client.download_file(BUCKET, key, str(local))
        return True
    except Exception as e:
        print(f"  [SKIP] {date_str}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Download Polygon Flat Files")
    parser.add_argument("--start", type=str, default="2021-12-18", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--years", type=int, default=None, help="Years to download (overrides --start)")
    args = parser.parse_args()
    
    end_date = date.fromisoformat(args.end) if args.end else date.today()
    
    if args.years:
        start_date = end_date - timedelta(days=args.years * 365)
    else:
        start_date = date.fromisoformat(args.start)
    
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"=== Polygon Flat Files Download ===")
    print(f"Period: {start_date} to {end_date}")
    print(f"Cache: {CACHE_DIR}")
    print()
    
    client = get_s3_client()
    
    current = start_date
    downloaded = 0
    skipped = 0
    cached = 0
    total = 0
    
    while current <= end_date:
        # Skip weekends
        if current.weekday() < 5:
            total += 1
            local = CACHE_DIR / f"{current.isoformat()}.csv.gz"
            
            if local.exists():
                cached += 1
            elif download_day(client, current):
                downloaded += 1
                if downloaded % 50 == 0:
                    print(f"  Downloaded {downloaded} files...")
            else:
                skipped += 1
        
        current += timedelta(days=1)
    
    print()
    print(f"=== Complete ===")
    print(f"Downloaded: {downloaded}")
    print(f"Cached: {cached}")
    print(f"Skipped: {skipped}")
    print(f"Total trading days: {total}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
