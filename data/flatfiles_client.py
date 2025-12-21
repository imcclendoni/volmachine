"""
Polygon Flat Files Client

Downloads daily Options aggregates from Polygon's S3-compatible endpoint.
Eliminates REST API rate limits and gaps for historical data.
"""

import os
import boto3
from pathlib import Path
from datetime import date
from typing import Optional


S3_ENDPOINT = "https://files.massive.com"
BUCKET = "flatfiles"
DEFAULT_CACHE_DIR = Path("cache/flatfiles")


class FlatFilesClient:
    """Client for downloading Polygon Flat Files via S3."""
    
    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        cache_dir: Optional[Path] = None,
    ):
        self.access_key = access_key or os.environ.get("POLYGON_S3_ACCESS_KEY")
        self.secret_key = secret_key or os.environ.get("POLYGON_S3_SECRET_KEY")
        self.cache_dir = cache_dir or DEFAULT_CACHE_DIR
        
        if not self.access_key or not self.secret_key:
            raise ValueError(
                "S3 credentials required. Set POLYGON_S3_ACCESS_KEY and "
                "POLYGON_S3_SECRET_KEY environment variables."
            )
        
        self.s3 = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    def download_options_day_aggs(self, target_date: date) -> Optional[Path]:
        """
        Download daily options aggregates CSV for a specific date.
        
        Returns path to local cached file, or None if download fails.
        """
        date_str = target_date.isoformat()
        
        # S3 path format: us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
        key = f"us_options_opra/day_aggs_v1/{date_str[:4]}/{date_str[5:7]}/{date_str}.csv.gz"
        
        # Local cache path
        local = self.cache_dir / "options_aggs" / f"{date_str}.csv.gz"
        local.parent.mkdir(parents=True, exist_ok=True)
        
        # Return cached if exists
        if local.exists():
            return local
        
        try:
            self.s3.download_file(BUCKET, key, str(local))
            return local
        except Exception as e:
            print(f"[FlatFiles] Failed to download {key}: {e}")
            return None
    
    def list_available_dates(self, year: int, month: Optional[int] = None) -> list:
        """List available dates in S3 for a given year/month."""
        prefix = f"us_options/day_aggs_v1/{year}/"
        if month:
            prefix += f"{month:02d}/"
        
        try:
            response = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            files = response.get('Contents', [])
            return [f['Key'] for f in files]
        except Exception as e:
            print(f"[FlatFiles] Failed to list {prefix}: {e}")
            return []
    
    def download_date_range(self, start_date: date, end_date: date) -> int:
        """
        Download all daily aggs for a date range.
        
        Returns count of successfully downloaded files.
        """
        from datetime import timedelta
        
        current = start_date
        downloaded = 0
        
        while current <= end_date:
            # Skip weekends
            if current.weekday() < 5:  # Monday = 0, Friday = 4
                path = self.download_options_day_aggs(current)
                if path:
                    downloaded += 1
            current += timedelta(days=1)
        
        return downloaded
