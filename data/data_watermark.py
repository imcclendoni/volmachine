"""
Data Watermark Utility

Computes the effective trading date from flatfile data availability.
Used by production runner to determine what date to evaluate.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
import gzip


@dataclass
class DataWatermark:
    """Data availability summary."""
    data_max_date: date
    effective_date: date
    is_stale: bool
    stale_reason: Optional[str]
    files_found: int


def get_trading_day_before(d: date) -> date:
    """Get the previous trading day (skip weekends)."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:  # Saturday=5, Sunday=6
        prev -= timedelta(days=1)
    return prev


def get_data_watermark(
    flatfiles_dir: Path,
    reference_date: Optional[date] = None
) -> DataWatermark:
    """
    Compute the effective trading date from flatfile availability.
    
    Args:
        flatfiles_dir: Path to cache/flatfiles/options_aggs/
        reference_date: Date to compare against (defaults to today)
    
    Returns:
        DataWatermark with:
        - data_max_date: latest date with flatfile data
        - effective_date: date to use for trading signals (same as data_max_date)
        - is_stale: True if data_max_date < expected_max_date
        - stale_reason: explanation if stale
    """
    if reference_date is None:
        reference_date = date.today()
    
    options_dir = flatfiles_dir / "options_aggs"
    
    if not options_dir.exists():
        return DataWatermark(
            data_max_date=date(1900, 1, 1),
            effective_date=date(1900, 1, 1),
            is_stale=True,
            stale_reason="options_aggs directory not found",
            files_found=0
        )
    
    # Find all date-based files
    dates_found = []
    for f in options_dir.glob("*.csv.gz"):
        try:
            date_str = f.stem.replace(".csv", "")
            dates_found.append(date.fromisoformat(date_str))
        except ValueError:
            continue
    
    if not dates_found:
        return DataWatermark(
            data_max_date=date(1900, 1, 1),
            effective_date=date(1900, 1, 1),
            is_stale=True,
            stale_reason="no flatfiles found",
            files_found=0
        )
    
    data_max_date = max(dates_found)
    
    # Determine expected max date (last trading day)
    expected_max_date = get_trading_day_before(reference_date)
    if reference_date.weekday() < 5:  # Weekday
        # If today is a weekday, we might expect today's data (after market close)
        # But conservatively, expect at least yesterday
        pass
    
    # Check staleness (more than 3 trading days behind to allow for weekends/publishing delays)
    days_behind = (expected_max_date - data_max_date).days
    is_stale = days_behind > 3
    
    stale_reason = None
    if is_stale:
        stale_reason = f"data is {days_behind} days behind (max: {data_max_date}, expected: {expected_max_date})"
    
    return DataWatermark(
        data_max_date=data_max_date,
        effective_date=data_max_date,
        is_stale=is_stale,
        stale_reason=stale_reason,
        files_found=len(dates_found)
    )


def get_symbols_with_data(flatfiles_dir: Path, target_date: date) -> list[str]:
    """
    List symbols that have data for a given date.
    
    Reads the first few lines of the flatfile to extract unique tickers.
    """
    options_dir = flatfiles_dir / "options_aggs"
    target_file = options_dir / f"{target_date.isoformat()}.csv.gz"
    
    if not target_file.exists():
        return []
    
    symbols = set()
    try:
        with gzip.open(target_file, 'rt') as f:
            header = f.readline()  # Skip header
            for i, line in enumerate(f):
                if i > 10000:  # Sample first 10k rows
                    break
                parts = line.split(',')
                if len(parts) > 1:
                    # Extract ticker from option symbol (e.g., O:SPY240920C00500000 -> SPY)
                    ticker = parts[0].replace('O:', '').split(':')[-1]
                    # Extract base symbol (first 3-5 chars before date)
                    for j in range(3, min(6, len(ticker))):
                        if ticker[j:j+2].isdigit():
                            symbols.add(ticker[:j])
                            break
    except Exception:
        pass
    
    return sorted(symbols)


if __name__ == '__main__':
    # Quick test
    watermark = get_data_watermark(Path("cache/flatfiles"))
    print(f"Data max date: {watermark.data_max_date}")
    print(f"Effective date: {watermark.effective_date}")
    print(f"Is stale: {watermark.is_stale}")
    print(f"Stale reason: {watermark.stale_reason}")
    print(f"Files found: {watermark.files_found}")
