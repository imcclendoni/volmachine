"""
Polygon Backtest Data Provider.

Fetches historical option and underlying daily bars for backtesting.
Uses Polygon /v2/aggs/ticker/ endpoints.
"""

import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
import json

import requests

# Cache directory for backtest data
CACHE_DIR = Path(__file__).parent.parent / 'cache' / 'backtest'


def get_polygon_api_key() -> str:
    """Get Polygon API key from environment or secrets."""
    # Try environment first
    key = os.environ.get('POLYGON_API_KEY')
    if key:
        return key
    
    # Try streamlit secrets
    try:
        import streamlit as st
        if hasattr(st, 'secrets'):
            return st.secrets.get('POLYGON_API_KEY', '')
    except:
        pass
    
    # Try local secrets file
    secrets_path = Path(__file__).parent.parent / '.streamlit' / 'secrets.toml'
    if secrets_path.exists():
        import toml
        secrets = toml.load(secrets_path)
        return secrets.get('POLYGON_API_KEY', '')
    
    return ''


def occ_to_polygon_ticker(occ_symbol: str) -> str:
    """
    Convert OCC option symbol to Polygon ticker format.
    
    OCC: SPY   251220P00680000
    Polygon: O:SPY251220P00680000
    """
    # Remove spaces from OCC symbol
    clean = occ_symbol.replace(' ', '')
    return f"O:{clean}"


def polygon_ticker_to_occ(polygon_ticker: str) -> str:
    """Convert Polygon ticker back to OCC format."""
    # Remove O: prefix
    if polygon_ticker.startswith('O:'):
        return polygon_ticker[2:]
    return polygon_ticker


def get_option_daily_bars(
    occ_symbol: str,
    start_date: date,
    end_date: date,
    use_cache: bool = True,
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch daily OHLCV bars for an option from Polygon.
    
    Args:
        occ_symbol: OCC option symbol (e.g., "SPY   251220P00680000")
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        use_cache: Whether to use cached data
        
    Returns:
        List of daily bars with: date, open, high, low, close, volume
        None if no data available
    """
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return None
    
    # Convert to Polygon ticker
    ticker = occ_to_polygon_ticker(occ_symbol)
    
    # Check cache first
    if use_cache:
        cached = _load_from_cache('option', ticker, start_date, end_date)
        if cached is not None:
            return cached
    
    # Build URL
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{start_date.isoformat()}/{end_date.isoformat()}"
    )
    params = {
        'adjusted': 'true',
        'sort': 'asc',
        'limit': 5000,
        'apiKey': api_key,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if data.get('status') not in ['OK', 'DELAYED']:
            print(f"Polygon API error for {ticker}: {data.get('status', 'unknown')}")
            return None
        
        results = data.get('results', [])
        if not results:
            return None
        
        # Parse results
        bars = []
        for r in results:
            bar_date = datetime.fromtimestamp(r['t'] / 1000).date()
            bars.append({
                'date': bar_date.isoformat(),
                'open': r.get('o', 0),
                'high': r.get('h', 0),
                'low': r.get('l', 0),
                'close': r.get('c', 0),
                'volume': r.get('v', 0),
                'vwap': r.get('vw', 0),
            })
        
        # Cache results
        if use_cache:
            _save_to_cache('option', ticker, start_date, end_date, bars)
        
        return bars
        
    except Exception as e:
        print(f"Error fetching option data for {ticker}: {e}")
        return None


def get_underlying_daily_bars(
    symbol: str,
    start_date: date,
    end_date: date,
    use_cache: bool = True,
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch daily OHLCV bars for an underlying from Polygon.
    
    Args:
        symbol: Underlying symbol (e.g., "SPY")
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        use_cache: Whether to use cached data
        
    Returns:
        List of daily bars with: date, open, high, low, close, volume
        None if no data available
    """
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return None
    
    # Check cache first
    if use_cache:
        cached = _load_from_cache('underlying', symbol, start_date, end_date)
        if cached is not None:
            return cached
    
    # Build URL
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
        f"{start_date.isoformat()}/{end_date.isoformat()}"
    )
    params = {
        'adjusted': 'true',
        'sort': 'asc',
        'limit': 5000,
        'apiKey': api_key,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if data.get('status') not in ['OK', 'DELAYED']:
            print(f"Polygon API error for {symbol}: {data.get('status', 'unknown')}")
            return None
        
        results = data.get('results', [])
        if not results:
            return None
        
        # Parse results
        bars = []
        for r in results:
            bar_date = datetime.fromtimestamp(r['t'] / 1000).date()
            bars.append({
                'date': bar_date.isoformat(),
                'open': r.get('o', 0),
                'high': r.get('h', 0),
                'low': r.get('l', 0),
                'close': r.get('c', 0),
                'volume': r.get('v', 0),
                'vwap': r.get('vw', 0),
            })
        
        # Cache results
        if use_cache:
            _save_to_cache('underlying', symbol, start_date, end_date, bars)
        
        return bars
        
    except Exception as e:
        print(f"Error fetching underlying data for {symbol}: {e}")
        return None


def _get_cache_path(data_type: str, symbol: str, start: date, end: date) -> Path:
    """Get cache file path."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Clean symbol for filename
    clean_symbol = symbol.replace(':', '_').replace(' ', '')
    return CACHE_DIR / f"{data_type}_{clean_symbol}_{start}_{end}.json"


def _load_from_cache(data_type: str, symbol: str, start: date, end: date) -> Optional[List]:
    """Load data from cache if available and fresh."""
    path = _get_cache_path(data_type, symbol, start, end)
    if not path.exists():
        return None
    
    # Check if cache is fresh (less than 24 hours old)
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    if datetime.now() - mtime > timedelta(hours=24):
        return None
    
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except:
        return None


def _save_to_cache(data_type: str, symbol: str, start: date, end: date, data: List):
    """Save data to cache."""
    path = _get_cache_path(data_type, symbol, start, end)
    try:
        with open(path, 'w') as f:
            json.dump(data, f)
    except:
        pass


# Test function
def test_polygon_option_data():
    """Quick test of Polygon option data access."""
    # Test with a recent SPY option
    today = date.today()
    start = today - timedelta(days=30)
    
    # Find recent expiry
    expiry = today + timedelta(days=30)
    exp_str = expiry.strftime('%y%m%d')
    
    # Test underlying first
    print("Testing underlying (SPY)...")
    underlying_bars = get_underlying_daily_bars('SPY', start, today)
    if underlying_bars:
        print(f"  ✅ Got {len(underlying_bars)} daily bars for SPY")
        print(f"  Latest: {underlying_bars[-1]}")
    else:
        print("  ❌ No underlying data")
    
    # Test option
    print("\nTesting option data access...")
    # Try a simple ATM put
    spot = underlying_bars[-1]['close'] if underlying_bars else 600
    strike = round(spot, -1)  # Round to nearest 10
    occ = f"SPY   {exp_str}P{int(strike*1000):08d}"
    
    print(f"  Trying OCC: {occ}")
    option_bars = get_option_daily_bars(occ, start, today)
    if option_bars:
        print(f"  ✅ Got {len(option_bars)} daily bars")
        print(f"  Latest: {option_bars[-1]}")
    else:
        print("  ❌ No option data (may need Options Starter plan)")
    
    return underlying_bars is not None


if __name__ == "__main__":
    test_polygon_option_data()
