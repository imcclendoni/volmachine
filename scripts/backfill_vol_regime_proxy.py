#!/usr/bin/env python3
"""
VIX Term Structure Backfill for VolMachine.

Fetches historical VIX-related data from Polygon and computes volatility regime.

Usage:
    python3 scripts/backfill_vix_term_structure.py --days 90
    python3 scripts/backfill_vix_term_structure.py --days 252  # 1 year

Fetches:
    - VXX (Short-term VIX futures ETN)
    - SVXY (Inverse VIX ETF - when rising, volatility is low)

Note: I:VIX indices require higher Polygon plan. Using ETFs as proxy.

Stores to:
    ./cache/market/vix_term_structure.json

Labels:
    - low_vol: VXX declining (20d change < 0)
    - high_vol: VXX rising (20d change > 10%)
    - elevated: VXX above recent range
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
import yaml


def load_polygon_api_key() -> str:
    """Load Polygon API key from environment or config."""
    api_key = os.environ.get('POLYGON_API_KEY')
    if api_key:
        return api_key
    
    secrets_path = Path('./.streamlit/secrets.toml')
    if secrets_path.exists():
        import toml
        secrets = toml.load(secrets_path)
        if 'POLYGON_API_KEY' in secrets:
            return secrets['POLYGON_API_KEY']
    
    settings_path = Path('./config/settings.yaml')
    if settings_path.exists():
        with open(settings_path) as f:
            settings = yaml.safe_load(f)
        api_key = settings.get('data', {}).get('polygon', {}).get('api_key', '')
        if api_key and not api_key.startswith('${'):
            return api_key
    
    raise ValueError("POLYGON_API_KEY not found in environment or config")


def fetch_ticker_history(symbol: str, api_key: str, start_date: str, end_date: str) -> list:
    """Fetch daily OHLCV for a ticker from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {
        'apiKey': api_key,
        'adjusted': 'true',
        'sort': 'asc',
        'limit': 50000,
    }
    
    print(f"  Fetching {symbol}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Accept both OK and DELAYED status (free plan returns DELAYED)
        if data.get('status') not in ['OK', 'DELAYED'] or not data.get('results'):
            print(f"    ⚠️ No data for {symbol}")
            return []
        
        results = []
        for bar in data['results']:
            ts = bar['t'] / 1000
            date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            results.append({
                'date': date,
                'open': bar['o'],
                'high': bar['h'],
                'low': bar['l'],
                'close': bar['c'],
                'volume': bar.get('v', 0),
            })
        
        print(f"    ✅ Got {len(results)} days")
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Error: {e}")
        return []


def compute_volatility_regime(vxx_data: list, svxy_data: list) -> dict:
    """
    Compute volatility regime labels for each date.
    
    Uses VXX (long vol) and SVXY (short vol) to determine regime:
    - low_vol: VXX declining, SVXY rising
    - high_vol: VXX rising sharply
    - normal: neutral
    """
    # Build date-indexed lookup
    vxx_by_date = {bar['date']: bar['close'] for bar in vxx_data}
    svxy_by_date = {bar['date']: bar['close'] for bar in svxy_data}
    
    # Get sorted dates
    all_dates = sorted(set(vxx_by_date.keys()) & set(svxy_by_date.keys()))
    
    if len(all_dates) < 21:
        print("  ⚠️ Not enough data for 20-day calculations")
        return {}
    
    labels = {}
    
    for i, date in enumerate(all_dates):
        if i < 20:
            continue
        
        vxx_now = vxx_by_date[date]
        vxx_20d_ago = vxx_by_date[all_dates[i - 20]]
        
        svxy_now = svxy_by_date[date]
        svxy_20d_ago = svxy_by_date[all_dates[i - 20]]
        
        # Calculate 20-day returns
        vxx_return = (vxx_now - vxx_20d_ago) / vxx_20d_ago if vxx_20d_ago else 0
        svxy_return = (svxy_now - svxy_20d_ago) / svxy_20d_ago if svxy_20d_ago else 0
        
        # Calculate 20-day SMA for VXX
        vxx_window = [vxx_by_date[all_dates[j]] for j in range(i - 19, i + 1)]
        vxx_sma = sum(vxx_window) / len(vxx_window)
        vxx_vs_sma = (vxx_now - vxx_sma) / vxx_sma if vxx_sma else 0
        
        # Determine regime
        if vxx_return > 0.15:  # VXX up >15% in 20 days = high vol
            label = 'high_vol'
        elif vxx_return < -0.10:  # VXX down >10% in 20 days = low vol
            label = 'low_vol'
        elif vxx_vs_sma > 0.10:  # VXX >10% above SMA = elevated
            label = 'elevated'
        else:
            label = 'normal'
        
        labels[date] = {
            'vxx_close': round(vxx_now, 2),
            'svxy_close': round(svxy_now, 2),
            'vxx_20d_return': round(vxx_return * 100, 1),
            'svxy_20d_return': round(svxy_return * 100, 1),
            'vxx_vs_sma_pct': round(vxx_vs_sma * 100, 1),
            'label': label,
        }
    
    return labels


def main():
    parser = argparse.ArgumentParser(description="Backfill VIX term structure history")
    parser.add_argument('--days', type=int, default=90, help='Days of history (default: 90)')
    parser.add_argument('--output', type=str, default='./cache/market/vix_term_structure.json',
                        help='Output file path')
    args = parser.parse_args()
    
    print("=" * 60)
    print("VIX Volatility Regime Backfill")
    print("=" * 60)
    print(f"Days: {args.days}")
    print(f"Output: {args.output}")
    print()
    
    try:
        api_key = load_polygon_api_key()
        print("✅ Polygon API key loaded")
    except ValueError as e:
        print(f"❌ {e}")
        return 1
    
    # Date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=args.days + 30)).strftime('%Y-%m-%d')
    
    print(f"\n📊 Fetching VIX ETF history...")
    print(f"  Range: {start_date} to {end_date}")
    
    # Fetch VXX and SVXY
    vxx_data = fetch_ticker_history('VXX', api_key, start_date, end_date)
    svxy_data = fetch_ticker_history('SVXY', api_key, start_date, end_date)
    
    if not vxx_data or not svxy_data:
        print("\n❌ Failed to fetch required data")
        return 1
    
    print(f"\n📈 Computing volatility regime labels...")
    labels = compute_volatility_regime(vxx_data, svxy_data)
    
    if not labels:
        print("❌ No labels computed")
        return 1
    
    # Count labels
    label_counts = {}
    for info in labels.values():
        label = info['label']
        label_counts[label] = label_counts.get(label, 0) + 1
    
    print(f"  Total dates: {len(labels)}")
    for label, count in sorted(label_counts.items()):
        pct = count / len(labels) * 100
        print(f"    {label}: {count} ({pct:.0f}%)")
    
    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'days_requested': args.days,
        'days_fetched': len(labels),
        'data_source': 'VXX/SVXY ETFs (I:VIX requires higher Polygon plan)',
        'label_distribution': label_counts,
        'history': labels,
    }
    
    # Save
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n✅ Saved to {output_path}")
    print(f"   File size: {output_path.stat().st_size / 1024:.1f} KB")
    
    # Show recent
    print("\n📋 Recent volatility regime (last 5 days):")
    recent = sorted(labels.keys())[-5:]
    for date in recent:
        info = labels[date]
        print(f"  {date}: VXX={info['vxx_close']:.1f}, 20d={info['vxx_20d_return']:+.0f}%, {info['label'].upper()}")
    
    print(f"\n{'=' * 60}")
    print("BACKFILL COMPLETE")
    print(f"{'=' * 60}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
