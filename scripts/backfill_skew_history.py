#!/usr/bin/env python3
"""
Skew History Backfill for VolMachine.

Computes historical put_call_skew values to exit FALLBACK mode.

Usage:
    python3 scripts/backfill_skew_history.py --days 30
    python3 scripts/backfill_skew_history.py --days 60 --symbols SPY,IWM,TLT

Note: This uses historical option price data from Polygon and computes IV
using Black-Scholes inversion. If IV cannot be computed, skips that day.

Output: logs/edge_health/skew_histories.json
"""

import argparse
import json
import math
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
import yaml
from scipy.stats import norm
from scipy.optimize import brentq


def load_polygon_api_key() -> str:
    """Load Polygon API key."""
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
    
    raise ValueError("POLYGON_API_KEY not found")


def black_scholes_price(S, K, T, r, sigma, option_type='call'):
    """Calculate Black-Scholes option price."""
    if T <= 0 or sigma <= 0:
        return 0
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    if option_type == 'call':
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def implied_volatility(price, S, K, T, r, option_type='call'):
    """Calculate implied volatility using Brent's method."""
    if price <= 0 or T <= 0:
        return None
    
    try:
        def objective(sigma):
            return black_scholes_price(S, K, T, r, sigma, option_type) - price
        
        # Search between 1% and 300% IV
        iv = brentq(objective, 0.01, 3.0, xtol=1e-6)
        return iv
    except (ValueError, RuntimeError):
        return None


def get_trading_days(start: date, end: date) -> list[date]:
    """Generate list of trading days (weekdays only, simplified)."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday=0, Friday=4
            days.append(current)
        current += timedelta(days=1)
    return days


def fetch_underlying_price(symbol: str, api_key: str, as_of: date) -> float:
    """Fetch underlying price for a specific date."""
    date_str = as_of.strftime('%Y-%m-%d')
    
    # Get daily bar for that date
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{date_str}/{date_str}"
    params = {'apiKey': api_key, 'adjusted': 'false'}  # UNADJUSTED to match OPRA strikes
    
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get('status') in ['OK', 'DELAYED'] and data.get('results'):
            return data['results'][0]['c']
    except:
        pass
    
    return 0


def fetch_option_chain_summary(symbol: str, api_key: str, as_of: date, target_dte: int = 30) -> dict:
    """
    Fetch option chain and compute 25-delta put/call IV approximation.
    
    Returns dict with put_iv_25d, call_iv_25d, atm_iv, or None if failed.
    """
    date_str = as_of.strftime('%Y-%m-%d')
    
    # Calculate target expiration (approximately target_dte days out)
    target_exp = as_of + timedelta(days=target_dte)
    exp_str = target_exp.strftime('%Y-%m-%d')
    
    # Get underlying price
    spot = fetch_underlying_price(symbol, api_key, as_of)
    if spot <= 0:
        return None
    
    # Find ATM strikes
    atm_strike = round(spot)
    
    # Build option tickers for puts and calls
    # Format: O:SPY251219P00595000
    exp_yyyymmdd = target_exp.strftime('%Y%m%d')[2:]  # YYMMDD
    
    # Get options at various strikes to find 25 delta approximation
    # For puts: ~5-7% OTM, for calls: ~5-7% OTM
    put_strike = round(spot * 0.93)  # ~7% OTM put
    call_strike = round(spot * 1.07)  # ~7% OTM call
    
    # Time to expiry in years
    T = target_dte / 365.0
    r = 0.045  # Risk-free rate
    
    results = {}
    
    # Fetch ATM options
    for option_type in ['P', 'C']:
        strike_str = f"{atm_strike * 1000:08d}"
        ticker = f"O:{symbol}{exp_yyyymmdd}{option_type}{strike_str}"
        
        # Get daily bar for that date
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_str}/{date_str}"
        params = {'apiKey': api_key, 'adjusted': 'false'}  # UNADJUSTED
        
        try:
            r_resp = requests.get(url, params=params, timeout=15)
            data = r_resp.json()
            if data.get('status') in ['OK', 'DELAYED'] and data.get('results'):
                mid_price = (data['results'][0]['h'] + data['results'][0]['l']) / 2
                iv = implied_volatility(mid_price, spot, atm_strike, T, r, 
                                         'call' if option_type == 'C' else 'put')
                if iv:
                    results[f'atm_{option_type.lower()}'] = iv
        except:
            pass
    
    # Fetch OTM puts (25 delta approx)
    for option_type, strike in [('P', put_strike), ('C', call_strike)]:
        strike_str = f"{strike * 1000:08d}"
        ticker = f"O:{symbol}{exp_yyyymmdd}{option_type}{strike_str}"
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_str}/{date_str}"
        params = {'apiKey': api_key, 'adjusted': 'false'}  # UNADJUSTED
        
        try:
            r_resp = requests.get(url, params=params, timeout=15)
            data = r_resp.json()
            if data.get('status') in ['OK', 'DELAYED'] and data.get('results'):
                mid_price = (data['results'][0]['h'] + data['results'][0]['l']) / 2
                iv = implied_volatility(mid_price, spot, strike, T, r,
                                         'call' if option_type == 'C' else 'put')
                if iv:
                    results[f'otm_{option_type.lower()}'] = iv
        except:
            pass
    
    # Compute final metrics
    put_iv = results.get('otm_p') or results.get('atm_p')
    call_iv = results.get('otm_c') or results.get('atm_c')
    atm_iv = (results.get('atm_p', 0) + results.get('atm_c', 0)) / 2 if results.get('atm_p') and results.get('atm_c') else None
    
    if put_iv and call_iv:
        return {
            'put_iv_25d': put_iv,
            'call_iv_25d': call_iv,
            'atm_iv': atm_iv or (put_iv + call_iv) / 2,
            'put_call_skew': put_iv - call_iv,
        }
    
    return None


def main():
    parser = argparse.ArgumentParser(description="Backfill skew history")
    parser.add_argument('--days', type=int, default=90, help='Days of history (default: 90)')
    parser.add_argument('--symbols', type=str, default='ALL_ENABLED',
                        help='Comma-separated symbols or ALL_ENABLED (default: ALL_ENABLED)')
    parser.add_argument('--output', type=str, default='./logs/edge_health/skew_histories.json')
    parser.add_argument('--min-history-days', type=int, default=30)
    args = parser.parse_args()
    
    # Resolve symbols
    if args.symbols.upper() == 'ALL_ENABLED':
        # Read from universe.yaml
        universe_path = Path('./config/universe.yaml')
        if universe_path.exists():
            with open(universe_path) as f:
                universe = yaml.safe_load(f)
            symbols = [
                s for s, cfg in universe.get('symbols', {}).items()
                if cfg.get('enabled', False)
            ]
            print(f"📂 Loaded {len(symbols)} enabled symbols from universe.yaml")
        else:
            symbols = ['SPY', 'QQQ', 'IWM', 'TLT']
            print("⚠️ universe.yaml not found, using defaults")
    else:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
    
    print("=" * 60)
    print("Skew History Backfill")
    print("=" * 60)
    print(f"Days: {args.days}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Output: {args.output}")
    print()
    
    try:
        api_key = load_polygon_api_key()
        print("✅ Polygon API key loaded")
    except ValueError as e:
        print(f"❌ {e}")
        return 1
    
    # Load existing history
    output_path = Path(args.output)
    if output_path.exists():
        with open(output_path) as f:
            history = json.load(f)
        print(f"📂 Loaded existing history: {len(history)} symbols")
    else:
        history = {}
    
    # Generate trading days
    end_date = date.today() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=args.days + 10)  # Buffer
    trading_days = get_trading_days(start_date, end_date)[-args.days:]
    
    print(f"\n📅 Processing {len(trading_days)} trading days: {trading_days[0]} to {trading_days[-1]}")
    
    # Process each symbol
    for symbol in symbols:
        print(f"\n📊 {symbol}:")
        
        if symbol not in history:
            history[symbol] = []
        
        success_count = 0
        
        for day in trading_days:
            metrics = fetch_option_chain_summary(symbol, api_key, day)
            
            if metrics and 'put_call_skew' in metrics:
                skew = metrics['put_call_skew']
                history[symbol].append(skew)
                success_count += 1
                
                if success_count <= 3 or success_count == len(trading_days):
                    print(f"    {day}: skew={skew:.4f} (put_iv={metrics['put_iv_25d']:.2%}, call_iv={metrics['call_iv_25d']:.2%})")
                elif success_count == 4:
                    print(f"    ... (processing)")
        
        # Trim to last 252 days
        history[symbol] = history[symbol][-252:]
        
        print(f"    ✅ Added {success_count} days, total history: {len(history[symbol])}")
        
        # Check if we meet minimum
        if len(history[symbol]) >= args.min_history_days:
            print(f"    🎯 history_mode=1 (>= {args.min_history_days} days)")
        else:
            print(f"    ⚠️ history_mode=0 ({len(history[symbol])} < {args.min_history_days} days)")
    
    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(history, f, indent=2)
    
    print(f"\n✅ Saved to {output_path}")
    
    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    for symbol, skews in history.items():
        status = "✅ READY" if len(skews) >= args.min_history_days else "⚠️ FALLBACK"
        print(f"  {symbol}: {len(skews)} days {status}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
