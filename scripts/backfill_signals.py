#!/usr/bin/env python3
"""
Backfill Historical Signals - Version 3.

Aligned with live engine (skew_extremes.py):
- Uses IV for skew calculation (not price ratio)
- steep (>=90th percentile) → direction=SHORT → credit_spread
- flat (<=10th percentile) → direction=LONG → debit_spread
- Outputs: put_iv_25d, call_iv_25d, put_call_skew, skew_percentile, is_flat/is_steep, history_mode

This uses REAL historical data from Polygon.
"""

import argparse
import sys
import json
import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ============================================================
# CONSTANTS - Match live engine config
# ============================================================

PERCENTILE_EXTREME_HIGH = 90  # Steep skew threshold
PERCENTILE_EXTREME_LOW = 10   # Flat skew threshold
TARGET_DTE = 30               # Target DTE for skew measurement
DTE_TOLERANCE = 15            # ±15 days
MIN_HISTORY_FOR_PERCENTILE = 20  # Minimum data points for valid percentile


# ============================================================
# API HELPERS
# ============================================================

def get_polygon_api_key() -> str:
    """Get Polygon API key."""
    import os
    key = os.environ.get('POLYGON_API_KEY')
    if key:
        return key
    
    secrets_path = Path(__file__).parent.parent / '.streamlit' / 'secrets.toml'
    if secrets_path.exists():
        import toml
        return toml.load(secrets_path).get('POLYGON_API_KEY', '')
    return ''


def get_underlying_price(symbol: str, as_of_date: date, api_key: str) -> Optional[float]:
    """Get underlying close price for a specific date."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{as_of_date.isoformat()}/{as_of_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'true'}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        results = data.get('results', [])
        if results:
            return results[0].get('c')
    except:
        pass
    return None


def get_option_data(occ: str, target_date: date, api_key: str) -> Optional[Dict]:
    """Get option bar data for a specific date."""
    ticker = f"O:{occ.replace(' ', '')}"
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date.isoformat()}/{target_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'true'}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        results = data.get('results', [])
        if results:
            return {
                'close': results[0].get('c', 0),
                'open': results[0].get('o', 0),
                'high': results[0].get('h', 0),
                'low': results[0].get('l', 0),
                'volume': results[0].get('v', 0),
            }
    except:
        pass
    return None


# ============================================================
# BLACK-SCHOLES IV CALCULATION
# ============================================================

def bs_call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call price."""
    if T <= 0 or sigma <= 0:
        return max(0, S - K)
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)


def bs_put_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes put price."""
    if T <= 0 or sigma <= 0:
        return max(0, K - S)
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)


def norm_cdf(x: float) -> float:
    """Standard normal CDF approximation."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def implied_volatility(
    price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: str,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> Optional[float]:
    """Calculate implied volatility using Newton-Raphson."""
    if T <= 0 or price <= 0:
        return None
    
    # Initial guess
    sigma = 0.3
    
    for _ in range(max_iter):
        if option_type == 'call':
            bs_price = bs_call_price(S, K, T, r, sigma)
        else:
            bs_price = bs_put_price(S, K, T, r, sigma)
        
        diff = bs_price - price
        
        if abs(diff) < tol:
            return sigma
        
        # Vega (approximation)
        vega = S * math.sqrt(T) * norm_pdf((math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T)))
        
        if vega < 1e-10:
            return None
        
        sigma = sigma - diff / vega
        
        if sigma <= 0.001:
            sigma = 0.001
        if sigma > 5:
            return None
    
    return sigma if 0.01 < sigma < 5 else None


def norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)


# ============================================================
# STRIKE SELECTION (25-delta approximation)
# ============================================================

def find_25_delta_strikes(spot: float, dte: int, atm_iv: float) -> Tuple[float, float]:
    """
    Approximate 25-delta put and call strikes.
    
    25-delta put is ~1 standard deviation below spot
    25-delta call is ~1 standard deviation above spot
    """
    if atm_iv <= 0 or dte <= 0:
        # Fallback: use 5% OTM
        return (round(spot * 0.95 / 5) * 5, round(spot * 1.05 / 5) * 5)
    
    T = dte / 365
    std_move = spot * atm_iv * math.sqrt(T)
    
    # 25-delta is approximately 0.67 standard deviations OTM
    put_strike = spot - 0.67 * std_move
    call_strike = spot + 0.67 * std_move
    
    # Round to nearest $5
    put_strike = round(put_strike / 5) * 5
    call_strike = round(call_strike / 5) * 5
    
    return (put_strike, call_strike)


def find_monthly_expiry(as_of: date, target_dte: int = 30, tolerance: int = 15) -> date:
    """Find monthly expiry within target DTE range."""
    # Try to find 3rd Friday of next month
    target = as_of + timedelta(days=target_dte)
    
    first_of_month = target.replace(day=1)
    first_friday = first_of_month + timedelta(days=(4 - first_of_month.weekday()) % 7)
    third_friday = first_friday + timedelta(days=14)
    
    # Check if this is within tolerance
    dte = (third_friday - as_of).days
    if abs(dte - target_dte) <= tolerance:
        return third_friday
    
    # Try next month
    next_month = first_of_month + timedelta(days=32)
    first_of_month = next_month.replace(day=1)
    first_friday = first_of_month + timedelta(days=(4 - first_of_month.weekday()) % 7)
    third_friday = first_friday + timedelta(days=14)
    
    return third_friday


def build_occ_symbol(symbol: str, expiry: date, strike: float, right: str) -> str:
    """Build OCC option symbol."""
    exp_str = expiry.strftime('%y%m%d')
    strike_int = int(strike * 1000)
    return f"{symbol.ljust(6)}{exp_str}{right}{strike_int:08d}"


# ============================================================
# SKEW CALCULATION - MATCHES LIVE ENGINE
# ============================================================

def calculate_skew_metrics(
    symbol: str,
    as_of_date: date,
    underlying_price: float,
    api_key: str,
    delay: float = 0.2,
) -> Optional[Dict]:
    """
    Calculate skew metrics matching live engine output.
    
    Returns dict with: put_iv_25d, call_iv_25d, put_call_skew, atm_iv, expiry, dte
    """
    # Find target expiry
    expiry = find_monthly_expiry(as_of_date, TARGET_DTE, DTE_TOLERANCE)
    dte = (expiry - as_of_date).days
    
    if dte < 7:
        return None
    
    T = dte / 365
    r = 0.05  # Risk-free rate assumption
    
    # Get ATM option prices to estimate ATM IV
    atm_strike = round(underlying_price / 5) * 5
    atm_call_occ = build_occ_symbol(symbol, expiry, atm_strike, 'C')
    atm_put_occ = build_occ_symbol(symbol, expiry, atm_strike, 'P')
    
    atm_call_data = get_option_data(atm_call_occ, as_of_date, api_key)
    time.sleep(delay)
    atm_put_data = get_option_data(atm_put_occ, as_of_date, api_key)
    time.sleep(delay)
    
    if not atm_call_data or not atm_put_data:
        return None
    
    # Calculate ATM IV
    atm_call_iv = implied_volatility(atm_call_data['close'], underlying_price, atm_strike, T, r, 'call')
    atm_put_iv = implied_volatility(atm_put_data['close'], underlying_price, atm_strike, T, r, 'put')
    
    if not atm_call_iv or not atm_put_iv:
        return None
    
    atm_iv = (atm_call_iv + atm_put_iv) / 2
    
    # Find 25-delta strikes
    put_25d_strike, call_25d_strike = find_25_delta_strikes(underlying_price, dte, atm_iv)
    
    # Get 25-delta option prices
    put_25d_occ = build_occ_symbol(symbol, expiry, put_25d_strike, 'P')
    call_25d_occ = build_occ_symbol(symbol, expiry, call_25d_strike, 'C')
    
    put_25d_data = get_option_data(put_25d_occ, as_of_date, api_key)
    time.sleep(delay)
    call_25d_data = get_option_data(call_25d_occ, as_of_date, api_key)
    time.sleep(delay)
    
    if not put_25d_data or not call_25d_data:
        return None
    
    # Calculate 25-delta IVs
    put_iv_25d = implied_volatility(put_25d_data['close'], underlying_price, put_25d_strike, T, r, 'put')
    call_iv_25d = implied_volatility(call_25d_data['close'], underlying_price, call_25d_strike, T, r, 'call')
    
    if not put_iv_25d or not call_iv_25d:
        return None
    
    put_call_skew = put_iv_25d - call_iv_25d
    
    return {
        'put_iv_25d': put_iv_25d,
        'call_iv_25d': call_iv_25d,
        'put_call_skew': put_call_skew,
        'atm_iv': atm_iv,
        'put_strike': put_25d_strike,
        'call_strike': call_25d_strike,
        'atm_strike': atm_strike,
        'expiry': expiry,
        'dte': dte,
        'underlying_price': underlying_price,
    }


def detect_skew_edge(
    metrics: Dict,
    skew_history: List[float],
) -> Optional[Dict]:
    """
    Detect skew edge - MATCHES LIVE ENGINE LOGIC.
    
    - Steep (>=90th percentile): direction=SHORT → credit_spread
    - Flat (<=10th percentile): direction=LONG → debit_spread
    """
    current_skew = metrics['put_call_skew']
    
    # Check for sufficient history
    if len(skew_history) < MIN_HISTORY_FOR_PERCENTILE:
        # Not enough history - cannot determine percentile
        return None
    
    # Calculate percentile
    below = sum(1 for s in skew_history if s < current_skew)
    percentile = (below / len(skew_history)) * 100
    
    # Check for steep skew (high fear premium in puts)
    if percentile >= PERCENTILE_EXTREME_HIGH:
        # Puts very expensive → Sell put premium → Credit spread
        strength = 0.6 + (percentile - PERCENTILE_EXTREME_HIGH) / 20 * 0.4
        strength = max(0.0, min(1.0, strength))
        
        return {
            'type': 'skew_extreme',
            'direction': 'SHORT',
            'structure_type': 'credit_spread',
            'strength': strength,
            'is_steep': True,
            'is_flat': False,
            'metrics': {
                'put_iv_25d': round(metrics['put_iv_25d'], 4),
                'call_iv_25d': round(metrics['call_iv_25d'], 4),
                'atm_iv': round(metrics['atm_iv'], 4),
                'put_call_skew': round(current_skew, 4),
                'skew_percentile': round(percentile, 1),
                'is_steep': 1.0,
                'is_flat': 0.0,
                'history_mode': 1.0,  # Percentile-based
            },
            'rationale': f"Steep put skew: {metrics['put_iv_25d']:.1%} put IV vs {metrics['call_iv_25d']:.1%} call IV = {current_skew:.1%} spread ({percentile:.0f}th percentile)",
        }
    
    # Check for flat skew (puts unusually cheap)
    if percentile <= PERCENTILE_EXTREME_LOW:
        # Puts cheap → Buy put protection → Debit spread
        strength = 0.5 + (PERCENTILE_EXTREME_LOW - percentile) / 20 * 0.3
        strength = max(0.0, min(1.0, strength))
        
        return {
            'type': 'skew_extreme',
            'direction': 'LONG',
            'structure_type': 'debit_spread',
            'strength': strength,
            'is_steep': False,
            'is_flat': True,
            'metrics': {
                'put_iv_25d': round(metrics['put_iv_25d'], 4),
                'call_iv_25d': round(metrics['call_iv_25d'], 4),
                'atm_iv': round(metrics['atm_iv'], 4),
                'put_call_skew': round(current_skew, 4),
                'skew_percentile': round(percentile, 1),
                'is_steep': 0.0,
                'is_flat': 1.0,
                'history_mode': 1.0,
            },
            'rationale': f"Flat put skew: {metrics['put_iv_25d']:.1%} put IV vs {metrics['call_iv_25d']:.1%} call IV = {current_skew:.1%} spread ({percentile:.0f}th percentile)",
        }
    
    # Normal skew - no edge
    return None


def build_spread_structure(edge: Dict, symbol: str, skew_metrics: Dict) -> Dict:
    """
    Build spread structure based on edge direction.
    
    - SHORT (steep) → Credit put spread: sell higher strike put, buy lower
    - LONG (flat) → Debit put spread: buy higher strike put, sell lower
    """
    direction = edge['direction']
    expiry = skew_metrics['expiry']
    underlying = skew_metrics['underlying_price']
    atm_strike = skew_metrics['atm_strike']
    
    spread_width = 5  # $5 wide
    
    if direction == 'SHORT':
        # Credit put spread (bull put): sell OTM put, buy further OTM put
        short_strike = atm_strike - 5   # Sell 5 below ATM
        long_strike = atm_strike - 10   # Buy 10 below ATM
        
        legs = [
            {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat()},
            {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat()},
        ]
        
        # Estimate credit (typically 30-40% of width for OTM spreads)
        estimated_credit = spread_width * 0.35  # ~$1.75 for $5 wide
        max_loss = (spread_width - estimated_credit) * 100  # ~$325
        max_profit = estimated_credit * 100  # ~$175
        
        return {
            'type': 'credit_spread',
            'spread_type': 'credit',
            'legs': legs,
            'width': spread_width,
            'estimated_credit': estimated_credit,
            'max_loss_dollars': max_loss,
            'max_profit_dollars': max_profit,
        }
    
    else:  # LONG
        # Debit put spread (bear put): buy closer to ATM put, sell further OTM
        long_strike = atm_strike - 5    # Buy 5 below ATM
        short_strike = atm_strike - 10  # Sell 10 below ATM
        
        legs = [
            {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat()},
            {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat()},
        ]
        
        # Estimate debit (typically 40-50% of width for OTM spreads)
        estimated_debit = spread_width * 0.45  # ~$2.25 for $5 wide
        max_loss = estimated_debit * 100  # ~$225
        max_profit = (spread_width - estimated_debit) * 100  # ~$275
        
        return {
            'type': 'debit_spread',
            'spread_type': 'debit',
            'legs': legs,
            'width': spread_width,
            'estimated_debit': estimated_debit,
            'max_loss_dollars': max_loss,
            'max_profit_dollars': max_profit,
        }


def save_backfill_report(
    report_date: date,
    symbol: str,
    edge: Dict,
    structure: Dict,
    output_dir: Path,
) -> Path:
    """Save backfilled signal as report JSON - format matches live engine."""
    report = {
        'report_date': report_date.isoformat(),
        'generated_at': datetime.now().isoformat(),
        'session': 'backfill',
        'trading_allowed': True,
        'do_not_trade_reasons': [],
        'regime': {'state': 'unknown', 'confidence': 0.5},
        'edges': [{
            'symbol': symbol,
            'edge_type': edge['type'],
            'strength': edge['strength'],
            'direction': edge['direction'],
            'metrics': edge['metrics'],
            'rationale': edge['rationale'],
        }],
        'candidates': [{
            'symbol': symbol,
            'recommendation': 'TRADE',
            'edge': {
                'type': edge['type'],
                'strength': edge['strength'],
                'direction': edge['direction'],
                'metrics': edge['metrics'],
            },
            'structure': structure,
        }],
    }
    
    filename = f"{report_date.isoformat()}_backfill.json"
    path = output_dir / filename
    
    with open(path, 'w') as f:
        json.dump(report, f, indent=2)
    
    return path


# ============================================================
# HISTORY MANAGEMENT
# ============================================================

def load_skew_history(symbol: str) -> List[float]:
    """Load historical put_call_skew values."""
    cache_path = Path(__file__).parent.parent / 'cache' / 'edges' / f'{symbol}_skew_history.json'
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                return json.load(f)
        except:
            pass
    return []


def save_skew_history(symbol: str, history: List[float]):
    """Save skew history for future percentile calculations."""
    cache_dir = Path(__file__).parent.parent / 'cache' / 'edges'
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f'{symbol}_skew_history.json'
    with open(path, 'w') as f:
        json.dump(history[-252:], f)  # Keep last 252 trading days


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical signals using IV-based skew detection (matches live engine)"
    )
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "IWM", "TLT"])
    parser.add_argument("--output", default="./logs/reports")
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--build-history", action="store_true", help="Build skew history only, no signals")
    
    args = parser.parse_args()
    
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return 1
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    end_date = date.today() - timedelta(days=1)  # Yesterday (settled data)
    start_date = end_date - timedelta(days=args.days)
    
    print(f"=== Signal Backfill v3 (IV-based, aligned with live engine) ===")
    print(f"Period: {start_date} to {end_date}")
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"Mode: {'Build History' if args.build_history else 'Detect Signals'}")
    print()
    
    signals_found = 0
    dates_processed = 0
    steep_count = 0
    flat_count = 0
    
    # Load existing history
    skew_histories = {sym: load_skew_history(sym) for sym in args.symbols}
    
    current = start_date
    while current <= end_date:
        if current.weekday() >= 5:  # Skip weekends
            current += timedelta(days=1)
            continue
        
        dates_processed += 1
        print(f"Processing {current}...", end=" ", flush=True)
        
        day_signals = []
        
        for symbol in args.symbols:
            # Get underlying price
            underlying = get_underlying_price(symbol, current, api_key)
            if not underlying:
                continue
            
            # Calculate skew metrics (with IV)
            metrics = calculate_skew_metrics(symbol, current, underlying, api_key, args.delay)
            
            if not metrics:
                continue
            
            # Record skew for history
            skew_histories[symbol].append(metrics['put_call_skew'])
            
            if not args.build_history:
                # Detect edge using history (excluding current)
                edge = detect_skew_edge(metrics, skew_histories[symbol][:-1])
                
                if edge and edge['strength'] >= 0.5:
                    structure = build_spread_structure(edge, symbol, metrics)
                    save_backfill_report(current, symbol, edge, structure, output_dir)
                    signals_found += 1
                    
                    if edge['is_steep']:
                        steep_count += 1
                        day_signals.append(f"{symbol}: STEEP→{edge['structure_type']} (p={edge['metrics']['skew_percentile']:.0f})")
                    else:
                        flat_count += 1
                        day_signals.append(f"{symbol}: FLAT→{edge['structure_type']} (p={edge['metrics']['skew_percentile']:.0f})")
        
        if day_signals:
            print(f"✅ {', '.join(day_signals)}")
        else:
            print("scanned" if not args.build_history else "recorded")
        
        current += timedelta(days=1)
    
    # Save histories
    for symbol in args.symbols:
        save_skew_history(symbol, skew_histories[symbol])
    
    print()
    print(f"=== Backfill Complete ===")
    print(f"Dates processed: {dates_processed}")
    print(f"Skew history length: {len(skew_histories.get('SPY', []))} days")
    
    if not args.build_history:
        print(f"Signals found: {signals_found}")
        print(f"  - Steep (credit_spread): {steep_count}")
        print(f"  - Flat (debit_spread): {flat_count}")
        print()
        
        if signals_found > 0:
            print(f"Now run: python3 scripts/run_backtest.py --days {args.days}")
        else:
            print("No extreme skew detected. Ensure sufficient history (run --build-history first).")
    else:
        print("History built. Run again without --build-history to detect signals.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
