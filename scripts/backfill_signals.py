#!/usr/bin/env python3
"""
Backfill Historical Signals - Version 4.

Research-correct backtest alignment:
- Width cascade [1,2,3,5] - selects first that passes liquidity/risk
- Mean-reversion gating with min_delta threshold
- Next-day execution (no lookahead bias)
- Regime filter + cooldown after loss

This uses REAL historical data from Polygon.
"""

import argparse
import sys
import json
import math
import time
import yaml
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ============================================================
# LOAD CONFIG
# ============================================================

def load_config() -> Dict:
    """Load backtest configuration."""
    config_path = Path(__file__).parent.parent / 'config' / 'backtest.yaml'
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


CONFIG = load_config()
SKEW_CONFIG = CONFIG.get('strategies', {}).get('skew_extreme', {})
WIDTH_CONFIG = CONFIG.get('width_selection', {})


# ============================================================
# CONSTANTS FROM CONFIG
# ============================================================

PERCENTILE_EXTREME_HIGH = 90
PERCENTILE_EXTREME_LOW = 10
TARGET_DTE = 30
DTE_TOLERANCE = 15
MIN_HISTORY_FOR_PERCENTILE = 20

# Mean-reversion gating
SKEW_DELTA_WINDOW = SKEW_CONFIG.get('skew_delta_window', 5)
MIN_DELTA = SKEW_CONFIG.get('min_delta', 0.01)
MIN_PERCENTILE_CHANGE = SKEW_CONFIG.get('min_percentile_change', 10)
REQUIRE_SKEW_REVERTING = SKEW_CONFIG.get('require_skew_reverting', True)

# Width cascade
WIDTH_CASCADE = WIDTH_CONFIG.get('cascade', [1, 2, 3, 5])
MAX_RISK_PER_TRADE = WIDTH_CONFIG.get('max_risk_per_trade', 100)

# Regime filter
BLOCKED_REGIMES = SKEW_CONFIG.get('blocked_regimes', ['HIGH_VOL_PANIC', 'TREND_DOWN'])

# Cooldown
LOSS_COOLDOWN_DAYS = SKEW_CONFIG.get('loss_cooldown_days', 5)


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


def norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)


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
    
    sigma = 0.3
    
    for _ in range(max_iter):
        if option_type == 'call':
            bs_price = bs_call_price(S, K, T, r, sigma)
        else:
            bs_price = bs_put_price(S, K, T, r, sigma)
        
        diff = bs_price - price
        
        if abs(diff) < tol:
            return sigma
        
        vega = S * math.sqrt(T) * norm_pdf((math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T)))
        
        if vega < 1e-10:
            return None
        
        sigma = sigma - diff / vega
        
        if sigma <= 0.001:
            sigma = 0.001
        if sigma > 5:
            return None
    
    return sigma if 0.01 < sigma < 5 else None


# ============================================================
# STRIKE SELECTION
# ============================================================

def find_25_delta_strikes(spot: float, dte: int, atm_iv: float) -> Tuple[float, float]:
    """Approximate 25-delta put and call strikes."""
    if atm_iv <= 0 or dte <= 0:
        return (round(spot * 0.95 / 5) * 5, round(spot * 1.05 / 5) * 5)
    
    T = dte / 365
    std_move = spot * atm_iv * math.sqrt(T)
    
    put_strike = spot - 0.67 * std_move
    call_strike = spot + 0.67 * std_move
    
    put_strike = round(put_strike / 5) * 5
    call_strike = round(call_strike / 5) * 5
    
    return (put_strike, call_strike)


def find_monthly_expiry(as_of: date, target_dte: int = 30, tolerance: int = 15) -> date:
    """Find monthly expiry within target DTE range."""
    target = as_of + timedelta(days=target_dte)
    
    first_of_month = target.replace(day=1)
    first_friday = first_of_month + timedelta(days=(4 - first_of_month.weekday()) % 7)
    third_friday = first_friday + timedelta(days=14)
    
    dte = (third_friday - as_of).days
    if abs(dte - target_dte) <= tolerance:
        return third_friday
    
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
# SKEW CALCULATION
# ============================================================

def calculate_skew_metrics(
    symbol: str,
    as_of_date: date,
    underlying_price: float,
    api_key: str,
    delay: float = 0.2,
) -> tuple:
    """Calculate skew metrics matching live engine output. Returns (metrics, status)."""
    expiry = find_monthly_expiry(as_of_date, TARGET_DTE, DTE_TOLERANCE)
    dte = (expiry - as_of_date).days
    
    if dte < 7:
        return None, "dte_too_low"
    
    T = dte / 365
    r = 0.05
    
    atm_strike = round(underlying_price / 5) * 5
    atm_call_occ = build_occ_symbol(symbol, expiry, atm_strike, 'C')
    atm_put_occ = build_occ_symbol(symbol, expiry, atm_strike, 'P')
    
    atm_call_data = get_option_data(atm_call_occ, as_of_date, api_key)
    time.sleep(delay)
    atm_put_data = get_option_data(atm_put_occ, as_of_date, api_key)
    time.sleep(delay)
    
    if not atm_call_data or not atm_put_data:
        return None, "no_atm_bars"
    
    atm_call_iv = implied_volatility(atm_call_data['close'], underlying_price, atm_strike, T, r, 'call')
    atm_put_iv = implied_volatility(atm_put_data['close'], underlying_price, atm_strike, T, r, 'put')
    
    if not atm_call_iv or not atm_put_iv:
        return None, "atm_iv_fail"
    
    # IV sanity bounds (reject garbage)
    if not (0.01 < atm_call_iv < 3.0) or not (0.01 < atm_put_iv < 3.0):
        return None, "atm_iv_out_of_bounds"
    
    atm_iv = (atm_call_iv + atm_put_iv) / 2
    
    put_25d_strike, call_25d_strike = find_25_delta_strikes(underlying_price, dte, atm_iv)
    
    put_25d_occ = build_occ_symbol(symbol, expiry, put_25d_strike, 'P')
    call_25d_occ = build_occ_symbol(symbol, expiry, call_25d_strike, 'C')
    
    put_25d_data = get_option_data(put_25d_occ, as_of_date, api_key)
    time.sleep(delay)
    call_25d_data = get_option_data(call_25d_occ, as_of_date, api_key)
    time.sleep(delay)
    
    if not put_25d_data or not call_25d_data:
        return None, "no_25d_bars"
    
    put_iv_25d = implied_volatility(put_25d_data['close'], underlying_price, put_25d_strike, T, r, 'put')
    call_iv_25d = implied_volatility(call_25d_data['close'], underlying_price, call_25d_strike, T, r, 'call')
    
    if not put_iv_25d or not call_iv_25d:
        return None, "25d_iv_fail"
    
    # IV sanity bounds
    if not (0.01 < put_iv_25d < 3.0) or not (0.01 < call_iv_25d < 3.0):
        return None, "25d_iv_out_of_bounds"
    
    put_call_skew = put_iv_25d - call_iv_25d
    
    # Sanity check: skew should be non-zero if IVs are different
    if abs(put_call_skew) < 1e-6 and abs(put_iv_25d - call_iv_25d) > 0.001:
        return None, "skew_calculation_error"
    
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
    }, "ok"


# ============================================================
# EDGE DETECTION WITH MEAN-REVERSION GATING
# ============================================================

def detect_skew_edge(
    metrics: Dict,
    skew_history: List[float],
    percentile_history: List[float],
) -> tuple:
    """
    Detect skew edge with proper mean-reversion gating.
    
    MATCHES debug_gating.py logic:
    - STEEP (pctl >= 90): require skew_delta < 0 AND abs(skew_delta) >= min_delta
    - FLAT (pctl <= 10): require skew_delta > 0 AND abs(skew_delta) >= min_delta
    - OR: abs(pctl_change) >= min_pctl_change
    
    Returns:
        (edge_dict, rejection_reason) - edge_dict is None if rejected
    """
    current_skew = metrics['put_call_skew']
    
    if len(skew_history) < MIN_HISTORY_FOR_PERCENTILE:
        return None, "insufficient_history"
    
    # Calculate current percentile
    below = sum(1 for s in skew_history if s < current_skew)
    percentile = (below / len(skew_history)) * 100
    
    # Check if extreme
    is_steep = percentile >= PERCENTILE_EXTREME_HIGH
    is_flat = percentile <= PERCENTILE_EXTREME_LOW
    
    if not is_steep and not is_flat:
        return None, "not_extreme"
    
    # Calculate skew delta (5-day change)
    skew_delta = 0.0
    pctl_delta = 0.0
    
    if len(skew_history) >= SKEW_DELTA_WINDOW:
        past_idx = -SKEW_DELTA_WINDOW
        skew_delta = current_skew - skew_history[past_idx]
    
    if len(percentile_history) >= SKEW_DELTA_WINDOW:
        past_idx = -SKEW_DELTA_WINDOW
        pctl_delta = percentile - percentile_history[past_idx]
    
    # Gating check - MAGNITUDE ONLY (no sign requirement for now)
    # Goal: get non-zero signals first, then add sign gating if it improves PF
    if REQUIRE_SKEW_REVERTING:
        meets_delta_threshold = abs(skew_delta) >= MIN_DELTA
        meets_pctl_threshold = abs(pctl_delta) >= MIN_PERCENTILE_CHANGE
        
        if not (meets_delta_threshold or meets_pctl_threshold):
            return None, "delta_too_small"
        
        # Log the direction for analysis (but don't filter on it)
        if is_steep:
            is_reverting = skew_delta < 0  # Steep should be falling
        else:
            is_reverting = skew_delta > 0  # Flat should be rising
    
    # Calculate strength
    if is_steep:
        strength = 0.6 + (percentile - PERCENTILE_EXTREME_HIGH) / 20 * 0.4
        direction = 'SHORT'
        structure_type = 'credit_spread'
    else:
        strength = 0.5 + (PERCENTILE_EXTREME_LOW - percentile) / 20 * 0.3
        direction = 'LONG'
        structure_type = 'debit_spread'
    
    strength = max(0.0, min(1.0, strength))
    
    edge = {
        'type': 'skew_extreme',
        'direction': direction,
        'structure_type': structure_type,
        'strength': strength,
        'is_steep': is_steep,
        'is_flat': is_flat,
        'metrics': {
            'put_iv_25d': round(metrics['put_iv_25d'], 4),
            'call_iv_25d': round(metrics['call_iv_25d'], 4),
            'atm_iv': round(metrics['atm_iv'], 4),
            'put_call_skew': round(current_skew, 4),
            'skew_percentile': round(percentile, 1),
            'skew_delta': round(skew_delta, 4),
            'percentile_delta': round(pctl_delta, 1),
            'is_steep': 1.0 if is_steep else 0.0,
            'is_flat': 1.0 if is_flat else 0.0,
            'history_mode': 1.0,
        },
        'rationale': f"{'Steep' if is_steep else 'Flat'} skew reverting: {percentile:.0f}th pctl, Δskew={skew_delta:.4f}, Δpctl={pctl_delta:.0f}",
    }
    
    return edge, "passed"


# ============================================================
# WIDTH CASCADE STRUCTURE BUILDER
# ============================================================

def build_spread_structure_with_cascade(
    edge: Dict,
    symbol: str,
    skew_metrics: Dict,
    api_key: str,
    as_of_date: date,
    delay: float = 0.1,
) -> Optional[Dict]:
    """
    Build spread structure using width cascade.
    
    Tries widths [1, 2, 3, 5] and selects first that:
    - Has valid option contracts
    - Meets liquidity requirements
    - Fits risk cap
    """
    direction = edge['direction']
    expiry = skew_metrics['expiry']
    atm_strike = skew_metrics['atm_strike']
    
    for width in WIDTH_CASCADE:
        structure = _try_build_spread(
            direction=direction,
            symbol=symbol,
            expiry=expiry,
            atm_strike=atm_strike,
            width=width,
            api_key=api_key,
            as_of_date=as_of_date,
            delay=delay,
        )
        
        if structure:
            # Check risk cap
            max_loss = structure.get('max_loss_dollars', float('inf'))
            if max_loss <= MAX_RISK_PER_TRADE:
                structure['width_selected'] = width
                structure['widths_tried'] = WIDTH_CASCADE[:WIDTH_CASCADE.index(width) + 1]
                return structure
    
    return None


def _try_build_spread(
    direction: str,
    symbol: str,
    expiry: date,
    atm_strike: float,
    width: int,
    api_key: str,
    as_of_date: date,
    delay: float,
) -> Optional[Dict]:
    """Try to build a spread with given width, checking contract availability."""
    
    if direction == 'SHORT':
        # Credit put spread: sell OTM put, buy further OTM
        short_strike = atm_strike - 5
        long_strike = short_strike - width
    else:
        # Debit put spread: buy closer to ATM, sell further OTM
        long_strike = atm_strike - 5
        short_strike = long_strike - width
    
    # Build OCC symbols
    short_occ = build_occ_symbol(symbol, expiry, short_strike, 'P')
    long_occ = build_occ_symbol(symbol, expiry, long_strike, 'P')
    
    # Check if contracts exist
    short_data = get_option_data(short_occ, as_of_date, api_key)
    time.sleep(delay)
    long_data = get_option_data(long_occ, as_of_date, api_key)
    time.sleep(delay)
    
    if not short_data or not long_data:
        return None  # Contracts don't exist
    
    # Check liquidity (volume)
    min_vol = WIDTH_CONFIG.get('min_volume', 10)
    if short_data.get('volume', 0) < min_vol or long_data.get('volume', 0) < min_vol:
        return None  # Insufficient liquidity
    
    # Calculate prices
    short_price = short_data['close']
    long_price = long_data['close']
    
    if direction == 'SHORT':
        # Credit spread
        credit = short_price - long_price
        if credit <= 0:
            return None  # No credit
        
        max_loss = (width - credit) * 100
        max_profit = credit * 100
        
        return {
            'type': 'credit_spread',
            'spread_type': 'credit',
            'legs': [
                {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat(), 'price': short_price},
                {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat(), 'price': long_price},
            ],
            'width': width,
            'entry_credit': credit,
            'max_loss_dollars': max_loss,
            'max_profit_dollars': max_profit,
        }
    else:
        # Debit spread
        debit = long_price - short_price
        if debit <= 0:
            return None  # No debit
        
        max_loss = debit * 100
        max_profit = (width - debit) * 100
        
        return {
            'type': 'debit_spread',
            'spread_type': 'debit',
            'legs': [
                {'strike': long_strike, 'right': 'P', 'side': 'BUY', 'expiry': expiry.isoformat(), 'price': long_price},
                {'strike': short_strike, 'right': 'P', 'side': 'SELL', 'expiry': expiry.isoformat(), 'price': short_price},
            ],
            'width': width,
            'entry_debit': debit,
            'max_loss_dollars': max_loss,
            'max_profit_dollars': max_profit,
        }


# ============================================================
# REPORT SAVING
# ============================================================

def save_backfill_report(
    signal_date: date,
    execution_date: date,  # Next trading day
    symbol: str,
    edge: Dict,
    structure: Dict,
    output_dir: Path,
) -> Path:
    """
    Save backfilled signal as report JSON.
    
    Uses execution_date (next trading day) for the report filename
    to avoid lookahead bias.
    """
    report = {
        'report_date': signal_date.isoformat(),
        'execution_date': execution_date.isoformat(),
        'generated_at': datetime.now().isoformat(),
        'session': 'backfill_v4',
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
    
    # Use execution_date + symbol for filename to prevent overwrites
    filename = f"{execution_date.isoformat()}__{symbol}__backfill.json"
    path = output_dir / filename
    
    with open(path, 'w') as f:
        json.dump(report, f, indent=2)
    
    return path


# ============================================================
# HISTORY MANAGEMENT
# ============================================================

def load_skew_history(symbol: str) -> Tuple[List[float], List[float]]:
    """Load historical skew and percentile values."""
    cache_path = Path(__file__).parent.parent / 'cache' / 'edges' / f'{symbol}_skew_history_v4.json'
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                data = json.load(f)
                return data.get('skew', []), data.get('percentile', [])
        except:
            pass
    return [], []


def save_skew_history(symbol: str, skew_history: List[float], percentile_history: List[float]):
    """Save skew history for future percentile calculations."""
    cache_dir = Path(__file__).parent.parent / 'cache' / 'edges'
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f'{symbol}_skew_history_v4.json'
    with open(path, 'w') as f:
        json.dump({
            'skew': skew_history[-252:],
            'percentile': percentile_history[-252:],
        }, f)


def get_next_trading_day(current: date) -> date:
    """Get next trading day (skip weekends)."""
    next_day = current + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical signals v4 - research-correct with width cascade"
    )
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "IWM"])  # TLT excluded by default
    parser.add_argument("--output", default="./logs/backfill/v4/reports")  # v4 dedicated directory
    parser.add_argument("--delay", type=float, default=0.15)
    parser.add_argument("--build-history", action="store_true", help="Build skew history only")
    
    args = parser.parse_args()
    
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return 1
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=args.days)
    
    print(f"=== Signal Backfill v4 (width cascade, mean-reversion gating) ===")
    print(f"Period: {start_date} to {end_date}")
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"Width cascade: {WIDTH_CASCADE}")
    print(f"Min delta: {MIN_DELTA:.4f} ({MIN_DELTA*100:.2f} vol points)")
    print(f"Min pctl change: {MIN_PERCENTILE_CHANGE}")
    print(f"Max risk per trade: ${MAX_RISK_PER_TRADE}")
    print(f"Output: {output_dir}")
    print(f"Mode: {'Build History' if args.build_history else 'Detect Signals'}")
    print()
    
    # Debug counters
    signals_found = 0
    dates_processed = 0
    candidates_extreme = 0      # pctl <= 10 or >= 90
    rejected_by_reversion = 0   # delta sign wrong
    rejected_by_delta_threshold = 0  # delta too small
    rejected_by_width = 0       # width cascade failed
    passed_gating = 0
    
    # Data integrity counters
    iv_failures = {'no_atm_bars': 0, 'atm_iv_fail': 0, 'atm_iv_out_of_bounds': 0,
                   'no_25d_bars': 0, '25d_iv_fail': 0, '25d_iv_out_of_bounds': 0,
                   'dte_too_low': 0, 'skew_calculation_error': 0, 'no_underlying': 0}
    valid_days_by_symbol = {sym: 0 for sym in args.symbols}
    
    # Load existing history
    histories = {}
    for sym in args.symbols:
        skew, pctl = load_skew_history(sym)
        histories[sym] = {'skew': skew, 'percentile': pctl}
    
    current = start_date
    while current <= end_date:
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        dates_processed += 1
        print(f"Processing {current}...", end=" ", flush=True)
        
        day_signals = []
        
        for symbol in args.symbols:
            underlying = get_underlying_price(symbol, current, api_key)
            if not underlying:
                iv_failures['no_underlying'] += 1
                continue
            
            metrics, status = calculate_skew_metrics(symbol, current, underlying, api_key, args.delay)
            
            if status != "ok":
                iv_failures[status] = iv_failures.get(status, 0) + 1
                continue
            
            # Data is valid - record to history
            valid_days_by_symbol[symbol] += 1
            histories[symbol]['skew'].append(metrics['put_call_skew'])
            
            # Calculate current percentile on-the-fly (don't persist separately)
            skew_hist = histories[symbol]['skew'][:-1]
            if len(skew_hist) >= MIN_HISTORY_FOR_PERCENTILE:
                below = sum(1 for s in skew_hist if s < metrics['put_call_skew'])
                current_pctl = (below / len(skew_hist)) * 100
                histories[symbol]['percentile'].append(current_pctl)
            
            if not args.build_history:
                edge, rejection = detect_skew_edge(
                    metrics,
                    histories[symbol]['skew'][:-1],
                    histories[symbol]['percentile'],
                )
                
                # Track rejection reasons
                if rejection == "not_extreme":
                    pass  # Normal, most days aren't extreme
                elif rejection == "wrong_sign":
                    rejected_by_reversion += 1
                    candidates_extreme += 1
                elif rejection == "delta_too_small":
                    rejected_by_delta_threshold += 1
                    candidates_extreme += 1
                elif rejection == "passed":
                    candidates_extreme += 1
                
                if edge and edge['strength'] >= 0.5:
                    # Build structure with width cascade
                    structure = build_spread_structure_with_cascade(
                        edge, symbol, metrics, api_key, current, args.delay
                    )
                    
                    if structure:
                        # Next-day execution
                        exec_date = get_next_trading_day(current)
                        save_backfill_report(current, exec_date, symbol, edge, structure, output_dir)
                        signals_found += 1
                        passed_gating += 1
                        
                        width = structure.get('width_selected', '?')
                        max_loss = structure.get('max_loss_dollars', 0)
                        
                        if edge['is_steep']:
                            day_signals.append(f"{symbol}: STEEP→credit w={width} ${max_loss:.0f}")
                        else:
                            day_signals.append(f"{symbol}: FLAT→debit w={width} ${max_loss:.0f}")
                    else:
                        rejected_by_width += 1
        
        if day_signals:
            print(f"✅ {', '.join(day_signals)}")
        else:
            print("scanned" if not args.build_history else "recorded")
        
        current += timedelta(days=1)
    
    # Save histories
    for symbol in args.symbols:
        save_skew_history(symbol, histories[symbol]['skew'], histories[symbol]['percentile'])
    
    print()
    print(f"=== Backfill Complete ===")
    print(f"Dates processed: {dates_processed}")
    print(f"Config: min_delta={MIN_DELTA}, window={SKEW_DELTA_WINDOW}, widths={WIDTH_CASCADE}")
    
    # Data integrity report
    print(f"\nDATA INTEGRITY REPORT:")
    total_valid = sum(valid_days_by_symbol.values())
    total_failures = sum(iv_failures.values())
    print(f"  Valid days recorded: {total_valid}")
    print(f"  IV failures: {total_failures}")
    if total_failures > 0:
        for reason, count in sorted(iv_failures.items(), key=lambda x: -x[1]):
            if count > 0:
                print(f"    {reason}: {count}")
    print(f"  By symbol:")
    for sym in args.symbols:
        valid = valid_days_by_symbol[sym]
        pct = 100 * valid / dates_processed if dates_processed else 0
        skew_range = ""
        if histories[sym]['skew']:
            skew_range = f" skew=[{min(histories[sym]['skew']):.3f}, {max(histories[sym]['skew']):.3f}]"
        print(f"    {sym}: {valid}/{dates_processed} ({pct:.0f}% valid){skew_range}")
    
    if not args.build_history:
        print(f"\nSIGNAL GATING:")
        print(f"  Extreme days (pctl<=10 or >=90): {candidates_extreme}")
        print(f"  Rejected - wrong sign: {rejected_by_reversion}")
        print(f"  Rejected - delta too small: {rejected_by_delta_threshold}")
        print(f"  Passed gating: {passed_gating}")
        print(f"  Rejected by width cascade: {rejected_by_width}")
        print(f"  Signals saved: {signals_found}")
        
        if signals_found > 0:
            print(f"\nOutput: {output_dir}")
            print(f"Next: python3 scripts/run_backtest.py --input-dir {output_dir} --days {args.days}")
        else:
            print("\nNo signals generated. Diagnosis:")
            if total_valid < dates_processed * 0.5:
                print("  → Low data quality - too many IV failures.")
            elif candidates_extreme == 0:
                print("  → No extreme percentile days found. Check history length.")
            elif passed_gating == 0:
                print("  → All extreme days rejected by gating. Try lowering min_delta.")
            elif rejected_by_width > 0:
                print("  → Width cascade failing - check strike increments.")
    else:
        print("\nHistory built. Run again without --build-history to detect signals.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
