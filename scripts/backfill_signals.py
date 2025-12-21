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

from data.option_bar_store import OptionBarStore

# Flat file cache directory
FLATFILE_CACHE = Path("cache/flatfiles")

# Global bar store (initialized in main())
BAR_STORE: Optional[OptionBarStore] = None


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
MIN_HISTORY_FOR_PERCENTILE = 60

# Mean-reversion gating
SKEW_DELTA_WINDOW = SKEW_CONFIG.get('skew_delta_window', 5)
MIN_DELTA = SKEW_CONFIG.get('min_delta', 0.005)  # 0.5 vol points default
MIN_PERCENTILE_CHANGE = SKEW_CONFIG.get('min_percentile_change', 10)
REQUIRE_SKEW_REVERTING = SKEW_CONFIG.get('require_skew_reverting', True)

# Valid rejection reasons (enum-like)
VALID_REJECTION_REASONS = {
    'insufficient_history',
    'not_extreme', 
    'skew_not_reverting',
    'delta_too_small',
    'passed',
}

# Width cascade
WIDTH_CASCADE = WIDTH_CONFIG.get('cascade', [1, 2, 3, 5])
MAX_RISK_PER_TRADE = WIDTH_CONFIG.get('max_risk_per_trade', 100)

# Regime filter
BLOCKED_REGIMES = SKEW_CONFIG.get('blocked_regimes', ['HIGH_VOL_PANIC', 'TREND_DOWN'])

# Cooldown
LOSS_COOLDOWN_DAYS = SKEW_CONFIG.get('loss_cooldown_days', 5)

# Per-symbol strike increment (default $5 for SPY/QQQ/IWM)
# Full 21-symbol universe with correct increments
STRIKE_INCREMENT = {
    # US Broad
    'SPY': 5.0,
    'QQQ': 5.0,
    'IWM': 1.0,
    'DIA': 1.0,
    # Sectors
    'XLF': 1.0,
    'XLE': 1.0,
    'XLK': 1.0,
    'XLI': 1.0,
    'XLY': 1.0,
    'XLP': 1.0,
    'XLU': 1.0,
    # Rates
    'TLT': 1.0,
    'IEF': 0.5,
    'HYG': 0.5,
    'LQD': 0.5,
    # Commodities
    'GLD': 1.0,
    'SLV': 0.5,
    'USO': 0.5,
    'UUP': 0.5,
    # Intl
    'EEM': 0.5,
    'EFA': 1.0,
}

def get_strike_increment(symbol: str) -> float:
    """Get strike increment for a symbol (default $5 for unknown)."""
    return STRIKE_INCREMENT.get(symbol, 5.0)


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
    """Get underlying close price for a specific date.
    
    IMPORTANT: Uses UNADJUSTED prices to match OPRA option strike coordinates.
    Split-adjusted prices would mismatch with unadjusted option strikes.
    
    Priority:
    1. Local OHLCV cache (cache/ohlcv/{symbol}_daily.json)
    2. REST API fallback
    """
    # Try local cache first
    cache_path = Path(__file__).parent.parent / 'cache' / 'ohlcv' / f'{symbol}_daily.json'
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                data = json.load(f)
                bars = data.get('bars', [])
                date_str = as_of_date.isoformat()
                for bar in bars:
                    # Polygon uses milliseconds timestamp
                    bar_date = datetime.fromtimestamp(bar['t'] / 1000).date().isoformat()
                    if bar_date == date_str:
                        return bar.get('c')
        except:
            pass
    
    # REST fallback - use UNADJUSTED prices to match OPRA strikes
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{as_of_date.isoformat()}/{as_of_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'false'}
    
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
    """
    Get option bar data for a specific date.
    
    Priority:
    1. Flat file store (global BAR_STORE if available)
    2. REST API fallback (only for recent days)
    """
    global BAR_STORE
    ticker = f"O:{occ.replace(' ', '')}"
    
    # Try flat file store first (for historical data)
    if BAR_STORE is not None:
        bar = BAR_STORE.get_bar(target_date, ticker)
        if bar:
            return bar
    
    # Check if date is recent (within 5 days) - use REST for fresh data
    days_ago = (date.today() - target_date).days
    
    # For historical data (>5 days), if no flat file, return None (don't hit REST)
    if BAR_STORE is not None and days_ago > 5:
        return None  # Flat file should have it; if missing, treat as no data
    
    # REST fallback for recent days only
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date.isoformat()}/{target_date.isoformat()}"
    params = {'apiKey': api_key, 'adjusted': 'false'}  # UNADJUSTED to match OPRA
    
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

def find_25_delta_strikes(spot: float, dte: int, atm_iv: float, increment: float = 5.0) -> Tuple[float, float]:
    """Approximate 25-delta put and call strikes using per-symbol increment."""
    if atm_iv <= 0 or dte <= 0:
        return (round(spot * 0.95 / increment) * increment, round(spot * 1.05 / increment) * increment)
    
    T = dte / 365
    std_move = spot * atm_iv * math.sqrt(T)
    
    put_strike = spot - 0.67 * std_move
    call_strike = spot + 0.67 * std_move
    
    put_strike = round(put_strike / increment) * increment
    call_strike = round(call_strike / increment) * increment
    
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


def find_strike_with_fallback(symbol: str, expiry: date, base_strike: float, right: str,
                               as_of_date: date, api_key: str, delay: float = 0.2,
                               max_steps: int = 10, step_size: float = None) -> Tuple[Optional[Dict], float, bool]:
    """
    Try to find option data at base_strike, then search ±1 to ±max_steps.
    
    Returns: (option_data, actual_strike, used_fallback)
    """
    # Use per-symbol increment if step_size not explicitly provided
    if step_size is None:
        step_size = get_strike_increment(symbol)
    # Try primary strike first
    occ = build_occ_symbol(symbol, expiry, base_strike, right)
    data = get_option_data(occ, as_of_date, api_key)
    if data and data.get('close', 0) > 0:
        return data, base_strike, False
    
    time.sleep(delay)
    
    # Try fallback ladder: ±1, ±2, ... ±max_steps
    for step in range(1, max_steps + 1):
        # For puts, search lower first; for calls, search higher first
        if right == 'P':
            offsets = [-step * step_size, step * step_size]  # Lower, then higher
        else:  # C
            offsets = [step * step_size, -step * step_size]  # Higher, then lower
        
        for offset in offsets:
            try_strike = base_strike + offset
            if try_strike <= 0:
                continue
            
            occ = build_occ_symbol(symbol, expiry, try_strike, right)
            data = get_option_data(occ, as_of_date, api_key)
            time.sleep(delay * 0.5)  # Shorter delay for fallback searches
            
            if data and data.get('close', 0) > 0:
                return data, try_strike, True
    
    return None, base_strike, False


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
    
    # Chain-driven expiry selection: use available expiries from flat file
    # Checks that expiry is USABLE (has valid ATM call+put pair)
    expiry, dte = BAR_STORE.find_best_expiry(
        as_of_date, symbol, TARGET_DTE, DTE_TOLERANCE, spot=underlying_price
    )
    
    if expiry is None:
        return None, "no_available_expiry"
    
    if dte < 7:
        return None, "dte_too_low"
    
    T = dte / 365
    r = 0.05
    
    # Chain-driven ATM selection: find strike with both call+put having data
    actual_atm_strike, atm_call_data, atm_put_data = BAR_STORE.find_atm_strike(
        as_of_date, symbol, expiry, underlying_price
    )
    
    if actual_atm_strike is None:
        # Log diagnostic info
        available_expiries = BAR_STORE.get_available_expiries(as_of_date, symbol)
        available_strikes = BAR_STORE.get_available_strikes(as_of_date, symbol, expiry)
        print(f"  ⚠ no_atm_bars: {symbol} {as_of_date} | underlying=${underlying_price:.2f} | "
              f"expiry={expiry} (dte={dte}) | available_expiries={len(available_expiries)} | "
              f"strikes_for_expiry={len(available_strikes)}")
        return None, "no_atm_bars"
    
    # Derive increment from chain
    increment = BAR_STORE.derive_increment(as_of_date, symbol, expiry, underlying_price)
    base_atm_strike = round(underlying_price / increment) * increment
    
    # Track ATM fallback
    used_atm_fallback = abs(actual_atm_strike - base_atm_strike) > 0.01
    atm_fallback_distance = abs(actual_atm_strike - base_atm_strike)
    
    # Mark INVALID if ATM fallback distance too large (>5 * increment)
    if atm_fallback_distance > 5:
        return None, "atm_fallback_too_far"
    
    atm_call_iv = implied_volatility(atm_call_data['close'], underlying_price, actual_atm_strike, T, r, 'call')
    atm_put_iv = implied_volatility(atm_put_data['close'], underlying_price, actual_atm_strike, T, r, 'put')
    
    if not atm_call_iv or not atm_put_iv:
        return None, "atm_iv_fail"
    
    # IV sanity bounds (reject garbage)
    if not (0.01 < atm_call_iv < 3.0) or not (0.01 < atm_put_iv < 3.0):
        return None, "atm_iv_out_of_bounds"
    
    atm_iv = (atm_call_iv + atm_put_iv) / 2
    
    put_25d_strike, call_25d_strike = find_25_delta_strikes(underlying_price, dte, atm_iv, increment)
    
    # Use fallback ladder for 25-delta strikes
    put_25d_data, actual_put_strike, put_used_fallback = find_strike_with_fallback(
        symbol, expiry, put_25d_strike, 'P', as_of_date, api_key, delay
    )
    call_25d_data, actual_call_strike, call_used_fallback = find_strike_with_fallback(
        symbol, expiry, call_25d_strike, 'C', as_of_date, api_key, delay
    )
    
    if not put_25d_data or not call_25d_data:
        return None, "no_25d_bars"
    
    # Use actual strikes for IV calculation
    put_iv_25d = implied_volatility(put_25d_data['close'], underlying_price, actual_put_strike, T, r, 'put')
    call_iv_25d = implied_volatility(call_25d_data['close'], underlying_price, actual_call_strike, T, r, 'call')
    
    if not put_iv_25d or not call_iv_25d:
        return None, "25d_iv_fail"
    
    # IV sanity bounds
    if not (0.01 < put_iv_25d < 3.0) or not (0.01 < call_iv_25d < 3.0):
        return None, "25d_iv_out_of_bounds"
    
    put_call_skew = put_iv_25d - call_iv_25d
    
    # Sanity check: skew should be non-zero if IVs are different
    if abs(put_call_skew) < 1e-6 and abs(put_iv_25d - call_iv_25d) > 0.001:
        return None, "skew_calculation_error"
    
    used_fallback = put_used_fallback or call_used_fallback
    
    return {
        'put_iv_25d': put_iv_25d,
        'call_iv_25d': call_iv_25d,
        'put_call_skew': put_call_skew,
        'atm_iv': atm_iv,
        'put_strike': actual_put_strike,
        'call_strike': actual_call_strike,
        'atm_strike': actual_atm_strike,
        'original_atm_strike': base_atm_strike,
        'expiry': expiry,
        'dte': dte,
        'underlying_price': underlying_price,
        # 25-delta fallback tracking
        'used_fallback_strike': used_fallback,
        'original_put_strike': put_25d_strike,
        'original_call_strike': call_25d_strike,
        # ATM fallback tracking
        'used_atm_fallback': used_atm_fallback,
        'atm_fallback_distance': atm_fallback_distance,
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
    
    # Gating check - REQUIRE REVERTING CONFIRMATION (OR condition)
    # Either raw skew or percentile must be reverting
    if REQUIRE_SKEW_REVERTING:
        if is_steep:
            # STEEP: require either skew falling OR pctl stopped rising
            is_reverting = (skew_delta < 0) or (pctl_delta <= 0)
        else:
            # FLAT: require either skew rising OR pctl stopped falling
            is_reverting = (skew_delta > 0) or (pctl_delta >= 0)
        
        if not is_reverting:
            return None, "skew_not_reverting"
        
        # Then check magnitude threshold
        meets_delta_threshold = abs(skew_delta) >= MIN_DELTA
        meets_pctl_threshold = abs(pctl_delta) >= MIN_PERCENTILE_CHANGE
        
        if not (meets_delta_threshold or meets_pctl_threshold):
            return None, "delta_too_small"
    
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
    width: int,  # This is now interpreted as DOLLAR WIDTH (e.g., 5 = $5 wide)
    api_key: str,
    as_of_date: date,
    delay: float,
) -> Optional[Dict]:
    """Try to build a spread with given width, checking contract availability."""
    
    # Get per-symbol strike increment (not hardcoded $5)
    increment = get_strike_increment(symbol)
    
    # FIXED: width is now DOLLAR WIDTH, not increment count
    # Compute how many increments we need to achieve this width
    # For example: width=5 with increment=5 -> 1 increment apart
    #              width=5 with increment=1 -> 5 increments apart
    if width < increment:
        return None  # Cannot build spread narrower than strike increment
    
    num_increments = int(width / increment)
    if num_increments < 1:
        return None
    
    # actual_width_dollars is what we'll actually get (might round to increment)
    actual_width_dollars = num_increments * increment
    
    if direction == 'SHORT':
        # Credit put spread: sell OTM put, buy further OTM
        short_strike = atm_strike - increment
        long_strike = short_strike - (num_increments * increment)
        option_right = 'P'  # PUT for credit spread
    else:
        # FLAT: CALL debit spread - buy call at ATM, sell call above
        # Per EDGE_FLAT_v1.md spec: long ATM call, short ATM+width call
        long_strike = atm_strike
        short_strike = atm_strike + (num_increments * increment)
        option_right = 'C'  # CALL for debit spread
    
    # Build OCC symbols
    short_occ = build_occ_symbol(symbol, expiry, short_strike, option_right)
    long_occ = build_occ_symbol(symbol, expiry, long_strike, option_right)
    
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
        
        # FIXED: Use actual_width_dollars, not width (which is increment count)
        max_loss = (actual_width_dollars - credit) * 100
        max_profit = credit * 100
        
        return {
            'type': 'credit_spread',
            'spread_type': 'credit',
            'legs': [
                {'strike': short_strike, 'right': option_right, 'side': 'SELL', 'expiry': expiry.isoformat(), 'price': short_price},
                {'strike': long_strike, 'right': option_right, 'side': 'BUY', 'expiry': expiry.isoformat(), 'price': long_price},
            ],
            'width': actual_width_dollars,  # Store actual dollar width for clarity
            'width_increments': width,       # Store original increment count
            'entry_credit': credit,
            'max_loss_dollars': max_loss,
            'max_profit_dollars': max_profit,
        }
    else:
        # Debit spread (CALL for FLAT)
        debit = long_price - short_price
        if debit <= 0:
            return None  # No debit
        
        # FIXED: Use actual_width_dollars, not width
        max_loss = debit * 100
        max_profit = (actual_width_dollars - debit) * 100
        
        return {
            'type': 'call_debit_spread',  # Correct type per spec
            'spread_type': 'debit',
            'legs': [
                {'strike': long_strike, 'right': option_right, 'side': 'BUY', 'expiry': expiry.isoformat(), 'price': long_price},
                {'strike': short_strike, 'right': option_right, 'side': 'SELL', 'expiry': expiry.isoformat(), 'price': short_price},
            ],
            'width': actual_width_dollars,  # Store actual dollar width for clarity
            'width_increments': width,       # Store original increment count
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


def load_iv_history(symbol: str) -> List[float]:
    """Load historical ATM IV values for percentile calculation."""
    cache_path = Path(__file__).parent.parent / 'cache' / 'edges' / f'{symbol}_iv_history_v4.json'
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                data = json.load(f)
                return data.get('atm_iv', [])
        except:
            pass
    return []


def save_iv_history(symbol: str, iv_history: List[float]):
    """Save ATM IV history for regime filtering."""
    cache_dir = Path(__file__).parent.parent / 'cache' / 'edges'
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f'{symbol}_iv_history_v4.json'
    with open(path, 'w') as f:
        json.dump({
            'atm_iv': iv_history[-252:],  # Keep 1 year of history
        }, f)


def compute_iv_percentile(current_iv: float, iv_history: List[float], window: int = 60) -> Optional[float]:
    """
    Compute ATM IV percentile using trailing window (no lookahead).
    
    Returns percentile (0-100) or None if insufficient history.
    """
    if len(iv_history) < window:
        return None
    
    # Use last 'window' observations
    recent = iv_history[-window:]
    below = sum(1 for iv in recent if iv < current_iv)
    return (below / len(recent)) * 100


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
    parser.add_argument("--days", type=int, default=90, help="Number of days to backfill")
    parser.add_argument("--years", type=int, default=None, help="Number of years to backfill (overrides --days)")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (overrides --days/--years)")
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "IWM"])
    parser.add_argument("--output", default="./logs/backfill/v4/reports")
    parser.add_argument("--delay", type=float, default=0.15)
    parser.add_argument("--build-history", action="store_true", help="Build skew history only")
    parser.add_argument("--resume", action="store_true", default=True, help="Skip dates where all symbols already have reports")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Process all dates even if reports exist")
    parser.add_argument("--checkpoint-every", type=int, default=25, help="Save history to cache every N days")
    parser.add_argument("--fresh-history", action="store_true", help="Ignore cached history, start fresh (required for multi-year validation)")
    parser.add_argument("--phase", choices=["phase1", "phase2", "phase3"], default=None,
                        help="Phase preset: phase1=edge_validation, phase2=tradeability, phase3=optimization")
    
    args = parser.parse_args()
    
    # Load phase configuration if specified
    PHASE_CONFIG = {}
    if args.phase:
        PHASE_CONFIG = CONFIG.get('phases', {}).get(args.phase, {})
        phase_desc = PHASE_CONFIG.get('description', args.phase)
        print(f"\n{'='*60}")
        print(f"PHASE: {args.phase.upper()} - {phase_desc}")
        print(f"{'='*60}")
        if args.phase == 'phase1':
            print("⚠️  RiskEngine: DISABLED (validation mode)")
            print(f"⚠️  Credit gate: {PHASE_CONFIG.get('min_credit_to_width', 0.15)*100:.0f}% (relaxed)")
        else:
            print(f"✅ RiskEngine: ENABLED")
            print(f"✅ Credit gate: {PHASE_CONFIG.get('min_credit_to_width', 0.20)*100:.0f}%")
        print()
    
    api_key = get_polygon_api_key()
    if not api_key:
        print("ERROR: No Polygon API key found")
        return 1
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Calculate date range
    end_date = date.today() - timedelta(days=1)
    if args.start:
        start_date = date.fromisoformat(args.start)
    elif args.years:
        start_date = end_date - timedelta(days=args.years * 365)
    else:
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
    
    # Initialize flat file store for option bar lookups
    global BAR_STORE
    BAR_STORE = OptionBarStore(FLATFILE_CACHE)
    print(f"Flat Files: {FLATFILE_CACHE}/options_aggs/")
    print()
    
    # Debug counters
    signals_found = 0
    dates_processed = 0
    candidates_extreme = 0      # pctl <= 10 or >= 90
    rejected_insufficient_history = 0  # not enough days for percentile
    rejected_not_extreme = 0     # pctl in 10-90 range
    rejected_by_reversion = 0   # delta sign wrong
    rejected_by_delta_threshold = 0  # delta too small
    rejected_by_width = 0        # width cascade failed
    rejected_by_credit_quality = 0  # credit too small vs width
    passed_gating = 0
    
    # Percentile distribution tracking (after warm-up)
    pctl_stats = {sym: {'min': 100, 'max': 0, 'low': 0, 'high': 0, 'count': 0} for sym in args.symbols}
    
    # Data integrity counters
    iv_failures = {'no_atm_bars': 0, 'atm_iv_fail': 0, 'atm_iv_out_of_bounds': 0,
                   'no_25d_bars': 0, '25d_iv_fail': 0, '25d_iv_out_of_bounds': 0,
                   'dte_too_low': 0, 'skew_calculation_error': 0, 'no_underlying': 0}
    valid_days_by_symbol = {sym: 0 for sym in args.symbols}
    
    # Monthly missingness heatmap: {sym: {month: {reason: count, valid: count}}}
    missingness_by_month = {sym: {} for sym in args.symbols}
    
    # Coverage JSONL: logs/backfill/v4/coverage_{start}_{end}.jsonl
    coverage_file = output_dir / f"coverage_{start_date.isoformat()}_{end_date.isoformat()}.jsonl"
    coverage_records = []  # Will write at end
    
    # Candidate funnel JSONL: logs/backfill/v4/candidates_{start}_{end}.jsonl
    candidates_file_path = output_dir / f"candidates_{start_date.isoformat()}_{end_date.isoformat()}.jsonl"
    candidates_file = open(candidates_file_path, 'w')
    
    def write_candidate(dt: date, symbol: str, skew: float, percentile: float, 
                        is_steep: bool, is_flat: bool, rejection_reason: str,
                        edge: dict = None, structure: dict = None, 
                        structure_fail_reason: str = None, saved: bool = False):
        """Write candidate to funnel JSONL."""
        row = {
            'date': dt.isoformat(),
            'symbol': symbol,
            'skew': round(skew, 4),
            'percentile': round(percentile, 1),
            'is_steep': is_steep,
            'is_flat': is_flat,
            'rejection_reason': rejection_reason,
            'structure_status': 'built' if structure else ('failed' if edge else 'not_attempted'),
            'structure_fail_reason': structure_fail_reason,
            'credit_to_width': round(structure.get('entry_credit', 0) / structure.get('width_selected', 5), 3) if structure and structure.get('width_selected') else None,
            'max_loss': structure.get('max_loss_dollars') if structure else None,
            'width_selected': structure.get('width_selected') if structure else None,
            'skew_delta': edge.get('skew_delta') if edge else None,
            'pctl_delta': edge.get('pctl_delta') if edge else None,
            'saved_as_signal': saved,
        }
        candidates_file.write(json.dumps(row) + '\n')
    
    def record_coverage(symbol: str, dt: date, status: str, failure_reason: str = None, details: dict = None):
        """Record coverage status for a symbol/day."""
        record = {
            'date': dt.isoformat(),
            'symbol': symbol,
            'status': status,  # 'VALID' or 'INVALID'
            'failure_reason': failure_reason,
            'details': details or {}
        }
        coverage_records.append(record)
        
        # Also track in monthly heatmap
        month_key = dt.strftime('%Y-%m')
        if month_key not in missingness_by_month[symbol]:
            missingness_by_month[symbol][month_key] = {'valid': 0}
        if status == 'VALID':
            missingness_by_month[symbol][month_key]['valid'] = missingness_by_month[symbol][month_key].get('valid', 0) + 1
        elif failure_reason:
            missingness_by_month[symbol][month_key][failure_reason] = missingness_by_month[symbol][month_key].get(failure_reason, 0) + 1
    
    # Load existing history (or start fresh for multi-year validation)
    histories = {}
    iv_histories = {}  # ATM IV history for regime filtering
    for sym in args.symbols:
        if args.fresh_history:
            # Start with empty history - required for uncontaminated multi-year validation
            histories[sym] = {'skew': [], 'percentile': []}
            iv_histories[sym] = []
        else:
            skew, pctl = load_skew_history(sym)
            histories[sym] = {'skew': skew, 'percentile': pctl}
            iv_histories[sym] = load_iv_history(sym)
            if skew:
                print(f"  ⚠️  Loaded cached history for {sym}: {len(skew)} skew days, {len(iv_histories[sym])} IV days")
    
    if args.fresh_history:
        print("  → Fresh history mode: building from scratch (no cache contamination)")
    print()
    
    current = start_date
    checkpoint_counter = 0
    skipped_resume = 0
    
    while current <= end_date:
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        # Skip market holidays: only process days with actual flat file data
        if not BAR_STORE.has_date(current):
            current += timedelta(days=1)
            continue
        
        # Resume mode: skip if all symbols already have reports for this date
        if args.resume and not args.build_history:
            execution_date = get_next_trading_day(current)
            all_exist = True
            for sym in args.symbols:
                report_path = output_dir / f"{execution_date.isoformat()}__{sym}__backfill.json"
                if not report_path.exists():
                    all_exist = False
                    break
            if all_exist:
                skipped_resume += 1
                current += timedelta(days=1)
                continue
        
        dates_processed += 1
        checkpoint_counter += 1
        print(f"Processing {current}...", end=" ", flush=True)
        
        # Load all options for this day (O(1) lookups for rest of day)
        BAR_STORE.load_day(current)
        
        day_signals = []
        
        for symbol in args.symbols:
            underlying = get_underlying_price(symbol, current, api_key)
            if not underlying:
                iv_failures['no_underlying'] += 1
                record_coverage(symbol, current, 'INVALID', 'no_underlying')
                continue
            
            metrics, status = calculate_skew_metrics(symbol, current, underlying, api_key, args.delay)
            
            if status != "ok":
                iv_failures[status] = iv_failures.get(status, 0) + 1
                record_coverage(symbol, current, 'INVALID', status)
                continue
            
            # Validate skew is sane (not NaN, not extreme)
            skew_val = metrics.get('put_call_skew', 0)
            if not (isinstance(skew_val, (int, float)) and abs(skew_val) < 1.0):
                iv_failures['skew_calculation_error'] = iv_failures.get('skew_calculation_error', 0) + 1
                record_coverage(symbol, current, 'INVALID', 'skew_out_of_bounds', {'skew': skew_val})
                continue
            
            # Data is valid - record to history and coverage
            valid_days_by_symbol[symbol] += 1
            coverage_details = {
                'skew': skew_val,
                'used_fallback_strike': metrics.get('used_fallback_strike', False),
            }
            if metrics.get('used_fallback_strike'):
                coverage_details['original_put_strike'] = metrics.get('original_put_strike')
                coverage_details['actual_put_strike'] = metrics.get('put_strike')
                coverage_details['original_call_strike'] = metrics.get('original_call_strike')
                coverage_details['actual_call_strike'] = metrics.get('call_strike')
            record_coverage(symbol, current, 'VALID', None, coverage_details)
            histories[symbol]['skew'].append(metrics['put_call_skew'])
            
            # Track ATM IV history for regime filtering
            atm_iv = metrics.get('atm_iv', 0)
            iv_histories[symbol].append(atm_iv)
            
            # Compute ATM IV percentile (trailing, no lookahead)
            iv_hist_before_today = iv_histories[symbol][:-1]
            current_iv_pctl = compute_iv_percentile(atm_iv, iv_hist_before_today, window=60)
            metrics['atm_iv_percentile'] = current_iv_pctl  # None if insufficient history
            
            # Calculate current percentile on-the-fly (don't persist separately)
            skew_hist = histories[symbol]['skew'][:-1]
            if len(skew_hist) >= MIN_HISTORY_FOR_PERCENTILE:
                below = sum(1 for s in skew_hist if s < metrics['put_call_skew'])
                current_pctl = (below / len(skew_hist)) * 100
                histories[symbol]['percentile'].append(current_pctl)
                
                # Track percentile distribution (after warm-up)
                pctl_stats[symbol]['count'] += 1
                pctl_stats[symbol]['min'] = min(pctl_stats[symbol]['min'], current_pctl)
                pctl_stats[symbol]['max'] = max(pctl_stats[symbol]['max'], current_pctl)
                if current_pctl <= PERCENTILE_EXTREME_LOW:
                    pctl_stats[symbol]['low'] += 1
                if current_pctl >= PERCENTILE_EXTREME_HIGH:
                    pctl_stats[symbol]['high'] += 1
            
            if not args.build_history:
                edge, rejection = detect_skew_edge(
                    metrics,
                    histories[symbol]['skew'][:-1],        # History before today
                    histories[symbol]['percentile'][:-1],  # History before today (aligned)
                )
                
                # Debug assertion: rejection must be a valid key
                assert rejection in VALID_REJECTION_REASONS, f"Invalid rejection: {rejection}"
                
                # Track rejection reasons
                if rejection == "insufficient_history":
                    rejected_insufficient_history += 1
                elif rejection == "not_extreme":
                    rejected_not_extreme += 1
                elif rejection == "skew_not_reverting":
                    rejected_by_reversion += 1
                    candidates_extreme += 1
                elif rejection == "delta_too_small":
                    rejected_by_delta_threshold += 1
                    candidates_extreme += 1
                elif rejection == "passed":
                    candidates_extreme += 1
                
                if edge and edge['strength'] >= 0.5:
                    # Inject ATM IV percentile into edge metrics (for regime filtering)
                    if current_iv_pctl is not None:
                        edge['metrics']['atm_iv_percentile'] = round(current_iv_pctl, 1)
                    
                    # Build structure with width cascade
                    structure = build_spread_structure_with_cascade(
                        edge, symbol, metrics, api_key, current, args.delay
                    )
                    
                    if structure:
                        # Credit quality gate: ONLY for credit spreads
                        spread_type = structure.get('type', '')
                        entry_credit = structure.get('entry_credit', 0)
                        width = structure.get('width_selected', 5)
                        credit_to_width = entry_credit / width if width > 0 else 0
                        
                        # Only apply credit gate to credit spreads
                        if 'credit' in spread_type:
                            MIN_CREDIT_TO_WIDTH = PHASE_CONFIG.get('min_credit_to_width',
                                SKEW_CONFIG.get('min_credit_to_width', 0.20))
                            if credit_to_width < MIN_CREDIT_TO_WIDTH:
                                rejected_by_credit_quality += 1
                                write_candidate(current, symbol, metrics['put_call_skew'], current_pctl,
                                    current_pctl >= PERCENTILE_EXTREME_HIGH, current_pctl <= PERCENTILE_EXTREME_LOW,
                                    'credit_quality_fail', edge, structure, 'credit_to_width_too_low')
                                continue
                        # Debit spreads: no credit gate (could add separate validation if needed)
                        
                        # Next-day execution
                        exec_date = get_next_trading_day(current)
                        save_backfill_report(current, exec_date, symbol, edge, structure, output_dir)
                        signals_found += 1
                        passed_gating += 1
                        
                        # Write to candidate funnel as saved
                        write_candidate(current, symbol, metrics['put_call_skew'], current_pctl,
                            current_pctl >= PERCENTILE_EXTREME_HIGH, current_pctl <= PERCENTILE_EXTREME_LOW,
                            'passed', edge, structure, None, saved=True)
                        
                        width = structure.get('width_selected', '?')
                        max_loss = structure.get('max_loss_dollars', 0)
                        
                        if edge['is_steep']:
                            day_signals.append(f"{symbol}: STEEP→credit w={width} ${max_loss:.0f}")
                        else:
                            day_signals.append(f"{symbol}: FLAT→debit w={width} ${max_loss:.0f}")
                    else:
                        rejected_by_width += 1
                        write_candidate(current, symbol, metrics['put_call_skew'], current_pctl,
                            current_pctl >= PERCENTILE_EXTREME_HIGH, current_pctl <= PERCENTILE_EXTREME_LOW,
                            'width_cascade_failed', edge, None, 'no_valid_width')
                else:
                    # Write non-passing candidates to funnel
                    if len(histories[symbol]['skew']) > MIN_HISTORY_FOR_PERCENTILE:
                        write_candidate(current, symbol, metrics['put_call_skew'], current_pctl,
                            current_pctl >= PERCENTILE_EXTREME_HIGH, current_pctl <= PERCENTILE_EXTREME_LOW,
                            rejection)
        
        if day_signals:
            print(f"✅ {', '.join(day_signals)}")
        else:
            print("scanned" if not args.build_history else "recorded")
        
        # Evict day from cache to free memory
        BAR_STORE.evict_day(current)
        
        # Checkpoint: save history periodically to survive interruptions
        if checkpoint_counter >= args.checkpoint_every:
            for sym in args.symbols:
                save_skew_history(sym, histories[sym]['skew'], histories[sym]['percentile'])
                save_iv_history(sym, iv_histories[sym])
            checkpoint_counter = 0
            print(f"  [Checkpoint saved - {dates_processed} days processed]")
        
        current += timedelta(days=1)
    
    # Final save of histories
    for symbol in args.symbols:
        save_skew_history(symbol, histories[symbol]['skew'], histories[symbol]['percentile'])
        save_iv_history(symbol, iv_histories[symbol])
    
    print()
    print(f"=== Backfill Complete ===")
    print(f"Dates processed: {dates_processed}")
    if skipped_resume > 0:
        print(f"Dates skipped (resume): {skipped_resume}")
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
        print(f"\\nREJECTION BREAKDOWN:")
        print(f"  insufficient_history:  {rejected_insufficient_history}")
        print(f"  not_extreme:           {rejected_not_extreme}")
        print(f"  skew_not_reverting:    {rejected_by_reversion}")
        print(f"  delta_too_small:       {rejected_by_delta_threshold}")
        print(f"  credit_quality_fail:   {rejected_by_credit_quality}")
        print(f"  structure_fail:        {rejected_by_width}")
        print(f"  signals_saved:         {signals_found}")
        print()
        
        # Percentile distribution debug (after warm-up)
        print(f"PERCENTILE DISTRIBUTION (after warm-up):")
        for sym in args.symbols:
            stats = pctl_stats[sym]
            if stats['count'] > 0:
                print(f"  {sym}: n={stats['count']}, range=[{stats['min']:.1f}, {stats['max']:.1f}], "
                      f"<=10: {stats['low']}, >=90: {stats['high']}")
            else:
                print(f"  {sym}: n=0 (still in warm-up)")
        print()
        
        print(f"SIGNAL SUMMARY:")
        print(f"  Extreme days (pctl<=10 or >=90): {candidates_extreme}")
        print(f"  Passed gating:                   {passed_gating}")
        print(f"  Signals saved:                   {signals_found}")
        
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
    
    # Write coverage JSONL file
    if coverage_records:
        with open(coverage_file, 'w') as f:
            for record in coverage_records:
                f.write(json.dumps(record) + '\n')
        print(f"\nCoverage log: {coverage_file}")
    
    # Write missingness heatmap
    heatmap_file = output_dir / f"missingness_heatmap_{start_date.isoformat()}_{end_date.isoformat()}.json"
    with open(heatmap_file, 'w') as f:
        json.dump(missingness_by_month, f, indent=2)
    print(f"Missingness heatmap: {heatmap_file}")
    
    # Print coverage summary with clear pass/fail for each symbol
    print("\n" + "=" * 60)
    print("COVERAGE SUMMARY")
    print("=" * 60)
    trading_days = dates_processed  # Approximation - weekdays processed
    min_coverage_threshold = 0.90
    all_pass = True
    for sym in args.symbols:
        valid = valid_days_by_symbol[sym]
        coverage_rate = valid / trading_days if trading_days else 0
        status_icon = "✅" if coverage_rate >= min_coverage_threshold else "❌"
        if coverage_rate < min_coverage_threshold:
            all_pass = False
        print(f"  {sym}: {valid}/{trading_days} = {coverage_rate:.1%} coverage {status_icon}")
    
    if not all_pass:
        print(f"\n⚠️  WARNING: Coverage below {min_coverage_threshold:.0%} threshold.")
        print("   Backtest results will be marked INVALID.")
    else:
        print(f"\n✅  All symbols meet {min_coverage_threshold:.0%} coverage threshold.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
