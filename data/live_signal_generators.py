"""
Live Signal Generators

Compute signals directly from flatfiles for the effective_date.
No dependency on backfill reports.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

from data.live_signal_provider import (
    LiveSignalProvider,
    compute_rolling_zscore,
    compute_realized_volatility,
    compute_trend,
)


def next_trading_day(d: date) -> date:
    """Get next trading day (skip weekends)."""
    next_d = d + timedelta(days=1)
    while next_d.weekday() >= 5:  # Saturday=5, Sunday=6
        next_d += timedelta(days=1)
    return next_d


def compute_skew_from_flatfiles(
    provider: LiveSignalProvider,
    symbol: str,
    trade_date: date,
    underlying_price: float,
) -> Optional[dict]:
    """
    Compute put/call skew proxy from flatfiles for a single date.
    
    Skew = (OTM put IV proxy) - (ATM IV proxy)
    Where OTM put is ~5% below ATM.
    
    Returns dict with skew value and metadata, or None if insufficient data.
    """
    # Find target expiry (30-45 DTE)
    expiries = provider.bar_store.get_available_expiries(trade_date, symbol)
    target_expiry = None
    target_dte = None
    for exp_date, dte in expiries:
        if 30 <= dte <= 45:
            target_expiry = exp_date
            target_dte = dte
            break
    
    if target_expiry is None:
        return None
    
    # Get strikes
    strikes_data = provider.bar_store.get_available_strikes(trade_date, symbol, target_expiry)
    if not strikes_data:
        return None
    
    atm_strike = round(underlying_price / 5) * 5
    otm_put_strike = round((underlying_price * 0.95) / 5) * 5  # ~5% OTM
    
    # Get ATM call and OTM put prices
    atm_bar = strikes_data.get(atm_strike, {}).get('C')
    otm_put_bar = strikes_data.get(otm_put_strike, {}).get('P')
    
    if not atm_bar or not otm_put_bar:
        return None
    
    atm_price = atm_bar.get('close', 0) or atm_bar.get('c', 0)
    otm_put_price = otm_put_bar.get('close', 0) or otm_put_bar.get('c', 0)
    
    if atm_price <= 0:
        return None
    
    # Compute IV proxy using straddle approximation
    dte = (target_expiry - trade_date).days
    if dte <= 0:
        return None
    
    sqrt_t = (dte / 365) ** 0.5
    atm_iv_proxy = atm_price / (underlying_price * sqrt_t) * 2.5
    
    # OTM put IV proxy (adjusted for moneyness)
    distance = abs(otm_put_strike - underlying_price) / underlying_price
    otm_put_iv_proxy = otm_put_price / (underlying_price * sqrt_t * (1 - distance)) * 2.5 if distance < 1 else 0
    
    skew = otm_put_iv_proxy - atm_iv_proxy
    
    return {
        'skew': skew,
        'target_expiry': target_expiry.isoformat(),
        'dte': target_dte,
        'atm_strike': atm_strike,
        'otm_put_strike': otm_put_strike,
        'atm_price': atm_price,
        'otm_put_price': otm_put_price,
        'atm_iv_proxy': round(atm_iv_proxy, 4),
        'otm_put_iv_proxy': round(otm_put_iv_proxy, 4),
    }


def generate_iv_carry_mr_live(
    provider: LiveSignalProvider,
    effective_date: date,
    universe: List[str],
) -> Dict[str, Any]:
    """
    Generate IV Carry MR signals by computing from flatfiles.
    
    For each symbol:
    1. Load 120 trading days of ATM IV
    2. Compute iv_zscore
    3. Load 60 days of underlying prices
    4. Compute rv_20d, rv_iv_ratio, trend
    5. If triggered, build credit spread structure
    
    Returns: signals dict ready for JSON output
    """
    signal_date = effective_date  # Signal computed after close
    execution_date = next_trading_day(effective_date)
    
    # Get lookback dates
    lookback_dates = provider.iter_past_trading_days(effective_date, 120)
    if len(lookback_dates) < 60:
        return {
            'error': f'Insufficient lookback data: {len(lookback_dates)} days (need 60+)',
            'candidate_count': 0,
            'candidates': [],
        }
    
    candidates = []
    gate_samples = []
    symbols_scanned = 0
    
    for symbol in universe:
        symbols_scanned += 1
        
        # Get underlying prices for lookback - maintain chronological order
        prices_raw = provider.get_underlying_prices(symbol, lookback_dates)
        # Build ordered list aligned with lookback_dates (chronological)
        prices_ordered = [(d, p) for d, p in zip(lookback_dates, prices_raw) if p is not None]
        prices_by_date = {d: p for d, p in prices_ordered}
        prices = [p for d, p in prices_ordered]  # Chronological order
        
        if len(prices) < 60:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': f'insufficient price history ({len(prices)} days)',
            })
            continue
        
        # Current price is the last available (chronological)
        current_price = prices[-1]
        
        # Prefer price for effective_date, but fall back to most recent if missing
        price_on_effective = prices_by_date.get(effective_date)
        price_stale = False
        if price_on_effective is not None:
            current_price = price_on_effective
        else:
            # Use most recent available - flag it
            sorted_dates = sorted(prices_by_date.keys())
            if sorted_dates:
                current_price = prices_by_date[sorted_dates[-1]]
                price_stale = True
        
        # Compute ATM IV for each day in lookback (120 days per frozen spec)
        iv_history = []
        days_with_iv = sorted(prices_by_date.keys())[-120:]  # Last 120 days with price data (sorted)
        
        diag = {
            'days_attempted': len(days_with_iv),
            'days_loaded': 0,
            'days_with_expiry': 0,
            'days_with_atm': 0,
            'days_with_valid_iv': 0,
            'errors': [],
        }
        
        for d in days_with_iv:
            try:
                price_on_day = prices_by_date.get(d, current_price)
                rows = provider.load_day(d)
                diag['days_loaded'] += 1
                
                iv = provider.compute_atm_iv(d, symbol, price_on_day)
                if iv is not None:
                    iv_history.append((d, iv))
                    diag['days_with_valid_iv'] += 1
            except Exception as e:
                diag['errors'].append(f"{d}: {str(e)[:50]}")
        
        if len(iv_history) < 30:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': f'insufficient IV history ({len(iv_history)} days)',
                'diagnostics': diag,
            })
            continue
        
        # Current ATM IV
        provider.load_day(effective_date)
        current_iv = provider.compute_atm_iv(effective_date, symbol, current_price)
        
        if current_iv is None:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': 'could not compute current ATM IV',
            })
            continue
        
        # Compute z-score
        iv_values = [iv for _, iv in iv_history]
        iv_zscore, iv_mean, iv_std = compute_rolling_zscore(iv_values, current_iv, lookback=120)
        
        # Compute RV
        rv_20d = compute_realized_volatility(prices, window=20)
        if rv_20d is None:
            rv_20d = 0.0
        
        rv_iv_ratio = rv_20d / current_iv if current_iv > 0 else 999
        
        # Compute trend
        trend, ma_fast, ma_slow = compute_trend(prices, fast_window=20, slow_window=60)
        
        # Gate sample - only trigger on ELEVATED IV (z >= 2.0), not depleted
        gate_pass = iv_zscore >= 2.0 and rv_iv_ratio < 1.0 and trend != 'neutral'
        gate_samples.append({
            'symbol': symbol,
            'iv_zscore': round(iv_zscore, 2),
            'rv_iv_ratio': round(rv_iv_ratio, 2),
            'trend': trend,
            'gate_pass': gate_pass,
        })
        
        if not gate_pass:
            continue
        
        # Track rejection reason if we fail to produce candidate despite gate_pass
        reject_reason = None
        
        # Determine direction
        if iv_zscore >= 2.0:
            direction = 'SELL_CALLS' if trend == 'bearish' else 'SELL_PUTS'
        else:
            reject_reason = 'iv_zscore_below_threshold'
            gate_samples[-1]['candidate_reject_reason'] = reject_reason
            continue
        
        # Find target expiry (30-45 DTE)
        expiries = provider.bar_store.get_available_expiries(effective_date, symbol)
        target_expiry = None
        for exp_date, dte in expiries:
            if 30 <= dte <= 45:
                target_expiry = exp_date
                break
        
        if target_expiry is None:
            reject_reason = 'no_target_expiry_30_45'
            gate_samples[-1]['candidate_reject_reason'] = reject_reason
            continue
        
        # Build credit spread structure
        atm_strike = round(current_price / 5) * 5  # Round to $5 grid
        
        # Get available strikes
        strikes_data = provider.bar_store.get_available_strikes(effective_date, symbol, target_expiry)
        if not strikes_data:
            reject_reason = 'no_strikes_for_expiry'
            gate_samples[-1]['candidate_reject_reason'] = reject_reason
            continue
        
        available_strikes = sorted(strikes_data.keys())
        closest_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
        
        # Build spread
        width = 5
        if direction == 'SELL_PUTS':
            short_strike = closest_strike
            long_strike = short_strike - width
            option_type = 'P'
        else:  # SELL_CALLS
            short_strike = closest_strike
            long_strike = short_strike + width
            option_type = 'C'
        
        # Check long strike exists
        if long_strike not in available_strikes:
            width = 10
            if direction == 'SELL_PUTS':
                long_strike = short_strike - width
            else:
                long_strike = short_strike + width
            
            if long_strike not in available_strikes:
                reject_reason = 'long_strike_not_found'
                gate_samples[-1]['candidate_reject_reason'] = reject_reason
                continue
        
        # Get prices for max_loss calculation
        short_bar = strikes_data.get(short_strike, {}).get(option_type)
        long_bar = strikes_data.get(long_strike, {}).get(option_type)
        
        if not short_bar or not long_bar:
            reject_reason = 'missing_short_or_long_bar'
            gate_samples[-1]['candidate_reject_reason'] = reject_reason
            continue
        
        short_price = short_bar.get('close', 0) or short_bar.get('c', 0)
        long_price = long_bar.get('close', 0) or long_bar.get('c', 0)
        
        entry_credit = short_price - long_price
        max_loss_usd = (width * 100) - (entry_credit * 100)
        
        # Sanity check: skip if pricing is invalid
        if entry_credit <= 0 or max_loss_usd <= 0:
            reject_reason = f'invalid_pricing:entry_credit={entry_credit:.2f},max_loss={max_loss_usd:.2f}'
            gate_samples[-1]['candidate_reject_reason'] = reject_reason
            continue
        
        candidates.append({
            'symbol': symbol,
            'signal_date': signal_date.isoformat(),
            'execution_date': execution_date.isoformat(),
            'iv_zscore': round(iv_zscore, 2),
            'atm_iv': round(current_iv, 4),
            'iv_mean': round(iv_mean, 4),
            'iv_std': round(iv_std, 4),
            'rv_20d': round(rv_20d, 4),
            'rv_iv_ratio': round(rv_iv_ratio, 2),
            'trend': trend,
            'ma_fast': round(ma_fast, 2),
            'ma_slow': round(ma_slow, 2),
            'direction': direction,
            'underlying_price': round(current_price, 2),
            'target_expiry': target_expiry.isoformat(),
            'structure': {
                'type': 'credit_spread',
                'short_strike': short_strike,
                'long_strike': long_strike,
                'width': width,
                'expiry': target_expiry.isoformat(),
                'entry_credit': round(entry_credit, 2),
                'max_loss_usd': round(max_loss_usd, 2),
                'legs': [
                    {
                        'strike': short_strike,
                        'right': option_type,
                        'side': 'SELL',
                        'expiry': target_expiry.isoformat(),
                    },
                    {
                        'strike': long_strike,
                        'right': option_type,
                        'side': 'BUY',
                        'expiry': target_expiry.isoformat(),
                    }
                ]
            },
            'edge_strength': round(abs(iv_zscore) / 2.0, 2),
            'rationale': f"IV z-score {iv_zscore:.2f}, {direction} ({trend} trend)"
        })
    
    provider._data_proof['symbols_scanned'] = symbols_scanned
    
    return {
        'edge_id': 'iv_carry_mr',
        'edge_version': 'v1.0',
        'source': 'live',
        'signal_date': signal_date.isoformat(),
        'execution_date': execution_date.isoformat(),
        'universe': universe,
        'regime_gate': {
            'iv_zscore_threshold': 2.0,
            'rv_iv_max': 1.0,
        },
        'iv_method': 'atm_iv_from_flatfiles',  # Explicit method tag
        'data_snapshot': provider.get_data_proof(),
        'gate_samples': gate_samples,
        'candidates': candidates,
        'candidate_count': len(candidates),
    }


def generate_flat_live(
    provider: LiveSignalProvider,
    effective_date: date,
    universe: List[str],
    ivp_gate: float = 75.0,
) -> Dict[str, Any]:
    """
    Generate FLAT signals by computing from flatfiles.
    
    FLAT = flat skew (≤10th percentile) that is reverting.
    
    For each symbol:
    1. Load skew history from cache
    2. Compute ATM IV percentile (IVp gate)
    3. Compute current skew and percentile
    4. Check if flat + reverting
    5. Build call debit spread structure
    
    Returns: signals dict ready for JSON output
    """
    import numpy as np
    
    signal_date = effective_date
    execution_date = next_trading_day(effective_date)
    
    candidates = []
    gate_samples = []
    symbols_scanned = 0
    
    # Load today's options
    provider.load_day(effective_date)
    
    for symbol in universe:
        symbols_scanned += 1
        
        # Get underlying price for effective date
        prices = provider.get_underlying_prices(symbol, [effective_date])
        price = prices[0] if prices and prices[0] else 0
        
        if price <= 0:
            # Try to get from most recent available
            lookback = provider.iter_past_trading_days(effective_date, 5)
            for d in lookback:
                p = provider.get_underlying_prices(symbol, [d])
                if p and p[0]:
                    price = p[0]
                    break
        
        if price <= 0:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': 'no underlying price available',
            })
            continue
        
        # Compute today's skew from flatfiles
        skew_result = compute_skew_from_flatfiles(provider, symbol, effective_date, price)
        
        if skew_result is None:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': 'could not compute current skew from flatfiles',
            })
            continue
        
        current_skew = skew_result['skew']
        skew_metadata = skew_result  # Store for transparency
        
        # Build skew history from last 60 trading days for percentile calculation
        lookback_dates = provider.iter_past_trading_days(effective_date, 60)
        skew_history = []
        
        recent_skew_days = lookback_dates[-30:]  # Most recent 30 days
        for d in recent_skew_days:
            provider.load_day(d)
            hist_prices = provider.get_underlying_prices(symbol, [d])
            hist_price = hist_prices[0] if hist_prices and hist_prices[0] else price
            
            hist_skew_result = compute_skew_from_flatfiles(provider, symbol, d, hist_price)
            if hist_skew_result is not None:
                skew_history.append(hist_skew_result['skew'])
        
        if len(skew_history) < 20:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': f'insufficient skew history ({len(skew_history)} days)',
            })
            continue
        
        # Compute skew percentile
        skew_percentile = sum(1 for s in skew_history if s <= current_skew) / len(skew_history) * 100
        
        # Compute skew delta (difference from 5d ago)
        skew_delta = 0.0
        if len(skew_history) >= 5:
            skew_5d_ago = skew_history[-5] if len(skew_history) >= 5 else skew_history[0]
            skew_delta = current_skew - skew_5d_ago
        
        # Compute ATM IV for IVp gate
        current_iv = provider.compute_atm_iv(effective_date, symbol, price)
        if current_iv is None:
            current_iv = 0.2  # Default
        
        # Compute IVp from recent IV history (using historical prices for each day)
        iv_history = []
        recent_lookback = lookback_dates[-30:]  # Most recent 30 days
        for d in recent_lookback:
            provider.load_day(d)  # Ensure option bars are loaded
            hist_prices = provider.get_underlying_prices(symbol, [d])
            hist_price = hist_prices[0] if hist_prices and hist_prices[0] else price
            iv = provider.compute_atm_iv(d, symbol, hist_price)
            if iv is not None:
                iv_history.append(iv)
        
        ivp = 50.0  # default
        if iv_history and len(iv_history) >= 10:
            ivp = sum(1 for iv in iv_history if iv <= current_iv) / len(iv_history) * 100
        
        # Gate checks (using live-computed values)
        ivp_pass = ivp <= ivp_gate
        is_flat = skew_percentile <= 10
        is_reverting = skew_delta > 0.005  # Skew increasing (reverting from flat)
        
        gate_pass = ivp_pass and is_flat and is_reverting
        
        gate_samples.append({
            'symbol': symbol,
            'ivp': round(ivp, 1),
            'skew_pctl': round(skew_percentile, 1),
            'current_skew': round(current_skew, 4),
            'skew_delta': round(skew_delta, 4),
            'ivp_pass': ivp_pass,
            'is_flat': is_flat,
            'is_reverting': is_reverting,
            'gate_pass': gate_pass,
            # Transparency metadata
            'skew_target_expiry': skew_metadata.get('target_expiry'),
            'skew_dte': skew_metadata.get('dte'),
            'skew_atm_strike': skew_metadata.get('atm_strike'),
            'skew_otm_put_strike': skew_metadata.get('otm_put_strike'),
        })
        
        if not gate_pass:
            continue
        
        # price already computed above
        
        # Find target expiry (21-45 DTE)
        expiries = provider.bar_store.get_available_expiries(effective_date, symbol)
        target_expiry = None
        for exp_date, dte in expiries:
            if 21 <= dte <= 45:
                target_expiry = exp_date
                break
        
        if target_expiry is None:
            continue
        
        # Build call debit spread
        atm_strike = round(price / 5) * 5
        
        strikes_data = provider.bar_store.get_available_strikes(effective_date, symbol, target_expiry, right='C')
        if not strikes_data:
            continue
        
        available_strikes = sorted(strikes_data.keys())
        
        # Long ATM, Short OTM
        long_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
        width = 5
        short_strike = long_strike + width
        
        if short_strike not in available_strikes:
            width = 10
            short_strike = long_strike + width
            if short_strike not in available_strikes:
                continue
        
        # Get prices
        long_bar = strikes_data.get(long_strike, {}).get('C')
        short_bar = strikes_data.get(short_strike, {}).get('C')
        
        if not long_bar or not short_bar:
            continue
        
        long_price = long_bar.get('close', 0)
        short_price = short_bar.get('close', 0)
        
        entry_debit = long_price - short_price
        max_loss = entry_debit * 100
        max_profit = (width - entry_debit) * 100
        
        if entry_debit <= 0:
            continue
        
        candidates.append({
            'symbol': symbol,
            'signal_date': signal_date.isoformat(),
            'execution_date': execution_date.isoformat(),
            'atm_iv_percentile': round(ivp, 1),
            'skew_percentile': round(skew_percentile, 1),
            'skew_delta': round(skew_delta, 4),
            'current_skew': round(current_skew, 4),
            'underlying_price': round(price, 2),
            'target_expiry': target_expiry.isoformat(),
            'structure': {
                'type': 'call_debit_spread',
                'long_strike': long_strike,
                'short_strike': short_strike,
                'width': width,
                'expiry': target_expiry.isoformat(),
                'entry_debit': round(entry_debit, 2),
                'max_loss': round(max_loss, 2),
                'max_profit': round(max_profit, 2),
                'legs': [
                    {
                        'strike': long_strike,
                        'right': 'C',
                        'side': 'BUY',
                        'expiry': target_expiry.isoformat(),
                    },
                    {
                        'strike': short_strike,
                        'right': 'C',
                        'side': 'SELL',
                        'expiry': target_expiry.isoformat(),
                    }
                ]
            },
            'edge_strength': round((100 - skew_percentile) / 100, 2),
            'rationale': f"FLAT: skew at {skew_percentile:.0f}th pctl, reverting (Δ={skew_delta:.4f}). IVp {ivp:.0f} (gate: {ivp_gate})"
        })
    
    provider._data_proof['symbols_scanned'] = symbols_scanned
    
    return {
        'edge_id': 'flat',
        'edge_version': 'v1.0',
        'source': 'live',
        'signal_date': signal_date.isoformat(),
        'execution_date': execution_date.isoformat(),
        'universe': universe,
        'regime_gate': {
            'max_atm_iv_pctl': ivp_gate,
            'skew_pctl_threshold': 10,
        },
        'skew_method': 'price_proxy_v1',  # Explicit method tag
        'spec_warning': 'FLAT live uses price-based skew proxy which may differ from backtested IV-based skew. Equivalence not yet validated.',
        'data_snapshot': provider.get_data_proof(),
        'gate_samples': gate_samples,
        'candidates': candidates,
        'candidate_count': len(candidates),
    }

