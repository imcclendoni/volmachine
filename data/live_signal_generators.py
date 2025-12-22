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
        
        # Get underlying prices for lookback (keep as dict for proper lookup)
        prices_raw = provider.get_underlying_prices(symbol, lookback_dates)
        prices_by_date = {lookback_dates[i]: p for i, p in enumerate(prices_raw) if p is not None}
        prices = list(prices_by_date.values())
        
        if len(prices) < 60:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': f'insufficient price history ({len(prices)} days)',
            })
            continue
        
        current_price = prices[-1]
        
        # Compute ATM IV for each day in lookback (need at least 30 days)
        iv_history = []
        days_with_iv = list(prices_by_date.keys())[-60:]  # Last 60 days with price data
        
        for d in days_with_iv:
            try:
                price_on_day = prices_by_date.get(d, current_price)
                provider.load_day(d)
                iv = provider.compute_atm_iv(d, symbol, price_on_day)
                if iv is not None:
                    iv_history.append((d, iv))
            except Exception as e:
                pass
        
        if len(iv_history) < 30:
            gate_samples.append({
                'symbol': symbol,
                'status': 'skip',
                'reason': f'insufficient IV history ({len(iv_history)} days)',
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
        
        # Gate sample
        gate_pass = abs(iv_zscore) >= 2.0 and rv_iv_ratio < 1.0 and trend != 'neutral'
        gate_samples.append({
            'symbol': symbol,
            'iv_zscore': round(iv_zscore, 2),
            'rv_iv_ratio': round(rv_iv_ratio, 2),
            'trend': trend,
            'gate_pass': gate_pass,
        })
        
        if not gate_pass:
            continue
        
        # Determine direction
        if iv_zscore >= 2.0:
            direction = 'SELL_CALLS' if trend == 'bearish' else 'SELL_PUTS'
        else:
            continue  # Only sell premium on elevated IV
        
        # Find target expiry (30-45 DTE)
        expiries = provider.bar_store.get_available_expiries(effective_date, symbol)
        target_expiry = None
        for exp_date, dte in expiries:
            if 30 <= dte <= 45:
                target_expiry = exp_date
                break
        
        if target_expiry is None:
            continue
        
        # Build credit spread structure
        atm_strike = round(current_price / 5) * 5  # Round to $5 grid
        
        # Get available strikes
        strikes_data = provider.bar_store.get_available_strikes(effective_date, symbol, target_expiry)
        if not strikes_data:
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
                continue
        
        # Get prices for max_loss calculation
        short_bar = strikes_data.get(short_strike, {}).get(option_type)
        long_bar = strikes_data.get(long_strike, {}).get(option_type)
        
        if not short_bar or not long_bar:
            continue
        
        short_price = short_bar.get('close', 0) or short_bar.get('c', 0)
        long_price = long_bar.get('close', 0) or long_bar.get('c', 0)
        
        entry_credit = short_price - long_price
        max_loss_usd = (width * 100) - (entry_credit * 100)
        
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
        'data_snapshot': provider.get_data_proof(),
        'gate_samples': gate_samples,
        'candidates': candidates,
        'candidate_count': len(candidates),
    }
