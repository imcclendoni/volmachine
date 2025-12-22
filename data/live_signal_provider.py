"""
Live Signal Provider

Provides data for live signal computation from flatfiles.
No dependency on backfill reports - computes signals from raw market data.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import numpy as np

from data.option_bar_store import OptionBarStore
from data.data_watermark import get_data_watermark


class LiveSignalProvider:
    """
    Provides data for live signal computation.
    
    Wraps OptionBarStore and adds:
    - Rolling window lookbacks
    - ATM IV computation
    - Underlying price history
    - Data proof tracking
    """
    
    def __init__(self, flatfiles_dir: Path):
        self.flatfiles_dir = flatfiles_dir
        self.bar_store = OptionBarStore(flatfiles_dir, mode='thin')
        self._data_proof = {
            'options_rows_loaded': 0,
            'days_loaded': [],
            'symbols_scanned': 0,
        }
        self._underlying_cache: Dict[str, Dict[str, float]] = {}  # symbol -> {date_str: price}
    
    def get_effective_date(self) -> date:
        """Get latest trading date with data (from watermark)."""
        watermark = get_data_watermark(self.flatfiles_dir)
        return watermark.effective_date
    
    def load_day(self, target_date: date) -> int:
        """
        Load options for date.
        
        Returns: number of options rows loaded.
        """
        rows = self.bar_store.load_day(target_date)
        self._data_proof['options_rows_loaded'] += rows
        self._data_proof['days_loaded'].append(target_date.isoformat())
        return rows
    
    def has_date(self, target_date: date) -> bool:
        """Check if data exists for date."""
        return self.bar_store.has_date(target_date)
    
    def iter_past_trading_days(self, end_date: date, n: int) -> List[date]:
        """
        Get last n trading days with data, ending at end_date.
        
        Scans backwards until n days found or 2*n days checked.
        """
        days = []
        current = end_date
        max_lookback = n * 3  # Allow for weekends/holidays
        
        for _ in range(max_lookback):
            if self.has_date(current):
                days.append(current)
                if len(days) >= n:
                    break
            current -= timedelta(days=1)
        
        return list(reversed(days))  # Oldest first
    
    def get_underlying_prices(self, symbol: str, dates: List[date]) -> List[Optional[float]]:
        """
        Get close prices for symbol on dates.
        
        Uses cached OHLCV data if available.
        """
        cache_path = self.flatfiles_dir.parent / 'ohlcv' / f'{symbol}_daily.json'
        
        if symbol not in self._underlying_cache:
            self._underlying_cache[symbol] = {}
            if cache_path.exists():
                try:
                    with open(cache_path) as f:
                        data = json.load(f)
                    for bar in data.get('bars', []):
                        bar_date = date.fromtimestamp(bar['t'] / 1000).isoformat()
                        self._underlying_cache[symbol][bar_date] = bar['c']
                except Exception:
                    pass
        
        prices = []
        for d in dates:
            price = self._underlying_cache[symbol].get(d.isoformat())
            prices.append(price)
        
        return prices
    
    def compute_atm_iv(
        self, 
        target_date: date, 
        symbol: str, 
        underlying_price: float,
        target_dte_min: int = 25,
        target_dte_max: int = 45,
    ) -> Optional[float]:
        """
        Compute ATM IV for symbol on date.
        
        Uses average of ATM call and put IV within DTE range.
        """
        # Ensure day is loaded
        if target_date.isoformat() not in self.bar_store._loaded_dates:
            self.load_day(target_date)
        
        # Find expiry in target DTE range
        expiries = self.bar_store.get_available_expiries(target_date, symbol)
        if not expiries:
            return None
        
        target_expiry = None
        for exp_date, dte in expiries:
            if target_dte_min <= dte <= target_dte_max:
                target_expiry = exp_date
                break
        
        if target_expiry is None:
            return None
        
        # Find ATM strike
        atm_strike, call_bar, put_bar = self.bar_store.find_atm_strike(
            target_date, symbol, target_expiry, underlying_price
        )
        
        if atm_strike is None or call_bar is None or put_bar is None:
            return None
        
        # Use close prices to estimate IV (simplified - using mid of close prices)
        # In production, would use Black-Scholes to back out IV
        call_price = call_bar.get('close', 0) or call_bar.get('c', 0)
        put_price = put_bar.get('close', 0) or put_bar.get('c', 0)
        
        if call_price <= 0 or put_price <= 0:
            return None
        
        # Simplified IV proxy: (call + put) / underlying as rough IV estimate
        # Real implementation would use Black-Scholes
        dte = (target_expiry - target_date).days
        if dte <= 0:
            return None
        
        # Rough ATM straddle IV approximation
        straddle_price = call_price + put_price
        # IV ≈ straddle / (0.8 * spot * sqrt(T))
        t_years = dte / 365.0
        atm_iv = straddle_price / (0.8 * underlying_price * np.sqrt(t_years))
        
        return min(max(atm_iv, 0.05), 2.0)  # Clamp to reasonable range
    
    def get_data_proof(self) -> Dict[str, Any]:
        """Get data proof block for audit."""
        return {
            'flatfiles_dir': str(self.flatfiles_dir),
            'options_rows_loaded': self._data_proof['options_rows_loaded'],
            'days_loaded': len(self._data_proof['days_loaded']),
            'symbols_scanned': self._data_proof['symbols_scanned'],
        }
    
    def evict_day(self, target_date: date):
        """Free memory for a day."""
        self.bar_store.evict_day(target_date)


def compute_rolling_zscore(
    values: List[float],
    current_value: float,
    lookback: int = 120,
) -> Tuple[float, float, float]:
    """
    Compute z-score of current value vs rolling window.
    
    Returns: (z_score, mean, std)
    """
    if len(values) < lookback:
        lookback = len(values)
    
    if lookback < 20:
        return 0.0, current_value, 0.0
    
    window = values[-lookback:]
    mean = np.mean(window)
    std = np.std(window)
    
    if std < 0.0001:
        return 0.0, mean, std
    
    z_score = (current_value - mean) / std
    return z_score, mean, std


def compute_realized_volatility(
    prices: List[float],
    window: int = 20,
    annualize: bool = True,
) -> Optional[float]:
    """
    Compute realized volatility from price series.
    
    Uses log returns, standard deviation, annualized.
    """
    if len(prices) < window + 1:
        return None
    
    # Get last window+1 prices for window returns
    recent = prices[-(window + 1):]
    
    # Log returns
    returns = []
    for i in range(1, len(recent)):
        if recent[i] > 0 and recent[i-1] > 0:
            returns.append(np.log(recent[i] / recent[i-1]))
    
    if len(returns) < window:
        return None
    
    rv = np.std(returns)
    
    if annualize:
        rv *= np.sqrt(252)
    
    return rv


def compute_trend(
    prices: List[float],
    fast_window: int = 20,
    slow_window: int = 60,
) -> Tuple[str, float, float]:
    """
    Compute trend from moving averages.
    
    Returns: (trend, ma_fast, ma_slow)
    """
    if len(prices) < slow_window:
        return 'neutral', 0.0, 0.0
    
    ma_fast = np.mean(prices[-fast_window:])
    ma_slow = np.mean(prices[-slow_window:])
    
    if ma_fast > ma_slow:
        trend = 'bullish'
    elif ma_fast < ma_slow:
        trend = 'bearish'
    else:
        trend = 'neutral'
    
    return trend, ma_fast, ma_slow
