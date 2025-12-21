"""
Option Bar Store

Fast local lookup for options OHLCV data from Polygon Flat Files.
Pattern: Load entire day in thin mode, O(1) lookups, evict after day completes.
"""

import gzip
import csv
from pathlib import Path
from datetime import date
from typing import Optional, Dict, Set, Any


class OptionBarStore:
    """
    Local store for options bar data from Flat Files.
    
    Usage for backfill (fast pattern):
        BAR_STORE.load_day(date_str)  # Load all tickers for day
        # ... all get_bar() calls are O(1) dict lookups ...
        BAR_STORE.evict_day(date_str)  # Free memory
    
    Modes:
        - 'thin': Only loads close price (default, ~50% memory)
        - 'full': Loads all OHLCV data
    """
    
    def __init__(self, cache_dir: Path, mode: str = 'thin'):
        self.cache_dir = cache_dir
        self.mode = mode
        self._day_cache: Dict[str, Dict[str, dict]] = {}  # date -> {ticker: bar}
        self._loaded_dates: set = set()
    
    def load_day(self, target_date) -> int:
        """
        Load all tickers for a day into memory.
        
        Call this once at start of processing each day, then all
        get_bar() calls become O(1) dict lookups.
        
        Returns: number of options loaded
        """
        date_str = target_date.isoformat() if isinstance(target_date, date) else target_date
        if date_str in self._loaded_dates:
            return len(self._day_cache.get(date_str, {}))
        return self._load_day_full(date_str)
    
    def evict_day(self, target_date):
        """Evict a day from cache to free memory."""
        date_str = target_date.isoformat() if isinstance(target_date, date) else target_date
        if date_str in self._day_cache:
            del self._day_cache[date_str]
        self._loaded_dates.discard(date_str)
    
    def get_bar(self, target_date: date, option_ticker: str) -> Optional[dict]:
        """
        Get bar for a single option ticker on a date.
        
        If load_day() was called first, this is O(1).
        Otherwise loads on-demand (slower for multiple calls).
        """
        date_str = target_date.isoformat() if isinstance(target_date, date) else target_date
        
        if date_str not in self._loaded_dates:
            self._load_day_full(date_str)
        
        return self._day_cache.get(date_str, {}).get(option_ticker)
    
    def has_date(self, target_date: date) -> bool:
        """Check if flat file exists for date."""
        date_str = target_date.isoformat() if isinstance(target_date, date) else target_date
        path = self.cache_dir / "options_aggs" / f"{date_str}.csv.gz"
        return path.exists()
    
    def _load_day_full(self, date_str: str) -> int:
        """Load entire day's options into memory (thin or full mode)."""
        self._loaded_dates.add(date_str)
        
        path = self.cache_dir / "options_aggs" / f"{date_str}.csv.gz"
        if not path.exists():
            self._day_cache[date_str] = {}
            return 0
        
        bars = {}
        try:
            with gzip.open(path, 'rt') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ticker = row.get('ticker') or row.get('Ticker') or ''
                    if not ticker:
                        continue
                    
                    if self.mode == 'thin':
                        bars[ticker] = {
                            'close': float(row.get('close') or row.get('Close') or 0),
                            'volume': int(float(row.get('volume') or row.get('Volume') or 0)),
                        }
                    else:
                        bars[ticker] = {
                            'open': float(row.get('open') or row.get('Open') or 0),
                            'high': float(row.get('high') or row.get('High') or 0),
                            'low': float(row.get('low') or row.get('Low') or 0),
                            'close': float(row.get('close') or row.get('Close') or 0),
                            'volume': int(float(row.get('volume') or row.get('Volume') or 0)),
                        }
            
            self._day_cache[date_str] = bars
            print(f"[BarStore] Loaded {len(bars)} options for {date_str}")
            return len(bars)
            
        except Exception as e:
            print(f"[BarStore] Failed to load {path}: {e}")
            self._day_cache[date_str] = {}
            return 0
    
    def clear_cache(self):
        """Clear all cached data."""
        self._day_cache.clear()
        self._loaded_dates.clear()
    
    def stats(self) -> dict:
        """Get cache statistics."""
        total_options = sum(len(v) for v in self._day_cache.values())
        return {
            'dates_loaded': len(self._loaded_dates),
            'total_options': total_options,
            'mode': self.mode,
        }
    
    # =========================================================================
    # Chain-Driven Selection Helpers
    # =========================================================================
    
    def get_available_expiries(self, target_date: date, symbol: str) -> list:
        """
        Get available expiries for a symbol from loaded day's data.
        Returns list of (expiry_date, dte) tuples sorted by DTE.
        """
        date_str = target_date.isoformat() if isinstance(target_date, date) else target_date
        if date_str not in self._loaded_dates:
            return []
        
        bars = self._day_cache.get(date_str, {})
        expiries = set()
        
        for ticker in bars.keys():
            if not ticker.startswith(f'O:{symbol}'):
                continue
            # Parse expiry from ticker: O:SPY220121C00420000
            try:
                rest = ticker[2 + len(symbol):]  # After "O:{symbol}"
                exp_str = rest[:6]  # YYMMDD
                exp_date = date(2000 + int(exp_str[:2]), int(exp_str[2:4]), int(exp_str[4:6]))
                expiries.add(exp_date)
            except:
                continue
        
        # Sort by DTE
        if isinstance(target_date, str):
            target_date = date.fromisoformat(target_date)
        
        result = [(exp, (exp - target_date).days) for exp in expiries]
        return sorted(result, key=lambda x: x[1])
    
    def get_available_strikes(self, target_date: date, symbol: str, expiry: date,
                              right: str = None) -> dict:
        """
        Get available strikes for a symbol/expiry from loaded data.
        
        Args:
            right: 'C' for calls only, 'P' for puts only, None for both
            
        Returns: {strike: {'C': bar_dict, 'P': bar_dict}}
        """
        date_str = target_date.isoformat() if isinstance(target_date, date) else target_date
        if date_str not in self._loaded_dates:
            return {}
        
        bars = self._day_cache.get(date_str, {})
        exp_str = expiry.strftime('%y%m%d')
        strikes = {}
        
        for ticker, bar in bars.items():
            if not ticker.startswith(f'O:{symbol}{exp_str}'):
                continue
            
            # Parse strike and right: O:SPY220121C00420000
            try:
                rest = ticker[2 + len(symbol) + 6:]  # After "O:{symbol}{exp_str}"
                option_right = rest[0]  # C or P
                strike = int(rest[1:]) / 1000
                
                if right and option_right != right:
                    continue
                
                if strike not in strikes:
                    strikes[strike] = {}
                strikes[strike][option_right] = bar
            except:
                continue
        
        return strikes
    
    def find_atm_strike(self, target_date: date, symbol: str, expiry: date,
                        spot: float) -> tuple:
        """
        Find ATM strike that has both call and put with non-zero close.
        
        Returns: (atm_strike, call_bar, put_bar) or (None, None, None)
        """
        strikes = self.get_available_strikes(target_date, symbol, expiry)
        
        # Find strikes with both call and put having tradable data
        # Tradable = close > 0 OR volume > 0
        valid_strikes = []
        for strike, options in strikes.items():
            call_bar = options.get('C', {})
            put_bar = options.get('P', {})
            call_tradable = call_bar.get('close', 0) > 0 or call_bar.get('volume', 0) > 0
            put_tradable = put_bar.get('close', 0) > 0 or put_bar.get('volume', 0) > 0
            if call_tradable and put_tradable:
                valid_strikes.append((strike, call_bar, put_bar))
        
        if not valid_strikes:
            return None, None, None
        
        # Choose strike nearest to spot
        valid_strikes.sort(key=lambda x: abs(x[0] - spot))
        return valid_strikes[0]
    
    def derive_increment(self, target_date: date, symbol: str, expiry: date,
                         spot: float, window_pct: float = 0.1) -> float:
        """
        Derive strike increment from chain data near ATM.
        
        Args:
            window_pct: Window around spot to analyze (default 10%)
            
        Returns: Modal increment (clamped to valid values)
        """
        strikes = self.get_available_strikes(target_date, symbol, expiry)
        
        # Filter to strikes within window of spot
        window = spot * window_pct
        nearby_strikes = sorted([s for s in strikes.keys() 
                                  if abs(s - spot) <= window])
        
        if len(nearby_strikes) < 2:
            return 1.0  # Default if insufficient data
        
        # Compute differences
        from collections import Counter
        diffs = []
        for i in range(1, len(nearby_strikes)):
            diff = round(nearby_strikes[i] - nearby_strikes[i-1], 2)
            if diff > 0:
                diffs.append(diff)
        
        if not diffs:
            return 1.0
        
        # Return mode (most common)
        counter = Counter(diffs)
        modal = counter.most_common(1)[0][0]
        
        # Clamp to valid increments
        valid = [0.5, 1.0, 2.5, 5.0]
        return min(valid, key=lambda x: abs(x - modal))
    
    def find_best_expiry(self, target_date: date, symbol: str,
                         target_dte: int = 30, tolerance: int = 15,
                         spot: float = None) -> tuple:
        """
        Find best USABLE expiry for symbol using chain data.
        
        An expiry is usable if it has at least one strike with both call+put
        having non-zero close AND that strike is within 2% of spot.
        
        Returns: (expiry_date, dte) or (None, None)
        """
        if spot is None or spot <= 0:
            return None, None
            
        expiries = self.get_available_expiries(target_date, symbol)
        
        if not expiries:
            return None, None
        
        # Filter to DTE > 7 and within band (14-60)
        candidates = [(exp, dte) for exp, dte in expiries if 7 < dte <= 60]
        
        if not candidates:
            # Fallback: any expiry > 7 DTE
            candidates = [(exp, dte) for exp, dte in expiries if dte > 7]
        
        if not candidates:
            return None, None
        
        # Maximum ATM distance: 2% of spot or $3, whichever is larger
        max_atm_distance = max(spot * 0.02, 3.0)
        
        # Check each candidate for usability (has valid ATM pair NEAR spot)
        usable_expiries = []
        for exp, dte in candidates:
            # Find ATM strike for this expiry
            atm_strike, call_bar, put_bar = self.find_atm_strike(
                target_date, symbol, exp, spot
            )
            
            if atm_strike is not None:
                # Check if ATM is close enough to spot
                if abs(atm_strike - spot) <= max_atm_distance:
                    usable_expiries.append((exp, dte))
        
        if not usable_expiries:
            # No usable expiry found - return (None, None) for honest failure
            return None, None
        
        # From usable expiries, pick closest to target DTE, preferring within tolerance
        within_tolerance = [(exp, dte) for exp, dte in usable_expiries
                            if abs(dte - target_dte) <= tolerance]
        
        if within_tolerance:
            within_tolerance.sort(key=lambda x: abs(x[1] - target_dte))
            return within_tolerance[0]
        
        # No usable expiry within tolerance - return closest usable
        usable_expiries.sort(key=lambda x: abs(x[1] - target_dte))
        return usable_expiries[0]
