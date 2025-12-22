"""
Term Structure Mean-Reversion Signal Generator.

Computes z-score of term slope (front IV - back IV) and emits signals
when dislocation exceeds threshold.
"""

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, List, Dict, Any

import numpy as np

from edges.term_structure_mr.config import TermStructureMRConfig


@dataclass
class TermStructureMRSignal:
    """Computed term structure signal."""
    
    symbol: str
    signal_date: date
    
    # IV metrics
    front_iv: float
    front_dte: int
    front_expiry: date
    back_iv: float
    back_dte: int
    back_expiry: date
    
    # Term slope = front - back (positive = inverted/backwardation)
    term_slope: float
    
    # Z-score of term slope
    term_z: float
    
    # Signal type
    signal_type: str  # 'long_compression' or 'short_compression' or 'none'
    
    # Additional context
    atm_iv_pctl: Optional[float] = None
    underlying_price: Optional[float] = None
    
    @property
    def is_triggered(self) -> bool:
        """Check if signal meets threshold."""
        return self.signal_type != 'none'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'symbol': self.symbol,
            'signal_date': self.signal_date.isoformat(),
            'front_iv': round(self.front_iv, 4),
            'front_dte': self.front_dte,
            'front_expiry': self.front_expiry.isoformat(),
            'back_iv': round(self.back_iv, 4),
            'back_dte': self.back_dte,
            'back_expiry': self.back_expiry.isoformat(),
            'term_slope': round(self.term_slope, 4),
            'term_z': round(self.term_z, 2),
            'signal_type': self.signal_type,
            'atm_iv_pctl': round(self.atm_iv_pctl, 1) if self.atm_iv_pctl else None,
            'underlying_price': round(self.underlying_price, 2) if self.underlying_price else None,
        }


def compute_atm_iv_for_expiry(
    bar_store,
    target_date: date,
    symbol: str,
    expiry: date,
    underlying_price: float,
) -> Optional[float]:
    """
    Compute ATM IV for a specific expiry using flat file data.
    
    Returns average of ATM call and put IV, or None if unavailable.
    """
    # Find ATM strike with both call and put
    atm_strike, call_bar, put_bar = bar_store.find_atm_strike(
        target_date, symbol, expiry, underlying_price
    )
    
    if atm_strike is None or call_bar is None or put_bar is None:
        return None
    
    # Get close prices
    call_close = call_bar.get('close', 0)
    put_close = put_bar.get('close', 0)
    
    if call_close <= 0 or put_close <= 0:
        return None
    
    # Calculate IV using Black-Scholes
    dte = (expiry - target_date).days
    if dte <= 0:
        return None
    
    T = dte / 365.0
    r = 0.05  # Risk-free rate assumption
    
    call_iv = _implied_volatility(call_close, underlying_price, atm_strike, T, r, 'call')
    put_iv = _implied_volatility(put_close, underlying_price, atm_strike, T, r, 'put')
    
    if call_iv is None or put_iv is None:
        return None
    
    # Sanity bounds
    if not (0.01 < call_iv < 3.0) or not (0.01 < put_iv < 3.0):
        return None
    
    return (call_iv + put_iv) / 2


def _implied_volatility(
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
            bs_price = _bs_call_price(S, K, T, r, sigma)
        else:
            bs_price = _bs_put_price(S, K, T, r, sigma)
        
        diff = bs_price - price
        
        if abs(diff) < tol:
            return sigma
        
        vega = S * math.sqrt(T) * _norm_pdf(
            (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        )
        
        if vega < 1e-10:
            return None
        
        sigma = sigma - diff / vega
        
        if sigma <= 0.001:
            sigma = 0.001
        if sigma > 5:
            return None
    
    return sigma if 0.01 < sigma < 5 else None


def _bs_call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call price."""
    if T <= 0 or sigma <= 0:
        return max(0, S - K)
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def _bs_put_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes put price."""
    if T <= 0 or sigma <= 0:
        return max(0, K - S)
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)


class TermStructureMRDetector:
    """
    Term Structure Mean-Reversion Edge Detector.
    
    Maintains rolling history of term slopes and computes z-scores
    for signal generation.
    """
    
    def __init__(self, config: Optional[TermStructureMRConfig] = None):
        self.config = config or TermStructureMRConfig()
        
        # Rolling history: symbol -> list of (date, term_slope)
        self._history: Dict[str, List[tuple[date, float]]] = {}
    
    def detect(
        self,
        bar_store,
        target_date: date,
        symbol: str,
        underlying_price: float,
        atm_iv_pctl: Optional[float] = None,
        vix_level: Optional[float] = None,
    ) -> Optional[TermStructureMRSignal]:
        """
        Detect term structure mean-reversion signal.
        
        Args:
            bar_store: OptionBarStore with loaded day
            target_date: Analysis date
            symbol: Underlying symbol
            underlying_price: Current underlying price
            atm_iv_pctl: ATM IV percentile (for regime gate)
            vix_level: Current VIX level (for regime gate)
            
        Returns:
            TermStructureMRSignal if edge detected (or computed data), None on error
        """
        # Apply regime gates
        if atm_iv_pctl is not None and atm_iv_pctl > self.config.max_atm_iv_pctl:
            return None
        
        if vix_level is not None and vix_level > self.config.max_vix:
            return None
        
        # Find front expiry
        front_expiry, front_dte = self._find_expiry_in_range(
            bar_store, target_date, symbol, underlying_price,
            self.config.front_dte_range
        )
        
        if front_expiry is None:
            return None
        
        # Find back expiry
        back_expiry, back_dte = self._find_expiry_in_range(
            bar_store, target_date, symbol, underlying_price,
            self.config.back_dte_range
        )
        
        if back_expiry is None:
            return None
        
        # Compute ATM IV for both expiries
        front_iv = compute_atm_iv_for_expiry(
            bar_store, target_date, symbol, front_expiry, underlying_price
        )
        
        if front_iv is None:
            return None
        
        back_iv = compute_atm_iv_for_expiry(
            bar_store, target_date, symbol, back_expiry, underlying_price
        )
        
        if back_iv is None:
            return None
        
        # Compute term slope
        term_slope = front_iv - back_iv
        
        # Update history
        self._update_history(symbol, target_date, term_slope)
        
        # Compute z-score
        term_z = self._compute_zscore(symbol, term_slope)
        
        # Determine signal type
        if term_z >= self.config.z_threshold:
            signal_type = 'long_compression'
        elif term_z <= -self.config.z_threshold:
            signal_type = 'short_compression'
        else:
            signal_type = 'none'
        
        return TermStructureMRSignal(
            symbol=symbol,
            signal_date=target_date,
            front_iv=front_iv,
            front_dte=front_dte,
            front_expiry=front_expiry,
            back_iv=back_iv,
            back_dte=back_dte,
            back_expiry=back_expiry,
            term_slope=term_slope,
            term_z=term_z,
            signal_type=signal_type,
            atm_iv_pctl=atm_iv_pctl,
            underlying_price=underlying_price,
        )
    
    def _find_expiry_in_range(
        self,
        bar_store,
        target_date: date,
        symbol: str,
        underlying_price: float,
        dte_range: tuple[int, int],
    ) -> tuple[Optional[date], Optional[int]]:
        """Find best expiry within DTE range that has valid ATM data."""
        expiries = bar_store.get_available_expiries(target_date, symbol)
        
        min_dte, max_dte = dte_range
        
        # Filter to range
        candidates = [(exp, dte) for exp, dte in expiries if min_dte <= dte <= max_dte]
        
        if not candidates:
            return None, None
        
        # Check each for usability (has ATM pair)
        for exp, dte in sorted(candidates, key=lambda x: x[1]):
            atm_strike, call_bar, put_bar = bar_store.find_atm_strike(
                target_date, symbol, exp, underlying_price
            )
            if atm_strike is not None:
                return exp, dte
        
        return None, None
    
    def _update_history(self, symbol: str, target_date: date, term_slope: float):
        """Update rolling history for symbol."""
        if symbol not in self._history:
            self._history[symbol] = []
        
        self._history[symbol].append((target_date, term_slope))
        
        # Keep bounded to lookback + buffer
        max_len = self.config.lookback_days + 20
        if len(self._history[symbol]) > max_len:
            self._history[symbol] = self._history[symbol][-max_len:]
    
    def _compute_zscore(self, symbol: str, current_slope: float) -> float:
        """Compute z-score of current term slope vs rolling history."""
        if symbol not in self._history:
            return 0.0
        
        history = self._history[symbol]
        
        if len(history) < self.config.min_history_days:
            return 0.0
        
        # Get last N days of slopes
        slopes = [s for _, s in history[-self.config.lookback_days:]]
        
        if len(slopes) < 2:
            return 0.0
        
        mean = np.mean(slopes)
        std = np.std(slopes)
        
        if std < 1e-6:
            return 0.0
        
        return (current_slope - mean) / std
    
    def load_history(self, symbol: str, history: List[tuple[date, float]]):
        """Load pre-computed history for symbol (for backfill)."""
        self._history[symbol] = history
    
    def get_history(self, symbol: str) -> List[tuple[date, float]]:
        """Get current history for symbol."""
        return self._history.get(symbol, [])
    
    def clear_history(self, symbol: Optional[str] = None):
        """Clear history for symbol or all symbols."""
        if symbol:
            self._history.pop(symbol, None)
        else:
            self._history.clear()
