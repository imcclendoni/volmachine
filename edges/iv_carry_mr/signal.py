"""
IV Carry Mean Reversion Edge - Signal Generator.

Detects when ATM IV is significantly elevated (z-score >= 2) with
favorable regime conditions for selling premium.
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional, List, Dict, Any
from collections import defaultdict
import numpy as np

from .config import IVCarryMRConfig


@dataclass
class IVCarryMRSignal:
    """Signal from IV Carry MR detector."""
    symbol: str
    signal_date: date
    
    # IV metrics
    atm_iv: float
    iv_zscore: float
    iv_mean: float
    iv_std: float
    
    # Regime gates
    rv_20d: float
    rv_iv_ratio: float
    
    # Trend
    trend: str  # "bullish" or "bearish"
    ma_fast: float
    ma_slow: float
    
    # Direction
    direction: str  # "SELL_PUTS" or "SELL_CALLS"
    
    # Structure info
    structure_type: str = "credit_spread"
    underlying_price: float = 0.0
    target_expiry: Optional[date] = None
    
    @property
    def is_triggered(self) -> bool:
        return abs(self.iv_zscore) >= 2.0 and self.rv_iv_ratio < 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'symbol': self.symbol,
            'signal_date': self.signal_date.isoformat() if isinstance(self.signal_date, date) else self.signal_date,
            'atm_iv': round(self.atm_iv, 4),
            'iv_zscore': round(self.iv_zscore, 2),
            'iv_mean': round(self.iv_mean, 4),
            'iv_std': round(self.iv_std, 4),
            'rv_20d': round(self.rv_20d, 4),
            'rv_iv_ratio': round(self.rv_iv_ratio, 2),
            'trend': self.trend,
            'ma_fast': round(self.ma_fast, 2),
            'ma_slow': round(self.ma_slow, 2),
            'direction': self.direction,
            'structure_type': self.structure_type,
            'underlying_price': round(self.underlying_price, 2),
            'target_expiry': self.target_expiry.isoformat() if self.target_expiry else None,
        }


def compute_atm_iv_for_date(
    bar_store,
    target_date: date,
    symbol: str,
    underlying_price: float,
    target_dte_min: int = 25,
    target_dte_max: int = 45,
) -> Optional[float]:
    """
    Compute ATM IV for a target date within DTE range.
    
    Returns average of ATM call and put IV, or None if unavailable.
    """
    from edges.term_structure_mr.signal import compute_atm_iv_for_expiry
    
    # Find expiry in target DTE range
    # get_available_expiries returns list of (expiry_date, dte) tuples
    expiries_data = bar_store.get_available_expiries(target_date, symbol)
    if not expiries_data:
        return None
    
    target_expiry = None
    for exp_date, dte in expiries_data:
        if target_dte_min <= dte <= target_dte_max:
            target_expiry = exp_date
            break
    
    if target_expiry is None:
        return None
    
    # Compute ATM IV
    atm_iv = compute_atm_iv_for_expiry(
        bar_store, target_date, symbol, target_expiry, underlying_price
    )
    
    return atm_iv


def calculate_realized_volatility(
    prices: List[float],
    window: int = 20,
    annualize: bool = True,
) -> float:
    """Calculate realized volatility from price series."""
    if len(prices) < window + 1:
        return 0.0
    
    returns = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            returns.append(np.log(prices[i] / prices[i-1]))
    
    if len(returns) < window:
        return 0.0
    
    recent_returns = returns[-window:]
    rv = np.std(recent_returns)
    
    if annualize:
        rv *= np.sqrt(252)
    
    return rv


class IVCarryMRDetector:
    """
    IV Carry Mean Reversion Edge Detector.
    
    Detects when ATM IV z-score is elevated and conditions favor
    selling premium (low RV/IV ratio, clear trend).
    """
    
    def __init__(self, config: Optional[IVCarryMRConfig] = None):
        self.config = config or IVCarryMRConfig()
        self._iv_history: Dict[str, List[tuple]] = defaultdict(list)
    
    def _update_iv_history(
        self, 
        symbol: str, 
        target_date: date, 
        atm_iv: float,
    ) -> None:
        """Add IV observation to history."""
        history = self._iv_history[symbol]
        
        # Avoid duplicates
        if history and history[-1][0] >= target_date:
            return
        
        history.append((target_date, atm_iv))
        
        # Keep only lookback_days + buffer
        max_len = self.config.lookback_days + 30
        if len(history) > max_len:
            self._iv_history[symbol] = history[-max_len:]
    
    def _compute_iv_zscore(
        self, 
        symbol: str, 
        current_iv: float,
    ) -> tuple:
        """
        Compute z-score of current IV vs rolling history.
        
        Returns (z_score, mean, std)
        """
        history = self._iv_history.get(symbol, [])
        
        if len(history) < self.config.lookback_days // 2:
            return 0.0, current_iv, 0.01
        
        # Get IVs from last lookback_days
        ivs = [iv for _, iv in history[-self.config.lookback_days:]]
        
        if len(ivs) < 20:
            return 0.0, current_iv, 0.01
        
        mean_iv = np.mean(ivs)
        std_iv = np.std(ivs)
        
        if std_iv < 0.001:
            return 0.0, mean_iv, 0.01
        
        z_score = (current_iv - mean_iv) / std_iv
        
        return z_score, mean_iv, std_iv
    
    def _compute_trend(
        self,
        prices: List[float],
    ) -> tuple:
        """
        Compute trend from price moving averages.
        
        Returns (trend, ma_fast, ma_slow)
        """
        if len(prices) < self.config.trend_slow_ma:
            return "neutral", 0.0, 0.0
        
        ma_fast = np.mean(prices[-self.config.trend_fast_ma:])
        ma_slow = np.mean(prices[-self.config.trend_slow_ma:])
        
        if ma_fast > ma_slow:
            trend = "bullish"
        elif ma_fast < ma_slow:
            trend = "bearish"
        else:
            trend = "neutral"
        
        return trend, ma_fast, ma_slow
    
    def detect(
        self,
        bar_store,
        target_date: date,
        symbol: str,
        underlying_price: float,
        price_history: List[float],
    ) -> Optional[IVCarryMRSignal]:
        """
        Detect IV Carry MR signal.
        
        Args:
            bar_store: OptionBarStore for option data
            target_date: Date to check signal
            symbol: Underlying symbol
            underlying_price: Current underlying price
            price_history: List of recent underlying prices (oldest first)
            
        Returns:
            IVCarryMRSignal if triggered, None otherwise
        """
        # 1. Compute ATM IV
        atm_iv = compute_atm_iv_for_date(
            bar_store, target_date, symbol, underlying_price,
            self.config.min_dte, self.config.max_dte,
        )
        
        if atm_iv is None or atm_iv <= 0:
            return None
        
        # Update history
        self._update_iv_history(symbol, target_date, atm_iv)
        
        # 2. Compute IV z-score
        iv_zscore, iv_mean, iv_std = self._compute_iv_zscore(symbol, atm_iv)
        
        # 3. Compute RV and RV/IV ratio
        rv_20d = calculate_realized_volatility(
            price_history, self.config.rv_window, annualize=True
        )
        
        rv_iv_ratio = rv_20d / atm_iv if atm_iv > 0 else 1.0
        
        # Gate: Reject if RV/IV is too high (vol spiking)
        if rv_iv_ratio > self.config.rv_iv_max:
            return None
        
        # 4. Compute trend
        trend, ma_fast, ma_slow = self._compute_trend(price_history)
        
        # Gate: Require clear trend
        if trend == "neutral":
            return None
        
        # 5. Check z-score threshold
        if abs(iv_zscore) < self.config.iv_zscore_threshold:
            return None
        
        # 6. Determine direction based on trend
        if trend == "bullish":
            direction = "SELL_PUTS"
        else:
            direction = "SELL_CALLS"
        
        # 7. Find target expiry (expiries is list of (date, dte) tuples)
        expiries_data = bar_store.get_available_expiries(target_date, symbol)
        target_expiry = None
        if expiries_data:
            for exp_date, dte in expiries_data:
                if self.config.min_dte <= dte <= self.config.max_dte:
                    target_expiry = exp_date
                    break
        
        return IVCarryMRSignal(
            symbol=symbol,
            signal_date=target_date,
            atm_iv=atm_iv,
            iv_zscore=iv_zscore,
            iv_mean=iv_mean,
            iv_std=iv_std,
            rv_20d=rv_20d,
            rv_iv_ratio=rv_iv_ratio,
            trend=trend,
            ma_fast=ma_fast,
            ma_slow=ma_slow,
            direction=direction,
            structure_type="credit_spread",
            underlying_price=underlying_price,
            target_expiry=target_expiry,
        )
    
    def load_iv_history(self, symbol: str, history: List[tuple]) -> None:
        """Load pre-existing IV history for a symbol."""
        self._iv_history[symbol] = list(history)
    
    def get_iv_history(self, symbol: str) -> List[tuple]:
        """Get IV history for a symbol."""
        return list(self._iv_history.get(symbol, []))
