"""
Volatility Risk Premium (VRP) Edge Detector.

Detects when implied volatility is significantly overpriced relative to
realized volatility - a persistent edge in options markets.

The VRP exists because options sellers demand a premium for taking
on tail risk, and this premium tends to be systematically too high.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from data.schemas import (
    EdgeSignal,
    EdgeType,
    TradeDirection,
    RegimeState,
    OptionChain,
    OHLCV,
)
from regime.features import calculate_realized_volatility


@dataclass
class VRPConfig:
    """Configuration for VRP edge detection."""
    
    # Realized vol calculation
    rv_window: int = 20  # Days for RV calculation
    
    # IV to use
    use_atm_iv: bool = True
    
    # Signal thresholds
    iv_rv_ratio_threshold: float = 1.3  # Signal when IV/RV > 1.3
    lookback_days: int = 252  # For percentile calculation
    percentile_threshold: float = 80  # Top 20% = signal
    
    # Minimum absolute IV level
    min_iv: float = 0.10  # 10% - don't signal on very low IV
    
    # Signal strength mapping
    strong_signal_percentile: float = 90
    weak_signal_percentile: float = 70


@dataclass
class VRPMetrics:
    """Computed VRP metrics."""
    
    atm_iv: float
    rv_20d: float
    iv_rv_ratio: float
    iv_rv_percentile: float  # vs history
    iv_percentile: float  # current IV vs history
    rv_percentile: float  # current RV vs history
    
    # Historical context
    avg_iv_rv_ratio: float
    median_iv_rv_ratio: float


def calculate_atm_iv(
    option_chain: OptionChain,
    target_dte: int = 30,
    as_of_date: Optional[date] = None,
) -> Optional[float]:
    """
    Calculate ATM implied volatility.
    
    Uses the nearest expiration to target DTE and averages
    ATM call and put IV.
    
    Args:
        option_chain: Full option chain
        target_dte: Target days to expiration
        as_of_date: Reference date for DTE calculation (default: today)
        
    Returns:
        ATM IV or None if unavailable
    """
    ref_date = as_of_date or date.today()
    
    # Find nearest expiration to target DTE
    best_exp = None
    best_dte_diff = float('inf')
    
    for exp in option_chain.expirations:
        dte = (exp - ref_date).days
        if dte > 0 and abs(dte - target_dte) < best_dte_diff:
            best_dte_diff = abs(dte - target_dte)
            best_exp = exp
    
    if best_exp is None:
        return None
    
    # Get ATM strike
    atm_strike = option_chain.get_atm_strike(best_exp)
    
    # Get call and put at ATM
    from data.schemas import OptionType
    call = option_chain.get_contract(best_exp, atm_strike, OptionType.CALL)
    put = option_chain.get_contract(best_exp, atm_strike, OptionType.PUT)
    
    ivs = []
    if call and call.iv and call.iv > 0:
        ivs.append(call.iv)
    if put and put.iv and put.iv > 0:
        ivs.append(put.iv)
    
    if not ivs:
        return None
    
    return np.mean(ivs)


def calculate_vrp_metrics(
    option_chain: OptionChain,
    ohlcv_history: list[OHLCV],
    iv_history: Optional[pd.Series] = None,
    rv_history: Optional[pd.Series] = None,
    config: Optional[VRPConfig] = None,
) -> VRPMetrics:
    """
    Calculate VRP metrics from option chain and price history.
    
    Args:
        option_chain: Current option chain with IV
        ohlcv_history: Price history for RV calculation (need at least rv_window + lookback)
        iv_history: Optional historical IV series
        rv_history: Optional historical RV series
        config: VRP configuration
        
    Returns:
        VRPMetrics
    """
    if config is None:
        config = VRPConfig()
    
    # Get current ATM IV
    atm_iv = calculate_atm_iv(option_chain)
    if atm_iv is None:
        raise ValueError("Cannot calculate ATM IV from option chain")
    
    # Calculate current RV
    prices = pd.Series([bar.close for bar in ohlcv_history])
    rv_series = calculate_realized_volatility(prices, config.rv_window, annualize=True)
    rv_20d = rv_series.iloc[-1] if pd.notna(rv_series.iloc[-1]) else 0.15
    
    # IV/RV ratio
    iv_rv_ratio = atm_iv / rv_20d if rv_20d > 0 else 1.0
    
    # Historical percentiles
    if iv_history is not None and len(iv_history) >= config.lookback_days:
        iv_percentile = (iv_history[-config.lookback_days:] < atm_iv).sum() / config.lookback_days * 100
    else:
        iv_percentile = 50.0  # Default to median
    
    if rv_history is not None and len(rv_history) >= config.lookback_days:
        rv_percentile = (rv_history[-config.lookback_days:] < rv_20d).sum() / config.lookback_days * 100
    else:
        rv_percentile = 50.0
    
    # IV/RV ratio history
    if iv_history is not None and rv_history is not None:
        ratio_history = iv_history / rv_history
        ratio_history = ratio_history.replace([np.inf, -np.inf], np.nan).dropna()
        
        if len(ratio_history) >= config.lookback_days:
            recent_ratios = ratio_history[-config.lookback_days:]
            iv_rv_percentile = (recent_ratios < iv_rv_ratio).sum() / len(recent_ratios) * 100
            avg_iv_rv_ratio = recent_ratios.mean()
            median_iv_rv_ratio = recent_ratios.median()
        else:
            iv_rv_percentile = 50.0
            avg_iv_rv_ratio = 1.15  # Historical average is around 1.15
            median_iv_rv_ratio = 1.10
    else:
        # Use reasonable defaults based on historical studies
        iv_rv_percentile = 50.0
        avg_iv_rv_ratio = 1.15
        median_iv_rv_ratio = 1.10
        
        # Estimate percentile from ratio alone
        # If ratio > 1.3, it's likely in upper percentiles
        if iv_rv_ratio > 1.5:
            iv_rv_percentile = 90.0
        elif iv_rv_ratio > 1.3:
            iv_rv_percentile = 80.0
        elif iv_rv_ratio > 1.15:
            iv_rv_percentile = 60.0
        elif iv_rv_ratio < 0.9:
            iv_rv_percentile = 20.0
    
    return VRPMetrics(
        atm_iv=atm_iv,
        rv_20d=rv_20d,
        iv_rv_ratio=iv_rv_ratio,
        iv_rv_percentile=iv_rv_percentile,
        iv_percentile=iv_percentile,
        rv_percentile=rv_percentile,
        avg_iv_rv_ratio=avg_iv_rv_ratio,
        median_iv_rv_ratio=median_iv_rv_ratio,
    )


def detect_vrp_edge(
    metrics: VRPMetrics,
    regime: RegimeState,
    config: Optional[VRPConfig] = None,
) -> Optional[EdgeSignal]:
    """
    Detect VRP edge signal.
    
    Signals when IV is expensive relative to RV and market regime
    is favorable for selling premium.
    
    Args:
        metrics: Computed VRP metrics
        regime: Current market regime
        config: VRP configuration
        
    Returns:
        EdgeSignal if edge detected, None otherwise
    """
    if config is None:
        config = VRPConfig()
    
    # Check basic conditions
    # 1. IV must be above minimum threshold
    if metrics.atm_iv < config.min_iv:
        return None
    
    # 2. Check IV/RV ratio
    ratio_signal = metrics.iv_rv_ratio >= config.iv_rv_ratio_threshold
    
    # 3. Check percentile
    percentile_signal = metrics.iv_rv_percentile >= config.percentile_threshold
    
    # Need both ratio and percentile to signal
    if not (ratio_signal or percentile_signal):
        return None
    
    # Regime filter
    # Don't sell premium in panic - even if VRP looks attractive
    if regime == RegimeState.HIGH_VOL_PANIC:
        # Only signal if extremely attractive AND regime is calming
        if metrics.iv_rv_percentile < 95:
            return None
    
    # Calculate signal strength
    if metrics.iv_rv_percentile >= config.strong_signal_percentile:
        strength = 1.0
    elif metrics.iv_rv_percentile >= config.percentile_threshold:
        strength = 0.7
    elif metrics.iv_rv_percentile >= config.weak_signal_percentile:
        strength = 0.4
    else:
        strength = 0.2
    
    # Boost strength if both conditions met
    if ratio_signal and percentile_signal:
        strength = min(strength + 0.2, 1.0)
    
    # Direction: SHORT (sell premium)
    direction = TradeDirection.SHORT
    
    # Build rationale
    rationale = (
        f"VRP edge: IV ({metrics.atm_iv:.1%}) vs RV ({metrics.rv_20d:.1%}) = "
        f"ratio {metrics.iv_rv_ratio:.2f} ({metrics.iv_rv_percentile:.0f}th percentile). "
        f"IV is rich relative to realized vol."
    )
    
    return EdgeSignal(
        timestamp=datetime.now(),
        symbol="",  # To be filled by caller
        edge_type=EdgeType.VOLATILITY_RISK_PREMIUM,
        strength=strength,
        direction=direction,
        metrics={
            'atm_iv': round(metrics.atm_iv, 4),
            'rv_20d': round(metrics.rv_20d, 4),
            'iv_rv_ratio': round(metrics.iv_rv_ratio, 4),
            'iv_rv_percentile': round(metrics.iv_rv_percentile, 1),
            'iv_percentile': round(metrics.iv_percentile, 1),
            'rv_percentile': round(metrics.rv_percentile, 1),
        },
        rationale=rationale,
        regime_at_signal=regime,
    )


class VRPDetector:
    """Volatility Risk Premium edge detector."""
    
    def __init__(self, config: Optional[VRPConfig] = None, cache_dir: str = './cache'):
        self.config = config or VRPConfig()
        self.cache_dir = cache_dir
        self._iv_history: dict[str, pd.Series] = {}
        self._rv_history: dict[str, pd.Series] = {}
        
        # Load persisted history on init
        self._load_histories()
    
    def detect(
        self,
        symbol: str,
        option_chain: OptionChain,
        ohlcv_history: list[OHLCV],
        regime: RegimeState,
        as_of_date: Optional[date] = None,
    ) -> Optional[EdgeSignal]:
        """
        Detect VRP edge for a symbol.
        
        Args:
            symbol: Underlying symbol
            option_chain: Current option chain
            ohlcv_history: Price history
            regime: Current market regime
            as_of_date: Reference date for calculations
            
        Returns:
            EdgeSignal if edge detected
        """
        try:
            metrics = calculate_vrp_metrics(
                option_chain=option_chain,
                ohlcv_history=ohlcv_history,
                iv_history=self._iv_history.get(symbol),
                rv_history=self._rv_history.get(symbol),
                config=self.config,
            )
            
            # Update histories
            self._update_histories(symbol, metrics, as_of_date)
            
            signal = detect_vrp_edge(metrics, regime, self.config)
            
            if signal:
                signal.symbol = symbol
            
            return signal
            
        except Exception as e:
            # Log error but don't crash
            print(f"VRP detection error for {symbol}: {e}")
            return None
    
    def _update_histories(self, symbol: str, metrics: VRPMetrics, as_of_date: Optional[date] = None):
        """Update historical IV and RV series."""
        if symbol not in self._iv_history:
            self._iv_history[symbol] = pd.Series(dtype=float)
        if symbol not in self._rv_history:
            self._rv_history[symbol] = pd.Series(dtype=float)
        
        # Use as_of_date or today
        ref_date = as_of_date or date.today()
        self._iv_history[symbol][ref_date] = metrics.atm_iv
        self._rv_history[symbol][ref_date] = metrics.rv_20d
        
        # Keep bounded
        if len(self._iv_history[symbol]) > 500:
            self._iv_history[symbol] = self._iv_history[symbol].iloc[-500:]
        if len(self._rv_history[symbol]) > 500:
            self._rv_history[symbol] = self._rv_history[symbol].iloc[-500:]
        
        # Persist after update
        self._save_histories()
    
    def _save_histories(self):
        """Save IV/RV histories to cache directory."""
        import os
        import json
        
        os.makedirs(self.cache_dir, exist_ok=True)
        cache_file = os.path.join(self.cache_dir, 'vrp_histories.json')
        
        data = {
            'iv_history': {
                sym: {str(k): v for k, v in series.items()}
                for sym, series in self._iv_history.items()
            },
            'rv_history': {
                sym: {str(k): v for k, v in series.items()}
                for sym, series in self._rv_history.items()
            },
        }
        
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Warning: Could not save VRP histories: {e}")
    
    def _load_histories(self):
        """Load IV/RV histories from cache directory."""
        import os
        import json
        
        cache_file = os.path.join(self.cache_dir, 'vrp_histories.json')
        
        if not os.path.exists(cache_file):
            return
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            for sym, series_data in data.get('iv_history', {}).items():
                self._iv_history[sym] = pd.Series({
                    date.fromisoformat(k): v for k, v in series_data.items()
                })
            
            for sym, series_data in data.get('rv_history', {}).items():
                self._rv_history[sym] = pd.Series({
                    date.fromisoformat(k): v for k, v in series_data.items()
                })
        except Exception as e:
            print(f"Warning: Could not load VRP histories: {e}")

            self._rv_history[symbol] = self._rv_history[symbol].iloc[-500:]
