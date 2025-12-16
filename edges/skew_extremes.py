"""
Skew Extremes Edge Detector.

Analyzes the put/call IV skew to detect when fear (put premium) or
complacency (call premium) reaches extremes.

Typical behavior: Puts are more expensive than calls (negative skew)
due to crash protection demand. Extremes in either direction signal edges.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import numpy as np

from data.schemas import (
    EdgeSignal,
    EdgeType,
    TradeDirection,
    RegimeState,
    OptionChain,
    OptionType,
)
from structures.greeks import find_strike_for_delta


@dataclass
class SkewConfig:
    """Configuration for skew edge detection."""
    
    # Delta for skew measurement
    target_delta: float = 0.25  # 25-delta puts/calls
    
    # Target DTE for measurement
    target_dte: int = 30
    dte_tolerance: int = 15  # ±15 days
    
    # Historical lookback for percentile
    lookback_days: int = 60
    
    # Percentile thresholds
    percentile_extreme_high: float = 90  # Top 10%
    percentile_extreme_low: float = 10   # Bottom 10%
    
    # Absolute skew thresholds (fallback)
    skew_extreme_steep: float = 0.10   # 10 vol points
    skew_extreme_flat: float = 0.02    # 2 vol points (unusually flat)


@dataclass
class SkewMetrics:
    """Computed skew metrics."""
    
    put_iv_25d: float
    call_iv_25d: float
    put_strike: float
    call_strike: float
    atm_iv: float
    
    # Skew measures
    put_call_skew: float  # put IV - call IV (usually positive)
    put_skew_ratio: float  # put IV / ATM IV (usually > 1)
    call_skew_ratio: float  # call IV / ATM IV (usually < 1)
    
    # Context
    expiration_dte: int


def calculate_skew_metrics(
    option_chain: OptionChain,
    config: Optional[SkewConfig] = None,
) -> Optional[SkewMetrics]:
    """
    Calculate skew metrics from option chain.
    
    Args:
        option_chain: Option chain with IV data
        config: Skew configuration
        
    Returns:
        SkewMetrics or None if insufficient data
    """
    if config is None:
        config = SkewConfig()
    
    today = date.today()
    spot = option_chain.underlying_price
    
    # Find best expiration
    best_exp = None
    best_dte = None
    
    for exp in option_chain.expirations:
        dte = (exp - today).days
        if abs(dte - config.target_dte) <= config.dte_tolerance:
            if best_dte is None or abs(dte - config.target_dte) < abs(best_dte - config.target_dte):
                best_exp = exp
                best_dte = dte
    
    if best_exp is None:
        return None
    
    # Get ATM strike and IV
    atm_strike = option_chain.get_atm_strike(best_exp)
    atm_call = option_chain.get_contract(best_exp, atm_strike, OptionType.CALL)
    atm_put = option_chain.get_contract(best_exp, atm_strike, OptionType.PUT)
    
    atm_ivs = []
    if atm_call and atm_call.iv:
        atm_ivs.append(atm_call.iv)
    if atm_put and atm_put.iv:
        atm_ivs.append(atm_put.iv)
    
    if not atm_ivs:
        return None
    
    atm_iv = np.mean(atm_ivs)
    
    # Find 25-delta strikes
    # We need to approximate since we don't have exact deltas
    # Use the find_strike_for_delta utility
    
    put_strike = find_strike_for_delta(
        spot=spot,
        target_delta=config.target_delta,  # Will be negated for puts
        expiration=best_exp,
        iv=atm_iv,
        option_type="put",
        strike_increment=1.0
    )
    
    call_strike = find_strike_for_delta(
        spot=spot,
        target_delta=config.target_delta,
        expiration=best_exp,
        iv=atm_iv,
        option_type="call",
        strike_increment=1.0
    )
    
    # Get IV at these strikes
    put_contract = option_chain.get_contract(best_exp, put_strike, OptionType.PUT)
    call_contract = option_chain.get_contract(best_exp, call_strike, OptionType.CALL)
    
    # If exact strike not found, find closest
    if put_contract is None:
        # Search for closest put
        exp_contracts = option_chain.get_expiration(best_exp)
        puts = [c for c in exp_contracts if c.option_type == OptionType.PUT and c.iv and c.iv > 0]
        if puts:
            put_contract = min(puts, key=lambda c: abs(c.strike - put_strike))
    
    if call_contract is None:
        exp_contracts = option_chain.get_expiration(best_exp)
        calls = [c for c in exp_contracts if c.option_type == OptionType.CALL and c.iv and c.iv > 0]
        if calls:
            call_contract = min(calls, key=lambda c: abs(c.strike - call_strike))
    
    if put_contract is None or call_contract is None:
        return None
    
    if put_contract.iv is None or call_contract.iv is None:
        return None
    
    put_iv = put_contract.iv
    call_iv = call_contract.iv
    
    return SkewMetrics(
        put_iv_25d=put_iv,
        call_iv_25d=call_iv,
        put_strike=put_contract.strike,
        call_strike=call_contract.strike,
        atm_iv=atm_iv,
        put_call_skew=put_iv - call_iv,
        put_skew_ratio=put_iv / atm_iv if atm_iv > 0 else 1,
        call_skew_ratio=call_iv / atm_iv if atm_iv > 0 else 1,
        expiration_dte=best_dte,
    )


def detect_skew_edge(
    metrics: SkewMetrics,
    regime: RegimeState,
    skew_history: Optional[list[float]] = None,
    config: Optional[SkewConfig] = None,
) -> Optional[EdgeSignal]:
    """
    Detect skew edge.
    
    ONLY emits when skew is at EXTREME percentiles (>=90 or <=10).
    Does NOT emit for "normal" ~50th percentile readings.
    
    Edges:
    - Extreme steep skew: Puts very expensive, calls cheap
      -> Opportunity to sell put spreads or buy call spreads
    - Extreme flat skew: Puts unusually cheap relative to calls
      -> Opportunity to buy put spreads (tail protection cheap)
    
    Args:
        metrics: Skew metrics
        regime: Current market regime
        skew_history: Historical put-call skew values
        config: Configuration
        
    Returns:
        EdgeSignal if edge detected (ONLY at extremes)
    """
    if config is None:
        config = SkewConfig()
    
    current_skew = metrics.put_call_skew
    
    # Calculate percentile - REQUIRED for signal generation
    # Without sufficient history, we cannot determine if skew is extreme
    if skew_history is None or len(skew_history) < 10:
        # Not enough history - cannot emit edge
        # This prevents false signals on first runs
        return None
    
    recent = skew_history[-config.lookback_days:] if len(skew_history) >= config.lookback_days else skew_history
    percentile = sum(1 for s in recent if s < current_skew) / len(recent) * 100
    
    # Check for extreme steep skew (high fear premium in puts)
    # ONLY emit if percentile is truly extreme (>= extreme_high)
    if percentile >= config.percentile_extreme_high:
        # Puts very expensive vs calls
        # In panic, this is expected - lower signal strength
        if regime == RegimeState.HIGH_VOL_PANIC:
            strength = 0.4
        else:
            # Scale strength from 0.6 to 1.0 based on how extreme
            strength = 0.6 + (percentile - config.percentile_extreme_high) / 20 * 0.4
        
        # CLAMP strength to [0, 1]
        strength = max(0.0, min(1.0, strength))
        
        rationale = (
            f"Steep put skew: 25d put IV ({metrics.put_iv_25d:.1%}) vs "
            f"25d call IV ({metrics.call_iv_25d:.1%}) = {current_skew:.1%} spread. "
            f"Put premium extreme ({percentile:.0f}th percentile)."
        )
        
        return EdgeSignal(
            timestamp=datetime.now(),
            symbol="",
            edge_type=EdgeType.SKEW_EXTREME,
            strength=strength,
            direction=TradeDirection.SHORT,  # Sell put premium
            metrics={
                'put_iv_25d': round(metrics.put_iv_25d, 4),
                'call_iv_25d': round(metrics.call_iv_25d, 4),
                'atm_iv': round(metrics.atm_iv, 4),
                'put_call_skew': round(current_skew, 4),
                'skew_percentile': round(percentile, 1),
                'is_steep': 1.0,  # Numeric flag for steep skew
            },
            rationale=rationale,
            regime_at_signal=regime,
        )
    
    # Check for extreme flat skew (puts unusually cheap)
    # ONLY emit if percentile is truly extreme (<= extreme_low)
    if percentile <= config.percentile_extreme_low:
        # Puts unusually cheap - tail protection is affordable
        # This is unusual and worth noting
        
        # Scale strength from 0.5 to 0.8 based on how extreme
        strength = 0.5 + (config.percentile_extreme_low - percentile) / 20 * 0.3
        
        # CLAMP strength to [0, 1]
        strength = max(0.0, min(1.0, strength))
        
        rationale = (
            f"Flat put skew: 25d put IV ({metrics.put_iv_25d:.1%}) vs "
            f"25d call IV ({metrics.call_iv_25d:.1%}) = {current_skew:.1%} spread. "
            f"Put premium unusually low ({percentile:.0f}th percentile). "
            f"Tail protection cheap."
        )
        
        return EdgeSignal(
            timestamp=datetime.now(),
            symbol="",
            edge_type=EdgeType.SKEW_EXTREME,
            strength=strength,
            direction=TradeDirection.LONG,  # Buy put protection
            metrics={
                'put_iv_25d': round(metrics.put_iv_25d, 4),
                'call_iv_25d': round(metrics.call_iv_25d, 4),
                'atm_iv': round(metrics.atm_iv, 4),
                'put_call_skew': round(current_skew, 4),
                'skew_percentile': round(percentile, 1),
                'is_flat': 1.0,  # Numeric flag for flat skew
            },
            rationale=rationale,
            regime_at_signal=regime,
        )
    
    # Percentile is in normal range (10-90) - NO EDGE
    return None


class SkewDetector:
    """Skew extremes edge detector."""
    
    def __init__(self, config: Optional[SkewConfig] = None):
        self.config = config or SkewConfig()
        self._history: dict[str, list[float]] = {}
    
    def detect(
        self,
        symbol: str,
        option_chain: OptionChain,
        regime: RegimeState,
    ) -> Optional[EdgeSignal]:
        """
        Detect skew edge for a symbol.
        
        Args:
            symbol: Underlying symbol
            option_chain: Current option chain
            regime: Current market regime
            
        Returns:
            EdgeSignal if edge detected
        """
        try:
            metrics = calculate_skew_metrics(option_chain, self.config)
            
            if metrics is None:
                return None
            
            # Get history for percentile calculation
            history = self._history.get(symbol)
            
            signal = detect_skew_edge(metrics, regime, history, self.config)
            
            # Update history
            self._update_history(symbol, metrics.put_call_skew)
            
            if signal:
                signal.symbol = symbol
            
            return signal
            
        except Exception as e:
            print(f"Skew detection error for {symbol}: {e}")
            return None
    
    def _update_history(self, symbol: str, skew: float):
        """Update skew history."""
        if symbol not in self._history:
            self._history[symbol] = []
        
        self._history[symbol].append(skew)
        
        if len(self._history[symbol]) > 252:
            self._history[symbol] = self._history[symbol][-252:]
