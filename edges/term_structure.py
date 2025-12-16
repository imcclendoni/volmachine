"""
Term Structure Edge Detector.

Analyzes the volatility term structure (front-month vs back-month IV)
to detect contango/backwardation extremes.

Normal state: Contango (front < back) - longer-dated options have higher IV
Inverted: Backwardation (front > back) - fear is elevated, near-term premium

Edge opportunities arise at extremes of this structure.
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


@dataclass
class TermStructureConfig:
    """Configuration for term structure edge detection."""
    
    # Target DTE ranges for comparison
    front_dte_range: tuple[int, int] = (7, 30)  # Front month
    back_dte_range: tuple[int, int] = (30, 60)  # Back month
    
    # Thresholds
    contango_threshold: float = 0.02  # 2% - front < back
    backwardation_threshold: float = -0.02  # -2% - front > back
    
    # Signal thresholds
    extreme_contango: float = 0.08  # 8% spread = opportunity
    extreme_backwardation: float = -0.08
    
    # Historical percentile
    lookback_days: int = 60
    percentile_extreme: float = 85


@dataclass
class TermStructureMetrics:
    """Computed term structure metrics."""
    
    front_iv: float
    front_dte: int
    back_iv: float
    back_dte: int
    
    # Spread
    iv_spread: float  # back - front (positive = contango)
    iv_spread_pct: float  # spread as percentage of front
    
    # State
    is_contango: bool
    is_backwardation: bool


def calculate_expiry_iv(
    option_chain: OptionChain,
    target_dte_range: tuple[int, int]
) -> tuple[Optional[float], Optional[int]]:
    """
    Calculate ATM IV for an expiration within the target DTE range.
    
    Returns:
        Tuple of (IV, actual DTE) or (None, None)
    """
    today = date.today()
    
    best_exp = None
    best_dte = None
    min_dte, max_dte = target_dte_range
    
    for exp in option_chain.expirations:
        dte = (exp - today).days
        if min_dte <= dte <= max_dte:
            if best_dte is None or dte < best_dte:
                best_exp = exp
                best_dte = dte
    
    if best_exp is None:
        return None, None
    
    # Get ATM IV for this expiration
    atm_strike = option_chain.get_atm_strike(best_exp)
    
    call = option_chain.get_contract(best_exp, atm_strike, OptionType.CALL)
    put = option_chain.get_contract(best_exp, atm_strike, OptionType.PUT)
    
    ivs = []
    if call and call.iv and call.iv > 0:
        ivs.append(call.iv)
    if put and put.iv and put.iv > 0:
        ivs.append(put.iv)
    
    if not ivs:
        return None, None
    
    return np.mean(ivs), best_dte


def calculate_term_structure(
    option_chain: OptionChain,
    config: Optional[TermStructureConfig] = None,
) -> Optional[TermStructureMetrics]:
    """
    Calculate term structure metrics from option chain.
    
    Args:
        option_chain: Option chain with multiple expirations
        config: Configuration
        
    Returns:
        TermStructureMetrics or None if insufficient data
    """
    if config is None:
        config = TermStructureConfig()
    
    # Get front month IV
    front_iv, front_dte = calculate_expiry_iv(
        option_chain, config.front_dte_range
    )
    
    # Get back month IV
    back_iv, back_dte = calculate_expiry_iv(
        option_chain, config.back_dte_range
    )
    
    if front_iv is None or back_iv is None:
        return None
    
    # Calculate spread
    iv_spread = back_iv - front_iv  # Positive = contango
    iv_spread_pct = iv_spread / front_iv if front_iv > 0 else 0
    
    return TermStructureMetrics(
        front_iv=front_iv,
        front_dte=front_dte,
        back_iv=back_iv,
        back_dte=back_dte,
        iv_spread=iv_spread,
        iv_spread_pct=iv_spread_pct,
        is_contango=iv_spread_pct > config.contango_threshold,
        is_backwardation=iv_spread_pct < config.backwardation_threshold,
    )


def detect_term_structure_edge(
    metrics: TermStructureMetrics,
    regime: RegimeState,
    config: Optional[TermStructureConfig] = None,
) -> Optional[EdgeSignal]:
    """
    Detect term structure edge.
    
    Edges:
    - Extreme contango: Calendar spreads (buy front, sell back)
    - Extreme backwardation: Calendar spreads (sell front, buy back)
      or signals elevated near-term fear
    
    Args:
        metrics: Term structure metrics
        regime: Current market regime
        config: Configuration
        
    Returns:
        EdgeSignal if edge detected
    """
    if config is None:
        config = TermStructureConfig()
    
    # Check for extreme contango
    if metrics.iv_spread_pct >= config.extreme_contango:
        # Back-month IV significantly higher than front
        # Opportunity: Calendar spreads buying front, selling back
        # Or: Front-month might be under-priced
        
        strength = min(
            (metrics.iv_spread_pct - config.extreme_contango) / 0.05 + 0.5,
            1.0
        )
        
        rationale = (
            f"Extreme contango: front {metrics.front_iv:.1%} ({metrics.front_dte}d) vs "
            f"back {metrics.back_iv:.1%} ({metrics.back_dte}d) = {metrics.iv_spread_pct:.1%} spread. "
            f"Back-month IV elevated."
        )
        
        return EdgeSignal(
            timestamp=datetime.now(),
            symbol="",
            edge_type=EdgeType.TERM_STRUCTURE,
            strength=strength,
            direction=TradeDirection.SHORT,  # Sell back-month premium
            metrics={
                'front_iv': round(metrics.front_iv, 4),
                'front_dte': metrics.front_dte,
                'back_iv': round(metrics.back_iv, 4),
                'back_dte': metrics.back_dte,
                'iv_spread_pct': round(metrics.iv_spread_pct, 4),
                'is_contango': 1.0,  # Numeric flag for contango
            },
            rationale=rationale,
            regime_at_signal=regime,
        )
    
    # Check for extreme backwardation
    if metrics.iv_spread_pct <= config.extreme_backwardation:
        # Front-month IV significantly higher than back
        # This usually indicates elevated near-term fear
        # Opportunity: Sell front-month premium or calendar spreads
        
        # Be careful in panic regime - backwardation is expected
        if regime == RegimeState.HIGH_VOL_PANIC:
            # Still signal but lower strength
            strength = 0.4
        else:
            strength = min(
                (config.extreme_backwardation - metrics.iv_spread_pct) / 0.05 + 0.5,
                1.0
            )
        
        rationale = (
            f"Extreme backwardation: front {metrics.front_iv:.1%} ({metrics.front_dte}d) vs "
            f"back {metrics.back_iv:.1%} ({metrics.back_dte}d) = {metrics.iv_spread_pct:.1%} spread. "
            f"Near-term fear elevated."
        )
        
        return EdgeSignal(
            timestamp=datetime.now(),
            symbol="",
            edge_type=EdgeType.TERM_STRUCTURE,
            strength=strength,
            direction=TradeDirection.SHORT,  # Sell front-month premium
            metrics={
                'front_iv': round(metrics.front_iv, 4),
                'front_dte': metrics.front_dte,
                'back_iv': round(metrics.back_iv, 4),
                'back_dte': metrics.back_dte,
                'iv_spread_pct': round(metrics.iv_spread_pct, 4),
                'is_backwardation': 1.0,  # Numeric flag for backwardation
            },
            rationale=rationale,
            regime_at_signal=regime,
        )
    
    return None


class TermStructureDetector:
    """Term structure edge detector."""
    
    def __init__(self, config: Optional[TermStructureConfig] = None):
        self.config = config or TermStructureConfig()
        self._history: dict[str, list[float]] = {}  # symbol -> spread history
    
    def detect(
        self,
        symbol: str,
        option_chain: OptionChain,
        regime: RegimeState,
    ) -> Optional[EdgeSignal]:
        """
        Detect term structure edge for a symbol.
        
        Args:
            symbol: Underlying symbol
            option_chain: Current option chain
            regime: Current market regime
            
        Returns:
            EdgeSignal if edge detected
        """
        try:
            metrics = calculate_term_structure(option_chain, self.config)
            
            if metrics is None:
                return None
            
            # Update history
            self._update_history(symbol, metrics.iv_spread_pct)
            
            signal = detect_term_structure_edge(metrics, regime, self.config)
            
            if signal:
                signal.symbol = symbol
            
            return signal
            
        except Exception as e:
            print(f"Term structure detection error for {symbol}: {e}")
            return None
    
    def _update_history(self, symbol: str, spread_pct: float):
        """Update spread history."""
        if symbol not in self._history:
            self._history[symbol] = []
        
        self._history[symbol].append(spread_pct)
        
        # Keep bounded
        if len(self._history[symbol]) > 252:
            self._history[symbol] = self._history[symbol][-252:]
    
    def get_term_structure_state(
        self,
        option_chain: OptionChain
    ) -> str:
        """Get simple term structure state string."""
        metrics = calculate_term_structure(option_chain, self.config)
        
        if metrics is None:
            return "unknown"
        
        if metrics.is_contango:
            return "contango"
        elif metrics.is_backwardation:
            return "backwardation"
        else:
            return "flat"
