"""
Gamma Pressure Edge Detector.

Estimates dealer gamma exposure from option open interest.
Identifies "pin zones" where expiring options concentrate,
and "gamma flip" levels where dealer positioning changes.

NOTE: This is a PROXY based on publicly available data (OI * estimated gamma).
Actual dealer positioning requires institutional data.
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
from structures.pricing import (
    bs_greeks,
    OptionSide,
    time_to_expiry_years,
    get_risk_free_rate,
)


@dataclass
class GammaConfig:
    """Configuration for gamma pressure edge detection."""
    
    # Pin zone detection
    pin_zone_width_pct: float = 1.0  # ±1% around potential pin
    
    # DTE range for gamma analysis
    max_dte: int = 7  # Only look at near-term expirations
    
    # Gamma flip threshold
    flip_threshold: float = 0.0  # Negative = short gamma dominant
    
    # Minimum OI for significance
    min_strike_oi: int = 1000
    
    # Contract multiplier
    contract_multiplier: int = 100


@dataclass
class GammaMetrics:
    """Computed gamma exposure metrics."""
    
    # Total gamma exposure estimate
    total_gamma_exposure: float  # In dollar terms
    
    # Per-strike breakdown
    strike_gamma: dict[float, float]  # strike -> gamma exposure
    
    # Key levels
    max_gamma_strike: float  # Strike with highest gamma
    pin_zone_low: float
    pin_zone_high: float
    
    # Estimated dealer positioning
    net_gamma: float  # Positive = long gamma, negative = short gamma
    gamma_flip_level: Optional[float]  # Price where dealer gamma flips
    
    # Current price context
    current_price: float
    price_vs_max_gamma: float  # Distance from max gamma strike


def calculate_strike_gamma(
    option_chain: OptionChain,
    expiration: date,
    config: Optional[GammaConfig] = None,
) -> dict[float, float]:
    """
    Calculate gamma exposure at each strike.
    
    Gamma Exposure = OI * Gamma * 100 * Spot^2 * 0.01
    
    Assumption: Dealers are net short options (hedging retail flow).
    So high call OI = dealer short gamma, high put OI = dealer long gamma.
    
    Args:
        option_chain: Option chain with OI and Greeks
        expiration: Target expiration
        config: Configuration
        
    Returns:
        Dictionary of strike -> gamma exposure
    """
    if config is None:
        config = GammaConfig()
    
    spot = option_chain.underlying_price
    t = time_to_expiry_years(expiration)
    r = get_risk_free_rate()
    
    strike_gamma = {}
    
    for contract in option_chain.contracts:
        if contract.expiration != expiration:
            continue
        
        if contract.open_interest < config.min_strike_oi:
            continue
        
        strike = contract.strike
        
        # Calculate gamma if not provided
        if contract.greeks and contract.greeks.gamma:
            gamma = contract.greeks.gamma
        elif contract.iv and contract.iv > 0:
            # Calculate from IV
            side = OptionSide.CALL if contract.option_type == OptionType.CALL else OptionSide.PUT
            bs = bs_greeks(side, spot, strike, t, r, contract.iv)
            gamma = bs.gamma
        else:
            continue
        
        # Calculate gamma exposure
        # Gamma in dollar terms = gamma * OI * 100 * spot^2 * 0.01
        oi = contract.open_interest
        gex = gamma * oi * config.contract_multiplier * spot * spot * 0.01
        
        # Dealer positioning assumption:
        # - Calls: dealers are short (write to retail), so negative gamma when price rises
        # - Puts: dealers are short, so positive gamma when price falls
        # Net effect: Dealers hedge by buying when price falls, selling when price rises (stabilizing)
        
        if contract.option_type == OptionType.CALL:
            # Short call gamma = negative
            gex = -gex
        else:
            # Short put gamma = positive (for puts, gamma works opposite on price)
            pass
        
        if strike not in strike_gamma:
            strike_gamma[strike] = 0
        strike_gamma[strike] += gex
    
    return strike_gamma


def calculate_gamma_metrics(
    option_chain: OptionChain,
    config: Optional[GammaConfig] = None,
) -> Optional[GammaMetrics]:
    """
    Calculate aggregate gamma exposure metrics.
    
    Args:
        option_chain: Option chain with OI
        config: Configuration
        
    Returns:
        GammaMetrics or None
    """
    if config is None:
        config = GammaConfig()
    
    today = date.today()
    spot = option_chain.underlying_price
    
    # Find near-term expiration
    target_exp = None
    for exp in option_chain.expirations:
        dte = (exp - today).days
        if 0 < dte <= config.max_dte:
            target_exp = exp
            break
    
    if target_exp is None:
        # Use first available expiration
        if not option_chain.expirations:
            return None
        target_exp = option_chain.expirations[0]
    
    # Calculate per-strike gamma
    strike_gamma = calculate_strike_gamma(option_chain, target_exp, config)
    
    if not strike_gamma:
        return None
    
    # Find key levels
    max_gamma_strike = max(strike_gamma.keys(), key=lambda s: abs(strike_gamma[s]))
    total_gamma = sum(strike_gamma.values())
    
    # Pin zone
    pin_zone_width = spot * config.pin_zone_width_pct / 100
    pin_zone_low = max_gamma_strike - pin_zone_width
    pin_zone_high = max_gamma_strike + pin_zone_width
    
    # Gamma flip level (where cumulative gamma crosses zero)
    gamma_flip_level = None
    sorted_strikes = sorted(strike_gamma.keys())
    cumulative_gamma = 0
    prev_strike = None
    prev_cum = 0
    
    for strike in sorted_strikes:
        cumulative_gamma += strike_gamma[strike]
        if prev_cum < 0 and cumulative_gamma >= 0:
            # Flip from negative to positive
            gamma_flip_level = (prev_strike + strike) / 2 if prev_strike else strike
        elif prev_cum > 0 and cumulative_gamma <= 0:
            # Flip from positive to negative
            gamma_flip_level = (prev_strike + strike) / 2 if prev_strike else strike
        prev_strike = strike
        prev_cum = cumulative_gamma
    
    return GammaMetrics(
        total_gamma_exposure=total_gamma,
        strike_gamma=strike_gamma,
        max_gamma_strike=max_gamma_strike,
        pin_zone_low=pin_zone_low,
        pin_zone_high=pin_zone_high,
        net_gamma=total_gamma,
        gamma_flip_level=gamma_flip_level,
        current_price=spot,
        price_vs_max_gamma=(spot - max_gamma_strike) / spot * 100,
    )


def detect_gamma_edge(
    metrics: GammaMetrics,
    regime: RegimeState,
    config: Optional[GammaConfig] = None,
) -> Optional[EdgeSignal]:
    """
    Detect gamma-based edge.
    
    Opportunities:
    1. Price near max gamma strike before expiration -> potential pin
    2. Negative gamma environment -> larger moves likely
    3. Near gamma flip level -> volatility expansion
    
    Args:
        metrics: Gamma metrics
        regime: Current market regime
        config: Configuration
        
    Returns:
        EdgeSignal if edge detected
    """
    if config is None:
        config = GammaConfig()
    
    signals = []
    
    # Check if in pin zone
    in_pin_zone = metrics.pin_zone_low <= metrics.current_price <= metrics.pin_zone_high
    
    if in_pin_zone and abs(metrics.price_vs_max_gamma) < 0.5:
        # Very close to max gamma strike
        # Expect pinning action toward expiration
        
        strength = 0.6 + (0.5 - abs(metrics.price_vs_max_gamma)) * 0.8
        
        rationale = (
            f"Gamma pin zone: Price {metrics.current_price:.2f} near max gamma strike "
            f"{metrics.max_gamma_strike:.2f} ({metrics.price_vs_max_gamma:+.1f}%). "
            f"Potential pinning action. Consider butterfly around pin."
        )
        
        return EdgeSignal(
            timestamp=datetime.now(),
            symbol="",
            edge_type=EdgeType.GAMMA_PRESSURE,
            strength=min(strength, 1.0),
            direction=TradeDirection.SHORT,  # Sell vol around pin
            metrics={
                'max_gamma_strike': metrics.max_gamma_strike,
                'pin_zone_low': round(metrics.pin_zone_low, 2),
                'pin_zone_high': round(metrics.pin_zone_high, 2),
                'price_vs_max_gamma': round(metrics.price_vs_max_gamma, 2),
                'total_gamma_exposure': round(metrics.total_gamma_exposure, 0),
                'is_pin_zone': 1.0,  # Numeric flag for edge subtype
            },
            rationale=rationale,
            regime_at_signal=regime,
        )
    
    # Check for negative gamma environment
    if metrics.net_gamma < config.flip_threshold:
        # Dealers are short gamma - need to buy dips and sell rips
        # This amplifies moves
        
        strength = min(abs(metrics.net_gamma) / 1e9, 0.8)  # Normalize
        
        if regime == RegimeState.HIGH_VOL_PANIC:
            # Negative gamma in panic = explosive moves possible
            strength = min(strength + 0.2, 1.0)
        
        rationale = (
            f"Negative dealer gamma: Net GEX {metrics.net_gamma/1e6:.0f}M. "
            f"Dealer hedging may amplify moves. "
            f"Consider directional structures or wider spreads."
        )
        
        return EdgeSignal(
            timestamp=datetime.now(),
            symbol="",
            edge_type=EdgeType.GAMMA_PRESSURE,
            strength=strength,
            direction=TradeDirection.LONG,  # Long vol / expect larger moves
            metrics={
                'net_gamma': round(metrics.net_gamma, 0),
                'gamma_flip_level': metrics.gamma_flip_level or 0.0,
                'current_price': metrics.current_price,
                'is_negative_gamma': 1.0,  # Numeric flag for edge subtype
            },
            rationale=rationale,
            regime_at_signal=regime,
        )
    
    return None


class GammaPressureDetector:
    """Gamma pressure edge detector (proxy)."""
    
    def __init__(self, config: Optional[GammaConfig] = None):
        self.config = config or GammaConfig()
    
    def detect(
        self,
        symbol: str,
        option_chain: OptionChain,
        regime: RegimeState,
    ) -> Optional[EdgeSignal]:
        """
        Detect gamma pressure edge for a symbol.
        
        Note: This is a PROXY estimate based on public OI data.
        
        Args:
            symbol: Underlying symbol
            option_chain: Current option chain
            regime: Current market regime
            
        Returns:
            EdgeSignal if edge detected
        """
        try:
            metrics = calculate_gamma_metrics(option_chain, self.config)
            
            if metrics is None:
                return None
            
            signal = detect_gamma_edge(metrics, regime, self.config)
            
            if signal:
                signal.symbol = symbol
            
            return signal
            
        except Exception as e:
            print(f"Gamma detection error for {symbol}: {e}")
            return None
    
    def get_gamma_levels(
        self,
        option_chain: OptionChain
    ) -> Optional[dict]:
        """Get key gamma levels for display."""
        metrics = calculate_gamma_metrics(option_chain, self.config)
        
        if metrics is None:
            return None
        
        return {
            'max_gamma_strike': metrics.max_gamma_strike,
            'gamma_flip_level': metrics.gamma_flip_level,
            'net_gamma': metrics.net_gamma,
            'current_price': metrics.current_price,
        }
