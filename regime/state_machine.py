"""
Regime State Machine.

Classifies market into one of five regimes based on features.
Uses robust heuristics (not ML) for v1.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from data.schemas import RegimeState, RegimeClassification
from regime.features import RegimeFeatures


@dataclass
class RegimeThresholds:
    """Thresholds for regime classification."""
    
    # VIX thresholds
    vix_low: float = 15.0
    vix_elevated: float = 20.0
    vix_high: float = 25.0
    vix_panic: float = 35.0
    
    # Trend thresholds (price vs MA)
    trend_strong: float = 0.03   # 3% above/below MA
    trend_weak: float = 0.01     # 1% above/below MA
    
    # Volatility thresholds
    rv_low: float = 0.10         # 10% annualized
    rv_elevated: float = 0.20    # 20% annualized
    rv_high: float = 0.30        # 30% annualized
    
    # Vol ratio thresholds (short/long)
    vol_ratio_spike: float = 1.5  # Vol spike when short >> long
    vol_ratio_calm: float = 0.8   # Calming when short << long
    
    # Drawdown thresholds
    dd_correction: float = -0.10  # 10% correction
    dd_bear: float = -0.20        # 20% bear market
    
    # Return thresholds
    return_strong: float = 0.05   # 5% in period
    return_weak: float = 0.02     # 2% in period


def classify_regime(
    features: RegimeFeatures,
    thresholds: Optional[RegimeThresholds] = None
) -> RegimeClassification:
    """
    Classify market regime based on features.
    
    Regime Priority (in order of dominance):
    1. HIGH_VOL_PANIC - Extreme vol or VIX, significant drawdown
    2. TREND_DOWN - Clear downtrend with elevated vol
    3. TREND_UP - Clear uptrend with low/normal vol
    4. CHOP - No clear trend, mixed signals
    5. LOW_VOL_GRIND - Low vol, grinding higher
    
    Args:
        features: RegimeFeatures from feature extraction
        thresholds: Optional custom thresholds
        
    Returns:
        RegimeClassification with regime, confidence, and rationale
    """
    if thresholds is None:
        thresholds = RegimeThresholds()
    
    # Collect evidence for each regime
    evidence = {
        RegimeState.HIGH_VOL_PANIC: 0.0,
        RegimeState.TREND_DOWN: 0.0,
        RegimeState.TREND_UP: 0.0,
        RegimeState.CHOP: 0.0,
        RegimeState.LOW_VOL_GRIND: 0.0,
    }
    
    rationale_parts = []
    
    # =========================================================================
    # HIGH_VOL_PANIC signals
    # =========================================================================
    
    # Extreme VIX
    if features.vix_level is not None:
        if features.vix_level >= thresholds.vix_panic:
            evidence[RegimeState.HIGH_VOL_PANIC] += 3.0
            rationale_parts.append(f"VIX panic level ({features.vix_level:.1f})")
        elif features.vix_level >= thresholds.vix_high:
            evidence[RegimeState.HIGH_VOL_PANIC] += 1.5
            rationale_parts.append(f"VIX elevated ({features.vix_level:.1f})")
    
    # Extreme realized vol
    if features.rv_5d >= thresholds.rv_high:
        evidence[RegimeState.HIGH_VOL_PANIC] += 2.0
        rationale_parts.append(f"RV5d extreme ({features.rv_5d:.1%})")
    
    # Vol spike (short-term >> long-term)
    if features.rv_ratio_5_20 >= thresholds.vol_ratio_spike:
        evidence[RegimeState.HIGH_VOL_PANIC] += 1.5
        rationale_parts.append(f"Vol spiking (5d/20d={features.rv_ratio_5_20:.2f})")
    
    # Significant drawdown
    if features.drawdown_from_high <= thresholds.dd_bear:
        evidence[RegimeState.HIGH_VOL_PANIC] += 1.5
        evidence[RegimeState.TREND_DOWN] += 1.0
        rationale_parts.append(f"Bear market DD ({features.drawdown_from_high:.1%})")
    elif features.drawdown_from_high <= thresholds.dd_correction:
        evidence[RegimeState.HIGH_VOL_PANIC] += 0.5
        evidence[RegimeState.TREND_DOWN] += 1.0
        rationale_parts.append(f"Correction ({features.drawdown_from_high:.1%})")
    
    # =========================================================================
    # TREND_DOWN signals
    # =========================================================================
    
    # Price below MAs
    if features.price_vs_ma50 < -thresholds.trend_strong:
        evidence[RegimeState.TREND_DOWN] += 1.5
        rationale_parts.append(f"Price below MA50 ({features.price_vs_ma50:.1%})")
    
    if features.price_vs_ma200 < -thresholds.trend_strong:
        evidence[RegimeState.TREND_DOWN] += 1.0
        rationale_parts.append(f"Price below MA200 ({features.price_vs_ma200:.1%})")
    
    # MA breakdown (20 < 50)
    if features.ma20_vs_ma50 < 0.98:
        evidence[RegimeState.TREND_DOWN] += 1.0
        rationale_parts.append("MA20 < MA50")
    
    # Negative returns
    if features.return_20d < -thresholds.return_strong:
        evidence[RegimeState.TREND_DOWN] += 1.0
        rationale_parts.append(f"Negative 20d return ({features.return_20d:.1%})")
    
    # =========================================================================
    # TREND_UP signals
    # =========================================================================
    
    # Price above MAs
    if features.price_vs_ma50 > thresholds.trend_strong:
        evidence[RegimeState.TREND_UP] += 1.5
        rationale_parts.append(f"Price above MA50 ({features.price_vs_ma50:.1%})")
    
    if features.price_vs_ma200 > thresholds.trend_strong:
        evidence[RegimeState.TREND_UP] += 1.0
    
    # MA alignment (20 > 50)
    if features.ma20_vs_ma50 > 1.02:
        evidence[RegimeState.TREND_UP] += 1.0
        rationale_parts.append("MA20 > MA50")
    
    # Positive returns
    if features.return_20d > thresholds.return_strong:
        evidence[RegimeState.TREND_UP] += 1.0
        rationale_parts.append(f"Positive 20d return ({features.return_20d:.1%})")
    
    # Near highs
    if features.days_since_high < 10:
        evidence[RegimeState.TREND_UP] += 0.5
        evidence[RegimeState.LOW_VOL_GRIND] += 0.5
        rationale_parts.append(f"Near highs ({features.days_since_high}d)")
    
    # =========================================================================
    # LOW_VOL_GRIND signals
    # =========================================================================
    
    # Low VIX
    if features.vix_level is not None and features.vix_level < thresholds.vix_low:
        evidence[RegimeState.LOW_VOL_GRIND] += 1.5
        rationale_parts.append(f"Low VIX ({features.vix_level:.1f})")
    
    # Low realized vol
    if features.rv_20d < thresholds.rv_low:
        evidence[RegimeState.LOW_VOL_GRIND] += 1.5
        rationale_parts.append(f"Low RV ({features.rv_20d:.1%})")
    
    # Vol calming (short-term < long-term)
    if features.rv_ratio_5_20 < thresholds.vol_ratio_calm:
        evidence[RegimeState.LOW_VOL_GRIND] += 1.0
        rationale_parts.append("Vol calming")
    
    # Slight uptrend + low vol = grind
    if (features.price_vs_ma20 > 0 and 
        features.price_vs_ma20 < thresholds.trend_strong and
        features.rv_20d < thresholds.rv_elevated):
        evidence[RegimeState.LOW_VOL_GRIND] += 1.0
    
    # =========================================================================
    # CHOP signals (no strong trend, mixed)
    # =========================================================================
    
    # Near MAs but not clearly above/below
    if abs(features.price_vs_ma50) < thresholds.trend_weak:
        evidence[RegimeState.CHOP] += 1.0
        rationale_parts.append("Price near MA50")
    
    # MAs converging
    if abs(features.ma20_vs_ma50 - 1.0) < 0.01:
        evidence[RegimeState.CHOP] += 0.5
        rationale_parts.append("MAs converging")
    
    # Small returns despite volatility
    if abs(features.return_20d) < thresholds.return_weak and features.rv_20d > thresholds.rv_low:
        evidence[RegimeState.CHOP] += 1.0
        rationale_parts.append("Low returns, elevated vol")
    
    # =========================================================================
    # Determine final regime
    # =========================================================================
    
    # Find regime with highest evidence
    best_regime = max(evidence.keys(), key=lambda r: evidence[r])
    best_score = evidence[best_regime]
    
    # Calculate confidence (0-1)
    # Higher score = higher confidence, max out around 5 points
    confidence = min(best_score / 5.0, 1.0)
    
    # If no clear winner, default to CHOP with low confidence
    if best_score < 1.0:
        best_regime = RegimeState.CHOP
        confidence = 0.3
        rationale_parts.append("No clear regime signals")
    
    # Build rationale string
    rationale = f"Regime: {best_regime.value} (score: {best_score:.1f}). " + "; ".join(rationale_parts[:5])
    
    return RegimeClassification(
        timestamp=features.as_of,
        regime=best_regime,
        confidence=confidence,
        features={
            'price_vs_ma20': features.price_vs_ma20,
            'price_vs_ma50': features.price_vs_ma50,
            'price_vs_ma200': features.price_vs_ma200,
            'rv_5d': features.rv_5d,
            'rv_20d': features.rv_20d,
            'rv_60d': features.rv_60d,
            'vix_level': features.vix_level or 0,
            'drawdown': features.drawdown_from_high,
            'return_20d': features.return_20d,
        },
        rationale=rationale
    )


def should_trade_in_regime(
    regime: RegimeState,
    confidence: float,
    min_confidence: float = 0.4
) -> tuple[bool, str]:
    """
    Determine if trading is advisable in current regime.
    
    Returns:
        Tuple of (should_trade, reason)
    """
    # Low confidence = don't trade
    if confidence < min_confidence:
        return False, f"Low regime confidence ({confidence:.0%})"
    
    # Panic = very cautious (can still trade defined risk)
    if regime == RegimeState.HIGH_VOL_PANIC:
        return True, "HIGH_VOL_PANIC - cautious positions only"
    
    # Chop = reduced size
    if regime == RegimeState.CHOP:
        return True, "CHOP - reduce position sizes"
    
    # Trends and low vol grind = normal trading
    return True, f"{regime.value} - normal trading conditions"


def get_regime_bias(regime: RegimeState) -> dict:
    """
    Get trading biases based on regime.
    
    Returns dictionary with preferences for:
    - direction: bullish, bearish, neutral
    - premium: sell (short vol) or buy (long vol)
    - size_multiplier: 0.5, 1.0, 1.5 etc.
    """
    biases = {
        RegimeState.LOW_VOL_GRIND: {
            'direction': 'bullish',
            'premium': 'sell',  # Sell premium in low vol
            'size_multiplier': 1.0,
            'structures': ['iron_condor', 'credit_spread', 'butterfly'],
        },
        RegimeState.TREND_UP: {
            'direction': 'bullish',
            'premium': 'neutral',
            'size_multiplier': 1.0,
            'structures': ['debit_spread', 'calendar', 'diagonal'],
        },
        RegimeState.TREND_DOWN: {
            'direction': 'bearish',
            'premium': 'buy',  # Long vol in downtrends
            'size_multiplier': 0.75,
            'structures': ['debit_spread', 'calendar'],
        },
        RegimeState.HIGH_VOL_PANIC: {
            'direction': 'neutral',
            'premium': 'sell',  # Sell elevated premium (carefully)
            'size_multiplier': 0.5,  # Half size
            'structures': ['iron_condor', 'butterfly'],
        },
        RegimeState.CHOP: {
            'direction': 'neutral',
            'premium': 'sell',
            'size_multiplier': 0.5,
            'structures': ['iron_condor', 'butterfly'],
        },
    }
    
    return biases.get(regime, biases[RegimeState.CHOP])
