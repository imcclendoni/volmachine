"""
Explainability Blocks for VolMachine.

Provides human-readable explanations for edges and trade candidates.
No LLM calls - uses deterministic templates with inserted metrics.

The explanations read like desk memos, teaching the user:
- Why an edge exists
- What could make it fail
- Why the structure was chosen
- The exact risk math
"""

from typing import Optional
from data.schemas import (
    EdgeSignal,
    EdgeType,
    RegimeClassification,
    RegimeState,
    TradeCandidate,
    OptionStructure,
    StructureType,
)


# ============================================================================
# Edge Explanation Templates
# ============================================================================

EDGE_MECHANISM_TEMPLATES = {
    EdgeType.VOLATILITY_RISK_PREMIUM: """
**Why this edge exists:** Option sellers systematically overcharge for volatility
insurance. Implied volatility (what you pay for options) tends to exceed realized
volatility (what actually happens). This "volatility risk premium" is compensation
for bearing tail risk. When IV/RV ratio is elevated, you're being paid more than
average to sell volatility.
""".strip(),

    EdgeType.TERM_STRUCTURE: """
**Why this edge exists:** The term structure of volatility reflects market expectations.
In contango (front < back), markets expect volatility to increase—often wrong after
events pass. In backwardation (front > back), near-term panic creates opportunities
when the catalyst fades. Mean reversion typically occurs within 5-10 days.
""".strip(),

    EdgeType.SKEW_EXTREME: """
**Why this edge exists:** Options skew (the price difference between puts and calls)
reflects fear and positioning. Extreme skew often overshoots fair value due to hedging
demand or retail speculation. When skew hits historical extremes, it tends to mean-revert
as the catalyst passes or positioning unwinds.
""".strip(),

    EdgeType.EVENT_VOL: """
**Why this edge exists:** Before known events (earnings, Fed, economic data), options
get bid up as traders hedge. After the event, IV collapses ("vol crush") even if the
move was significant. If the pre-event IV implies a larger move than historical reality,
selling that premium captures the decay.
""".strip(),

    EdgeType.GAMMA_PRESSURE: """
**Why this edge exists:** Dealer gamma hedging creates mechanical price pressure.
When dealers are short gamma near large open interest strikes, they must buy high
and sell low to stay hedged. This creates pin risk (prices magnetize to strikes)
and amplified moves away from the pin zone.
""".strip(),
}

EDGE_FAILURE_TEMPLATES = {
    EdgeType.VOLATILITY_RISK_PREMIUM: [
        "**Realized vol spikes** — a multi-sigma move occurs, and you pay more than collected",
        "**Regime shift** — market transitions from low-vol grind to high-vol panic",
        "**Gamma expansion** — overnight gaps exceed your spread width",
        "**Correlation breakdown** — diversification fails during systemic stress",
    ],
    EdgeType.TERM_STRUCTURE: [
        "**Event extension** — the catalyst (Fed, earnings) gets delayed or compounds",
        "**New risk emerges** — unexpected headline causes front IV to spike further",
        "**Carry bleeds** — contango flattens before your spread gains",
        "**Spot moves against you** — directional exposure dominates term structure",
    ],
    EdgeType.SKEW_EXTREME: [
        "**Tail risk materializes** — the fear priced into skew was justified",
        "**Trend continuation** — skew was cheap because downside keeps going",
        "**Skew re-rates** — new information makes extreme skew the new normal",
    ],
    EdgeType.EVENT_VOL: [
        "**Event is larger than implied** — the actual move exceeds IV expectations",
        "**Guidance/follow-through** — earnings beat, but guidance tanks the stock",
        "**Vol doesn't crush** — market remains uncertain, IV stays bid",
        "**Gap risk** — stock moves overnight before you can exit",
    ],
    EdgeType.GAMMA_PRESSURE: [
        "**Breakout occurs** — price explodes through the gamma zone",
        "**Positioning changes** — dealers cover and gamma flips sign",
        "**Time decay** — expiration approaches and gamma peaks violently",
    ],
}


def explain_edge(
    edge: EdgeSignal,
    regime: RegimeClassification,
    context: dict = None,
) -> str:
    """
    Generate a human-readable explanation for an edge signal.
    
    Args:
        edge: The detected edge signal
        regime: Current market regime
        context: Additional context (e.g., historical percentiles)
        
    Returns:
        Multi-section explanation string
    """
    context = context or {}
    lines = []
    
    # Header
    lines.append(f"## Edge Explanation: {edge.edge_type.value.upper()}")
    lines.append("")
    
    # Section A: Trigger Facts
    lines.append("### Trigger Facts")
    lines.append(_format_trigger_facts(edge, context))
    lines.append("")
    
    # Section B: Mechanism
    lines.append("### Why This Edge Exists")
    mechanism = EDGE_MECHANISM_TEMPLATES.get(
        edge.edge_type,
        "No mechanism template available for this edge type."
    )
    lines.append(mechanism)
    lines.append("")
    
    # Section C: Failure Modes
    lines.append("### What Could Go Wrong")
    failures = EDGE_FAILURE_TEMPLATES.get(edge.edge_type, [])
    for failure in failures:
        lines.append(f"- {failure}")
    lines.append("")
    
    # Section D: Regime Fit
    lines.append("### Regime Fit")
    lines.append(_format_regime_fit(edge, regime))
    lines.append("")
    
    return "\n".join(lines)


def _format_trigger_facts(edge: EdgeSignal, context: dict) -> str:
    """Format the trigger facts section based on edge type."""
    metrics = edge.metrics
    lines = []
    
    if edge.edge_type == EdgeType.VOLATILITY_RISK_PREMIUM:
        iv_rv = metrics.get("iv_rv_ratio", 0)
        percentile = metrics.get("percentile", 0)
        threshold = context.get("threshold", 1.3)
        lines.append(f"- **IV/RV Ratio**: {iv_rv:.2f} (threshold: {threshold})")
        lines.append(f"- **Historical Percentile**: {percentile:.0f}th")
        lines.append(f"- **Signal Strength**: {edge.strength:.0%}")
        
    elif edge.edge_type == EdgeType.TERM_STRUCTURE:
        slope = metrics.get("term_slope", 0)
        front_iv = metrics.get("front_iv", 0)
        back_iv = metrics.get("back_iv", 0)
        structure = "contango" if slope > 0 else "backwardation"
        lines.append(f"- **Structure**: {structure}")
        lines.append(f"- **Front IV**: {front_iv:.1%} | **Back IV**: {back_iv:.1%}")
        lines.append(f"- **Slope**: {slope:+.1%}")
        
    elif edge.edge_type == EdgeType.SKEW_EXTREME:
        skew = metrics.get("skew_25d", 0)
        percentile = metrics.get("percentile", 0)
        lines.append(f"- **25-delta Skew**: {skew:.1%}")
        lines.append(f"- **Historical Percentile**: {percentile:.0f}th")
        
    elif edge.edge_type == EdgeType.EVENT_VOL:
        iv_premium = metrics.get("iv_premium_pct", 0)
        expected_move = metrics.get("expected_move_pct", 0)
        historical_move = metrics.get("historical_avg_move", 0)
        lines.append(f"- **IV Premium**: {iv_premium:.0f}% above normal")
        lines.append(f"- **Implied Move**: ±{expected_move:.1%}")
        lines.append(f"- **Historical Avg Move**: ±{historical_move:.1%}")
        
    elif edge.edge_type == EdgeType.GAMMA_PRESSURE:
        gamma_sign = "short" if metrics.get("net_gamma", 0) < 0 else "long"
        pin_strike = metrics.get("pin_strike", 0)
        distance = metrics.get("distance_to_pin_pct", 0)
        lines.append(f"- **Dealer Position**: Net {gamma_sign} gamma")
        lines.append(f"- **Pin Strike**: ${pin_strike:.0f}")
        lines.append(f"- **Distance to Pin**: {distance:.1%}")
    
    else:
        lines.append(f"- **Strength**: {edge.strength:.0%}")
        lines.append(f"- **Direction**: {edge.direction.value}")
        for k, v in metrics.items():
            if isinstance(v, float):
                lines.append(f"- **{k}**: {v:.2f}")
    
    return "\n".join(lines)


def _format_regime_fit(edge: EdgeSignal, regime: RegimeClassification) -> str:
    """Explain how the current regime supports or weakens the edge."""
    regime_state = regime.regime
    confidence = regime.confidence
    
    # Regime-edge fit matrix
    fit_matrix = {
        (EdgeType.VOLATILITY_RISK_PREMIUM, RegimeState.LOW_VOL_GRIND): (
            "strong", 
            "Low-vol grind is ideal for selling premium. VRP tends to be most reliable "
            "when markets are range-bound and realized vol undershoots implied."
        ),
        (EdgeType.VOLATILITY_RISK_PREMIUM, RegimeState.HIGH_VOL_PANIC): (
            "weak",
            "High-vol panic is dangerous for short premium. Realized vol often exceeds IV "
            "during stress. Consider smaller size or passing."
        ),
        (EdgeType.VOLATILITY_RISK_PREMIUM, RegimeState.TREND_UP): (
            "moderate",
            "Trending markets have moderate VRP. Volatility tends to compress in trends, "
            "but directional risk requires careful strike selection."
        ),
        (EdgeType.VOLATILITY_RISK_PREMIUM, RegimeState.TREND_DOWN): (
            "weak",
            "Downtrends often see volatility expansion. VRP can be negative (RV > IV). "
            "Prefer call spreads over put spreads if trading."
        ),
        (EdgeType.VOLATILITY_RISK_PREMIUM, RegimeState.CHOP): (
            "moderate",
            "Choppy markets can support premium selling, but whipsaw risk is elevated. "
            "Use wider strikes and smaller size."
        ),
    }
    
    key = (edge.edge_type, regime_state)
    
    if key in fit_matrix:
        fit_strength, explanation = fit_matrix[key]
    else:
        # Default fit assessment
        if regime_state == RegimeState.LOW_VOL_GRIND:
            fit_strength, explanation = "neutral", "Low-vol environments offer stable conditions."
        elif regime_state == RegimeState.HIGH_VOL_PANIC:
            fit_strength, explanation = "weak", "High-vol panic increases risk for most strategies."
        else:
            fit_strength, explanation = "neutral", "Regime has no strong bias for this edge type."
    
    return (
        f"**Fit: {fit_strength.upper()}** (regime confidence: {confidence:.0%})\n\n"
        f"{explanation}"
    )


# ============================================================================
# Candidate Explanation Templates
# ============================================================================

STRUCTURE_RATIONALE = {
    StructureType.CREDIT_SPREAD: """
**Why this structure:** A credit spread collects premium while capping max loss.
It converts a directional view into a probability-of-profit trade. The short strike
captures theta decay; the long strike limits catastrophic loss.
""".strip(),

    StructureType.IRON_CONDOR: """
**Why this structure:** An iron condor profits from range-bound markets by selling
strangles and buying wings. It's ideal for VRP edges in low-vol regimes where you
expect the underlying to stay between strikes.
""".strip(),

    StructureType.IRON_BUTTERFLY: """
**Why this structure:** An iron butterfly maximizes premium collection by selling
ATM strikes. Higher theta than iron condor, but narrower profit zone. Best when
expecting a pin or low realized volatility.
""".strip(),

    StructureType.BUTTERFLY: """
**Why this structure:** A butterfly offers cheap exposure to a specific target.
Low cost, high reward if the underlying pins at the center strike. Ideal for
gamma pressure edges where pin risk is elevated.
""".strip(),

    StructureType.DEBIT_SPREAD: """
**Why this structure:** A debit spread defines risk for a directional bet.
You pay a premium but cap your loss. Ideal when you have conviction on direction
but want to limit exposure.
""".strip(),

    StructureType.CALENDAR: """
**Why this structure:** A calendar spread profits from front-month decay exceeding
back-month decay. Best in contango term structure where you sell expensive near-term
and hold cheaper long-term options.
""".strip(),
}


def explain_candidate(
    candidate: TradeCandidate,
    risk_budget: dict = None,
    context: dict = None,
) -> str:
    """
    Generate a human-readable explanation for a trade candidate.
    
    Args:
        candidate: The trade candidate
        risk_budget: Risk budget info (account_equity, per_trade_pct, etc.)
        context: Additional context
        
    Returns:
        Multi-section explanation string
    """
    risk_budget = risk_budget or {}
    context = context or {}
    
    s = candidate.structure
    lines = []
    
    # Header
    lines.append(f"## Trade Explanation: {s.structure_type.value.upper()}")
    lines.append("")
    
    # Section A: Structure Mapping
    lines.append("### Why This Structure")
    rationale = STRUCTURE_RATIONALE.get(
        s.structure_type,
        "No rationale template available for this structure type."
    )
    lines.append(rationale)
    lines.append("")
    
    # Section B: Risk Math
    lines.append("### Risk Math")
    lines.append(_format_risk_math(candidate, risk_budget))
    lines.append("")
    
    # Section C: Pricing Assumptions
    lines.append("### Pricing Assumptions")
    lines.append(_format_pricing_assumptions(s, context))
    lines.append("")
    
    # Section D: Monitoring / Exit Plan
    lines.append("### Monitoring & Exit Plan")
    lines.append(_format_exit_plan(candidate, s))
    lines.append("")
    
    return "\n".join(lines)


def _format_risk_math(candidate: TradeCandidate, risk_budget: dict) -> str:
    """Format the risk math section with explicit calculations."""
    s = candidate.structure
    
    # Get values with defaults
    account_equity = risk_budget.get("account_equity", 100000)
    risk_pct = risk_budget.get("per_trade_pct", 1.0)
    
    # Calculate in dollars
    max_loss_per_contract = (s.max_loss or 0) * 100  # points to dollars
    risk_budget_dollars = account_equity * (risk_pct / 100)
    
    # The actual sizing
    contracts = candidate.recommended_contracts
    total_risk = candidate.total_risk
    
    lines = []
    lines.append("```")
    lines.append(f"Account Equity:           ${account_equity:,.0f}")
    lines.append(f"Risk Budget (1%):         ${risk_budget_dollars:,.0f}")
    lines.append(f"Max Loss per Contract:    ${max_loss_per_contract:,.0f}")
    lines.append(f"")
    lines.append(f"Calculation:")
    lines.append(f"  floor({risk_budget_dollars:,.0f} / {max_loss_per_contract:,.0f}) = {contracts} contracts")
    lines.append(f"")
    lines.append(f"Recommended Contracts:    {contracts}")
    lines.append(f"Total Risk:               ${total_risk:,.0f}")
    lines.append(f"Risk % of Equity:         {(total_risk / account_equity * 100):.2f}%")
    lines.append("```")
    
    # Warning if PASS
    if candidate.recommendation == "PASS":
        lines.append("")
        lines.append(f"⚠️ **PASS**: {candidate.rationale}")
    
    return "\n".join(lines)


def _format_pricing_assumptions(s: OptionStructure, context: dict) -> str:
    """Format the pricing assumptions section."""
    lines = []
    
    if s.entry_credit:
        credit_dollars = (s.entry_credit or 0) * 100
        lines.append(f"- **Entry Target**: ${credit_dollars:.0f} credit (mid price)")
        lines.append(f"- **Conservative Credit**: Uses bid for shorts, ask for longs")
        lines.append(f"- **Risk Calculation**: Based on conservative (worst-case) fill")
    elif s.entry_debit:
        debit_dollars = (s.entry_debit or 0) * 100
        lines.append(f"- **Entry Target**: ${debit_dollars:.0f} debit (mid price)")
        lines.append(f"- **Conservative Debit**: Uses ask for longs, bid for shorts")
        lines.append(f"- **Risk Calculation**: Based on conservative (worst-case) fill")
    
    max_profit = (s.max_profit or 0) * 100 if s.max_profit else None
    max_loss = (s.max_loss or 0) * 100 if s.max_loss else None
    
    if max_profit and max_loss and max_loss > 0:
        lines.append(f"- **Risk/Reward Ratio**: {max_profit / max_loss:.2f}:1")
    
    if s.breakevens:
        be_str = ", ".join(f"${b:.2f}" for b in s.breakevens)
        lines.append(f"- **Breakevens**: {be_str}")
    
    return "\n".join(lines) if lines else "- Using mid-price for entry target, conservative for risk."


def _format_exit_plan(candidate: TradeCandidate, s: OptionStructure) -> str:
    """Format the exit plan and monitoring section."""
    lines = []
    
    # Suggested TP/SL
    if s.entry_credit:
        credit_dollars = (s.entry_credit or 0) * 100
        lines.append("**Suggested Targets:**")
        lines.append(f"- Take Profit: 50% of credit (${credit_dollars * 0.5:.0f})")
        lines.append(f"- Stop Loss: 100% of credit (${credit_dollars:.0f} debit to close)")
        lines.append(f"- Time Stop: 21 DTE or 50% of holding period")
    else:
        max_profit = (s.max_profit or 0) * 100
        lines.append("**Suggested Targets:**")
        lines.append(f"- Take Profit: 50-100% of max profit (${max_profit * 0.5:.0f}-${max_profit:.0f})")
        lines.append(f"- Stop Loss: 50% of debit paid")
        lines.append(f"- Time Stop: Manage near expiration (gamma risk)")
    
    lines.append("")
    
    # Do Not Trade conditions
    lines.append("**Do Not Trade If:**")
    lines.append("- Quote is invalid (bid=0 or bid > ask)")
    lines.append("- Bid-ask spread > 5% of mid")
    lines.append("- Open interest < 500 contracts")
    lines.append("- After-hours/pre-market (stale quotes)")
    lines.append("")
    
    # What to watch
    lines.append("**Monitor:**")
    lines.append("- IV changes (vol expansion hurts short premium)")
    lines.append("- Spot price vs short strikes (delta blowup)")
    lines.append("- Regime indicators (VIX spike, trend break)")
    lines.append("- Gamma as expiration approaches")
    
    return "\n".join(lines)


# ============================================================================
# Quality Score
# ============================================================================

def calculate_quality_score(
    candidate: TradeCandidate,
    edge: EdgeSignal,
    regime: RegimeClassification,
    liquidity_metrics: dict = None,
) -> dict:
    """
    Calculate a 0-100 quality score for a trade candidate.
    
    Components:
    - Edge strength (0-25)
    - Regime confidence (0-25)
    - Liquidity score (0-25)
    - Pricing quality (0-25)
    
    Args:
        candidate: The trade candidate
        edge: The triggering edge
        regime: Current regime
        liquidity_metrics: OI, volume, spread metrics
        
    Returns:
        Dict with total score and component breakdown
    """
    liquidity_metrics = liquidity_metrics or {}
    
    # Component 1: Edge Strength (0-25)
    edge_score = edge.strength * 25
    
    # Component 2: Regime Confidence (0-25)
    # Also factor in regime-edge fit
    base_regime = regime.confidence * 20
    
    # Regime-edge fit adjustment
    fit_boost = 0
    if regime.regime == RegimeState.LOW_VOL_GRIND:
        if edge.edge_type == EdgeType.VOLATILITY_RISK_PREMIUM:
            fit_boost = 5
        else:
            fit_boost = 2
    elif regime.regime == RegimeState.HIGH_VOL_PANIC:
        fit_boost = -5  # Penalty in high-vol
    
    regime_score = max(0, min(25, base_regime + fit_boost))
    
    # Component 3: Liquidity Score (0-25)
    oi = liquidity_metrics.get("open_interest", 1000)
    volume = liquidity_metrics.get("volume", 100)
    spread_pct = liquidity_metrics.get("bid_ask_pct", 5.0)
    
    oi_score = min(10, oi / 500)  # 0-10 based on OI
    vol_score = min(10, volume / 100)  # 0-10 based on volume
    spread_score = max(0, 5 - spread_pct)  # 0-5 inverse of spread
    
    liquidity_score = oi_score + vol_score + spread_score
    
    # Component 4: Pricing Quality (0-25)
    s = candidate.structure
    
    # Ratio of credit/max_loss - higher is better
    if s.entry_credit and s.max_loss and s.max_loss > 0:
        credit_ratio = s.entry_credit / s.max_loss
        pricing_score = min(25, credit_ratio * 50)  # 50% credit = 25 pts
    elif s.max_profit and s.entry_debit and s.entry_debit > 0:
        profit_ratio = s.max_profit / s.entry_debit
        pricing_score = min(25, profit_ratio * 12.5)  # 2:1 ratio = 25 pts
    else:
        pricing_score = 10  # Neutral
    
    # Total
    total_score = edge_score + regime_score + liquidity_score + pricing_score
    
    return {
        "total": round(total_score),
        "edge_strength": round(edge_score, 1),
        "regime_fit": round(regime_score, 1),
        "liquidity": round(liquidity_score, 1),
        "pricing_quality": round(pricing_score, 1),
        "grade": _score_to_grade(total_score),
    }


def _score_to_grade(score: float) -> str:
    """Convert numeric score to letter grade."""
    if score >= 90:
        return "A+"
    elif score >= 85:
        return "A"
    elif score >= 80:
        return "A-"
    elif score >= 75:
        return "B+"
    elif score >= 70:
        return "B"
    elif score >= 65:
        return "B-"
    elif score >= 60:
        return "C+"
    elif score >= 55:
        return "C"
    elif score >= 50:
        return "C-"
    else:
        return "D"


def format_quality_score(score_dict: dict) -> str:
    """Format quality score for display."""
    lines = [
        f"**Quality Score: {score_dict['total']}/100 ({score_dict['grade']})**",
        "",
        "| Component | Score |",
        "|-----------|-------|",
        f"| Edge Strength | {score_dict['edge_strength']}/25 |",
        f"| Regime Fit | {score_dict['regime_fit']}/25 |",
        f"| Liquidity | {score_dict['liquidity']}/25 |",
        f"| Pricing Quality | {score_dict['pricing_quality']}/25 |",
    ]
    return "\n".join(lines)
