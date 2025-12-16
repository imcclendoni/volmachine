"""
Trade Candidate Decision Objects.

Encapsulates trade recommendations with full audit trail.

UNITS: All "$" printed values are in DOLLARS (points * 100).
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import uuid4

from data.schemas import (
    EdgeSignal,
    RegimeClassification,
    OptionStructure,
    TradeCandidate,
)
from risk.sizing import SizingResult


# Contract multiplier for converting points to dollars
CONTRACT_MULTIPLIER = 100


def points_to_dollars(points: float) -> float:
    """Convert points to dollars."""
    return points * CONTRACT_MULTIPLIER


def create_trade_candidate(
    symbol: str,
    structure: OptionStructure,
    edge: EdgeSignal,
    regime: RegimeClassification,
    sizing: SizingResult,
    validation_messages: list[str] = None,
    include_explanations: bool = True,
    risk_budget: dict = None,
    liquidity_metrics: dict = None,
) -> TradeCandidate:
    """
    Create a trade candidate with full audit trail.
    
    Args:
        symbol: Underlying symbol
        structure: Option structure
        edge: Edge signal that triggered this
        regime: Market regime at time of signal
        sizing: Position sizing result
        validation_messages: Any validation warnings/errors
        include_explanations: Whether to generate explanation blocks
        risk_budget: Risk budget info for explanations
        liquidity_metrics: Liquidity info for quality score
        
    Returns:
        TradeCandidate with explanations and quality score
    """
    if validation_messages is None:
        validation_messages = []
    
    # Calculate dollar values for display
    max_loss_dollars = points_to_dollars(structure.max_loss) if structure.max_loss else 0
    
    # Determine recommendation
    if not sizing.allowed:
        recommendation = "PASS"
        rationale = f"Position sizing rejected: {sizing.rejection_reason}"
    elif sizing.recommended_contracts == 0:
        recommendation = "PASS"
        rationale = "No contracts recommended"
    elif len(validation_messages) > 0:
        recommendation = "REVIEW"
        rationale = f"Structure valid but has warnings: {'; '.join(validation_messages)}"
    else:
        recommendation = "TRADE"
        rationale = (
            f"Edge: {edge.edge_type.value} (strength {edge.strength:.0%}). "
            f"Regime: {regime.regime.value} (confidence {regime.confidence:.0%}). "
            f"Structure: {structure.structure_type.value}, max loss ${max_loss_dollars:.0f}/contract."
        )
    
    # Generate explanations and quality score
    edge_explanation = None
    candidate_explanation = None
    quality_score = None
    
    if include_explanations:
        try:
            from engine.explain import (
                explain_edge,
                explain_candidate,
                calculate_quality_score,
            )
            
            edge_explanation = explain_edge(edge, regime)
            
            # Create temporary candidate for explanation
            temp_candidate = TradeCandidate(
                id=str(uuid4()),
                timestamp=datetime.now(),
                symbol=symbol,
                structure=structure,
                edge=edge,
                regime=regime,
                recommended_contracts=sizing.recommended_contracts,
                risk_per_contract=sizing.risk_per_contract_dollars,
                total_risk=sizing.total_risk_dollars,
                is_valid=sizing.allowed and len(validation_messages) == 0,
                validation_messages=validation_messages,
                recommendation=recommendation,
                rationale=rationale,
            )
            
            candidate_explanation = explain_candidate(
                temp_candidate,
                risk_budget=risk_budget or {},
            )
            
            quality_score = calculate_quality_score(
                temp_candidate,
                edge,
                regime,
                liquidity_metrics=liquidity_metrics or {},
            )
        except Exception:
            # If explanation generation fails, continue without
            pass
    
    return TradeCandidate(
        id=str(uuid4()),
        timestamp=datetime.now(),
        symbol=symbol,
        structure=structure,
        edge=edge,
        regime=regime,
        recommended_contracts=sizing.recommended_contracts,
        risk_per_contract=sizing.risk_per_contract_dollars,
        total_risk=sizing.total_risk_dollars,
        is_valid=sizing.allowed and len(validation_messages) == 0,
        validation_messages=validation_messages,
        recommendation=recommendation,
        rationale=rationale,
        edge_explanation=edge_explanation,
        candidate_explanation=candidate_explanation,
        quality_score=quality_score,
    )


def format_candidate_summary(candidate: TradeCandidate) -> str:
    """
    Format a trade candidate for display.
    
    ALL "$" VALUES ARE IN DOLLARS (points * 100).
    """
    s = candidate.structure
    
    lines = [
        f"[{candidate.recommendation}] {candidate.symbol} - {s.structure_type.value}",
        f"  Edge: {candidate.edge.edge_type.value} ({candidate.edge.strength:.0%})",
        f"  Regime: {candidate.regime.regime.value}",
    ]
    
    # Structure details
    if s.legs:
        strikes = [f"{leg.contract.strike}{'-' if leg.quantity < 0 else '+'}" for leg in s.legs]
        lines.append(f"  Strikes: {' / '.join(strikes)}")
    
    # Credit/Debit in DOLLARS
    if s.entry_credit:
        credit_dollars = points_to_dollars(s.entry_credit)
        lines.append(f"  Credit: ${credit_dollars:.0f}")
    elif s.entry_debit:
        debit_dollars = points_to_dollars(s.entry_debit)
        lines.append(f"  Debit: ${debit_dollars:.0f}")
    
    # Max Loss in DOLLARS
    if s.max_loss:
        max_loss_dollars = points_to_dollars(s.max_loss)
        lines.append(f"  Max Loss: ${max_loss_dollars:.0f}/contract")
    
    # Max Profit in DOLLARS
    if s.max_profit:
        max_profit_dollars = points_to_dollars(s.max_profit)
        lines.append(f"  Max Profit: ${max_profit_dollars:.0f}/contract")
    
    if s.breakevens:
        lines.append(f"  Breakevens: {', '.join(f'{b:.2f}' for b in s.breakevens)}")
    
    # Greeks (per contract)
    lines.append(f"  Greeks: Δ={s.net_delta:.2f} Γ={s.net_gamma:.4f} Θ={s.net_theta:.2f} V={s.net_vega:.2f}")
    
    # Sizing (already in dollars from SizingResult)
    lines.append(f"  Recommended: {candidate.recommended_contracts} contracts")
    lines.append(f"  Total Risk: ${candidate.total_risk:.0f}")
    
    # Rationale
    lines.append(f"  Rationale: {candidate.rationale}")
    
    return "\n".join(lines)


def candidate_to_dict(candidate: TradeCandidate) -> dict:
    """
    Convert trade candidate to dictionary for JSON logging.
    
    Includes both points and dollars for clarity.
    """
    s = candidate.structure
    
    return {
        'id': candidate.id,
        'timestamp': candidate.timestamp.isoformat(),
        'symbol': candidate.symbol,
        'recommendation': candidate.recommendation,
        'structure': {
            'type': s.structure_type.value,
            'legs': [
                {
                    'strike': leg.contract.strike,
                    'type': leg.contract.option_type.value,
                    'expiration': leg.contract.expiration.isoformat(),
                    'quantity': leg.quantity,
                }
                for leg in s.legs
            ] if s.legs else [],
            'entry_credit_points': s.entry_credit,
            'entry_credit_dollars': points_to_dollars(s.entry_credit) if s.entry_credit else None,
            'entry_debit_points': s.entry_debit,
            'entry_debit_dollars': points_to_dollars(s.entry_debit) if s.entry_debit else None,
            'max_loss_points': s.max_loss,
            'max_loss_dollars': points_to_dollars(s.max_loss) if s.max_loss else None,
            'max_profit_points': s.max_profit,
            'max_profit_dollars': points_to_dollars(s.max_profit) if s.max_profit else None,
            'breakevens': s.breakevens,
            'greeks': {
                'delta': s.net_delta,
                'gamma': s.net_gamma,
                'theta': s.net_theta,
                'vega': s.net_vega,
            },
        },
        'edge': {
            'type': candidate.edge.edge_type.value,
            'strength': candidate.edge.strength,
            'direction': candidate.edge.direction.value,
            'metrics': candidate.edge.metrics,
            'rationale': candidate.edge.rationale,
        },
        'regime': {
            'state': candidate.regime.regime.value,
            'confidence': candidate.regime.confidence,
            'rationale': candidate.regime.rationale,
        },
        'sizing': {
            'recommended_contracts': candidate.recommended_contracts,
            'risk_per_contract_dollars': candidate.risk_per_contract,
            'total_risk_dollars': candidate.total_risk,
        },
        'is_valid': candidate.is_valid,
        'validation_messages': candidate.validation_messages,
        'rationale': candidate.rationale,
    }
