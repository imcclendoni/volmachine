"""
JSON Report Exporter for VolMachine.

Exports daily run results as structured JSON for the Desk UI.
"""

import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Any

from data.schemas import TradeCandidate, RegimeClassification, EdgeSignal


def export_report_json(
    report_date: date,
    regime: Optional[RegimeClassification],
    edges: list[EdgeSignal],
    candidates: list[TradeCandidate],
    trading_allowed: bool = True,
    do_not_trade_reasons: list[str] = None,
    portfolio: dict = None,
    output_dir: str = './logs/reports',
    # New diagnostic fields
    provider_status: dict = None,
    universe_scan: dict = None,
    vrp_metrics: list[dict] = None,
) -> Path:
    """
    Export daily report as JSON.
    
    Args:
        report_date: Date of the report
        regime: Current regime classification
        edges: List of detected edges
        candidates: List of trade candidates
        trading_allowed: Whether trading is allowed today
        do_not_trade_reasons: Reasons if trading blocked
        portfolio: Portfolio state dict
        output_dir: Directory to write report
        provider_status: { connected: bool, source: str, last_run: str }
        universe_scan: { symbols_scanned: int, with_data: int, with_edges: int, with_trades: int, symbol_list: [...] }
        vrp_metrics: [{ symbol: str, atm_iv: float, rv_20: float, iv_rv_ratio: float, threshold: float }]
        
    Returns:
        Path to the written JSON file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Build report structure
    report = {
        'report_date': report_date.isoformat(),
        'generated_at': datetime.now().isoformat(),
        'trading_allowed': trading_allowed,
        'do_not_trade_reasons': do_not_trade_reasons or [],
        
        # New diagnostic fields
        'provider_status': provider_status or {
            'connected': False,
            'source': 'unknown',
            'last_run': datetime.now().isoformat(),
        },
        'universe_scan': universe_scan or {
            'symbols_scanned': 0,
            'symbols_with_data': 0,
            'symbols_with_edges': 0,
            'symbols_with_trades': 0,
            'symbol_list': [],
        },
        'vrp_metrics': vrp_metrics or [],
        
        'regime': _serialize_regime(regime),
        'edges': [_serialize_edge(e) for e in edges],
        'candidates': [_serialize_candidate(c) for c in candidates],
        'portfolio': portfolio or _default_portfolio(),
    }
    
    # Write dated file
    json_path = output_path / f'{report_date.isoformat()}.json'
    with open(json_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    # Also write latest.json for easy UI access
    latest_path = output_path / 'latest.json'
    with open(latest_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    return json_path


def _serialize_regime(regime: Optional[RegimeClassification]) -> dict:
    """Serialize regime to dict."""
    if not regime:
        return {
            'state': 'unknown',
            'confidence': 0.0,
            'rationale': 'No regime classification available',
        }
    
    return {
        'state': regime.regime.value if hasattr(regime.regime, 'value') else str(regime.regime),
        'confidence': regime.confidence,
        'rationale': regime.rationale or '',
    }


def _serialize_edge(edge: EdgeSignal) -> dict:
    """Serialize edge signal to dict."""
    return {
        'symbol': edge.symbol,
        'type': edge.edge_type.value if hasattr(edge.edge_type, 'value') else str(edge.edge_type),
        'strength': edge.strength,
        'direction': edge.direction.value if hasattr(edge.direction, 'value') else str(edge.direction),
        'metrics': edge.metrics,
        'rationale': edge.rationale or '',
    }


def _serialize_candidate(candidate: TradeCandidate) -> dict:
    """Serialize trade candidate to dict."""
    structure = candidate.structure
    edge = candidate.edge
    
    # Build legs info
    legs = []
    if structure and structure.legs:
        for leg in structure.legs:
            legs.append({
                'action': 'SELL' if leg.quantity < 0 else 'BUY',
                'quantity': abs(leg.quantity),
                'symbol': candidate.symbol,
                'expiration': leg.contract.expiration.isoformat() if leg.contract else '',
                'strike': leg.contract.strike if leg.contract else 0,
                'option_type': leg.contract.option_type.value.upper() if leg.contract else '',
            })
    
    # Check for valid structure
    has_valid_structure = (
        structure is not None and 
        structure.legs and 
        len(structure.legs) > 0 and
        not structure.is_placeholder
    )
    
    # Compute DTE if we have expiration
    dte = None
    if has_valid_structure and structure.legs[0].contract:
        exp = structure.legs[0].contract.expiration
        dte = (exp - date.today()).days
    
    return {
        'id': str(candidate.id),
        'symbol': candidate.symbol,
        'recommendation': candidate.recommendation,
        'is_valid': candidate.is_valid,
        'rationale': candidate.rationale or '',
        'validation_messages': candidate.validation_messages,
        
        # Structure details
        'structure': {
            'type': structure.structure_type if structure else 'none',
            'legs': legs,
            'expiration': structure.legs[0].contract.expiration.isoformat() if has_valid_structure and structure.legs[0].contract else None,
            'dte': dte,
            'entry_credit_points': float(structure.entry_credit or 0.0) if structure else 0.0,
            'entry_credit_dollars': float(structure.entry_credit_dollars or 0.0) if structure else 0.0,
            'entry_debit_points': float(structure.entry_debit or 0.0) if structure else 0.0,
            'entry_debit_dollars': float(structure.entry_debit_dollars or 0.0) if structure else 0.0,
            'max_loss_points': float(structure.max_loss or 0.0) if structure else 0.0,
            'max_loss_dollars': float(structure.max_loss_dollars or 0.0) if structure else 0.0,
            'max_profit_points': float(structure.max_profit or 0.0) if structure else 0.0,
            'breakevens': structure.breakevens if structure else [],
        } if structure else None,
        
        # Sizing - use direct attributes, not candidate.sizing
        'sizing': {
            'recommended_contracts': candidate.recommended_contracts,
            'risk_per_contract_dollars': candidate.risk_per_contract,
            'total_risk_dollars': candidate.total_risk,
            # What-if sizing at alternative risk levels (2%, 5%, 10%)
            'what_if_sizes': getattr(candidate, 'what_if_sizes', None) or {},
        },
        
        # Edge info
        'edge': {
            'type': edge.edge_type if edge else 'unknown',
            'strength': edge.strength if edge else 0,
            'direction': edge.direction if edge else 'unknown',
            'metrics': edge.metrics if edge else {},
            'rationale': edge.rationale if edge else '',
        },
        
        # Regime at time of candidate
        'regime': {
            'state': candidate.regime.regime.value if candidate.regime and hasattr(candidate.regime.regime, 'value') else str(candidate.regime.regime) if candidate.regime else 'unknown',
            'confidence': candidate.regime.confidence if candidate.regime else 0,
        },
        
        # PASS diagnostics
        'pass_diagnostics': [
            {
                'structure_type': d.structure_type,
                'width_points': d.width_points,
                'expiration_dte': d.expiration_dte,
                'failure_reason': d.failure_reason,
                'min_oi_found': d.min_oi_found,
                'min_volume_found': d.min_volume_found,
                'max_bid_ask_pct': d.max_bid_ask_pct,
                'conservative_credit': d.conservative_credit,
                'conservative_debit': d.conservative_debit,
                'max_loss_dollars': d.max_loss_dollars,
                'risk_cap_dollars': d.risk_cap_dollars,
            }
            for d in (candidate.pass_diagnostics or [])
        ],
    }


def _default_portfolio() -> dict:
    """Default empty portfolio state."""
    return {
        'positions_open': 0,
        'total_max_loss_dollars': 0.0,
        'realized_pnl_today_dollars': 0.0,
        'unrealized_pnl_dollars': 0.0,
        'kill_switch_active': False,
        'kill_switch_reason': None,
    }
