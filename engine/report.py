"""
Daily Desk Report Generator.

Creates markdown and HTML reports summarizing:
- Regime classification
- Detected edges
- Trade candidates
- Portfolio state
- Risk status
"""

from datetime import date, datetime
from pathlib import Path
from typing import Optional

from data.schemas import (
    DailyReport,
    RegimeClassification,
    EdgeSignal,
    TradeCandidate,
    PortfolioState,
)
from engine.decision import format_candidate_summary


def generate_markdown_report(report: DailyReport) -> str:
    """
    Generate markdown report from DailyReport data.
    
    Args:
        report: DailyReport data
        
    Returns:
        Markdown string
    """
    lines = []
    
    # Header
    lines.append(f"# Daily Desk Report - {report.report_date}")
    lines.append(f"*Generated: {report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}*")
    lines.append("")
    
    # Trading Status
    if report.trading_allowed:
        lines.append("**Status: 🟢 TRADING ALLOWED**")
    else:
        lines.append("**Status: 🔴 TRADING BLOCKED**")
        for reason in report.do_not_trade_reasons:
            lines.append(f"- {reason}")
    lines.append("")
    
    # Regime
    lines.append("## Market Regime")
    r = report.regime
    lines.append(f"**{r.regime.value.upper()}** (confidence: {r.confidence:.0%})")
    lines.append(f"> {r.rationale}")
    lines.append("")
    
    # Volatility State
    lines.append("## Volatility State")
    lines.append("| Symbol | ATM IV | Term Structure |")
    lines.append("|--------|--------|----------------|")
    for symbol, iv in report.vol_state.items():
        ts = report.term_structure.get(symbol, "N/A")
        lines.append(f"| {symbol} | {iv:.1%} | {ts} |")
    lines.append("")
    
    # Edges
    lines.append("## Edges Detected")
    if report.edges:
        for edge in sorted(report.edges, key=lambda e: -e.strength):
            emoji = "🔥" if edge.strength > 0.7 else "⚡" if edge.strength > 0.4 else "💡"
            lines.append(f"### {emoji} {edge.symbol} - {edge.edge_type.value}")
            lines.append(f"- **Strength**: {edge.strength:.0%}")
            lines.append(f"- **Direction**: {edge.direction.value}")
            lines.append(f"- **Rationale**: {edge.rationale}")
            lines.append("")
    else:
        lines.append("*No significant edges detected today.*")
        lines.append("")
    
    # Trade Candidates
    lines.append("## Trade Candidates")
    if report.candidates:
        for i, candidate in enumerate(report.candidates, 1):
            rec = candidate.recommendation
            emoji = "✅" if rec == "TRADE" else "⚠️" if rec == "REVIEW" else "❌"
            lines.append(f"### {emoji} Candidate {i}: {candidate.symbol}")
            lines.append(f"**Recommendation: {rec}**")
            lines.append("")
            
            # Quality Score (if available)
            if candidate.quality_score:
                qs = candidate.quality_score
                lines.append(f"**Quality Score: {qs.get('total', 0)}/100 ({qs.get('grade', 'N/A')})**")
                lines.append("")
            
            # Trade summary
            lines.append("```")
            lines.append(format_candidate_summary(candidate))
            lines.append("```")
            lines.append("")
            
            # Edge Explanation (if available)
            if candidate.edge_explanation:
                lines.append("<details>")
                lines.append("<summary>📊 Edge Explanation (click to expand)</summary>")
                lines.append("")
                lines.append(candidate.edge_explanation)
                lines.append("</details>")
                lines.append("")
            
            # Candidate Explanation (if available)
            if candidate.candidate_explanation:
                lines.append("<details>")
                lines.append("<summary>📈 Trade Explanation (click to expand)</summary>")
                lines.append("")
                lines.append(candidate.candidate_explanation)
                lines.append("</details>")
                lines.append("")
            
            # Probability Metrics (if available)
            if candidate.probability_metrics:
                pm = candidate.probability_metrics
                lines.append("<details>")
                lines.append("<summary>📊 Probability Metrics (click to expand)</summary>")
                lines.append("")
                lines.append(f"⚠️ *{pm.get('warning', 'Model-based estimates only.')}*")
                lines.append("")
                
                # Core probabilities
                lines.append("**Core Probabilities:**")
                lines.append("| Metric | Value |")
                lines.append("|--------|-------|")
                lines.append(f"| Model PoP (expiration) | {pm.get('pop_expiry', 0):.1%} |")
                lines.append(f"| P(Short Strike OTM) | {pm.get('p_otm_short_strike', 0):.1%} |")
                lines.append(f"| Breakeven Distance | {pm.get('breakeven_distance_pct', 0):.1f}% from spot |")
                lines.append("")
                
                # Expected value
                lines.append("**Expected Value:**")
                lines.append("| Metric | Value |")
                lines.append("|--------|-------|")
                lines.append(f"| EV at Expiration | ${pm.get('expected_pnl_expiry', 0):+.0f} |")
                lines.append(f"| EV per $1 Risk | ${pm.get('ev_per_dollar_risk', 0):.3f} |")
                lines.append("")
                
                # Honesty metrics
                lines.append("**Honesty Metrics:**")
                lines.append("| Metric | Value |")
                lines.append("|--------|-------|")
                lines.append(f"| Credit / Width | {pm.get('credit_to_width_ratio', 0):.1%} |")
                lines.append(f"| Reward / Risk | {pm.get('reward_to_risk_ratio', 0):.2f}:1 |")
                lines.append("")
                
                # Stress scenarios
                stress = pm.get("stress_scenarios", {})
                if stress:
                    lines.append("**Stress Scenarios:**")
                    lines.append("| Scenario | PnL |")
                    lines.append("|----------|-----|")
                    for scenario, pnl in sorted(stress.items()):
                        lines.append(f"| {scenario} | ${pnl:+.0f} |")
                    lines.append("")
                
                # Assumptions
                assumptions = pm.get("assumptions", {})
                if assumptions:
                    lines.append("**Model Assumptions:**")
                    lines.append(f"- IV: {assumptions.get('iv', 0):.1%}")
                    lines.append(f"- Time: {assumptions.get('time_to_expiry_days', 0):.0f} days")
                    lines.append(f"- Rate: {assumptions.get('risk_free_rate', 0):.2%}")
                
                lines.append("</details>")
                lines.append("")
            
            lines.append("---")
            lines.append("")
    else:
        lines.append("*No trade candidates for today.*")
        lines.append("")
    
    # Portfolio State
    lines.append("## Portfolio State")
    p = report.portfolio
    lines.append(f"- **Open Positions**: {p.trades_open}")
    lines.append(f"- **Total Max Loss Exposure**: ${p.total_max_loss:.2f}")
    lines.append(f"- **Unrealized P&L**: ${p.unrealized_pnl:.2f}")
    lines.append(f"- **Realized P&L (Today)**: ${p.realized_pnl_today:.2f}")
    lines.append("")
    
    if p.trades_open > 0:
        lines.append("### Aggregate Greeks")
        lines.append(f"- Delta: {p.portfolio_delta:.2f}")
        lines.append(f"- Gamma: {p.portfolio_gamma:.4f}")
        lines.append(f"- Theta: ${p.portfolio_theta:.2f}/day")
        lines.append(f"- Vega: ${p.portfolio_vega:.2f}")
        lines.append("")
    
    # Risk Status
    lines.append("## Risk Status")
    lines.append(f"- Daily Loss: {p.daily_loss_pct:.1f}%")
    lines.append(f"- Weekly Loss: {p.weekly_loss_pct:.1f}%")
    lines.append(f"- Max Drawdown: {p.max_drawdown_pct:.1f}%")
    
    if p.kill_switch_active:
        lines.append("")
        lines.append(f"**⚠️ KILL SWITCH ACTIVE: {p.kill_switch_reason}**")
    lines.append("")
    
    # Footer
    lines.append("---")
    lines.append("*This report is for informational purposes only. No guaranteed profits.*")
    
    return "\n".join(lines)


def generate_html_report(report: DailyReport) -> str:
    """
    Generate HTML report from DailyReport data.
    
    Simple HTML wrapper around markdown content.
    """
    markdown = generate_markdown_report(report)
    
    # Simple HTML template
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Daily Desk Report - {report.report_date}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .report {{
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{ color: #1a1a1a; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        h2 {{ color: #333; margin-top: 30px; }}
        h3 {{ color: #555; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background: #f8f9fa; }}
        pre {{ background: #f8f9fa; padding: 15px; border-radius: 4px; overflow-x: auto; }}
        code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }}
        blockquote {{ border-left: 4px solid #007bff; margin: 15px 0; padding-left: 15px; color: #666; }}
        .status-green {{ color: #28a745; }}
        .status-red {{ color: #dc3545; }}
    </style>
</head>
<body>
    <div class="report">
        <pre style="white-space: pre-wrap;">{markdown}</pre>
    </div>
</body>
</html>"""
    
    return html


def save_report(
    report: DailyReport,
    output_dir: str = "./logs/reports",
    formats: list[str] = None,
) -> list[str]:
    """
    Save report to files.
    
    Args:
        report: DailyReport data
        output_dir: Output directory
        formats: List of formats ['markdown', 'html']
        
    Returns:
        List of saved file paths
    """
    if formats is None:
        formats = ['markdown', 'html']
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    saved = []
    
    if 'markdown' in formats:
        md_path = output_path / f"{report.report_date}.md"
        md_content = generate_markdown_report(report)
        md_path.write_text(md_content)
        saved.append(str(md_path))
    
    if 'html' in formats:
        html_path = output_path / f"{report.report_date}.html"
        html_content = generate_html_report(report)
        html_path.write_text(html_content)
        saved.append(str(html_path))
    
    return saved


def create_daily_report(
    report_date: date,
    regime: RegimeClassification,
    vol_state: dict[str, float],
    term_structure: dict[str, str],
    edges: list[EdgeSignal],
    candidates: list[TradeCandidate],
    portfolio: PortfolioState,
    trading_allowed: bool = True,
    do_not_trade_reasons: list[str] = None,
) -> DailyReport:
    """
    Create a DailyReport from components.
    
    Args:
        report_date: Date of report
        regime: Current regime classification
        vol_state: Symbol -> ATM IV
        term_structure: Symbol -> contango/backwardation
        edges: List of detected edges
        candidates: List of trade candidates
        portfolio: Current portfolio state
        trading_allowed: Whether trading is allowed
        do_not_trade_reasons: Reasons for no trading
        
    Returns:
        DailyReport
    """
    return DailyReport(
        report_date=report_date,
        generated_at=datetime.now(),
        regime=regime,
        vol_state=vol_state,
        term_structure=term_structure,
        edges=edges,
        candidates=candidates,
        portfolio=portfolio,
        trading_allowed=trading_allowed,
        do_not_trade_reasons=do_not_trade_reasons or [],
    )
