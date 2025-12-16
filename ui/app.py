"""
VolMachine Desk UI - Streamlit Dashboard

Hedge-fund style trading dashboard for daily operations.
Run with: streamlit run ui/app.py
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# Page config
st.set_page_config(
    page_title="VolMachine Desk",
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom CSS
st.markdown("""
<style>
/* Dark theme override */
.stApp {
    background-color: #0a0e14;
}

/* Trade card styling */
.trade-card {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.1) 0%, rgba(16, 185, 129, 0.05) 100%);
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 12px;
    padding: 20px;
    margin: 10px 0;
}

.pass-card {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, rgba(239, 68, 68, 0.05) 100%);
    border: 1px solid rgba(239, 68, 68, 0.3);
    border-radius: 12px;
    padding: 20px;
    margin: 10px 0;
}

.order-ticket {
    background: #1a2029;
    border: 1px solid #2a3444;
    border-radius: 8px;
    padding: 16px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 14px;
}

.regime-gauge {
    background: linear-gradient(90deg, #10b981, #f59e0b, #ef4444);
    height: 8px;
    border-radius: 4px;
    margin: 10px 0;
}

/* Status pills */
.status-pill-allowed {
    background: rgba(16, 185, 129, 0.2);
    color: #10b981;
    padding: 12px 24px;
    border-radius: 24px;
    font-size: 18px;
    font-weight: 600;
    display: inline-block;
}

.status-pill-blocked {
    background: rgba(239, 68, 68, 0.2);
    color: #ef4444;
    padding: 12px 24px;
    border-radius: 24px;
    font-size: 18px;
    font-weight: 600;
    display: inline-block;
}
</style>
""", unsafe_allow_html=True)


def load_latest_report() -> dict:
    """Load the latest JSON report."""
    reports_dir = Path(__file__).parent.parent / 'logs' / 'reports'
    latest_path = reports_dir / 'latest.json'
    
    if latest_path.exists():
        with open(latest_path) as f:
            return json.load(f)
    return None


def run_engine():
    """Trigger the engine to run."""
    script_path = Path(__file__).parent.parent / 'scripts' / 'run_daily.py'
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).parent.parent),
    )
    return result


def format_dollars(value: float) -> str:
    """Format a value as dollars."""
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def format_percent(value: float) -> str:
    """Format as percentage."""
    if value is None:
        return "N/A"
    return f"{value:.0%}"


def render_trade_card(candidate: dict):
    """Render a TRADE card with executable legs."""
    symbol = candidate['symbol']
    structure = candidate.get('structure') or {}
    edge = candidate.get('edge') or {}
    regime = candidate.get('regime') or {}
    sizing = candidate.get('sizing') or {}
    
    # Header
    st.markdown(f"### 🟢 TRADE — {symbol}")
    st.caption(f"Edge: {edge.get('type', 'unknown')} ({format_percent(edge.get('strength', 0))}) • Regime: {regime.get('state', 'unknown')} ({format_percent(regime.get('confidence', 0))})")
    
    # Order ticket
    st.markdown("#### Order Ticket")
    
    legs = structure.get('legs', [])
    if legs:
        struct_type = structure.get('type', 'unknown')
        expiration = structure.get('expiration', 'N/A')
        dte = structure.get('dte', 'N/A')
        
        ticket_lines = [
            f"**Structure:** {struct_type}",
            f"**Expiration:** {expiration} (DTE: {dte})",
            "",
            "**LEGS:**",
        ]
        
        for leg in legs:
            action = leg.get('action', 'BUY')
            qty = leg.get('quantity', 1)
            exp = leg.get('expiration', '')
            strike = leg.get('strike', 0)
            opt_type = leg.get('option_type', 'CALL')
            ticket_lines.append(f"`{action} {qty} {symbol} {exp} {strike} {opt_type}`")
        
        # Pricing
        credit = structure.get('entry_credit_dollars', 0)
        debit = structure.get('entry_debit_dollars', 0)
        if credit > 0:
            ticket_lines.append(f"\n**Target (mid):** {format_dollars(credit)} credit")
            conserv = credit * 0.8  # Conservative estimate
            ticket_lines.append(f"**Conservative:** {format_dollars(conserv)} credit")
        elif debit > 0:
            ticket_lines.append(f"\n**Target (mid):** {format_dollars(debit)} debit")
        
        # Risk
        max_loss = structure.get('max_loss_dollars', 0)
        contracts = sizing.get('recommended_contracts', 0)
        total_risk = sizing.get('total_risk_dollars', 0)
        
        ticket_lines.append(f"\n**Max Loss per contract:** {format_dollars(max_loss)}")
        ticket_lines.append(f"**Recommended contracts:** {contracts}")
        ticket_lines.append(f"**Total Risk:** {format_dollars(total_risk)}")
        
        with st.container():
            st.code('\n'.join(ticket_lines).replace('**', '').replace('`', ''), language=None)
    else:
        st.warning("No legs defined for this structure")
    
    # Expandable sections
    with st.expander("WHY THIS TRADE"):
        st.write(f"**Edge Rationale:** {edge.get('rationale', 'N/A')}")
        st.write(f"**Candidate Rationale:** {candidate.get('rationale', 'N/A')}")
        
        # What could go wrong
        st.markdown("**What Could Go Wrong:**")
        st.markdown("""
        - Underlying moves sharply against position
        - IV crush reduces premium faster than theta decay
        - Early assignment risk on short legs
        - Wider-than-expected fills impact P&L
        """)
    
    with st.expander("PROBABILITY CONTEXT"):
        st.warning("⚠️ Model-based context, not prediction.")
        st.info("Probability metrics not available for this candidate.")


def render_pass_card(candidate: dict):
    """Render a PASS card with diagnostics."""
    symbol = candidate['symbol']
    edge = candidate.get('edge') or {}
    diagnostics = candidate.get('pass_diagnostics', [])
    
    # Header
    st.markdown(f"### 🔴 PASS — {symbol}")
    
    # Primary reason
    reason = candidate.get('rationale', 'No valid structure found')
    st.error(f"**WHY PASS:** {reason}")
    
    # Diagnostics table
    if diagnostics:
        with st.expander("DIAGNOSTICS (Structure Attempts)"):
            for d in diagnostics:
                cols = st.columns(4)
                with cols[0]:
                    st.metric("Structure", d.get('structure_type', 'N/A'))
                with cols[1]:
                    st.metric("Width", d.get('width_points', 'N/A'))
                with cols[2]:
                    st.metric("DTE", d.get('expiration_dte', 'N/A'))
                with cols[3]:
                    st.metric("Failure", d.get('failure_reason', 'N/A'))
                
                st.caption(f"OI: {d.get('min_oi_found', 'N/A')} | Credit: {format_dollars(d.get('conservative_credit', 0))} | Max Loss: {format_dollars(d.get('max_loss_dollars', 0))}")
                st.divider()
    else:
        st.info("No detailed diagnostics available")


def main():
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("◉ VOLMACHINE DESK")
    with col2:
        if st.button("🔄 Run Desk Now", type="primary"):
            with st.spinner("Running engine..."):
                result = run_engine()
                if result.returncode == 0:
                    st.success("Run complete!")
                    st.rerun()
                else:
                    st.error(f"Run failed: {result.stderr}")
    
    # Load data
    report = load_latest_report()
    
    if not report:
        st.warning("No report data available. Click 'Run Desk Now' to generate a report.")
        return
    
    # ═══════════════════════════════════════════════════════════
    # A) TOP BANNER - TRADING STATUS
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("📋 What To Do Today")
    
    trading_allowed = report.get('trading_allowed', True)
    if trading_allowed:
        st.markdown('<div class="status-pill-allowed">🟢 TRADING ALLOWED</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-pill-blocked">🔴 TRADING BLOCKED</div>', unsafe_allow_html=True)
        reasons = report.get('do_not_trade_reasons', [])
        for reason in reasons:
            st.error(f"❌ {reason}")
    
    st.caption(f"Report: {report.get('report_date')} | Generated: {report.get('generated_at', 'N/A')[:19]}")
    
    # ═══════════════════════════════════════════════════════════
    # B) MARKET REGIME CARD
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("📊 Market Regime")
    
    regime = report.get('regime', {})
    regime_state = regime.get('state', 'unknown').upper()
    regime_confidence = regime.get('confidence', 0)
    regime_rationale = regime.get('rationale', 'No rationale available')
    
    col1, col2 = st.columns([2, 1])
    with col1:
        # Regime state with color
        color_map = {
            'BULL': '🟢',
            'BEAR': '🔴',
            'CHOP': '🟠',
            'RECOVERY': '🔵',
        }
        emoji = color_map.get(regime_state, '⚪')
        st.markdown(f"## {emoji} {regime_state}")
        st.write(regime_rationale)
    
    with col2:
        # Confidence gauge
        st.metric("Confidence", f"{regime_confidence:.0%}")
        st.progress(regime_confidence)
    
    # ═══════════════════════════════════════════════════════════
    # C) TRADE DECISIONS
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("💼 Trade Decisions")
    
    candidates = report.get('candidates', [])
    
    # Split into TRADE and PASS
    trade_candidates = [c for c in candidates if c.get('recommendation') == 'TRADE']
    pass_candidates = [c for c in candidates if c.get('recommendation') in ['PASS', 'REVIEW']]
    
    # TRADE section
    if trade_candidates:
        st.markdown("### 🟢 TRADE TODAY")
        for candidate in trade_candidates:
            with st.container():
                render_trade_card(candidate)
                st.markdown("---")
    else:
        st.info("No TRADE candidates today. See PASS analysis below for details.")
    
    # PASS section
    if pass_candidates:
        st.markdown("### 🔴 NO TRADE (PASS / REVIEW)")
        for candidate in pass_candidates:
            with st.container():
                render_pass_card(candidate)
                st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════
    # E) EDGES TABLE
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("📈 Edges Detected (Raw)")
    
    edges = report.get('edges', [])
    if edges:
        edge_data = []
        for e in edges:
            edge_data.append({
                'Symbol': e.get('symbol', 'N/A'),
                'Type': e.get('type', 'N/A'),
                'Strength': f"{e.get('strength', 0):.0%}",
                'Direction': e.get('direction', 'N/A'),
            })
        st.table(edge_data)
        
        # Click to show rationale
        selected_edge = st.selectbox(
            "Select edge for details:",
            options=range(len(edges)),
            format_func=lambda i: f"{edges[i].get('symbol')} - {edges[i].get('type')}"
        )
        if selected_edge is not None:
            st.info(edges[selected_edge].get('rationale', 'No rationale available'))
    else:
        st.info("No edges detected today.")
    
    # ═══════════════════════════════════════════════════════════
    # F) PORTFOLIO & RISK
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.subheader("💰 Portfolio & Risk")
    
    portfolio = report.get('portfolio', {})
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Positions Open", portfolio.get('positions_open', 0))
    with col2:
        st.metric("Max Loss Exposure", format_dollars(portfolio.get('total_max_loss_dollars', 0)))
    with col3:
        st.metric("Realized P&L Today", format_dollars(portfolio.get('realized_pnl_today_dollars', 0)))
    with col4:
        st.metric("Unrealized P&L", format_dollars(portfolio.get('unrealized_pnl_dollars', 0)))
    
    # Kill switch status
    if portfolio.get('kill_switch_active'):
        st.error(f"🚨 KILL SWITCH ACTIVE: {portfolio.get('kill_switch_reason', 'Unknown reason')}")
    
    # ═══════════════════════════════════════════════════════════
    # File Links
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.caption("📁 Report Files")
    reports_dir = Path(__file__).parent.parent / 'logs' / 'reports'
    report_date = report.get('report_date', 'latest')
    
    col1, col2, col3 = st.columns(3)
    with col1:
        md_path = reports_dir / f'{report_date}.md'
        if md_path.exists():
            st.markdown(f"[📄 Markdown Report]({md_path})")
    with col2:
        html_path = reports_dir / f'{report_date}.html'
        if html_path.exists():
            st.markdown(f"[🌐 HTML Report]({html_path})")
    with col3:
        json_path = reports_dir / f'{report_date}.json'
        if json_path.exists():
            st.markdown(f"[📊 JSON Data]({json_path})")


if __name__ == "__main__":
    main()
