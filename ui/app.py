"""
VolMachine Desk UI - Streamlit Dashboard

Hedge-fund style trading dashboard for daily operations.
Run with: streamlit run ui/app.py
"""

import json
import os
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

# Premium dark theme CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* Global dark theme */
.stApp {
    background: linear-gradient(180deg, #0a0e14 0%, #0d1117 100%);
    color: #e6e8eb;
}

/* Headers */
h1, h2, h3 {
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em;
}

/* Main title styling */
h1 {
    background: linear-gradient(135deg, #00d9ff 0%, #7c3aed 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 2.5rem !important;
}

/* Cards and containers */
.stContainer, [data-testid="stVerticalBlock"] > div {
    backdrop-filter: blur(10px);
}

/* Metric cards */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(26, 32, 41, 0.8) 0%, rgba(26, 32, 41, 0.4) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 16px;
}

[data-testid="stMetricLabel"] {
    font-family: 'Inter', sans-serif;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #8b95a5 !important;
}

[data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    font-size: 1.8rem;
    color: #fff !important;
}

/* Trading status pills */
.trading-allowed {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.2) 0%, rgba(16, 185, 129, 0.1) 100%);
    border: 1px solid rgba(16, 185, 129, 0.4);
    color: #10b981;
    padding: 16px 32px;
    border-radius: 50px;
    font-family: 'Inter', sans-serif;
    font-size: 20px;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    display: inline-block;
    box-shadow: 0 0 30px rgba(16, 185, 129, 0.2);
}

.trading-blocked {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.2) 0%, rgba(239, 68, 68, 0.1) 100%);
    border: 1px solid rgba(239, 68, 68, 0.4);
    color: #ef4444;
    padding: 16px 32px;
    border-radius: 50px;
    font-family: 'Inter', sans-serif;
    font-size: 20px;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    display: inline-block;
    box-shadow: 0 0 30px rgba(239, 68, 68, 0.2);
}

/* Regime display */
.regime-bull { color: #10b981 !important; }
.regime-bear { color: #ef4444 !important; }
.regime-chop { color: #f59e0b !important; }
.regime-recovery { color: #3b82f6 !important; }

.regime-box {
    background: linear-gradient(135deg, rgba(26, 32, 41, 0.9) 0%, rgba(26, 32, 41, 0.6) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 16px;
    padding: 24px;
    text-align: center;
}

.regime-state {
    font-family: 'Inter', sans-serif;
    font-size: 2.5rem;
    font-weight: 800;
    letter-spacing: 0.05em;
    margin-bottom: 8px;
}

/* Order ticket */
.order-ticket {
    background: linear-gradient(135deg, #1a2029 0%, #12181f 100%);
    border: 1px solid rgba(0, 217, 255, 0.2);
    border-radius: 12px;
    padding: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    line-height: 1.8;
}

/* Code blocks */
.stCodeBlock {
    background: linear-gradient(135deg, #1a2029 0%, #12181f 100%) !important;
    border: 1px solid rgba(0, 217, 255, 0.15) !important;
    border-radius: 12px !important;
}

/* Buttons */
.stButton > button {
    background: linear-gradient(135deg, #00d9ff 0%, #7c3aed 100%);
    color: white;
    font-family: 'Inter', sans-serif;
    font-weight: 600;
    padding: 12px 24px;
    border-radius: 12px;
    border: none;
    transition: all 0.3s ease;
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 30px rgba(0, 217, 255, 0.3);
}

/* Expanders */
[data-testid="stExpander"] {
    background: rgba(26, 32, 41, 0.6);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 12px;
}

/* Progress bar */
.stProgress > div > div {
    background: linear-gradient(90deg, #00d9ff 0%, #7c3aed 100%);
}

/* Info/Warning/Error boxes */
.stAlert {
    border-radius: 12px;
}

/* Dividers */
hr {
    border-color: rgba(255,255,255,0.08) !important;
    margin: 24px 0 !important;
}

/* Tables */
.stTable {
    background: rgba(26, 32, 41, 0.6);
    border-radius: 12px;
}

/* Selectbox */
.stSelectbox > div > div {
    background: rgba(26, 32, 41, 0.8);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 8px;
}

/* Caption text */
.stCaption {
    color: #5a6373 !important;
    font-size: 12px !important;
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
    env = os.environ.copy()
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).parent.parent),
        env=env,
    )
    return result


def format_dollars(value: float) -> str:
    """Format a value as dollars."""
    if value is None:
        return "N/A"
    if value == 0:
        return "$0.00"
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
    
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(16, 185, 129, 0.05) 100%);
                border: 1px solid rgba(16, 185, 129, 0.3);
                border-left: 4px solid #10b981;
                border-radius: 16px;
                padding: 24px;
                margin: 16px 0;">
        <h3 style="color: #10b981; margin: 0 0 8px 0;">🟢 TRADE — {symbol}</h3>
        <p style="color: #8b95a5; margin: 0; font-size: 14px;">
            Edge: {edge.get('type', 'unknown')} ({format_percent(edge.get('strength', 0))}) • 
            Regime: {regime.get('state', 'unknown')} ({format_percent(regime.get('confidence', 0))})
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Order ticket
    st.markdown("##### 📋 Order Ticket")
    
    legs = structure.get('legs', [])
    if legs:
        struct_type = structure.get('type', 'unknown')
        expiration = structure.get('expiration', 'N/A')
        dte = structure.get('dte', 'N/A')
        
        ticket_text = f"""Structure: {struct_type}
Expiration: {expiration} (DTE: {dte})

LEGS:
"""
        for leg in legs:
            action = leg.get('action', 'BUY')
            qty = leg.get('quantity', 1)
            exp = leg.get('expiration', '')
            strike = leg.get('strike', 0)
            opt_type = leg.get('option_type', 'CALL')
            ticket_text += f"  {action} {qty} {symbol} {exp} {strike} {opt_type}\n"
        
        credit = structure.get('entry_credit_dollars', 0)
        debit = structure.get('entry_debit_dollars', 0)
        if credit > 0:
            ticket_text += f"\nTarget (mid): {format_dollars(credit)} credit"
            ticket_text += f"\nConservative: {format_dollars(credit * 0.8)} credit"
        elif debit > 0:
            ticket_text += f"\nTarget (mid): {format_dollars(debit)} debit"
        
        max_loss = structure.get('max_loss_dollars', 0)
        contracts = sizing.get('recommended_contracts', 0)
        total_risk = sizing.get('total_risk_dollars', 0)
        
        ticket_text += f"\n\nMax Loss per contract: {format_dollars(max_loss)}"
        ticket_text += f"\nRecommended contracts: {contracts}"
        ticket_text += f"\nTotal Risk: {format_dollars(total_risk)}"
        
        st.code(ticket_text, language=None)
    else:
        st.warning("⚠️ No legs defined for this structure")
    
    # Expandable sections
    col1, col2 = st.columns(2)
    with col1:
        with st.expander("💡 WHY THIS TRADE", expanded=False):
            st.markdown(f"**Edge Rationale:** {edge.get('rationale', 'N/A')}")
            st.markdown(f"**Candidate Rationale:** {candidate.get('rationale', 'N/A')}")
            st.markdown("---")
            st.markdown("**⚠️ What Could Go Wrong:**")
            st.markdown("""
            - Underlying moves sharply against position
            - IV crush reduces premium faster than theta
            - Early assignment risk on short legs
            - Fill worse than target price
            """)
    
    with col2:
        with st.expander("📊 PROBABILITY CONTEXT", expanded=False):
            st.warning("⚠️ Model-based context, not prediction.")
            st.info("Probability metrics will be displayed here when available.")


def render_pass_card(candidate: dict):
    """Render a PASS card with diagnostics."""
    symbol = candidate['symbol']
    edge = candidate.get('edge') or {}
    diagnostics = candidate.get('pass_diagnostics', [])
    reason = candidate.get('rationale', 'No valid structure found')
    
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, rgba(239, 68, 68, 0.03) 100%);
                border: 1px solid rgba(239, 68, 68, 0.2);
                border-left: 4px solid #ef4444;
                border-radius: 16px;
                padding: 24px;
                margin: 16px 0;">
        <h3 style="color: #ef4444; margin: 0 0 8px 0;">🔴 PASS — {symbol}</h3>
        <p style="color: #f87171; margin: 0; font-size: 14px; font-weight: 500;">
            {reason}
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    if diagnostics:
        with st.expander("🔍 DIAGNOSTICS (Structure Attempts)", expanded=False):
            for i, d in enumerate(diagnostics):
                cols = st.columns(4)
                with cols[0]:
                    st.metric("Structure", d.get('structure_type', 'N/A'))
                with cols[1]:
                    st.metric("Width", f"{d.get('width_points', 'N/A')} pts")
                with cols[2]:
                    st.metric("DTE", d.get('expiration_dte', 'N/A'))
                with cols[3]:
                    st.metric("Failure", d.get('failure_reason', 'N/A'))
                
                st.caption(f"Min OI: {d.get('min_oi_found', 'N/A')} | Credit: {format_dollars(d.get('conservative_credit', 0))} | Max Loss: {format_dollars(d.get('max_loss_dollars', 0))}")
                if i < len(diagnostics) - 1:
                    st.divider()


def main():
    # ═══════════════════════════════════════════════════════════
    # HEADER
    # ═══════════════════════════════════════════════════════════
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown("# ◉ VOLMACHINE DESK")
    with col2:
        pass
    with col3:
        if st.button("🔄 Run Desk Now", type="primary", use_container_width=True):
            with st.spinner("Running engine..."):
                result = run_engine()
                if result.returncode == 0:
                    st.success("✅ Run complete!")
                    st.rerun()
                else:
                    st.error(f"❌ Run failed: {result.stderr[:200]}")
    
    # Load data
    report = load_latest_report()
    
    if not report:
        st.markdown("---")
        st.warning("📭 No report data available. Click **Run Desk Now** to generate a report.")
        st.info("Make sure POLYGON_API_KEY environment variable is set before running.")
        return
    
    # ═══════════════════════════════════════════════════════════
    # A) TOP BANNER - WHAT TO DO TODAY
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### 📋 What To Do Today")
        trading_allowed = report.get('trading_allowed', True)
        if trading_allowed:
            st.markdown('<div class="trading-allowed">🟢 TRADING ALLOWED</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="trading-blocked">🔴 TRADING BLOCKED</div>', unsafe_allow_html=True)
            for reason in report.get('do_not_trade_reasons', []):
                st.error(f"❌ {reason}")
    
    with col2:
        st.caption("Report Info")
        st.markdown(f"**Date:** {report.get('report_date')}")
        gen_time = report.get('generated_at', '')[:19].replace('T', ' ')
        st.markdown(f"**Generated:** {gen_time}")
    
    # ═══════════════════════════════════════════════════════════
    # B) MARKET REGIME
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 📊 Market Regime")
    
    regime = report.get('regime', {})
    regime_state = regime.get('state', 'unknown').upper()
    regime_confidence = regime.get('confidence', 0)
    regime_rationale = regime.get('rationale', 'No rationale available')
    
    # Color mapping
    color_map = {
        'BULL': ('🟢', '#10b981', 'regime-bull'),
        'BEAR': ('🔴', '#ef4444', 'regime-bear'),
        'CHOP': ('🟠', '#f59e0b', 'regime-chop'),
        'RECOVERY': ('🔵', '#3b82f6', 'regime-recovery'),
    }
    emoji, color, css_class = color_map.get(regime_state, ('⚪', '#8b95a5', ''))
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(f"""
        <div class="regime-box">
            <div class="regime-state {css_class}" style="color: {color};">{emoji} {regime_state}</div>
            <div style="font-size: 14px; color: #8b95a5; margin-top: 12px;">{regime_rationale}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("##### Confidence")
        st.metric("", f"{regime_confidence:.0%}")
        st.progress(regime_confidence)
    
    # ═══════════════════════════════════════════════════════════
    # C) TRADE DECISIONS
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 💼 Trade Decisions")
    
    candidates = report.get('candidates', [])
    trade_candidates = [c for c in candidates if c.get('recommendation') == 'TRADE']
    pass_candidates = [c for c in candidates if c.get('recommendation') in ['PASS', 'REVIEW']]
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Candidates", len(candidates))
    with col2:
        st.metric("TRADE", len(trade_candidates), delta="actionable" if trade_candidates else None)
    with col3:
        st.metric("PASS/REVIEW", len(pass_candidates))
    
    st.markdown("")
    
    # TRADE section
    if trade_candidates:
        st.markdown("#### 🟢 TRADE TODAY")
        for candidate in trade_candidates:
            render_trade_card(candidate)
    else:
        st.info("📭 **No TRADE candidates today.** All detected edges failed structure validation. See PASS cards below for details on what didn't work.")
    
    # PASS section
    if pass_candidates:
        st.markdown("#### 🔴 NO TRADE (PASS / REVIEW)")
        for candidate in pass_candidates:
            render_pass_card(candidate)
    
    # ═══════════════════════════════════════════════════════════
    # D) EDGES TABLE
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 📈 Edges Detected")
    
    edges = report.get('edges', [])
    if edges:
        edge_data = []
        for e in edges:
            edge_data.append({
                '🏷️ Symbol': e.get('symbol', 'N/A'),
                '📊 Type': e.get('type', 'N/A'),
                '💪 Strength': f"{e.get('strength', 0):.0%}",
                '📍 Direction': e.get('direction', 'N/A'),
            })
        st.table(edge_data)
        
        with st.expander("📝 Edge Details"):
            for e in edges:
                st.markdown(f"**{e.get('symbol')} - {e.get('type')}:** {e.get('rationale', 'No rationale')}")
    else:
        st.info("📭 **No edges detected today.** The volatility metrics did not exceed thresholds for any tradeable edge.")
    
    # ═══════════════════════════════════════════════════════════
    # E) PORTFOLIO & RISK
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 💰 Portfolio & Risk")
    
    portfolio = report.get('portfolio', {})
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Positions Open", portfolio.get('positions_open', 0))
    with col2:
        st.metric("Max Loss Exposure", format_dollars(portfolio.get('total_max_loss_dollars', 0)))
    with col3:
        pnl_today = portfolio.get('realized_pnl_today_dollars', 0)
        delta_color = "normal" if pnl_today >= 0 else "inverse"
        st.metric("Realized P&L Today", format_dollars(pnl_today))
    with col4:
        unrealized = portfolio.get('unrealized_pnl_dollars', 0)
        st.metric("Unrealized P&L", format_dollars(unrealized))
    
    if portfolio.get('kill_switch_active'):
        st.error(f"🚨 **KILL SWITCH ACTIVE:** {portfolio.get('kill_switch_reason', 'Unknown reason')}")
    
    # ═══════════════════════════════════════════════════════════
    # FOOTER
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.caption("📁 Report Files")
    reports_dir = Path(__file__).parent.parent / 'logs' / 'reports'
    report_date = report.get('report_date', 'latest')
    
    col1, col2, col3 = st.columns(3)
    md_path = reports_dir / f'{report_date}.md'
    html_path = reports_dir / f'{report_date}.html'
    json_path = reports_dir / f'{report_date}.json'
    
    with col1:
        if md_path.exists():
            st.markdown(f"📄 [Markdown Report]({md_path})")
    with col2:
        if html_path.exists():
            st.markdown(f"🌐 [HTML Report]({html_path})")
    with col3:
        if json_path.exists():
            st.markdown(f"📊 [JSON Data]({json_path})")


if __name__ == "__main__":
    main()
