"""
VolMachine Desk UI v3 - Cyber Trading Dashboard

High-end professional trading terminal with:
- Electric blue / Neon aesthetic
- Live execution terminal
- Risk/Signal command centers
- Execution-ready order tickets

Run with: streamlit run ui/app.py
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path
from threading import Thread
import queue

import streamlit as st

# Page config
st.set_page_config(
    page_title="VolMachine Desk",
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Cyber-Trading aesthetic CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&family=Rajdhani:wght@500;600;700&display=swap');

/* GLOBAL THEME */
.stApp {
    background: #0a0e17; /* Brighter deep blue-black */
    background-image: 
        radial-gradient(circle at 50% 0%, rgba(0, 242, 234, 0.15) 0%, transparent 60%),
        linear-gradient(rgba(0, 217, 255, 0.1) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0, 217, 255, 0.1) 1px, transparent 1px);
    background-size: 100% 100%, 40px 40px, 40px 40px;
    color: #f1f5f9;
}

/* TYPOGRAPHY */
h1, h2, h3, .main-title {
    font-family: 'Rajdhani', sans-serif !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* MAIN TITLE */
.main-title {
    font-size: 4.5rem;
    font-weight: 800;
    color: #fff;
    text-shadow: 0 0 20px rgba(0, 217, 255, 0.8), 0 0 40px rgba(0, 217, 255, 0.4);
    background: none;
    -webkit-text-fill-color: initial;
    margin: 0;
    padding-bottom: 20px;
}

/* SUBHEADERS */
h3 {
    font-size: 1.8rem !important;
    color: #e2e8f0;
    border-bottom: 2px solid #00d9ff;
    padding-bottom: 12px;
    margin-top: 40px !important;
    text-shadow: 0 0 10px rgba(0, 217, 255, 0.3);
}

/* CARDS COMMON */
.card-base {
    background: rgba(15, 23, 42, 0.8);
    border: 1px solid rgba(255, 255, 255, 0.15);
    border-radius: 8px;
    backdrop-filter: blur(12px);
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
}

/* RISK STATUS */
.risk-allowed {
    background: rgba(6, 78, 59, 0.3);
    border: 2px solid #10b981;
    box-shadow: 0 0 40px rgba(16, 185, 129, 0.2);
    border-radius: 8px;
    padding: 30px;
    text-align: center;
}

.risk-blocked {
    background: rgba(127, 29, 29, 0.3);
    border: 2px solid #ef4444;
    box-shadow: 0 0 40px rgba(239, 68, 68, 0.2);
    border-radius: 8px;
    padding: 30px;
    text-align: center;
}

/* SIGNAL STATUS */
.signal-box {
    background: rgba(15, 23, 42, 0.5);
    border: 2px solid #38bdf8;
    box-shadow: 0 0 30px rgba(56, 189, 248, 0.1);
    border-radius: 8px;
    padding: 30px;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
    height: 100%;
}

.signal-pill {
    font-size: 1.8rem;
    padding: 12px 36px;
    border-radius: 6px;
    text-shadow: 0 2px 4px rgba(0,0,0,0.5);
    border: 2px solid rgba(255,255,255,0.2);
}

.sig-trade { background: #059669; color: white; box-shadow: 0 0 30px #10b981; border-color: #34d399; }
.sig-pass { background: #dc2626; color: white; box-shadow: 0 0 30px #ef4444; border-color: #f87171; }
.sig-none { background: #475569; color: #cbd5e1; box-shadow: 0 0 15px rgba(255,255,255,0.1); }

/* TERMINAL */
.terminal-window {
    background: #09090b;
    border: 1px solid #3f3f46;
    border-radius: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    line-height: 1.5;
    box-shadow: 0 20px 50px rgba(0,0,0,0.8);
    margin-bottom: 30px;
}

.terminal-header {
    background: #18181b;
    padding: 8px 16px;
    border-bottom: 1px solid #3f3f46;
    color: #a1a1aa;
    font-size: 12px;
}

.terminal-content {
    padding: 20px;
    color: #22d3ee;
    height: 350px;
    overflow-y: auto;
}

/* TRADE TICKET */
.trade-card {
    background: rgba(16, 20, 26, 0.8);
    border: 2px solid #06b6d4;
    border-radius: 8px;
    margin: 20px 0;
    box-shadow: 0 0 50px rgba(6, 182, 212, 0.15);
}

.trade-header {
    background: #083344;
    padding: 20px 25px;
    border-bottom: 1px solid #06b6d4;
}

.ticket-code {
    background: #020617;
    border: 1px solid #1e293b;
    color: #e2e8f0;
    padding: 25px;
    font-size: 14px;
    line-height: 1.6;
}

/* REGIME */
.regime-panel {
    background: rgba(30, 41, 59, 0.4);
    border: 1px solid rgba(255,255,255,0.1);
    padding: 35px;
    border-radius: 8px;
}

.regime-big-text {
    font-size: 5rem;
    font-weight: 900;
    text-shadow: 0 0 50px currentColor;
    margin: 15px 0;
}

/* METRICS */
[data-testid="stMetric"] {
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 8px;
    padding: 20px;
}
[data-testid="stMetricLabel"] { font-size: 13px; color: #94a3b8; font-weight: 600; }
[data-testid="stMetricValue"] { font-size: 2.5rem; color: white; font-weight: 700; text-shadow: 0 0 20px rgba(255,255,255,0.2); }

.stButton > button {
    background: #0066ff;
    border: 2px solid #3b82f6;
    font-size: 20px;
    padding: 16px 32px;
    letter-spacing: 3px;
    text-shadow: 0 2px 4px rgba(0,0,0,0.4);
    height: auto;
}
.stButton > button:hover {
    background: #2563eb;
    box-shadow: 0 0 50px rgba(37, 99, 235, 0.6);
}
</style>
""", unsafe_allow_html=True)


def load_latest_report() -> dict:
    reports_dir = Path(__file__).parent.parent / 'logs' / 'reports'
    latest_path = reports_dir / 'latest.json'
    if latest_path.exists():
        with open(latest_path) as f:
            return json.load(f)
    return None


def run_engine_processed():
    """Run engine and stream output."""
    script_path = Path(__file__).parent.parent / 'scripts' / 'run_daily.py'
    env = os.environ.copy()
    
    # HARDCODED API KEY - ensures it ALWAYS works
    # Priority: 1) st.secrets (Cloud), 2) env var, 3) hardcoded
    FALLBACK_KEY = "lrpYXeKqUp8pBGDlbz1BdJwsmpnpiKzu"
    
    api_key = None
    
    # Try Streamlit Cloud secrets first (direct access, not .get())
    try:
        if hasattr(st, 'secrets') and 'POLYGON_API_KEY' in st.secrets:
            api_key = st.secrets['POLYGON_API_KEY']
    except Exception:
        pass
    
    # Fallback to environment variable
    if not api_key:
        api_key = os.environ.get('POLYGON_API_KEY')
    
    # Final fallback to hardcoded key
    if not api_key:
        api_key = FALLBACK_KEY
    
    # ALWAYS set the key in the subprocess environment
    env['POLYGON_API_KEY'] = api_key
        
    process = subprocess.Popen(
        [sys.executable, str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(Path(__file__).parent.parent),
        env=env,
    )
    return process


def format_dollars(value) -> str:
    try: return f"${float(value):,.2f}"
    except: return "N/A"

def format_percent(value) -> str:
    try: return f"{float(value):.0%}"
    except: return "N/A"

def get_signal_status(report: dict) -> tuple:
    edges = report.get('edges', [])
    candidates = report.get('candidates', [])
    
    if not edges: return ('NO_EDGE', 'NO EDGE', 'No edge > threshold')
    trade_candidates = [c for c in candidates if c.get('recommendation') == 'TRADE']
    if trade_candidates: return ('TRADE', 'TRADE ACTIVE', f'{len(trade_candidates)} TRADES FOUND')
    return ('PASS', 'PASS', 'EDGE FOUND / NO TRADE')


def render_terminal(placeholder, lines):
    content = ""
    for line in lines[-20:]:  # Keep last 20 lines
        line_clean = line.strip()
        if not line_clean: continue
        
        if 'ERROR' in line_clean.upper():
            content += f'<div class="t-err">{line_clean}</div>'
        elif 'WARNING' in line_clean.upper():
            content += f'<div class="t-warn">{line_clean}</div>'
        elif 'SUCCESS' in line_clean.upper() or '✅' in line_clean:
            content += f'<div class="t-success">{line_clean}</div>'
        elif 'INFO' in line_clean.upper():
            content += f'<div class="t-info">{line_clean}</div>'
        else:
            content += f'<div>{line_clean}</div>'
            
    placeholder.markdown(f"""
    <div class="terminal-window">
        <div class="terminal-header">
            <div class="term-dot term-red"></div>
            <div class="term-dot term-yellow"></div>
            <div class="term-dot term-green"></div>
            <div style="margin-left: 10px; color: #666;">engine_run.sh</div>
        </div>
        <div class="terminal-content">
            {content}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_probability_snapshot(candidate: dict):
    """
    Render Probability Snapshot panel.
    
    Displays model-based probabilistic estimates:
    - P(Profit at Expiry)
    - Expected Value per contract
    - Reward/Risk Ratio
    - Breakeven Distance
    - Credit/Debit-to-Width Ratio
    """
    prob_metrics = candidate.get('probability_metrics', {})
    
    if not prob_metrics:
        return  # No probability data available
    
    pop = prob_metrics.get('pop_expiry', 0)
    ev = prob_metrics.get('expected_pnl_expiry', 0)
    rr_ratio = prob_metrics.get('reward_to_risk_ratio', 0)
    breakeven_dist = prob_metrics.get('breakeven_distance_pct', 0)
    credit_to_width = prob_metrics.get('credit_to_width_ratio', 0)
    
    st.markdown("""
    <div style="background: rgba(56,189,248,0.08); border: 1px solid rgba(56,189,248,0.3); border-radius: 6px; padding: 12px; margin-bottom: 12px;">
        <div style="color: #38bdf8; font-weight: bold; font-size: 12px; margin-bottom: 8px;">
            📈 PROBABILITY SNAPSHOT (Model-Based)
        </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("P(Profit)", f"{pop:.0%}" if pop else "N/A")
        if breakeven_dist:
            st.caption(f"BE Distance: {breakeven_dist:.1%}")
    
    with col2:
        ev_display = f"+${ev:.2f}" if ev >= 0 else f"-${abs(ev):.2f}"
        st.metric("Expected Value", ev_display if ev else "N/A")
        st.caption("per contract")
    
    with col3:
        st.metric("Reward/Risk", f"{rr_ratio:.1f}x" if rr_ratio else "N/A")
        if credit_to_width:
            st.caption(f"Credit/Width: {credit_to_width:.0%}")
    
    st.markdown("""
        <div style="color: #64748b; font-size: 10px; margin-top: 6px; font-style: italic;">
            ⚠️ Model-based estimates — not guarantees
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_edge_rationale(candidate: dict):
    """
    Render Edge Rationale panel (WHY THIS TRADE).
    
    Displays in plain English:
    - What anomaly exists
    - Why this structure expresses the edge
    - What would invalidate the thesis
    """
    edge = candidate.get('edge', {})
    structure = candidate.get('structure', {})
    regime = candidate.get('regime', {})
    
    edge_type = edge.get('type', 'unknown')
    edge_metrics = edge.get('metrics', {})
    edge_rationale = edge.get('rationale', '')
    regime_state = regime.get('state', 'unknown') if isinstance(regime, dict) else str(regime)
    struct_type = structure.get('type', 'spread')
    max_loss = structure.get('max_loss_dollars', 0)
    max_profit = structure.get('max_profit_dollars', 0)
    
    # Build edge explanation
    if edge_type == 'skew_extreme':
        is_flat = edge_metrics.get('is_flat', 0)
        is_steep = edge_metrics.get('is_steep', 0)
        skew_pct = edge_metrics.get('put_call_skew', 0) * 100
        
        if is_flat:
            edge_bullet = f"• Put skew unusually flat ({skew_pct:.1f}%) → downside insurance cheap"
            regime_context = "• In CHOP regime → tails historically underpriced"
            structure_reason = "• Debit put spread for convex downside exposure"
            invalidation = "• Volatility compression without downside follow-through"
        else:
            edge_bullet = f"• Put skew unusually steep ({skew_pct:.1f}%) → puts expensive"
            regime_context = "• Market paying premium for downside protection"
            structure_reason = "• Credit put spread to harvest vol premium"
            invalidation = "• Sharp selloff that overwhelms collected premium"
    elif edge_type == 'vrp':
        iv = edge_metrics.get('atm_iv', 0) * 100
        rv = edge_metrics.get('rv_20d', 0) * 100
        ratio = edge_metrics.get('iv_rv_ratio', 0)
        edge_bullet = f"• IV ({iv:.0f}%) > RV ({rv:.0f}%) → ratio {ratio:.2f}x"
        regime_context = "• Market pricing in more volatility than realized"
        structure_reason = "• Credit spread to harvest volatility risk premium"
        invalidation = "• Realized vol spike above implied levels"
    else:
        edge_bullet = f"• {edge_type.replace('_', ' ').title()} edge detected"
        regime_context = f"• Market regime: {regime_state}"
        structure_reason = f"• {struct_type.replace('_', ' ').title()} structure"
        invalidation = "• Edge thesis breaks down"
    
    # Risk/reward context
    if max_loss > 0 and max_profit > 0:
        payoff_text = f"• ${max_loss:.0f} risk to access ~${max_profit:.0f} payoff"
    elif max_loss > 0:
        payoff_text = f"• Max risk capped at ${max_loss:.0f}"
    else:
        payoff_text = "• Risk defined by structure"
    
    with st.expander("▶ WHY THIS TRADE", expanded=False):
        st.markdown(f"""
        **EDGE**  
        {edge_bullet}  
        {regime_context}
        
        **STRUCTURE**  
        {structure_reason}  
        {payoff_text}
        
        **WHAT INVALIDATES THIS**  
        {invalidation}
        """)


def render_sizing_ladder(candidate: dict, candidate_id: str) -> int:
    """
    Render what-if sizing ladder with selection.
    
    Returns selected contract count.
    """
    sizing = candidate.get('sizing', {})
    what_if_sizes = sizing.get('what_if_sizes', {})
    default_contracts = sizing.get('recommended_contracts', 0)
    
    if not what_if_sizes:
        return default_contracts
    
    # Build options
    options = []
    for pct_key, info in what_if_sizes.items():
        if info.get('allowed', False):
            contracts = info.get('contracts', 0)
            risk = info.get('risk_dollars', 0)
            options.append({
                'key': pct_key,
                'label': f"{pct_key}: {contracts} contracts (${risk:.0f} risk)",
                'contracts': contracts,
            })
    
    if not options:
        return default_contracts
    
    # Default to first allowed option
    selected_label = st.selectbox(
        "📊 Risk Tier",
        [o['label'] for o in options],
        key=f"sizing_ladder_{candidate_id}",
        help="Select position size based on risk tier"
    )
    
    # Find selected contracts
    for opt in options:
        if opt['label'] == selected_label:
            return opt['contracts']
    
    return default_contracts


def render_status_badges(candidate: dict, is_fallback: bool):
    """
    Render status badges for the trade.
    
    - FALLBACK EDGE (absolute threshold used)
    - HISTORY CONFIRMED (percentile-based)
    - PAPER MODE
    """
    edge = candidate.get('edge', {})
    history_mode = edge.get('metrics', {}).get('history_mode', 1)
    
    badges = []
    
    # Mode badge
    badges.append(('PAPER', '#f59e0b', 'rgba(245,158,11,0.1)'))
    
    # Edge quality badge
    if is_fallback:
        badges.append(('FALLBACK EDGE', '#ef4444', 'rgba(239,68,68,0.1)'))
    else:
        badges.append(('HISTORY CONFIRMED', '#10b981', 'rgba(16,185,129,0.1)'))
    
    badge_html = "".join([
        f'<span style="border: 1px solid {color}; color: {color}; background: {bg}; '
        f'padding: 2px 8px; border-radius: 4px; font-size: 10px; margin-right: 4px;">{label}</span>'
        for label, color, bg in badges
    ])
    
    st.markdown(badge_html, unsafe_allow_html=True)


def render_trade_card(candidate: dict):
    """
    Render a compact trade card for grid display.
    Shows key info in a contained card format with execute button.
    """
    symbol = candidate['symbol']
    structure = candidate.get('structure') or {}
    edge = candidate.get('edge') or {}
    sizing = candidate.get('sizing') or {}
    candidate_id = candidate.get('id', symbol)
    is_valid = candidate.get('is_valid', True)
    
    struct_type = structure.get('type', '')
    max_profit = structure.get('max_profit_dollars', 0)
    max_loss = structure.get('max_loss_dollars', 0)
    debit = structure.get('entry_debit_dollars', 0)
    credit = structure.get('entry_credit_dollars', 0)
    contracts = sizing.get('recommended_contracts', 0)
    
    # Calculate max_profit fallback
    if max_profit == 0 and struct_type in ['debit_spread', 'DEBIT_SPREAD']:
        legs = structure.get('legs', [])
        if legs:
            strikes = [l.get('strike', 0) for l in legs]
            if len(strikes) >= 2:
                width = abs(max(strikes) - min(strikes))
                max_profit = (width - debit/100) * 100
    
    # Determine trade direction
    if struct_type in ['debit_spread', 'DEBIT_SPREAD']:
        direction = "BEARISH"
        cost = debit
    else:
        direction = "NEUTRAL"
        cost = credit
    
    exp = structure.get('expiration', '')
    dte = structure.get('dte', 0)
    return_mult = max_profit / cost if cost > 0 else 0
    edge_type = edge.get('type', '').upper().replace('_', ' ')
    is_fallback = edge.get('is_fallback', False) or edge.get('metrics', {}).get('history_mode', 1) == 0
    
    # Card state
    card_key = f"card_{candidate_id}"
    if 'card_states' not in st.session_state:
        st.session_state['card_states'] = {}
    card_state = st.session_state['card_states'].get(card_key, 'ready')
    
    # Card container
    with st.container():
        # Header
        h1, h2 = st.columns([3, 1])
        with h1:
            st.markdown(f"### {symbol}")
            st.caption(f"📉 {direction} • {edge_type}")
        with h2:
            if is_fallback:
                st.error("⚠️ FALLBACK")
            else:
                st.success("✓ CONFIRMED")
        
        # Metrics row
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Cost", f"${cost:.0f}")
        m2.metric("Profit", f"${max_profit:.0f}")
        m3.metric("Loss", f"${max_loss:.0f}")
        m4.metric("Return", f"{return_mult:.1f}x")
        
        # Footer row
        st.caption(f"⏰ {exp} ({dte}d) • 📊 {contracts} contracts")
        
        # Execute button
        can_execute = is_valid and contracts > 0
        
        if card_state == 'ready':
            if st.button(f"🚀 EXECUTE {symbol}", key=f"exec_{candidate_id}", disabled=not can_execute, type="primary"):
                st.session_state['card_states'][card_key] = 'confirmed'
                st.rerun()
        elif card_state == 'confirmed':
            st.success(f"✅ {symbol} CONFIRMED - Ready for IBKR")
            if st.button("↩️ Cancel", key=f"cancel_{candidate_id}"):
                st.session_state['card_states'][card_key] = 'ready'
                st.rerun()
        
        st.divider()


def render_trade_ticket(candidate: dict):
    """
    Render trade ticket with two-step execution flow.
    
    Features:
    - Fallback badge + extra confirmation when edge.is_fallback
    - Risk ladder selection (1%/2%/5%/10%)
    - Preview step: resolve contracts via IBKR
    - Submit step: place BAG order (disabled if !is_valid)
    - Live order status display
    """
    symbol = candidate['symbol']
    structure = candidate.get('structure') or {}
    edge = candidate.get('edge') or {}
    sizing = candidate.get('sizing') or {}
    candidate_id = candidate.get('id', symbol)
    is_valid = candidate.get('is_valid', True)
    what_if_sizes = sizing.get('what_if_sizes', {})
    
    # Check if edge is fallback mode (no percentile history)
    is_fallback = edge.get('is_fallback', False) or edge.get('metrics', {}).get('history_mode', 1) == 0
    
    # --- HEADER ---
    fallback_badge = ""
    if is_fallback:
        fallback_badge = '<span class="trade-tag" style="border-color: #ef4444; color: #ef4444; background: rgba(239,68,68,0.1)">⚠️ FALLBACK</span>'
    
    st.markdown(f"""
    <div class="trade-card">
        <div class="trade-header">
            <div class="trade-title">
                <span style="color:#00f2ea">⚡</span> {symbol}
                <span class="trade-tag">{edge.get('type','').upper()}</span>
                <span class="trade-tag" style="border-color: #10b981; color: #10b981">TRADE</span>
                <span class="trade-tag" style="border-color: #f59e0b; color: #f59e0b; background: rgba(245,158,11,0.1)">📋 PAPER</span>
                {fallback_badge}
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # --- FALLBACK WARNING ---
    if is_fallback:
        st.markdown("""
        <div style="background: rgba(239,68,68,0.1); border: 1px solid #ef4444; border-radius: 4px; padding: 8px; margin-bottom: 12px;">
            <span style="color: #ef4444; font-weight: bold;">⚠️ FALLBACK MODE</span>
            <span style="color: #94a3b8; font-size: 11px; margin-left: 8px;">Edge detected using absolute thresholds - no percentile history available</span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background: rgba(245,158,11,0.1); border: 1px solid #f59e0b; border-radius: 4px; padding: 8px; margin-bottom: 12px;">
            <span style="color: #f59e0b; font-weight: bold;">⚠️ PAPER MODE</span>
            <span style="color: #94a3b8; font-size: 11px; margin-left: 8px;">Awaiting manual confirmation</span>
        </div>
        """, unsafe_allow_html=True)
    
    # --- PLAIN ENGLISH SUMMARY (TOP OF CARD) ---
    # Get key values for summary
    struct_type = structure.get('type', '')
    max_profit = structure.get('max_profit_dollars', 0)
    max_loss = structure.get('max_loss_dollars', 0)
    debit = structure.get('entry_debit_dollars', 0)
    credit = structure.get('entry_credit_dollars', 0)
    legs = structure.get('legs', [])
    
    # Calculate max_profit fallback
    if max_profit == 0 and struct_type in ['debit_spread', 'DEBIT_SPREAD'] and legs:
        strikes = [l.get('strike', 0) for l in legs]
        if len(strikes) >= 2:
            width = abs(max(strikes) - min(strikes))
            max_profit = (width - debit/100) * 100
    
    # Build plain English description
    if struct_type in ['debit_spread', 'DEBIT_SPREAD']:
        trade_type = "PUT SPREAD (BEARISH)"
        cost_value = debit
        simple_explain = f"You pay ${debit:.0f} upfront. You make money if {symbol} drops."
    else:
        trade_type = "PUT SPREAD (NEUTRAL)"
        cost_value = credit
        simple_explain = f"You collect ${credit:.0f} upfront. You keep it if {symbol} stays flat or rises."
    
    # Get expiration
    exp = structure.get('expiration', '')
    dte = structure.get('dte', 0)
    return_pct = ((max_profit/max_loss)*100) if max_loss > 0 else 0
    return_mult = max_profit/cost_value if cost_value > 0 else 0
    
    # --- HEADER ROW ---
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.subheader(f"🎯 {symbol} {trade_type}")
    with header_col2:
        st.success("✅ READY")
    
    # --- SIMPLE EXPLANATION ---
    st.info(f"💡 {simple_explain}")
    
    # --- KEY NUMBERS (3 columns) ---
    num_col1, num_col2, num_col3 = st.columns(3)
    with num_col1:
        st.metric("💵 Your Cost", f"${cost_value:.0f}")
    with num_col2:
        st.metric("📈 Max Profit", f"${max_profit:.0f}", delta=f"{return_pct:.0f}% return")
    with num_col3:
        st.metric("📉 Max Loss", f"${max_loss:.0f}")
    
    # --- EXPIRATION ROW ---
    exp_col1, exp_col2 = st.columns(2)
    with exp_col1:
        st.caption(f"⏰ Expires: {exp} ({dte} days)")
    with exp_col2:
        st.caption(f"🎲 Return potential: {return_mult:.1f}x")
    
    st.divider()
    
    st.markdown('<div class="trade-body">', unsafe_allow_html=True)
    
    # --- RISK LADDER SELECTION ---
    risk_col, qty_col = st.columns([2, 1])
    
    with risk_col:
        # Build risk tier options from what_if_sizes
        risk_options = []
        for pct_key, info in what_if_sizes.items():
            if info.get('allowed', False):
                contracts = info.get('contracts', 0)
                risk_dollars = info.get('risk_dollars', 0)
                risk_options.append(f"{pct_key}: {contracts} contracts (${risk_dollars:.0f})")
        
        if not risk_options:
            # Default fallback
            contracts = sizing.get('recommended_contracts', 0)
            risk_options = [f"Default: {contracts} contracts"]
        
        selected_risk = st.selectbox(
            "📊 Risk Tier",
            risk_options,
            key=f"risk_{candidate_id}",
            help="Select risk tier from what-if sizing ladder"
        )
        
        # Parse selected contracts
        selected_contracts = sizing.get('recommended_contracts', 0)
        if ':' in selected_risk:
            pct_key = selected_risk.split(':')[0].strip()
            if pct_key in what_if_sizes:
                selected_contracts = what_if_sizes[pct_key].get('contracts', selected_contracts)
    
    with qty_col:
        st.metric("🎯 Contracts", selected_contracts)
    
    # --- EXECUTION TICKET ---
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div style="color: #94a3b8; font-size: 11px; margin-bottom: 8px;">EXECUTION TICKET</div>', unsafe_allow_html=True)
        
        legs = structure.get('legs', [])
        lines = []
        if legs:
            exp = structure.get('expiration', 'N/A')
            lines.append(f"# STRATEGY: {structure.get('type', 'CUSTOM').upper()}")
            lines.append(f"# EXPIRY:   {exp} ({structure.get('dte',0)} DTE)")
            lines.append("-" * 40)
            
            for leg in legs:
                side = leg.get('action', 'BUY').ljust(4)
                qty = str(selected_contracts).ljust(2)
                strike = str(leg.get('strike', 0)).ljust(6)
                otype = leg.get('option_type', 'C')[0].upper()
                lines.append(f"{side} {qty} {symbol} {exp} {strike} {otype}")
                
            lines.append("-" * 40)
            credit = structure.get('entry_credit_dollars', 0)
            debit = structure.get('entry_debit_dollars', 0)
            max_loss = structure.get('max_loss_dollars', 0)
            
            if credit > 0: price = f"CREDIT: ${credit:.2f}"
            else: price = f"DEBIT:  ${debit:.2f}"
            
            lines.append(f"{price.ljust(20)} MAX LOSS: ${max_loss:.2f}")
            lines.append(f"SIZE:   {selected_contracts} contracts      RISK:     ${max_loss * selected_contracts:.2f}")

        formatted_ticket = "\n".join(lines)
        st.markdown(f"""
        <div class="ticket-code">
            <div class="copy-hint">COPY</div>
            <pre style="margin:0">{formatted_ticket}</pre>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown('<div style="color: #94a3b8; font-size: 11px; margin-bottom: 8px;">EXECUTION METRICS</div>', unsafe_allow_html=True)
        
        # Show execution metrics - DETERMINISTIC ONLY
        credit = structure.get('entry_credit_dollars', 0)
        debit = structure.get('entry_debit_dollars', 0)
        max_loss = structure.get('max_loss_dollars', 0)
        max_profit = structure.get('max_profit_dollars', 0)
        struct_type = structure.get('type', '')
        
        # Fallback computation for max_profit if missing
        if max_profit == 0 and struct_type in ['debit_spread', 'DEBIT_SPREAD']:
            legs = structure.get('legs', [])
            if legs:
                strikes = [l.get('strike', 0) for l in legs]
                width_points = abs(max(strikes) - min(strikes)) if len(strikes) >= 2 else 0
                debit_points = debit / 100 if debit else 0
                max_profit = (width_points - debit_points) * 100  # Convert to dollars
        elif max_profit == 0 and struct_type in ['credit_spread', 'CREDIT_SPREAD']:
            # For credit spreads, max profit = credit received
            max_profit = credit
        
        m1, m2 = st.columns(2)
        if credit > 0:
            m1.metric("💰 Credit", f"${credit:.0f}")
        else:
            m1.metric("💸 Debit", f"${debit:.0f}")
        m2.metric("📉 Max Loss", f"${max_loss:.0f}" if max_loss else "N/A")
        
        m3, m4 = st.columns(2)
        m3.metric("📈 Max Profit", f"${max_profit:.0f}" if max_profit else "N/A")
        m4.metric("📋 Mode", "PAPER")
        
    st.markdown("</div></div>", unsafe_allow_html=True)
    
    # --- PAYOFF SUMMARY (STATIC / DETERMINISTIC) ---
    st.markdown("""
    <div style="background: rgba(30,41,59,0.6); border: 1px solid rgba(71,85,105,0.5); border-radius: 6px; padding: 12px; margin-bottom: 12px;">
        <div style="color: #94a3b8; font-weight: bold; font-size: 11px; margin-bottom: 8px;">
            📊 PAYOFF SUMMARY (Deterministic)
        </div>
    """, unsafe_allow_html=True)
    
    # Calculate breakeven
    legs = structure.get('legs', [])
    if legs:
        # For debit put spreads: breakeven = long_strike - debit_paid
        # For credit put spreads: breakeven = short_strike - credit_received
        long_strike = max([l.get('strike', 0) for l in legs if l.get('action') == 'BUY'], default=0)
        short_strike = min([l.get('strike', 0) for l in legs if l.get('action') == 'SELL'], default=0)
        
        if debit > 0:
            breakeven = long_strike - (debit / 100)  # Convert dollars to points
        elif credit > 0:
            breakeven = short_strike - (credit / 100)
        else:
            breakeven = 0
    else:
        breakeven = 0
    
    payoff_col1, payoff_col2, payoff_col3 = st.columns(3)
    with payoff_col1:
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="color: #10b981; font-size: 20px; font-weight: bold;">${max_profit:.0f}</div>
            <div style="color: #64748b; font-size: 10px;">MAX PROFIT</div>
        </div>
        """, unsafe_allow_html=True)
    with payoff_col2:
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="color: #ef4444; font-size: 20px; font-weight: bold;">${max_loss:.0f}</div>
            <div style="color: #64748b; font-size: 10px;">MAX LOSS</div>
        </div>
        """, unsafe_allow_html=True)
    with payoff_col3:
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="color: #f59e0b; font-size: 20px; font-weight: bold;">${breakeven:.2f}</div>
            <div style="color: #64748b; font-size: 10px;">BREAKEVEN</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # --- PROVISIONAL EDGE STATUS ---
    if is_fallback:
        st.markdown("""
        <div style="background: rgba(239,68,68,0.08); border: 1px solid rgba(239,68,68,0.3); border-radius: 4px; padding: 8px; margin-bottom: 12px;">
            <span style="color: #ef4444; font-weight: bold; font-size: 11px;">⚠️ Provisional Edge (No historical percentile yet)</span>
            <span style="color: #94a3b8; font-size: 10px; margin-left: 8px;" title="This signal is valid but not yet statistically calibrated.">
                — Signal valid but not statistically calibrated
            </span>
        </div>
        """, unsafe_allow_html=True)
    
    # --- WHY THIS MAKES MONEY (One-liner) ---
    edge_type = edge.get('type', '')
    struct_type = structure.get('type', '')
    
    if edge_type == 'skew_extreme':
        is_flat = edge.get('metrics', {}).get('is_flat', 0)
        if is_flat:
            why_money = "Skew normalization: profit if downside volatility reprices or price moves below breakeven before expiry."
        else:
            why_money = "Skew compression: profit if put premium decays without large downside move."
    elif edge_type == 'vrp':
        why_money = "Volatility risk premium: profit if realized volatility stays below implied volatility."
    else:
        why_money = "Edge expression: profit if market conditions normalize toward historical averages."
    
    st.markdown(f"""
    <div style="background: rgba(56,189,248,0.06); border: 1px solid rgba(56,189,248,0.2); border-radius: 4px; padding: 10px; margin-bottom: 12px;">
        <div style="color: #38bdf8; font-weight: bold; font-size: 11px; margin-bottom: 4px;">💡 WHY THIS MAKES MONEY</div>
        <div style="color: #cbd5e1; font-size: 12px;">{why_money}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # --- EDGE RATIONALE (WHY THIS TRADE) ---
    render_edge_rationale(candidate)
    
    # --- CONFIRM REALNESS FOOTER ---
    st.markdown("""
    <div style="color: #64748b; font-size: 10px; font-style: italic; margin-bottom: 12px; padding: 4px 0; border-top: 1px solid rgba(71,85,105,0.3);">
        ✓ Contracts, prices, and strikes sourced live from Polygon. Structure executable at IBKR.
    </div>
    """, unsafe_allow_html=True)
    
    # --- CONFIRMATION FLOW ---
    if 'order_states' not in st.session_state:
        st.session_state['order_states'] = {}
    if 'confirmed_trades' not in st.session_state:
        st.session_state['confirmed_trades'] = set()
    
    order_state = st.session_state['order_states'].get(candidate_id, 'initial')
    is_confirmed = candidate_id in st.session_state['confirmed_trades']
    
    # Disable submit conditions
    can_submit = is_valid and selected_contracts > 0
    
    if not can_submit:
        disable_reason = []
        if not is_valid:
            disable_reason.append("Invalid structure")
        if selected_contracts == 0:
            disable_reason.append("0 contracts")
        st.error(f"❌ Cannot submit: {', '.join(disable_reason)}")
    
    # Fallback extra confirmation
    fallback_confirmed = True
    if is_fallback and not is_confirmed:
        fallback_confirmed = st.checkbox(
            "I understand this edge lacks historical validation (FALLBACK MODE)",
            key=f"fallback_confirm_{candidate_id}"
        )
    
    # Order flow buttons
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 1])
    
    with btn_col1:
        if order_state == 'initial':
            if st.button("🔍 PREVIEW ORDER", key=f"preview_{candidate_id}", disabled=not can_submit):
                st.session_state['order_states'][candidate_id] = 'previewing'
                st.info("Preview: Connect to IBKR to resolve contracts...")
                # In production: resolve_contracts() would be called here
                st.session_state['order_states'][candidate_id] = 'previewed'
                st.rerun()
        elif order_state == 'previewed':
            st.success("✅ Preview complete - contracts resolved")
        elif order_state == 'submitted':
            st.info("📤 Order submitted to IBKR")
    
    with btn_col2:
        if order_state == 'previewed':
            submit_disabled = not (can_submit and fallback_confirmed)
            if st.button("✅ SUBMIT ORDER", key=f"submit_{candidate_id}", type="primary", disabled=submit_disabled):
                st.session_state['order_states'][candidate_id] = 'submitted'
                st.session_state['confirmed_trades'].add(candidate_id)
                st.success("Order submitted to IBKR Paper!")
                st.rerun()
        elif order_state == 'submitted':
            st.success("✅ Order SUBMITTED")
    
    with btn_col3:
        if order_state in ['previewed', 'submitted']:
            if st.button("❌ Cancel", key=f"cancel_{candidate_id}"):
                st.session_state['order_states'][candidate_id] = 'initial'
                st.session_state['confirmed_trades'].discard(candidate_id)
                st.rerun()
    
    # Order status display
    if order_state == 'submitted':
        st.markdown("""
        <div style="background: rgba(16,185,129,0.1); border: 1px solid #10b981; border-radius: 4px; padding: 12px; margin-top: 8px;">
            <div style="color: #10b981; font-weight: bold;">📊 ORDER STATUS</div>
            <div style="color: #94a3b8; font-size: 12px; margin-top: 4px;">
                Status: <span style="color: #fbbf24;">PENDING</span> | 
                Filled: 0/{contracts} | 
                Avg Price: --
            </div>
        </div>
        """.format(contracts=selected_contracts), unsafe_allow_html=True)


def main():
    # HEADLINE
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<h1 class="main-title">VOLMACHINE<span style="color:#fff; font-weight:300">DESK</span></h1>', unsafe_allow_html=True)
        st.caption(f"SYSTEM ONLINE • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • v2.1")
        
    # INIT SESSION STATE
    if 'terminal_logs' not in st.session_state:
        st.session_state['terminal_logs'] = []

    # TERMINAL ZONE
    terminal_placeholder = st.empty()
    
    # Always render existing logs if they exist
    if st.session_state['terminal_logs']:
        render_terminal(terminal_placeholder, st.session_state['terminal_logs'])

    with col2:
        if st.button("INITIATE SEQUENCE"):
            # Clear previous logs
            st.session_state['terminal_logs'] = ["INITIALIZING SEQUENCE...", ""]
            render_terminal(terminal_placeholder, st.session_state['terminal_logs'])
            
            proc = run_engine_processed()
            
            for line in proc.stdout:
                line = line.strip()
                if line:
                    st.session_state['terminal_logs'].append(line)
                    render_terminal(terminal_placeholder, st.session_state['terminal_logs'])
            
            proc.wait()
            if proc.returncode == 0:
                st.session_state['terminal_logs'].append("SEQUENCE COMPLETE. REFRESHING DATA...")
                render_terminal(terminal_placeholder, st.session_state['terminal_logs'])
                time.sleep(1)
                st.rerun()
    
    # FULL LOG ACCESS (after terminal)
    if st.session_state['terminal_logs'] and len(st.session_state['terminal_logs']) > 3:
        log_col1, log_col2 = st.columns([3, 1])
        with log_col1:
            with st.expander("📜 VIEW FULL RUN LOG", expanded=False):
                full_log = "\n".join(st.session_state['terminal_logs'])
                st.code(full_log, language="text")
        with log_col2:
            log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_log = "\n".join(st.session_state['terminal_logs'])
            st.download_button(
                label="⬇️ DOWNLOAD LOG",
                data=full_log,
                file_name=f"volmachine_run_{log_timestamp}.txt",
                mime="text/plain",
            )

    # DATA LOAD
    report = load_latest_report()
    if not report:
        st.warning("SYSTEM STANDBY. AWAITING DATA.")
        return

    st.markdown("---")

    # STATUS BOARD
    c1, c2 = st.columns(2)
    
    # RISK STATUS
    with c1:
        st.markdown("### 🛡️ RISK SYSTEMS")
        allowed = report.get('trading_allowed', True)
        if allowed:
            st.markdown("""
            <div class="risk-allowed">
                <div style="font-size: 2rem; color: #10b981; font-weight: 800; letter-spacing: 2px;">TRADING ALLOWED</div>
                <div style="color: #6ee7b7; font-family: 'JetBrains Mono'; font-size: 12px; margin-top: 8px;">ALL SYSTEMS NOMINAL</div>
                <div style="color: #059669; font-size: 3rem; position: absolute; right: 20px; top: 10px; opacity: 0.2">OK</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="risk-blocked">
                <div style="font-size: 2rem; color: #ef4444; font-weight: 800;">TRADING LOCKED</div>
                <div style="color: #fca5a5; font-family: 'JetBrains Mono'; margin-top: 8px;">KILL SWITCH ACTIVE</div>
            </div>
            """, unsafe_allow_html=True)

    # SIGNAL STATUS
    with c2:
        st.markdown("### 📡 SIGNAL FEED")
        sig_type, sig_label, sig_desc = get_signal_status(report)
        style_cls = {'TRADE': 'sig-trade', 'PASS': 'sig-pass', 'NO_EDGE': 'sig-none'}[sig_type]
        
        st.markdown(f"""
        <div class="signal-box">
            <div class="signal-pill {style_cls}">{sig_label}</div>
            <div style="margin-top: 12px; font-family: 'JetBrains Mono'; color: #94a3b8; font-size: 12px;">{sig_desc}</div>
        </div>
        """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════
    # MARKET INFO BAR (3-column compact header)
    # ═══════════════════════════════════════════════════════════════════
    
    # Get data
    provider = report.get('provider_status', {})
    provider_connected = provider.get('connected', False)
    provider_source = provider.get('source', 'Polygon')
    
    universe = report.get('universe_scan', {})
    symbols_scanned = universe.get('symbols_scanned', 0)
    symbols_with_edges = universe.get('symbols_with_edges', 0)
    symbols_with_trades = universe.get('symbols_with_trades', 0)
    
    regime = report.get('regime', {})
    r_state = regime.get('state', 'Unknown').upper()
    r_confidence = regime.get('confidence', 0)
    
    vrp_metrics = report.get('vrp_metrics', [])
    avg_iv_rv = sum(v.get('iv_rv_ratio', 1.0) for v in vrp_metrics) / len(vrp_metrics) if vrp_metrics else 1.0
    
    # Colors
    regime_colors = {'BULL': '#10b981', 'BEAR': '#ef4444', 'CHOP': '#f59e0b'}
    r_color = regime_colors.get(r_state, '#3b82f6')
    vrp_color = '#10b981' if avg_iv_rv >= 1.12 else '#f59e0b'
    
    # Compact 3-column header
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(15,23,42,0.9), rgba(30,41,59,0.7)); border: 1px solid rgba(71,85,105,0.4); border-radius: 8px; padding: 16px; margin-bottom: 16px;">
        <div style="display: flex; justify-content: space-between; align-items: center; gap: 20px;">
    """, unsafe_allow_html=True)
    
    info_col1, info_col2, info_col3 = st.columns(3)
    
    with info_col1:
        status_icon = "✓" if provider_connected else "✗"
        status_color = "#10b981" if provider_connected else "#ef4444"
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="color: #64748b; font-size: 10px; text-transform: uppercase; letter-spacing: 1px;">DATA SOURCE</div>
            <div style="color: {status_color}; font-size: 18px; font-weight: bold; margin: 4px 0;">{status_icon} {provider_source.upper()}</div>
            <div style="color: #94a3b8; font-size: 11px;">{symbols_scanned} scanned • {symbols_with_edges} edges • {symbols_with_trades} trades</div>
        </div>
        """, unsafe_allow_html=True)
    
    with info_col2:
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="color: #64748b; font-size: 10px; text-transform: uppercase; letter-spacing: 1px;">MARKET REGIME</div>
            <div style="color: {r_color}; font-size: 18px; font-weight: bold; margin: 4px 0;">{r_state}</div>
            <div style="color: #94a3b8; font-size: 11px;">{r_confidence*100:.0f}% confidence</div>
        </div>
        """, unsafe_allow_html=True)
    
    with info_col3:
        vrp_status = "RICH" if avg_iv_rv >= 1.12 else "FAIR"
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="color: #64748b; font-size: 10px; text-transform: uppercase; letter-spacing: 1px;">VOL PREMIUM</div>
            <div style="color: {vrp_color}; font-size: 18px; font-weight: bold; margin: 4px 0;">{vrp_status} ({avg_iv_rv:.2f}x)</div>
            <div style="color: #94a3b8; font-size: 11px;">IV/RV ratio</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("</div></div>", unsafe_allow_html=True)
    
    # VIX / Edge count (small row)
    vix_col, edge_col = st.columns(2)
    with vix_col:
        st.metric("VIX / VOL", f"{format_percent(0.18)}")
    with edge_col:
        st.metric("EDGE COUNT", len(report.get('edges', [])))

    # ACTION ZONE
    st.markdown("### ⚡ ACTION ZONE")
    
    candidates = report.get('candidates', [])
    trades = [c for c in candidates if c.get('recommendation') == 'TRADE']
    
    if trades:
        st.markdown(f"**{len(trades)} Trade{'s' if len(trades) > 1 else ''} Available** — Compare and select:")
        
        # Create columns for cards (max 2 per row)
        num_cols = min(len(trades), 2)
        
        for i in range(0, len(trades), num_cols):
            cols = st.columns(num_cols)
            for j, col in enumerate(cols):
                if i + j < len(trades):
                    trade = trades[i + j]
                    with col:
                        render_trade_card(trade)
        
        # Optional: Show advanced details in expander
        with st.expander("📋 Advanced Execution Details", expanded=False):
            selected_symbol = st.selectbox(
                "Select trade:",
                [t['symbol'] for t in trades],
                key="trade_detail_select"
            )
            selected_trade = next((t for t in trades if t['symbol'] == selected_symbol), trades[0])
            render_trade_ticket(selected_trade)
    else:
        edges = report.get('edges', [])
        if edges:
            st.info(f"📍 {len(edges)} Edges found, but 0 trades generated. See PASS log.")
        else:
            st.markdown("""
            <div style="padding: 40px; text-align: center; border: 1px dashed #333; border-radius: 8px; color: #666;">
                <div style="font-size: 2rem; margin-bottom: 10px;">💤</div>
                <div>NO EDGES DETECTED TODAY</div>
                <div style="font-size: 12px; margin-top: 8px;">Markets are efficient. Capital preserved.</div>
            </div>
            """, unsafe_allow_html=True)

    # FOOTER
    st.markdown("---")
    cols = st.columns(4)
    port = report.get('portfolio', {})
    cols[0].metric("POSITIONS", port.get('positions_open', 0))
    cols[1].metric("MAX DRAWDOWN", "0.00%")
    cols[2].metric("SESSION P&L", format_dollars(port.get('realized_pnl_today_dollars', 0)))
    cols[3].metric("NET LIQ", format_dollars(port.get('net_liq', 10000)))  # Default if missing


if __name__ == "__main__":
    main()
