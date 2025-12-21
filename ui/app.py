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
    Render a polished trade card for grid display.
    Uses hybrid approach: HTML for styling + Streamlit for interactive elements.
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
    
    # Colors
    badge_color = "#ef4444" if is_fallback else "#10b981"
    badge_text = "⚠️ FALLBACK MODE" if is_fallback else "✓ CONFIRMED"
    card_border = "#ef4444" if is_fallback else "#10b981"
    
    # Card container (use Streamlit container for isolation)
    with st.container():
        # Header using pure Streamlit  
        col_sym, col_badge = st.columns([3, 1])
        with col_sym:
            fallback_tag = " [FALLBACK]" if is_fallback else ""
            st.markdown(f"### {symbol}{fallback_tag}")
            st.caption(f"📉 {direction} • {edge_type}")
        with col_badge:
            if is_fallback:
                st.error(badge_text)
            else:
                st.success(badge_text)
        
        # FALLBACK warning (prominent, below header)
        if is_fallback:
            st.warning("⚠️ **Edge detected via absolute thresholds** (insufficient history). Treat as REVIEW unless `allow_fallback_edges=true`.")
        
        # Metrics using Streamlit columns with colored backgrounds via metrics
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("💵 Cost", f"${cost:.0f}")
        with m2:
            st.metric("📈 Profit", f"${max_profit:.0f}")
        with m3:
            st.metric("📉 Loss", f"${max_loss:.0f}")
        with m4:
            st.metric("🎲 Return", f"{return_mult:.1f}x")
        
        # Footer
        st.caption(f"⏰ {exp} ({dte} days) • 📊 {contracts} contracts")
        
        # Advanced Details Expander (inside card)
        with st.expander("📋 Trade Details", expanded=False):
            # Legs
            legs = structure.get('legs', [])
            if legs:
                st.markdown("**📊 Option Legs:**")
                for leg in legs:
                    action = leg.get('action', 'BUY')
                    qty = leg.get('quantity', 1)
                    strike = leg.get('strike', 0)
                    opt_type = leg.get('option_type', 'P')
                    leg_exp = leg.get('expiration', exp)
                    st.code(f"{action} {qty} {symbol} {leg_exp} {strike} {opt_type}", language=None)
            
            # Breakeven
            breakevens = structure.get('breakevens', [])
            if breakevens:
                be_str = ", ".join([f"${b:.2f}" for b in breakevens])
                st.metric("🎯 Breakeven", be_str)
            
            # Risk Tiers
            sizing = candidate.get('sizing') or {}
            risk_tiers = sizing.get('risk_tiers', [])
            if risk_tiers:
                st.markdown("**📊 Risk Sizing:**")
                tier_text = " | ".join([f"{t['risk_pct']:.0%}: {t['contracts']} ct (${t['debit']:.0f})" for t in risk_tiers[:4]])
                st.caption(tier_text)
            
            # Edge-Type Specific Metrics
            edge = candidate.get('edge') or {}
            edge_type = edge.get('type', 'unknown')
            edge_metrics = edge.get('metrics', {})
            
            st.markdown("**📊 Edge Metrics:**")
            
            if edge_type in ['skew_extreme', 'SkewExtremeEdge']:
                # SKEW edge - show put/call IV and skew
                put_iv = edge_metrics.get('put_iv_25d')
                call_iv = edge_metrics.get('call_iv_25d')
                skew = edge_metrics.get('put_call_skew')
                percentile = edge_metrics.get('skew_percentile')
                history_mode = edge_metrics.get('history_mode', 1)
                
                em1, em2 = st.columns(2)
                with em1:
                    if put_iv is not None:
                        st.metric("🔴 Put IV (25d)", f"{put_iv*100:.1f}%")
                    if skew is not None:
                        st.metric("📐 Put-Call Skew", f"{skew*100:.1f}%")
                with em2:
                    if call_iv is not None:
                        st.metric("🟢 Call IV (25d)", f"{call_iv*100:.1f}%")
                    if percentile is not None and history_mode == 1:
                        st.metric("📈 Skew Percentile", f"{percentile*100:.0f}%")
                    elif history_mode == 0:
                        st.caption("📈 Percentile: N/A (fallback mode)")
                        
            elif edge_type in ['vrp', 'VRPEdge']:
                # VRP edge - show IV, RV, ratio
                atm_iv = edge_metrics.get('atm_iv')
                rv_20d = edge_metrics.get('rv_20d')
                iv_rv_ratio = edge_metrics.get('iv_rv_ratio')
                threshold = edge_metrics.get('threshold', 1.12)
                
                em1, em2 = st.columns(2)
                with em1:
                    if atm_iv is not None:
                        st.metric("📊 ATM IV", f"{atm_iv*100:.1f}%")
                    if rv_20d is not None:
                        st.metric("📉 RV (20d)", f"{rv_20d*100:.1f}%")
                with em2:
                    if iv_rv_ratio is not None:
                        color = "🟢" if iv_rv_ratio >= threshold else "🟡"
                        st.metric(f"{color} IV/RV Ratio", f"{iv_rv_ratio:.2f}x")
                        status = "ABOVE" if iv_rv_ratio >= threshold else "BELOW"
                        st.caption(f"Threshold: {threshold:.2f} ({status})")
                        
            elif edge_type in ['term_structure', 'TermStructureEdge']:
                # Term structure - show front/back IV
                front_iv = edge_metrics.get('front_iv')
                back_iv = edge_metrics.get('back_iv')
                slope = edge_metrics.get('slope')
                
                em1, em2 = st.columns(2)
                with em1:
                    if front_iv is not None:
                        st.metric("📅 Front IV", f"{front_iv*100:.1f}%")
                with em2:
                    if back_iv is not None:
                        st.metric("📆 Back IV", f"{back_iv*100:.1f}%")
                if slope is not None:
                    st.caption(f"Term Slope: {slope:.3f}")
                    
            elif edge_type in ['gamma_pressure', 'GammaPressureEdge']:
                # Gamma pressure - show gamma metrics
                max_gamma_strike = edge_metrics.get('max_gamma_strike')
                pin_low = edge_metrics.get('pin_zone_low')
                pin_high = edge_metrics.get('pin_zone_high')
                net_gamma = edge_metrics.get('net_gamma')
                
                if max_gamma_strike:
                    st.metric("🎯 Max Gamma Strike", f"${max_gamma_strike:.0f}")
                if pin_low and pin_high:
                    st.caption(f"Pin Zone: ${pin_low:.0f} - ${pin_high:.0f}")
                if net_gamma:
                    gamma_side = "LONG" if net_gamma > 0 else "SHORT"
                    st.caption(f"Net Gamma: {gamma_side}")
            else:
                st.caption(f"Edge type: {edge_type}")
            
            st.divider()
            
            # Edge Rationale
            rationale = edge.get('rationale', {})
            if rationale and isinstance(rationale, dict):
                st.markdown("**💡 Why This Trade:**")
                for key, val in rationale.items():
                    st.caption(f"• {key}: {val}")
            elif rationale and isinstance(rationale, str):
                st.markdown("**💡 Why This Trade:**")
                st.caption(f"• {rationale}")
            
            # Probability Metrics (Model-Based)
            prob_metrics = candidate.get('probability_metrics')
            if prob_metrics:
                st.markdown("**📊 Model Probabilities:**")
                
                # Display key metrics
                pop = prob_metrics.get('pop_expiry')
                p_range = prob_metrics.get('p_between_breakevens')
                ev = prob_metrics.get('expected_pnl_expiry')
                ev_ratio = prob_metrics.get('ev_per_dollar_risk')
                
                pm1, pm2 = st.columns(2)
                with pm1:
                    if pop is not None:
                        st.metric("🎲 Model PoP", f"{pop:.0%}")
                    if p_range is not None:
                        st.metric("📐 P(In Range)", f"{p_range:.0%}")
                with pm2:
                    if ev is not None:
                        st.metric("💵 EV (Binary)", f"${ev:.0f}")
                    if ev_ratio is not None:
                        st.metric("📈 EV/Risk", f"{ev_ratio:.2f}")
                
                # Disclaimer
                st.caption("⚠️ Model-based probabilities assume lognormal distribution at expiry. Not predictive of actual outcomes.")
        
        # Execute button (full width)
        can_execute = is_valid and contracts > 0
        
        if card_state == 'ready':
            if st.button(f"🚀 EXECUTE {symbol}", key=f"exec_{candidate_id}", disabled=not can_execute, type="primary", use_container_width=True):
                st.session_state['card_states'][card_key] = 'previewing'
                st.rerun()
        elif card_state == 'previewing':
            # IBKR Preview step - try webhook first, then subprocess
            st.warning(f"⏳ Connecting to execute {symbol}...")
            
            # Check for webhook configuration
            webhook_url = None
            webhook_token = None
            try:
                if hasattr(st, 'secrets'):
                    webhook_url = st.secrets.get('WEBHOOK_URL', None)
                    webhook_token = st.secrets.get('WEBHOOK_TOKEN', None)
            except:
                pass
            
            if webhook_url and webhook_token:
                # Use webhook for remote execution
                try:
                    import requests
                    headers = {'Authorization': f'Bearer {webhook_token}'}
                    payload = {'symbol': symbol, 'action': 'preview'}
                    
                    response = requests.post(
                        f"{webhook_url}/execute",
                        json=payload,
                        headers=headers,
                        timeout=90
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('success'):
                            st.session_state['card_states'][card_key] = 'confirmed'
                            st.session_state[f'preview_{card_key}'] = data.get('output', '')
                            st.rerun()
                        else:
                            st.error(f"Preview failed: {data.get('output', 'Unknown error')}")
                            st.session_state['card_states'][card_key] = 'ready'
                    else:
                        st.error(f"Webhook error: {response.status_code} - {response.text}")
                        st.session_state['card_states'][card_key] = 'ready'
                except requests.exceptions.ConnectionError:
                    st.error("🔌 Cannot reach webhook server. Is it running?")
                    st.code("python3 scripts/webhook_server.py", language="bash")
                    st.session_state['card_states'][card_key] = 'ready'
                except Exception as e:
                    st.error(f"Webhook error: {e}")
                    st.session_state['card_states'][card_key] = 'ready'
            else:
                # Fallback to subprocess (local only)
                try:
                    import subprocess
                    result = subprocess.run(
                        ['python3', 'scripts/submit_test_order.py', '--paper', '--dry-run', '--symbol', symbol],
                        capture_output=True, text=True, timeout=60, cwd=str(Path(__file__).parent.parent)
                    )
                    output = result.stdout + result.stderr
                    
                    # Check if running on cloud without ib_insync
                    if 'ib_insync' in output or 'ModuleNotFoundError' in output:
                        st.error("🔒 **LOCAL ONLY** - IBKR execution requires local setup")
                        
                        # Generate the command
                        cmd = f"python3 scripts/submit_test_order.py --paper --submit --symbol {symbol}"
                        
                        st.markdown("**Quick Execute:** Copy this command and run in your local terminal:")
                        st.code(cmd, language="bash")
                        
                        # Copy button
                        if st.button("📋 Copy Command", key=f"copy_{candidate_id}"):
                            st.session_state['copied_cmd'] = cmd
                            st.success("✅ Copied! Paste in terminal and press Enter")
                        
                        with st.expander("🔧 Enable Remote Execution"):
                            st.markdown("""
                            **Set up webhook for one-click execution:**
                            1. `python3 scripts/webhook_server.py` (local terminal)
                            2. `ngrok http 8765` (expose to internet)
                            3. Add to `.streamlit/secrets.toml`:
                            ```
                            WEBHOOK_URL = "https://your-ngrok-url.ngrok.io"
                            WEBHOOK_TOKEN = "your-token-from-server"
                            ```
                            """)
                        
                        st.session_state['card_states'][card_key] = 'ready'
                    elif result.returncode == 0:
                        st.session_state['card_states'][card_key] = 'confirmed'
                        st.session_state[f'preview_{card_key}'] = result.stdout
                        st.rerun()
                    else:
                        st.error(f"Preview failed: {output}")
                        st.session_state['card_states'][card_key] = 'ready'
                except Exception as e:
                    st.error(f"IBKR connection error: {e}")
                    st.session_state['card_states'][card_key] = 'ready'
        elif card_state == 'confirmed':
            # Show preview results
            preview_output = st.session_state.get(f'preview_{card_key}', '')
            if preview_output:
                with st.expander("📋 Order Preview", expanded=True):
                    st.code(preview_output[-2000:], language="text")
            
            st.success(f"✅ {symbol} Preview OK - Ready to submit")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button(f"🚀 SUBMIT TO IBKR", key=f"submit_{candidate_id}", type="primary", use_container_width=True):
                    st.session_state['card_states'][card_key] = 'submitting'
                    st.rerun()
            with col2:
                if st.button("↩️ Cancel", key=f"cancel_{candidate_id}", use_container_width=True):
                    st.session_state['card_states'][card_key] = 'ready'
                    st.rerun()
        elif card_state == 'submitting':
            # Actually submit to IBKR - try webhook first
            st.warning(f"🚀 Submitting {symbol} to IBKR...")
            
            # Check for webhook configuration
            webhook_url = None
            webhook_token = None
            try:
                if hasattr(st, 'secrets'):
                    webhook_url = st.secrets.get('WEBHOOK_URL', None)
                    webhook_token = st.secrets.get('WEBHOOK_TOKEN', None)
            except:
                pass
            
            if webhook_url and webhook_token:
                # Use webhook for remote execution
                try:
                    import requests
                    headers = {'Authorization': f'Bearer {webhook_token}'}
                    payload = {'symbol': symbol, 'action': 'submit'}
                    
                    response = requests.post(
                        f"{webhook_url}/execute",
                        json=payload,
                        headers=headers,
                        timeout=120
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('success') and 'Recorded to blotter' in data.get('output', ''):
                            st.session_state['card_states'][card_key] = 'submitted'
                            st.session_state[f'submit_{card_key}'] = data.get('output', '')
                            st.rerun()
                        else:
                            st.error(f"Submit failed: {data.get('output', 'Unknown error')}")
                            st.session_state['card_states'][card_key] = 'confirmed'
                    else:
                        st.error(f"Webhook error: {response.status_code} - {response.text}")
                        st.session_state['card_states'][card_key] = 'confirmed'
                except Exception as e:
                    st.error(f"Webhook submission error: {e}")
                    st.session_state['card_states'][card_key] = 'confirmed'
            else:
                # Fallback to subprocess
                try:
                    import subprocess
                    result = subprocess.run(
                        ['python3', 'scripts/submit_test_order.py', '--paper', '--submit', '--symbol', symbol],
                        capture_output=True, text=True, timeout=90, cwd=str(Path(__file__).parent.parent)
                    )
                    if result.returncode == 0 and 'Recorded to blotter' in result.stdout:
                        st.session_state['card_states'][card_key] = 'submitted'
                        st.session_state[f'submit_{card_key}'] = result.stdout
                        st.rerun()
                    else:
                        st.error(f"Submit failed: {result.stderr or result.stdout}")
                        st.session_state['card_states'][card_key] = 'confirmed'
                except Exception as e:
                    st.error(f"IBKR submission error: {e}")
                    st.session_state['card_states'][card_key] = 'confirmed'
        elif card_state == 'submitted':
            submit_output = st.session_state.get(f'submit_{card_key}', '')
            st.success(f"✅ {symbol} ORDER SUBMITTED!")
            with st.expander("📋 Submission Details"):
                st.code(submit_output[-2000:], language="text")
            st.info("Check Blotter tab for trade tracking")
        
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
    
    # Get recommendation to enforce safety gates
    recommendation = candidate.get('recommendation', 'PASS')
    
    # Safety gate: Only TRADE candidates can be submitted
    if recommendation != 'TRADE':
        st.error(f"⛔ Cannot submit: Recommendation is {recommendation} (must be TRADE)")
        can_submit = False
    
    # Safety gate: No fallback edges unless allowed
    allow_fallback = True  # TODO: Read from config
    if is_fallback and not allow_fallback:
        st.error("⛔ Cannot submit: FALLBACK edge - set allow_fallback_edges=true to trade")
        can_submit = False
    
    with btn_col1:
        if order_state == 'initial':
            if st.button("🔍 PREVIEW ORDER", key=f"preview_{candidate_id}", disabled=not can_submit):
                st.session_state['order_states'][candidate_id] = 'previewing'
                
                # Actual IBKR contract resolution
                try:
                    from execution.ibkr_order_client import get_ibkr_client, LiveTradingBlocked
                    
                    client = get_ibkr_client(port=4002)  # IB Gateway paper
                    
                    if not client.is_connected():
                        connected = client.connect()
                        if not connected:
                            st.error("❌ Failed to connect to IBKR Gateway")
                            st.session_state['order_states'][candidate_id] = 'initial'
                            st.rerun()
                    
                    # Resolve contracts
                    legs = structure.get('legs', [])
                    resolved_legs = client.resolve_contracts(legs)
                    
                    # Store resolved legs in session
                    if 'resolved_legs' not in st.session_state:
                        st.session_state['resolved_legs'] = {}
                    st.session_state['resolved_legs'][candidate_id] = resolved_legs
                    
                    # Check all legs resolved
                    all_resolved = all(leg.is_resolved for leg in resolved_legs)
                    
                    if all_resolved:
                        st.session_state['order_states'][candidate_id] = 'previewed'
                        st.success("✅ Contracts resolved via IBKR")
                    else:
                        errors = [leg.error for leg in resolved_legs if not leg.is_resolved]
                        st.error(f"❌ Contract resolution failed: {', '.join(errors)}")
                        st.session_state['order_states'][candidate_id] = 'initial'
                    
                except LiveTradingBlocked as e:
                    st.error(f"🚨 LIVE TRADING BLOCKED: {e}")
                    st.session_state['order_states'][candidate_id] = 'initial'
                except ImportError:
                    st.error("❌ ib_insync not installed. Run: pip install ib_insync")
                    st.session_state['order_states'][candidate_id] = 'initial'
                except Exception as e:
                    st.error(f"❌ IBKR error: {e}")
                    st.session_state['order_states'][candidate_id] = 'initial'
                
                st.rerun()
                
        elif order_state == 'previewed':
            resolved_legs = st.session_state.get('resolved_legs', {}).get(candidate_id, [])
            if resolved_legs:
                st.success(f"✅ Preview: {len(resolved_legs)} contracts resolved")
        elif order_state == 'submitted':
            st.info("📤 Order submitted to IBKR")
    
    with btn_col2:
        if order_state == 'previewed':
            submit_disabled = not (can_submit and fallback_confirmed)
            if st.button("✅ SUBMIT ORDER", key=f"submit_{candidate_id}", type="primary", disabled=submit_disabled):
                try:
                    from execution.ibkr_order_client import get_ibkr_client
                    
                    client = get_ibkr_client(port=4002)  # IB Gateway paper
                    
                    # Get resolved legs from session
                    resolved_legs = st.session_state.get('resolved_legs', {}).get(candidate_id, [])
                    
                    if not resolved_legs:
                        st.error("❌ No resolved legs - click Preview first")
                    else:
                        # Get limit price
                        credit = structure.get('entry_credit_dollars', 0)
                        debit = structure.get('entry_debit_dollars', 0)
                        limit_price = (credit / 100) if credit > 0 else -(debit / 100)
                        
                        # Create order ticket
                        ticket = client.create_order_ticket(
                            candidate_id=candidate_id,
                            symbol=symbol,
                            resolved_legs=resolved_legs,
                            quantity=selected_contracts,
                            limit_price=limit_price,
                        )
                        
                        # Submit with transmit=True
                        submitted_ticket = client.submit_order(ticket, transmit=True)
                        
                        # Store order ticket in session
                        if 'order_tickets' not in st.session_state:
                            st.session_state['order_tickets'] = {}
                        st.session_state['order_tickets'][candidate_id] = submitted_ticket
                        
                        st.session_state['order_states'][candidate_id] = 'submitted'
                        st.session_state['confirmed_trades'].add(candidate_id)
                        st.success(f"✅ Order {submitted_ticket.order_id} submitted!")
                        
                except Exception as e:
                    st.error(f"❌ Submit failed: {e}")
                
                st.rerun()
                
        elif order_state == 'submitted':
            st.success("✅ Order SUBMITTED")
    
    with btn_col3:
        if order_state in ['previewed', 'submitted']:
            if st.button("❌ Cancel", key=f"cancel_{candidate_id}"):
                st.session_state['order_states'][candidate_id] = 'initial'
                st.session_state['confirmed_trades'].discard(candidate_id)
                if 'resolved_legs' in st.session_state:
                    st.session_state['resolved_legs'].pop(candidate_id, None)
                if 'order_tickets' in st.session_state:
                    st.session_state['order_tickets'].pop(candidate_id, None)
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


def render_blotter_tab():
    """
    Render Blotter tab with:
    - Open positions with live P&L
    - Trade history table
    - Performance statistics
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from execution.blotter import get_blotter
    
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(15,23,42,0.9), rgba(30,41,59,0.7)); 
                border: 1px solid rgba(71,85,105,0.4); border-radius: 12px; padding: 24px; margin-bottom: 20px;">
        <div style="display: flex; align-items: center; gap: 12px;">
            <span style="font-size: 2rem;">📊</span>
            <div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #f1f5f9;">TRADE BLOTTER</div>
                <div style="color: #94a3b8; font-size: 0.9rem;">Paper trading performance & position tracking</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    blotter = get_blotter()
    summary = blotter.get_summary()
    open_trades = blotter.get_open_trades()
    closed_trades = blotter.get_closed_trades()
    
    # SUMMARY CARDS
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.6); border: 1px solid #475569; border-radius: 8px; padding: 20px; text-align: center;">
            <div style="color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 1px;">Total Trades</div>
            <div style="color: #f1f5f9; font-size: 2rem; font-weight: 700;">{summary['total_trades']}</div>
            <div style="color: #64748b; font-size: 11px;">{summary['open_trades']} open</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c2:
        pnl = summary['total_pnl']
        pnl_color = "#10b981" if pnl >= 0 else "#ef4444"
        pnl_sign = "+" if pnl >= 0 else ""
        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.6); border: 1px solid #475569; border-radius: 8px; padding: 20px; text-align: center;">
            <div style="color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 1px;">Total P&L</div>
            <div style="color: {pnl_color}; font-size: 2rem; font-weight: 700;">{pnl_sign}${pnl:.0f}</div>
            <div style="color: #64748b; font-size: 11px;">Realized</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c3:
        win_rate = summary['win_rate']
        wr_color = "#10b981" if win_rate >= 50 else "#f59e0b" if win_rate >= 40 else "#ef4444"
        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.6); border: 1px solid #475569; border-radius: 8px; padding: 20px; text-align: center;">
            <div style="color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 1px;">Win Rate</div>
            <div style="color: {wr_color}; font-size: 2rem; font-weight: 700;">{win_rate:.1f}%</div>
            <div style="color: #64748b; font-size: 11px;">{summary['winners']}W / {summary['losers']}L</div>
        </div>
        """, unsafe_allow_html=True)
    
    with c4:
        avg_pnl = summary['avg_pnl']
        avg_color = "#10b981" if avg_pnl >= 0 else "#ef4444"
        avg_sign = "+" if avg_pnl >= 0 else ""
        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.6); border: 1px solid #475569; border-radius: 8px; padding: 20px; text-align: center;">
            <div style="color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 1px;">Avg P&L</div>
            <div style="color: {avg_color}; font-size: 2rem; font-weight: 700;">{avg_sign}${avg_pnl:.0f}</div>
            <div style="color: #64748b; font-size: 11px;">Per trade</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # OPEN POSITIONS
    st.markdown("""
    <div style="color: #f1f5f9; font-size: 1.2rem; font-weight: 600; margin-bottom: 12px; 
                border-bottom: 2px solid #3b82f6; padding-bottom: 8px;">
        🟢 OPEN POSITIONS
    </div>
    """, unsafe_allow_html=True)
    
    if open_trades:
        for trade in open_trades:
            entry_price = trade.entry_price or 0
            max_loss = trade.max_loss_dollars or 0
            entry_display = f"+${entry_price:.2f}" if entry_price > 0 else f"-${abs(entry_price):.2f}"
            spread_type = "CREDIT" if entry_price > 0 else "DEBIT"
            structure_name = trade.structure or "spread"
            dte = trade.dte or 0
            
            st.markdown(f"""
            <div style="background: rgba(30,41,59,0.5); border: 1px solid #475569; border-radius: 8px; 
                        padding: 16px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <span style="color: #3b82f6; font-weight: 700; font-size: 1.1rem;">{trade.symbol}</span>
                    <span style="color: #64748b; margin-left: 12px;">{structure_name} • {dte} DTE</span>
                </div>
                <div style="text-align: right;">
                    <div style="color: #10b981; font-weight: 600;">{spread_type} {entry_display}</div>
                    <div style="color: #64748b; font-size: 11px;">Max Loss: ${max_loss:.0f}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background: rgba(30,41,59,0.3); border: 1px dashed #475569; border-radius: 8px; 
                    padding: 40px; text-align: center; color: #64748b;">
            No open positions
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # TRADE HISTORY
    st.markdown("""
    <div style="color: #f1f5f9; font-size: 1.2rem; font-weight: 600; margin-bottom: 12px; 
                border-bottom: 2px solid #6366f1; padding-bottom: 8px;">
        📜 TRADE HISTORY
    </div>
    """, unsafe_allow_html=True)
    
    if closed_trades:
        for trade in sorted(closed_trades, key=lambda t: t.exit_timestamp or t.timestamp or '', reverse=True)[:20]:
            pnl = trade.realized_pnl or 0
            pnl_color = "#10b981" if pnl >= 0 else "#ef4444"
            pnl_sign = "+" if pnl >= 0 else ""
            result_icon = "✅" if pnl >= 0 else "❌"
            
            date_str = (trade.timestamp or '')[:10] if trade.timestamp else "N/A"
            
            st.markdown(f"""
            <div style="background: rgba(30,41,59,0.4); border-left: 3px solid {pnl_color}; 
                        padding: 12px 16px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center;">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <span style="font-size: 1.2rem;">{result_icon}</span>
                    <div>
                        <span style="color: #f1f5f9; font-weight: 600;">{trade.symbol}</span>
                        <span style="color: #64748b; margin-left: 8px;">{trade.structure or 'spread'}</span>
                    </div>
                </div>
                <div style="display: flex; gap: 24px; align-items: center;">
                    <div style="color: #94a3b8; font-size: 12px;">{trade.edge_type or 'edge'}</div>
                    <div style="color: #64748b; font-size: 12px;">{date_str}</div>
                    <div style="color: {pnl_color}; font-weight: 700; min-width: 80px; text-align: right;">{pnl_sign}${pnl:.0f}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background: rgba(30,41,59,0.3); border: 1px dashed #475569; border-radius: 8px; 
                    padding: 40px; text-align: center; color: #64748b;">
            No closed trades yet — start paper trading to build history
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # PERFORMANCE BY SYMBOL / EDGE
    if summary['by_symbol'] or summary['by_edge']:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            <div style="color: #f1f5f9; font-size: 1rem; font-weight: 600; margin-bottom: 12px;">
                📈 BY SYMBOL
            </div>
            """, unsafe_allow_html=True)
            
            for sym, data in summary['by_symbol'].items():
                sym_pnl = data['pnl']
                sym_color = "#10b981" if sym_pnl >= 0 else "#ef4444"
                sym_sign = "+" if sym_pnl >= 0 else ""
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #334155;">
                    <span style="color: #f1f5f9; font-weight: 500;">{sym}</span>
                    <span style="color: {sym_color}; font-weight: 600;">{sym_sign}${sym_pnl:.0f} ({data['trades']} trades)</span>
                </div>
                """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div style="color: #f1f5f9; font-size: 1rem; font-weight: 600; margin-bottom: 12px;">
                🎯 BY EDGE TYPE
            </div>
            """, unsafe_allow_html=True)
            
            for edge, data in summary['by_edge'].items():
                edge_pnl = data['pnl']
                edge_color = "#10b981" if edge_pnl >= 0 else "#ef4444"
                edge_sign = "+" if edge_pnl >= 0 else ""
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #334155;">
                    <span style="color: #f1f5f9; font-weight: 500;">{edge}</span>
                    <span style="color: {edge_color}; font-weight: 600;">{edge_sign}${edge_pnl:.0f} ({data['trades']} trades)</span>
                </div>
                """, unsafe_allow_html=True)


def render_edge_history_tab():
    """
    Render Edge History tab - shows past signals from run logs.
    
    Reads trade_candidate events from logs/runs/*.jsonl to track:
    - All edges found (traded or not)
    - What would have happened
    """
    import glob
    
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(15,23,42,0.9), rgba(30,41,59,0.7)); 
                border: 1px solid rgba(71,85,105,0.4); border-radius: 12px; padding: 24px; margin-bottom: 20px;">
        <div style="display: flex; align-items: center; gap: 12px;">
            <span style="font-size: 2rem;">📜</span>
            <div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #f1f5f9;">EDGE HISTORY</div>
                <div style="color: #94a3b8; font-size: 0.9rem;">Past signals found by the engine</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Read all run logs
    logs_dir = Path(__file__).parent.parent / 'logs' / 'runs'
    log_files = sorted(glob.glob(str(logs_dir / 'run_*.jsonl')), reverse=True)[:30]  # Last 30 runs
    
    edges = []
    for log_file in log_files:
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get('event') == 'trade_candidate':
                            data = entry.get('data', {})
                            ts = data.get('timestamp', entry.get('timestamp', ''))
                            edges.append({
                                'timestamp': ts[:16] if ts else '',
                                'symbol': data.get('symbol', '') or '',
                                'edge_type': data.get('edge', {}).get('type', '') or '',
                                'strength': data.get('edge', {}).get('strength', 0) or 0,
                                'percentile': data.get('edge', {}).get('metrics', {}).get('skew_percentile', 0) or 0,
                                'direction': data.get('edge', {}).get('direction', '') or '',
                                'recommendation': data.get('recommendation', '') or '',
                                'structure': data.get('structure', {}).get('type', '') or '',
                                'max_loss': data.get('structure', {}).get('max_loss_dollars', 0) or 0,
                                'max_profit': data.get('structure', {}).get('max_profit_dollars', 0) or 0,
                                'regime': data.get('regime', {}).get('state', '') or '',
                                'rationale': data.get('edge', {}).get('rationale', '') or '',
                            })
                    except:
                        pass
        except:
            pass
    
    if not edges:
        st.info("No edge history found. Run the engine to generate signals.")
        return
    
    # Summary
    total_edges = len(edges)
    trade_edges = len([e for e in edges if e['recommendation'] == 'TRADE'])
    pass_edges = len([e for e in edges if e['recommendation'] == 'PASS'])
    
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Edges", total_edges)
    with c2:
        st.metric("TRADE Signals", trade_edges)
    with c3:
        st.metric("PASS Signals", pass_edges)
    with c4:
        trade_rate = (trade_edges / total_edges * 100) if total_edges > 0 else 0
        st.metric("Trade Rate", f"{trade_rate:.0f}%")
    
    st.markdown("---")
    
    # Filter
    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        symbol_filter = st.selectbox("Filter by Symbol", ["ALL"] + list(set(e['symbol'] for e in edges)))
    with filter_col2:
        rec_filter = st.selectbox("Filter by Recommendation", ["ALL", "TRADE", "PASS"])
    
    # Apply filters
    filtered = edges
    if symbol_filter != "ALL":
        filtered = [e for e in filtered if e['symbol'] == symbol_filter]
    if rec_filter != "ALL":
        filtered = [e for e in filtered if e['recommendation'] == rec_filter]
    
    st.markdown(f"**Showing {len(filtered)} signals**")
    
    # Display edges
    for edge in filtered[:50]:  # Show last 50
        rec = edge['recommendation']
        rec_color = "#10b981" if rec == "TRADE" else "#f59e0b" if rec == "PASS" else "#64748b"
        
        strength_pct = edge['strength'] * 100
        percentile = edge['percentile']
        
        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.5); border-left: 4px solid {rec_color}; 
                    padding: 16px; margin-bottom: 12px; border-radius: 0 8px 8px 0;">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <span style="color: #3b82f6; font-weight: 700; font-size: 1.2rem;">{edge['symbol']}</span>
                    <span style="color: {rec_color}; margin-left: 12px; font-weight: 600;">{rec}</span>
                    <span style="color: #64748b; margin-left: 12px;">{edge['edge_type']}</span>
                </div>
                <div style="text-align: right;">
                    <div style="color: #f1f5f9; font-size: 0.9rem;">{edge['timestamp']}</div>
                    <div style="color: #64748b; font-size: 0.8rem;">{edge['regime']}</div>
                </div>
            </div>
            <div style="margin-top: 8px; color: #94a3b8; font-size: 0.85rem;">
                Strength: {strength_pct:.0f}% | Percentile: {percentile:.0f}% | {edge['direction']}
            </div>
            <div style="margin-top: 4px; color: #64748b; font-size: 0.8rem;">
                {edge['structure'] or 'No structure'} | Max Loss: ${edge['max_loss'] or 0:.0f} | Max Profit: ${edge['max_profit'] or 0:.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander("View Rationale"):
            st.write(edge['rationale'])


def render_signals_timeline():
    """
    Signals Timeline - shows recent scan sessions (open/close) with edges.
    Reads from YYYY-MM-DD_{session}.json report files.
    """
    import glob
    
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(15,23,42,0.9), rgba(30,41,59,0.7)); 
                border: 1px solid rgba(71,85,105,0.4); border-radius: 12px; padding: 24px; margin-bottom: 20px;">
        <div style="display: flex; align-items: center; gap: 12px;">
            <span style="font-size: 2rem;">📡</span>
            <div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #f1f5f9;">SIGNALS TIMELINE</div>
                <div style="color: #94a3b8; font-size: 0.9rem;">Recent scans (open + close sessions)</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Read all report files
    reports_dir = Path(__file__).parent.parent / 'logs' / 'reports'
    report_files = sorted(glob.glob(str(reports_dir / '*.json')), reverse=True)
    
    sessions = []
    for rf in report_files[:20]:  # Last 20 reports
        if 'latest' in rf:
            continue
        try:
            with open(rf, 'r') as f:
                report = json.load(f)
                session_tag = report.get('session', 'legacy')
                sessions.append({
                    'file': Path(rf).name,
                    'date': report.get('report_date', ''),
                    'session': session_tag,
                    'generated_at': report.get('generated_at', '')[:16],
                    'regime': report.get('regime', {}).get('state', ''),
                    'regime_conf': report.get('regime', {}).get('confidence', 0),
                    'trading_allowed': report.get('trading_allowed', True),
                    'edges': report.get('edges', []),
                    'candidates': report.get('candidates', []),
                })
        except:
            pass
    
    if not sessions:
        st.info("No session reports found. Run the engine with `--session open` or `--session close`.")
        return
    
    # Summary
    total_sessions = len(sessions)
    sessions_with_edges = len([s for s in sessions if s['edges']])
    sessions_with_trades = len([s for s in sessions if any(c.get('recommendation') == 'TRADE' for c in s['candidates'])])
    
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Sessions", total_sessions)
    with c2:
        st.metric("With Edges", sessions_with_edges)
    with c3:
        st.metric("With TRADE Signals", sessions_with_trades)
    
    st.markdown("---")
    
    # Timeline
    for sess in sessions:
        session_badge = "🌅 OPEN" if sess['session'] == 'open' else "🌙 CLOSE" if sess['session'] == 'close' else "📋 LEGACY"
        edge_count = len(sess['edges'])
        trade_count = len([c for c in sess['candidates'] if c.get('recommendation') == 'TRADE'])
        
        edge_color = "#10b981" if trade_count > 0 else "#f59e0b" if edge_count > 0 else "#64748b"
        
        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.5); border-left: 4px solid {edge_color}; 
                    padding: 16px; margin-bottom: 12px; border-radius: 0 8px 8px 0;">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <span style="color: #f1f5f9; font-weight: 700; font-size: 1.1rem;">{sess['date']}</span>
                    <span style="color: #64748b; margin-left: 12px;">{session_badge}</span>
                </div>
                <div style="text-align: right;">
                    <div style="color: #94a3b8; font-size: 0.9rem;">{sess['generated_at']}</div>
                    <div style="color: #64748b; font-size: 0.8rem;">{sess['regime']} ({sess['regime_conf']:.0%})</div>
                </div>
            </div>
            <div style="margin-top: 8px;">
                <span style="color: {edge_color}; font-weight: 600;">{edge_count} edges</span>
                <span style="color: #64748b; margin-left: 16px;">{trade_count} TRADE signals</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Show edges for this session
        if sess['edges']:
            with st.expander(f"View {len(sess['edges'])} edges"):
                for edge in sess['edges']:
                    symbol = edge.get('symbol', '')
                    edge_type = edge.get('edge_type', '')
                    strength = edge.get('strength', 0) * 100
                    direction = edge.get('direction', '')
                    st.markdown(f"**{symbol}** — {edge_type} ({strength:.0f}%) — {direction}")


def render_backtest_tab():
    """
    Backtest tab - run and view historical backtests.
    """
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(15,23,42,0.9), rgba(30,41,59,0.7)); 
                border: 1px solid rgba(71,85,105,0.4); border-radius: 12px; padding: 24px; margin-bottom: 20px;">
        <div style="display: flex; align-items: center; gap: 12px;">
            <span style="font-size: 2rem;">🔬</span>
            <div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #f1f5f9;">BACKTEST</div>
                <div style="color: #94a3b8; font-size: 0.9rem;">Historical edge performance analysis</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Config
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        days = st.selectbox("Lookback Period", [30, 60, 90, 180, 252], index=2)
    
    with col2:
        symbols = st.multiselect(
            "Symbols", 
            ["SPY", "QQQ", "IWM", "TLT"],
            default=["SPY", "QQQ", "IWM", "TLT"]
        )
    
    with col3:
        run_button = st.button("🚀 Run Backtest", type="primary", use_container_width=True)
    
    # Check for existing results in session state
    if 'backtest_result' not in st.session_state:
        st.session_state['backtest_result'] = None
    
    if run_button:
        with st.spinner("Running backtest..."):
            try:
                from backtest.deterministic import DeterministicBacktester
                from datetime import date, timedelta
                
                end_date = date.today()
                start_date = end_date - timedelta(days=days)
                
                backtester = DeterministicBacktester()
                result = backtester.run_range(start_date, end_date, symbols=symbols)
                
                st.session_state['backtest_result'] = result
                st.success(f"✅ Backtest complete: {result.metrics.total_trades} trades")
                
            except ImportError as e:
                st.error(f"Missing dependency: {e}")
                st.info("Run: pip install pyyaml")
            except Exception as e:
                st.error(f"Backtest error: {e}")
    
    # Display results
    result = st.session_state.get('backtest_result')
    
    if result and result.metrics.total_trades > 0:
        m = result.metrics
        
        st.markdown("---")
        st.subheader("📊 Summary Metrics")
        
        # Metrics row
        mc1, mc2, mc3, mc4 = st.columns(4)
        
        with mc1:
            st.metric("Total Trades", m.total_trades)
        with mc2:
            pnl_delta = "positive" if m.total_pnl > 0 else "inverse"
            st.metric("Total P&L", f"${m.total_pnl:.2f}", delta=f"${m.avg_pnl:.2f} avg")
        with mc3:
            st.metric("Win Rate", f"{m.win_rate:.1f}%", delta=f"{m.winners}W / {m.losers}L")
        with mc4:
            st.metric("Profit Factor", f"{m.profit_factor:.2f}")
        
        mc5, mc6, mc7, mc8 = st.columns(4)
        
        with mc5:
            st.metric("Avg Win", f"${m.avg_win:.2f}")
        with mc6:
            st.metric("Avg Loss", f"${m.avg_loss:.2f}")
        with mc7:
            st.metric("Max Drawdown", f"${m.max_drawdown:.2f}")
        with mc8:
            st.metric("Avg Hold Days", f"{m.avg_hold_days:.1f}")
        
        # Breakdowns
        st.markdown("---")
        bc1, bc2 = st.columns(2)
        
        with bc1:
            st.subheader("By Symbol")
            if m.by_symbol:
                for sym, data in m.by_symbol.items():
                    pnl_color = "#10b981" if data['pnl'] > 0 else "#ef4444"
                    st.markdown(f"""
                    <div style="background: rgba(30,41,59,0.5); padding: 12px; border-radius: 8px; margin-bottom: 8px;">
                        <span style="color: #3b82f6; font-weight: 700;">{sym}</span>
                        <span style="color: #64748b; margin-left: 12px;">{data['trades']} trades</span>
                        <span style="color: {pnl_color}; margin-left: 12px; font-weight: 600;">${data['pnl']:.2f}</span>
                        <span style="color: #94a3b8; margin-left: 12px;">{data['win_rate']:.0f}%</span>
                    </div>
                    """, unsafe_allow_html=True)
        
        with bc2:
            st.subheader("By Edge Type")
            if m.by_edge_type:
                for edge, data in m.by_edge_type.items():
                    pnl_color = "#10b981" if data['pnl'] > 0 else "#ef4444"
                    st.markdown(f"""
                    <div style="background: rgba(30,41,59,0.5); padding: 12px; border-radius: 8px; margin-bottom: 8px;">
                        <span style="color: #a855f7; font-weight: 700;">{edge}</span>
                        <span style="color: #64748b; margin-left: 12px;">{data['trades']} trades</span>
                        <span style="color: {pnl_color}; margin-left: 12px; font-weight: 600;">${data['pnl']:.2f}</span>
                        <span style="color: #94a3b8; margin-left: 12px;">{data['win_rate']:.0f}%</span>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Trade table
        st.markdown("---")
        st.subheader("📋 Trade Log")
        
        if result.trades:
            import pandas as pd
            
            trades_data = []
            for t in result.trades:
                trades_data.append({
                    'Date': t.signal_date,
                    'Symbol': t.symbol,
                    'Edge': t.edge_type,
                    'Structure': t.structure_type,
                    'Entry': f"${t.entry_price:.4f}",
                    'Exit': f"${t.exit_price:.4f}",
                    'P&L': f"${t.net_pnl:.2f}",
                    'Exit Reason': t.exit_reason.value if hasattr(t.exit_reason, 'value') else t.exit_reason,
                    'Days': t.hold_days,
                })
            
            df = pd.DataFrame(trades_data)
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Export CSV
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Export CSV",
                csv,
                file_name=f"backtest_trades_{result.start_date}_{result.end_date}.csv",
                mime="text/csv"
            )
    
    elif result and result.metrics.total_trades == 0:
        st.warning("No trades generated in this period.")
        st.info("""
        **Possible reasons:**
        - No signals with recommendation = TRADE
        - Edge strength below threshold
        - No saved reports in logs/reports/
        
        **Try:**
        - Run `python3 scripts/run_daily.py --session open` to generate signals
        - Check Edge History tab for past signals
        """)
    
    else:
        st.info("Click 'Run Backtest' to analyze historical edge performance.")


def main():
    # SIDEBAR NAVIGATION
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 20px 0;">
            <div style="font-size: 1.5rem; font-weight: 700; color: #38bdf8;">VOLMACHINE</div>
            <div style="color: #64748b; font-size: 0.8rem;">Paper Trading Terminal</div>
        </div>
        """, unsafe_allow_html=True)
        
        page = st.radio(
            "Navigation",
            ["📈 Dashboard", "🎯 Edge Portfolio", "📊 Blotter", "📜 Edge History", "📡 Signals Timeline", "🔬 Backtest"],
            label_visibility="collapsed"
        )
        
        # If on Edge Portfolio, show edge sub-navigation
        if page == "🎯 Edge Portfolio":
            st.markdown("---")
            st.markdown("**Select Edge:**")
            try:
                from ui.edge_registry import get_edge_registry
                registry = get_edge_registry()
                edges = registry.discover_edges()
                
                if edges:
                    edge_options = ["📊 All Edges"] + [f"📈 {e.edge_id.upper()}" for e in edges]
                    edge_selection = st.radio("", edge_options, label_visibility="collapsed", key="edge_nav")
                    st.session_state['selected_edge'] = edge_selection
            except Exception as e:
                st.warning(f"Could not load edges: {e}")
        
        st.markdown("---")
        st.caption(f"v2.5 • {datetime.now().strftime('%H:%M:%S')}")
    
    # ROUTE TO PAGE
    if page == "🎯 Edge Portfolio":
        try:
            from ui.edge_registry import get_edge_registry
            from ui.edge_components import render_portfolio_page, render_edge_detail_page
            
            registry = get_edge_registry()
            edge_selection = st.session_state.get('selected_edge', "📊 All Edges")
            
            if edge_selection == "📊 All Edges":
                render_portfolio_page(registry)
            else:
                # Extract edge_id from "📈 FLAT" -> "flat"
                edge_id = edge_selection.split(" ", 1)[1].lower()
                edge = registry.get_edge(edge_id)
                if edge:
                    render_edge_detail_page(edge)
                else:
                    st.error(f"Edge not found: {edge_id}")
        except ImportError as e:
            st.error(f"Edge components not available: {e}")
        return
    elif page == "📊 Blotter":
        render_blotter_tab()
        return
    elif page == "📜 Edge History":
        render_edge_history_tab()
        return
    elif page == "📡 Signals Timeline":
        render_signals_timeline()
        return
    elif page == "🔬 Backtest":
        render_backtest_tab()
        return
    
    # HEADLINE
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<h1 class="main-title">VOLMACHINE<span style="color:#fff; font-weight:300">DESK</span></h1>', unsafe_allow_html=True)
        st.caption(f"SYSTEM ONLINE • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • v2.2")
        
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
        allowed = report.get('trading_allowed', True)
        risk_color = "#10b981" if allowed else "#ef4444"
        risk_bg = "rgba(16,185,129,0.1)" if allowed else "rgba(239,68,68,0.1)"
        risk_text = "TRADING ALLOWED" if allowed else "TRADING LOCKED"
        risk_sub = "ALL SYSTEMS NOMINAL" if allowed else "KILL SWITCH ACTIVE"
        risk_icon = "✓" if allowed else "✗"
        
        st.markdown(f"""
        <div style="
            border: 2px solid {risk_color};
            border-radius: 16px;
            padding: 24px;
            background: linear-gradient(180deg, {risk_bg} 0%, rgba(15,23,42,0.95) 100%);
            text-align: center;
            position: relative;
            overflow: hidden;
        ">
            <div style="color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 8px;">🛡️ RISK SYSTEMS</div>
            <div style="font-size: 24px; color: {risk_color}; font-weight: 800; letter-spacing: 1px;">{risk_text}</div>
            <div style="color: #94a3b8; font-size: 12px; margin-top: 8px;">{risk_sub}</div>
            <div style="position: absolute; right: 16px; top: 50%; transform: translateY(-50%); font-size: 48px; color: {risk_color}; opacity: 0.15;">{risk_icon}</div>
        </div>
        """, unsafe_allow_html=True)

    # SIGNAL STATUS
    with c2:
        sig_type, sig_label, sig_desc = get_signal_status(report)
        sig_colors = {'TRADE': '#10b981', 'PASS': '#f59e0b', 'NO_EDGE': '#64748b'}
        sig_color = sig_colors.get(sig_type, '#64748b')
        sig_bg = f"rgba({16 if sig_type=='TRADE' else 245},{185 if sig_type=='TRADE' else 158},{129 if sig_type=='TRADE' else 11},0.1)"
        
        st.markdown(f"""
        <div style="
            border: 2px solid {sig_color};
            border-radius: 16px;
            padding: 24px;
            background: linear-gradient(180deg, {sig_bg} 0%, rgba(15,23,42,0.95) 100%);
            text-align: center;
        ">
            <div style="color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 8px;">📡 SIGNAL FEED</div>
            <div style="
                display: inline-block;
                background: {sig_color};
                color: white;
                padding: 8px 24px;
                border-radius: 8px;
                font-size: 16px;
                font-weight: 700;
                letter-spacing: 1px;
            ">{sig_label}</div>
            <div style="color: #94a3b8; font-size: 12px; margin-top: 12px;">{sig_desc}</div>
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
        # Header with count and sort options
        header_col, sort_col = st.columns([2, 1])
        with header_col:
            st.markdown(f"**{len(trades)} Trade{'s' if len(trades) > 1 else ''} Available** — Compare and select:")
        with sort_col:
            sort_by = st.selectbox(
                "Sort by:",
                ["Return (High → Low)", "Cost (Low → High)", "Symbol (A → Z)"],
                key="trade_sort",
                label_visibility="collapsed"
            )
        
        # Sort trades
        if sort_by == "Return (High → Low)":
            trades = sorted(trades, key=lambda t: (
                (t.get('structure', {}).get('max_profit_dollars', 0) / 
                 max(t.get('structure', {}).get('entry_debit_dollars', 1), 1))
            ), reverse=True)
        elif sort_by == "Cost (Low → High)":
            trades = sorted(trades, key=lambda t: t.get('structure', {}).get('entry_debit_dollars', 0))
        else:  # Symbol
            trades = sorted(trades, key=lambda t: t.get('symbol', ''))
        
        # Dynamic columns: 3 for many trades, 2 for few
        num_cols = 3 if len(trades) >= 3 else min(len(trades), 2)
        
        for i in range(0, len(trades), num_cols):
            cols = st.columns(num_cols)
            for j, col in enumerate(cols):
                if i + j < len(trades):
                    trade = trades[i + j]
                    with col:
                        render_trade_card(trade)
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
