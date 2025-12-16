"""
VolMachine Desk UI v2 - Premium Trading Dashboard

Hedge-fund style trading dashboard with:
- Risk Status + Signal Status separation
- Live terminal output during runs  
- Execution-ready trade cards
- PASS diagnostics

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

# Premium CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* Global */
.stApp {
    background: linear-gradient(180deg, #0a0e14 0%, #0d1117 100%);
}

/* Title with gradient */
.main-title {
    font-family: 'Inter', sans-serif;
    font-size: 2.8rem;
    font-weight: 800;
    background: linear-gradient(135deg, #00d9ff 0%, #7c3aed 50%, #f472b6 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: -0.02em;
    margin: 0;
}

/* Status cards */
.status-section {
    background: linear-gradient(135deg, rgba(26, 32, 41, 0.9) 0%, rgba(26, 32, 41, 0.5) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 16px;
    padding: 24px;
    margin: 8px 0;
}

/* Risk status */
.risk-status-green {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.25) 0%, rgba(16, 185, 129, 0.1) 100%);
    border: 2px solid rgba(16, 185, 129, 0.5);
    border-radius: 16px;
    padding: 20px 32px;
    text-align: center;
}

.risk-status-red {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.25) 0%, rgba(239, 68, 68, 0.1) 100%);
    border: 2px solid rgba(239, 68, 68, 0.5);
    border-radius: 16px;
    padding: 20px 32px;
    text-align: center;
}

/* Signal status pills */
.signal-trade {
    background: linear-gradient(135deg, #10b981 0%, #059669 100%);
    color: white;
    padding: 12px 28px;
    border-radius: 30px;
    font-family: 'Inter', sans-serif;
    font-size: 16px;
    font-weight: 700;
    display: inline-block;
    box-shadow: 0 4px 20px rgba(16, 185, 129, 0.4);
}

.signal-pass {
    background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
    color: white;
    padding: 12px 28px;
    border-radius: 30px;
    font-family: 'Inter', sans-serif;
    font-size: 16px;
    font-weight: 700;
    display: inline-block;
    box-shadow: 0 4px 20px rgba(239, 68, 68, 0.4);
}

.signal-none {
    background: linear-gradient(135deg, #6b7280 0%, #4b5563 100%);
    color: white;
    padding: 12px 28px;
    border-radius: 30px;
    font-family: 'Inter', sans-serif;
    font-size: 16px;
    font-weight: 700;
    display: inline-block;
}

/* Trade card */
.trade-card {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(16, 185, 129, 0.05) 100%);
    border: 2px solid rgba(16, 185, 129, 0.4);
    border-left: 6px solid #10b981;
    border-radius: 16px;
    padding: 28px;
    margin: 20px 0;
}

.trade-card-header {
    font-family: 'Inter', sans-serif;
    font-size: 1.5rem;
    font-weight: 700;
    color: #10b981;
    margin-bottom: 4px;
}

.trade-card-subheader {
    font-size: 14px;
    color: #8b95a5;
    margin-bottom: 20px;
}

/* Order ticket */
.order-ticket {
    background: linear-gradient(135deg, #0d1117 0%, #1a2029 100%);
    border: 1px solid rgba(0, 217, 255, 0.3);
    border-radius: 12px;
    padding: 24px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 14px;
    line-height: 2;
}

.order-ticket pre {
    margin: 0;
    color: #e6e8eb;
}

.order-leg {
    color: #00d9ff;
    font-weight: 600;
}

/* Terminal output */
.terminal {
    background: #0d1117;
    border: 1px solid rgba(0, 217, 255, 0.2);
    border-radius: 12px;
    padding: 16px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: #8b95a5;
    max-height: 300px;
    overflow-y: auto;
}

.terminal-line {
    margin: 4px 0;
}

.terminal-info { color: #00d9ff; }
.terminal-success { color: #10b981; }
.terminal-warning { color: #f59e0b; }
.terminal-error { color: #ef4444; }

/* Regime box */
.regime-display {
    background: linear-gradient(135deg, rgba(26, 32, 41, 0.95) 0%, rgba(26, 32, 41, 0.7) 100%);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 20px;
    padding: 32px;
    text-align: center;
}

.regime-name {
    font-family: 'Inter', sans-serif;
    font-size: 3rem;
    font-weight: 800;
    letter-spacing: 0.1em;
    margin-bottom: 12px;
}

.regime-bull { color: #10b981; }
.regime-bear { color: #ef4444; }
.regime-chop { color: #f59e0b; }
.regime-recovery { color: #3b82f6; }

/* Metrics */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(26, 32, 41, 0.8) 0%, rgba(26, 32, 41, 0.4) 100%);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    padding: 16px;
}

[data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
}

/* Expander styling */
[data-testid="stExpander"] {
    background: rgba(26, 32, 41, 0.5);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 12px;
}

/* Button */
.stButton > button {
    background: linear-gradient(135deg, #00d9ff 0%, #7c3aed 100%);
    color: white;
    font-family: 'Inter', sans-serif;
    font-weight: 600;
    padding: 14px 28px;
    border-radius: 12px;
    border: none;
    transition: all 0.3s ease;
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 30px rgba(0, 217, 255, 0.4);
}

/* Progress */
.stProgress > div > div {
    background: linear-gradient(90deg, #00d9ff 0%, #7c3aed 100%);
    border-radius: 10px;
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


def run_engine_with_output():
    """Run engine and capture output line by line."""
    script_path = Path(__file__).parent.parent / 'scripts' / 'run_daily.py'
    env = os.environ.copy()
    
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
    """Format as dollars."""
    if value is None:
        return "N/A"
    try:
        return f"${float(value):,.2f}"
    except:
        return "N/A"


def format_percent(value) -> str:
    """Format as percentage."""
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.0%}"
    except:
        return "N/A"


def get_signal_status(report: dict) -> tuple:
    """Determine signal status: TRADE, PASS, or NO_EDGE."""
    edges = report.get('edges', [])
    candidates = report.get('candidates', [])
    
    if not edges:
        return ('NO_EDGE', '⚪ NO EDGE', 'No edges detected today')
    
    trade_candidates = [c for c in candidates if c.get('recommendation') == 'TRADE']
    if trade_candidates:
        return ('TRADE', '🔥 TRADE', f'{len(trade_candidates)} actionable trade(s)')
    
    return ('PASS', '❌ PASS', 'Edge detected but structure failed')


def render_trade_card(candidate: dict):
    """Render premium TRADE card with execution details."""
    symbol = candidate['symbol']
    structure = candidate.get('structure') or {}
    edge = candidate.get('edge') or {}
    regime = candidate.get('regime') or {}
    sizing = candidate.get('sizing') or {}
    
    # Card header
    st.markdown(f"""
    <div class="trade-card">
        <div class="trade-card-header">🔥 TRADE — {symbol} {edge.get('type', '').upper()}</div>
        <div class="trade-card-subheader">
            Edge Strength: {format_percent(edge.get('strength', 0))} • 
            Regime: {regime.get('state', 'unknown').upper()} ({format_percent(regime.get('confidence', 0))})
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Build order ticket
    legs = structure.get('legs', [])
    struct_type = structure.get('type', 'Unknown Structure')
    expiration = structure.get('expiration', 'N/A')
    dte = structure.get('dte', 'N/A')
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("##### 📋 Order Ticket (Copy to Broker)")
        
        if legs:
            ticket_lines = [
                f"Strategy: {struct_type}",
                f"Exp: {expiration} ({dte} DTE)",
                "",
            ]
            
            for leg in legs:
                action = leg.get('action', 'BUY')
                qty = leg.get('quantity', 1)
                strike = leg.get('strike', 0)
                opt_type = leg.get('option_type', 'CALL')
                ticket_lines.append(f"{action}: {qty}x {symbol} {expiration} ${strike} {opt_type}")
            
            credit = structure.get('entry_credit_dollars', 0) or 0
            debit = structure.get('entry_debit_dollars', 0) or 0
            max_loss = structure.get('max_loss_dollars', 0) or 0
            contracts = sizing.get('recommended_contracts', 0) or 0
            total_risk = sizing.get('total_risk_dollars', 0) or 0
            
            ticket_lines.append("")
            if credit > 0:
                ticket_lines.append(f"Credit: {format_dollars(credit)}")
            elif debit > 0:
                ticket_lines.append(f"Debit: {format_dollars(debit)}")
            ticket_lines.append(f"Max Loss: {format_dollars(max_loss)} per contract")
            ticket_lines.append(f"Contracts: {contracts}")
            ticket_lines.append(f"Total Risk: {format_dollars(total_risk)}")
            
            st.code('\n'.join(ticket_lines), language=None)
        else:
            st.warning("⚠️ Structure details not available")
    
    with col2:
        st.markdown("##### 📊 Key Metrics")
        st.metric("Regime Fit", regime.get('state', 'N/A').upper())
        st.metric("Edge Type", edge.get('type', 'N/A'))
        st.metric("Risk", format_dollars(sizing.get('total_risk_dollars', 0)))
    
    # Expandable details
    with st.expander("💡 Why This Trade", expanded=False):
        st.markdown(f"**Edge Rationale:**")
        st.info(edge.get('rationale', 'No rationale available'))
        st.markdown(f"**Structure Rationale:**")
        st.info(candidate.get('rationale', 'No rationale available'))
    
    with st.expander("⚠️ Execution Checklist", expanded=False):
        st.markdown("""
        **Before Placing Order:**
        - [ ] Check current bid/ask spread (< 20% of credit)
        - [ ] Verify fills at mid or better
        - [ ] Confirm no earnings/events before expiration
        - [ ] Total risk within daily budget
        
        **Do NOT Trade If:**
        - ❌ Spread > 30% of credit
        - ❌ Volume < 100 on any leg
        - ❌ Market moving > 1% during placement
        """)


def render_pass_card(candidate: dict):
    """Render PASS card with diagnostics."""
    symbol = candidate['symbol']
    reason = candidate.get('rationale', 'No valid structure found')
    diagnostics = candidate.get('pass_diagnostics', [])
    edge = candidate.get('edge') or {}
    
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, rgba(239, 68, 68, 0.03) 100%);
                border: 1px solid rgba(239, 68, 68, 0.2);
                border-left: 6px solid #ef4444;
                border-radius: 16px;
                padding: 24px;
                margin: 16px 0;">
        <div style="font-size: 1.3rem; font-weight: 700; color: #ef4444; margin-bottom: 8px;">
            ❌ PASS — {symbol}
        </div>
        <div style="color: #f87171; font-size: 14px;">
            {reason}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if diagnostics:
        with st.expander("🔍 What Was Tried", expanded=False):
            for d in diagnostics:
                cols = st.columns(4)
                cols[0].metric("Structure", d.get('structure_type', 'N/A'))
                cols[1].metric("Width", f"{d.get('width_points', 0)} pts")
                cols[2].metric("DTE", d.get('expiration_dte', 'N/A'))
                cols[3].metric("Failed", d.get('failure_reason', 'N/A')[:15])


def main():
    # ═══════════════════════════════════════════════════════════
    # HEADER
    # ═══════════════════════════════════════════════════════════
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<h1 class="main-title">◉ VOLMACHINE DESK</h1>', unsafe_allow_html=True)
        st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    with col2:
        run_button = st.button("🚀 Run Engine", type="primary", use_container_width=True)
    
    # Handle engine run with live output
    if run_button:
        st.markdown("---")
        st.markdown("### 🖥️ Engine Output")
        
        terminal_placeholder = st.empty()
        progress_bar = st.progress(0)
        
        process = run_engine_with_output()
        output_lines = []
        
        try:
            for line in process.stdout:
                line = line.strip()
                if line:
                    output_lines.append(line)
                    # Color code output
                    display_lines = []
                    for l in output_lines[-20:]:  # Last 20 lines
                        if 'ERROR' in l.upper():
                            display_lines.append(f'<span class="terminal-error">{l}</span>')
                        elif 'WARNING' in l.upper():
                            display_lines.append(f'<span class="terminal-warning">{l}</span>')
                        elif 'INFO' in l.upper() or '===' in l:
                            display_lines.append(f'<span class="terminal-info">{l}</span>')
                        elif '✅' in l or 'success' in l.lower():
                            display_lines.append(f'<span class="terminal-success">{l}</span>')
                        else:
                            display_lines.append(l)
                    
                    terminal_placeholder.markdown(
                        f'<div class="terminal">{"<br>".join(display_lines)}</div>',
                        unsafe_allow_html=True
                    )
                    
                    # Update progress
                    if 'regime_classified' in l:
                        progress_bar.progress(30)
                    elif 'edge' in l.lower():
                        progress_bar.progress(60)
                    elif 'completed' in l.lower():
                        progress_bar.progress(100)
            
            process.wait()
            
            if process.returncode == 0:
                st.success("✅ Engine run complete!")
                time.sleep(1)
                st.rerun()
            else:
                st.error("❌ Engine run failed")
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    # Load report
    report = load_latest_report()
    
    if not report:
        st.markdown("---")
        st.warning("📭 **No report data.** Click **Run Engine** to generate today's analysis.")
        st.info("💡 Make sure `POLYGON_API_KEY` is set in your environment.")
        return
    
    # ═══════════════════════════════════════════════════════════
    # STATUS SECTION - Risk + Signal
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    # RISK STATUS
    with col1:
        st.markdown("#### 🛡️ Risk Status")
        trading_allowed = report.get('trading_allowed', True)
        
        if trading_allowed:
            st.markdown("""
            <div class="risk-status-green">
                <span style="font-size: 2rem;">🟢</span><br>
                <span style="font-size: 1.4rem; font-weight: 700; color: #10b981;">TRADING ALLOWED</span><br>
                <span style="font-size: 12px; color: #6ee7b7;">Risk systems are green</span>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="risk-status-red">
                <span style="font-size: 2rem;">🔴</span><br>
                <span style="font-size: 1.4rem; font-weight: 700; color: #ef4444;">TRADING BLOCKED</span><br>
                <span style="font-size: 12px; color: #fca5a5;">Kill switch or limits hit</span>
            </div>
            """, unsafe_allow_html=True)
            for reason in report.get('do_not_trade_reasons', []):
                st.error(f"⛔ {reason}")
    
    # SIGNAL STATUS
    with col2:
        st.markdown("#### 📡 Signal Status")
        signal_type, signal_label, signal_desc = get_signal_status(report)
        
        css_class = {
            'TRADE': 'signal-trade',
            'PASS': 'signal-pass', 
            'NO_EDGE': 'signal-none'
        }.get(signal_type, 'signal-none')
        
        st.markdown(f"""
        <div class="status-section" style="text-align: center; padding: 28px;">
            <div class="{css_class}">{signal_label}</div>
            <p style="margin-top: 12px; color: #8b95a5; font-size: 14px;">{signal_desc}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Report info
    st.caption(f"📅 Report: {report.get('report_date')} | Generated: {report.get('generated_at', '')[:19]}")
    
    # ═══════════════════════════════════════════════════════════
    # MARKET REGIME
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 📊 Market Regime")
    
    regime = report.get('regime', {})
    regime_state = regime.get('state', 'unknown').upper()
    regime_confidence = regime.get('confidence', 0)
    regime_rationale = regime.get('rationale', '')
    
    color_map = {
        'BULL': ('#10b981', 'regime-bull', '📈'),
        'BEAR': ('#ef4444', 'regime-bear', '📉'),
        'CHOP': ('#f59e0b', 'regime-chop', '🔀'),
        'RECOVERY': ('#3b82f6', 'regime-recovery', '🔄'),
    }
    color, css_class, emoji = color_map.get(regime_state, ('#8b95a5', '', '⚪'))
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(f"""
        <div class="regime-display">
            <div class="regime-name {css_class}" style="color: {color};">{emoji} {regime_state}</div>
            <div style="color: #8b95a5; font-size: 14px; max-width: 600px; margin: 0 auto;">{regime_rationale}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("##### Confidence")
        st.metric("", f"{regime_confidence:.0%}")
        st.progress(regime_confidence)
    
    # ═══════════════════════════════════════════════════════════
    # TRADE DECISIONS
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 💼 Trade Decisions")
    
    candidates = report.get('candidates', [])
    edges = report.get('edges', [])
    
    trade_candidates = [c for c in candidates if c.get('recommendation') == 'TRADE']
    pass_candidates = [c for c in candidates if c.get('recommendation') in ['PASS', 'REVIEW']]
    
    # Summary
    col1, col2, col3 = st.columns(3)
    col1.metric("Edges Detected", len(edges))
    col2.metric("🔥 TRADE", len(trade_candidates))
    col3.metric("❌ PASS", len(pass_candidates))
    
    st.markdown("")
    
    # TRADE cards
    if trade_candidates:
        for c in trade_candidates:
            render_trade_card(c)
    elif edges:
        st.warning("📭 **Edge detected but no valid structure.** See PASS diagnostics below.")
    else:
        st.info("📭 **No edges today.** Volatility metrics did not exceed thresholds. This is normal - walk away.")
    
    # PASS cards
    if pass_candidates:
        with st.expander("❌ PASS Details", expanded=False):
            for c in pass_candidates:
                render_pass_card(c)
    
    # ═══════════════════════════════════════════════════════════
    # EDGES TABLE
    # ═══════════════════════════════════════════════════════════
    if edges:
        st.markdown("---")
        with st.expander("📈 Raw Edges", expanded=False):
            for e in edges:
                cols = st.columns([1, 1, 1, 3])
                cols[0].markdown(f"**{e.get('symbol')}**")
                cols[1].markdown(f"{e.get('type')}")
                cols[2].markdown(f"{format_percent(e.get('strength', 0))}")
                cols[3].caption(e.get('rationale', '')[:100])
    
    # ═══════════════════════════════════════════════════════════
    # PORTFOLIO
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 💰 Portfolio")
    
    portfolio = report.get('portfolio', {})
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Open Positions", portfolio.get('positions_open', 0))
    col2.metric("Max Loss", format_dollars(portfolio.get('total_max_loss_dollars', 0)))
    col3.metric("Realized P&L", format_dollars(portfolio.get('realized_pnl_today_dollars', 0)))
    col4.metric("Unrealized", format_dollars(portfolio.get('unrealized_pnl_dollars', 0)))
    
    if portfolio.get('kill_switch_active'):
        st.error(f"🚨 KILL SWITCH: {portfolio.get('kill_switch_reason')}")


if __name__ == "__main__":
    main()
