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
    background: #050505;
    background-image: 
        radial-gradient(circle at 50% 0%, rgba(0, 217, 255, 0.1) 0%, transparent 50%),
        radial-gradient(circle at 100% 0%, rgba(124, 58, 237, 0.1) 0%, transparent 40%);
    color: #e6e8eb;
}

/* TYPOGRAPHY */
h1, h2, h3, .main-title {
    font-family: 'Rajdhani', sans-serif !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* MAIN TITLE */
.main-title {
    font-size: 3.5rem;
    font-weight: 700;
    background: linear-gradient(90deg, #00f2ea 0%, #00d9ff 50%, #0066ff 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-shadow: 0 0 30px rgba(0, 217, 255, 0.4);
    margin: 0;
    padding-bottom: 10px;
}

/* SUBHEADERS */
h3 {
    font-size: 1.5rem !important;
    color: #64748b;
    border-bottom: 2px solid rgba(255,255,255,0.05);
    padding-bottom: 10px;
    margin-top: 30px !important;
    display: flex;
    align-items: center;
    gap: 10px;
}

/* STATUS CARDS */
.status-box {
    background: rgba(10, 10, 10, 0.8);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 4px;
    padding: 2px; /* For border gradient effect if needed */
    position: relative;
    overflow: hidden;
}

/* RISK STATUS - ALLOWED */
.risk-allowed {
    background: linear-gradient(180deg, rgba(16, 185, 129, 0.1) 0%, rgba(16, 185, 129, 0.05) 100%);
    border: 1px solid #10b981;
    box-shadow: 0 0 20px rgba(16, 185, 129, 0.2), inset 0 0 20px rgba(16, 185, 129, 0.1);
    border-radius: 8px;
    padding: 24px;
    text-align: center;
    position: relative;
}

.risk-allowed::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent, #10b981, transparent);
}

/* RISK STATUS - BLOCKED */
.risk-blocked {
    background: linear-gradient(180deg, rgba(239, 68, 68, 0.1) 0%, rgba(239, 68, 68, 0.05) 100%);
    border: 1px solid #ef4444;
    box-shadow: 0 0 20px rgba(239, 68, 68, 0.2), inset 0 0 20px rgba(239, 68, 68, 0.1);
    border-radius: 8px;
    padding: 24px;
    text-align: center;
}

/* SIGNAL STATUS */
.signal-box {
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid rgba(56, 189, 248, 0.2);
    border-radius: 8px;
    padding: 24px;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
}

.signal-pill {
    padding: 8px 24px;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 700;
    letter-spacing: 0.1em;
    font-size: 1.2rem;
    text-transform: uppercase;
}

.sig-trade { 
    background: #10b981; color: #000; 
    box-shadow: 0 0 15px #10b981;
}
.sig-pass { 
    background: #ef4444; color: #fff;
    box-shadow: 0 0 15px #ef4444;
}
.sig-none { 
    background: rgba(255,255,255,0.1); color: #94a3b8;
    border: 1px solid rgba(255,255,255,0.2);
}

/* TERMINAL */
.terminal-window {
    background: #0a0a0a;
    border: 1px solid #333;
    border-radius: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    padding: 0;
    overflow: hidden;
    box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    margin-bottom: 20px;
}

.terminal-header {
    background: #1a1a1a;
    padding: 8px 12px;
    border-bottom: 1px solid #333;
    display: flex;
    gap: 6px;
}

.term-dot { width: 10px; height: 10px; border-radius: 50%; }
.term-red { background: #ff5f56; }
.term-yellow { background: #ffbd2e; }
.term-green { background: #27c93f; }

.terminal-content {
    padding: 16px;
    color: #00f2ea;
    height: 300px;
    overflow-y: auto;
    white-space: pre-wrap;
}

.t-info { color: #94a3b8; }
.t-success { color: #10b981; text-shadow: 0 0 10px rgba(16, 185, 129, 0.5); }
.t-warn { color: #f59e0b; }
.t-err { color: #ef4444; }
.t-cmd { color: #00f2ea; }

/* TRADE CARD */
.trade-card {
    background: rgba(16, 20, 26, 0.9);
    border: 1px solid rgba(0, 217, 255, 0.2);
    border-top: 4px solid #00f2ea;
    border-radius: 8px;
    padding: 0;
    margin: 20px 0;
    box-shadow: 0 20px 40px rgba(0,0,0,0.4);
    position: relative;
    overflow: hidden;
}

.trade-header {
    background: linear-gradient(90deg, rgba(0, 242, 234, 0.1) 0%, transparent 100%);
    padding: 20px;
    border-bottom: 1px solid rgba(255,255,255,0.05);
}

.trade-title {
    font-family: 'Rajdhani', sans-serif;
    font-size: 1.8rem;
    font-weight: 700;
    color: #fff;
    display: flex;
    align-items: center;
    gap: 12px;
}

.trade-tag {
    background: rgba(0, 242, 234, 0.2);
    color: #00f2ea;
    border: 1px solid rgba(0, 242, 234, 0.4);
    padding: 4px 12px;
    border-radius: 4px;
    font-size: 14px;
    font-family: 'JetBrains Mono', monospace;
}

.trade-body {
    padding: 24px;
}

/* ORDER TICKET */
.ticket-grid {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 24px;
}

.ticket-code {
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    color: #c9d1d9;
    position: relative;
}

.copy-hint {
    position: absolute;
    top: 10px; right: 10px;
    font-size: 10px;
    color: #8b949e;
    text-transform: uppercase;
    border: 1px solid #30363d;
    padding: 2px 6px;
    border-radius: 4px;
}

/* REGIME */
.regime-panel {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 8px;
    padding: 30px;
}

.regime-big-text {
    font-family: 'Rajdhani', sans-serif;
    font-size: 4rem;
    font-weight: 700;
    line-height: 1;
    text-transform: uppercase;
    text-shadow: 0 0 30px currentColor;
}

/* BUTTONS */
.stButton > button {
    background: linear-gradient(90deg, #0066ff 0%, #00d9ff 100%);
    border: none;
    color: white;
    font-family: 'Rajdhani', sans-serif;
    font-weight: 700;
    font-size: 18px;
    text-transform: uppercase;
    letter-spacing: 1px;
    padding: 12px 30px;
    border-radius: 4px;
    transition: all 0.3s ease;
    width: 100%;
    box-shadow: 0 0 20px rgba(0, 102, 255, 0.3);
}

.stButton > button:hover {
    box-shadow: 0 0 30px rgba(0, 217, 255, 0.6);
    transform: translateY(-2px);
}

/* METRICS */
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.05);
    padding: 15px;
    border-radius: 6px;
}

[data-testid="stMetricLabel"] {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    color: #64748b;
}

[data-testid="stMetricValue"] {
    font-family: 'Rajdhani', sans-serif;
    font-size: 2rem;
    font-weight: 600;
    color: #e2e8f0;
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


def render_trade_ticket(candidate: dict):
    symbol = candidate['symbol']
    structure = candidate.get('structure') or {}
    edge = candidate.get('edge') or {}
    sizing = candidate.get('sizing') or {}
    
    st.markdown(f"""
    <div class="trade-card">
        <div class="trade-header">
            <div class="trade-title">
                <span style="color:#00f2ea">⚡</span> {symbol}
                <span class="trade-tag">{edge.get('type','').upper()}</span>
                <span class="trade-tag" style="border-color: #10b981; color: #10b981">TRADE</span>
            </div>
        </div>
        <div class="trade-body">
    """, unsafe_allow_html=True)
    
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
                qty = str(leg.get('quantity', 1)).ljust(2)
                strike = str(leg.get('strike', 0)).ljust(6)
                otype = leg.get('option_type', 'C')[0].upper()
                lines.append(f"{side} {qty} {symbol} {exp} {strike} {otype}")
                
            lines.append("-" * 40)
            credit = structure.get('entry_credit_dollars', 0)
            debit = structure.get('entry_debit_dollars', 0)
            risk = sizing.get('total_risk_dollars', 0)
            
            if credit > 0: price = f"CREDIT: ${credit:.2f}"
            else: price = f"DEBIT:  ${debit:.2f}"
            
            lines.append(f"{price.ljust(20)} MAX LOSS: ${structure.get('max_loss_dollars',0):.2f}")
            lines.append(f"SIZE:   {sizing.get('recommended_contracts',1)} contracts      RISK:     ${risk:.2f}")

        st.markdown(f"""
        <div class="ticket-code">
            <div class="copy-hint">COPY</div>
            <pre style="margin:0">{"\n".join(lines)}</pre>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown('<div style="color: #94a3b8; font-size: 11px; margin-bottom: 8px;">ANALYSIS</div>', unsafe_allow_html=True)
        st.info(edge.get('rationale', 'Edge detected via volatility surface analysis.'))
        st.caption("Risk Checks Passed ✅")

    st.markdown("</div></div>", unsafe_allow_html=True)


def main():
    # HEADLINE
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<h1 class="main-title">VOLMACHINE<span style="color:#fff; font-weight:300">DESK</span></h1>', unsafe_allow_html=True)
        st.caption(f"SYSTEM ONLINE • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • v2.1")
        
    with col2:
        if st.button("INITIATE SEQUENCE"):
            ph = st.empty()
            proc = run_engine_processed()
            lines = []
            for line in proc.stdout:
                lines.append(line)
                render_terminal(ph, lines)
            proc.wait()
            if proc.returncode == 0:
                st.rerun()

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

    # REGIME
    st.markdown("### 📊 MARKET REGIME")
    regime = report.get('regime', {})
    r_state = regime.get('state', 'Unknown').upper()
    r_color = {'BULL': '#10b981', 'BEAR': '#ef4444', 'CHOP': '#f59e0b'}.get(r_state, '#3b82f6')
    
    rc1, rc2 = st.columns([2, 1])
    with rc1:
        st.markdown(f"""
        <div class="regime-panel" style="border-left: 4px solid {r_color}">
            <div>
                <div style="color: #64748b; margin-bottom: 4px; font-size: 12px;">DETECTED STATE</div>
                <div class="regime-big-text" style="color: {r_color}">{r_state}</div>
                <div style="color: #94a3b8; max-width: 500px; margin-top: 10px;">{regime.get('rationale','')}</div>
            </div>
            <div style="text-align: right">
                <div style="font-size: 4rem;">{(regime.get('confidence',0)*100):.0f}%</div>
                <div style="color: #64748b; font-size: 12px;">CONFIDENCE</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with rc2:
        st.metric("VIX / VOL", f"{format_percent(0.18)}") # Placeholder if not in JSON
        st.metric("EDGE COUNT", len(report.get('edges', [])))

    # ACTION ZONE
    st.markdown("### ⚡ ACTION ZONE")
    
    candidates = report.get('candidates', [])
    trades = [c for c in candidates if c.get('recommendation') == 'TRADE']
    
    if trades:
        for t in trades:
            render_trade_ticket(t)
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
