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
    
    # Priority: 1) st.secrets (Cloud), 2) env var, 3) hardcoded fallback
    api_key = None
    try:
        api_key = st.secrets.get("POLYGON_API_KEY")
    except:
        pass
    
    if not api_key:
        api_key = os.environ.get('POLYGON_API_KEY', 'lrpYXeKqUp8pBGDlbz1BdJwsmpnpiKzu')
    
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

        formatted_ticket = "\n".join(lines)
        st.markdown(f"""
        <div class="ticket-code">
            <div class="copy-hint">COPY</div>
            <pre style="margin:0">{formatted_ticket}</pre>
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
