"""
Edge Components - Streamlit UI Components for Edge Portfolio

Renders edge cards, candidate tables, and detail pages.
Uses data from EdgeRegistry (no signal computation here).
"""

import streamlit as st
from typing import List, Optional
from datetime import datetime

from ui.edge_registry import EdgeData, EdgeRegistry, get_edge_registry


def render_edge_card(edge: EdgeData):
    """
    Render a single edge card for the portfolio view.
    
    Shows:
    - Edge name and status
    - Today's candidate count
    - Performance metrics (Phase1/Phase2)
    - Regime gate info
    """
    with st.container():
        # Card styling with border
        st.markdown(f"""
        <div style="
            border: 2px solid {'#22c55e' if edge.candidate_count > 0 else '#64748b'};
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 16px;
            background: rgba(30, 41, 59, 0.4);
        ">
        """, unsafe_allow_html=True)
        
        # Header
        col1, col2 = st.columns([3, 1])
        with col1:
            status_color = {"LIVE": "🟢", "LOCKED": "🔒", "RESEARCH": "🔬"}.get(edge.status, "⚪")
            st.markdown(f"### {status_color} {edge.edge_id.upper()} {edge.version}")
        with col2:
            if edge.candidate_count > 0:
                st.markdown(f"<div style='text-align: right; font-size: 2rem; color: #22c55e;'>{edge.candidate_count}</div>", unsafe_allow_html=True)
                st.caption("candidates today")
            else:
                st.markdown("<div style='text-align: right; color: #64748b;'>—</div>", unsafe_allow_html=True)
                st.caption("no signals")
        
        # Metrics row
        if edge.snapshot:
            p1 = edge.snapshot.get('phase1', {})
            p2 = edge.snapshot.get('phase2', {})
            
            mcol1, mcol2, mcol3, mcol4 = st.columns(4)
            with mcol1:
                st.metric("Trades", p1.get('trades', '—'))
            with mcol2:
                wr = p1.get('wr', 0)
                st.metric("Win Rate", f"{wr*100:.0f}%" if wr else "—")
            with mcol3:
                pf = p1.get('pf', 0)
                st.metric("Profit Factor", f"{pf:.2f}" if pf else "—")
            with mcol4:
                dd = p1.get('max_dd_pct', 0)
                st.metric("Max DD", f"{dd*100:.1f}%" if dd else "—")
        
        # Regime gate
        if edge.regime_gate:
            ivp_gate = edge.regime_gate.get('max_atm_iv_pctl')
            if ivp_gate:
                st.caption(f"📊 Regime Gate: IVp ≤ {ivp_gate}")
        
        # Notes
        if edge.snapshot and edge.snapshot.get('notes'):
            with st.expander("Details"):
                for note in edge.snapshot['notes']:
                    st.markdown(f"• {note}")
        
        st.markdown("</div>", unsafe_allow_html=True)


def render_portfolio_page(registry: EdgeRegistry):
    """
    Render the main portfolio page showing all edges.
    """
    st.title("🎯 Edge Portfolio")
    
    edges = registry.discover_edges()
    
    # Summary metrics
    total_candidates = registry.get_total_candidates()
    active_edges = registry.get_active_edges()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Edges", len(edges))
    with col2:
        st.metric("Active Today", len(active_edges))
    with col3:
        st.metric("Total Candidates", total_candidates)
    
    st.divider()
    
    # Edge cards
    if not edges:
        st.warning("No edges discovered. Check docs/edges/ and logs/edges/ directories.")
    else:
        for edge in edges:
            render_edge_card(edge)


def render_candidate_table(edge: EdgeData):
    """
    Render candidates table for an edge.
    """
    if not edge.signals or not edge.signals.get('candidates'):
        st.info("No candidates for this date.")
        return
    
    candidates = edge.signals['candidates']
    
    # Build table data
    rows = []
    for c in candidates:
        structure = c.get('structure', {})
        metrics = c.get('metrics', {})
        rows.append({
            'Symbol': c.get('symbol'),
            'IVp': f"{metrics.get('atm_iv_percentile', 0):.0f}",
            'Skew %ile': f"{metrics.get('skew_percentile', 0):.0f}",
            'Type': structure.get('type', '').replace('_', ' ').title(),
            'Expiry': structure.get('expiry', ''),
            'Entry': f"${structure.get('entry_debit', 0):.2f}",
            'Max Loss': f"${structure.get('max_loss', 0):.0f}",
            'Max Profit': f"${structure.get('max_profit', 0):.0f}",
        })
    
    st.dataframe(rows, use_container_width=True)


def render_edge_detail_page(edge: EdgeData):
    """
    Render detailed edge page.
    
    Sections:
    1. Edge documentation (markdown)
    2. Today's candidates
    3. Performance snapshot
    4. Universe & gating
    """
    st.title(f"📈 Edge: {edge.edge_id.upper()} {edge.version}")
    
    # Status and summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Status", edge.status)
    with col2:
        st.metric("Candidates Today", edge.candidate_count)
    with col3:
        if edge.signals:
            exec_date = edge.signals.get('execution_date', 'Unknown')
            st.metric("Execution Date", exec_date)
    
    st.divider()
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Specification", "🎯 Today's Candidates", "📊 Performance", "🔧 Config"])
    
    with tab1:
        if edge.doc_content:
            st.markdown(edge.doc_content)
        else:
            st.warning("No documentation found for this edge.")
    
    with tab2:
        render_candidate_table(edge)
    
    with tab3:
        if edge.snapshot:
            st.subheader("Phase 1 (Edge Existence)")
            p1 = edge.snapshot.get('phase1', {})
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Trades", p1.get('trades', '—'))
            with col2:
                st.metric("Win Rate", f"{p1.get('wr', 0)*100:.1f}%")
            with col3:
                st.metric("Profit Factor", f"{p1.get('pf', 0):.2f}")
            with col4:
                st.metric("Max DD", f"{p1.get('max_dd_pct', 0)*100:.1f}%")
            
            st.subheader("Phase 2 (Tradeability)")
            p2 = edge.snapshot.get('phase2', {})
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Trades", p2.get('trades', '—'))
            with col2:
                st.metric("Win Rate", f"{p2.get('wr', 0)*100:.1f}%")
            with col3:
                st.metric("Profit Factor", f"{p2.get('pf', 0):.2f}")
            with col4:
                st.metric("Max DD", f"{p2.get('max_dd_pct', 0)*100:.1f}%")
        else:
            st.info("No performance snapshot available.")
    
    with tab4:
        if edge.signals:
            st.subheader("Universe")
            universe = edge.signals.get('universe', [])
            st.write(", ".join(universe))
            
            st.subheader("Regime Gate")
            gate = edge.signals.get('regime_gate', {})
            for key, value in gate.items():
                st.write(f"**{key}**: {value}")
        
        if edge.snapshot and edge.snapshot.get('config'):
            st.subheader("Risk Parameters")
            config = edge.snapshot['config']
            for key, value in config.items():
                if key != 'regime_gate':  # Already shown above
                    st.write(f"**{key}**: {value}")


def render_edge_sidebar(registry: EdgeRegistry) -> Optional[str]:
    """
    Render sidebar navigation for edges.
    
    Returns selected page: 'portfolio' or edge_id.
    """
    st.sidebar.title("📊 Navigation")
    
    edges = registry.discover_edges()
    
    # Main pages
    page = st.sidebar.radio(
        "View",
        ["🎯 Portfolio"] + [f"📈 {e.edge_id.upper()}" for e in edges],
        key="nav_page"
    )
    
    if page == "🎯 Portfolio":
        return "portfolio"
    else:
        # Extract edge_id from "📈 FLAT" -> "flat"
        edge_id = page.split(" ", 1)[1].lower()
        return edge_id
