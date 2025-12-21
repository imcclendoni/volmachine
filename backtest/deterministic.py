"""
Deterministic Backtester for VolMachine.

Simulates trades using historical Polygon data with configurable exit rules.
Designed for audit-grade reproducibility with no lookahead bias.
"""

import hashlib
import json
import yaml
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import glob

from .result import BacktestTrade, BacktestMetrics, BacktestResult, ExitReason
from .fill_model import (
    FillConfig, 
    calculate_entry_fill, 
    calculate_exit_fill, 
    calculate_realized_pnl,
    calculate_strict_entry_fill,
    calculate_strict_exit_fill,
)
from portfolio.risk_engine import RiskEngine, RiskConfig, TradeCandidate, RejectionReason


class DeterministicBacktester:
    """
    Deterministic backtester for options strategies.
    
    Key principles:
    - No lookahead bias: signals use prior-day data only
    - Reproducible: same config + dates = same results
    - Audit-grade: all intermediate data persisted
    """
    
    def __init__(
        self,
        config_path: str = './config/backtest.yaml',
        reports_dir: str = './logs/reports',
    ):
        """
        Initialize backtester.
        
        Args:
            config_path: Path to backtest config YAML
            reports_dir: Path to saved daily reports
        """
        self.config_path = Path(config_path)
        self.reports_dir = Path(reports_dir)
        
        # Load config
        self.config = self._load_config()
        self.fill_config = FillConfig.from_yaml(config_path)
        
        # Import data layer (lazy to avoid circular imports)
        from data.polygon_backtest import get_option_daily_bars, get_underlying_daily_bars
        self.get_option_bars = get_option_daily_bars
        self.get_underlying_bars = get_underlying_daily_bars
    
    def _load_config(self) -> Dict[str, Any]:
        """Load backtest configuration."""
        if not self.config_path.exists():
            return self._default_config()
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _default_config(self) -> Dict[str, Any]:
        """Default backtest configuration."""
        return {
            'exit_rules': {
                'credit_spread': {
                    'take_profit_pct': 50,
                    'stop_loss_mult': 2.0,
                    'time_stop_dte': 5,
                },
                'debit_spread': {
                    'take_profit_pct': 100,
                    'stop_loss_pct': 50,
                    'time_stop_dte': 5,
                },
            },
            'defaults': {
                'lookback_days': 90,
                'min_edge_strength': 0.50,
                'trade_only': True,
                'symbols': ['SPY', 'QQQ', 'IWM', 'TLT'],
            },
        }
    
    def run_range(
        self,
        start_date: date,
        end_date: date,
        symbols: Optional[List[str]] = None,
        signals_source: str = 'saved_reports',
        portfolio_mode_override: Optional[str] = None,
        edge_slice: str = 'both',  # 'steep', 'flat', or 'both'
        max_iv: Optional[float] = None,  # Max ATM IV filter (e.g., 0.30)
        max_atm_iv_pctl: Optional[float] = None,  # Max ATM IV percentile (e.g., 80)
    ) -> BacktestResult:
        """
        Run backtest over a date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            symbols: Symbols to include (None = all from config)
            signals_source: 'saved_reports' or 'regenerate'
            
        Returns:
            BacktestResult with all trades and metrics
        """
        print(f"\n=== Backtest: {start_date} to {end_date} ===\n")
        
        symbols = symbols or self.config.get('defaults', {}).get('symbols', ['SPY'])
        min_strength = self.config.get('defaults', {}).get('min_edge_strength', 0.50)
        trade_only = self.config.get('defaults', {}).get('trade_only', True)
        
        # Config-driven regime gate (CLI arg takes precedence)
        if max_atm_iv_pctl is None:
            regime_gate = self.config.get('regime_gate', {})
            if edge_slice == 'flat':
                max_atm_iv_pctl = regime_gate.get('flat', {}).get('max_atm_iv_pctl')
                # FLAT production REQUIRES regime gate - enforce it
                if max_atm_iv_pctl is None:
                    raise ValueError(
                        "FLAT production requires regime gate. "
                        "Set regime_gate.flat.max_atm_iv_pctl in config or use --max-atm-iv-pctl CLI arg."
                    )
            elif edge_slice == 'steep':
                max_atm_iv_pctl = regime_gate.get('steep', {}).get('max_atm_iv_pctl')
                # STEEP gate is optional (not yet validated)
        
        # Load signals from saved reports
        signals, drop_counts = self._load_signals(start_date, end_date, symbols, min_strength, trade_only)
        
        # Apply edge slice filter (steep/flat)
        # STEEP = direction=SHORT (credit spread, selling puts)
        # FLAT = direction=LONG (debit spread, buying puts)
        edge_slice_filtered = 0
        if edge_slice != 'both':
            original_count = len(signals)
            if edge_slice == 'steep':
                # Include signals where direction=SHORT or spread_type=credit
                signals = [s for s in signals if 
                    s.get('candidate', {}).get('edge', {}).get('direction') == 'SHORT' or
                    s.get('candidate', {}).get('structure', {}).get('spread_type') == 'credit']
            elif edge_slice == 'flat':
                # Include signals where direction=LONG or spread_type=debit
                signals = [s for s in signals if 
                    s.get('candidate', {}).get('edge', {}).get('direction') == 'LONG' or
                    s.get('candidate', {}).get('structure', {}).get('spread_type') == 'debit']
            edge_slice_filtered = original_count - len(signals)
        
        # Apply max IV filter (regime filter)
        iv_filtered = 0
        iv_excluded_signals = []
        if max_iv is not None:
            original_count = len(signals)
            filtered_signals = []
            for s in signals:
                # atm_iv is stored in candidate.edge.metrics.atm_iv
                atm_iv = s.get('candidate', {}).get('edge', {}).get('metrics', {}).get('atm_iv', 0)
                if atm_iv is None:
                    atm_iv = 0
                if atm_iv <= max_iv:
                    filtered_signals.append(s)
                else:
                    iv_excluded_signals.append({
                        'symbol': s.get('symbol'),
                        'date': s.get('signal_date', ''),
                        'atm_iv': atm_iv,
                    })
            signals = filtered_signals
            iv_filtered = original_count - len(signals)
        
        # Apply IV percentile filter (asset-independent regime filter)
        iv_pctl_filtered = 0
        iv_pctl_invalid = 0  # Track invalid IVp separately
        iv_pctl_excluded_signals = []
        iv_pctl_invalid_signals = []
        if max_atm_iv_pctl is not None:
            original_count = len(signals)
            filtered_signals = []
            for s in signals:
                # atm_iv_percentile is stored in candidate.edge.metrics.atm_iv_percentile
                atm_iv_pctl = s.get('candidate', {}).get('edge', {}).get('metrics', {}).get('atm_iv_percentile')
                
                # Validity check: reject None, ≤0, or >100 (invalid/missing data)
                if atm_iv_pctl is None or atm_iv_pctl <= 0 or atm_iv_pctl > 100:
                    iv_pctl_invalid_signals.append({
                        'symbol': s.get('candidate', {}).get('symbol'),
                        'date': s.get('signal_date', ''),
                        'atm_iv_pctl': atm_iv_pctl,
                        'reason': 'invalid' if atm_iv_pctl is None else f'{atm_iv_pctl:.1f} out of range'
                    })
                    continue
                
                # Apply threshold filter
                if atm_iv_pctl <= max_atm_iv_pctl:
                    filtered_signals.append(s)
                else:
                    iv_pctl_excluded_signals.append({
                        'symbol': s.get('candidate', {}).get('symbol'),
                        'date': s.get('signal_date', ''),
                        'atm_iv_pctl': atm_iv_pctl,
                    })
            signals = filtered_signals
            iv_pctl_invalid = len(iv_pctl_invalid_signals)
            iv_pctl_filtered = original_count - len(signals) - iv_pctl_invalid
        
        # Print drop accounting
        print(f"SIGNAL LOADING:")
        print(f"  Files found: {drop_counts['files_found']}")
        print(f"  Candidates parsed: {drop_counts['candidates_parsed']}")
        print(f"  Dropped:")
        if drop_counts['disabled_symbol'] > 0:
            print(f"    - Disabled symbol: {drop_counts['disabled_symbol']}")
        if drop_counts['not_in_enabled_symbols'] > 0:
            print(f"    - Not in enabled symbols: {drop_counts['not_in_enabled_symbols']}")
        if drop_counts['not_in_backtest_symbols'] > 0:
            print(f"    - Not in backtest symbols: {drop_counts['not_in_backtest_symbols']}")
        if drop_counts['recommendation_filtered'] > 0:
            print(f"    - Recommendation != TRADE: {drop_counts['recommendation_filtered']}")
        if drop_counts['strength_below_threshold'] > 0:
            print(f"    - Strength below threshold: {drop_counts['strength_below_threshold']}")
        if drop_counts['debit_filtered'] > 0:
            print(f"    - Debit spread filtered: {drop_counts['debit_filtered']}")
        if drop_counts['credit_filtered'] > 0:
            print(f"    - Credit spread filtered: {drop_counts['credit_filtered']}")
        if edge_slice_filtered > 0:
            print(f"    - Edge slice filter ({edge_slice}): {edge_slice_filtered}")
        if iv_filtered > 0:
            print(f"    - IV regime filter (atm_iv > {max_iv}): {iv_filtered}")
            for ex in iv_excluded_signals:
                print(f"        → {ex['symbol']} {ex['date']}: atm_iv={ex['atm_iv']:.2%}")
        if iv_pctl_filtered > 0:
            print(f"    - IV percentile filter (> {max_atm_iv_pctl}th): {iv_pctl_filtered}")
            for ex in iv_pctl_excluded_signals:
                print(f"        → {ex['symbol']} {ex['date']}: pctl={ex['atm_iv_pctl']:.1f}")
        if iv_pctl_invalid > 0:
            print(f"    - Invalid IVp (None/≤0/>100): {iv_pctl_invalid}")
            for ex in iv_pctl_invalid_signals:
                print(f"        → {ex['symbol']} {ex['date']}: {ex['reason']}")
        print(f"  Passed filters: {drop_counts['passed_filters'] - edge_slice_filtered - iv_filtered - iv_pctl_filtered - iv_pctl_invalid}")
        print(f"  Signals to simulate: {len(signals)}")
        print()
        
        if not signals:
            return self._empty_result(start_date, end_date, signals_source)
        
        # Check if using strict fills
        use_strict = self.config.get('slippage', {}).get('use_strict_fills', True)
        
        # Position limits config
        strategy_config = self.config.get('strategies', {}).get('skew_extreme', {})
        # Phase override takes precedence
        if portfolio_mode_override:
            portfolio_mode = portfolio_mode_override
        else:
            portfolio_mode = strategy_config.get('portfolio_mode', 'portfolio_safe')
        
        # Width config for risk estimation
        width_config = self.config.get('width_selection', {})
        max_risk_per_trade = width_config.get('max_risk_per_trade', 500)
        
        # Initialize RiskEngine for portfolio_safe mode
        risk_engine = None
        if portfolio_mode == 'signal_quality':
            # Unlimited for research/validation
            max_positions_per_symbol = 100
            max_concurrent_positions = 100
            max_total_risk_dollars = 50000
            cooldown_after_sl_days = 0
            print(f"📊 Mode: SIGNAL_QUALITY (no position limits)")
        else:
            # Use RiskEngine for live trading simulation
            re_config = self.config.get('risk_engine', {})
            risk_config = RiskConfig(
                # Equity-based sizing (new)
                initial_equity=re_config.get('initial_equity', 10000),
                risk_per_trade_pct=re_config.get('risk_per_trade_pct'),  # None = use fixed USD
                max_total_risk_pct=re_config.get('max_total_risk_pct'),  # None = use fixed USD
                # Base sizing (fallback)
                risk_per_trade_usd=re_config.get('risk_per_trade_usd', 500),
                max_open_positions=re_config.get('max_open_positions', 3),
                max_total_risk_usd=re_config.get('max_total_risk_usd', 1500),
                clusters=re_config.get('clusters', {'equity_etf': ['SPY', 'QQQ', 'IWM']}),
                max_cluster_risk_usd=re_config.get('max_cluster_risk_usd', 750),
                max_cluster_positions=re_config.get('max_cluster_positions', 1),
                cluster_dedup_mode=re_config.get('cluster_dedup_mode', 'best_edge'),
                cluster_risk_multiplier=re_config.get('cluster_risk_multiplier', 0.5),
                dd_kill_pct=re_config.get('dd_kill_pct', 0.10),
                symbol_cooldown_after_sl_days=re_config.get('symbol_cooldown_after_sl_days', 10),
                cluster_cooldown_after_sl_days=re_config.get('cluster_cooldown_after_sl_days', 5),
            )
            risk_engine = RiskEngine(risk_config)
            
            # Set legacy vars for backward compat (not used when risk_engine active)
            max_positions_per_symbol = 1
            max_concurrent_positions = risk_config.max_open_positions
            max_total_risk_dollars = risk_engine.get_max_total_risk()  # Use dynamic getter
            cooldown_after_sl_days = risk_config.symbol_cooldown_after_sl_days
            
            # Show sizing mode
            sizing_mode = "PCT" if risk_config.risk_per_trade_pct else "FIXED"
            risk_str = f"{risk_config.risk_per_trade_pct*100:.1f}%" if risk_config.risk_per_trade_pct else f"${risk_config.risk_per_trade_usd}"
            print(f"📊 Mode: PORTFOLIO_SAFE (RiskEngine: {sizing_mode} {risk_str}/trade, max {risk_config.max_open_positions} pos, cluster cap {risk_config.max_cluster_positions})")
        
        # Simulate signals
        trades = []
        unexecutable_count = 0
        skipped_no_data = 0
        
        # RiskEngine rejection tracking
        rejection_counts = {r: 0 for r in RejectionReason}
        
        if risk_engine is not None:
            # === PORTFOLIO_SAFE MODE: Use RiskEngine with date batching ===
            
            # Group signals by execution date
            from collections import defaultdict
            signals_by_date = defaultdict(list)
            for signal in signals:
                exec_date_str = signal.get('execution_date', signal.get('signal_date'))
                try:
                    exec_date = date.fromisoformat(exec_date_str)
                except:
                    exec_date = date.today()
                signals_by_date[exec_date].append(signal)
            
            # Process each date in order
            for exec_date in sorted(signals_by_date.keys()):
                day_signals = signals_by_date[exec_date]
                
                # Convert signals to TradeCandidate objects
                candidates = []
                signal_map = {}  # candidate -> original signal
                for signal in day_signals:
                    candidate_data = signal.get('candidate', {})
                    symbol = signal.get('symbol', candidate_data.get('symbol', ''))
                    edge = candidate_data.get('edge', {})
                    structure = candidate_data.get('structure', {})
                    
                    # ========================================
                    # COMPUTE REAL MAX LOSS FROM STRUCTURE LEGS
                    # ========================================
                    legs = structure.get('legs', [])
                    strikes = [leg.get('strike', 0) for leg in legs if leg.get('strike')]
                    if len(strikes) >= 2:
                        width_dollars = abs(max(strikes) - min(strikes))
                    else:
                        width_dollars = 0
                    
                    entry_credit = structure.get('entry_credit', 0)
                    spread_type = structure.get('spread_type', 'credit')
                    
                    if spread_type == 'credit' and width_dollars > 0:
                        # Credit spread: max_loss = (width - credit) * 100
                        computed_max_loss_usd = (width_dollars - entry_credit) * 100
                    else:
                        # Fallback to stored value or default
                        computed_max_loss_usd = abs(structure.get('max_loss_dollars', max_risk_per_trade))
                    
                    # Sanity check: max_loss should be positive and reasonable
                    if computed_max_loss_usd <= 0:
                        computed_max_loss_usd = max_risk_per_trade
                    
                    candidate = TradeCandidate(
                        symbol=symbol,
                        signal_date=date.fromisoformat(signal.get('signal_date', exec_date.isoformat())),
                        execution_date=exec_date,
                        edge_strength=edge.get('strength', 0.5),
                        edge_type=edge.get('type', 'unknown'),
                        structure_type=structure.get('type', 'credit_spread'),
                        max_loss_usd=computed_max_loss_usd,  # REAL RISK, not constant
                        credit_usd=entry_credit * 100 if entry_credit else None,
                    )
                    candidates.append(candidate)
                    signal_map[id(candidate)] = signal
                
                # Call RiskEngine
                approved, rejected = risk_engine.evaluate_candidates(candidates, exec_date)
                
                # Daily audit print
                if len(candidates) > 1:
                    print(f"\n  📅 {exec_date}: {len(candidates)} candidates")
                    for c in candidates:
                        cluster = risk_engine.get_cluster(c.symbol)
                        print(f"      {c.symbol} (edge={c.edge_strength:.2f}, cluster={cluster})")
                    print(f"    → Approved: {[a.candidate.symbol for a in approved]}")
                    print(f"    → Rejected: {[(r.candidate.symbol, r.reason.value) for r in rejected]}")
                
                # STRUCTURE AUDIT for rejected trades (Task 1: diagnose width issue)
                for rej in rejected:
                    if rej.reason.value == 'max_risk_per_trade_exceeded':
                        # Find original signal to get structure details
                        orig_signal = signal_map.get(id(rej.candidate))
                        if orig_signal:
                            structure = orig_signal.get('candidate', {}).get('structure', {})
                            legs = structure.get('legs', [])
                            strikes = [leg.get('strike', 0) for leg in legs]
                            if len(strikes) >= 2:
                                short_strike = max(strikes)  # For put spread, short is higher
                                long_strike = min(strikes)
                                width_pts = abs(short_strike - long_strike)
                                entry_credit = structure.get('entry_credit', 0)
                                computed_max_loss = (width_pts - entry_credit) * 100
                                print(f"      🔍 STRUCTURE AUDIT: {rej.candidate.symbol}")
                                print(f"         short_strike: {short_strike}, long_strike: {long_strike}")
                                print(f"         width_pts: ${width_pts:.2f}")
                                print(f"         entry_credit: ${entry_credit:.2f}")
                                print(f"         computed max_loss: ${computed_max_loss:.2f}")
                
                # Track rejections
                for rej in rejected:
                    rejection_counts[rej.reason] += 1
                
                # Simulate approved trades
                for approved_trade in approved:
                    original_signal = signal_map.get(id(approved_trade.candidate))
                    if not original_signal:
                        # Fallback: find signal by symbol match
                        for sig in day_signals:
                            if sig.get('symbol') == approved_trade.candidate.symbol:
                                original_signal = sig
                                break
                    
                    if not original_signal:
                        continue
                    
                    trade = self._simulate_trade(original_signal, use_strict=use_strict)
                    if trade is None:
                        skipped_no_data += 1
                    elif trade == 'unexecutable':
                        unexecutable_count += 1
                    else:
                        trades.append(trade)
                        print(f"  {trade.signal_date} {trade.symbol}: {trade.structure_type} -> "
                              f"${trade.net_pnl:.2f} ({trade.exit_reason.value})")
                        
                        # Get actual exit date from simulated trade
                        actual_exit_dt = trade.exit_date if isinstance(trade.exit_date, date) else date.fromisoformat(trade.exit_date)
                        
                        # Update RiskEngine: close position with actual exit date
                        risk_engine.close_position(trade.symbol, actual_exit_dt)
                        risk_engine.update_equity(trade.net_pnl)
                        
                        # Set cooldown if stop-loss
                        if trade.exit_reason == ExitReason.STOP_LOSS:
                            risk_engine.set_symbol_cooldown(trade.symbol, actual_exit_dt)
            
            # Print rejection summary
            print(f"\n📊 RISK ENGINE REJECTIONS:")
            for reason, count in rejection_counts.items():
                if count > 0:
                    print(f"  {reason.value}: {count}")
        
        else:
            # === SIGNAL_QUALITY MODE: No position limits ===
            # Track open positions for legacy mode (just for logging, not gating)
            open_positions = []
            cooldown_until = {}
            
            for signal in signals:
                symbol = signal.get('symbol')
                exec_date_str = signal.get('execution_date', signal.get('signal_date'))
                try:
                    exec_date = date.fromisoformat(exec_date_str)
                except:
                    exec_date = date.today()
                
                trade = self._simulate_trade(signal, use_strict=use_strict)
                if trade is None:
                    skipped_no_data += 1
                elif trade == 'unexecutable':
                    unexecutable_count += 1
                else:
                    trades.append(trade)
                    print(f"  {trade.signal_date} {trade.symbol}: {trade.structure_type} -> "
                          f"${trade.net_pnl:.2f} ({trade.exit_reason.value})")
        
        print(f"\nCompleted {len(trades)} trades")
        if unexecutable_count > 0:
            print(f"Unexecutable (bad bid/ask): {unexecutable_count}")
        if skipped_no_data > 0:
            print(f"Skipped (no data): {skipped_no_data}")
        if risk_engine is None:
            # Legacy stats for signal_quality mode
            pass
        
        # Build equity curve
        equity_curve = []
        cumulative_pnl = 0
        peak_equity = 0
        max_drawdown = 0
        
        # Sort trades by exit date for proper equity curve
        sorted_trades = sorted(trades, key=lambda t: t.exit_date if isinstance(t.exit_date, str) else t.exit_date.isoformat())
        
        for trade in sorted_trades:
            cumulative_pnl += trade.net_pnl
            peak_equity = max(peak_equity, cumulative_pnl)
            drawdown = peak_equity - cumulative_pnl
            max_drawdown = max(max_drawdown, drawdown)
            equity_curve.append({
                'date': trade.exit_date if isinstance(trade.exit_date, str) else trade.exit_date.isoformat(),
                'symbol': trade.symbol,
                'pnl': trade.net_pnl,
                'cumulative': cumulative_pnl,
                'peak': peak_equity,
                'drawdown': drawdown,
            })
        
        if trades:
            print(f"\n📈 Equity Curve: Final ${cumulative_pnl:.2f}, Max DD ${max_drawdown:.2f}")
        
        # Calculate metrics
        metrics = self._calculate_metrics(trades)
        
        # Build result
        result = BacktestResult(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            config_hash=self._hash_config(),
            trades=trades,
            metrics=metrics,
            generated_at=datetime.now().isoformat(),
            signals_source=signals_source,
            data_source='polygon',
            config_used=self.config,
        )
        
        return result
    
    def _load_signals(
        self,
        start_date: date,
        end_date: date,
        symbols: List[str],
        min_strength: float,
        trade_only: bool,
    ) -> tuple:
        """
        Load signals from saved daily reports.
        
        Applies strategy toggles and symbol gates from config.
        Returns (signals, drop_counts) for accounting.
        """
        signals = []
        
        # Drop accounting
        drop_counts = {
            'files_found': 0,
            'candidates_parsed': 0,
            'not_in_date_range': 0,
            'disabled_symbol': 0,
            'not_in_enabled_symbols': 0,
            'not_in_backtest_symbols': 0,
            'recommendation_filtered': 0,
            'strength_below_threshold': 0,
            'debit_filtered': 0,
            'credit_filtered': 0,
            'passed_filters': 0,
        }
        
        # Get strategy config
        strategies_config = self.config.get('strategies', {})
        skew_config = strategies_config.get('skew_extreme', {})
        
        # Strategy toggles
        enable_credit = skew_config.get('enable_credit_spread', True)
        enable_debit = skew_config.get('enable_debit_spread', True)
        
        # Symbol gates
        enabled_symbols = skew_config.get('enabled_symbols', symbols)
        disabled_symbols = skew_config.get('disabled_symbols', [])
        
        # Find all report files in range
        current = start_date
        while current <= end_date:
            # Try all filename patterns including backfill (support both old and new naming)
            date_str = current.isoformat()
            patterns = [
                self.reports_dir / f'{date_str}.json',
                self.reports_dir / f'{date_str}_open.json',
                self.reports_dir / f'{date_str}_close.json',
                self.reports_dir / f'{date_str}_backfill.json',  # Old format
            ]
            
            # Add new format: YYYY-MM-DD__SYMBOL__backfill.json using glob
            for p in self.reports_dir.glob(f'{date_str}__*__backfill.json'):
                patterns.append(p)
            
            for report_path in patterns:
                if not report_path.exists():
                    continue
                
                drop_counts['files_found'] += 1
                
                try:
                    with open(report_path, 'r') as f:
                        report = json.load(f)
                    
                    candidates = report.get('candidates', [])
                    
                    for candidate in candidates:
                        drop_counts['candidates_parsed'] += 1
                        symbol = candidate.get('symbol', '')
                        
                        # Filter by symbol (use strategy-specific gate if available)
                        if symbol in disabled_symbols:
                            drop_counts['disabled_symbol'] += 1
                            continue
                        if enabled_symbols and symbol not in enabled_symbols:
                            drop_counts['not_in_enabled_symbols'] += 1
                            continue
                        if symbol not in symbols:
                            drop_counts['not_in_backtest_symbols'] += 1
                            continue
                        
                        # Filter by recommendation
                        if trade_only and candidate.get('recommendation', '') != 'TRADE':
                            drop_counts['recommendation_filtered'] += 1
                            continue
                        
                        # Filter by edge strength
                        edge = candidate.get('edge', {})
                        strength = edge.get('strength', 0)
                        if strength < min_strength:
                            drop_counts['strength_below_threshold'] += 1
                            continue
                        
                        # Filter by structure type (strategy toggle)
                        structure = candidate.get('structure', {})
                        spread_type = structure.get('spread_type', structure.get('type', ''))
                        
                        if spread_type == 'credit' and not enable_credit:
                            drop_counts['credit_filtered'] += 1
                            continue
                        if spread_type == 'debit' and not enable_debit:
                            drop_counts['debit_filtered'] += 1
                            continue
                        
                        # Also check structure.type for credit_spread/debit_spread
                        struct_type = structure.get('type', '')
                        if 'credit' in struct_type and not enable_credit:
                            drop_counts['credit_filtered'] += 1
                            continue
                        if 'debit' in struct_type and not enable_debit:
                            drop_counts['debit_filtered'] += 1
                            continue
                        
                        # FLAT/STEEP detection from signal metrics
                        edge = candidate.get('edge', {})
                        metrics = edge.get('metrics', {})
                        skew_pctl = metrics.get('skew_percentile', 50)
                        is_steep = metrics.get('is_steep', skew_pctl >= 90)
                        is_flat = metrics.get('is_flat', skew_pctl <= 10)
                        
                        # Note: Both STEEP and FLAT signals are allowed
                        # STEEP = credit spread (sell skew), FLAT = debit spread (buy skew)
                        
                        # REGIME FILTER: Skip high-IV environments
                        # Uses ATM IV percentile (trailing, no lookahead) from signal
                        regime_filter = self.config.get('regime_filter', {})
                        max_iv_pctl = regime_filter.get('max_iv_percentile', None)
                        atm_iv_pctl = metrics.get('atm_iv_percentile', None)
                        
                        if max_iv_pctl is not None and atm_iv_pctl is not None:
                            if atm_iv_pctl > max_iv_pctl:
                                if 'high_iv_filtered' not in drop_counts:
                                    drop_counts['high_iv_filtered'] = 0
                                drop_counts['high_iv_filtered'] += 1
                                continue
                        
                        # Passed all filters
                        drop_counts['passed_filters'] += 1
                        
                        # Add signal with date context
                        signals.append({
                            'signal_date': current.isoformat(),
                            'report_date': report.get('report_date', current.isoformat()),
                            'candidate': candidate,
                            'regime': report.get('regime', {}),
                        })
                        
                except Exception as e:
                    print(f"Error loading {report_path}: {e}")
            
            current += timedelta(days=1)
        
        return signals, drop_counts
    
    def _simulate_trade(self, signal: Dict[str, Any], use_strict: bool = True):
        """
        Simulate a single trade from signal to exit.
        
        Entry: signal_date close
        Exit: determined by exit rules
        
        Returns:
            - BacktestTrade on success
            - None if no data
            - 'unexecutable' if bid/ask bounds violated
        """
        candidate = signal['candidate']
        symbol = candidate.get('symbol', '')
        structure = candidate.get('structure', {})
        edge = candidate.get('edge', {})
        
        # Parse signal date (observation) and execution date (entry)
        try:
            signal_date = date.fromisoformat(signal['signal_date'])
            # Use execution_date (N+1) for entry - no lookahead
            exec_date_str = signal.get('execution_date') or signal.get('signal_date')
            execution_date = date.fromisoformat(exec_date_str)
        except:
            return None
        
        # Get structure details
        spread_type = structure.get('spread_type', 'credit')
        legs = structure.get('legs', [])
        
        if not legs:
            return None
        
        # Parse expiry from first leg or structure
        try:
            expiry_str = structure.get('expiry') or legs[0].get('expiry', '')
            if isinstance(expiry_str, date):
                expiry = expiry_str
            else:
                expiry = date.fromisoformat(str(expiry_str)[:10])
        except:
            return None
        
        # Calculate DTE at entry (from execution_date, not signal_date)
        dte_at_entry = (expiry - execution_date).days
        if dte_at_entry <= 0:
            return None
        
        # Get historical data for each leg
        leg_data = {}
        for leg in legs:
            occ = leg.get('occ_symbol', '')
            if not occ:
                # Build OCC from components if not provided
                strike = leg.get('strike', 0)
                right = leg.get('right', 'P')
                exp_str = expiry.strftime('%y%m%d')
                occ = f"{symbol.ljust(6)}{exp_str}{right}{int(strike*1000):08d}"
            
            # Fetch bars from execution_date to expiry
            bars = self.get_option_bars(occ, execution_date, expiry)
            if not bars:
                return None  # Can't simulate without data
            
            leg_data[occ] = {
                'bars': bars,
                'side': leg.get('side', 'BUY'),
                'strike': leg.get('strike', 0),
                'right': leg.get('right', 'P'),
            }
        
        # Entry at execution_date close (N+1, no lookahead)
        entry_closes = {}
        entry_sides = {}
        for occ, data in leg_data.items():
            bars = data['bars']
            # Find execution_date bar (not signal_date)
            entry_bar = next((b for b in bars if b['date'] == execution_date.isoformat()), None)
            if not entry_bar:
                return None
            entry_closes[occ] = entry_bar['close']
            entry_sides[occ] = data['side']
        
        # Use strict or relaxed fill model
        if use_strict:
            entry_fill = calculate_strict_entry_fill(entry_closes, entry_sides, self.fill_config)
            if entry_fill.get('unexecutable', False):
                return 'unexecutable'
        else:
            entry_fill = calculate_entry_fill(entry_closes, entry_sides, self.fill_config)
        
        entry_net = entry_fill['net_premium']
        entry_commissions = entry_fill['commissions']
        
        # ========================================
        # RUNTIME MAX LOSS COMPUTATION (Source of Truth)
        # ========================================
        # Compute from actual strikes, do NOT trust stored max_loss_dollars
        strikes = [data['strike'] for data in leg_data.values()]
        if len(strikes) >= 2:
            width_dollars = abs(max(strikes) - min(strikes))
        else:
            width_dollars = 0
        
        # entry_net is positive for credit received, negative for debit paid
        if spread_type == 'credit':
            # Credit spread: max_loss = (width - credit) * 100
            entry_credit = entry_net  # Positive
            computed_max_loss = (width_dollars - entry_credit) * 100
            computed_max_profit = entry_credit * 100
        else:
            # Debit spread: max_loss = debit paid
            entry_debit = abs(entry_net)  # Make positive
            computed_max_loss = entry_debit * 100
            computed_max_profit = (width_dollars - entry_debit) * 100
        
        # Get exit rules
        exit_rules = self.config.get('exit_rules', {}).get(f'{spread_type}_spread', {})
        tp_pct = exit_rules.get('take_profit_pct', 50)
        sl_mult = exit_rules.get('stop_loss_mult', 2.0)
        sl_pct = exit_rules.get('stop_loss_pct', 50)
        time_stop_dte = exit_rules.get('time_stop_dte', 5)
        
        # Early exit on "no progress" (Experiment B1)
        early_exit = exit_rules.get('early_exit', {})
        early_exit_enabled = early_exit.get('enabled', False)
        early_exit_dte = early_exit.get('dte', 14)
        early_exit_min_mfe_pct = early_exit.get('min_mfe_pct', 15)
        
        # Calculate TP/SL thresholds (skip if null - time-stop-only mode)
        if spread_type == 'credit':
            # Credit: TP at X% of credit, SL at Yx credit
            credit_received = entry_net  # Positive
            tp_target = credit_received * (1 - tp_pct / 100) if tp_pct is not None else None
            sl_threshold = -credit_received * sl_mult if sl_mult is not None else None
        else:
            # Debit: TP at X% of max profit, SL at Y% of debit
            debit_paid = -entry_net  # Positive
            max_profit = structure.get('max_profit_dollars', debit_paid * 2) / 100
            tp_target = debit_paid * (tp_pct / 100) if tp_pct is not None else None
            sl_threshold = -debit_paid * (sl_pct / 100) if sl_pct is not None else None
        
        # Simulate daily until exit
        exit_date = None
        exit_reason = ExitReason.EXPIRY
        mfe = 0.0
        mae = 0.0
        daily_pnls = []
        
        current = signal_date + timedelta(days=1)
        while current <= expiry:
            # Get closes for all legs on this date
            current_closes = {}
            all_have_data = True
            for occ, data in leg_data.items():
                bar = next((b for b in data['bars'] if b['date'] == current.isoformat()), None)
                if not bar:
                    all_have_data = False
                    break
                current_closes[occ] = bar['close']
            
            if not all_have_data:
                current += timedelta(days=1)
                continue
            
            # Calculate current PnL (mark-to-market)
            exit_fill = calculate_exit_fill(current_closes, entry_sides, self.fill_config)
            exit_net = exit_fill['net_premium']
            
            mtm_pnl = (entry_net + exit_net) * 100
            daily_pnls.append(mtm_pnl)
            
            # Update MFE/MAE
            mfe = max(mfe, mtm_pnl)
            mae = min(mae, mtm_pnl)
            
            # Check exit conditions in priority order:
            # IMPORTANT: Exit TRIGGERS on day t, but EXECUTES on day t+1 (no lookahead)
            # 1. Take profit (primary goal)
            # 2. Stop loss
            # 3. Time stop (secondary, only if no TP/SL)
            dte = (expiry - current).days
            
            # 1. Take profit (highest priority) - skip if tp_pct is null
            if tp_pct is not None:
                if spread_type == 'credit':
                    # For credit: TP when we've captured tp_pct of credit
                    tp_threshold = credit_received * (tp_pct / 100) * 100  # In dollars
                    if mtm_pnl >= tp_threshold:
                        exit_date = current + timedelta(days=1)
                        exit_reason = ExitReason.TAKE_PROFIT
                        break
                else:
                    # For debit: profit when position value increased
                    if mtm_pnl >= tp_target * 100:
                        exit_date = current + timedelta(days=1)
                        exit_reason = ExitReason.TAKE_PROFIT
                        break
            
            # 2. Stop loss - skip if sl_mult/sl_threshold is null
            if sl_threshold is not None and mtm_pnl <= sl_threshold * 100:
                exit_date = current + timedelta(days=1)
                exit_reason = ExitReason.STOP_LOSS
                break
            
            # 3. Early exit on "no progress" (only for debit spreads)
            # Rule: If at DTE <= early_exit_dte and MFE < min_mfe_pct of max profit, exit
            # Asks: "Has the edge shown up yet? If not, odds deteriorate."
            if early_exit_enabled and spread_type == 'debit' and dte <= early_exit_dte:
                min_mfe_threshold = computed_max_profit * (early_exit_min_mfe_pct / 100)
                if mfe < min_mfe_threshold:
                    exit_date = current
                    exit_reason = ExitReason.TIME_STOP  # Use time_stop reason for now
                    break
            
            # 4. Time stop (only if TP/SL/early_exit not hit) - execute same day
            if dte <= time_stop_dte:
                exit_date = current
                exit_reason = ExitReason.TIME_STOP
                break
            
            current += timedelta(days=1)
        
        # If no exit triggered, use expiry or last available date
        if not exit_date:
            exit_date = min(expiry, current - timedelta(days=1))
            exit_reason = ExitReason.EXPIRY
        
        # Final exit fill
        final_closes = {}
        for occ, data in leg_data.items():
            bar = next((b for b in reversed(data['bars']) if b['date'] <= exit_date.isoformat()), None)
            if bar:
                final_closes[occ] = bar['close']
        
        if not final_closes:
            return None
        
        final_fill = calculate_exit_fill(final_closes, entry_sides, self.fill_config)
        exit_net = final_fill['net_premium']
        exit_commissions = final_fill['commissions']
        
        # Calculate realized PnL
        pnl = calculate_realized_pnl(entry_net, exit_net, entry_commissions, exit_commissions, 1)
        
        # ========================================
        # HARD INVARIANT: PnL must not exceed theoretical max loss
        # If violated, STOP the run immediately (correctness bug)
        # ========================================
        # Using computed_max_loss (from runtime strike calculation), NOT stored value
        epsilon = 50  # Allow slippage/commissions buffer
        if spread_type == 'credit' and computed_max_loss > 0:
            if abs(pnl['net_pnl']) > computed_max_loss + epsilon:
                error_msg = f"""
❌ INVARIANT VIOLATION - HALTING BACKTEST
=========================================
Symbol: {symbol}
Signal Date: {signal_date}
net_pnl: ${pnl['net_pnl']:.2f}
computed_max_loss: ${computed_max_loss:.2f}
width_dollars: ${width_dollars:.2f}
entry_credit: ${entry_net:.2f}
Legs:
"""
                for occ, data in leg_data.items():
                    error_msg += f"  {occ}: {data['side']} strike={data['strike']} right={data['right']}\n"
                error_msg += f"""
This indicates a correctness bug in structure/risk calculation.
Fix the bug before running backtests.
"""
                print(error_msg)
                raise ValueError(error_msg)
        
        # Build trade record
        trade = BacktestTrade(
            trade_id=f"{signal_date.isoformat()}_{symbol}_{structure.get('type', 'spread')}",
            symbol=symbol,
            edge_type=edge.get('type', ''),
            edge_strength=edge.get('strength', 0),
            edge_percentile=edge.get('metrics', {}).get('skew_percentile', 0),
            regime=signal.get('regime', {}).get('state', ''),
            structure_type=structure.get('type', ''),
            spread_type=spread_type,
            dte_at_entry=dte_at_entry,
            signal_date=signal_date.isoformat(),
            entry_date=execution_date.isoformat(),  # N+1 entry
            exit_date=exit_date.isoformat(),
            entry_price=entry_net,
            exit_price=exit_net,
            max_loss_theoretical=computed_max_loss,
            max_profit_theoretical=computed_max_profit,
            gross_pnl=pnl['gross_pnl'],
            commissions=pnl['commissions'],
            net_pnl=pnl['net_pnl'],
            pnl_pct=pnl['net_pnl'] / computed_max_loss * 100 if computed_max_loss > 0 else 0,
            mfe=mfe,
            mae=mae,
            exit_reason=exit_reason,
            hold_days=(exit_date - execution_date).days,  # From execution_date
            contracts=1,
            legs=[{
                'occ': occ,
                'side': data['side'],
                'strike': data['strike'],
                'right': data['right'],
            } for occ, data in leg_data.items()],
            data_source='polygon',
        )
        
        # Compute edge diagnostics (forward-looking from signal date)
        diagnostics = self._compute_edge_diagnostics(symbol, signal_date, edge)
        trade.diagnostics = diagnostics
        
        return trade
    
    def _compute_edge_diagnostics(self, symbol: str, signal_date: date, edge: dict) -> dict:
        """
        Compute forward-looking diagnostics for edge validation.
        
        Goal: Answer 'did skew actually revert before price damage?'
        """
        diagnostics = {}
        
        try:
            # Get underlying price bars for forward returns
            end_date = signal_date + timedelta(days=15)
            bars = self.get_underlying_bars(symbol, signal_date, end_date)
            if bars:
                # Find price at signal date
                signal_bar = next((b for b in bars if b['date'] == signal_date.isoformat()), None)
                if signal_bar:
                    p0 = signal_bar['close']
                    
                    # Compute forward returns
                    for days in [1, 3, 5, 10]:
                        target_date = signal_date + timedelta(days=days)
                        target_bar = next((b for b in bars if b['date'] >= target_date.isoformat()), None)
                        if target_bar:
                            p_n = target_bar['close']
                            ret = (p_n - p0) / p0
                            diagnostics[f'price_return_{days}d'] = round(ret * 100, 2)  # As percentage
            
            # Store edge info at signal - handle both old and new format
            metrics = edge.get('metrics', {})
            # Skew value: prefer put_call_skew (new), fall back to skew_ratio (old)
            diagnostics['skew_at_signal'] = metrics.get('put_call_skew', metrics.get('skew_ratio', None))
            diagnostics['skew_pctl_at_signal'] = metrics.get('skew_percentile', None)
            # is_steep/is_flat: can be in metrics (new format) or edge top-level (old format)
            diagnostics['is_steep'] = metrics.get('is_steep', edge.get('is_steep', None))
            diagnostics['is_flat'] = metrics.get('is_flat', edge.get('is_flat', None))
            # Compute is_steep/is_flat from percentile if missing
            pctl = diagnostics['skew_pctl_at_signal']
            if diagnostics['is_steep'] is None and pctl is not None:
                diagnostics['is_steep'] = pctl >= 90
            if diagnostics['is_flat'] is None and pctl is not None:
                diagnostics['is_flat'] = pctl <= 10
            
        except Exception as e:
            diagnostics['error'] = str(e)
        
        return diagnostics
    
    def _calculate_metrics(self, trades: List[BacktestTrade]) -> BacktestMetrics:
        """Calculate aggregate metrics from trades."""
        if not trades:
            return BacktestMetrics()
        
        metrics = BacktestMetrics()
        metrics.total_trades = len(trades)
        
        winners = [t for t in trades if t.net_pnl > 0]
        losers = [t for t in trades if t.net_pnl <= 0]
        
        metrics.winners = len(winners)
        metrics.losers = len(losers)
        metrics.win_rate = len(winners) / len(trades) * 100 if trades else 0
        
        # PnL
        metrics.total_pnl = sum(t.net_pnl for t in trades)
        metrics.total_commissions = sum(t.commissions for t in trades)
        metrics.avg_pnl = metrics.total_pnl / len(trades) if trades else 0
        metrics.avg_win = sum(t.net_pnl for t in winners) / len(winners) if winners else 0
        metrics.avg_loss = sum(t.net_pnl for t in losers) / len(losers) if losers else 0
        
        # Profit factor
        gross_wins = sum(t.net_pnl for t in winners)
        gross_losses = abs(sum(t.net_pnl for t in losers))
        metrics.profit_factor = gross_wins / gross_losses if gross_losses > 0 else float('inf')
        
        # Expectancy
        win_pct = len(winners) / len(trades) if trades else 0
        loss_pct = len(losers) / len(trades) if trades else 0
        metrics.expectancy = (win_pct * metrics.avg_win) + (loss_pct * metrics.avg_loss)
        
        # Hold days
        metrics.avg_hold_days = sum(t.hold_days for t in trades) / len(trades) if trades else 0
        metrics.total_exposure_days = sum(t.hold_days for t in trades)
        
        # Max drawdown
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in sorted(trades, key=lambda x: x.signal_date):
            cumulative += t.net_pnl
            peak = max(peak, cumulative)
            dd = peak - cumulative
            max_dd = max(max_dd, dd)
        metrics.max_drawdown = max_dd
        
        # Breakdowns
        metrics.by_edge_type = self._breakdown_by_field(trades, 'edge_type')
        metrics.by_regime = self._breakdown_by_field(trades, 'regime')
        metrics.by_structure = self._breakdown_by_field(trades, 'structure_type')
        metrics.by_symbol = self._breakdown_by_field(trades, 'symbol')
        
        return metrics
    
    def _breakdown_by_field(self, trades: List[BacktestTrade], field: str) -> Dict[str, Dict]:
        """Calculate breakdown by a specific field."""
        breakdown = {}
        
        for trade in trades:
            key = getattr(trade, field, 'unknown')
            if key not in breakdown:
                breakdown[key] = {'trades': 0, 'pnl': 0.0, 'winners': 0}
            
            breakdown[key]['trades'] += 1
            breakdown[key]['pnl'] += trade.net_pnl
            if trade.net_pnl > 0:
                breakdown[key]['winners'] += 1
        
        # Add win rate
        for key in breakdown:
            total = breakdown[key]['trades']
            breakdown[key]['win_rate'] = breakdown[key]['winners'] / total * 100 if total > 0 else 0
        
        return breakdown
    
    def _empty_result(self, start: date, end: date, source: str) -> BacktestResult:
        """Create empty result when no signals found."""
        return BacktestResult(
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            config_hash=self._hash_config(),
            trades=[],
            metrics=BacktestMetrics(),
            generated_at=datetime.now().isoformat(),
            signals_source=source,
            data_source='polygon',
            config_used=self.config,
        )
    
    def _hash_config(self) -> str:
        """Generate hash of config for reproducibility check."""
        config_str = json.dumps(self.config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()[:8]
