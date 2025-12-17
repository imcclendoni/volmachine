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
        
        # Load signals from saved reports
        signals, drop_counts = self._load_signals(start_date, end_date, symbols, min_strength, trade_only)
        
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
        print(f"  Passed filters: {drop_counts['passed_filters']}")
        print(f"  Signals to simulate: {len(signals)}")
        print()
        
        if not signals:
            return self._empty_result(start_date, end_date, signals_source)
        
        # Check if using strict fills
        use_strict = self.config.get('slippage', {}).get('use_strict_fills', True)
        
        # Position limits config
        strategy_config = self.config.get('strategies', {}).get('skew_extreme', {})
        max_positions_per_symbol = strategy_config.get('max_positions_per_symbol', 1)
        cooldown_after_sl_days = strategy_config.get('cooldown_after_sl_days', 10)
        
        # Simulate each signal
        trades = []
        unexecutable_count = 0
        skipped_no_data = 0
        skipped_overlap = 0
        skipped_cooldown = 0
        
        # Track open positions: symbol -> exit_date
        open_positions = {}
        # Track cooldown: symbol -> earliest_entry_date
        cooldown_until = {}
        
        for signal in signals:
            symbol = signal.get('symbol')
            exec_date_str = signal.get('execution_date', signal.get('signal_date'))
            try:
                exec_date = date.fromisoformat(exec_date_str)
            except:
                exec_date = date.today()
            
            # Check position overlap
            if symbol in open_positions and exec_date < open_positions[symbol]:
                skipped_overlap += 1
                continue
            
            # Check cooldown after SL
            if symbol in cooldown_until and exec_date < cooldown_until[symbol]:
                skipped_cooldown += 1
                continue
            
            trade = self._simulate_trade(signal, use_strict=use_strict)
            if trade is None:
                skipped_no_data += 1
            elif trade == 'unexecutable':
                unexecutable_count += 1
            else:
                trades.append(trade)
                print(f"  {trade.signal_date} {trade.symbol}: {trade.structure_type} -> "
                      f"${trade.net_pnl:.2f} ({trade.exit_reason.value})")
                
                # Update position tracking
                open_positions[symbol] = trade.exit_date if isinstance(trade.exit_date, date) else date.fromisoformat(trade.exit_date)
                
                # Set cooldown if stop-loss
                if trade.exit_reason == ExitReason.STOP_LOSS:
                    exit_dt = trade.exit_date if isinstance(trade.exit_date, date) else date.fromisoformat(trade.exit_date)
                    cooldown_until[symbol] = exit_dt + timedelta(days=cooldown_after_sl_days)
        
        print(f"\nCompleted {len(trades)} trades")
        if unexecutable_count > 0:
            print(f"Unexecutable (bad bid/ask): {unexecutable_count}")
        if skipped_no_data > 0:
            print(f"Skipped (no data): {skipped_no_data}")
        if skipped_overlap > 0:
            print(f"Skipped (position overlap): {skipped_overlap}")
        if skipped_cooldown > 0:
            print(f"Skipped (SL cooldown): {skipped_cooldown}")
        
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
        
        # Parse signal date
        try:
            signal_date = date.fromisoformat(signal['signal_date'])
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
        
        # Calculate DTE at entry
        dte_at_entry = (expiry - signal_date).days
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
                occ = f"{symbol}   {exp_str}{right}{int(strike*1000):08d}"
            
            # Fetch bars from signal_date to expiry
            bars = self.get_option_bars(occ, signal_date, expiry)
            if not bars:
                return None  # Can't simulate without data
            
            leg_data[occ] = {
                'bars': bars,
                'side': leg.get('side', 'BUY'),
                'strike': leg.get('strike', 0),
                'right': leg.get('right', 'P'),
            }
        
        # Entry at signal_date close
        entry_closes = {}
        entry_sides = {}
        for occ, data in leg_data.items():
            bars = data['bars']
            # Find signal_date bar
            entry_bar = next((b for b in bars if b['date'] == signal_date.isoformat()), None)
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
        
        # Get exit rules
        exit_rules = self.config.get('exit_rules', {}).get(f'{spread_type}_spread', {})
        tp_pct = exit_rules.get('take_profit_pct', 50)
        sl_mult = exit_rules.get('stop_loss_mult', 2.0)
        sl_pct = exit_rules.get('stop_loss_pct', 50)
        time_stop_dte = exit_rules.get('time_stop_dte', 5)
        
        # Calculate TP/SL thresholds
        if spread_type == 'credit':
            # Credit: TP at X% of credit, SL at Yx credit
            credit_received = entry_net  # Positive
            tp_target = credit_received * (1 - tp_pct / 100)  # Pay back less = profit
            sl_threshold = -credit_received * sl_mult  # Loss = -2x credit
        else:
            # Debit: TP at X% of max profit, SL at Y% of debit
            debit_paid = -entry_net  # Positive
            max_profit = structure.get('max_profit_dollars', debit_paid * 2) / 100
            tp_target = debit_paid * (tp_pct / 100)  # Receive back TP% of debit
            sl_threshold = -debit_paid * (sl_pct / 100)  # Lose SL% of debit
        
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
            
            # 1. Take profit (highest priority) - based on PnL, not exit_net
            if spread_type == 'credit':
                # For credit: TP when we've captured tp_pct of credit
                # e.g., entry credit $1.50, tp_pct=50 -> TP at $75 profit
                tp_threshold = credit_received * (tp_pct / 100) * 100  # In dollars
                if mtm_pnl >= tp_threshold:
                    # Execute on NEXT trading day (no lookahead)
                    exit_date = current + timedelta(days=1)
                    exit_reason = ExitReason.TAKE_PROFIT
                    break
            else:
                # For debit: profit when position value increased
                if mtm_pnl >= tp_target * 100:
                    exit_date = current + timedelta(days=1)
                    exit_reason = ExitReason.TAKE_PROFIT
                    break
            
            # 2. Stop loss - based on PnL, execute next day
            if mtm_pnl <= sl_threshold * 100:
                exit_date = current + timedelta(days=1)
                exit_reason = ExitReason.STOP_LOSS
                break
            
            # 3. Time stop (only if TP/SL not hit) - execute same day since pre-planned
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
            entry_date=signal_date.isoformat(),
            exit_date=exit_date.isoformat(),
            entry_price=entry_net,
            exit_price=exit_net,
            max_loss_theoretical=abs(structure.get('max_loss_dollars', 0)),
            max_profit_theoretical=abs(structure.get('max_profit_dollars', 0)),
            gross_pnl=pnl['gross_pnl'],
            commissions=pnl['commissions'],
            net_pnl=pnl['net_pnl'],
            pnl_pct=pnl['net_pnl'] / abs(structure.get('max_loss_dollars', 1)) * 100 if structure.get('max_loss_dollars') else 0,
            mfe=mfe,
            mae=mae,
            exit_reason=exit_reason,
            hold_days=(exit_date - signal_date).days,
            contracts=1,
            legs=[{
                'occ': occ,
                'side': data['side'],
                'strike': data['strike'],
                'right': data['right'],
            } for occ, data in leg_data.items()],
            data_source='polygon',
        )
        
        return trade
    
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
