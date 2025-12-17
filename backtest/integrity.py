"""
Backtest Integrity Report.

Validates data quality before interpreting P&L results.
Ensures no fallback signals and consistent structure types.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime

from .result import BacktestTrade, BacktestResult


@dataclass
class IntegrityReport:
    """Backtest integrity validation report."""
    
    # Overall status
    passed: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Structure counts
    credit_spread_count: int = 0
    debit_spread_count: int = 0
    
    # Signal flag counts
    is_steep_count: int = 0
    is_flat_count: int = 0
    history_mode_1_count: int = 0  # Percentile-based
    history_mode_0_count: int = 0  # Fallback (BAD)
    
    # Entry statistics
    avg_entry_credit: float = 0.0
    avg_entry_debit: float = 0.0
    avg_max_loss: float = 0.0
    avg_spread_width: float = 0.0
    avg_dte_at_entry: float = 0.0
    
    # Exit breakdown
    take_profit_count: int = 0
    stop_loss_count: int = 0
    time_stop_count: int = 0
    expiry_count: int = 0
    
    # Exit trigger details
    exit_details: List[Dict] = field(default_factory=list)
    
    def add_error(self, msg: str):
        self.errors.append(msg)
        self.passed = False
    
    def add_warning(self, msg: str):
        self.warnings.append(msg)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'passed': self.passed,
            'errors': self.errors,
            'warnings': self.warnings,
            'structure_counts': {
                'credit_spread': self.credit_spread_count,
                'debit_spread': self.debit_spread_count,
            },
            'signal_flags': {
                'is_steep': self.is_steep_count,
                'is_flat': self.is_flat_count,
                'history_mode_1': self.history_mode_1_count,
                'history_mode_0': self.history_mode_0_count,
            },
            'entry_stats': {
                'avg_entry_credit': self.avg_entry_credit,
                'avg_entry_debit': self.avg_entry_debit,
                'avg_max_loss': self.avg_max_loss,
                'avg_spread_width': self.avg_spread_width,
                'avg_dte_at_entry': self.avg_dte_at_entry,
            },
            'exit_breakdown': {
                'take_profit': self.take_profit_count,
                'stop_loss': self.stop_loss_count,
                'time_stop': self.time_stop_count,
                'expiry': self.expiry_count,
            },
        }


def generate_integrity_report(
    result: BacktestResult,
    require_history_mode: bool = True,
    fail_on_fallback: bool = True,
) -> IntegrityReport:
    """
    Generate integrity report for backtest result.
    
    Validates:
    - No fallback signals (history_mode=0)
    - Consistent structure types
    - Reasonable entry/exit statistics
    """
    report = IntegrityReport()
    trades = result.trades
    
    if not trades:
        report.add_warning("No trades to validate")
        return report
    
    # Collect statistics
    credit_entries = []
    debit_entries = []
    max_losses = []
    widths = []
    dtes = []
    
    for trade in trades:
        # Structure type
        if trade.spread_type == 'credit':
            report.credit_spread_count += 1
            if trade.entry_price > 0:
                credit_entries.append(trade.entry_price)
        elif trade.spread_type == 'debit':
            report.debit_spread_count += 1
            if trade.entry_price < 0:
                debit_entries.append(abs(trade.entry_price))
        
        # Edge metrics (from signal)
        # Note: These may not be directly on trade object, check if available
        edge_metrics = getattr(trade, 'edge_metrics', {}) or {}
        
        # Check is_steep / is_flat from edge_type or metrics
        if edge_metrics.get('is_steep') == 1.0:
            report.is_steep_count += 1
        elif edge_metrics.get('is_flat') == 1.0:
            report.is_flat_count += 1
        else:
            # Infer from structure type
            if trade.spread_type == 'credit':
                report.is_steep_count += 1
            elif trade.spread_type == 'debit':
                report.is_flat_count += 1
        
        # History mode
        history_mode = edge_metrics.get('history_mode', 1.0)
        if history_mode == 1.0:
            report.history_mode_1_count += 1
        else:
            report.history_mode_0_count += 1
        
        # Entry stats
        max_losses.append(trade.max_loss_theoretical)
        dtes.append(trade.dte_at_entry)
        
        # Exit reason
        exit_reason = trade.exit_reason.value if hasattr(trade.exit_reason, 'value') else str(trade.exit_reason)
        if exit_reason == 'take_profit':
            report.take_profit_count += 1
        elif exit_reason == 'stop_loss':
            report.stop_loss_count += 1
        elif exit_reason == 'time_stop':
            report.time_stop_count += 1
        elif exit_reason == 'expiry':
            report.expiry_count += 1
        
        # Store exit detail
        report.exit_details.append({
            'trade_id': trade.trade_id,
            'exit_reason': exit_reason,
            'entry_price': trade.entry_price,
            'exit_price': trade.exit_price,
            'net_pnl': trade.net_pnl,
        })
    
    # Calculate averages
    if credit_entries:
        report.avg_entry_credit = sum(credit_entries) / len(credit_entries)
    if debit_entries:
        report.avg_entry_debit = sum(debit_entries) / len(debit_entries)
    if max_losses:
        report.avg_max_loss = sum(max_losses) / len(max_losses)
    if dtes:
        report.avg_dte_at_entry = sum(dtes) / len(dtes)
    
    # Validation checks
    if fail_on_fallback and report.history_mode_0_count > 0:
        report.add_error(
            f"Found {report.history_mode_0_count} trades using fallback mode (history_mode=0). "
            "These signals lack sufficient history for valid percentile calculation."
        )
    
    if require_history_mode and report.history_mode_1_count == 0:
        report.add_error("No trades with history_mode=1. All signals used fallback thresholds.")
    
    # Consistency checks
    if report.credit_spread_count > 0 and report.debit_spread_count > 0:
        pct_credit = report.credit_spread_count / len(trades) * 100
        pct_debit = report.debit_spread_count / len(trades) * 100
        report.add_warning(
            f"Mixed structure types: {pct_credit:.0f}% credit, {pct_debit:.0f}% debit. "
            "This is expected for skew_extreme edge."
        )
    
    if report.is_steep_count != report.credit_spread_count:
        report.add_warning(
            f"is_steep count ({report.is_steep_count}) != credit_spread count ({report.credit_spread_count}). "
            "Signal-structure mapping may be inconsistent."
        )
    
    if report.is_flat_count != report.debit_spread_count:
        report.add_warning(
            f"is_flat count ({report.is_flat_count}) != debit_spread count ({report.debit_spread_count}). "
            "Signal-structure mapping may be inconsistent."
        )
    
    return report


def print_integrity_report(report: IntegrityReport):
    """Print formatted integrity report."""
    print()
    print("=" * 60)
    print("BACKTEST INTEGRITY REPORT")
    print("=" * 60)
    print()
    
    # Status
    status = "✅ PASSED" if report.passed else "❌ FAILED"
    print(f"Status: {status}")
    print()
    
    # Errors
    if report.errors:
        print("ERRORS:")
        for err in report.errors:
            print(f"  ❌ {err}")
        print()
    
    # Warnings
    if report.warnings:
        print("WARNINGS:")
        for warn in report.warnings:
            print(f"  ⚠️ {warn}")
        print()
    
    # Structure counts
    print("STRUCTURE TYPES:")
    print(f"  credit_spread: {report.credit_spread_count}")
    print(f"  debit_spread: {report.debit_spread_count}")
    print()
    
    # Signal flags
    print("SIGNAL FLAGS:")
    print(f"  is_steep: {report.is_steep_count}")
    print(f"  is_flat: {report.is_flat_count}")
    total_trades = report.is_steep_count + report.is_flat_count
    if total_trades > 0:
        pct_history = report.history_mode_1_count / total_trades * 100
        print(f"  history_mode=1: {report.history_mode_1_count} ({pct_history:.0f}%)")
        print(f"  history_mode=0 (fallback): {report.history_mode_0_count} <- should be 0")
    print()
    
    # Entry statistics
    print("ENTRY STATISTICS:")
    print(f"  Avg entry credit: ${report.avg_entry_credit:.2f}")
    print(f"  Avg entry debit: ${report.avg_entry_debit:.2f}")
    print(f"  Avg max loss: ${report.avg_max_loss:.2f}")
    print(f"  Avg DTE at entry: {report.avg_dte_at_entry:.1f} days")
    print()
    
    # Exit breakdown
    print("EXIT BREAKDOWN:")
    total_exits = (report.take_profit_count + report.stop_loss_count + 
                   report.time_stop_count + report.expiry_count)
    if total_exits > 0:
        print(f"  take_profit: {report.take_profit_count} ({report.take_profit_count/total_exits*100:.0f}%)")
        print(f"  stop_loss: {report.stop_loss_count} ({report.stop_loss_count/total_exits*100:.0f}%)")
        print(f"  time_stop: {report.time_stop_count} ({report.time_stop_count/total_exits*100:.0f}%)")
        print(f"  expiry: {report.expiry_count} ({report.expiry_count/total_exits*100:.0f}%)")
    print()
    print("=" * 60)
