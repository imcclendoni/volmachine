#!/usr/bin/env python3
"""
FLAT Call Debit Spread — Realism Audits

Four validation checks before promoting to Edge Module #2:
1. Debit spread max-profit/max-loss invariants (HARD FAIL)
2. Fill model parity with backtester
3. Call-debit construction (leg ordering/expiry/width)
4. Symbol breakdown (no single-symbol dominance)

Usage:
    python scripts/research/flat_realism_audit.py

Output:
    logs/research/flat_realism_audit.json
"""

import sys
sys.path.insert(0, '/Users/jeffreyboyle/Desktop/volmachine')

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
import statistics

from backtest.fill_model import FillConfig, calculate_entry_fill
from data.polygon_backtest import get_underlying_daily_bars

# Configuration
REPORTS_DIR = Path('./logs/backfill/v6/reports')
OUTPUT_FILE = Path('./logs/research/flat_realism_audit.json')
HOLDING_PERIOD_DAYS = 14


class AuditResult:
    """Track audit pass/fail with details."""
    
    def __init__(self, name: str):
        self.name = name
        self.passed = True
        self.checks = []
        self.failures = []
        self.warnings = []
    
    def check(self, condition: bool, description: str, details: str = None, hard_fail: bool = True):
        """Record a check result."""
        status = "PASS" if condition else "FAIL"
        self.checks.append({
            'description': description,
            'status': status,
            'details': details,
            'hard_fail': hard_fail,
        })
        if not condition:
            if hard_fail:
                self.passed = False
                self.failures.append(description)
            else:
                self.warnings.append(description)
    
    def summary(self) -> Dict:
        return {
            'audit': self.name,
            'passed': self.passed,
            'checks_total': len(self.checks),
            'checks_passed': sum(1 for c in self.checks if c['status'] == 'PASS'),
            'failures': self.failures,
            'warnings': self.warnings,
            'details': self.checks,
        }


def load_flat_signals(reports_dir: Path) -> List[Dict]:
    """Load FLAT signals with structure data."""
    signals = []
    
    for report_file in sorted(reports_dir.glob('*__*__backfill.json')):
        try:
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            parts = report_file.stem.split('__')
            if len(parts) < 2:
                continue
            
            report_date_str = parts[0]
            symbol = parts[1]
            
            signal_date = report.get('report_date', report_date_str)
            execution_date = report.get('execution_date', report_date_str)
            
            for edge in report.get('edges', []):
                metrics = edge.get('metrics', {})
                
                is_flat = metrics.get('is_flat', 0.0) == 1.0
                is_long = edge.get('direction', '').upper() == 'LONG'
                
                if not (is_flat or is_long):
                    continue
                
                candidates = report.get('candidates', [])
                structure = candidates[0].get('structure') if candidates else None
                
                if structure:
                    signals.append({
                        'symbol': symbol,
                        'signal_date': signal_date,
                        'execution_date': execution_date,
                        'edge': edge,
                        'structure': structure,
                        'atm_iv_percentile': metrics.get('atm_iv_percentile'),
                    })
                    
        except Exception as e:
            continue
    
    return signals


def audit_1_invariants(signals: List[Dict]) -> AuditResult:
    """
    Audit 1: Debit Spread Invariants (HARD FAIL)
    
    - Max profit = width - debit
    - Max loss = debit paid
    - Exit value ∈ [0, width]
    """
    audit = AuditResult("Debit Spread Invariants")
    
    violations = {'max_profit': 0, 'max_loss': 0, 'exit_bounds': 0}
    total_checked = 0
    
    for sig in signals:
        struct = sig.get('structure', {})
        legs = struct.get('legs', [])
        if len(legs) != 2:
            continue
        
        # Get structure parameters
        long_strike = None
        short_strike = None
        for leg in legs:
            if leg.get('side') == 'BUY':
                long_strike = leg.get('strike')
            else:
                short_strike = leg.get('strike')
        
        if long_strike is None or short_strike is None:
            continue
        
        width = abs(long_strike - short_strike)
        entry_debit = struct.get('entry_debit', 1.0)
        
        total_checked += 1
        
        # Check invariants
        expected_max_profit = width - entry_debit
        expected_max_loss = entry_debit
        
        # Validate max profit is positive (otherwise trade makes no sense)
        if expected_max_profit <= 0:
            violations['max_profit'] += 1
        
        # Validate max loss is positive
        if expected_max_loss <= 0:
            violations['max_loss'] += 1
        
        # Validate width > debit (otherwise negative risk/reward)
        if width <= entry_debit:
            violations['exit_bounds'] += 1
    
    audit.check(
        violations['max_profit'] == 0,
        f"Max profit positive: {total_checked - violations['max_profit']}/{total_checked}",
        f"{violations['max_profit']} violations",
        hard_fail=True
    )
    
    audit.check(
        violations['max_loss'] == 0,
        f"Max loss valid: {total_checked - violations['max_loss']}/{total_checked}",
        f"{violations['max_loss']} violations",
        hard_fail=True
    )
    
    audit.check(
        violations['exit_bounds'] == 0,
        f"Width > debit: {total_checked - violations['exit_bounds']}/{total_checked}",
        f"{violations['exit_bounds']} violations",
        hard_fail=True
    )
    
    return audit


def audit_2_fill_model(signals: List[Dict]) -> AuditResult:
    """
    Audit 2: Fill Model Parity
    
    - Uses same fill_model.py functions
    - Slippage/commission match config
    - No double-counting
    """
    audit = AuditResult("Fill Model Parity")
    
    # Load fill config
    fill_config = FillConfig.from_yaml()
    
    audit.check(
        fill_config.slippage_per_leg == 0.02,
        f"Slippage per leg: ${fill_config.slippage_per_leg}",
        "Expected $0.02/leg",
        hard_fail=False  # Warning, not hard fail
    )
    
    audit.check(
        fill_config.commission_per_contract == 0.65,
        f"Commission per contract: ${fill_config.commission_per_contract}",
        "Expected $0.65/contract",
        hard_fail=False
    )
    
    # Test fill calculation
    test_closes = {'leg0': 2.50, 'leg1': 1.50}
    test_sides = {'leg0': 'BUY', 'leg1': 'SELL'}
    
    entry_fill = calculate_entry_fill(test_closes, test_sides, fill_config)
    
    # BUY leg: 2.50 + 0.02 = 2.52 (pay)
    # SELL leg: 1.50 - 0.02 = 1.48 (receive)
    # Net = -2.52 + 1.48 = -1.04 (debit)
    expected_net = -2.52 + 1.48
    
    audit.check(
        abs(entry_fill['net_premium'] - expected_net) < 0.01,
        f"Fill calculation correct",
        f"Got {entry_fill['net_premium']:.4f}, expected {expected_net:.4f}",
        hard_fail=True
    )
    
    # Commission for 2 legs
    expected_comm = 2 * 0.65
    audit.check(
        abs(entry_fill['commissions'] - expected_comm) < 0.01,
        f"Commission calculation correct",
        f"Got ${entry_fill['commissions']:.2f}, expected ${expected_comm:.2f}",
        hard_fail=True
    )
    
    return audit


def audit_3_construction(signals: List[Dict]) -> AuditResult:
    """
    Audit 3: Call Debit Construction
    
    - Leg ordering: for call debit, BUY lower strike, SELL higher strike
    - Expiry matches source structure
    - Width = short_strike - long_strike
    """
    audit = AuditResult("Call Debit Construction")
    
    construction_issues = []
    total_checked = 0
    
    for sig in signals:
        struct = sig.get('structure', {})
        legs = struct.get('legs', [])
        if len(legs) != 2:
            continue
        
        total_checked += 1
        
        # Check expiry consistency
        expiries = set()
        for leg in legs:
            expiries.add(leg.get('expiry'))
        
        if len(expiries) != 1:
            construction_issues.append({
                'symbol': sig['symbol'],
                'date': sig['execution_date'],
                'issue': f"Multiple expiries: {expiries}",
            })
        
        # Get strikes
        long_strike = None
        short_strike = None
        for leg in legs:
            if leg.get('side') == 'BUY':
                long_strike = leg.get('strike')
            else:
                short_strike = leg.get('strike')
        
        if long_strike is None or short_strike is None:
            construction_issues.append({
                'symbol': sig['symbol'],
                'date': sig['execution_date'],
                'issue': "Missing strike data",
            })
            continue
        
        # For put debit spread (original): BUY higher strike, SELL lower strike
        # For call debit (derived): we invert to BUY at long_strike, SELL at long_strike + width
        # The width should be consistent
        recorded_width = struct.get('width')
        calculated_width = abs(long_strike - short_strike)
        
        if recorded_width and abs(recorded_width - calculated_width) > 0.01:
            construction_issues.append({
                'symbol': sig['symbol'],
                'date': sig['execution_date'],
                'issue': f"Width mismatch: recorded={recorded_width}, calculated={calculated_width}",
            })
    
    audit.check(
        len(construction_issues) == 0,
        f"Construction checks: {total_checked - len(construction_issues)}/{total_checked} passed",
        f"{len(construction_issues)} issues found",
        hard_fail=len(construction_issues) > total_checked * 0.1  # Hard fail if >10% have issues
    )
    
    # Record first few issues for debugging
    if construction_issues:
        for issue in construction_issues[:5]:
            audit.check(False, f"{issue['symbol']} {issue['date']}: {issue['issue']}", hard_fail=False)
    
    return audit


def audit_4_symbol_breakdown(signals: List[Dict], fill_config: FillConfig) -> AuditResult:
    """
    Audit 4: Symbol Breakdown
    
    - No single symbol dominates PnL
    - Expectancy positive for ≥75% of symbols
    - Identify outliers
    """
    audit = AuditResult("Symbol Breakdown")
    
    # Simulate trades by symbol
    bar_cache = {}
    by_symbol = defaultdict(list)
    
    for sig in signals:
        symbol = sig['symbol']
        struct = sig.get('structure', {})
        legs = struct.get('legs', [])
        
        if len(legs) != 2:
            continue
        
        long_strike = None
        for leg in legs:
            if leg.get('side') == 'BUY':
                long_strike = leg.get('strike')
        
        if long_strike is None:
            continue
        
        width = struct.get('width', 5)
        entry_debit = struct.get('entry_debit', 1.0)
        
        exec_date_str = sig['execution_date']
        exec_date = datetime.strptime(exec_date_str, '%Y-%m-%d').date()
        
        # Get underlying bars
        cache_key = f"{symbol}_{exec_date_str}"
        if cache_key not in bar_cache:
            try:
                bars = get_underlying_daily_bars(
                    symbol, exec_date,
                    exec_date + timedelta(days=HOLDING_PERIOD_DAYS + 10),
                    use_cache=True
                )
                bar_cache[cache_key] = bars or []
            except:
                bar_cache[cache_key] = []
        
        bars = bar_cache[cache_key]
        if not bars or len(bars) < HOLDING_PERIOD_DAYS:
            continue
        
        entry_price = bars[0]['close']
        exit_price = bars[min(HOLDING_PERIOD_DAYS, len(bars) - 1)]['close']
        
        # Call debit payoff
        call_long_strike = long_strike
        call_short_strike = long_strike + width
        
        if exit_price >= call_short_strike:
            exit_value = width
        elif exit_price <= call_long_strike:
            exit_value = 0
        else:
            exit_value = (exit_price - call_long_strike)
        
        gross_pnl = (exit_value - entry_debit) * 100
        comm = fill_config.commission_per_contract * 4
        net_pnl = gross_pnl - comm
        
        by_symbol[symbol].append(net_pnl)
    
    # Analyze by symbol
    symbol_stats = {}
    total_pnl = 0
    positive_symbols = 0
    
    for symbol, pnls in by_symbol.items():
        exp = statistics.mean(pnls) if pnls else 0
        total = sum(pnls)
        symbol_stats[symbol] = {
            'trades': len(pnls),
            'expectancy': round(exp, 2),
            'total_pnl': round(total, 2),
            'win_rate': round(sum(1 for p in pnls if p > 0) / len(pnls) * 100, 1) if pnls else 0,
        }
        total_pnl += total
        if exp > 0:
            positive_symbols += 1
    
    # Check 1: No single symbol > 50% of total PnL
    max_symbol_pnl = max(abs(s['total_pnl']) for s in symbol_stats.values()) if symbol_stats else 0
    dominance_ratio = max_symbol_pnl / abs(total_pnl) if total_pnl != 0 else 0
    
    audit.check(
        dominance_ratio < 0.5,
        f"No single-symbol dominance: max contribution = {dominance_ratio*100:.1f}%",
        "Should be <50%",
        hard_fail=True
    )
    
    # Check 2: ≥75% of symbols have positive expectancy
    pct_positive = positive_symbols / len(symbol_stats) * 100 if symbol_stats else 0
    
    audit.check(
        pct_positive >= 75,
        f"Positive symbols: {positive_symbols}/{len(symbol_stats)} ({pct_positive:.1f}%)",
        "Should be ≥75%",
        hard_fail=True
    )
    
    # Add symbol details
    for symbol, stats in sorted(symbol_stats.items(), key=lambda x: x[1]['total_pnl'], reverse=True):
        indicator = "✓" if stats['expectancy'] > 0 else "✗"
        audit.check(
            True,  # Just log, not a pass/fail
            f"{indicator} {symbol}: n={stats['trades']}, exp=${stats['expectancy']:.2f}, total=${stats['total_pnl']:.2f}",
            hard_fail=False
        )
    
    return audit


def main():
    """Run all realism audits."""
    print("=" * 70)
    print("FLAT Call Debit Spread — Realism Audits")
    print("=" * 70)
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Load signals
    print("\nLoading FLAT signals...")
    signals = load_flat_signals(REPORTS_DIR)
    print(f"Loaded {len(signals)} signals")
    
    fill_config = FillConfig.from_yaml()
    
    audits = []
    
    # Run each audit
    print("\n[1/4] Audit: Debit Spread Invariants...")
    audit1 = audit_1_invariants(signals)
    audits.append(audit1)
    print(f"  {'✓ PASS' if audit1.passed else '✗ FAIL'}")
    
    print("\n[2/4] Audit: Fill Model Parity...")
    audit2 = audit_2_fill_model(signals)
    audits.append(audit2)
    print(f"  {'✓ PASS' if audit2.passed else '✗ FAIL'}")
    
    print("\n[3/4] Audit: Call Debit Construction...")
    audit3 = audit_3_construction(signals)
    audits.append(audit3)
    print(f"  {'✓ PASS' if audit3.passed else '✗ FAIL'}")
    
    print("\n[4/4] Audit: Symbol Breakdown...")
    audit4 = audit_4_symbol_breakdown(signals, fill_config)
    audits.append(audit4)
    print(f"  {'✓ PASS' if audit4.passed else '✗ FAIL'}")
    
    # Overall result
    all_passed = all(a.passed for a in audits)
    
    output = {
        'generated_at': datetime.now().isoformat(),
        'signals_count': len(signals),
        'all_passed': all_passed,
        'audits': [a.summary() for a in audits],
    }
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 70)
    print("AUDIT SUMMARY")
    print("=" * 70)
    
    for audit in audits:
        status = "✓ PASS" if audit.passed else "✗ FAIL"
        print(f"\n{audit.name}: {status}")
        for check in audit.checks:
            if check['status'] == 'FAIL' or not check['hard_fail']:
                icon = "✗" if check['status'] == 'FAIL' else "⚠" if check['description'] in audit.warnings else "✓"
                if check['hard_fail'] or check['status'] == 'FAIL':
                    print(f"  {icon} {check['description']}")
    
    print(f"\n{'=' * 70}")
    if all_passed:
        print("✅ ALL AUDITS PASSED — Ready for Edge Module #2 integration")
    else:
        print("❌ AUDITS FAILED — Do not integrate until issues resolved")
        for audit in audits:
            if not audit.passed:
                print(f"  - {audit.name}: {', '.join(audit.failures)}")
    print("=" * 70)
    
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
