# FLAT Call Debit — Dashboard Guide

**Edge Module #2** | Status: Validated | Frozen: Yes

---

## Quick Reference

| Metric | Value |
|--------|-------|
| Edge Type | FLAT Skew (≤10th percentile) |
| Structure | Call Debit Spread |
| Direction | Bullish (long underlying) |
| Win Rate | 67.6% |
| Expectancy | $156/trade |
| Profit Factor | 4.82 |

---

## When This Edge Fires

FLAT signals occur when put/call skew is **unusually low**:
- Skew percentile ≤ 10th
- `is_flat == 1.0` in edge metrics
- OR `direction == LONG` for skew_extreme edges

**Thesis**: Low fear premium → market continuation → call debit profits

---

## Structure Details

```
CALL DEBIT SPREAD

      MAX PROFIT ($width - debit)
           ↑
           │     ╱───────
           │    ╱
    0 ─────┼───╱─────────── Underlying
           │  ╱
           │ ╱
           ↓
      MAX LOSS ($debit)
           
    Long Call        Short Call
    (ATM)            (ATM + $5)
```

**Parameters**:
- Width: $5.00
- DTE: 21-45 days
- Hold: 14 days
- Exit: 80% profit / 50% loss / 7 DTE

---

## Symbol Coverage

| Symbol | Status | Strike Increment |
|--------|--------|------------------|
| SPY | ✓ Enabled | $1.00 |
| QQQ | ✓ Enabled | $1.00 |
| IWM | ✓ Enabled | $1.00 |
| XLF | ✓ Enabled | $0.50 |
| GLD | ✓ Enabled | $1.00 |
| TLT | ✓ Enabled | $1.00 |
| DIA | ✓ Enabled | $1.00 |
| EEM | ✗ Excluded | $0.50 (mismatch) |

---

## Validation History

### Research Metrics (111 trades, 2022-2024)

| Year | Trades | Win Rate | Expectancy |
|------|--------|----------|------------|
| 2022 | 18 | 55.6% | $104 |
| 2023 | 65 | 73.8% | $186 |
| 2024 | 28 | 60.7% | $120 |

### Audits Passed ✓
1. Debit spread invariants (max profit/loss correct)
2. Fill model parity (slippage/commissions match)
3. Call debit construction (leg ordering correct)
4. Symbol breakdown (no single-symbol dominance)

---

## How It Differs from Edge Module #1

| Aspect | Module #1 (STEEP) | Module #2 (FLAT) |
|--------|-------------------|------------------|
| Signal | High skew (≥90th pctl) | Low skew (≤10th pctl) |
| Structure | Credit put spread | Debit call spread |
| Direction | Short vol | Long underlying |
| Thesis | Sell fear premium | Ride continuation |

**No conflict**: FLAT and STEEP are mutually exclusive by definition.

---

## To Enable in Production

Add to `config/backtest.yaml`:

```yaml
strategies:
  flat_call_debit:
    enabled: true
    enabled_symbols: [SPY, QQQ, IWM, XLF, GLD, TLT, DIA]
    min_strength: 0.40
    
    structure:
      type: call_debit_spread
      width: 5.0
      min_dte: 21
      max_dte: 45
    
    exit_rules:
      take_profit_pct: 80
      stop_loss_pct: 50
      time_stop_dte: 7
```

---

## Files

| File | Purpose |
|------|---------|
| `edges/flat_call_debit/__init__.py` | Module logic (FROZEN) |
| `edges/flat_call_debit/config.yaml` | Parameters & validation |
| `scripts/research/flat_*.py` | Research scripts |
| `logs/research/flat_*.json` | Research outputs |
