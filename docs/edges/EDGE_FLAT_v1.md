# EDGE: FLAT v1

> **Status**: LOCKED (2025-12-20)  
> **Strategy**: Skew Mean-Reversion via Call Debit Spreads  
> **Expected Performance**: ~22 trades/4yr, ~77% WR, PF ~2.0+

## Signal Definition

**FLAT** is a bullish mean-reversion trade triggered when:
- Put skew is **unusually flat** (low put premium vs historical)
- The flat condition is **reverting** toward normal
- Volatility environment is **not extreme** (IVp ≤ 75)

### Trigger Conditions
| Field | Requirement |
|-------|-------------|
| `is_flat` | `== 1.0` |
| `atm_iv_percentile` | `<= 75` (mandatory) |
| `atm_iv_percentile` | Valid (not None, not ≤0, not >100) |
| `skew_percentile` | Low (handled by signal generator) |
| `skew_reverting` | True (Δ≥0.5% or pctl_change≥10) |

## Structure

**Call Debit Spread** (bullish, defined risk)

| Component | Value |
|-----------|-------|
| Type | `call_debit_spread` |
| Direction | `LONG` |
| Long Leg | ATM call (buy) |
| Short Leg | OTM call (sell) |
| Width | $5 or $10 cascade |
| Max Loss | Entry debit × 100 |
| Max Profit | Width - Entry debit |

## Entry Timing

- **Signal Date**: N (report generated after close)
- **Execution Date**: N+1 (trade at next open)
- Fill model: `close_plus_slippage`

## Exit Rules (LOCKED)

| Rule | Value |
|------|-------|
| Take Profit | **70%** of max profit |
| Stop Loss | **Disabled** |
| Time Stop | **DTE ≤ 7** |
| Early Exit | **Disabled** (tested, destroyed expectancy) |

## Regime Gate (MANDATORY)

```yaml
regime_gate:
  flat:
    max_atm_iv_pctl: 75
```

Signals with `atm_iv_percentile > 75` are rejected.

### Regime Bands
| IVp Range | Action |
|-----------|--------|
| 🟢 ≤ 75 | Trade (aggressive) |
| 🟡 75-80 | Cautious |
| 🔴 > 80 | Do not trade |

## Production Universe (18 symbols)

### Tier-1 (17 symbols) - Always active
```
SPY QQQ IWM DIA
XLF XLE XLK XLI XLY XLP XLU
TLT IEF
GLD SLV USO EEM
```

### Tier-1.5 (1 symbol) - Optional
```
XLV (Healthcare - defensive, mean-reverting)
```

### Explicitly Excluded
```
SMH SOXX XLB TIP IJR IWF IWB IWD IWP
```
Reason: Negative expectancy under IVp≤75 gate

## Risk Math

| Parameter | Value |
|-----------|-------|
| `initial_equity` | $25,000 |
| `risk_per_trade_pct` | 2% ($500) |
| `max_open_positions` | 3 |
| `max_total_risk_pct` | 6% ($1,500) |
| `cluster_dedup_mode` | `best_edge` |
| `max_cluster_positions` | 2 |

## Acceptance Tests

### Test A: Phase 1 (Edge Existence)
```bash
python3 scripts/run_backtest.py \
  --input-dir logs/backfill/v7/reports \
  --years 4 --phase phase1 --edge-slice flat \
  --symbols SPY QQQ IWM DIA XLF XLE XLK XLI XLY XLP XLU TLT IEF GLD SLV USO EEM \
  --force
```
**Expected**: ~22 trades, ~77% WR, PF ~2.0+

### Test B: Phase 2 (Tradeability)
```bash
python3 scripts/run_backtest.py \
  --input-dir logs/backfill/v7/reports \
  --years 4 --phase phase2 --edge-slice flat \
  --symbols SPY QQQ IWM DIA XLF XLE XLK XLI XLY XLP XLU TLT IEF GLD SLV USO EEM \
  --force
```
**Expected**: ~18 trades, no `max_risk_per_trade_exceeded` rejections

## Data Sources

| Data | Source |
|------|--------|
| Option bars | Polygon flat files via `OptionBarStore` |
| Underlying | Polygon API (`adjusted=false`) |
| Reports | `logs/backfill/v7/reports` |

## Invariants (Do Not Change)

1. Debit spreads are NOT flagged unexecutable by credit-spread logic
2. Option bar retrieval uses flat files, not Polygon REST
3. OCC symbols are not double-prefixed (`O:O:`)
4. IVp validity check rejects None/≤0/>100

## Version History

| Version | Date | Change |
|---------|------|--------|
| v1.0 | 2025-12-20 | Initial locked version |
