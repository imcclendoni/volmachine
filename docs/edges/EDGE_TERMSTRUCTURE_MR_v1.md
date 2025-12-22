# EDGE: Term Structure Mean-Reversion v1

> **Status**: IN DEVELOPMENT (2025-12-21)  
> **Strategy**: Term Structure Mean-Reversion via Back-Expiry Debit Spreads  
> **Expected Performance**: ~8–15 trades/year, ~55–65% WR, PF ~1.3–1.6

## Signal Definition

**TS-MR** is a neutral-to-directional trade triggered when front-month IV becomes abnormally dislocated from back-month IV.

### Expiry Selection
| Expiry | DTE Range | Description |
|--------|-----------|-------------|
| Front | 20–35 DTE | Near-term IV |
| Back | 60–90 DTE | Intermediate IV |

### Signal Computation
```python
term_slope = IV_front - IV_back  # Positive = inverted
term_z = zscore(term_slope, rolling_window=120 days)
```

### Trigger Conditions
| Trigger | term_z | Interpretation |
|---------|--------|----------------|
| Long compression | ≥ +2.0 | Front IV inflated vs back |
| Short compression | ≤ −2.0 | Front IV depressed vs back |

## Structure

**Back-Expiry Vertical Debit Spread** (v1 default)

| Component | Value |
|-----------|-------|
| Type | `call_debit_spread` or `put_debit_spread` |
| Direction | Based on term_z sign |
| Expiry | Back expiry (60–90 DTE) |
| Long Leg | ATM option (buy) |
| Short Leg | OTM option (sell) |
| Width | $5 or $10 cascade |

## Regime Gate (MANDATORY)

```yaml
regime_gate:
  term_structure_mr:
    max_atm_iv_pctl: 85
    max_vix: 30
```

| Condition | Action |
|-----------|--------|
| ATM IV percentile > 85 | Reject |
| VIX > 30 | Reject |

## Production Universe (17 symbols)

Same Tier-1 universe as FLAT v1 (XLV excluded until Phase-1):
```
SPY QQQ IWM DIA
XLF XLE XLK XLI XLY XLP XLU
TLT IEF
GLD SLV USO EEM
```

## Risk Math

| Parameter | Value |
|-----------|-------|
| `initial_equity` | $25,000 |
| `risk_per_trade_pct` | 2% ($500) |
| `max_open_positions` | 3 |

## Exit Rules (Initial)

| Rule | Value |
|------|-------|
| Take Profit | 50% of max profit |
| Stop Loss | Disabled |
| Time Stop | DTE ≤ 14 |

## Data Sources

| Data | Source |
|------|--------|
| Option bars | Polygon flat files via `OptionBarStore` |
| ATM IV | Calculated from option prices |
| Reports | `logs/backfill/termstructure_mr/reports` |

## Version History

| Version | Date | Change |
|---------|------|--------|
| v1.0-dev | 2025-12-21 | Initial development version |
