# EDGE: IV Carry MR v1

> **Status**: PRODUCTION (2025-12-21)  
> **Universe**: SPY, QQQ, DIA, XLK, XLE  
> **Strategy**: IV Mean-Reversion via Credit Spreads

---

## Signal Definition (FROZEN)

| Parameter | Value |
|-----------|-------|
| `iv_zscore_threshold` | ≥ 2.0 |
| `lookback_days` | 120 |
| `rv_iv_max` | < 1.0 |
| `rv_window` | 20 days (close-to-close log returns, annualized) |
| `trend_gate` | MA20 vs MA60 (no tolerance, pure > or <) |
| `dte_range` | 30-45 days |

---

## Structure (FROZEN)

### Strike Selection
```python
# Anchor to nearest $5 grid point, then find closest available
anchor_strike = round(underlying_price / 5) * 5
short_strike = min(available_strikes, key=lambda x: abs(x - anchor_strike))
# Note: For symbols with $1 or $0.50 increments, adjust anchor logic
```

### Direction Logic
| Trend | Structure |
|-------|-----------|
| `MA20 > MA60` | Put Credit Spread |
| `MA20 < MA60` | Call Credit Spread |

### Width: $5 primary, $10 fallback

---

## Exit Rules (FROZEN)

| Rule | Value |
|------|-------|
| Take Profit | 50% of credit |
| Time Stop | DTE ≤ 7 |
| Stop Loss | Disabled |

---

## Risk Math (FROZEN)

| Parameter | Value |
|-----------|-------|
| `initial_equity` | $25,000 |
| `risk_per_trade_pct` | 2% |
| Position sizing | `contracts = floor(risk_budget / max_loss)` |

---

## Production Universe (5 symbols) ✅

| Symbol | Trades | Skipped | Edge PF | WR | DD (Phase 2) | Return (Phase 2) |
|--------|--------|---------|---------|-----|--------------|------------------|
| SPY | 32 | 0 | 1.82 | 69% | 2.3% | +14% |
| QQQ | 31 | 0 | 1.76 | 71% | 7.4% | +18% |
| DIA | 20 | 2 | inf* | 90% | 0.0%* | +18% |
| XLK | 15 | 5 | 5.11 | 67% | 0.9% | +5% |
| XLE | 21 | 6 | 2.48 | 71% | 1.6% | +5% |

> *DIA: PF=infinity (no realized losses). This is valid but **fragile**—any future loss will collapse PF toward normal. Treat as promising but unproven at scale.

---

## Admission Criteria

| Status | Requirements |
|--------|-------------|
| **Production** | PF ≥ 1.3 AND DD ≤ 20% AND (≥20 trades OR ≥90% executable) |
| **Marginal** | PF 1.1–1.3 OR low trade count |
| **Quarantine** | Data-limited (high skip rate, survivorship bias) |
| **Excluded** | PF < 1.1 OR negative expectancy |

---

## Marginal (acceptable for smoothing) ⚠️

| Symbol | PF | Issue |
|--------|----|-------|
| XLF | 1.27 | Below 1.3 threshold |
| TLT | 1.18 | Below 1.3 threshold |

---

## Quarantine (data-limited, track forward only) 🧪

| Symbol | Trades | Skipped | Issue |
|--------|--------|---------|-------|
| XLY | 8 | 7 | Survivorship bias - early signals missing |
| XLI | 3 | 6 | Too few trades |
| XLP | 6 | 14 | High skip rate |

**Graduation:** ≥20 executed trades OR ≥2 years forward signals with PF ≥ 1.3

---

## Excluded (failed validation) ❌

| Symbol | PF | Reason |
|--------|----|--------|
| IWM | 0.97 | Negative expectancy |
| GLD | 0.67 | Negative expectancy |

---

## Invariants (LOCKED)

1. No parameter changes without v2
2. No structural changes without v2
3. No FLAT code mixing
4. Baselines in `logs/edges/iv_carry_mr/locked_baselines/`

---

## Files

| Purpose | Path |
|---------|------|
| Config | `edges/iv_carry_mr/config.py` |
| Signal | `edges/iv_carry_mr/signal.py` |
| Backtest | `scripts/run_iv_carry_backtest.py` |
| Baselines | `logs/edges/iv_carry_mr/locked_baselines/` |
