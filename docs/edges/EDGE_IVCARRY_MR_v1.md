# EDGE: IV Carry MR v1

> **Status**: PRODUCTION (2025-12-22)  
> **Universe**: SPY, QQQ, DIA, XLK, XLE  
> **Strategy**: IV Mean-Reversion via Credit Spreads

---

## Terminology

| Term | Definition |
|------|------------|
| **Signal** | Detector output (date, z-score, direction, trend) |
| **TradeCandidate** | Signal + fully specified credit-spread structure (legs, strikes, expiry, max_loss_usd) |
| **Executed Trade** | RiskEngine-approved TradeCandidate successfully simulated using flatfiles |

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
anchor_strike = round(underlying_price / 5) * 5
short_strike = min(available_strikes, key=lambda x: abs(x - anchor_strike))
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

## RiskEngine Config (FROZEN)

| Parameter | Value |
|-----------|-------|
| `initial_equity` | $25,000 |
| `risk_per_trade_pct` | 2% |
| `max_open_positions` | 6 |
| `max_total_risk_pct` | 6% |
| `clusters` | index_core=[SPY,QQQ,DIA], sector_xlk=[XLK], sector_xle=[XLE] |
| `max_cluster_positions` | 3 (index_core), 1 (sectors) |
| `dd_kill_pct` | 15% |

> **Effective cap ≈ 3 concurrent positions** at full size due to max_total_risk_pct (6% ÷ 2% = 3).

---

## Confirmed Baseline (LOCKED)

### Portfolio (with RiskEngine)
| Metric | Value |
|--------|-------|
| Trades (4yr) | 52 |
| Win Rate | 71.2% |
| Profit Factor | **2.22** |
| Total Return | +15.2% |
| CAGR | 3.6% |

> Trades per symbol reflect RiskEngine-executed trades, not raw signals.

### Walk-Forward Validation
| Period | Trades | PF | Return |
|--------|--------|-----|--------|
| In-sample (22-23) | 24 | 1.15 | +1.3% |
| **Out-of-sample (24-25)** | 28 | **5.06** | +13.8% |

### Friction Sensitivity
| Test | PF |
|------|-----|
| Base | 2.38 |
| +$0.10/leg | **1.59** ✅ |

---

## Production Universe (5 symbols) ✅

| Symbol | Trades | PF | WR |
|--------|--------|-----|-----|
| SPY | 12 | 1.82 | 75% |
| QQQ | 12 | 1.76 | 58% |
| DIA | 9 | ∞* | 89% |
| XLK | 6 | 5.11 | 67% |
| XLE | 13 | 2.48 | 69% |

> **DIA Caveat**: PF=∞ due to zero losses in sample. Treat as unstable—monitor forward. **Do not increase DIA sizing based on PF alone.**

---

## Admission Criteria

| Status | Requirements |
|--------|-------------|
| **Production** | PF ≥ 1.3 AND DD ≤ 20% AND (≥20 trades OR ≥90% executable) |
| **Marginal** | PF 1.1–1.3 OR low trade count |
| **Quarantine** | Data-limited (skip rate > 10% OR clustering) |
| **Excluded** | PF < 1.1 OR negative expectancy |

### Executable Rate Definition
```
executable_rate = executed_trades / total_signals_loaded
```
- Denominator: signals after filtering for signal gates, per symbol
- Data source: flatfiles only (no REST fallback)

### Data Integrity Requirements
- **Target**: skip rate ≤ 5%
- **Allowed**: skip rate ≤ 10% only if:
  - Not clustered in specific time periods
  - Reason-coded
  - Coverage artifacts present

---

## Excluded ❌

| Symbol | PF | Reason |
|--------|----|--------|
| IWM | 0.97 | Negative expectancy |
| GLD | 0.67 | Negative expectancy |
| XLF | 1.27 | Below 1.3 threshold |
| TLT | 1.18 | Below 1.3 threshold |

---

## Validation Artifacts ✅

| Artifact | Status | Path |
|----------|--------|------|
| Coverage report | ✅ Complete | `validation/coverage_iv_carry_mr.jsonl` |
| Missingness heatmap | ✅ Complete | `validation/missingness_heatmap_iv_carry_mr.json` |
| Signal stats | ✅ Complete | `validation/signal_stats_iv_carry_mr.json` |
| Tradeability report | ✅ Complete | `validation/tradeability_iv_carry_mr.json` |
| Skip-rate remediation | ✅ Complete | `validation/skip_rate_remediation_iv_carry_mr.md` |

> All artifacts in `logs/edges/iv_carry_mr/validation/`

---

## Invariants (LOCKED)

1. **No parameter changes without v2**
2. **No structural changes without v2**
3. **No FLAT code mixing**
4. Baselines in `logs/edges/iv_carry_mr/locked_baselines/`

---

## Files

| Purpose | Path |
|---------|------|
| Config | `edges/iv_carry_mr/config.py` |
| Signal | `edges/iv_carry_mr/signal.py` |
| Backtest | `scripts/run_iv_carry_backtest.py` |
| Baselines | `logs/edges/iv_carry_mr/locked_baselines/` |
