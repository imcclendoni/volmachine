# IV Carry MR v1 - Session Summary (2025-12-21)

## Current Status: RiskEngine Tuning In Progress

### Production Universe (validated)
```
SPY QQQ DIA XLK XLE
```

### Edge Parameters (FROZEN)
- IV z-score threshold: ≥ 2.0
- Lookback: 120 days
- RV/IV max: < 1.0
- RV window: 20 days
- Trend gate: MA20 vs MA60
- DTE range: 30-45
- Exit: 50% take profit, 7 DTE time stop

---

## Backtest Results Comparison

### 1. Naive (no RiskEngine)
- Trades: 114
- PF: 3.69
- Return: +70%
- Max DD: 9.9%

### 2. Best-First with RiskEngine (latest)
- Config: max_pos=6, max_risk=6%, index_core[cap=2], sector_xlk[cap=1], sector_xle[cap=1]
- Trades: 36
- PF: 1.66
- Return: +7.1%
- CAGR: 1.7%

### Rejection Distribution (healthy)
- same_day_dedup: 32
- cluster_position_cap: 28
- max_total_risk: 15 ✅ (binding - good!)
- max_positions: 2

### By Symbol
| Symbol | Trades | PnL | WR |
|--------|--------|-----|-----|
| SPY | 7 | +$621 | 71% |
| QQQ | 8 | -$536 | 50% |
| DIA | 3 | +$955 | 100% |
| XLK | 7 | +$216 | 71% |
| XLE | 11 | +$523 | 73% |

---

## Open Questions

1. **Raise sector caps to 2?** (allow more XLK/XLE trades per day)
2. **QQQ underperforming** - investigate if data issue or edge weakness
3. **Same-day dedup rejecting 32 signals** - too aggressive?

---

## Key Files

### Documentation
- `/docs/edges/EDGE_IVCARRY_MR_v1.md` - Edge specification (frozen)

### Baselines
- `/logs/edges/iv_carry_mr/locked_baselines/SPY_baseline.json`
- `/logs/edges/iv_carry_mr/locked_baselines/QQQ_baseline.json`
- `/logs/edges/iv_carry_mr/locked_baselines/DIA_baseline.json`
- `/logs/edges/iv_carry_mr/locked_baselines/XLE_baseline.json`
- `/logs/edges/iv_carry_mr/locked_baselines/XLK_baseline.json`
- `/logs/edges/iv_carry_mr/locked_baselines/PORTFOLIO_baseline.json`

### Signal Data
- `/logs/backfill/iv_carry_mr/reports/` - 127 signals across 5 symbols

### Code
- `/edges/iv_carry_mr/config.py` - Configuration
- `/edges/iv_carry_mr/signal.py` - Signal detection
- `/scripts/run_iv_carry_backtest.py` - Backtest runner
- `/scripts/backfill_iv_carry_signals.py` - Signal generation

---

## Quarantine Symbols (track forward only)
- XLY, XLI, XLP - data-limited, survivorship bias

## Excluded Symbols
- IWM (PF 0.97), XLF (PF 1.27), TLT (PF 1.18), GLD (PF 0.67)

---

## Next Steps

1. Decide on RiskEngine config (looser sector caps vs conservative)
2. Save final portfolio baseline
3. Implement forward signal tracking for quarantine symbols
4. Consider Term Structure MR edge validation next
