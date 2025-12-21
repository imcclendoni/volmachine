# FLAT Skew Signal Thesis

**Status**: Under Investigation (Agent #2 Research)  
**Date**: 2025-12-19

---

## What is a FLAT Signal?

FLAT signals occur when put/call IV skew is **unusually low** (≤10th percentile):
- Puts are **not** significantly more expensive than calls
- This is atypical—normally puts carry a crash premium

FLAT days may represent:
1. **Complacency**: Market participants are not demanding crash protection
2. **Call-rich flow**: Unusual demand for upside (speculation or hedging)
3. **Mean-reversion setup**: Skew often re-widens after flat episodes

---

## Thesis Candidates

### Thesis 1: Debit Put Spread (Tail Protection)

> When skew is flat, tail protection is cheap. Buy it.

**Rationale:**
- Low put premium = cheap convexity
- If skew mean-reverts (puts get more expensive), spread value increases
- Downside protection at discount

**Trade Expression:**
- **Structure**: Debit put spread (BUY ATM-5, SELL ATM-10)
- **Entry**: Signal day + 1 (execution date)
- **Exit**: 14 days or expiry
- **Target DTE**: 21-45 days

**Regime Filters:**
- May work better in **low/mid IV environments** (more room for expansion)
- Avoid if **already in high-vol panic** (skew already steep by definition)

---

### Thesis 2: Call Debit Spread (Continuation)

> Flat skew + low fear = risk-on continuation.

**Rationale:**
- Flat skew often coincides with bullish sentiment
- Market may continue grinding higher
- Call spreads profit from upside moves

**Trade Expression:**
- **Structure**: Call debit spread (BUY ATM, SELL ATM+5)
- **Entry**: Signal day + 1
- **Exit**: 14 days or expiry
- **Target DTE**: 21-45 days

**Regime Filters:**
- Works best in **trend_up** or **low_vol_grind** regimes
- Avoid in **high_vol_panic** or **trend_down**

---

### Thesis 3: Skew Normalization (Vega Play)

> Flat skew will widen—position for vega expansion.

**Rationale:**
- Skew is mean-reverting
- When flat, both puts and calls are "cheap"
- Buying vol at bottom of range

**Trade Expression:**
- **Structure**: Long strangle or calendar spread (requires different data)
- **Note**: Not testable with current daily bar data

**Status**: Deferred—insufficient data for proper simulation.

---

## Rejection Criteria

FLAT signals should be **dropped** from the trading system if:

1. **Expectancy ≤ 0** after 4-year backtest across all structures
2. **Profit factor < 1.0** in every IV regime bucket
3. **Forward returns show no directional bias** (+5D, +20D mean ≈ 0)
4. **Win rate < 40%** with no tail compensation (tail ratio < 1.0)

## Decision Framework

| Outcome | Action |
|---------|--------|
| Debit put spread PF > 1.2, expectancy > $20 | Enable in pipeline with IV filter |
| Call debit spread PF > 1.2, expectancy > $20 | Consider as alternative expression |
| Both structures PF < 1.0 | FLAT is noise—focus on STEEP only |
| Mixed results by IV bucket | Add regime filter, enable selectively |

---

## Next Steps

1. Run `flat_diagnostics.py` → examine forward return distribution
2. Run `flat_structure_ablation.py` → compare structure performance
3. Review results against rejection criteria
4. Document findings for pipeline integration decision
