A “mental model doc” for Opus is a short, unambiguous spec that prevents it from drifting between conflicting goals (prove edge vs trade safely vs increase frequency). It’s basically: what we’re building, what success looks like, what not to change, and the exact phased workflow.

Here’s one you can paste into Opus as the source-of-truth.

⸻

VolMachine v4 Mental Model (Source of Truth for Opus)

0) Purpose

VolMachine v4 is a research-correct, audit-grade options signal + deterministic backtest system designed to:
	1.	Prove or disprove a specific statistical edge (skew mean-reversion) with no lookahead
	2.	Only after proof, constrain the system with portfolio/risk rules for live-style realism
	3.	Only after that, optimize parameters (trade frequency is last, not first)

1) What This Strategy Is

This is a rare-event system.
	•	We compute put-call skew using 25Δ IVs
	•	We compute a percentile of today’s skew vs trailing history
	•	We only act on tail events (≥90th or ≤10th percentile)
	•	We require mean-reversion confirmation (not momentum chasing)
	•	We express trades as spreads (currently credit spreads enabled)

Expected trade frequency is naturally low.
Low trade count does not imply a bug.

2) Non-Negotiables (Do Not Change)

These are correctness guarantees:
	•	No lookahead: signals at day t execute at day t+1
	•	History integrity: when validating multi-year, use --fresh-history
	•	Coverage gating: backtest is invalid if coverage < threshold for period
	•	Schema normalization: option bars are {open, high, low, close, volume} everywhere
	•	Flat files are canonical for historical options bars (REST only for recent days / underlying)

If any of these are violated, results are “DO NOT USE”.

3) The Three Phases (Do Not Mix Them)

Phase 1 — Edge Existence (Research mode)

Goal: Answer only one question: Is expectancy positive before portfolio constraints?

Settings:
	•	portfolio_mode: signal_quality (RiskEngine OFF)
	•	Allow “all valid trades” (no cluster caps, no cooldowns)
	•	Use strict fills if desired, but keep consistent
	•	Keep extreme thresholds & mean-reversion gating unchanged
	•	Run 3–4 years minimum

Outputs to report:
	•	Trades count
	•	Win rate
	•	Avg win / avg loss
	•	Profit factor
	•	Tail ratio
	•	Net expectancy per trade
	•	Equity curve max drawdown (research)

If Phase 1 expectancy is negative → stop optimizing risk engine and revisit edge definition.

Phase 2 — Tradeability (Portfolio mode)

Goal: Measure how much “safety” costs.

Settings:
	•	portfolio_mode: portfolio_safe (RiskEngine ON)
	•	Cluster caps, cooldowns, DD kill switch enabled
	•	Same entry/exit logic, same signal logic

Outputs to report:
	•	Δ in trade count vs Phase 1
	•	Δ in expectancy vs Phase 1
	•	Rejection breakdown by reason (dedup, cluster cap, cooldown)
	•	Year-by-year summary

Phase 3 — Optimization (Only after Phase 1 is positive)

Goal: Improve risk-adjusted returns without breaking the thesis.

Allowed knobs:
	•	min_credit_to_width (execution feasibility)
	•	min_delta and min_percentile_change (signal sensitivity)
	•	DTE targeting / spread widths (risk profile)

Not allowed:
	•	weakening lookahead constraints
	•	removing coverage gating
	•	“optimizing” on short windows

4) Why Short Runs Show “No Signals”

With:
	•	MIN_HISTORY_FOR_PERCENTILE = 60
	•	a 90-day run has ~65 trading days

Only ~5 days are eligible for percentile evaluation after warm-up, so hitting ≥90th percentile is statistically unlikely. This is expected.

5) Thesis to Keep in Mind

We are exploiting skew mean-reversion:
	•	When put IV is extremely rich vs call IV (high skew percentile), it tends to normalize.
	•	Selling defined-risk premium (credit spreads) captures that normalization.
	•	The edge is not “always trade”; it’s “trade only when skew is abnormally extreme and stops worsening.”

6) Current Priority

Right now, prioritize:
	1.	Multi-year, fresh-history backfill
	2.	Phase 1 backtest (signal_quality)
	3.	Only then re-enable RiskEngine constraints

⸻
