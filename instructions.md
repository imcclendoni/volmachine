The single most important “tell Opus” request across these files
Make the engine time-consistent

Everywhere you currently use date.today() inside detection/building/pricing, replace with:

as_of_date or as_of_datetime from the chain snapshot / run_date

This matters for:

DTE selection

realized vol windows

IV history indexing

report reproducibility

backtests

Updated “tell Opus” list for these 3 files

Copy/paste to Opus:

Ensure SciPy is in requirements or implement pure-Python normal CDF/PDF + Brent fallback.

Pass as_of timestamp everywhere; eliminate date.today() defaults inside pricing/greeks/VRP.

Add dividend yield per symbol (universe.yaml) and pass q into BS pricing/greeks.

Move risk-free rate to config (no hardcoded 5%).

Replace grid-based calculate_max_loss/max_profit for defined-risk structures with analytic formulas based on legs + credit/debit; keep payoff curves only for plotting.

In greeks: if IV missing, compute IV from mid price using implied vol solver; don’t return zero Greeks unless truly unsolvable.

Delta-strike lookup should use actual chain strikes, not brute force scanning.

What’s good
* OptionContract.bid_ask_pct computed from mid is correct.
* OptionContract.model_post_init fills mid sensibly.
* OptionChain.get_atm_strike is fine.
* OptionStructure stores entry credit/debit, max_loss, max_profit, breakevens; this is correct for a v1 desk engine.
* Strong typing is helpful.

Critical issues and fixes
1) Unit clarity: all “structure” monetary fields are in points, not dollars
Right now:
* OptionStructure.entry_credit / entry_debit are option price points
* OptionStructure.max_loss / max_profit are points
* but multiple places print them as $ (engine/decision/reporting).
Tell Opus to fix:
Add explicit computed properties to the schema:

class OptionStructure(BaseModel):
    ...
    @property
    def entry_credit_dollars(self) -> Optional[float]:
        return self.entry_credit * 100 if self.entry_credit is not None else None
    ...
    @property
    def max_loss_dollars(self) -> float:
        return self.max_loss * 100
Then update all report formatting to use:
* *_dollars for printing $
* *_points (or plain fields) only for internal math
This will eliminate the biggest “machine says one thing, IBKR shows another” confusion.

2) OptionContract.model_post_init should not compute mid if bid/ask are stale or invalid
You currently compute mid whenever bid/ask >= 0, even if:
* ask = 0 (closed market / missing quote)
* bid = ask = 0
* bid > ask (bad feed)
That will generate fake mids and cause the engine to produce candidate credits that don’t exist.
Tell Opus to fix:
Harden mid computation:

if self.mid is None and self.bid is not None and self.ask is not None:
    if self.bid > 0 and self.ask > 0 and self.ask >= self.bid:
        self.mid = (self.bid + self.ask)/2
    else:
        self.mid = None
Also add a property:
* quote_is_valid boolean

3) Add as_of_date / as_of_datetime everywhere
You already have OptionChain.timestamp, which is great. But a lot of code still uses date.today().
Tell Opus:
* Use option_chain.timestamp.date() as the “as-of” in:
    * DTE selection
    * IV calculations
    * realized vol comparisons
    * VRP history indexing
This is required for after-hours correctness and any backtest reproducibility.

4) TradeCandidate.risk_per_contract field validation is wrong for “PASS”
Schema requires:

risk_per_contract: float = Field(gt=0)
total_risk: float = Field(gt=0)
But your sizing can reject and set zero values (or should). This can cause validation errors or force you to invent risk values.
Tell Opus:
* Change TradeCandidate constraints to allow 0 for PASS/REVIEW cases:

risk_per_contract: float = Field(ge=0)
total_risk: float = Field(ge=0)
Same for Position.entry_max_loss maybe, depending on structure types.

5) Greeks validators are too strict for theta
You have:

theta: float  # Usually negative for long options
But no constraints. That’s okay. Delta is constrained -1..1, good.
However, PositionGreeks.from_bs_output() multiplies delta by 100. That means structure net_delta can be >1, which is fine. But Greeks in OptionContract is per-option, not position. That’s fine.
No changes required here.

6) OptionChain.get_contract uses exact strike equality (float equality)
You do:

c.strike == strike
If strikes are floats like 707.5, and your builder uses round(spot) or integer strikes, you’ll miss exact matches, then find_contract fallback picks closest. That’s OK, but the exact-equality can still be brittle.
Tell Opus:
* Normalize strikes (e.g., round to nearest increment) inside provider ingestion, or
* change get_contract to allow tolerance based on tick size / strike increment.
Simplest:
* store strikes as exact decimals from provider and always select from actual chain strikes (best practice).

7) PortfolioState fields are inconsistent with intended meaning
In PortfolioState:

total_current_risk: float  # Current portfolio delta exposure
But the name implies dollar risk; comment implies delta exposure.
Tell Opus:
* Rename fields or fix semantics:
    * total_max_loss_dollars
    * total_risk_dollars
    * portfolio_delta is separate
Right now it’s confusing for reporting and kill switches.

The most important cross-file issue (ties schema to sizing bug)
Your schema confirms OptionStructure.max_loss is in points. That matches sizing’s * 100.
So the unit model is coherent — but only if you fix the earlier sizing bug:
✅ Do not force min 1 contract when raw_contracts < 1.
Otherwise, the engine will still recommend trades that violate your risk cap, regardless of schema.

What to tell Opus (copy/paste)
1. Add explicit unit helpers in OptionStructure: *_dollars properties; update reports/rationales to print dollars correctly.
2. Harden OptionContract mid calculation; if bid/ask invalid (0, bid>ask), set mid=None and mark quote invalid.
3. Replace all date.today() logic in engine/builders/detectors with option_chain.timestamp as-of date.
4. Relax TradeCandidate risk fields to allow 0 when recommendation is PASS/REVIEW.
5. Avoid float-equality strike matching; always select strikes from available chain strikes; optionally add strike increment/tolerance.
6. Clean up PortfolioState field naming (risk vs delta) to avoid confusing reports/limits.
A) engine/report.py audit
What’s good

Report sections are logically structured (regime → edges → candidates → portfolio → risk).

Using format_candidate_summary() is a good idea.

Critical problems
1) Units are wrong everywhere you print $

Your schema stores:

OptionStructure.max_loss in points (e.g., 0.75)

but the report prints:

Total Max Loss Exposure: ${p.total_max_loss:.2f}


Portfolio.get_total_max_loss() currently returns dollars (it multiplies by 100). But OptionStructure.max_loss prints as $ in candidate summary and rationale in engine/decision.py.

So right now:

some values are points but look like dollars

others are dollars

you cannot trust the report as an execution ticket

✅ Tell Opus:

Standardize: all “$” outputs must be dollars.

Add *_dollars properties on OptionStructure and PortfolioState and use those in report.

Update format_candidate_summary() to print:

credit/debit in dollars

max loss in dollars

max profit in dollars

Example:

Credit: $125 not $1.25

Max Loss: $75 not $0.75

2) HTML report is just a <pre> dump

Not a bug, just not a “desk report.” It’s okay for v1.

B) backtest/paper_simulator.py audit

This one has two serious logic errors that will give you incorrect PnL and incorrect risk tracking.

1) Exit slippage direction is wrong for credit vs debit

In close_position() you do:

fill_price = exit_price - slippage  # Receive less on close
pnl = self.portfolio.close_position(position_id, fill_price)


This assumes closing always means “receive less,” but:

Closing a credit spread means you BUY it back (pay a debit) → worse fill means pay more, so price should go up, not down.

Closing a debit spread means you SELL it → worse fill means receive less, so price goes down.

✅ Tell Opus:

Determine whether the position was entered as credit or debit and apply slippage accordingly:

If closing requires paying:

fill_price = exit_price + slippage

If closing results in receiving:

fill_price = exit_price - slippage

You need to store on Position whether it is “credit” or “debit” (or infer from structure.entry_credit).

2) PnL sign convention is inconsistent between simulator and portfolio

In execute_candidate() you pass:

entry_price = fill_price


Where fill_price is:

credit value (positive points) for credit spreads

debit value (positive points) for debit spreads

Then in Portfolio.close_position() you compute:

pnl = (exit_price - entry_price) * contracts * 100


That assumes “higher is better,” which is true for long (debit) positions (you want to sell higher), but for a credit position, profit is made when the spread value declines (you buy back cheaper).

So credit trades will report the wrong PnL sign unless entry/exit cashflow is modeled.

✅ Tell Opus:

Store cashflow not “price” for entry/exit.

Correct accounting model (simple):

For credit entry: entry_cashflow = +credit * 100 * contracts

For debit entry: entry_cashflow = -debit * 100 * contracts

For closing:

if you close a credit position, you pay a debit: exit_cashflow = -close_debit * 100 * contracts

if you close a debit position, you receive a credit: exit_cashflow = +close_credit * 100 * contracts

Then:

pnl = entry_cashflow + exit_cashflow


That is correct for everything.

3) PaperSimulator ignores sizing/validation flags in candidate

You only check:

if candidate.recommendation != "TRADE": pass


But your create_trade_candidate() can mark recommendation TRADE even if there are warnings (depending on logic changes).

✅ Tell Opus:

Require:

candidate.is_valid is True

sizing.allowed is True

validation.is_valid is True

max loss per trade under risk cap

4) Fill price is computed from structure.entry_credit, which is mid, not conservative

It’s okay for paper, but you should offer:

fill at mid / bid / ask, per config (you have fill_at but don’t use it)

currently you always use mid.

✅ Tell Opus:

Implement fill_at:

for credit: use bid (conservative)

for debit: use ask (conservative)

mid for neutral

C) Most important issue still unresolved: sizing bug forces 1 contract

Even with correct PnL, if sizing forces 1 contract when raw_contracts < 1, you will take trades that violate your risk cap.

This must be fixed before paper execution is meaningful.

Exactly what to tell Opus (copy/paste)

Fix units in reporting: Add *_dollars helpers and print dollars everywhere; never print points as $.

Fix PnL accounting: Track entry/exit cashflows (credit positive, debit negative). Compute pnl = entry_cashflow + exit_cashflow. Stop using (exit_price - entry_price) for credit positions.

Fix close slippage direction: credit close = pay more (price up), debit close = receive less (price down).

Implement fill_at config: mid/bid/ask behavior properly for entry and exit.

Paper simulator should only execute if candidate.is_valid and sizing.allowed and validation passed.

Fix sizing: do not force min 1 contract; reject trades that exceed risk cap.

Update portfolio/portfolio_state fields to clearly separate points vs dollars and delta exposure.