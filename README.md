# VolMachine - Options Volatility Trading Decision Engine

A production-grade volatility trading system that classifies market regimes, detects structural volatility mispricing, constructs defined-risk option structures, and enforces risk rules with full audit logging.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     VolMachineEngine                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐     │
│  │ Regime  │→→│  Edge   │→→│Structure│→→│    Risk     │     │
│  │ Engine  │  │Detectors│  │Builders │  │   Engine    │     │
│  └─────────┘  └─────────┘  └─────────┘  └─────────────┘     │
│       ↓            ↓            ↓              ↓             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │            Trade Candidate + Daily Report             │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
volmachine/
├── config/
│   ├── settings.yaml       # Global configuration
│   └── universe.yaml       # Symbol/asset configuration
├── data/
│   ├── schemas.py          # Pydantic data models
│   ├── cache.py            # Parquet-based caching
│   └── providers/          # Data provider adapters
│       ├── polygon.py
│       ├── tradier.py
│       └── ibkr.py (stub)
├── regime/
│   ├── features.py         # Regime feature extraction
│   ├── state_machine.py    # 5-state regime classifier
│   └── regime_engine.py    # Main regime orchestrator
├── edges/
│   ├── vol_risk_premium.py # VRP detector (IV vs RV)
│   ├── term_structure.py   # Contango/backwardation
│   ├── skew_extremes.py    # Put/call skew analysis
│   ├── event_vol.py        # Event premium detector
│   └── gamma_pressure.py   # Dealer gamma proxy
├── structures/
│   ├── pricing.py          # Black-Scholes model
│   ├── greeks.py           # Greeks calculation
│   ├── payoff.py           # P&L modeling
│   ├── builders.py         # Structure builders
│   └── validation.py       # Defined-risk validation
├── risk/
│   ├── sizing.py           # Position sizing
│   ├── limits.py           # Daily/weekly limits, kill switch
│   ├── stress.py           # Scenario stress testing
│   └── portfolio.py        # Portfolio management
├── engine/
│   ├── engine.py           # Main orchestrator
│   ├── decision.py         # Trade candidate objects
│   ├── logger.py           # Structured JSON logging
│   └── report.py           # Daily desk report generator
├── backtest/
│   ├── paper_simulator.py  # Paper trading with slippage
│   ├── metrics.py          # Sharpe, Sortino, drawdown
│   ├── walk_forward.py     # Walk-forward evaluation
│   └── event_study.py      # Historical event analysis
└── scripts/
    ├── run_daily.py        # Daily analysis CLI
    ├── run_intraday.py     # Intraday monitoring
    ├── backtest.py         # Historical backtest
    └── export_report.py    # Report generation
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Keys

Set environment variables for your data provider:
```bash
export POLYGON_API_KEY="your_key"
# or
export TRADIER_API_KEY="your_key"
```

### 3. Run Daily Analysis

```bash
python scripts/run_daily.py --config config/settings.yaml
```

### 4. Monitor Intraday

```bash
python scripts/run_intraday.py --interval 30
```

### 5. Run Backtest

```bash
python scripts/backtest.py --start 2024-01-01 --end 2024-12-01
```

## 📊 Market Regimes

The system classifies markets into 5 regimes:

| Regime | Description | Typical Strategy |
|--------|-------------|------------------|
| `LOW_VOL_GRIND` | Low volatility, steady trend | Iron condors, butterflies |
| `HIGH_VOL_PANIC` | Elevated VIX, sharp moves | Reduce exposure, wide spreads |
| `TREND_UP` | Strong bullish momentum | Bull put spreads |
| `TREND_DOWN` | Bearish with fear | Bear call spreads |
| `CHOP` | Sideways, indecisive | Neutral spreads |

## 🎯 Edge Detectors

### Volatility Risk Premium (VRP)
- Compares ATM IV to 20-day realized volatility
- Signal: IV/RV ratio > 1.3 (top percentile)
- Trade: Sell premium via credit spreads

### Term Structure
- Analyzes front vs back month IV
- Signals extreme contango or backwardation
- Trade: Calendar spreads

### Skew Extremes
- Measures 25-delta put vs call IV
- Signals when fear premium is extreme
- Trade: Vertical spreads on overpriced side

### Event Volatility
- Tracks IV premium before earnings/FOMC
- Signals when event premium > 20%
- Trade: Iron condors for vol crush

### Gamma Pressure
- Estimates dealer gamma from OI
- Identifies pin zones and gamma flip levels
- Trade: Butterflies around expected pin

## 🛡️ Risk Management

- **1% per trade** max risk
- **10% portfolio** total risk cap
- **3% daily / 5% weekly** loss limits
- **15% drawdown** kill switch
- Stress testing: ±5% gaps, ±5pt IV shocks

## 📋 Output: Daily Desk Report

```markdown
# Daily Desk Report - 2024-12-15

**Status: 🟢 TRADING ALLOWED**

## Market Regime
**LOW_VOL_GRIND** (confidence: 78%)

## Edges Detected
### 🔥 SPY - VOLATILITY_RISK_PREMIUM
- Strength: 85%
- IV 18.5% vs RV 12.3% = 1.50 ratio

## Trade Candidates
### ✅ Candidate 1: SPY
- Iron Condor 470/475P - 500/505C
- Credit: $1.45
- Max Loss: $3.55
- Recommended: 3 contracts
```

## 🔧 Configuration

### settings.yaml
```yaml
account:
  equity: 100000

risk:
  max_risk_per_trade_pct: 1.0
  max_total_risk_pct: 10.0
  daily_loss_limit_pct: 3.0

data:
  provider: polygon
```

### universe.yaml
```yaml
symbols:
  SPY:
    enabled: true
    min_dte: 7
    max_dte: 60
  QQQ:
    enabled: true
```

## 📝 Design Principles

1. **No Price Prediction** - Focus on volatility mispricing, not direction
2. **Defined Risk Only** - All structures have known max loss
3. **Auditable Decisions** - Every signal includes rationale and metrics
4. **Walk-Forward Validation** - No in-sample/out-of-sample mixing
5. **Kill Switch Protection** - Hard stops on drawdown
6. **Regime Awareness** - Adjust strategy to market conditions

## 📜 License

MIT

---

*Built for systematic options trading. No guaranteed profits.*
