"""
Data Schemas - Pydantic models for all data structures.

These provide type safety, validation, and serialization for the entire system.
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ============================================================================
# Enums
# ============================================================================

class OptionType(str, Enum):
    """Option type: call or put."""
    CALL = "call"
    PUT = "put"


class RegimeState(str, Enum):
    """Market regime classification."""
    LOW_VOL_GRIND = "low_vol_grind"
    HIGH_VOL_PANIC = "high_vol_panic"
    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    CHOP = "chop"


class EdgeType(str, Enum):
    """Types of edge signals."""
    VOLATILITY_RISK_PREMIUM = "vrp"
    TERM_STRUCTURE = "term_structure"
    SKEW_EXTREME = "skew_extreme"
    EVENT_VOL = "event_vol"
    GAMMA_PRESSURE = "gamma_pressure"


class StructureType(str, Enum):
    """Option structure types."""
    CREDIT_SPREAD = "credit_spread"
    DEBIT_SPREAD = "debit_spread"
    IRON_CONDOR = "iron_condor"
    IRON_BUTTERFLY = "iron_butterfly"
    BUTTERFLY = "butterfly"
    CALENDAR = "calendar"
    DIAGONAL = "diagonal"


class TradeDirection(str, Enum):
    """Trade direction."""
    LONG = "long"
    SHORT = "short"


# ============================================================================
# Market Data Models
# ============================================================================

class OHLCV(BaseModel):
    """OHLCV bar for underlying price data."""
    symbol: str
    timestamp: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: int = Field(ge=0)
    
    @field_validator('high')
    @classmethod
    def high_gte_low(cls, v, info):
        if 'low' in info.data and v < info.data['low']:
            raise ValueError('high must be >= low')
        return v


class Greeks(BaseModel):
    """Option Greeks."""
    delta: float = Field(ge=-1, le=1)
    gamma: float = Field(ge=0)
    theta: float  # Usually negative for long options
    vega: float = Field(ge=0)
    rho: Optional[float] = None
    
    # Extended Greeks (optional)
    vanna: Optional[float] = None
    charm: Optional[float] = None


class OptionContract(BaseModel):
    """Single option contract with full details."""
    symbol: str  # Underlying symbol
    contract_symbol: str  # Full OCC symbol
    option_type: OptionType
    strike: float = Field(gt=0)
    expiration: date
    
    # Pricing
    bid: float = Field(ge=0)
    ask: float = Field(ge=0)
    last: Optional[float] = Field(ge=0, default=None)
    mid: Optional[float] = None
    
    # Implied volatility
    iv: Optional[float] = Field(ge=0, default=None)
    
    # Greeks
    greeks: Optional[Greeks] = None
    
    # Liquidity metrics
    volume: int = Field(ge=0, default=0)
    open_interest: int = Field(ge=0, default=0)
    
    # Timestamps
    quote_time: Optional[datetime] = None
    
    @property
    def bid_ask_spread(self) -> float:
        """Bid-ask spread in absolute terms."""
        return self.ask - self.bid
    
    @property
    def bid_ask_pct(self) -> float:
        """Bid-ask spread as percentage of mid."""
        if self.mid is None or self.mid == 0:
            return float('inf')
        return (self.ask - self.bid) / self.mid * 100
    
    @property
    def quote_is_valid(self) -> bool:
        """
        Check if quoted bid/ask are valid.
        
        Invalid cases:
        - bid or ask is 0 or negative
        - bid > ask (crossed market)
        - ask is 0 (no offer)
        """
        if self.bid <= 0 or self.ask <= 0:
            return False
        if self.bid > self.ask:
            return False
        return True
    
    def model_post_init(self, __context):
        """
        Calculate mid price if not provided AND quote is valid.
        
        If bid/ask invalid (0, bid>ask), set mid=None.
        """
        if self.mid is None:
            if self.quote_is_valid:
                self.mid = (self.bid + self.ask) / 2
            else:
                self.mid = None


class OptionChain(BaseModel):
    """Complete option chain for a symbol."""
    symbol: str
    underlying_price: float = Field(gt=0)
    timestamp: datetime
    
    # Contracts by expiration
    expirations: list[date]
    contracts: list[OptionContract]
    
    def get_expiration(self, exp_date: date) -> list[OptionContract]:
        """Get all contracts for a specific expiration."""
        return [c for c in self.contracts if c.expiration == exp_date]
    
    def get_atm_strike(self, exp_date: date) -> float:
        """Get the at-the-money strike for an expiration."""
        exp_contracts = self.get_expiration(exp_date)
        if not exp_contracts:
            return self.underlying_price
        strikes = sorted(set(c.strike for c in exp_contracts))
        return min(strikes, key=lambda s: abs(s - self.underlying_price))
    
    def get_contract(
        self,
        exp_date: date,
        strike: float,
        option_type: OptionType,
        tolerance: float = 0.01  # Default 1 cent tolerance
    ) -> Optional[OptionContract]:
        """
        Get a specific contract with strike tolerance.
        
        Avoids float-equality issues by using tolerance-based matching.
        Selects from actual available chain strikes.
        
        Args:
            exp_date: Expiration date
            strike: Target strike price
            option_type: CALL or PUT
            tolerance: Strike matching tolerance (default: 0.01)
            
        Returns:
            OptionContract or None
        """
        # Filter to matching expiration and type
        candidates = [
            c for c in self.contracts
            if c.expiration == exp_date and c.option_type == option_type
        ]
        
        if not candidates:
            return None
        
        # Find exact match within tolerance
        for c in candidates:
            if abs(c.strike - strike) <= tolerance:
                return c
        
        # If no exact match, return closest strike
        return min(candidates, key=lambda c: abs(c.strike - strike))


class VolSurface(BaseModel):
    """Implied volatility surface."""
    symbol: str
    timestamp: datetime
    underlying_price: float
    
    # IV by (expiration, strike, type)
    surface: dict[str, float]  # Key format: "YYYY-MM-DD|strike|call/put"
    
    # Summary metrics
    atm_iv_front: Optional[float] = None  # Front month ATM IV
    atm_iv_back: Optional[float] = None   # Second month ATM IV
    iv_skew_25d: Optional[float] = None   # 25-delta put IV - call IV
    
    def get_iv(
        self,
        expiration: date,
        strike: float,
        option_type: OptionType
    ) -> Optional[float]:
        """Get IV for a specific point on the surface."""
        key = f"{expiration.isoformat()}|{strike}|{option_type.value}"
        return self.surface.get(key)


# ============================================================================
# Regime & Edge Models
# ============================================================================

class RegimeClassification(BaseModel):
    """Market regime classification result."""
    timestamp: datetime
    regime: RegimeState
    confidence: float = Field(ge=0, le=1)
    
    # Feature values that led to this classification
    features: dict[str, float]
    
    # Reasoning
    rationale: str


class EdgeSignal(BaseModel):
    """Detected edge signal."""
    timestamp: datetime
    symbol: str
    edge_type: EdgeType
    
    # Signal strength
    strength: float = Field(ge=0, le=1)  # 0 = no signal, 1 = maximum
    direction: TradeDirection  # Preferred trade direction
    
    # Supporting data
    metrics: dict[str, float]  # e.g., {"iv_rv_ratio": 1.45, "percentile": 85}
    
    # Rationale (for audit)
    rationale: str
    
    # Regime context
    regime_at_signal: RegimeState


# ============================================================================
# Trade Structure Models
# ============================================================================

class OptionLeg(BaseModel):
    """Single leg of an option structure."""
    contract: OptionContract
    quantity: int  # Positive = long, negative = short
    
    @property
    def is_long(self) -> bool:
        return self.quantity > 0
    
    @property
    def is_short(self) -> bool:
        return self.quantity < 0


class OptionStructure(BaseModel):
    """Complete option structure with all legs.
    
    UNITS:
    - entry_debit, entry_credit, max_loss, max_profit: all in POINTS (per share)
    - Use *_dollars properties for dollar values (points * 100)
    """
    structure_type: StructureType
    symbol: str
    legs: list[OptionLeg] = Field(default_factory=list)
    
    # Entry pricing (in POINTS)
    entry_debit: Optional[float] = None  # Positive = pay
    entry_credit: Optional[float] = None  # Positive = receive
    
    # Risk/reward (in POINTS)
    max_loss: Optional[float] = Field(default=None, ge=0)  # Always positive or None
    max_profit: Optional[float] = None  # None for undefined
    breakevens: list[float] = Field(default_factory=list)
    
    # Aggregate Greeks
    net_delta: float = 0
    net_gamma: float = 0
    net_theta: float = 0
    net_vega: float = 0
    
    # Margin estimate
    margin_requirement: Optional[float] = None
    
    # Contract multiplier
    _multiplier: int = 100
    
    @property
    def entry_debit_dollars(self) -> Optional[float]:
        """Entry debit in dollars per contract."""
        return self.entry_debit * self._multiplier if self.entry_debit else None
    
    @property
    def entry_credit_dollars(self) -> Optional[float]:
        """Entry credit in dollars per contract."""
        return self.entry_credit * self._multiplier if self.entry_credit else None
    
    @property
    def max_loss_dollars(self) -> Optional[float]:
        """Max loss in dollars per contract."""
        return self.max_loss * self._multiplier if self.max_loss else None
    
    @property
    def max_profit_dollars(self) -> Optional[float]:
        """Max profit in dollars per contract."""
        return self.max_profit * self._multiplier if self.max_profit else None
    
    @property
    def is_defined_risk(self) -> bool:
        """Check if the structure has defined max loss."""
        return self.max_loss is not None and self.max_loss > 0
    
    @property
    def risk_reward_ratio(self) -> Optional[float]:
        """Return max_profit / max_loss if defined."""
        if self.max_profit is not None and self.max_loss and self.max_loss > 0:
            return self.max_profit / self.max_loss
        return None


class TradeCandidate(BaseModel):
    """Trade candidate with full audit trail.
    
    UNITS:
    - risk_per_contract: dollars per contract
    - total_risk: total dollars at risk
    
    NOTE: For PASS/REVIEW recommendations, risk values may be 0.
    """
    id: str  # Unique identifier
    timestamp: datetime
    symbol: str
    
    # The structure
    structure: OptionStructure
    
    # Edge that triggered this
    edge: EdgeSignal
    regime: RegimeClassification
    
    # Sizing recommendation (in DOLLARS)
    recommended_contracts: int = Field(ge=0)
    risk_per_contract: float = Field(ge=0)  # Allow 0 for PASS
    total_risk: float = Field(ge=0)  # Allow 0 for PASS
    
    # Validation results
    is_valid: bool = True
    validation_messages: list[str] = Field(default_factory=list)
    
    # Trade or don't trade
    recommendation: str  # "TRADE", "PASS", "REVIEW"
    rationale: str
    
    # Explainability blocks (desk memo style)
    edge_explanation: Optional[str] = None
    candidate_explanation: Optional[str] = None
    
    # Quality score (0-100)
    quality_score: Optional[dict] = None  # {total, edge_strength, regime_fit, liquidity, pricing_quality, grade}
    
    # Probability metrics (model-based, NOT predictive)
    probability_metrics: Optional[dict] = None  # ProbabilityMetrics as dict


# ============================================================================
# Portfolio & Risk Models
# ============================================================================

class Position(BaseModel):
    """Open position in the portfolio."""
    id: str
    entry_timestamp: datetime
    symbol: str
    structure: OptionStructure
    contracts: int = Field(gt=0)
    
    # Entry details
    entry_price: float  # Net debit/credit per contract
    entry_max_loss: float
    
    # Current state
    current_value: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    days_held: int = 0
    
    # Linked to original trade candidate
    trade_candidate_id: str


class PortfolioState(BaseModel):
    """Current portfolio state snapshot.
    
    UNITS:
    - total_max_loss_dollars: aggregate max loss in dollars
    - total_delta_exposure: aggregate portfolio delta (not dollars)
    - realized_pnl_today_dollars: today's realized PnL in dollars
    - unrealized_pnl_dollars: current unrealized PnL in dollars
    """
    timestamp: datetime
    
    # Positions
    open_positions: list[Position]
    
    # Aggregate risk (DOLLARS)
    total_max_loss: float  # Sum of all position max losses in dollars
    total_current_risk: float  # Same as total_max_loss for now
    
    # Greeks (not dollars - these are units of underlying)
    portfolio_delta: float = 0  # Equivalent shares exposure
    portfolio_gamma: float = 0
    portfolio_theta: float = 0  # Daily theta in dollars
    portfolio_vega: float = 0   # Vega exposure in dollars
    
    # P&L (DOLLARS)
    realized_pnl_today: float = 0
    unrealized_pnl: float = 0
    
    # Limits status
    trades_open: int = 0
    daily_loss_pct: float = 0
    weekly_loss_pct: float = 0
    max_drawdown_pct: float = 0
    
    # Kill switch
    kill_switch_active: bool = False
    kill_switch_reason: Optional[str] = None


# ============================================================================
# Report Models
# ============================================================================

class DailyReport(BaseModel):
    """Daily desk report structure."""
    report_date: date
    generated_at: datetime
    
    # Market state
    regime: RegimeClassification
    
    # Volatility overview
    vol_state: dict[str, float]  # Symbol -> ATM IV
    term_structure: dict[str, str]  # Symbol -> "contango"/"backwardation"
    
    # Edges detected
    edges: list[EdgeSignal]
    
    # Trade candidates
    candidates: list[TradeCandidate]
    
    # Risk status
    portfolio: PortfolioState
    
    # Kill switch / do-not-trade reasons
    trading_allowed: bool = True
    do_not_trade_reasons: list[str] = Field(default_factory=list)
