"""
Event Volatility Edge Detector.

Detects opportunities around known events (earnings, FOMC, CPI, etc.)
where IV is elevated due to event risk, often overpriced.

Key insight: IV typically collapses after events ("vol crush"),
and pre-event IV premiums are often excessive.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from pathlib import Path

import pandas as pd

from data.schemas import (
    EdgeSignal,
    EdgeType,
    TradeDirection,
    RegimeState,
    OptionChain,
    OptionType,
)


@dataclass
class EventVolConfig:
    """Configuration for event vol edge detection."""
    
    # Event detection
    days_before_event: int = 5  # Check for events within this window
    
    # IV premium threshold
    min_iv_premium_pct: float = 20  # Min 20% IV lift vs normal
    
    # Historical crush analysis
    crush_lookback: int = 8  # Number of historical events to analyze
    
    # Signal thresholds
    high_premium_threshold: float = 30  # 30%+ premium
    extreme_premium_threshold: float = 50  # 50%+ premium
    
    # Event types
    event_types: list[str] = None  # Types to track
    
    def __post_init__(self):
        if self.event_types is None:
            self.event_types = ["earnings", "FOMC", "CPI", "NFP", "GDP"]


@dataclass
class EventInfo:
    """Information about an upcoming event."""
    symbol: str
    event_type: str
    event_date: date
    days_until: int
    description: Optional[str] = None


@dataclass
class EventVolMetrics:
    """Computed event volatility metrics."""
    
    event: EventInfo
    
    # Current IV
    current_atm_iv: float
    
    # Baseline (non-event) IV estimate
    baseline_iv: float
    
    # Premium
    iv_premium: float  # current - baseline
    iv_premium_pct: float  # (current / baseline - 1) * 100
    
    # Historical crush data (if available)
    avg_historical_crush: Optional[float] = None
    median_historical_crush: Optional[float] = None


class EventCalendar:
    """
    Manages event calendar (earnings, economic events).
    
    Can load from CSV or API in future.
    """
    
    def __init__(self, events_file: Optional[str] = None):
        self.events: list[EventInfo] = []
        
        if events_file:
            self.load_from_csv(events_file)
    
    def load_from_csv(self, filepath: str) -> None:
        """
        Load events from CSV file.
        
        Expected columns:
        - symbol (str)
        - event_type (str)
        - event_date (YYYY-MM-DD)
        - description (optional)
        """
        path = Path(filepath)
        if not path.exists():
            return
        
        df = pd.read_csv(filepath)
        
        for _, row in df.iterrows():
            event_date = pd.to_datetime(row['event_date']).date()
            today = date.today()
            days_until = (event_date - today).days
            
            self.events.append(EventInfo(
                symbol=row.get('symbol', 'ALL'),
                event_type=row['event_type'],
                event_date=event_date,
                days_until=days_until,
                description=row.get('description'),
            ))
    
    def add_event(self, event: EventInfo) -> None:
        """Add a single event."""
        self.events.append(event)
    
    def get_upcoming_events(
        self,
        symbol: str,
        days_ahead: int = 10
    ) -> list[EventInfo]:
        """Get events within N days for a symbol."""
        today = date.today()
        
        result = []
        for event in self.events:
            # Check if event is for this symbol or ALL
            if event.symbol not in [symbol, 'ALL']:
                continue
            
            # Check if within window
            if 0 <= event.days_until <= days_ahead:
                result.append(event)
        
        return sorted(result, key=lambda e: e.event_date)
    
    def has_event_soon(
        self,
        symbol: str,
        days_ahead: int = 5
    ) -> bool:
        """Check if symbol has an event within N days."""
        return len(self.get_upcoming_events(symbol, days_ahead)) > 0


def estimate_baseline_iv(
    option_chain: OptionChain,
    event_dte: int,
) -> float:
    """
    Estimate baseline (non-event) IV.
    
    Uses the term structure to interpolate what IV "should" be
    without event premium.
    
    Args:
        option_chain: Current option chain
        event_dte: DTE of expiration with event
        
    Returns:
        Estimated baseline IV
    """
    today = date.today()
    
    # Find expirations before and after the event
    ivs_by_dte = []
    
    for exp in option_chain.expirations:
        dte = (exp - today).days
        if dte <= 0:
            continue
        
        # Get ATM IV for this expiration
        atm_strike = option_chain.get_atm_strike(exp)
        call = option_chain.get_contract(exp, atm_strike, OptionType.CALL)
        put = option_chain.get_contract(exp, atm_strike, OptionType.PUT)
        
        ivs = []
        if call and call.iv and call.iv > 0:
            ivs.append(call.iv)
        if put and put.iv and put.iv > 0:
            ivs.append(put.iv)
        
        if ivs:
            ivs_by_dte.append((dte, sum(ivs) / len(ivs)))
    
    if len(ivs_by_dte) < 2:
        # Not enough data - return None or a default
        return ivs_by_dte[0][1] if ivs_by_dte else 0.20
    
    # Sort by DTE
    ivs_by_dte.sort(key=lambda x: x[0])
    
    # Find expirations that bracket the event (but skip the event expiry)
    # Use weighted average of nearby expirations
    pre_event = [(d, iv) for d, iv in ivs_by_dte if d < event_dte - 3]
    post_event = [(d, iv) for d, iv in ivs_by_dte if d > event_dte + 7]
    
    if pre_event and post_event:
        # Interpolate
        pre_iv = pre_event[-1][1]
        post_iv = post_event[0][1]
        return (pre_iv + post_iv) / 2
    elif pre_event:
        return pre_event[-1][1]
    elif post_event:
        return post_event[0][1]
    else:
        # Fall back to median
        return sorted([iv for _, iv in ivs_by_dte])[len(ivs_by_dte) // 2]


def calculate_event_vol_metrics(
    option_chain: OptionChain,
    event: EventInfo,
) -> Optional[EventVolMetrics]:
    """
    Calculate event volatility metrics.
    
    Args:
        option_chain: Current option chain
        event: Upcoming event info
        
    Returns:
        EventVolMetrics or None
    """
    today = date.today()
    
    # Find the expiration that contains the event
    event_exp = None
    event_dte = None
    
    for exp in option_chain.expirations:
        dte = (exp - today).days
        # Event expiration is the first one after the event date
        if exp > event.event_date and (event_exp is None or exp < event_exp):
            event_exp = exp
            event_dte = dte
    
    if event_exp is None:
        return None
    
    # Get ATM IV for event expiration
    atm_strike = option_chain.get_atm_strike(event_exp)
    call = option_chain.get_contract(event_exp, atm_strike, OptionType.CALL)
    put = option_chain.get_contract(event_exp, atm_strike, OptionType.PUT)
    
    ivs = []
    if call and call.iv and call.iv > 0:
        ivs.append(call.iv)
    if put and put.iv and put.iv > 0:
        ivs.append(put.iv)
    
    if not ivs:
        return None
    
    current_atm_iv = sum(ivs) / len(ivs)
    
    # Estimate baseline IV
    baseline_iv = estimate_baseline_iv(option_chain, event_dte)
    
    # Calculate premium
    iv_premium = current_atm_iv - baseline_iv
    iv_premium_pct = (current_atm_iv / baseline_iv - 1) * 100 if baseline_iv > 0 else 0
    
    return EventVolMetrics(
        event=event,
        current_atm_iv=current_atm_iv,
        baseline_iv=baseline_iv,
        iv_premium=iv_premium,
        iv_premium_pct=iv_premium_pct,
    )


def detect_event_vol_edge(
    metrics: EventVolMetrics,
    regime: RegimeState,
    config: Optional[EventVolConfig] = None,
) -> Optional[EdgeSignal]:
    """
    Detect event volatility edge.
    
    Signal when IV premium is elevated above threshold.
    
    Args:
        metrics: Event vol metrics
        regime: Current market regime
        config: Configuration
        
    Returns:
        EdgeSignal if edge detected
    """
    if config is None:
        config = EventVolConfig()
    
    # Check if premium is above threshold
    if metrics.iv_premium_pct < config.min_iv_premium_pct:
        return None
    
    # Calculate signal strength
    if metrics.iv_premium_pct >= config.extreme_premium_threshold:
        strength = 1.0
    elif metrics.iv_premium_pct >= config.high_premium_threshold:
        strength = 0.7
    else:
        strength = 0.4 + (metrics.iv_premium_pct - config.min_iv_premium_pct) / 30 * 0.3
    
    # Adjust for regime
    if regime == RegimeState.HIGH_VOL_PANIC:
        # Event premium in panic might be justified
        strength *= 0.7
    
    rationale = (
        f"Event vol edge: {metrics.event.event_type} on {metrics.event.event_date} "
        f"({metrics.event.days_until}d away). "
        f"Current IV {metrics.current_atm_iv:.1%} vs baseline {metrics.baseline_iv:.1%} = "
        f"{metrics.iv_premium_pct:.0f}% premium. "
        f"Consider defined-risk short premium structures."
    )
    
    return EdgeSignal(
        timestamp=datetime.now(),
        symbol="",
        edge_type=EdgeType.EVENT_VOL,
        strength=min(strength, 1.0),
        direction=TradeDirection.SHORT,  # Sell the elevated premium
        metrics={
            'event_type': metrics.event.event_type,
            'event_date': metrics.event.event_date.isoformat(),
            'days_until': metrics.event.days_until,
            'current_iv': round(metrics.current_atm_iv, 4),
            'baseline_iv': round(metrics.baseline_iv, 4),
            'iv_premium_pct': round(metrics.iv_premium_pct, 1),
        },
        rationale=rationale,
        regime_at_signal=regime,
    )


class EventVolDetector:
    """Event volatility edge detector."""
    
    def __init__(
        self,
        config: Optional[EventVolConfig] = None,
        events_file: Optional[str] = None,
    ):
        self.config = config or EventVolConfig()
        self.calendar = EventCalendar(events_file)
    
    def add_event(self, event: EventInfo):
        """Add an event to the calendar."""
        self.calendar.add_event(event)
    
    def detect(
        self,
        symbol: str,
        option_chain: OptionChain,
        regime: RegimeState,
    ) -> Optional[EdgeSignal]:
        """
        Detect event vol edge for a symbol.
        
        Args:
            symbol: Underlying symbol
            option_chain: Current option chain
            regime: Current market regime
            
        Returns:
            EdgeSignal if edge detected
        """
        try:
            # Get upcoming events
            events = self.calendar.get_upcoming_events(
                symbol, self.config.days_before_event
            )
            
            if not events:
                return None
            
            # Check the nearest event
            event = events[0]
            
            metrics = calculate_event_vol_metrics(option_chain, event)
            
            if metrics is None:
                return None
            
            signal = detect_event_vol_edge(metrics, regime, self.config)
            
            if signal:
                signal.symbol = symbol
            
            return signal
            
        except Exception as e:
            print(f"Event vol detection error for {symbol}: {e}")
            return None
    
    def has_event_soon(self, symbol: str) -> bool:
        """Check if symbol has an event soon."""
        return self.calendar.has_event_soon(symbol, self.config.days_before_event)
