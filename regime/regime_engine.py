"""
Regime Engine.

Main interface for regime classification.
Combines feature extraction and state machine for daily/intraday updates.
"""

from datetime import date, datetime
from typing import Optional

import pandas as pd

from data.schemas import OHLCV, RegimeClassification, RegimeState
from data.providers.base import DataProvider
from data.cache import DataCache
from regime.features import RegimeFeatures, extract_features, features_to_dict
from regime.state_machine import (
    classify_regime,
    should_trade_in_regime,
    get_regime_bias,
    RegimeThresholds,
)


class RegimeEngine:
    """
    Market Regime Classification Engine.
    
    Provides:
    - Daily regime classification
    - Intraday regime updates
    - Regime history tracking
    - Trading guidance based on regime
    """
    
    def __init__(
        self,
        provider: DataProvider,
        cache: Optional[DataCache] = None,
        thresholds: Optional[RegimeThresholds] = None,
    ):
        """
        Initialize regime engine.
        
        Args:
            provider: Data provider for market data
            cache: Optional cache for historical data
            thresholds: Custom regime thresholds
        """
        self.provider = provider
        self.cache = cache
        self.thresholds = thresholds or RegimeThresholds()
        
        # State
        self._current_regime: Optional[RegimeClassification] = None
        self._last_features: Optional[RegimeFeatures] = None
        self._regime_history: list[RegimeClassification] = []
    
    @property
    def current_regime(self) -> Optional[RegimeClassification]:
        """Get current regime classification."""
        return self._current_regime
    
    @property
    def last_features(self) -> Optional[RegimeFeatures]:
        """Get last computed features."""
        return self._last_features
    
    def classify(
        self,
        symbol: str = "SPY",
        as_of_date: Optional[date] = None,
        lookback_days: int = 252,
    ) -> RegimeClassification:
        """
        Classify current market regime.
        
        Args:
            symbol: Symbol to use for classification (default SPY)
            as_of_date: Date for classification (default today)
            lookback_days: Days of history to fetch
            
        Returns:
            RegimeClassification
        """
        if as_of_date is None:
            as_of_date = date.today()
        
        # Calculate date range
        from datetime import timedelta
        start_date = as_of_date - timedelta(days=lookback_days + 30)  # Buffer
        
        # Fetch OHLCV data
        ohlcv_data = self.provider.get_historical_ohlcv(
            symbol=symbol,
            start_date=start_date,
            end_date=as_of_date,
            timeframe="1d"
        )
        
        if len(ohlcv_data) < 60:
            raise ValueError(f"Insufficient data for regime classification: {len(ohlcv_data)} bars")
        
        # Get VIX if available
        vix_level = None
        vix_history = None
        try:
            vix_level = self.provider.get_vix()
            # TODO: Get VIX history for percentile calculation
        except Exception:
            pass
        
        # Extract features
        features = extract_features(
            ohlcv_data=ohlcv_data,
            vix_level=vix_level,
            vix_history=vix_history,
        )
        self._last_features = features
        
        # Classify regime
        regime = classify_regime(features, self.thresholds)
        
        # Store result
        self._current_regime = regime
        self._regime_history.append(regime)
        
        # Keep history bounded
        if len(self._regime_history) > 252:
            self._regime_history = self._regime_history[-252:]
        
        return regime
    
    def classify_intraday(
        self,
        symbol: str = "SPY",
        timeframe: str = "30m",
    ) -> RegimeClassification:
        """
        Update regime classification intraday.
        
        Uses shorter timeframe data for more responsive updates.
        Blends with daily regime to avoid whipsawing.
        
        Args:
            symbol: Symbol for classification
            timeframe: Intraday timeframe
            
        Returns:
            Updated RegimeClassification
        """
        # For intraday, we use a blend of daily and intraday signals
        # Get last 5 days of intraday data
        from datetime import timedelta
        end_date = date.today()
        start_date = end_date - timedelta(days=5)
        
        try:
            intraday_data = self.provider.get_historical_ohlcv(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe
            )
            
            if len(intraday_data) < 20:
                # Fall back to daily classification
                return self.classify(symbol)
            
            # Use last 100 bars for feature extraction
            # This gives us roughly 2-3 days of 30-min bars
            recent_bars = intraday_data[-100:]
            
            vix_level = None
            try:
                vix_level = self.provider.get_vix()
            except Exception:
                pass
            
            features = extract_features(
                ohlcv_data=recent_bars,
                vix_level=vix_level,
            )
            
            intraday_regime = classify_regime(features, self.thresholds)
            
            # Blend with daily if available
            if self._current_regime is not None:
                # If intraday disagrees with daily, reduce confidence
                if intraday_regime.regime != self._current_regime.regime:
                    intraday_regime.confidence *= 0.7
                    intraday_regime.rationale = (
                        f"[INTRADAY] {intraday_regime.rationale} "
                        f"(Daily: {self._current_regime.regime.value})"
                    )
            
            return intraday_regime
            
        except Exception as e:
            # Fall back to daily
            if self._current_regime is not None:
                return self._current_regime
            return self.classify(symbol)
    
    def get_trading_guidance(self) -> dict:
        """
        Get trading guidance based on current regime.
        
        Returns:
            Dictionary with:
            - should_trade: bool
            - reason: str
            - bias: dict with direction, premium, size_multiplier
            - regime: current regime state
            - confidence: regime confidence
        """
        if self._current_regime is None:
            return {
                'should_trade': False,
                'reason': 'No regime classification available',
                'bias': {},
                'regime': None,
                'confidence': 0,
            }
        
        should_trade, reason = should_trade_in_regime(
            self._current_regime.regime,
            self._current_regime.confidence
        )
        
        bias = get_regime_bias(self._current_regime.regime)
        
        return {
            'should_trade': should_trade,
            'reason': reason,
            'bias': bias,
            'regime': self._current_regime.regime,
            'confidence': self._current_regime.confidence,
            'rationale': self._current_regime.rationale,
        }
    
    def get_regime_history(
        self,
        n: int = 30
    ) -> list[RegimeClassification]:
        """Get last n regime classifications."""
        return self._regime_history[-n:]
    
    def get_regime_stats(self) -> dict:
        """
        Get statistics about regime distribution.
        
        Returns:
            Dictionary with regime counts and percentages
        """
        if not self._regime_history:
            return {}
        
        counts = {}
        for r in RegimeState:
            count = sum(1 for h in self._regime_history if h.regime == r)
            counts[r.value] = {
                'count': count,
                'pct': count / len(self._regime_history) * 100
            }
        
        return counts
    
    def to_dict(self) -> dict:
        """Export current state as dictionary."""
        return {
            'current_regime': self._current_regime.model_dump() if self._current_regime else None,
            'last_features': features_to_dict(self._last_features) if self._last_features else None,
            'guidance': self.get_trading_guidance(),
            'regime_stats': self.get_regime_stats(),
        }
