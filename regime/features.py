"""
Regime Feature Extraction.

Computes features used to classify market regime:
- Trend indicators (MA crossovers, price vs MAs)
- Volatility metrics (realized vol at multiple windows)
- VIX and VIX term structure
- Drawdown from highs
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from data.schemas import OHLCV


@dataclass
class RegimeFeatures:
    """Features used for regime classification."""
    
    # Timestamp
    as_of: datetime
    symbol: str
    
    # Current price
    current_price: float
    
    # Trend features
    price_vs_ma20: float   # (price - MA20) / MA20
    price_vs_ma50: float   # (price - MA50) / MA50
    price_vs_ma200: float  # (price - MA200) / MA200
    ma20_vs_ma50: float    # MA20 above/below MA50 (ratio)
    ma50_vs_ma200: float   # MA50 above/below MA200 (ratio)
    
    # Volatility features
    rv_5d: float   # 5-day realized vol (annualized)
    rv_20d: float  # 20-day realized vol (annualized)
    rv_60d: float  # 60-day realized vol (annualized)
    rv_ratio_5_20: float  # Short-term vs medium-term vol
    rv_ratio_20_60: float  # Medium-term vs long-term vol
    
    # VIX features (optional - may be None if not available)
    vix_level: Optional[float] = None
    vix_percentile: Optional[float] = None  # Percentile vs 1-year history
    vix_change_5d: Optional[float] = None   # 5-day change
    
    # Drawdown
    drawdown_from_high: float = 0.0  # Current drawdown from 52-week high
    days_since_high: int = 0
    
    # Momentum
    return_5d: float = 0.0
    return_20d: float = 0.0
    return_60d: float = 0.0


def calculate_moving_average(prices: pd.Series, window: int) -> pd.Series:
    """Calculate simple moving average."""
    return prices.rolling(window=window, min_periods=window).mean()


def calculate_realized_volatility(
    prices: pd.Series, 
    window: int,
    annualize: bool = True
) -> pd.Series:
    """
    Calculate realized volatility from log returns.
    
    Args:
        prices: Price series
        window: Lookback window in days
        annualize: If True, annualize the volatility
        
    Returns:
        Rolling realized volatility
    """
    log_returns = np.log(prices / prices.shift(1))
    rv = log_returns.rolling(window=window, min_periods=window).std()
    
    if annualize:
        rv = rv * np.sqrt(252)
    
    return rv


def calculate_drawdown(prices: pd.Series, lookback: int = 252) -> pd.Series:
    """
    Calculate drawdown from rolling high.
    
    Args:
        prices: Price series
        lookback: Lookback for high (default 1 year)
        
    Returns:
        Drawdown as negative percentage (0 = at high)
    """
    rolling_high = prices.rolling(window=lookback, min_periods=1).max()
    drawdown = (prices - rolling_high) / rolling_high
    return drawdown


def calculate_returns(prices: pd.Series, periods: list[int]) -> dict[int, float]:
    """Calculate returns over multiple periods."""
    returns = {}
    for period in periods:
        if len(prices) > period:
            returns[period] = (prices.iloc[-1] / prices.iloc[-period - 1]) - 1
        else:
            returns[period] = 0.0
    return returns


def extract_features(
    ohlcv_data: list[OHLCV],
    vix_level: Optional[float] = None,
    vix_history: Optional[pd.Series] = None,
) -> RegimeFeatures:
    """
    Extract regime features from OHLCV data.
    
    Args:
        ohlcv_data: List of OHLCV bars (should have at least 200 days)
        vix_level: Current VIX level (optional)
        vix_history: Historical VIX series for percentile (optional)
        
    Returns:
        RegimeFeatures dataclass
    """
    if len(ohlcv_data) < 60:
        raise ValueError("Need at least 60 days of data for regime features")
    
    # Convert to DataFrame
    df = pd.DataFrame([bar.model_dump() for bar in ohlcv_data])
    df = df.sort_values('timestamp')
    
    symbol = ohlcv_data[0].symbol
    prices = df['close']
    current_price = prices.iloc[-1]
    
    # Moving averages
    ma20 = calculate_moving_average(prices, 20)
    ma50 = calculate_moving_average(prices, 50)
    ma200 = calculate_moving_average(prices, 200) if len(prices) >= 200 else pd.Series([current_price])
    
    # Get latest MA values, handling NaN
    latest_ma20 = ma20.iloc[-1] if pd.notna(ma20.iloc[-1]) else current_price
    latest_ma50 = ma50.iloc[-1] if pd.notna(ma50.iloc[-1]) else current_price
    latest_ma200 = ma200.iloc[-1] if len(ma200) > 0 and pd.notna(ma200.iloc[-1]) else current_price
    
    # Trend features
    price_vs_ma20 = (current_price - latest_ma20) / latest_ma20 if latest_ma20 > 0 else 0
    price_vs_ma50 = (current_price - latest_ma50) / latest_ma50 if latest_ma50 > 0 else 0
    price_vs_ma200 = (current_price - latest_ma200) / latest_ma200 if latest_ma200 > 0 else 0
    ma20_vs_ma50 = latest_ma20 / latest_ma50 if latest_ma50 > 0 else 1
    ma50_vs_ma200 = latest_ma50 / latest_ma200 if latest_ma200 > 0 else 1
    
    # Realized volatility
    rv_5d = calculate_realized_volatility(prices, 5).iloc[-1]
    rv_20d = calculate_realized_volatility(prices, 20).iloc[-1]
    rv_60d = calculate_realized_volatility(prices, 60).iloc[-1] if len(prices) >= 60 else rv_20d
    
    rv_5d = rv_5d if pd.notna(rv_5d) else 0.15
    rv_20d = rv_20d if pd.notna(rv_20d) else 0.15
    rv_60d = rv_60d if pd.notna(rv_60d) else 0.15
    
    rv_ratio_5_20 = rv_5d / rv_20d if rv_20d > 0 else 1
    rv_ratio_20_60 = rv_20d / rv_60d if rv_60d > 0 else 1
    
    # Drawdown
    dd = calculate_drawdown(prices, min(252, len(prices)))
    drawdown_from_high = dd.iloc[-1] if pd.notna(dd.iloc[-1]) else 0
    
    # Days since high
    rolling_high = prices.rolling(window=min(252, len(prices)), min_periods=1).max()
    at_high = prices == rolling_high
    if at_high.iloc[-1]:
        days_since_high = 0
    else:
        last_high_idx = at_high[::-1].idxmax()
        days_since_high = len(prices) - 1 - df.index.get_loc(last_high_idx)
    
    # Returns
    returns = calculate_returns(prices, [5, 20, 60])
    
    # VIX features
    vix_percentile = None
    vix_change_5d = None
    if vix_level is not None and vix_history is not None and len(vix_history) > 0:
        vix_percentile = (vix_history < vix_level).sum() / len(vix_history) * 100
        if len(vix_history) >= 5:
            vix_change_5d = vix_level - vix_history.iloc[-5]
    
    return RegimeFeatures(
        as_of=datetime.now(),
        symbol=symbol,
        current_price=current_price,
        price_vs_ma20=price_vs_ma20,
        price_vs_ma50=price_vs_ma50,
        price_vs_ma200=price_vs_ma200,
        ma20_vs_ma50=ma20_vs_ma50,
        ma50_vs_ma200=ma50_vs_ma200,
        rv_5d=rv_5d,
        rv_20d=rv_20d,
        rv_60d=rv_60d,
        rv_ratio_5_20=rv_ratio_5_20,
        rv_ratio_20_60=rv_ratio_20_60,
        vix_level=vix_level,
        vix_percentile=vix_percentile,
        vix_change_5d=vix_change_5d,
        drawdown_from_high=drawdown_from_high,
        days_since_high=days_since_high,
        return_5d=returns.get(5, 0),
        return_20d=returns.get(20, 0),
        return_60d=returns.get(60, 0),
    )


def features_to_dict(features: RegimeFeatures) -> dict:
    """Convert RegimeFeatures to dictionary for logging/storage."""
    return {
        'as_of': features.as_of.isoformat(),
        'symbol': features.symbol,
        'current_price': features.current_price,
        'price_vs_ma20': round(features.price_vs_ma20, 4),
        'price_vs_ma50': round(features.price_vs_ma50, 4),
        'price_vs_ma200': round(features.price_vs_ma200, 4),
        'ma20_vs_ma50': round(features.ma20_vs_ma50, 4),
        'ma50_vs_ma200': round(features.ma50_vs_ma200, 4),
        'rv_5d': round(features.rv_5d, 4),
        'rv_20d': round(features.rv_20d, 4),
        'rv_60d': round(features.rv_60d, 4),
        'rv_ratio_5_20': round(features.rv_ratio_5_20, 4),
        'rv_ratio_20_60': round(features.rv_ratio_20_60, 4),
        'vix_level': features.vix_level,
        'vix_percentile': features.vix_percentile,
        'vix_change_5d': features.vix_change_5d,
        'drawdown_from_high': round(features.drawdown_from_high, 4),
        'days_since_high': features.days_since_high,
        'return_5d': round(features.return_5d, 4),
        'return_20d': round(features.return_20d, 4),
        'return_60d': round(features.return_60d, 4),
    }
