"""
Base Data Provider Interface.

All data providers must implement this abstract interface.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Optional

from data.schemas import OHLCV, OptionChain


class DataProviderError(Exception):
    """Base exception for data provider errors."""
    pass


class DataProvider(ABC):
    """Abstract base class for market data providers."""
    
    def __init__(self, config: dict):
        """Initialize with provider-specific config."""
        self.config = config
        self._connected = False
    
    @property
    def name(self) -> str:
        """Provider name for logging."""
        return self.__class__.__name__
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data provider."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data provider."""
        pass
    
    @property
    def is_connected(self) -> bool:
        """Check if connected to provider."""
        return self._connected
    
    # =========================================================================
    # Underlying Price Data
    # =========================================================================
    
    @abstractmethod
    def get_current_price(self, symbol: str) -> float:
        """
        Get the current price of an underlying.
        
        Args:
            symbol: Ticker symbol (e.g., 'SPY')
            
        Returns:
            Current price as float
            
        Raises:
            DataProviderError: If price cannot be fetched
        """
        pass
    
    @abstractmethod
    def get_historical_ohlcv(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1d"
    ) -> list[OHLCV]:
        """
        Get historical OHLCV data.
        
        Args:
            symbol: Ticker symbol
            start_date: Start of date range
            end_date: End of date range
            timeframe: Bar size ('1d', '1h', '30m', '15m', '5m', '1m')
            
        Returns:
            List of OHLCV bars
        """
        pass
    
    # =========================================================================
    # Options Data
    # =========================================================================
    
    @abstractmethod
    def get_option_chain(
        self,
        symbol: str,
        expiration: Optional[date] = None
    ) -> OptionChain:
        """
        Get option chain for a symbol.
        
        Args:
            symbol: Underlying ticker
            expiration: Specific expiration date (None = all expirations)
            
        Returns:
            OptionChain with all contracts
        """
        pass
    
    @abstractmethod
    def get_option_expirations(self, symbol: str) -> list[date]:
        """
        Get available option expiration dates.
        
        Args:
            symbol: Underlying ticker
            
        Returns:
            List of available expiration dates
        """
        pass
    
    # =========================================================================
    # Volatility Indices
    # =========================================================================
    
    @abstractmethod
    def get_vix(self) -> float:
        """
        Get current VIX value.
        
        Returns:
            Current VIX level
        """
        pass
    
    def get_vvix(self) -> Optional[float]:
        """
        Get current VVIX value (VIX of VIX).
        
        Returns:
            Current VVIX or None if not available
        """
        return None  # Optional - override in provider if available
    
    def get_volatility_index(self, symbol: str) -> Optional[float]:
        """
        Get a specific volatility index value.
        
        Args:
            symbol: Volatility index symbol (e.g., 'VXN' for Nasdaq vol)
            
        Returns:
            Current value or None if not available
        """
        return None
    
    # =========================================================================
    # Market Hours
    # =========================================================================
    
    def is_market_open(self) -> bool:
        """Check if the market is currently open."""
        # Default implementation - override for provider-specific logic
        now = datetime.now()
        # Simple check: weekday and between 9:30 AM - 4:00 PM ET
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        # This is simplified - real implementation should use exchange calendars
        return True
    
    def get_market_hours(self, date: date) -> tuple[datetime, datetime]:
        """
        Get market open and close times for a date.
        
        Returns:
            Tuple of (open_time, close_time)
        """
        # Default implementation - override for accurate hours
        open_time = datetime.combine(date, datetime.min.time().replace(hour=9, minute=30))
        close_time = datetime.combine(date, datetime.min.time().replace(hour=16, minute=0))
        return open_time, close_time
