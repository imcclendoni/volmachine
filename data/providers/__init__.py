"""
Data provider package.
"""

from data.providers.base import DataProvider, DataProviderError
from data.providers.polygon import PolygonProvider
from data.providers.tradier import TradierProvider
from data.providers.ibkr import IBKRProvider


def get_provider(provider_name: str, config: dict) -> DataProvider:
    """
    Factory function to get a data provider instance.
    
    Args:
        provider_name: Name of provider ('polygon', 'tradier', 'ibkr')
        config: Provider-specific configuration
        
    Returns:
        DataProvider instance
        
    Raises:
        ValueError: If provider_name is not recognized
    """
    providers = {
        'polygon': PolygonProvider,
        'tradier': TradierProvider,
        'ibkr': IBKRProvider,
    }
    
    if provider_name.lower() not in providers:
        raise ValueError(
            f"Unknown provider: {provider_name}. "
            f"Available: {list(providers.keys())}"
        )
    
    return providers[provider_name.lower()](config)


__all__ = [
    'DataProvider',
    'DataProviderError',
    'PolygonProvider',
    'TradierProvider',
    'IBKRProvider',
    'get_provider',
]
