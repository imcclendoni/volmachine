"""
Data Cache Layer.

Provides caching for market data using Parquet files or DuckDB.
Reduces API calls and enables backtesting with historical snapshots.
"""

import hashlib
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, TypeVar, Generic

import pandas as pd

from data.schemas import OHLCV, OptionChain


T = TypeVar('T')


class CacheError(Exception):
    """Cache operation error."""
    pass


class DataCache:
    """
    Cache layer for market data.
    
    Stores data as Parquet files organized by:
    - cache_dir/
      - ohlcv/{symbol}/{timeframe}/
      - options/{symbol}/{date}/
      - vix/{date}.parquet
    """
    
    def __init__(self, config: dict):
        """
        Initialize cache.
        
        Args:
            config: Cache configuration from settings.yaml
        """
        self.enabled = config.get('enabled', True)
        self.cache_dir = Path(config.get('directory', './cache'))
        self.ttl_hours = config.get('ttl_hours', 24)
        self.backend = config.get('backend', 'parquet')
        
        # Create cache directories
        if self.enabled:
            self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Create cache directory structure."""
        dirs = ['ohlcv', 'options', 'vix', 'regime', 'edges']
        for d in dirs:
            (self.cache_dir / d).mkdir(parents=True, exist_ok=True)
    
    def _get_cache_path(self, category: str, key: str) -> Path:
        """Get cache file path for a category and key."""
        return self.cache_dir / category / f"{key}.parquet"
    
    def _is_valid(self, path: Path) -> bool:
        """Check if cache file exists and is not expired."""
        if not path.exists():
            return False
        
        # Check modification time
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        age = datetime.now() - mtime
        return age < timedelta(hours=self.ttl_hours)
    
    def _hash_key(self, *args) -> str:
        """Create a hash key from arguments."""
        key_str = "|".join(str(a) for a in args)
        return hashlib.md5(key_str.encode()).hexdigest()[:12]
    
    # =========================================================================
    # OHLCV Caching
    # =========================================================================
    
    def get_ohlcv(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1d"
    ) -> Optional[list[OHLCV]]:
        """
        Get cached OHLCV data.
        
        Returns:
            List of OHLCV or None if not cached/expired
        """
        if not self.enabled:
            return None
        
        # For daily data, use date-based caching
        cache_key = f"{symbol}/{timeframe}/{start_date}_{end_date}"
        path = self._get_cache_path('ohlcv', cache_key.replace('/', '_'))
        
        if not self._is_valid(path):
            return None
        
        try:
            df = pd.read_parquet(path)
            return [
                OHLCV(
                    symbol=symbol,
                    timestamp=row['timestamp'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume']
                )
                for _, row in df.iterrows()
            ]
        except Exception:
            return None
    
    def store_ohlcv(
        self,
        symbol: str,
        data: list[OHLCV],
        timeframe: str = "1d"
    ) -> None:
        """Store OHLCV data in cache."""
        if not self.enabled or not data:
            return
        
        start_date = min(d.timestamp.date() for d in data)
        end_date = max(d.timestamp.date() for d in data)
        
        cache_key = f"{symbol}_{timeframe}_{start_date}_{end_date}"
        path = self._get_cache_path('ohlcv', cache_key)
        
        df = pd.DataFrame([d.model_dump() for d in data])
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
    
    # =========================================================================
    # Option Chain Caching
    # =========================================================================
    
    def get_option_chain(
        self,
        symbol: str,
        as_of_date: date
    ) -> Optional[OptionChain]:
        """
        Get cached option chain snapshot.
        
        Args:
            symbol: Underlying symbol
            as_of_date: Date of the snapshot
            
        Returns:
            OptionChain or None if not cached
        """
        if not self.enabled:
            return None
        
        cache_key = f"{symbol}_{as_of_date}"
        path = self._get_cache_path('options', cache_key)
        
        if not self._is_valid(path):
            return None
        
        try:
            # Read the cached chain
            df = pd.read_parquet(path)
            
            # Reconstruct OptionChain from DataFrame
            # This is a simplified reconstruction - full implementation
            # would need to store all nested data
            return None  # TODO: Implement full deserialization
        except Exception:
            return None
    
    def store_option_chain(
        self,
        chain: OptionChain,
        as_of_date: date
    ) -> None:
        """Store option chain snapshot in cache."""
        if not self.enabled:
            return
        
        cache_key = f"{chain.symbol}_{as_of_date}"
        path = self._get_cache_path('options', cache_key)
        
        # Flatten chain to DataFrame for storage
        records = []
        for contract in chain.contracts:
            record = contract.model_dump()
            record['underlying_price'] = chain.underlying_price
            record['chain_timestamp'] = chain.timestamp
            records.append(record)
        
        if records:
            df = pd.DataFrame(records)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, index=False)
    
    # =========================================================================
    # VIX Caching
    # =========================================================================
    
    def get_vix_history(
        self,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Get cached VIX history."""
        if not self.enabled:
            return None
        
        cache_key = f"vix_{start_date}_{end_date}"
        path = self._get_cache_path('vix', cache_key)
        
        if not self._is_valid(path):
            return None
        
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    
    def store_vix_history(
        self,
        df: pd.DataFrame,
        start_date: date,
        end_date: date
    ) -> None:
        """Store VIX history in cache."""
        if not self.enabled:
            return
        
        cache_key = f"vix_{start_date}_{end_date}"
        path = self._get_cache_path('vix', cache_key)
        
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
    
    # =========================================================================
    # Generic Caching
    # =========================================================================
    
    def get_dataframe(
        self,
        category: str,
        key: str
    ) -> Optional[pd.DataFrame]:
        """Get a cached DataFrame by category and key."""
        if not self.enabled:
            return None
        
        path = self._get_cache_path(category, key)
        
        if not self._is_valid(path):
            return None
        
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    
    def store_dataframe(
        self,
        df: pd.DataFrame,
        category: str,
        key: str
    ) -> None:
        """Store a DataFrame in cache."""
        if not self.enabled or df is None or df.empty:
            return
        
        path = self._get_cache_path(category, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
    
    # =========================================================================
    # Cache Management
    # =========================================================================
    
    def clear(self, category: Optional[str] = None) -> int:
        """
        Clear cached data.
        
        Args:
            category: Specific category to clear, or None for all
            
        Returns:
            Number of files deleted
        """
        count = 0
        
        if category:
            target_dir = self.cache_dir / category
            if target_dir.exists():
                for f in target_dir.rglob('*.parquet'):
                    f.unlink()
                    count += 1
        else:
            for f in self.cache_dir.rglob('*.parquet'):
                f.unlink()
                count += 1
        
        return count
    
    def get_cache_stats(self) -> dict:
        """Get cache usage statistics."""
        stats = {
            'enabled': self.enabled,
            'directory': str(self.cache_dir),
            'ttl_hours': self.ttl_hours,
            'categories': {}
        }
        
        for category_dir in self.cache_dir.iterdir():
            if category_dir.is_dir():
                files = list(category_dir.rglob('*.parquet'))
                total_size = sum(f.stat().st_size for f in files)
                stats['categories'][category_dir.name] = {
                    'file_count': len(files),
                    'size_mb': round(total_size / (1024 * 1024), 2)
                }
        
        return stats
