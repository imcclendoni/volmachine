"""
Polygon.io Data Provider Implementation.

Polygon provides comprehensive options data including chains, Greeks, and IV.
Documentation: https://polygon.io/docs/options
"""

import os
from datetime import date, datetime
from typing import Optional

import requests

from data.providers.base import DataProvider, DataProviderError
from data.schemas import (
    OHLCV,
    Greeks,
    OptionChain,
    OptionContract,
    OptionType,
)


class PolygonProvider(DataProvider):
    """Polygon.io data provider implementation."""
    
    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key = config.get('api_key') or os.environ.get('POLYGON_API_KEY')
        self.base_url = config.get('base_url', 'https://api.polygon.io')
        self._session: Optional[requests.Session] = None
        
        if not self.api_key:
            raise DataProviderError("Polygon API key not provided")
    
    def connect(self) -> bool:
        """Establish connection by validating API key."""
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {self.api_key}'
        })
        
        # Validate API key with a simple request
        try:
            response = self._session.get(
                f"{self.base_url}/v3/reference/tickers",
                params={'limit': 1}
            )
            if response.status_code == 200:
                self._connected = True
                return True
            elif response.status_code == 401:
                raise DataProviderError("Invalid Polygon API key")
            else:
                raise DataProviderError(f"Polygon connection failed: {response.text}")
        except requests.RequestException as e:
            raise DataProviderError(f"Polygon connection error: {e}")
    
    def disconnect(self) -> None:
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None
        self._connected = False
    
    def _request(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated request to Polygon API."""
        if not self._session:
            raise DataProviderError("Not connected to Polygon")
        
        url = f"{self.base_url}{endpoint}"
        response = self._session.get(url, params=params)
        
        if response.status_code != 200:
            raise DataProviderError(
                f"Polygon API error: {response.status_code} - {response.text}"
            )
        
        return response.json()
    
    # =========================================================================
    # Underlying Price Data
    # =========================================================================
    
    def get_current_price(self, symbol: str) -> float:
        """Get current price from Polygon snapshot."""
        data = self._request(f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
        
        if 'ticker' not in data:
            raise DataProviderError(f"No data found for {symbol}")
        
        ticker = data['ticker']
        # Use last trade price, or close if last not available
        price = ticker.get('lastTrade', {}).get('p') or ticker.get('day', {}).get('c')
        
        if price is None:
            raise DataProviderError(f"No price available for {symbol}")
        
        return float(price)
    
    def get_historical_ohlcv(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1d"
    ) -> list[OHLCV]:
        """Get historical bars from Polygon."""
        # Convert timeframe to Polygon format
        tf_map = {
            "1d": ("day", 1),
            "1h": ("hour", 1),
            "30m": ("minute", 30),
            "15m": ("minute", 15),
            "5m": ("minute", 5),
            "1m": ("minute", 1),
        }
        
        if timeframe not in tf_map:
            raise DataProviderError(f"Unsupported timeframe: {timeframe}")
        
        timespan, multiplier = tf_map[timeframe]
        
        data = self._request(
            f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}"
            f"/{start_date.isoformat()}/{end_date.isoformat()}",
            params={'adjusted': 'true', 'sort': 'asc', 'limit': 50000}
        )
        
        results = data.get('results', [])
        
        ohlcv_list = []
        for bar in results:
            ohlcv_list.append(OHLCV(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(bar['t'] / 1000),
                open=bar['o'],
                high=bar['h'],
                low=bar['l'],
                close=bar['c'],
                volume=bar['v']
            ))
        
        return ohlcv_list
    
    # =========================================================================
    # Options Data
    # =========================================================================
    
    def get_option_expirations(self, symbol: str) -> list[date]:
        """Get available option expirations from Polygon."""
        data = self._request(
            f"/v3/reference/options/contracts",
            params={
                'underlying_ticker': symbol,
                'limit': 1000,
                'order': 'asc',
                'sort': 'expiration_date'
            }
        )
        
        expirations = set()
        for contract in data.get('results', []):
            exp_str = contract.get('expiration_date')
            if exp_str:
                expirations.add(date.fromisoformat(exp_str))
        
        return sorted(expirations)
    
    def get_option_chain(
        self,
        symbol: str,
        expiration: Optional[date] = None
    ) -> OptionChain:
        """Get option chain with quotes and Greeks from Polygon."""
        # Get current underlying price
        underlying_price = self.get_current_price(symbol)
        
        # Build params for contract lookup
        params = {
            'underlying_ticker': symbol,
            'limit': 1000,
        }
        if expiration:
            params['expiration_date'] = expiration.isoformat()
        
        # Get contracts
        data = self._request("/v3/reference/options/contracts", params=params)
        contracts_meta = data.get('results', [])
        
        if not contracts_meta:
            raise DataProviderError(f"No options contracts found for {symbol}")
        
        # Get snapshots for quotes and Greeks
        contracts = []
        expirations_set = set()
        chain_timestamp = datetime.now()  # Will be updated if API provides
        
        for meta in contracts_meta:
            contract_symbol = meta.get('ticker')
            exp_date = date.fromisoformat(meta.get('expiration_date'))
            expirations_set.add(exp_date)
            
            # Get snapshot for this contract (quotes + Greeks)
            try:
                snapshot = self._request(
                    f"/v3/snapshot/options/{symbol}/{contract_symbol}"
                )
                details = snapshot.get('results', {})
                day = details.get('day', {})
                greeks_data = details.get('greeks', {})
                
                # Build Greeks if available
                greeks = None
                if greeks_data:
                    greeks = Greeks(
                        delta=greeks_data.get('delta', 0),
                        gamma=greeks_data.get('gamma', 0),
                        theta=greeks_data.get('theta', 0),
                        vega=greeks_data.get('vega', 0),
                    )
                
                # Use API timestamp if available, not datetime.now()
                # last_updated is in nanoseconds
                quote_timestamp = None
                if details.get('last_updated'):
                    try:
                        quote_timestamp = datetime.fromtimestamp(details['last_updated'] / 1e9)
                        chain_timestamp = quote_timestamp  # Update chain timestamp
                    except (ValueError, OSError):
                        quote_timestamp = datetime.now()
                else:
                    quote_timestamp = datetime.now()
                
                contract = OptionContract(
                    symbol=symbol,
                    contract_symbol=contract_symbol,
                    option_type=OptionType.CALL if meta.get('contract_type') == 'call' else OptionType.PUT,
                    strike=meta.get('strike_price'),
                    expiration=exp_date,
                    bid=day.get('bid', 0) or 0,
                    ask=day.get('ask', 0) or 0,
                    last=day.get('last', 0),
                    iv=details.get('implied_volatility'),
                    greeks=greeks,
                    volume=day.get('volume', 0) or 0,
                    open_interest=details.get('open_interest', 0) or 0,
                    quote_time=quote_timestamp,
                )
                contracts.append(contract)
                
            except DataProviderError:
                # Skip contracts without snapshots
                continue
        
        return OptionChain(
            symbol=symbol,
            underlying_price=underlying_price,
            timestamp=chain_timestamp,
            expirations=sorted(expirations_set),
            contracts=contracts
        )
    
    # =========================================================================
    # Volatility Indices
    # =========================================================================
    
    def get_vix(self) -> float:
        """Get current VIX value."""
        # VIX is ticker "I:VIX" on Polygon (index)
        data = self._request("/v2/snapshot/locale/us/markets/stocks/tickers/VIX")
        
        if 'ticker' not in data:
            raise DataProviderError("VIX data not available")
        
        return data['ticker'].get('day', {}).get('c', 0)
    
    def get_vvix(self) -> Optional[float]:
        """Get VVIX if available."""
        try:
            data = self._request("/v2/snapshot/locale/us/markets/stocks/tickers/VVIX")
            return data.get('ticker', {}).get('day', {}).get('c')
        except DataProviderError:
            return None
