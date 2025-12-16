"""
Tradier Data Provider Implementation.

Tradier provides brokerage + market data API with good options coverage.
Documentation: https://documentation.tradier.com/
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


class TradierProvider(DataProvider):
    """Tradier data provider implementation."""
    
    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key = config.get('api_key') or os.environ.get('TRADIER_API_KEY')
        self.sandbox = config.get('sandbox', True)
        
        if self.sandbox:
            self.base_url = 'https://sandbox.tradier.com/v1'
        else:
            self.base_url = config.get('base_url', 'https://api.tradier.com/v1')
        
        self._session: Optional[requests.Session] = None
        
        if not self.api_key:
            raise DataProviderError("Tradier API key not provided")
    
    def connect(self) -> bool:
        """Establish connection by validating API key."""
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        })
        
        # Validate API key
        try:
            response = self._session.get(f"{self.base_url}/user/profile")
            if response.status_code == 200:
                self._connected = True
                return True
            elif response.status_code == 401:
                raise DataProviderError("Invalid Tradier API key")
            else:
                raise DataProviderError(f"Tradier connection failed: {response.text}")
        except requests.RequestException as e:
            raise DataProviderError(f"Tradier connection error: {e}")
    
    def disconnect(self) -> None:
        """Close the session."""
        if self._session:
            self._session.close()
            self._session = None
        self._connected = False
    
    def _request(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated request to Tradier API."""
        if not self._session:
            raise DataProviderError("Not connected to Tradier")
        
        url = f"{self.base_url}{endpoint}"
        response = self._session.get(url, params=params)
        
        if response.status_code != 200:
            raise DataProviderError(
                f"Tradier API error: {response.status_code} - {response.text}"
            )
        
        return response.json()
    
    # =========================================================================
    # Underlying Price Data
    # =========================================================================
    
    def get_current_price(self, symbol: str) -> float:
        """Get current price from Tradier quotes."""
        data = self._request("/markets/quotes", params={'symbols': symbol})
        
        quotes = data.get('quotes', {})
        quote = quotes.get('quote', {})
        
        if not quote:
            raise DataProviderError(f"No quote data for {symbol}")
        
        # Use last price, or close if market is closed
        price = quote.get('last') or quote.get('close')
        
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
        """Get historical bars from Tradier."""
        # Tradier uses 'daily', 'weekly', 'monthly' for history
        # For intraday, need to use timesales endpoint
        
        if timeframe == "1d":
            data = self._request(
                "/markets/history",
                params={
                    'symbol': symbol,
                    'interval': 'daily',
                    'start': start_date.isoformat(),
                    'end': end_date.isoformat()
                }
            )
            
            history = data.get('history', {})
            days = history.get('day', [])
            
            if not isinstance(days, list):
                days = [days] if days else []
            
            ohlcv_list = []
            for bar in days:
                ohlcv_list.append(OHLCV(
                    symbol=symbol,
                    timestamp=datetime.fromisoformat(bar['date']),
                    open=bar['open'],
                    high=bar['high'],
                    low=bar['low'],
                    close=bar['close'],
                    volume=bar['volume']
                ))
            
            return ohlcv_list
        else:
            # Intraday - use timesales
            interval_map = {
                "1m": "1min",
                "5m": "5min",
                "15m": "15min",
            }
            
            if timeframe not in interval_map:
                raise DataProviderError(
                    f"Tradier doesn't support timeframe: {timeframe}"
                )
            
            data = self._request(
                "/markets/timesales",
                params={
                    'symbol': symbol,
                    'interval': interval_map[timeframe],
                    'start': f"{start_date.isoformat()} 09:30",
                    'end': f"{end_date.isoformat()} 16:00"
                }
            )
            
            series = data.get('series', {}).get('data', [])
            
            ohlcv_list = []
            for bar in series:
                ohlcv_list.append(OHLCV(
                    symbol=symbol,
                    timestamp=datetime.fromisoformat(bar['time']),
                    open=bar['open'],
                    high=bar['high'],
                    low=bar['low'],
                    close=bar['close'],
                    volume=bar.get('volume', 0)
                ))
            
            return ohlcv_list
    
    # =========================================================================
    # Options Data
    # =========================================================================
    
    def get_option_expirations(self, symbol: str) -> list[date]:
        """Get available option expirations from Tradier."""
        data = self._request(
            "/markets/options/expirations",
            params={'symbol': symbol}
        )
        
        expirations = data.get('expirations', {}).get('date', [])
        
        if not isinstance(expirations, list):
            expirations = [expirations] if expirations else []
        
        return [date.fromisoformat(exp) for exp in expirations]
    
    def get_option_chain(
        self,
        symbol: str,
        expiration: Optional[date] = None
    ) -> OptionChain:
        """Get option chain with quotes and Greeks from Tradier."""
        # Get underlying price
        underlying_price = self.get_current_price(symbol)
        
        # Get expirations
        if expiration:
            expirations = [expiration]
        else:
            expirations = self.get_option_expirations(symbol)
            # Limit to first 4 expirations to avoid rate limits
            expirations = expirations[:4]
        
        contracts = []
        
        for exp in expirations:
            # Get option chain for this expiration
            data = self._request(
                "/markets/options/chains",
                params={
                    'symbol': symbol,
                    'expiration': exp.isoformat(),
                    'greeks': 'true'
                }
            )
            
            options = data.get('options', {}).get('option', [])
            
            if not isinstance(options, list):
                options = [options] if options else []
            
            for opt in options:
                greeks_data = opt.get('greeks', {})
                greeks = None
                if greeks_data:
                    greeks = Greeks(
                        delta=greeks_data.get('delta', 0) or 0,
                        gamma=greeks_data.get('gamma', 0) or 0,
                        theta=greeks_data.get('theta', 0) or 0,
                        vega=greeks_data.get('vega', 0) or 0,
                    )
                
                option_type_str = opt.get('option_type', 'call').lower()
                
                contract = OptionContract(
                    symbol=symbol,
                    contract_symbol=opt.get('symbol', ''),
                    option_type=OptionType.CALL if option_type_str == 'call' else OptionType.PUT,
                    strike=opt.get('strike', 0),
                    expiration=exp,
                    bid=opt.get('bid', 0) or 0,
                    ask=opt.get('ask', 0) or 0,
                    last=opt.get('last'),
                    iv=greeks_data.get('mid_iv') if greeks_data else None,
                    greeks=greeks,
                    volume=opt.get('volume', 0) or 0,
                    open_interest=opt.get('open_interest', 0) or 0,
                    quote_time=datetime.now(),
                )
                contracts.append(contract)
        
        return OptionChain(
            symbol=symbol,
            underlying_price=underlying_price,
            timestamp=datetime.now(),
            expirations=sorted(expirations),
            contracts=contracts
        )
    
    # =========================================================================
    # Volatility Indices
    # =========================================================================
    
    def get_vix(self) -> float:
        """Get current VIX value."""
        # VIX is available as a regular quote
        data = self._request("/markets/quotes", params={'symbols': 'VIX'})
        
        quotes = data.get('quotes', {})
        quote = quotes.get('quote', {})
        
        if not quote:
            raise DataProviderError("VIX data not available")
        
        return quote.get('last') or quote.get('close', 0)
