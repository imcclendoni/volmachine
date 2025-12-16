"""
Polygon.io Data Provider Implementation.

Polygon provides comprehensive options data including chains, Greeks, and IV.
Documentation: https://polygon.io/docs/options
"""

import os
from datetime import date, datetime, timedelta
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
        
        # Handle ${ENV_VAR} syntax from YAML config
        api_key = config.get('api_key', '')
        if api_key.startswith('${') and api_key.endswith('}'):
            # Extract env var name and look it up
            env_var_name = api_key[2:-1]
            api_key = os.environ.get(env_var_name, '')
        
        # Fallback to direct env var
        self.api_key = api_key or os.environ.get('POLYGON_API_KEY')
        self.base_url = config.get('base_url', 'https://api.polygon.io')
        self._session: Optional[requests.Session] = None
        
        if not self.api_key:
            raise DataProviderError("Polygon API key not provided")
    
    def connect(self) -> bool:
        """Establish connection by validating API key."""
        self._session = requests.Session()
        # Polygon uses apiKey query parameter, not Bearer token
        
        # Validate API key with a simple request
        try:
            response = self._session.get(
                f"{self.base_url}/v3/reference/tickers",
                params={'limit': 1, 'apiKey': self.api_key}
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
    
    def _request(self, endpoint: str, params: dict = None, retries: int = 3) -> dict:
        """
        Make authenticated request to Polygon API with rate limit handling.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            retries: Number of retries on rate limit (429)
        """
        import time
        
        if not self._session:
            raise DataProviderError("Not connected to Polygon")
        
        # Add apiKey to all requests
        if params is None:
            params = {}
        params['apiKey'] = self.api_key
        
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(retries + 1):
            response = self._session.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            
            # Rate limit - wait and retry
            if response.status_code == 429:
                if attempt < retries:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    time.sleep(wait_time)
                    continue
            
            # Other errors - fail immediately
            raise DataProviderError(
                f"Polygon API error: {response.status_code} - {response.text}"
            )
        
        # Should not reach here, but safety fallback
        raise DataProviderError(f"Polygon API failed after {retries} retries")
    
    # =========================================================================
    # Underlying Price Data
    # =========================================================================
    
    def get_current_price(self, symbol: str) -> float:
        """
        Get current/latest price from Polygon.
        
        Uses daily aggregates (bars) endpoint instead of snapshot
        since snapshot requires Stocks Starter add-on.
        """
        from datetime import date, timedelta
        
        today = date.today()
        # Get last 5 days to handle weekends/holidays
        start = (today - timedelta(days=7)).isoformat()
        end = today.isoformat()
        
        data = self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
            params={'adjusted': 'true', 'sort': 'desc', 'limit': 1}
        )
        
        results = data.get('results', [])
        if not results:
            raise DataProviderError(f"No price data found for {symbol}")
        
        # Return most recent close
        return float(results[0]['c'])
    
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
        expiration: Optional[date] = None,
        min_dte: int = 0,
        max_dte: int = 90
    ) -> OptionChain:
        """
        Get option chain with quotes and Greeks from Polygon.
        
        Args:
            symbol: Underlying symbol
            expiration: Specific expiration date (if None, fetches range)
            min_dte: Minimum days to expiration (default 0)
            max_dte: Maximum days to expiration (default 90)
        
        Returns:
            OptionChain with contracts and expirations
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Get current underlying price
        underlying_price = self.get_current_price(symbol)
        
        today = date.today()
        
        # Build params for contract lookup with DATE RANGE
        params = {
            'underlying_ticker': symbol,
            'limit': 1000,
            'order': 'asc',
            'sort': 'expiration_date',
        }
        
        if expiration:
            # Specific expiration requested
            params['expiration_date'] = expiration.isoformat()
        else:
            # Use date range filtering - THIS IS THE FIX
            min_exp = today + timedelta(days=min_dte)
            max_exp = today + timedelta(days=max_dte)
            params['expiration_date.gte'] = min_exp.isoformat()
            params['expiration_date.lte'] = max_exp.isoformat()
        
        # Fetch contracts with PAGINATION
        all_contracts_meta = []
        next_url = None
        page_count = 0
        max_pages = 10  # Safety limit
        
        while page_count < max_pages:
            if next_url:
                # Use next_url for pagination (Polygon returns full URL)
                response = self._session.get(next_url, params={'apiKey': self.api_key})
                if response.status_code != 200:
                    break
                data = response.json()
            else:
                data = self._request("/v3/reference/options/contracts", params=params)
            
            results = data.get('results', [])
            all_contracts_meta.extend(results)
            page_count += 1
            
            # Check for next page
            next_url = data.get('next_url')
            if not next_url:
                break
        
        logger.info(f"Polygon {symbol}: fetched {len(all_contracts_meta)} contracts in {page_count} pages")
        
        if not all_contracts_meta:
            # Log diagnostic info
            logger.warning(f"No contracts found for {symbol} in DTE range {min_dte}-{max_dte}")
            
            # FALLBACK: Try to find ANY available expiration
            fallback_params = {
                'underlying_ticker': symbol,
                'limit': 100,
                'order': 'asc',
                'sort': 'expiration_date',
                'expiration_date.gte': today.isoformat(),  # Just future expiries
            }
            fallback_data = self._request("/v3/reference/options/contracts", params=fallback_params)
            fallback_results = fallback_data.get('results', [])
            
            if fallback_results:
                logger.warning(f"FALLBACK: Found {len(fallback_results)} contracts outside target DTE range")
                all_contracts_meta = fallback_results
                # Mark that we used fallback (can be used to mark candidates REVIEW)
                # This is logged for awareness
            else:
                raise DataProviderError(f"No options contracts found for {symbol}")
        
        # Collect unique expirations
        expirations_set = set()
        for meta in all_contracts_meta:
            exp_str = meta.get('expiration_date')
            if exp_str:
                expirations_set.add(date.fromisoformat(exp_str))
        
        sorted_expirations = sorted(expirations_set)
        
        # Log expiration info
        if sorted_expirations:
            min_exp = sorted_expirations[0]
            max_exp = sorted_expirations[-1]
            min_dte_found = (min_exp - today).days
            max_dte_found = (max_exp - today).days
            in_range = len([e for e in sorted_expirations if min_dte <= (e - today).days <= max_dte])
            logger.info(f"Polygon {symbol}: {len(sorted_expirations)} expirations, "
                       f"range: {min_exp} ({min_dte_found}d) to {max_exp} ({max_dte_found}d), "
                       f"{in_range} in target DTE window")
        
        # =====================================================================
        # PERFORMANCE OPTIMIZATION: Filter contracts before snapshotting
        # Only snapshot what we might realistically trade
        # =====================================================================
        
        # 1. Select best 2 expirations around target DTE (e.g., 30-45 days)
        target_dte = 30  # Ideal DTE for most strategies
        sorted_exps_with_dte = [(exp, abs((exp - today).days - target_dte)) for exp in sorted_expirations]
        sorted_exps_with_dte.sort(key=lambda x: x[1])  # Sort by distance from target
        selected_expirations = set([exp for exp, _ in sorted_exps_with_dte[:2]])  # Nearest 2
        
        if selected_expirations:
            logger.info(f"Polygon {symbol}: selected {len(selected_expirations)} expirations near {target_dte}d target")
        
        # 2. Filter strikes to ±20% of underlying price
        strike_min = underlying_price * 0.80
        strike_max = underlying_price * 1.20
        
        # 3. Apply filters to contract list
        filtered_contracts = []
        for meta in all_contracts_meta:
            exp_str = meta.get('expiration_date')
            strike = meta.get('strike_price', 0)
            
            if not exp_str:
                continue
                
            exp_date = date.fromisoformat(exp_str)
            
            # Check expiration filter
            if exp_date not in selected_expirations:
                continue
            
            # Check strike window
            if not (strike_min <= strike <= strike_max):
                continue
            
            filtered_contracts.append(meta)
        
        logger.info(f"Polygon {symbol}: filtered {len(all_contracts_meta)} -> {len(filtered_contracts)} contracts "
                   f"(strikes {strike_min:.0f}-{strike_max:.0f}, {len(selected_expirations)} expirations)")
        
        # Get snapshots for filtered contracts only
        contracts = []
        chain_timestamp = datetime.now()
        snapshot_errors = 0
        
        for meta in filtered_contracts:
            contract_symbol = meta.get('ticker')
            exp_date = date.fromisoformat(meta.get('expiration_date'))
            
            # Get snapshot for this contract
            try:
                snapshot = self._request(
                    f"/v3/snapshot/options/{symbol}/{contract_symbol}"
                )
                details = snapshot.get('results', {})
                day = details.get('day', {})
                greeks_data = details.get('greeks', {})
                
                # Build Greeks with EPSILON-based clamping for numerical noise only
                # This applies at contract level only - aggregated structure Greeks can be negative
                EPSILON = 1e-6
                greeks = None
                if greeks_data:
                    delta = greeks_data.get('delta', 0)
                    gamma = greeks_data.get('gamma', 0)
                    theta = greeks_data.get('theta', 0)  # Can be negative
                    vega = greeks_data.get('vega', 0)
                    
                    # Epsilon tolerance: only clamp if negative by tiny amount (noise)
                    if gamma < 0 and abs(gamma) < EPSILON:
                        gamma = 0
                    if vega < 0 and abs(vega) < EPSILON:
                        vega = 0
                    
                    # Clamp delta to [-1, 1] with epsilon tolerance
                    if delta < -1 and abs(delta + 1) < EPSILON:
                        delta = -1
                    elif delta > 1 and abs(delta - 1) < EPSILON:
                        delta = 1
                    
                    greeks = Greeks(
                        delta=delta,
                        gamma=gamma,
                        theta=theta,
                        vega=vega,
                    )
                
                # Use API timestamp if available
                quote_timestamp = None
                if details.get('last_updated'):
                    try:
                        quote_timestamp = datetime.fromtimestamp(details['last_updated'] / 1e9)
                        chain_timestamp = quote_timestamp
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
                
            except DataProviderError as e:
                snapshot_errors += 1
                if snapshot_errors <= 3:
                    logger.debug(f"Snapshot error for {contract_symbol}: {e}")
                continue
        
        if snapshot_errors > 0:
            logger.warning(f"Polygon {symbol}: {snapshot_errors} snapshot errors (may be rate limited)")
        
        logger.info(f"Polygon {symbol}: got snapshots for {len(contracts)} contracts")
        
        return OptionChain(
            symbol=symbol,
            underlying_price=underlying_price,
            timestamp=chain_timestamp,
            expirations=sorted_expirations,
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
