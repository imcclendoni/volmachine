"""
Main Engine Orchestrator.

Coordinates all components to produce trade candidates:
1. Classify regime
2. Detect edges
3. Build structures (iterating widths until valid)
4. Apply risk rules  
5. Generate candidates and report

FIXES:
- Use run_date throughout instead of date.today()
- Risk-aware structure building: search widths until validation+sizing passes
- Validation gating: if invalid or sizing fails → PASS, don't recommend
- No hardcoded width=5 for small accounts
"""

from datetime import date, datetime, timedelta
from typing import Optional
from pathlib import Path
import yaml

from data.schemas import (
    OHLCV,
    OptionChain,
    OptionStructure,
    RegimeClassification,
    EdgeSignal,
    TradeCandidate,
    PortfolioState,
    OptionType,
    DailyReport,
    EdgeType,
    RegimeState,
    TradeDirection,
)
from data.providers import get_provider, DataProvider
from data.cache import DataCache

from regime import RegimeEngine
from edges import (
    VRPDetector, VRPConfig,
    TermStructureDetector, TermStructureConfig,
    SkewDetector, SkewConfig,
    EventVolDetector, EventVolConfig,
    GammaPressureDetector, GammaConfig,
)
from structures import (
    build_credit_spread,
    build_iron_condor,
    build_butterfly,
    build_calendar,
    validate_structure,
    ValidationConfig,
)
from structures.builders import BuilderConfig
from risk import (
    Portfolio,
    SizingConfig,
    calculate_size,
    LimitTracker,
    LimitConfig,
    run_stress_test,
    StressConfig,
)
from engine.decision import create_trade_candidate, candidate_to_dict
from engine.logger import get_logger, StructuredLogger
from engine.report import create_daily_report, save_report


class VolMachineEngine:
    """
    Main orchestration engine for the volatility trading system.
    
    Coordinates:
    - Data fetching
    - Regime classification
    - Edge detection
    - Structure building
    - Risk management
    - Trade candidate generation
    """
    
    def __init__(
        self,
        config_path: str = "./config/settings.yaml",
        universe_path: str = "./config/universe.yaml",
    ):
        """
        Initialize the engine.
        
        Args:
            config_path: Path to settings.yaml
            universe_path: Path to universe.yaml
        """
        # Load config
        self.config = self._load_config(config_path)
        self.universe = self._load_config(universe_path)
        
        # Initialize logger
        log_config = self.config.get('logging', {})
        self.logger = get_logger(
            log_dir=log_config.get('log_directory', './logs'),
        )
        
        # Initialize data provider
        data_config = self.config.get('data', {})
        provider_name = data_config.get('provider', 'polygon')
        provider_config = data_config.get(provider_name, {})
        
        try:
            self.provider = get_provider(provider_name, provider_config)
        except Exception as e:
            self.logger.warning('provider_init_failed', error=str(e))
            self.provider = None
        
        # Initialize cache
        cache_config = self.config.get('cache', {})
        self.cache = DataCache(cache_config)
        
        # Initialize regime engine
        self.regime_engine = RegimeEngine(
            provider=self.provider,
            cache=self.cache,
        ) if self.provider else None
        
        # Initialize edge detectors
        self._init_edge_detectors()
        
        # Initialize risk components (using config equity)
        self._init_risk_components()
        
        # Portfolio (using config equity)
        account = self.config.get('account', {})
        account_equity = account.get('equity', 100000)
        
        self.portfolio = Portfolio(
            account_equity=account_equity,
        )
        
        # Edge health tracker (for monitoring, NOT optimization)
        from engine.edge_health import EdgePerformanceTracker
        self.edge_tracker = EdgePerformanceTracker(
            storage_path=Path(self.config.get('logging', {}).get('log_directory', './logs')) / 'edge_health'
        )
        
        # State
        self._current_regime: Optional[RegimeClassification] = None
        self._edge_signals: list[EdgeSignal] = []
        self._trade_candidates: list[TradeCandidate] = []
        self._run_date: date = date.today()
    
    def _load_config(self, path: str) -> dict:
        """Load YAML config file."""
        config_path = Path(path)
        if config_path.exists():
            with open(config_path) as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def _init_edge_detectors(self):
        """Initialize all edge detectors."""
        edge_config = self.config.get('edges', {})
        
        self.vrp_detector = VRPDetector(VRPConfig(
            iv_rv_ratio_threshold=edge_config.get('vrp', {}).get('iv_rv_ratio_threshold', 1.3),
            percentile_threshold=edge_config.get('vrp', {}).get('percentile_threshold', 80),
        ))
        
        self.term_structure_detector = TermStructureDetector(TermStructureConfig())
        self.skew_detector = SkewDetector(SkewConfig())
        self.event_vol_detector = EventVolDetector(EventVolConfig())
        self.gamma_detector = GammaPressureDetector(GammaConfig())
    
    def _init_risk_components(self):
        """Initialize risk management components."""
        risk_config = self.config.get('risk', {})
        account = self.config.get('account', {})
        account_equity = account.get('equity', 100000)
        
        self.sizing_config = SizingConfig(
            account_equity=account_equity,
            max_risk_per_trade_pct=risk_config.get('max_risk_per_trade_pct', 1.0),
            max_total_risk_pct=risk_config.get('max_total_risk_pct', 10.0),
            max_trades_open=risk_config.get('max_trades_open', 10),
        )
        
        self.limit_tracker = LimitTracker(LimitConfig(
            account_equity=account_equity,
            daily_loss_limit_pct=risk_config.get('daily_loss_limit_pct', 3.0),
            weekly_loss_limit_pct=risk_config.get('weekly_loss_limit_pct', 5.0),
            max_drawdown_pct=risk_config.get('max_drawdown_pct', 15.0),
        ))
        
        stress_config = risk_config.get('stress', {})
        self.stress_config = StressConfig(
            gap_scenarios=stress_config.get('gap_scenarios', [0.02, 0.05]),
            iv_shift_points=stress_config.get('iv_shift_points', 5),
        )
        
        # Builder config
        structure_config = self.config.get('structures', {})
        self.builder_config = BuilderConfig(
            preferred_width_points=structure_config.get('preferred_width', 5),
            min_width_points=structure_config.get('min_width', 1),
            max_width_points=structure_config.get('max_width', 20),
            min_dte=structure_config.get('min_dte', 7),
            max_dte=structure_config.get('max_dte', 60),
            target_dte=structure_config.get('target_dte', 30),
            enforce_liquidity=structure_config.get('enforce_liquidity', True),
        )
    
    def get_enabled_symbols(self) -> list[str]:
        """Get list of enabled symbols from universe."""
        symbols = []
        for sym, cfg in self.universe.get('symbols', {}).items():
            if cfg.get('enabled', True):
                symbols.append(sym)
        return symbols
    
    def connect(self) -> bool:
        """Connect to data provider."""
        if self.provider is None:
            self.logger.error('no_provider_configured')
            return False
        
        try:
            connected = self.provider.connect()
            if connected:
                self.logger.info('provider_connected', provider=self.provider.name)
            return connected
        except Exception as e:
            self.logger.error('provider_connect_failed', error=str(e))
            return False
    
    def run_daily(self, run_date: Optional[date] = None) -> DailyReport:
        """
        Run daily analysis.
        
        Args:
            run_date: Date to run for (default: today)
            
        Returns:
            DailyReport with all results
        """
        self._run_date = run_date or date.today()
        
        self.logger.info('daily_run_started', date=self._run_date.isoformat())
        
        # Check limits
        limit_status = self.limit_tracker.check_limits()
        if not limit_status.trading_allowed:
            self.logger.warning('trading_blocked', reason=limit_status.blocked_reason)
        
        # Get symbols
        symbols = self.get_enabled_symbols()
        
        # Classify regime
        regime = self._classify_regime()
        
        # Detect edges and build candidates for each symbol
        all_edges = []
        all_candidates = []
        vol_state = {}
        term_structure_state = {}
        
        for symbol in symbols:
            try:
                edges = self._detect_edges(symbol, regime)
                
                # Filter edges by health status (skip suspended edges)
                tradeable_edges = []
                for edge in edges:
                    is_tradeable, reason = self.edge_tracker.is_edge_tradeable(edge.edge_type)
                    if is_tradeable:
                        tradeable_edges.append(edge)
                    else:
                        self.logger.info(
                            'edge_suspended', 
                            edge_type=edge.edge_type.value, 
                            symbol=symbol,
                            reason=reason
                        )
                
                all_edges.extend(edges)  # Still track all edges for reporting
                
                candidates = self._build_candidates(symbol, tradeable_edges, regime)
                all_candidates.extend(candidates)
                
            except Exception as e:
                self.logger.error('symbol_processing_error', symbol=symbol, error=str(e))
        
        # Store state
        self._edge_signals = all_edges
        self._trade_candidates = all_candidates
        
        # Create report
        report = create_daily_report(
            report_date=self._run_date,
            regime=regime,
            vol_state=vol_state,
            term_structure=term_structure_state,
            edges=all_edges,
            candidates=all_candidates,
            portfolio=self.portfolio.get_state(),
            trading_allowed=limit_status.trading_allowed,
            do_not_trade_reasons=self.limit_tracker.get_do_not_trade_reasons(),
        )
        
        # Log candidates
        for candidate in all_candidates:
            self.logger.log_candidate(candidate_to_dict(candidate))
        
        self.logger.info('daily_run_completed', 
                        edges_found=len(all_edges),
                        candidates_generated=len(all_candidates))
        
        return report
    
    def _classify_regime(self) -> RegimeClassification:
        """Classify current market regime."""
        if self.regime_engine is None:
            return RegimeClassification(
                timestamp=datetime.now(),
                regime=RegimeState.CHOP,
                confidence=0.3,
                features={},
                rationale="No data provider - defaulting to CHOP"
            )
        
        try:
            regime = self.regime_engine.classify()
            self._current_regime = regime
            self.logger.log_regime({
                'regime': regime.regime.value,
                'confidence': regime.confidence,
                'rationale': regime.rationale,
            })
            return regime
        except Exception as e:
            self.logger.error('regime_classification_failed', error=str(e))
            return RegimeClassification(
                timestamp=datetime.now(),
                regime=RegimeState.CHOP,
                confidence=0.3,
                features={},
                rationale=f"Classification failed: {e}"
            )
    
    def _detect_edges(
        self,
        symbol: str,
        regime: RegimeClassification,
    ) -> list[EdgeSignal]:
        """Detect edges for a symbol."""
        if self.provider is None:
            return []
        
        edges = []
        
        try:
            # Get data using run_date as reference
            end_date = self._run_date
            start_date = end_date - timedelta(days=300)
            
            ohlcv = self.provider.get_historical_ohlcv(symbol, start_date, end_date)
            option_chain = self.provider.get_option_chain(symbol)
            
            # Run all detectors
            vrp = self.vrp_detector.detect(symbol, option_chain, ohlcv, regime.regime)
            if vrp:
                edges.append(vrp)
                self.logger.log_edge({'type': 'vrp', 'symbol': symbol, 'strength': vrp.strength})
            
            ts = self.term_structure_detector.detect(symbol, option_chain, regime.regime)
            if ts:
                edges.append(ts)
            
            skew = self.skew_detector.detect(symbol, option_chain, regime.regime)
            if skew:
                edges.append(skew)
            
            event = self.event_vol_detector.detect(symbol, option_chain, regime.regime)
            if event:
                edges.append(event)
            
            gamma = self.gamma_detector.detect(symbol, option_chain, regime.regime)
            if gamma:
                edges.append(gamma)
            
        except Exception as e:
            self.logger.error('edge_detection_failed', symbol=symbol, error=str(e))
        
        return edges
    
    def _build_candidates(
        self,
        symbol: str,
        edges: list[EdgeSignal],
        regime: RegimeClassification,
    ) -> list[TradeCandidate]:
        """
        Build trade candidates from edges.
        
        CRITICAL: Validation gating - if validation fails OR sizing fails, candidate is PASS.
        Risk-aware: Search multiple widths until one passes validation+sizing.
        """
        if not edges or self.provider is None:
            return []
        
        candidates = []
        
        try:
            option_chain = self.provider.get_option_chain(symbol)
            
            for edge in edges:
                # Try to find a valid structure (searching widths)
                structure, validation_messages, attempt_diagnostics = self._find_valid_structure(
                    edge, regime, option_chain
                )
                
                if structure is None:
                    # No valid structure found - create PASS candidate with diagnostics
                    candidate = self._create_pass_candidate(
                        symbol, edge, regime, 
                        "No valid structure found for risk parameters",
                        diagnostics=attempt_diagnostics
                    )
                    candidates.append(candidate)
                    continue
                
                # Validate
                validation = validate_structure(
                    structure,
                    self.sizing_config.account_equity,
                )
                
                # VALIDATION GATING: if not valid, candidate is PASS
                if not validation.is_valid:
                    candidate = self._create_pass_candidate(
                        symbol, edge, regime, 
                        f"Validation failed: {'; '.join(validation.messages)}"
                    )
                    candidates.append(candidate)
                    continue
                
                # Size
                sizing = calculate_size(
                    structure,
                    self.sizing_config,
                    self.portfolio.get_total_max_loss(),
                    self.portfolio.position_count,
                )
                
                # SIZING GATING: if sizing rejected, candidate is PASS
                if not sizing.allowed:
                    candidate = self._create_pass_candidate(
                        symbol, edge, regime,
                        f"Sizing rejected: {sizing.rejection_reason}"
                    )
                    candidates.append(candidate)
                    continue
                
                # Create TRADE candidate
                candidate = create_trade_candidate(
                    symbol=symbol,
                    structure=structure,
                    edge=edge,
                    regime=regime,
                    sizing=sizing,
                    validation_messages=validation.warnings,
                )
                
                candidates.append(candidate)
                
        except Exception as e:
            self.logger.error('candidate_building_failed', symbol=symbol, error=str(e))
        
        return candidates
    
    def _find_valid_structure(
        self,
        edge: EdgeSignal,
        regime: RegimeClassification,
        option_chain: OptionChain,
    ) -> tuple[Optional[OptionStructure], list[str], list]:
        """
        Find a structure that passes validation and sizing.
        
        Returns:
            Tuple of (structure, validation_messages, attempt_diagnostics)
            - structure: valid OptionStructure or None
            - validation_messages: warnings from successful validation
            - attempt_diagnostics: list of StructureAttempt for PASS diagnostics
        """
        from data.schemas import StructureAttempt
        
        spot = option_chain.underlying_price
        atm_strike = round(spot)
        
        # Calculate max allowable risk based on per-trade risk cap
        max_risk_dollars = self.sizing_config.account_equity * self.sizing_config.max_risk_per_trade_pct / 100
        max_width_for_risk = int(max_risk_dollars / 100)  # 100 = multiplier
        
        # Try widths from configured down to 1
        widths_to_try = list(range(
            min(self.builder_config.preferred_width_points, max_width_for_risk),
            self.builder_config.min_width_points - 1,
            -1
        ))
        
        if not widths_to_try:
            widths_to_try = [self.builder_config.min_width_points]
        
        # Collect all attempts for diagnostics
        attempts: list[StructureAttempt] = []
        
        # Log the search parameters
        self.logger.info('structure_search_started',
            symbol=option_chain.symbol,
            spot=round(spot, 2),
            atm_strike=atm_strike,
            widths_to_try=widths_to_try,
            max_risk_dollars=round(max_risk_dollars, 2),
            edge_type=edge.edge_type.value)
        
        for width in widths_to_try:
            structure, struct_type, failure_details = self._build_structure_with_full_diagnostics(
                edge, regime, option_chain, atm_strike, width
            )
            
            if structure is None:
                # Record the attempt
                attempt = StructureAttempt(
                    structure_type=struct_type,
                    width_points=width,
                    expiration_dte=failure_details.get('expiration_dte'),
                    short_strike=failure_details.get('short_strike'),
                    long_strike=failure_details.get('long_strike'),
                    failure_reason=failure_details.get('failure_reason', 'UNKNOWN'),
                    min_oi_found=failure_details.get('min_oi_found'),
                    min_volume_found=failure_details.get('min_volume_found'),
                    max_bid_ask_pct=failure_details.get('max_bid_ask_pct'),
                    conservative_credit=failure_details.get('conservative_credit'),
                    max_loss_dollars=failure_details.get('max_loss_dollars'),
                    risk_cap_dollars=round(max_risk_dollars, 2),
                )
                attempts.append(attempt)
                
                self.logger.info('structure_build_failed', 
                    symbol=option_chain.symbol, 
                    width=width,
                    struct_type=struct_type,
                    reason=failure_details.get('failure_reason', 'UNKNOWN'),
                    details=failure_details)
                continue
            
            # Quick validation
            validation = validate_structure(structure, self.sizing_config.account_equity)
            if not validation.is_valid:
                error_msgs = [str(e) for e in validation.errors[:3]]
                max_loss_dollars = structure.max_loss * 100 if structure.max_loss else 0
                
                attempt = StructureAttempt(
                    structure_type=structure.structure_type.value,
                    width_points=width,
                    expiration_dte=failure_details.get('expiration_dte'),
                    failure_reason='VALIDATION_FAIL',
                    max_loss_dollars=round(max_loss_dollars, 2),
                    risk_cap_dollars=round(max_risk_dollars, 2),
                )
                attempts.append(attempt)
                
                self.logger.info('structure_validation_failed',
                    symbol=option_chain.symbol, 
                    width=width, 
                    max_loss_dollars=round(max_loss_dollars, 2),
                    errors=error_msgs)
                continue
            
            # Quick sizing check
            sizing = calculate_size(
                structure,
                self.sizing_config,
                self.portfolio.get_total_max_loss(),
                self.portfolio.position_count,
            )
            
            if sizing.allowed:
                self.logger.info('structure_found',
                    symbol=option_chain.symbol,
                    width=width,
                    max_loss_dollars=round(structure.max_loss * 100, 2) if structure.max_loss else 0,
                    contracts=sizing.recommended_contracts)
                return structure, validation.warnings, []  # Success, no diagnostics needed
            else:
                max_loss_dollars = structure.max_loss * 100 if structure.max_loss else 0
                
                attempt = StructureAttempt(
                    structure_type=structure.structure_type.value,
                    width_points=width,
                    expiration_dte=failure_details.get('expiration_dte'),
                    failure_reason='SIZING_REJECT',
                    max_loss_dollars=round(max_loss_dollars, 2),
                    risk_cap_dollars=round(max_risk_dollars, 2),
                )
                attempts.append(attempt)
                
                self.logger.info('structure_sizing_failed',
                    symbol=option_chain.symbol, 
                    width=width,
                    max_loss_dollars=round(max_loss_dollars, 2),
                    risk_cap_dollars=round(max_risk_dollars, 2),
                    reason=sizing.rejection_reason or 'sizing_not_allowed')
        
        self.logger.info('no_valid_structure_found',
            symbol=option_chain.symbol, 
            widths_tried=widths_to_try,
            num_attempts=len(attempts),
            max_risk_cap_dollars=round(max_risk_dollars, 2))
        
        return None, [], attempts
    
    def _build_structure_with_full_diagnostics(
        self,
        edge: EdgeSignal,
        regime: RegimeClassification,
        option_chain: OptionChain,
        atm_strike: float,
        width_points: int,
    ) -> tuple[Optional[OptionStructure], str, dict]:
        """
        Build a structure with comprehensive failure diagnostics.
        
        Returns:
            Tuple of (structure, struct_type, details_dict)
            - structure: OptionStructure or None
            - struct_type: string name of structure type attempted
            - details_dict: failure diagnostics including expiration_dte, strikes, OI, credit, etc.
        """
        from datetime import date
        from structures.builders import find_best_expiration, find_contract
        
        details = {}
        
        # First check: valid expiration
        expiration = find_best_expiration(option_chain, self._run_date, self.builder_config)
        if expiration is None:
            dte_range = f"{self.builder_config.min_dte}-{self.builder_config.max_dte}"
            return None, "unknown", {'failure_reason': f'NO_EXPIRY({dte_range})'}
        
        # Compute DTE for diagnostics
        today = self._run_date if self._run_date else date.today()
        dte = (expiration - today).days
        details['expiration_dte'] = dte
        
        # Determine structure type based on edge
        struct_type = "unknown"
        structure = None
        
        if edge.edge_type == EdgeType.VOLATILITY_RISK_PREMIUM:
            if regime.regime in [RegimeState.LOW_VOL_GRIND, RegimeState.CHOP]:
                struct_type = "iron_condor"
                structure = build_iron_condor(
                    option_chain,
                    put_short_strike=atm_strike - width_points * 2,
                    call_short_strike=atm_strike + width_points * 2,
                    wing_width_points=width_points,
                    as_of_date=self._run_date,
                    config=self.builder_config,
                )
                details['short_strike'] = atm_strike - width_points * 2  # put short
            else:
                struct_type = "put_credit_spread"
                structure = build_credit_spread(
                    option_chain,
                    OptionType.PUT,
                    atm_strike - width_points,
                    width_points=width_points,
                    as_of_date=self._run_date,
                    config=self.builder_config,
                )
                details['short_strike'] = atm_strike - width_points
                details['long_strike'] = atm_strike - width_points * 2
        
        elif edge.edge_type == EdgeType.GAMMA_PRESSURE:
            struct_type = "butterfly"
            pin_strike = edge.metrics.get('max_gamma_strike', atm_strike)
            structure = build_butterfly(
                option_chain,
                center_strike=pin_strike,
                wing_width_points=width_points,
                as_of_date=self._run_date,
                config=self.builder_config,
            )
            details['short_strike'] = pin_strike
        
        elif edge.edge_type == EdgeType.TERM_STRUCTURE:
            struct_type = "calendar"
            structure = build_calendar(
                option_chain,
                strike=atm_strike,
                as_of_date=self._run_date,
                config=self.builder_config,
            )
            details['short_strike'] = atm_strike
        
        elif edge.edge_type == EdgeType.SKEW_EXTREME:
            if edge.metrics.get('is_steep', 0) == 1.0:
                struct_type = "put_credit_spread"
                structure = build_credit_spread(
                    option_chain,
                    OptionType.PUT,
                    atm_strike - width_points,
                    width_points=width_points,
                    as_of_date=self._run_date,
                    config=self.builder_config,
                )
                details['short_strike'] = atm_strike - width_points
                details['long_strike'] = atm_strike - width_points * 2
            else:
                details['failure_reason'] = 'SKEW_NOT_STEEP'
                return None, "put_credit_spread", details
        
        elif edge.edge_type == EdgeType.EVENT_VOL:
            struct_type = "iron_condor"
            structure = build_iron_condor(
                option_chain,
                put_short_strike=atm_strike - width_points,
                call_short_strike=atm_strike + width_points,
                wing_width_points=width_points,
                as_of_date=self._run_date,
                config=self.builder_config,
            )
            details['short_strike'] = atm_strike - width_points
        
        else:
            details['failure_reason'] = f'UNSUPPORTED_EDGE({edge.edge_type.value})'
            return None, struct_type, details
        
        # If structure built successfully, return it
        if structure is not None:
            return structure, struct_type, details
        
        # Structure is None - diagnose why
        short_strike = details.get('short_strike', atm_strike - width_points)
        long_strike = details.get('long_strike', short_strike - width_points)
        
        # Try to find contracts for diagnosis
        short_c = option_chain.get_contract(expiration, short_strike, OptionType.PUT)
        long_c = option_chain.get_contract(expiration, long_strike, OptionType.PUT)
        
        if short_c is None:
            details['failure_reason'] = f'SHORT_STRIKE_NOT_FOUND({short_strike})'
            return None, struct_type, details
        
        if long_c is None:
            details['failure_reason'] = f'LONG_STRIKE_NOT_FOUND({long_strike})'
            return None, struct_type, details
        
        # Collect diagnostic metrics
        details['min_oi_found'] = min(short_c.open_interest or 0, long_c.open_interest or 0)
        details['min_volume_found'] = min(short_c.volume or 0, long_c.volume or 0)
        
        # Check bid/ask spread
        if short_c.mid and short_c.mid > 0:
            short_spread_pct = ((short_c.ask - short_c.bid) / short_c.mid) * 100 if short_c.ask and short_c.bid else 0
        else:
            short_spread_pct = 999
        if long_c.mid and long_c.mid > 0:
            long_spread_pct = ((long_c.ask - long_c.bid) / long_c.mid) * 100 if long_c.ask and long_c.bid else 0
        else:
            long_spread_pct = 999
        details['max_bid_ask_pct'] = round(max(short_spread_pct, long_spread_pct), 1)
        
        # Check quote validity
        if short_c.bid <= 0:
            details['failure_reason'] = f'INVALID_QUOTE(short_bid=0)'
            return None, struct_type, details
        if long_c.ask <= 0:
            details['failure_reason'] = f'INVALID_QUOTE(long_ask=0)'
            return None, struct_type, details
        
        # Check credit
        credit = short_c.bid - long_c.ask
        details['conservative_credit'] = round(credit, 3)
        if credit <= 0:
            details['failure_reason'] = f'CREDIT_NONPOSITIVE({credit:.3f})'
            return None, struct_type, details
        
        # Check liquidity
        min_oi = self.builder_config.min_open_interest
        if short_c.open_interest < min_oi:
            details['failure_reason'] = f'LIQUIDITY_FAIL(short_oi={short_c.open_interest},min={min_oi})'
            return None, struct_type, details
        if long_c.open_interest < min_oi:
            details['failure_reason'] = f'LIQUIDITY_FAIL(long_oi={long_c.open_interest},min={min_oi})'
            return None, struct_type, details
        
        # Unknown failure
        details['failure_reason'] = f'BUILDER_FAILED({struct_type})'
        return None, struct_type, details
    
    def _build_structure_for_edge(
        self,
        edge: EdgeSignal,
        regime: RegimeClassification,
        option_chain: OptionChain,
        atm_strike: float,
        width_points: int,
    ):
        """Build a specific structure type based on edge (legacy wrapper)."""
        structure, _, _ = self._build_structure_with_full_diagnostics(
            edge, regime, option_chain, atm_strike, width_points
        )
        return structure
    
    def _create_pass_candidate(
        self,
        symbol: str,
        edge: EdgeSignal,
        regime: RegimeClassification,
        reason: str,
        diagnostics: list = None,
    ) -> TradeCandidate:
        """Create a PASS candidate when structure/validation/sizing fails."""
        from data.schemas import OptionStructure, StructureType, StructureAttempt
        from risk.sizing import SizingResult
        
        # Create minimal placeholder structure with safe defaults (not a real trade)
        # is_placeholder=True bypasses validation requirements
        # Use 0.0 instead of None to avoid formatting crashes
        dummy_structure = OptionStructure(
            structure_type=StructureType.CREDIT_SPREAD,
            symbol=symbol,
            legs=[],
            entry_credit=0.0,
            entry_debit=0.0,
            max_loss=0.0,
            max_profit=0.0,
            is_placeholder=True,
        )
        
        dummy_sizing = SizingResult(
            recommended_contracts=0,
            risk_per_contract_dollars=0,
            total_risk_dollars=0,
            risk_pct_of_equity=0,
            allowed=False,
            rejection_reason=reason,
        )
        
        candidate = create_trade_candidate(
            symbol=symbol,
            structure=dummy_structure,
            edge=edge,
            regime=regime,
            sizing=dummy_sizing,
            validation_messages=[reason],
            include_explanations=False,  # No explanations for PASS
        )
        
        # Add diagnostics if provided
        if diagnostics:
            candidate.pass_diagnostics = diagnostics
        
        return candidate
    
    def export_report(
        self,
        report: DailyReport,
        output_dir: Optional[str] = None
    ) -> list[str]:
        """Export report to files."""
        if output_dir is None:
            output_dir = self.config.get('reporting', {}).get('output_directory', './logs/reports')
        
        formats = self.config.get('reporting', {}).get('format', ['markdown', 'html'])
        
        return save_report(report, output_dir, formats)
