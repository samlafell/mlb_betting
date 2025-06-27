"""
Strategy Orchestrator Service - Phase 3C: The Missing Bridge

This service solves the critical architectural disconnect between:
- Phase 3A (Real-time Detection): MasterBettingDetector with hardcoded logic
- Phase 3B (Historical Backtesting): BacktestingService with dynamic processors

The orchestrator creates the missing feedback loop by:
1. Loading BacktestResult objects from Phase 3B
2. Generating dynamic configuration for Phase 3A
3. Ensuring the same strategy logic runs in both phases
4. Eliminating the vacuum where backtesting results never inform live detection
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import structlog

from ..core.logging import get_logger
from ..db.connection import get_db_manager
from ..services.database_coordinator import get_database_coordinator
from ..analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from ..services.betting_signal_repository import BettingSignalRepository
from ..models.betting_analysis import BettingSignal, SignalType


class StrategyStatus(Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"
    PROBATION = "probation"  # New: Strategy underperforming but not disabled yet
    QUARANTINE = "quarantine"  # New: Strategy disabled due to poor performance


class PerformanceTrend(Enum):
    IMPROVING = "IMPROVING"
    STABLE = "STABLE"
    DECLINING = "DECLINING"
    CRITICAL = "CRITICAL"  # New: Severe performance degradation


class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class UpdateTrigger(Enum):
    SCHEDULED = "scheduled"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    NEW_BACKTEST_RESULTS = "new_backtest_results"
    MANUAL_OVERRIDE = "manual_override"


@dataclass
class UpdatePolicy:
    """Configuration for when to update strategy configurations"""
    scheduled_interval_minutes: int = 15
    performance_degradation_threshold: float = -15.0  # ROI drops by 15%
    min_time_between_updates_minutes: int = 5  # Prevent update spam
    max_time_without_update_hours: int = 4  # Force update if too stale
    enable_performance_triggers: bool = True
    enable_new_backtest_triggers: bool = True


@dataclass
class LifecycleThresholds:
    """Thresholds for strategy lifecycle management"""
    # Automatic disable thresholds
    auto_disable_roi_threshold: float = -15.0  # Disable if ROI drops below -15%
    auto_disable_consecutive_losses: int = 8  # Disable after 8 consecutive losses
    auto_disable_win_rate_threshold: float = 0.35  # Disable if win rate < 35%
    
    # Probation thresholds (warning stage)
    probation_roi_threshold: float = -5.0  # Put on probation if ROI < -5%
    probation_win_rate_threshold: float = 0.45  # Put on probation if win rate < 45%
    
    # Confidence reduction parameters
    min_confidence_multiplier: float = 0.5  # Never reduce below 50%
    confidence_reduction_rate: float = 0.1  # Reduce by 10% per poor performance period
    
    # Sample size requirements
    min_sample_size_for_decisions: int = 10  # Need 10+ bets before lifecycle decisions
    robust_sample_size: int = 50  # 50+ bets for robust decisions
    
    # Time-based considerations
    grace_period_days: int = 7  # Give new strategies 7 days before lifecycle management
    performance_window_days: int = 14  # Look at last 14 days for trend analysis


@dataclass
class StrategyAlert:
    """Alert for strategy performance issues"""
    strategy_name: str
    alert_level: AlertLevel
    message: str
    current_roi: float
    current_win_rate: float
    sample_size: int
    trend: PerformanceTrend
    recommended_action: str
    timestamp: datetime


@dataclass
class StrategyConfiguration:
    """Dynamic strategy configuration based on backtesting performance"""
    strategy_name: str
    signal_type: SignalType
    is_enabled: bool
    confidence_multiplier: float  # 0.8-1.2 based on performance
    threshold_adjustment: float   # Adjust thresholds based on performance
    weight_in_ensemble: float    # Weight for ensemble voting
    
    # Performance metrics from backtesting
    recent_win_rate: float
    recent_roi: float
    sample_size: int
    performance_trend: str
    
    # Dynamic parameters
    min_differential_threshold: float
    max_recommendations_per_day: int
    
    # Metadata
    last_performance_update: datetime
    strategy_status: StrategyStatus


@dataclass
class LiveStrategyState:
    """Current state of strategies for live detection"""
    enabled_strategies: List[StrategyConfiguration]
    disabled_strategies: List[StrategyConfiguration]
    performance_summary: Dict[str, Any]
    last_updated: datetime
    configuration_version: str
    update_trigger: Optional[str] = None


@dataclass
class DefaultStrategyConfig:
    """Default configuration for new/unknown strategies during cold start"""
    confidence_multiplier: float = 0.8  # Conservative start
    threshold_adjustment: float = 0.2  # Higher thresholds initially
    weight_in_ensemble: float = 0.3  # Lower ensemble weight
    max_recommendations_per_day: int = 1  # Very conservative
    grace_period_days: int = 14  # Extended grace period for new strategies
    min_differential_threshold: float = 12.0  # Conservative threshold


@dataclass
class ColdStartManager:
    """Manages cold start scenarios for strategies and fresh deployments"""
    fallback_strategies: List[str]
    default_config: DefaultStrategyConfig
    bootstrap_required: bool = False
    
    @classmethod
    def create_bootstrap_manager(cls) -> 'ColdStartManager':
        """Create cold start manager for fresh deployment"""
        return cls(
            fallback_strategies=[
                "sharp_action",
                "opposing_markets", 
                "book_conflicts"
            ],
            default_config=DefaultStrategyConfig(),
            bootstrap_required=True
        )


class StrategyOrchestrator:
    """
    Orchestrates strategy execution by bridging backtesting results with live detection.
    
    This service solves the critical architectural disconnect where Phase 3A uses
    hardcoded logic while Phase 3B produces results that never influence live detection.
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.db_manager = get_db_manager()
        self.coordinator = get_database_coordinator()
        self.processor_factory = None
        self.signal_repository = None
        
        # Enhanced update policy
        self.update_policy = UpdatePolicy()
        
        # NEW: Lifecycle management
        self.lifecycle_thresholds = LifecycleThresholds()
        self._strategy_alerts: List[StrategyAlert] = []
        self._lifecycle_history: Dict[str, List[Dict[str, Any]]] = {}  # Strategy -> lifecycle events
        
        # NEW: Cold start management
        self._cold_start_manager: Optional[ColdStartManager] = None
        self._new_strategy_registry: Dict[str, datetime] = {}  # Strategy -> first seen timestamp
        
        # Current strategy state
        self._current_state: Optional[LiveStrategyState] = None
        self._last_update: Optional[datetime] = None
        self._last_performance_check: Optional[datetime] = None
        self._last_backtest_check: Optional[datetime] = None
        
        # Performance monitoring
        self._performance_baseline: Dict[str, float] = {}  # Strategy -> ROI baseline
        self._update_triggers_fired: List[Dict[str, Any]] = []  # Audit trail
        
    async def initialize(self):
        """Initialize the orchestrator with cold start detection"""
        try:
            from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
            from mlb_sharp_betting.services.strategy_validator import StrategyValidator
            from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig
            
            # Create configuration first
            processor_config = SignalProcessorConfig()
            
            # Initialize dependencies with config
            self.signal_repository = BettingSignalRepository(processor_config)
            
            # Get profitable strategies and create validator
            from mlb_sharp_betting.models.betting_analysis import StrategyThresholds
            profitable_strategies = await self.signal_repository.get_profitable_strategies()
            strategy_thresholds = StrategyThresholds()
            strategy_validator = StrategyValidator(profitable_strategies, strategy_thresholds)
            
            # Initialize factory with required dependencies
            self.processor_factory = StrategyProcessorFactory(
                repository=self.signal_repository,
                validator=strategy_validator, 
                config=processor_config
            )
            
            # Check if this is a cold start scenario
            await self._detect_cold_start_scenario()
            
            await self._update_strategy_configuration(UpdateTrigger.MANUAL_OVERRIDE)
            
            self.logger.info("Strategy orchestrator initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize strategy orchestrator: {e}")
            raise
    
    async def _detect_cold_start_scenario(self):
        """Detect and handle cold start scenarios"""
        try:
            # Check if backtesting schema exists and has data
            backtest_results = await self._get_recent_backtest_results()
            
            if not backtest_results:
                self.logger.warning("No backtesting results found - activating cold start mode")
                self._cold_start_manager = ColdStartManager.create_bootstrap_manager()
                
                # Try to bootstrap with any available data
                await self._bootstrap_cold_start()
            else:
                self.logger.info(f"Found {len(backtest_results)} recent backtest results")
                
        except Exception as e:
            self.logger.error(f"Cold start detection failed: {e}")
            # Activate cold start mode as fallback
            self._cold_start_manager = ColdStartManager.create_bootstrap_manager()
    
    async def _bootstrap_cold_start(self):
        """Bootstrap configuration for cold start scenario"""
        try:
            self.logger.info("Bootstrapping cold start configuration")
            
            # Create minimal configurations for fallback strategies
            bootstrap_configurations = []
            
            for strategy_name in self._cold_start_manager.fallback_strategies:
                signal_type = self._map_strategy_to_signal_type(strategy_name)
                if signal_type:
                    config = StrategyConfiguration(
                        strategy_name=strategy_name,
                        signal_type=signal_type,
                        is_enabled=True,  # Enable fallback strategies
                        strategy_status=StrategyStatus.ACTIVE,
                        confidence_multiplier=self._cold_start_manager.default_config.confidence_multiplier,
                        threshold_adjustment=self._cold_start_manager.default_config.threshold_adjustment,
                        weight_in_ensemble=self._cold_start_manager.default_config.weight_in_ensemble,
                        recent_win_rate=0.52,  # Conservative assumption
                        recent_roi=3.0,  # Conservative assumption
                        sample_size=0,  # No historical data
                        performance_trend=PerformanceTrend.STABLE.value,
                        min_differential_threshold=self._cold_start_manager.default_config.min_differential_threshold,
                        max_recommendations_per_day=self._cold_start_manager.default_config.max_recommendations_per_day,
                        last_performance_update=datetime.now(timezone.utc)
                    )
                    bootstrap_configurations.append(config)
                    
                    # Register as new strategy
                    self._new_strategy_registry[strategy_name] = datetime.now(timezone.utc)
            
            # Create minimal strategy state
            self._current_state = LiveStrategyState(
                enabled_strategies=bootstrap_configurations,
                disabled_strategies=[],
                performance_summary={
                    "total_strategies": len(bootstrap_configurations),
                    "enabled_strategies": len(bootstrap_configurations),
                    "disabled_strategies": 0,
                    "cold_start_mode": True,
                    "bootstrap_timestamp": datetime.now(timezone.utc).isoformat()
                },
                last_updated=datetime.now(timezone.utc),
                configuration_version="cold_start_v1",
                update_trigger="COLD_START_BOOTSTRAP"
            )
            
            self.logger.info(f"Cold start bootstrap complete with {len(bootstrap_configurations)} strategies")
            
        except Exception as e:
            self.logger.error(f"Cold start bootstrap failed: {e}")
            raise
    
    async def register_new_strategy(self, strategy_name: str) -> bool:
        """Register a new strategy and create conservative configuration"""
        try:
            if strategy_name in self._new_strategy_registry:
                self.logger.debug(f"Strategy {strategy_name} already registered")
                return False
            
            # Register the strategy
            self._new_strategy_registry[strategy_name] = datetime.now(timezone.utc)
            
            # Check if we can create a configuration for it
            signal_type = self._map_strategy_to_signal_type(strategy_name)
            if not signal_type:
                self.logger.warning(f"Cannot create configuration for unknown strategy: {strategy_name}")
                return False
            
            # Create conservative new strategy configuration
            new_config = StrategyConfiguration(
                strategy_name=strategy_name,
                signal_type=signal_type,
                is_enabled=False,  # Start disabled until it proves itself
                strategy_status=StrategyStatus.PROBATION,  # Start on probation
                confidence_multiplier=DefaultStrategyConfig().confidence_multiplier,
                threshold_adjustment=DefaultStrategyConfig().threshold_adjustment,
                weight_in_ensemble=DefaultStrategyConfig().weight_in_ensemble,
                recent_win_rate=0.50,  # Neutral assumption
                recent_roi=0.0,  # Neutral assumption
                sample_size=0,  # No data yet
                performance_trend=PerformanceTrend.STABLE.value,
                min_differential_threshold=DefaultStrategyConfig().min_differential_threshold,
                max_recommendations_per_day=DefaultStrategyConfig().max_recommendations_per_day
            )
            
            # Add to current state if available
            if self._current_state:
                self._current_state.disabled_strategies.append(new_config)
            
            self.logger.info(f"Registered new strategy: {strategy_name} (probationary status)")
            
            # Generate alert for new strategy
            await self._generate_lifecycle_alert(
                strategy_name=strategy_name,
                win_rate=0.50,
                roi=0.0,
                total_bets=0,
                lifecycle_action="new_strategy_registered"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register new strategy {strategy_name}: {e}")
            return False
    
    async def _get_strategy_age_days(self, strategy_name: str) -> int:
        """Get age of strategy in days since first registration"""
        if strategy_name in self._new_strategy_registry:
            first_seen = self._new_strategy_registry[strategy_name]
            age_delta = datetime.now(timezone.utc) - first_seen
            return age_delta.days
        
        # Check database for historical first appearance
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT MIN(created_at) as first_seen
                    FROM backtesting.strategy_performance
                    WHERE strategy_name = %s
                """, (strategy_name,))
                
                result = cursor.fetchone()
                if result and result['first_seen']:
                    first_seen = result['first_seen']
                    if isinstance(first_seen, str):
                        first_seen = datetime.fromisoformat(first_seen.replace('Z', '+00:00'))
                    
                    age_delta = datetime.now(timezone.utc) - first_seen
                    return age_delta.days
                
        except Exception as e:
            self.logger.debug(f"Could not determine age for {strategy_name}: {e}")
        
        # Default to mature strategy if we can't determine age
        return 30
    
    def is_cold_start_active(self) -> bool:
        """Check if system is in cold start mode"""
        return self._cold_start_manager is not None and self._cold_start_manager.bootstrap_required
    
    def get_new_strategies(self) -> Dict[str, datetime]:
        """Get list of strategies registered since startup"""
        return self._new_strategy_registry.copy()
    
    async def promote_strategy_from_probation(self, strategy_name: str) -> bool:
        """Promote a strategy from probation to active status"""
        try:
            # Find the strategy in current state
            if not self._current_state:
                return False
            
            # Look for strategy in disabled strategies
            for i, config in enumerate(self._current_state.disabled_strategies):
                if config.strategy_name == strategy_name and config.strategy_status == StrategyStatus.PROBATION:
                    # Move to enabled strategies with updated status
                    config.strategy_status = StrategyStatus.ACTIVE
                    config.is_enabled = True
                    config.confidence_multiplier = min(1.0, config.confidence_multiplier + 0.2)  # Boost confidence
                    
                    # Move from disabled to enabled
                    self._current_state.disabled_strategies.pop(i)
                    self._current_state.enabled_strategies.append(config)
                    
                    # Record lifecycle event
                    await self._record_lifecycle_event(strategy_name, "PROMOTED", "probation_to_active")
                    
                    self.logger.info(f"Promoted strategy from probation: {strategy_name}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to promote strategy {strategy_name}: {e}")
            return False
    
    async def get_live_strategy_configuration(self, force_refresh: bool = False) -> LiveStrategyState:
        """
        Get current live strategy configuration with intelligent update triggers.
        """
        now = datetime.now(timezone.utc)
        
        # Check multiple update triggers
        should_update = await self._should_update_configuration(now, force_refresh)
        
        if should_update['should_update']:
            await self._update_strategy_configuration(should_update['trigger'])
        
        return self._current_state
    
    async def _should_update_configuration(self, now: datetime, force_refresh: bool) -> Dict[str, Any]:
        """Determine if configuration should be updated and why"""
        
        if force_refresh:
            return {'should_update': True, 'trigger': UpdateTrigger.MANUAL_OVERRIDE}
        
        if self._current_state is None or self._last_update is None:
            return {'should_update': True, 'trigger': UpdateTrigger.SCHEDULED}
        
        # Check scheduled interval
        minutes_since_update = (now - self._last_update).total_seconds() / 60
        if minutes_since_update >= self.update_policy.scheduled_interval_minutes:
            return {'should_update': True, 'trigger': UpdateTrigger.SCHEDULED}
        
        # Check maximum staleness
        hours_since_update = minutes_since_update / 60
        if hours_since_update >= self.update_policy.max_time_without_update_hours:
            return {'should_update': True, 'trigger': UpdateTrigger.SCHEDULED}
        
        # Check performance degradation trigger
        if (self.update_policy.enable_performance_triggers and 
            minutes_since_update >= self.update_policy.min_time_between_updates_minutes):
            
            performance_degradation = await self._check_performance_degradation()
            if performance_degradation:
                return {'should_update': True, 'trigger': UpdateTrigger.PERFORMANCE_DEGRADATION}
        
        # Check for new backtest results
        if (self.update_policy.enable_new_backtest_triggers and 
            minutes_since_update >= self.update_policy.min_time_between_updates_minutes):
            
            new_backtest_results = await self._check_new_backtest_results()
            if new_backtest_results:
                return {'should_update': True, 'trigger': UpdateTrigger.NEW_BACKTEST_RESULTS}
        
        return {'should_update': False, 'trigger': None}
    
    async def _check_performance_degradation(self) -> bool:
        """Check if any strategy has degraded significantly since last baseline"""
        try:
            # Get current performance metrics
            current_performance = await self._get_current_strategy_performance()
            
            for strategy_name, current_roi in current_performance.items():
                baseline_roi = self._performance_baseline.get(strategy_name)
                
                if baseline_roi is not None:
                    roi_change = current_roi - baseline_roi
                    
                    if roi_change <= self.update_policy.performance_degradation_threshold:
                        self.logger.warning(
                            f"Performance degradation detected: {strategy_name}",
                            current_roi=current_roi,
                            baseline_roi=baseline_roi,
                            change=roi_change
                        )
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to check performance degradation: {e}")
            return False
    
    async def _check_new_backtest_results(self) -> bool:
        """Check if new backtest results are available since last check"""
        try:
            if self._last_backtest_check is None:
                return True
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as new_results
                    FROM backtesting.strategy_performance
                    WHERE created_at > %s
                """, (self._last_backtest_check,))
                
                result = cursor.fetchone()
                return result['new_results'] > 0
                
        except Exception as e:
            self.logger.error(f"Failed to check new backtest results: {e}")
            return False
    
    async def _get_recent_backtest_results(self) -> List[Dict[str, Any]]:
        """Get recent backtest results for strategy configuration"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        strategy_name,
                        roi_per_100,
                        win_rate,
                        total_bets,
                        confidence_level,
                        source_book_type,
                        split_type,
                        created_at
                    FROM backtesting.strategy_performance
                    WHERE created_at >= NOW() - INTERVAL '30 days'
                      AND total_bets >= 5
                    ORDER BY created_at DESC
                """)
                
                results = cursor.fetchall()
                return [dict(row) for row in results] if results else []
                
        except Exception as e:
            self.logger.warning(f"Failed to get recent backtest results: {e}")
            # Return empty list to allow cold start mode
            return []
    
    async def _create_strategy_configurations(self, backtest_results: List[Dict[str, Any]]) -> List[StrategyConfiguration]:
        """Create strategy configurations from backtest results"""
        configurations = []
        
        if not backtest_results:
            # If no backtest results, return cold start configurations
            if self._cold_start_manager:
                for strategy_name in self._cold_start_manager.fallback_strategies:
                    signal_type = self._map_strategy_to_signal_type(strategy_name)
                    if signal_type:
                        config = StrategyConfiguration(
                            strategy_name=strategy_name,
                            signal_type=signal_type,
                            is_enabled=True,
                            strategy_status=StrategyStatus.ACTIVE,
                            confidence_multiplier=self._cold_start_manager.default_config.confidence_multiplier,
                            threshold_adjustment=self._cold_start_manager.default_config.threshold_adjustment,
                            weight_in_ensemble=self._cold_start_manager.default_config.weight_in_ensemble,
                            recent_win_rate=0.52,
                            recent_roi=3.0,
                            sample_size=0,
                            performance_trend=PerformanceTrend.STABLE.value,
                            min_differential_threshold=self._cold_start_manager.default_config.min_differential_threshold,
                            max_recommendations_per_day=self._cold_start_manager.default_config.max_recommendations_per_day,
                            last_performance_update=datetime.now(timezone.utc)
                        )
                        configurations.append(config)
            return configurations
        
        # Process actual backtest results
        for result in backtest_results:
            strategy_name = result['strategy_name']
            signal_type = self._map_strategy_to_signal_type(strategy_name)
            
            if not signal_type:
                continue
            
            # Extract performance metrics
            roi = result.get('roi_per_100', 0.0)
            win_rate = result.get('win_rate', 0.0)
            total_bets = result.get('total_bets', 0)
            
            # Determine if strategy should be enabled based on performance
            is_enabled = (
                roi > self.lifecycle_thresholds.probation_roi_threshold and
                win_rate > self.lifecycle_thresholds.probation_win_rate_threshold and
                total_bets >= self.lifecycle_thresholds.min_sample_size_for_decisions
            )
            
            # Determine status
            if roi < self.lifecycle_thresholds.auto_disable_roi_threshold:
                status = StrategyStatus.QUARANTINE
                is_enabled = False
            elif roi < self.lifecycle_thresholds.probation_roi_threshold:
                status = StrategyStatus.PROBATION
                is_enabled = False
            else:
                status = StrategyStatus.ACTIVE
            
            # Calculate confidence multiplier based on performance
            if roi > 15.0:
                confidence_multiplier = 1.2
            elif roi > 10.0:
                confidence_multiplier = 1.1
            elif roi > 5.0:
                confidence_multiplier = 1.0
            elif roi > 0.0:
                confidence_multiplier = 0.9
            else:
                confidence_multiplier = 0.8
            
            # Calculate performance trend (simplified)
            if roi > 10.0:
                trend = PerformanceTrend.IMPROVING.value
            elif roi < 0.0:
                trend = PerformanceTrend.DECLINING.value
            else:
                trend = PerformanceTrend.STABLE.value
            
            config = StrategyConfiguration(
                strategy_name=strategy_name,
                signal_type=signal_type,
                is_enabled=is_enabled,
                strategy_status=status,
                confidence_multiplier=confidence_multiplier,
                threshold_adjustment=0.0 if roi > 10.0 else 0.1 if roi > 5.0 else 0.2,
                weight_in_ensemble=1.0 if roi > 15.0 else 0.8 if roi > 10.0 else 0.5,
                recent_win_rate=win_rate,
                recent_roi=roi,
                sample_size=total_bets,
                performance_trend=trend,
                min_differential_threshold=15.0 if roi > 15.0 else 20.0 if roi > 10.0 else 25.0,
                max_recommendations_per_day=5 if roi > 15.0 else 3 if roi > 10.0 else 1,
                last_performance_update=datetime.now(timezone.utc)
            )
            configurations.append(config)
        
        # Sort configurations by ROI descending (best performers first)
        configurations.sort(key=lambda c: c.recent_roi, reverse=True)
        
        return configurations
    
    async def _get_current_strategy_performance(self) -> Dict[str, float]:
        """Get current strategy performance baseline"""
        # For now, return empty dict - this would be populated from live tracking
        return {}
    
    def _generate_performance_summary(self, configurations: List[StrategyConfiguration]) -> Dict[str, Any]:
        """Generate performance summary from configurations"""
        enabled = [c for c in configurations if c.is_enabled]
        disabled = [c for c in configurations if not c.is_enabled]
        
        total_sample_size = sum(c.sample_size for c in configurations)
        avg_roi_enabled = sum(c.recent_roi for c in enabled) / len(enabled) if enabled else 0.0
        avg_win_rate_enabled = sum(c.recent_win_rate for c in enabled) / len(enabled) if enabled else 0.0
        
        trends = {}
        for trend in PerformanceTrend:
            trends[trend.value] = len([c for c in configurations if c.performance_trend == trend.value])
        
        return {
            'total_strategies': len(configurations),
            'enabled_strategies': len(enabled),
            'disabled_strategies': len(disabled),
            'avg_roi_enabled': avg_roi_enabled,
            'avg_win_rate_enabled': avg_win_rate_enabled,
            'total_sample_size': total_sample_size,
            'performance_trends': trends
        }
    
    def _map_strategy_to_signal_type(self, strategy_name: str) -> Optional[SignalType]:
        """Map strategy name to signal type"""
        strategy_lower = strategy_name.lower()
        
        if "sharp_action" in strategy_lower:
            return SignalType.SHARP_ACTION
        elif "opposing_markets" in strategy_lower:
            return SignalType.OPPOSING_MARKETS
        elif "book_conflicts" in strategy_lower:
            return SignalType.BOOK_CONFLICTS
        elif "public_fade" in strategy_lower or "public_money_fade" in strategy_lower:
            return SignalType.PUBLIC_FADE
        elif "late_flip" in strategy_lower or "late_sharp_flip" in strategy_lower:
            return SignalType.LATE_FLIP
        elif "consensus_moneyline" in strategy_lower:
            return SignalType.CONSENSUS_MONEYLINE
        elif "underdog" in strategy_lower:
            return SignalType.UNDERDOG_VALUE
        elif "line_movement" in strategy_lower:
            return SignalType.LINE_MOVEMENT
        elif "steam" in strategy_lower:
            return SignalType.STEAM_MOVE
        elif "timing" in strategy_lower:
            return SignalType.TIMING_BASED
        elif "total" in strategy_lower:
            return SignalType.TOTAL_SHARP
        else:
            # Default fallback
            return SignalType.SHARP_ACTION
    
    async def _update_strategy_configuration(self, trigger: UpdateTrigger):
        """Update strategy configuration and log the trigger"""
        update_start_time = datetime.now(timezone.utc)
        previous_config_version = self._current_state.configuration_version if self._current_state else None
        
        try:
            self.logger.info(f"Updating strategy configuration", trigger=trigger.value)
            
            # Record the trigger
            self._update_triggers_fired.append({
                'timestamp': datetime.now(timezone.utc),
                'trigger': trigger.value,
                'previous_update': self._last_update
            })
            
            # Keep only last 100 triggers
            if len(self._update_triggers_fired) > 100:
                self._update_triggers_fired = self._update_triggers_fired[-100:]
            
            # Get recent backtest results
            backtest_results = await self._get_recent_backtest_results()
            
            if not backtest_results:
                self.logger.warning("No recent backtest results found")
                return
            
            # Create strategy configurations
            configurations = await self._create_strategy_configurations(backtest_results)
            
            # Update performance baseline
            current_performance = await self._get_current_strategy_performance()
            self._performance_baseline.update(current_performance)
            
            # Generate new configuration version
            new_config_version = f"v{int(datetime.now(timezone.utc).timestamp())}"
            
            # Create new strategy state with ROI-sorted strategies
            enabled_strategies = [c for c in configurations if c.is_enabled]
            disabled_strategies = [c for c in configurations if not c.is_enabled]
            enabled_strategies.sort(key=lambda c: c.recent_roi, reverse=True)
            disabled_strategies.sort(key=lambda c: c.recent_roi, reverse=True)
            
            self._current_state = LiveStrategyState(
                enabled_strategies=enabled_strategies,
                disabled_strategies=disabled_strategies,
                performance_summary=self._generate_performance_summary(configurations),
                last_updated=datetime.now(timezone.utc),
                configuration_version=new_config_version,
                update_trigger=trigger.value
            )
            
            # Persist configuration snapshot
            await self._persist_configuration_snapshot(configurations, new_config_version, trigger)
            
            # Record update trigger in database
            update_duration_ms = int((datetime.now(timezone.utc) - update_start_time).total_seconds() * 1000)
            await self._record_update_trigger(
                trigger, 
                previous_config_version, 
                new_config_version, 
                len(configurations),
                update_duration_ms
            )
            
            self._last_update = datetime.now(timezone.utc)
            self._last_backtest_check = datetime.now(timezone.utc)
            
            self.logger.info(
                "Strategy configuration updated successfully",
                enabled_strategies=len(self._current_state.enabled_strategies),
                disabled_strategies=len(self._current_state.disabled_strategies),
                trigger=trigger.value,
                configuration_version=new_config_version,
                update_duration_ms=update_duration_ms
            )
            
        except Exception as e:
            self.logger.error(f"Failed to update strategy configuration: {e}")
            raise
    
    async def _persist_configuration_snapshot(
        self, 
        configurations: List[StrategyConfiguration], 
        config_version: str, 
        trigger: UpdateTrigger
    ):
        """Persist configuration snapshot for versioning and debugging"""
        try:
            with self.db_manager.get_cursor() as cursor:
                for config in configurations:
                    cursor.execute("""
                        INSERT INTO backtesting.strategy_config_history (
                            configuration_version,
                            strategy_name,
                            signal_type,
                            is_enabled,
                            strategy_status,
                            confidence_multiplier,
                            threshold_adjustment,
                            weight_in_ensemble,
                            recent_win_rate,
                            recent_roi,
                            sample_size,
                            performance_trend,
                            min_differential_threshold,
                            max_recommendations_per_day,
                            update_trigger
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        config_version,
                        config.strategy_name,
                        config.signal_type.value,
                        config.is_enabled,
                        config.strategy_status.value,
                        config.confidence_multiplier,
                        config.threshold_adjustment,
                        config.weight_in_ensemble,
                        config.recent_win_rate,
                        config.recent_roi,
                        config.sample_size,
                        config.performance_trend,
                        config.min_differential_threshold,
                        config.max_recommendations_per_day,
                        trigger.value
                    ))
                
                self.logger.debug(f"Persisted {len(configurations)} configuration snapshots for version {config_version}")
                
        except Exception as e:
            self.logger.error(f"Failed to persist configuration snapshot: {e}")
            # Don't raise - configuration should still work even if persistence fails
    
    async def _record_lifecycle_event(
        self, 
        strategy_name: str, 
        event_type: str, 
        event_reason: str,
        previous_status: Optional[str] = None,
        new_status: Optional[str] = None
    ):
        """Record strategy lifecycle event for auditing"""
        try:
            # Add to in-memory history
            if strategy_name not in self._lifecycle_history:
                self._lifecycle_history[strategy_name] = []
            
            event = {
                'timestamp': datetime.now(timezone.utc),
                'event_type': event_type,
                'event_reason': event_reason,
                'previous_status': previous_status,
                'new_status': new_status
            }
            
            self._lifecycle_history[strategy_name].append(event)
            
            # Keep only last 50 events per strategy
            if len(self._lifecycle_history[strategy_name]) > 50:
                self._lifecycle_history[strategy_name] = self._lifecycle_history[strategy_name][-50:]
            
            # Persist to database
            with self.db_manager.get_cursor() as cursor:
                # Get current performance metrics for the strategy
                current_roi = None
                current_win_rate = None
                current_sample_size = None
                
                if self._current_state:
                    for config in self._current_state.enabled_strategies + self._current_state.disabled_strategies:
                        if config.strategy_name == strategy_name:
                            current_roi = config.recent_roi
                            current_win_rate = config.recent_win_rate
                            current_sample_size = config.sample_size
                            break
                
                cursor.execute("""
                    INSERT INTO backtesting.strategy_lifecycle_events (
                        strategy_name,
                        event_type,
                        event_reason,
                        previous_status,
                        new_status,
                        roi_at_event,
                        win_rate_at_event,
                        sample_size_at_event,
                        configuration_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    strategy_name,
                    event_type,
                    event_reason,
                    previous_status,
                    new_status,
                    current_roi,
                    current_win_rate,
                    current_sample_size,
                    self._current_state.configuration_version if self._current_state else None
                ))
                
        except Exception as e:
            self.logger.error(f"Failed to record lifecycle event for {strategy_name}: {e}")
            # Don't raise - lifecycle tracking failure shouldn't stop the system
    
    async def _record_update_trigger(
        self, 
        trigger: UpdateTrigger, 
        previous_version: Optional[str], 
        new_version: str, 
        strategies_affected: int,
        update_duration_ms: int
    ):
        """Record update trigger for debugging update frequency"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO backtesting.orchestrator_update_triggers (
                        trigger_type,
                        trigger_reason,
                        previous_configuration_version,
                        new_configuration_version,
                        strategies_affected,
                        update_duration_ms
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    trigger.value,
                    f"Automatic update triggered by {trigger.value}",
                    previous_version,
                    new_version,
                    strategies_affected,
                    update_duration_ms
                ))
                
        except Exception as e:
            self.logger.error(f"Failed to record update trigger: {e}")
            # Don't raise - trigger recording failure shouldn't stop the system
    
    async def _persist_strategy_alert(self, alert: StrategyAlert):
        """Persist strategy alert to database"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO backtesting.strategy_alerts (
                        strategy_name,
                        alert_level,
                        message,
                        current_roi,
                        current_win_rate,
                        sample_size,
                        performance_trend,
                        recommended_action,
                        configuration_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    alert.strategy_name,
                    alert.alert_level.value,
                    alert.message,
                    alert.current_roi,
                    alert.current_win_rate,
                    alert.sample_size,
                    alert.trend.value,
                    alert.recommended_action,
                    self._current_state.configuration_version if self._current_state else None
                ))
                
        except Exception as e:
            self.logger.error(f"Failed to persist strategy alert: {e}")
            # Don't raise - alert persistence failure shouldn't stop the system
    
    async def _generate_lifecycle_alert(
        self, 
        strategy_name: str, 
        win_rate: float, 
        roi: float, 
        total_bets: int, 
        lifecycle_action: str
    ):
        """Generate alert for strategy lifecycle events"""
        
        # Determine alert level
        if "auto_disabled" in lifecycle_action.lower() or "quarantine" in lifecycle_action.lower():
            alert_level = AlertLevel.CRITICAL
            recommended_action = "Review strategy logic and consider disabling permanently"
        elif "probation" in lifecycle_action.lower():
            alert_level = AlertLevel.WARNING
            recommended_action = "Monitor closely and consider parameter adjustments"
        else:
            alert_level = AlertLevel.INFO
            recommended_action = "Continue monitoring"
        
        # Determine trend
        if roi < -10.0:
            trend = PerformanceTrend.CRITICAL
        elif roi < 0.0:
            trend = PerformanceTrend.DECLINING
        else:
            trend = PerformanceTrend.STABLE
        
        alert = StrategyAlert(
            strategy_name=strategy_name,
            alert_level=alert_level,
            message=f"Strategy lifecycle action: {lifecycle_action}",
            current_roi=roi,
            current_win_rate=win_rate,
            sample_size=total_bets,
            trend=trend,
            recommended_action=recommended_action,
            timestamp=datetime.now(timezone.utc)
        )
        
        self._strategy_alerts.append(alert)
        
        # Keep only last 100 alerts
        if len(self._strategy_alerts) > 100:
            self._strategy_alerts = self._strategy_alerts[-100:]
        
        # Persist to database
        await self._persist_strategy_alert(alert)
        
        # Log the alert
        self.logger.warning(
            f"Strategy lifecycle alert: {strategy_name}",
            action=lifecycle_action,
            roi=roi,
            win_rate=win_rate,
            sample_size=total_bets,
            alert_level=alert_level.value
        )
    
    def get_configuration_history(self, strategy_name: Optional[str] = None, days: int = 7) -> List[Dict[str, Any]]:
        """Get configuration history for debugging"""
        try:
            with self.db_manager.get_cursor() as cursor:
                if strategy_name:
                    cursor.execute("""
                        SELECT *
                        FROM backtesting.strategy_config_history
                        WHERE strategy_name = %s
                        AND created_at >= CURRENT_DATE - INTERVAL '%s days'
                        ORDER BY created_at DESC
                    """, (strategy_name, days))
                else:
                    cursor.execute("""
                        SELECT *
                        FROM backtesting.strategy_config_history
                        WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
                        ORDER BY created_at DESC
                    """, (days,))
                
                return cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"Failed to get configuration history: {e}")
            return []
    
    def get_configuration_performance(self, config_version: str) -> Optional[Dict[str, Any]]:
        """Get performance metrics for a specific configuration version"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        cp.*,
                        COUNT(bs.id) as actual_signals_generated
                    FROM backtesting.configuration_performance cp
                    LEFT JOIN betting_signals bs ON bs.configuration_version = cp.configuration_version
                    WHERE cp.configuration_version = %s
                    GROUP BY cp.configuration_version, cp.id
                """, (config_version,))
                
                return cursor.fetchone()
                
        except Exception as e:
            self.logger.error(f"Failed to get configuration performance: {e}")
            return None
    
    async def acknowledge_alert(self, alert_id: int, acknowledged_by: str) -> bool:
        """Acknowledge a strategy alert"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE backtesting.strategy_alerts
                    SET acknowledged = TRUE,
                        acknowledged_at = CURRENT_TIMESTAMP,
                        acknowledged_by = %s
                    WHERE id = %s
                """, (acknowledged_by, alert_id))
                
                if cursor.rowcount > 0:
                    self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
                    return True
                else:
                    self.logger.warning(f"Alert {alert_id} not found for acknowledgment")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Failed to acknowledge alert {alert_id}: {e}")
            return False
    
    async def execute_live_strategy_detection(self, minutes_ahead: int = 60) -> List[BettingSignal]:
        """
        Execute live strategy detection using dynamically configured strategies.
        
        This method replaces hardcoded analysis with performance-based execution.
        """
        strategy_state = await self.get_live_strategy_configuration()
        
        if not strategy_state.enabled_strategies:
            self.logger.warning("No enabled strategies for live detection")
            return []
        
        all_signals = []
        
        for strategy_config in strategy_state.enabled_strategies:
            try:
                # Execute strategy with performance-based configuration
                signals = await self._execute_strategy_with_config(strategy_config, minutes_ahead)
                
                # Apply strategy-specific adjustments
                adjusted_signals = self._apply_strategy_adjustments(signals, strategy_config)
                
                all_signals.extend(adjusted_signals)
                
            except Exception as e:
                self.logger.error(f"Failed to execute strategy {strategy_config.strategy_name}: {e}")
                continue
        
        # Apply ensemble logic
        final_signals = self._apply_ensemble_logic(all_signals, strategy_state.enabled_strategies)
        
        self.logger.info(
            "Live strategy detection completed",
            total_strategies=len(strategy_state.enabled_strategies),
            raw_signals=len(all_signals),
            final_signals=len(final_signals)
        )
        
        return final_signals
    
    async def _execute_strategy_with_config(
        self, 
        strategy_config: StrategyConfiguration, 
        minutes_ahead: int
    ) -> List[BettingSignal]:
        """Execute strategy with performance-based configuration"""
        try:
            # Get processors for this strategy type
            processors = self.processor_factory.get_processors_by_type(strategy_config.signal_type.value)
            
            if not processors:
                self.logger.warning(f"No processors found for {strategy_config.signal_type}")
                return []
            
            # Use the first available processor
            processor = processors[0]
            
            # Create profitable strategies for processor
            from ..models.betting_analysis import ProfitableStrategy
            
            profitable_strategies = [ProfitableStrategy(
                strategy_name=strategy_config.strategy_name,
                source_book="ORCHESTRATOR",  # Synthetic source for orchestrator-driven strategies  
                split_type="DYNAMIC",  # Indicates dynamic orchestrator strategy
                win_rate=strategy_config.recent_win_rate * 100,  # Convert to percentage
                roi=strategy_config.recent_roi,
                total_bets=strategy_config.sample_size,
                confidence="HIGH" if strategy_config.confidence_multiplier >= 1.1 else "MODERATE"
            )]
            
            # Execute processor
            signals = await processor.process(minutes_ahead, profitable_strategies)
            
            return signals or []
            
        except Exception as e:
            self.logger.error(f"Failed to execute strategy {strategy_config.strategy_name}: {e}")
            return []
    
    def _apply_strategy_adjustments(
        self, 
        signals: List[BettingSignal], 
        strategy_config: StrategyConfiguration
    ) -> List[BettingSignal]:
        """Apply strategy-specific adjustments to signals"""
        adjusted_signals = []
        
        for signal in signals:
            # Apply confidence multiplier
            signal.confidence_score *= strategy_config.confidence_multiplier
            signal.confidence_score = min(1.0, signal.confidence_score)
            
            # Apply threshold filter
            if signal.signal_strength < (strategy_config.min_differential_threshold + strategy_config.threshold_adjustment):
                continue
            
            # Add strategy metadata
            if not hasattr(signal, 'metadata'):
                signal.metadata = {}
            
            signal.metadata.update({
                'orchestrator_config': {
                    'strategy_weight': strategy_config.weight_in_ensemble,
                    'performance_trend': strategy_config.performance_trend,
                    'recent_roi': strategy_config.recent_roi
                }
            })
            
            adjusted_signals.append(signal)
        
        return adjusted_signals
    
    def _apply_ensemble_logic(
        self, 
        all_signals: List[BettingSignal], 
        enabled_strategies: List[StrategyConfiguration]
    ) -> List[BettingSignal]:
        """Apply ensemble weighting and conflict resolution"""
        if not all_signals:
            return []
        
        # Group signals by game
        game_signals = {}
        for signal in all_signals:
            game_key = (signal.away_team, signal.home_team, signal.game_time)
            if game_key not in game_signals:
                game_signals[game_key] = []
            game_signals[game_key].append(signal)
        
        final_signals = []
        
        # Process each game
        for game_key, signals in game_signals.items():
            if len(signals) == 1:
                final_signals.extend(signals)
            else:
                # Apply conflict resolution
                resolved_signals = self._resolve_signal_conflicts(signals, enabled_strategies)
                final_signals.extend(resolved_signals)
        
        return final_signals
    
    def _resolve_signal_conflicts(
        self, 
        signals: List[BettingSignal], 
        enabled_strategies: List[StrategyConfiguration]
    ) -> List[BettingSignal]:
        """Resolve conflicts using weighted voting"""
        strategy_weights = {
            config.strategy_name: config.weight_in_ensemble 
            for config in enabled_strategies
        }
        
        # Group by recommendation
        recommendation_groups = {}
        for signal in signals:
            rec_key = signal.recommendation
            if rec_key not in recommendation_groups:
                recommendation_groups[rec_key] = []
            recommendation_groups[rec_key].append(signal)
        
        if len(recommendation_groups) == 1:
            # All agree - return highest confidence
            return [max(signals, key=lambda s: s.confidence_score)]
        
        # Weighted voting for conflicts
        recommendation_scores = {}
        for rec, rec_signals in recommendation_groups.items():
            total_weight = 0
            total_confidence = 0
            
            for signal in rec_signals:
                weight = strategy_weights.get(signal.strategy_name, 0.5)
                total_weight += weight
                total_confidence += signal.confidence_score * weight
            
            recommendation_scores[rec] = {
                'weighted_confidence': total_confidence / total_weight if total_weight > 0 else 0,
                'signals': rec_signals
            }
        
        # Select best recommendation
        best_rec = max(recommendation_scores.keys(), 
                      key=lambda r: recommendation_scores[r]['weighted_confidence'])
        
        best_signals = recommendation_scores[best_rec]['signals']
        return [max(best_signals, key=lambda s: s.confidence_score)]


# Global instance
_strategy_orchestrator = None

async def get_strategy_orchestrator() -> StrategyOrchestrator:
    """Get the global strategy orchestrator instance"""
    global _strategy_orchestrator
    
    if _strategy_orchestrator is None:
        _strategy_orchestrator = StrategyOrchestrator()
        await _strategy_orchestrator.initialize()
    
    return _strategy_orchestrator 