"""
Consolidated Strategy Manager Service

This service consolidates the functionality from:
- strategy_orchestrator.py (orchestration and lifecycle management)
- strategy_config_manager.py (configuration management)
- strategy_auto_integration.py (auto-integration of profitable strategies)

This eliminates redundancy and provides a single point of control for all strategy management.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import structlog
import pytz

from ..core.logging import get_logger
from ..db.connection import get_db_manager, DatabaseManager
from ..core.exceptions import DatabaseError, ValidationError
from ..services.database_coordinator import get_database_coordinator
from ..analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from ..services.betting_signal_repository import BettingSignalRepository
from ..models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy, StrategyThresholds, SignalProcessorConfig
from ..services.juice_filter_service import get_juice_filter_service
from ..core.config import get_settings
from ..services.strategy_validation import StrategyValidation


class StrategyStatus(Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"
    PROBATION = "probation"
    QUARANTINE = "quarantine"


class PerformanceTrend(Enum):
    IMPROVING = "IMPROVING"
    STABLE = "STABLE"
    DECLINING = "DECLINING"
    CRITICAL = "CRITICAL"


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
class StrategyConfiguration:
    """Dynamic strategy configuration based on backtesting performance"""
    strategy_name: str
    signal_type: SignalType
    is_enabled: bool
    confidence_multiplier: float
    threshold_adjustment: float
    weight_in_ensemble: float
    
    # Performance metrics
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
class StrategyConfig:
    """Configuration for a validated strategy."""
    strategy_name: str
    source_book_type: str
    split_type: str
    
    # Performance metrics
    win_rate: float
    roi_per_100: float
    total_bets: int
    confidence_level: str
    
    # Thresholds
    min_threshold: float
    moderate_threshold: float
    high_threshold: float
    
    # Status
    is_active: bool
    last_updated: datetime
    
    # Risk metrics
    max_drawdown: float
    sharpe_ratio: float
    kelly_criterion: float


@dataclass
class ThresholdConfig:
    """Threshold configuration for a specific source/strategy."""
    source: str
    strategy_type: str
    
    # Signal strength thresholds
    high_confidence_threshold: float
    moderate_confidence_threshold: float
    minimum_threshold: float
    
    # Opposing markets thresholds
    opposing_high_threshold: float
    opposing_moderate_threshold: float
    
    # Steam move thresholds
    steam_threshold: float
    steam_time_window_hours: float
    
    # Performance requirements
    min_sample_size: int
    min_win_rate: float
    
    last_validated: datetime
    confidence_level: str


@dataclass
class HighROIStrategy:
    """A strategy identified as having high ROI and sufficient sample size."""
    strategy_id: str
    source_book_type: str
    split_type: str
    strategy_variant: str
    total_bets: int
    win_rate: float
    roi_per_100_unit: float
    confidence_level: str
    
    # Strategy specific configuration
    min_threshold: float
    high_threshold: float
    avg_odds: Optional[float] = None
    
    # Performance tracking
    last_backtesting_update: datetime = None
    integration_status: str = "PENDING"
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


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
class UpdatePolicy:
    """Configuration for when to update strategy configurations"""
    scheduled_interval_minutes: int = 15
    performance_degradation_threshold: float = -15.0
    min_time_between_updates_minutes: int = 5
    max_time_without_update_hours: int = 4
    enable_performance_triggers: bool = True
    enable_new_backtest_triggers: bool = True


@dataclass
class LifecycleThresholds:
    """
    Thresholds for strategy lifecycle management
    
    Updated to implement user-specified disabling rules:
    - Disable if Win Rate < 50% AND ROI < 0%
    - Disable if Win Rate < 55% AND ROI < -5%
    - Disable ANY strategy with ROI < -10%
    """
    # Primary auto-disable thresholds (user-specified)
    auto_disable_roi_threshold: float = -10.0  # Hard threshold: disable ANY strategy with ROI < -10%
    auto_disable_win_rate_threshold: float = 0.50  # Used in combination with ROI
    
    # Secondary thresholds for conditional disabling
    conditional_disable_win_rate_1: float = 0.50  # If win rate < 50% AND ROI < 0%
    conditional_disable_roi_1: float = 0.0
    conditional_disable_win_rate_2: float = 0.55  # If win rate < 55% AND ROI < -5%
    conditional_disable_roi_2: float = -5.0
    
    # Other lifecycle parameters
    auto_disable_consecutive_losses: int = 8
    probation_roi_threshold: float = -5.0
    probation_win_rate_threshold: float = 0.45
    min_confidence_multiplier: float = 0.5
    confidence_reduction_rate: float = 0.1
    min_sample_size_for_decisions: int = 5  # Updated to match implementation
    robust_sample_size: int = 50
    grace_period_days: int = 7
    performance_window_days: int = 14


class StrategyManager:
    """
    Consolidated Strategy Manager combining orchestration, configuration, and auto-integration.
    
    This service consolidates functionality from:
    - StrategyOrchestrator (orchestration and lifecycle management)
    - StrategyConfigManager (configuration management)
    - StrategyAutoIntegration (auto-integration of profitable strategies)
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or get_db_manager()
        self.coordinator = get_database_coordinator()
        self.processor_factory = None
        self.signal_repository = None
        self.settings = get_settings()
        self.juice_filter = get_juice_filter_service()
        
        # Strategy management policies
        self.update_policy = UpdatePolicy()
        self.lifecycle_thresholds = LifecycleThresholds()
        
        # Caches for performance
        self._strategy_cache: Dict[str, StrategyConfig] = {}
        self._threshold_cache: Dict[str, ThresholdConfig] = {}
        self._cache_expiry = datetime.now(timezone.utc)
        self._cache_duration = timedelta(minutes=15)
        
        # Auto-integration settings
        self.min_roi_threshold = 10.0
        self.min_bet_count = 10
        self.est = pytz.timezone('US/Eastern')
        
        # Performance tracking
        self.metrics = {
            "strategies_evaluated": 0,
            "strategies_integrated": 0,
            "strategies_updated": 0,
            "strategies_paused": 0,
            "contrarian_strategies_found": 0,
            "opposing_markets_strategies_found": 0
        }
        
        # Default threshold configurations
        self._default_thresholds = {
            "VSIN": ThresholdConfig(
                source="VSIN",
                strategy_type="default",
                high_confidence_threshold=25.0,
                moderate_confidence_threshold=20.0,
                minimum_threshold=15.0,
                opposing_high_threshold=40.0,
                opposing_moderate_threshold=30.0,
                steam_threshold=25.0,
                steam_time_window_hours=2.0,
                min_sample_size=10,
                min_win_rate=0.52,
                last_validated=datetime.now(timezone.utc),
                confidence_level="DEFAULT"
            ),
            "SBD": ThresholdConfig(
                source="SBD", 
                strategy_type="default",
                high_confidence_threshold=30.0,
                moderate_confidence_threshold=25.0,
                minimum_threshold=20.0,
                opposing_high_threshold=45.0,
                opposing_moderate_threshold=35.0,
                steam_threshold=30.0,
                steam_time_window_hours=2.0,
                min_sample_size=10,
                min_win_rate=0.52,
                last_validated=datetime.now(timezone.utc),
                confidence_level="DEFAULT"
            )
        }
    
    async def initialize(self):
        """Initialize the strategy manager"""
        try:
            # Create a default SignalProcessorConfig
            config = SignalProcessorConfig()
            
            # Initialize required dependencies for StrategyProcessorFactory
            self.signal_repository = BettingSignalRepository(config)
            
            # Initialize StrategyValidation with default parameters
            strategy_validation = StrategyValidation(db_manager=self.db_manager)
            await strategy_validation.initialize()
            
            # Now initialize the processor factory with required dependencies
            self.processor_factory = StrategyProcessorFactory(
                repository=self.signal_repository,
                validator=strategy_validation,
                config=config
            )
            
            await self._refresh_cache_if_needed()
            self.logger.info("✅ Strategy Manager initialized successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Strategy Manager: {e}")
            raise
    
    # ===========================================
    # STRATEGY CONFIGURATION MANAGEMENT
    # ===========================================
    
    async def get_active_strategies(self) -> List[StrategyConfig]:
        """Get all currently active and profitable strategies."""
        await self._refresh_cache_if_needed()
        
        if not self._strategy_cache:
            return []
        
        # Return only active strategies that are not disabled by our rules
        active_strategies = []
        for config in self._strategy_cache.values():
            if not config.is_active:
                continue
                
            # Use centralized disabling logic
            win_rate = config.win_rate
            roi = config.roi_per_100
            total_bets = config.total_bets
            
            should_disable = self._should_disable_strategy(win_rate, roi, total_bets)
            
            if not should_disable:
                active_strategies.append(config)
        
        # Sort by performance (ROI then win rate)
        active_strategies.sort(key=lambda x: (x.roi_per_100, x.win_rate), reverse=True)
        
        return active_strategies
    
    async def get_threshold_config(self, source: str) -> ThresholdConfig:
        """Get validated threshold configuration for a source."""
        await self._refresh_cache_if_needed()
        
        if self._threshold_cache and source in self._threshold_cache:
            return self._threshold_cache[source]
        
        # Fall back to defaults if no validated thresholds available
        if source in self._default_thresholds:
            self.logger.warning("Using default thresholds", source=source)
            return self._default_thresholds[source]
        
        # Ultimate fallback
        self.logger.warning("No thresholds found, using conservative defaults", source=source)
        return self._default_thresholds["VSIN"]
    
    async def get_strategy_performance(self, strategy_name: str) -> Optional[StrategyConfig]:
        """Get performance data for a specific strategy."""
        await self._refresh_cache_if_needed()
        return self._strategy_cache.get(strategy_name)
    
    async def get_best_strategies_by_type(self, split_type: str = None) -> List[StrategyConfig]:
        """Get the best performing strategies, optionally filtered by split type."""
        active_strategies = await self.get_active_strategies()
        
        if split_type:
            active_strategies = [s for s in active_strategies if s.split_type == split_type]
        
        return active_strategies[:3]
    
    async def is_strategy_enabled(self, strategy_name: str, min_win_rate: float = 0.50) -> bool:
        """Check if a strategy is enabled and not disabled by our rules."""
        config = await self.get_strategy_performance(strategy_name)
        if config is None or not config.is_active:
            return False
            
        # Use centralized disabling logic
        should_disable = self._should_disable_strategy(
            config.win_rate, 
            config.roi_per_100, 
            config.total_bets
        )
        
        return not should_disable and config.win_rate >= min_win_rate
    
    # ===========================================
    # STRATEGY ORCHESTRATION & LIFECYCLE
    # ===========================================
    
    async def get_live_strategy_configuration(self, force_refresh: bool = False) -> LiveStrategyState:
        """Get current live strategy configuration for real-time detection."""
        if force_refresh or await self._should_update_configuration():
            await self._update_strategy_configuration(UpdateTrigger.SCHEDULED)
        
        # Load current strategy configurations
        backtest_results = await self._get_recent_backtest_results()
        configurations = await self._create_strategy_configurations(backtest_results)
        
        enabled_strategies = [c for c in configurations if c.is_enabled]
        disabled_strategies = [c for c in configurations if not c.is_enabled]
        
        return LiveStrategyState(
            enabled_strategies=enabled_strategies,
            disabled_strategies=disabled_strategies,
            performance_summary=self._generate_performance_summary(configurations),
            last_updated=datetime.now(timezone.utc),
            configuration_version=f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    
    async def execute_live_strategy_detection(self, minutes_ahead: int = 60) -> List[BettingSignal]:
        """Execute live strategy detection using current configurations."""
        if not self.processor_factory:
            await self.initialize()
        
        live_config = await self.get_live_strategy_configuration()
        all_signals = []
        
        for strategy_config in live_config.enabled_strategies:
            try:
                signals = await self._execute_strategy_with_config(
                    strategy_config, minutes_ahead
                )
                all_signals.extend(signals)
            except Exception as e:
                self.logger.error(f"Failed to execute strategy {strategy_config.strategy_name}: {e}")
        
        # Apply ensemble logic and conflict resolution
        if len(all_signals) > 1:
            all_signals = self._apply_ensemble_logic(all_signals, live_config.enabled_strategies)
            all_signals = self._resolve_signal_conflicts(all_signals, live_config.enabled_strategies)
        
        return all_signals
    
    # ===========================================
    # AUTO-INTEGRATION OF PROFITABLE STRATEGIES
    # ===========================================
    
    async def identify_high_roi_strategies(self, lookback_days: int = 30) -> List[HighROIStrategy]:
        """Identify strategies with high ROI and sufficient sample size from recent backtesting."""
        self.logger.info("Identifying high-ROI strategies for auto-integration",
                        min_roi=self.min_roi_threshold,
                        min_bets=self.min_bet_count,
                        lookback_days=lookback_days)
        
        high_roi_strategies = []
        
        try:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=lookback_days)
            
            query = """
            SELECT DISTINCT
                strategy_name as strategy_variant,
                source_book_type,
                split_type,
                total_bets,
                wins,
                win_rate,
                roi_per_100 as roi_per_100_unit,
                created_at as last_updated
            FROM backtesting.strategy_performance 
            WHERE backtest_date >= %s
              AND total_bets >= %s
              AND roi_per_100 >= %s
              AND strategy_name IS NOT NULL
            ORDER BY roi_per_100 DESC, total_bets DESC
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (start_date, self.min_bet_count, self.min_roi_threshold))
                results = cursor.fetchall()
            
            for row in results:
                confidence_level = self._determine_confidence_level(
                    row['total_bets'], row['roi_per_100_unit']
                )
                
                min_threshold, high_threshold = self._calculate_thresholds(
                    row['strategy_variant'], row['roi_per_100_unit'], row['win_rate']
                )
                
                strategy = HighROIStrategy(
                    strategy_id=f"{row['source_book_type']}-{row['split_type']}-{row['strategy_variant']}",
                    source_book_type=row['source_book_type'],
                    split_type=row['split_type'],
                    strategy_variant=row['strategy_variant'],
                    total_bets=row['total_bets'],
                    win_rate=float(row['win_rate']),
                    roi_per_100_unit=float(row['roi_per_100_unit']),
                    confidence_level=confidence_level,
                    min_threshold=min_threshold,
                    high_threshold=high_threshold,
                    last_backtesting_update=row['last_updated']
                )
                
                high_roi_strategies.append(strategy)
                self.metrics["strategies_evaluated"] += 1
                
                # Track special strategy types
                if 'contrarian' in row['strategy_variant'].lower():
                    self.metrics["contrarian_strategies_found"] += 1
                if 'opposing' in row['strategy_variant'].lower():
                    self.metrics["opposing_markets_strategies_found"] += 1
            
            self.logger.info("High-ROI strategies identified",
                           total_strategies=len(high_roi_strategies),
                           contrarian_count=self.metrics["contrarian_strategies_found"],
                           opposing_markets_count=self.metrics["opposing_markets_strategies_found"])
            
            return high_roi_strategies
            
        except Exception as e:
            self.logger.error("Failed to identify high-ROI strategies", error=str(e))
            raise DatabaseError(f"Failed to identify strategies: {e}")
    
    async def auto_integrate_high_roi_strategies(self, lookback_days: int = 30) -> List[Dict[str, Any]]:
        """Auto-integrate high-ROI strategies into live system."""
        high_roi_strategies = await self.identify_high_roi_strategies(lookback_days)
        integration_results = []
        
        for strategy in high_roi_strategies:
            try:
                # Update strategy configuration
                config_updated = await self._update_strategy_configuration_for_integration(strategy)
                
                # Register as active strategy
                await self._register_active_strategy(strategy)
                
                # Log integration
                await self._log_strategy_integration(strategy)
                
                integration_results.append({
                    'strategy': strategy,
                    'integration_successful': True,
                    'configuration_updated': config_updated,
                    'error_message': None
                })
                
                self.metrics["strategies_integrated"] += 1
                
            except Exception as e:
                self.logger.error(f"Failed to integrate strategy {strategy.strategy_id}: {e}")
                integration_results.append({
                    'strategy': strategy,
                    'integration_successful': False,
                    'configuration_updated': False,
                    'error_message': str(e)
                })
        
        return integration_results
    
    # ===========================================
    # PRIVATE HELPER METHODS
    # ===========================================
    
    async def _refresh_cache_if_needed(self) -> None:
        """Refresh cache if expired"""
        if datetime.now(timezone.utc) > self._cache_expiry:
            await self._load_configurations()
    
    async def _load_configurations(self) -> None:
        """Load strategy and threshold configurations from database"""
        try:
            self._strategy_cache = await self._load_strategy_configs()
            self._threshold_cache = await self._load_threshold_configs()
            self._cache_expiry = datetime.now(timezone.utc) + self._cache_duration
        except Exception as e:
            self.logger.error(f"Failed to load configurations: {e}")
            raise
    
    async def _load_strategy_configs(self) -> Dict[str, StrategyConfig]:
        """Load strategy configurations from backtesting results"""
        configs = {}
        
        query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            win_rate,
            roi_per_100,
            total_bets,
            confidence_level,
            is_active,
            last_updated,
            max_drawdown,
            sharpe_ratio,
            kelly_criterion
        FROM backtesting.strategy_configurations
        WHERE is_active = true
        ORDER BY roi_per_100 DESC
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            for row in results:
                # Calculate dynamic thresholds
                min_threshold = max(15.0, 100.0 / max(row['roi_per_100'], 1.0))
                moderate_threshold = min_threshold * 1.5
                high_threshold = min_threshold * 2.0
                
                config = StrategyConfig(
                    strategy_name=row['strategy_name'],
                    source_book_type=row['source_book_type'],
                    split_type=row['split_type'],
                    win_rate=float(row['win_rate']),
                    roi_per_100=float(row['roi_per_100']),
                    total_bets=row['total_bets'],
                    confidence_level=row['confidence_level'],
                    min_threshold=min_threshold,
                    moderate_threshold=moderate_threshold,
                    high_threshold=high_threshold,
                    is_active=row['is_active'],
                    last_updated=row['last_updated'],
                    max_drawdown=float(row.get('max_drawdown', 0.0)),
                    sharpe_ratio=float(row.get('sharpe_ratio', 0.0)),
                    kelly_criterion=float(row.get('kelly_criterion', 0.0))
                )
                
                configs[row['strategy_name']] = config
                
        except Exception as e:
            self.logger.warning(f"Failed to load strategy configs from DB: {e}")
            # Return empty dict - will use defaults
        
        return configs
    
    async def _load_threshold_configs(self) -> Dict[str, ThresholdConfig]:
        """Load threshold configurations from database"""
        configs = {}
        
        query = """
        SELECT 
            source,
            strategy_type,
            high_confidence_threshold,
            moderate_confidence_threshold,
            minimum_threshold,
            opposing_high_threshold,
            opposing_moderate_threshold,
            steam_threshold,
            steam_time_window_hours,
            min_sample_size,
            min_win_rate,
            last_validated,
            confidence_level
        FROM backtesting.threshold_configurations
        WHERE is_active = true
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            for row in results:
                config = ThresholdConfig(
                    source=row['source'],
                    strategy_type=row['strategy_type'],
                    high_confidence_threshold=float(row['high_confidence_threshold']),
                    moderate_confidence_threshold=float(row['moderate_confidence_threshold']),
                    minimum_threshold=float(row['minimum_threshold']),
                    opposing_high_threshold=float(row['opposing_high_threshold']),
                    opposing_moderate_threshold=float(row['opposing_moderate_threshold']),
                    steam_threshold=float(row['steam_threshold']),
                    steam_time_window_hours=float(row['steam_time_window_hours']),
                    min_sample_size=row['min_sample_size'],
                    min_win_rate=float(row['min_win_rate']),
                    last_validated=row['last_validated'],
                    confidence_level=row['confidence_level']
                )
                
                configs[row['source']] = config
                
        except Exception as e:
            self.logger.warning(f"Failed to load threshold configs from DB: {e}")
            # Return empty dict - will use defaults
        
        return configs
    
    async def _should_update_configuration(self) -> bool:
        """Check if strategy configuration should be updated"""
        now = datetime.now(timezone.utc)
        
        # Check if enough time has passed since last update
        if hasattr(self, '_last_config_update'):
            time_since_update = (now - self._last_config_update).total_seconds() / 60
            if time_since_update < self.update_policy.min_time_between_updates_minutes:
                return False
        
        # Check for performance degradation
        if self.update_policy.enable_performance_triggers:
            if await self._check_performance_degradation():
                return True
        
        # Check for new backtest results
        if self.update_policy.enable_new_backtest_triggers:
            if await self._check_new_backtest_results():
                return True
        
        # Check scheduled update interval
        if hasattr(self, '_last_config_update'):
            time_since_update = (now - self._last_config_update).total_seconds() / 60
            if time_since_update >= self.update_policy.scheduled_interval_minutes:
                return True
        
        return False
    
    async def _check_performance_degradation(self) -> bool:
        """Check if any strategy has performance degradation"""
        try:
            current_performance = await self._get_current_strategy_performance()
            
            for strategy_name, current_roi in current_performance.items():
                config = await self.get_strategy_performance(strategy_name)
                if config and current_roi < (config.roi_per_100 + self.update_policy.performance_degradation_threshold):
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Failed to check performance degradation: {e}")
            return False
    
    async def _check_new_backtest_results(self) -> bool:
        """Check if there are new backtest results since last configuration update"""
        try:
            last_update = getattr(self, '_last_config_update', datetime.now(timezone.utc) - timedelta(hours=24))
            
            query = """
            SELECT COUNT(*) as new_results
            FROM backtesting.strategy_performance
            WHERE created_at > %s
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (last_update,))
                result = cursor.fetchone()
                return result['new_results'] > 0
                
        except Exception as e:
            self.logger.error(f"Failed to check new backtest results: {e}")
            return False
    
    async def _get_recent_backtest_results(self) -> List[Dict[str, Any]]:
        """Get recent backtest results for strategy configuration"""
        try:
            query = """
            SELECT 
                strategy_name,
                win_rate,
                roi_per_100,
                total_bets,
                confidence_level,
                source_book_type,
                split_type,
                created_at
            FROM backtesting.strategy_performance
            WHERE created_at >= %s
              AND total_bets >= 5
            ORDER BY roi_per_100 DESC
            """
            
            lookback_date = datetime.now(timezone.utc) - timedelta(days=7)
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (lookback_date,))
                return cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"Failed to get recent backtest results: {e}")
            return []
    
    async def _create_strategy_configurations(self, backtest_results: List[Dict[str, Any]]) -> List[StrategyConfiguration]:
        """Create strategy configurations from backtest results"""
        configurations = []
        
        for result in backtest_results:
            # Apply user-specified disabling rules
            win_rate = result['win_rate']
            roi = result['roi_per_100']
            total_bets = result['total_bets']
            
            # Use centralized disabling logic
            should_disable = self._should_disable_strategy(win_rate, roi, total_bets)
            
            # Log disabling decisions for transparency
            if should_disable:
                disable_reason = self._get_disable_reason(win_rate, roi, total_bets)
                self.logger.info(
                    f"🚫 Strategy DISABLED: {result['strategy_name']}",
                    reason=disable_reason,
                    win_rate=f"{win_rate:.1%}",
                    roi=f"{roi:.1f}%",
                    total_bets=total_bets
                )
            
            # Strategy is enabled if not disabled
            is_enabled = not should_disable
            
            # Calculate dynamic parameters
            confidence_multiplier = min(1.2, 0.8 + (result['roi_per_100'] / 100.0))
            threshold_adjustment = max(0.0, (60.0 - result['win_rate']) / 100.0)
            weight_in_ensemble = min(1.0, result['roi_per_100'] / 20.0)
            
            # Map strategy name to signal type
            signal_type = self._map_strategy_to_signal_type(result['strategy_name'])
            
            config = StrategyConfiguration(
                strategy_name=result['strategy_name'],
                signal_type=signal_type,
                is_enabled=is_enabled,
                confidence_multiplier=confidence_multiplier,
                threshold_adjustment=threshold_adjustment,
                weight_in_ensemble=weight_in_ensemble,
                recent_win_rate=result['win_rate'],
                recent_roi=result['roi_per_100'],
                sample_size=result['total_bets'],
                performance_trend="STABLE",  # Could be calculated from historical data
                min_differential_threshold=15.0 + threshold_adjustment * 10,
                max_recommendations_per_day=min(10, max(1, int(result['roi_per_100'] / 5))),
                last_performance_update=result['created_at'],
                strategy_status=StrategyStatus.ACTIVE if is_enabled else StrategyStatus.PAUSED
            )
            
            configurations.append(config)
        
        return configurations
    
    def _map_strategy_to_signal_type(self, strategy_name: str) -> Optional[SignalType]:
        """Map strategy names to signal types"""
        strategy_lower = strategy_name.lower()
        
        if 'book_conflicts' in strategy_lower:
            return SignalType.BOOK_CONFLICTS
        elif 'opposing_markets' in strategy_lower:
            return SignalType.OPPOSING_MARKETS
        elif 'late_flip' in strategy_lower or 'steam' in strategy_lower:
            return SignalType.LATE_FLIP
        elif 'public_fade' in strategy_lower:
            return SignalType.PUBLIC_FADE
        elif 'sharp' in strategy_lower:
            return SignalType.SHARP_ACTION
        else:
            return SignalType.SHARP_ACTION  # Default fallback
    
    async def _update_strategy_configuration(self, trigger: UpdateTrigger):
        """Update strategy configuration based on trigger"""
        try:
            self.logger.info(f"Updating strategy configuration: {trigger.value}")
            
            # Force refresh of configurations
            await self._load_configurations()
            
            # Update timestamp
            self._last_config_update = datetime.now(timezone.utc)
            
            self.logger.info("Strategy configuration updated successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to update strategy configuration: {e}")
            raise
    
    async def _execute_strategy_with_config(self, strategy_config: StrategyConfiguration, minutes_ahead: int) -> List[BettingSignal]:
        """Execute a strategy with its configuration"""
        try:
            # Get the appropriate processor for this strategy
            processor = self.processor_factory.get_processor(strategy_config.signal_type)
            
            if not processor:
                self.logger.warning(f"No processor found for signal type: {strategy_config.signal_type}")
                return []
            
            # Execute the processor
            signals = await processor.process_signals(minutes_ahead=minutes_ahead)
            
            # Apply strategy-specific adjustments
            adjusted_signals = self._apply_strategy_adjustments(signals, strategy_config)
            
            return adjusted_signals
            
        except Exception as e:
            self.logger.error(f"Failed to execute strategy {strategy_config.strategy_name}: {e}")
            return []
    
    def _apply_strategy_adjustments(self, signals: List[BettingSignal], strategy_config: StrategyConfiguration) -> List[BettingSignal]:
        """Apply strategy-specific adjustments to signals"""
        adjusted_signals = []
        
        for signal in signals:
            # Apply confidence multiplier
            adjusted_confidence = signal.confidence * strategy_config.confidence_multiplier
            adjusted_confidence = min(1.0, max(0.0, adjusted_confidence))  # Clamp to [0, 1]
            
            # Apply threshold adjustment
            if signal.signal_strength >= strategy_config.min_differential_threshold:
                adjusted_signal = BettingSignal(
                    game_id=signal.game_id,
                    signal_type=signal.signal_type,
                    signal_strength=signal.signal_strength,
                    confidence=adjusted_confidence,
                    recommended_bet=signal.recommended_bet,
                    odds=signal.odds,
                    metadata={
                        **signal.metadata,
                        'strategy_name': strategy_config.strategy_name,
                        'original_confidence': signal.confidence,
                        'confidence_multiplier': strategy_config.confidence_multiplier,
                        'threshold_used': strategy_config.min_differential_threshold
                    }
                )
                adjusted_signals.append(adjusted_signal)
        
        return adjusted_signals
    
    def _apply_ensemble_logic(self, all_signals: List[BettingSignal], enabled_strategies: List[StrategyConfiguration]) -> List[BettingSignal]:
        """Apply ensemble logic to combine signals from multiple strategies"""
        if len(all_signals) <= 1:
            return all_signals
        
        # Group signals by game_id and bet type
        signal_groups = {}
        for signal in all_signals:
            key = (signal.game_id, signal.recommended_bet)
            if key not in signal_groups:
                signal_groups[key] = []
            signal_groups[key].append(signal)
        
        ensemble_signals = []
        
        for (game_id, bet_type), signals in signal_groups.items():
            if len(signals) == 1:
                ensemble_signals.append(signals[0])
            else:
                # Create ensemble signal
                total_weight = sum(
                    next((s.weight_in_ensemble for s in enabled_strategies 
                         if s.strategy_name == signal.metadata.get('strategy_name', '')), 1.0)
                    for signal in signals
                )
                
                weighted_confidence = sum(
                    signal.confidence * next(
                        (s.weight_in_ensemble for s in enabled_strategies 
                        if s.strategy_name == signal.metadata.get('strategy_name', '')), 1.0
                    )
                    for signal in signals
                ) / total_weight if total_weight > 0 else 0.0
                
                # Use the signal with highest individual confidence as base
                best_signal = max(signals, key=lambda s: s.confidence)
                
                ensemble_signal = BettingSignal(
                    game_id=game_id,
                    signal_type=SignalType.ENSEMBLE,
                    signal_strength=best_signal.signal_strength,
                    confidence=weighted_confidence,
                    recommended_bet=bet_type,
                    odds=best_signal.odds,
                    metadata={
                        'ensemble_strategies': [s.metadata.get('strategy_name', 'unknown') for s in signals],
                        'individual_confidences': [s.confidence for s in signals],
                        'ensemble_weight': total_weight,
                        'base_signal': best_signal.metadata
                    }
                )
                
                ensemble_signals.append(ensemble_signal)
        
        return ensemble_signals
    
    def _resolve_signal_conflicts(self, signals: List[BettingSignal], enabled_strategies: List[StrategyConfiguration]) -> List[BettingSignal]:
        """Resolve conflicts between signals for the same game"""
        if len(signals) <= 1:
            return signals
        
        # Group by game_id
        game_signals = {}
        for signal in signals:
            if signal.game_id not in game_signals:
                game_signals[signal.game_id] = []
            game_signals[signal.game_id].append(signal)
        
        resolved_signals = []
        
        for game_id, game_signal_list in game_signals.items():
            if len(game_signal_list) == 1:
                resolved_signals.append(game_signal_list[0])
            else:
                # Check for direct conflicts (opposing bets)
                bet_types = [s.recommended_bet for s in game_signal_list]
                
                # If no conflicts, keep all signals
                if len(set(bet_types)) == len(bet_types):
                    resolved_signals.extend(game_signal_list)
                else:
                    # Keep the highest confidence signal
                    best_signal = max(game_signal_list, key=lambda s: s.confidence)
                    resolved_signals.append(best_signal)
        
        return resolved_signals
    
    def _generate_performance_summary(self, configurations: List[StrategyConfiguration]) -> Dict[str, Any]:
        """Generate performance summary for configurations"""
        if not configurations:
            return {}
        
        enabled_configs = [c for c in configurations if c.is_enabled]
        
        return {
            'total_strategies': len(configurations),
            'enabled_strategies': len(enabled_configs),
            'disabled_strategies': len(configurations) - len(enabled_configs),
            'avg_win_rate': sum(c.recent_win_rate for c in enabled_configs) / len(enabled_configs) if enabled_configs else 0,
            'avg_roi': sum(c.recent_roi for c in enabled_configs) / len(enabled_configs) if enabled_configs else 0,
            'total_sample_size': sum(c.sample_size for c in configurations),
            'high_performers': len([c for c in enabled_configs if c.recent_roi >= 15.0]),
            'moderate_performers': len([c for c in enabled_configs if 10.0 <= c.recent_roi < 15.0]),
            'low_performers': len([c for c in enabled_configs if c.recent_roi < 10.0])
        }
    
    async def _get_current_strategy_performance(self) -> Dict[str, float]:
        """Get current performance metrics for all strategies"""
        # This would query recent betting results to calculate current ROI
        # For now, return empty dict
        return {}
    
    def _determine_confidence_level(self, total_bets: int, roi: float) -> str:
        """Determine confidence level based on sample size and ROI."""
        if total_bets >= 50 and roi >= 20.0:
            return "VERY_HIGH"
        elif total_bets >= 30 and roi >= 15.0:
            return "HIGH"
        elif total_bets >= 20 and roi >= 10.0:
            return "MODERATE"
        else:
            return "LOW"
    
    def _calculate_thresholds(self, strategy_variant: str, roi: float, win_rate: float) -> Tuple[float, float]:
        """Calculate dynamic thresholds based on strategy performance."""
        # Base threshold inversely related to ROI - ensure float conversion
        base_threshold = max(10.0, 100.0 / max(float(roi), 1.0))
        
        # Adjust based on win rate - ensure float conversion
        win_rate_adjustment = (60.0 - float(win_rate)) / 10.0  # Higher threshold for lower win rates
        
        # Strategy-specific adjustments
        if 'opposing_markets' in strategy_variant.lower():
            base_threshold *= 1.5  # Higher threshold for opposing markets
        elif 'steam' in strategy_variant.lower():
            base_threshold *= 1.2  # Slightly higher for steam moves
        
        min_threshold = base_threshold + win_rate_adjustment
        high_threshold = min_threshold * 1.8
        
        return max(5.0, min_threshold), max(10.0, high_threshold)
    
    async def _update_strategy_configuration_for_integration(self, strategy: HighROIStrategy) -> bool:
        """Update strategy configuration for integration"""
        try:
            # Implementation would update database with new strategy configuration
            self.logger.info(f"Updated configuration for strategy {strategy.strategy_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update configuration for {strategy.strategy_id}: {e}")
            return False
    
    async def _register_active_strategy(self, strategy: HighROIStrategy):
        """Register strategy as active in the system"""
        try:
            # Implementation would register strategy in active strategies table
            self.logger.info(f"Registered active strategy {strategy.strategy_id}")
        except Exception as e:
            self.logger.error(f"Failed to register strategy {strategy.strategy_id}: {e}")
            raise
    
    async def _log_strategy_integration(self, strategy: HighROIStrategy):
        """Log strategy integration event"""
        try:
            # Implementation would log integration event
            self.logger.info(f"Logged integration for strategy {strategy.strategy_id}")
        except Exception as e:
            self.logger.error(f"Failed to log integration for {strategy.strategy_id}: {e}")
    
    def _should_disable_strategy(self, win_rate: float, roi: float, total_bets: int) -> bool:
        """
        Centralized logic to determine if a strategy should be disabled.
        
        User-specified disabling rules:
        - Disable if Win Rate < 50% AND ROI < 0%
        - Disable if Win Rate < 55% AND ROI < -5%
        - Disable ANY strategy with ROI < -10%
        - Disable if insufficient sample size (< 5 bets)
        
        Args:
            win_rate: Strategy win rate (0.0 to 1.0)
            roi: Strategy ROI per 100 units
            total_bets: Total number of bets for the strategy
            
        Returns:
            True if strategy should be disabled, False otherwise
        """
        return (
            # Rule 1: Disable if Win Rate < 50% AND ROI < 0%
            (win_rate < 0.50 and roi < 0.0) or
            # Rule 2: Disable if Win Rate < 55% AND ROI < -5%
            (win_rate < 0.55 and roi < -5.0) or
            # Rule 3: Disable ANY strategy with ROI < -10%
            (roi < -10.0) or
            # Additional safety: Insufficient sample size
            (total_bets < 5)
        )

    def _get_disable_reason(self, win_rate: float, roi: float, total_bets: int) -> str:
        """Get human-readable reason for strategy disabling."""
        if total_bets < 5:
            return f"Insufficient sample size ({total_bets} bets)"
        elif roi < -10.0:
            return f"ROI below -10% threshold ({roi:.1f}%)"
        elif win_rate < 0.50 and roi < 0.0:
            return f"Win rate < 50% ({win_rate:.1%}) AND ROI < 0% ({roi:.1f}%)"
        elif win_rate < 0.55 and roi < -5.0:
            return f"Win rate < 55% ({win_rate:.1%}) AND ROI < -5% ({roi:.1f}%)"
        else:
            return "Unknown disabling rule"

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for the strategy manager"""
        return {
            **self.metrics,
            'cache_status': {
                'strategy_cache_size': len(self._strategy_cache),
                'threshold_cache_size': len(self._threshold_cache),
                'cache_expiry': self._cache_expiry.isoformat(),
                'cache_valid': datetime.now(timezone.utc) < self._cache_expiry
            }
        }


# Singleton pattern for global access
_strategy_manager_instance: Optional[StrategyManager] = None

async def get_strategy_manager() -> StrategyManager:
    """Get singleton instance of Strategy Manager"""
    global _strategy_manager_instance
    if _strategy_manager_instance is None:
        _strategy_manager_instance = StrategyManager()
        await _strategy_manager_instance.initialize()
    return _strategy_manager_instance 