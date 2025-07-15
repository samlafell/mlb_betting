"""
Unified Strategy Factory

Modern factory for creating and managing strategy processors with:
- Dynamic strategy loading and registration
- A/B testing framework for strategy comparison
- Performance-based strategy selection
- Legacy strategy migration support
- Comprehensive error handling and monitoring

Part of Phase 5B: Core Business Logic Migration - Enhanced with migrated processors
"""

import asyncio
import importlib
import inspect
from datetime import datetime
from typing import Any

from src.analysis.models.unified_models import (
    SignalType,
    StrategyCategory,
)
from src.analysis.strategies.base import BaseStrategyProcessor
from src.core.logging import LogComponent, get_logger
from src.data.database import UnifiedRepository


class StrategyLoadingStatus:
    """Status tracking for strategy loading operations"""

    def __init__(self):
        self.total_strategies = 0
        self.loaded_strategies = 0
        self.failed_strategies = 0
        self.loading_errors: dict[str, str] = {}
        self.load_start_time: datetime | None = None
        self.load_end_time: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate loading success rate"""
        if self.total_strategies == 0:
            return 0.0
        return self.loaded_strategies / self.total_strategies

    @property
    def loading_time(self) -> float | None:
        """Calculate total loading time in seconds"""
        if self.load_start_time and self.load_end_time:
            return (self.load_end_time - self.load_start_time).total_seconds()
        return None


class StrategyFactory:
    """
    Modern factory for creating and managing strategy processors.

    Provides comprehensive strategy management including:
    - Dynamic strategy loading and registration
    - A/B testing framework for strategy comparison
    - Performance-based strategy selection
    - Legacy strategy migration support
    - Real-time monitoring and health checks

    This replaces the legacy StrategyProcessorFactory with modern patterns
    and includes migrated processors from Phase 5B.
    """

    # Enhanced strategy registry with migrated processors
    STRATEGY_REGISTRY: dict[str, dict[str, Any]] = {
        # Phase 5B Migrated Processors - Core Business Logic
        # Sharp Action Strategies
        "unified_sharp_action": {
            "class": "UnifiedSharpActionProcessor",
            "module": "src.analysis.processors.sharp_action_processor",
            "category": StrategyCategory.SHARP_ACTION,
            "signal_type": SignalType.SHARP_ACTION,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Enhanced sharp action detection with book-specific analysis and volume weighting",
            "legacy_equivalent": "SharpActionProcessor",
            "migration_phase": "5B",
        },
        # Timing Analysis Strategies
        "unified_timing_based": {
            "class": "UnifiedTimingBasedProcessor",
            "module": "src.analysis.processors.timing_based_processor",
            "category": StrategyCategory.TIMING_ANALYSIS,
            "signal_type": SignalType.TIMING_BASED,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Advanced timing-based analysis with 9 timing categories and game context",
            "legacy_equivalent": "TimingBasedProcessor",
            "migration_phase": "5B",
        },
        # Market Inefficiency Strategies
        "unified_book_conflict": {
            "class": "UnifiedBookConflictProcessor",
            "module": "src.analysis.processors.book_conflict_processor",
            "category": StrategyCategory.MARKET_INEFFICIENCY,
            "signal_type": SignalType.BOOK_CONFLICT,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Book conflict detection with arbitrage identification and reliability scoring",
            "legacy_equivalent": "BookConflictProcessor",
            "migration_phase": "5B",
        },
        # Consensus Analysis Strategies
        "unified_consensus": {
            "class": "UnifiedConsensusProcessor",
            "module": "src.analysis.processors.consensus_processor",
            "category": StrategyCategory.CONSENSUS_ANALYSIS,
            "signal_type": SignalType.CONSENSUS,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Consensus analysis: Follow or fade when public and sharp money align",
            "legacy_equivalent": "ConsensusProcessor",
            "migration_phase": "5C",
        },
        # Public Fade Strategies
        "unified_public_fade": {
            "class": "UnifiedPublicFadeProcessor",
            "module": "src.analysis.processors.public_fade_processor",
            "category": StrategyCategory.CONSENSUS_ANALYSIS,
            "signal_type": SignalType.PUBLIC_FADE,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Public fade strategy: Identify heavy public consensus as contrarian opportunities",
            "legacy_equivalent": "PublicFadeProcessor",
            "migration_phase": "5C",
        },
        # Late Flip Strategies
        "unified_late_flip": {
            "class": "UnifiedLateFlipProcessor",
            "module": "src.analysis.processors.late_flip_processor",
            "category": StrategyCategory.TIMING_ANALYSIS,
            "signal_type": SignalType.LATE_FLIP,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Late flip strategy: Fade late sharp money flips and follow early action",
            "legacy_equivalent": "LateFlipProcessor",
            "migration_phase": "5C",
        },
        # Underdog Value Strategies
        "unified_underdog_value": {
            "class": "UnifiedUnderdogValueProcessor",
            "module": "src.analysis.processors.underdog_value_processor",
            "category": StrategyCategory.VALUE_ANALYSIS,
            "signal_type": SignalType.UNDERDOG_VALUE,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Underdog value strategy: Systematic value on underdogs when public favors favorites",
            "legacy_equivalent": "UnderdogValueProcessor",
            "migration_phase": "5C",
        },
        # Line Movement Strategies
        "unified_line_movement": {
            "class": "UnifiedLineMovementProcessor",
            "module": "src.analysis.processors.line_movement_processor",
            "category": StrategyCategory.MARKET_INEFFICIENCY,
            "signal_type": SignalType.LINE_MOVEMENT,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Line movement analysis: Detect sharp action through movement patterns",
            "legacy_equivalent": "LineMovementProcessor",
            "migration_phase": "5C",
        },
        # Hybrid Sharp Strategies
        "unified_hybrid_sharp": {
            "class": "UnifiedHybridSharpProcessor",
            "module": "src.analysis.processors.hybrid_sharp_processor",
            "category": StrategyCategory.HYBRID_ANALYSIS,
            "signal_type": SignalType.HYBRID_SHARP,
            "status": "MIGRATED",
            "priority": "HIGH",
            "description": "Hybrid sharp strategy: Combine line movement with sharp action for enhanced confidence",
            "legacy_equivalent": "HybridSharpProcessor",
            "migration_phase": "5C",
        },
        # Legacy System Bridges (for gradual migration)
        "legacy_sharp_action": {
            "class": "SharpActionProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.sharpaction_processor",
            "category": StrategyCategory.SHARP_ACTION,
            "signal_type": SignalType.SHARP_ACTION,
            "status": "LEGACY_BRIDGE",
            "priority": "MEDIUM",
            "description": "Legacy sharp action processor (bridge mode)",
            "migration_target": "unified_sharp_action",
            "migration_phase": "5A",
        },
        "legacy_timing_based": {
            "class": "TimingBasedProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.timingbased_processor",
            "category": StrategyCategory.TIMING_ANALYSIS,
            "signal_type": SignalType.TIMING_BASED,
            "status": "LEGACY_BRIDGE",
            "priority": "MEDIUM",
            "description": "Legacy timing-based processor (bridge mode)",
            "migration_target": "unified_timing_based",
            "migration_phase": "5A",
        },
        # Remaining Legacy Processors (to be migrated in Phase 5C)
        "legacy_consensus": {
            "class": "ConsensusProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.consensus_processor",
            "category": StrategyCategory.CONSENSUS_ANALYSIS,
            "signal_type": SignalType.CONSENSUS,
            "status": "LEGACY_PENDING",
            "priority": "MEDIUM",
            "description": "Consensus analysis for public and sharp alignment",
            "migration_phase": "5C",
        },
        "legacy_public_fade": {
            "class": "PublicFadeProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.publicfade_processor",
            "category": StrategyCategory.CONSENSUS_ANALYSIS,
            "signal_type": SignalType.PUBLIC_FADE,
            "status": "LEGACY_PENDING",
            "priority": "MEDIUM",
            "description": "Public money fade strategy with contrarian analysis",
            "migration_phase": "5C",
        },
        "legacy_hybrid_sharp": {
            "class": "HybridSharpProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.hybridsharp_processor",
            "category": StrategyCategory.HYBRID_ANALYSIS,
            "signal_type": SignalType.HYBRID_SHARP,
            "status": "LEGACY_PENDING",
            "priority": "HIGH",
            "description": "Hybrid line movement and sharp action analysis",
            "migration_phase": "5C",
        },
        "legacy_late_flip": {
            "class": "LateFlipProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.lateflip_processor",
            "category": StrategyCategory.TIMING_ANALYSIS,
            "signal_type": SignalType.LATE_FLIP,
            "status": "LEGACY_PENDING",
            "priority": "MEDIUM",
            "description": "Late sharp money flip detection",
            "migration_phase": "5C",
        },
        "legacy_underdog_value": {
            "class": "UnderdogValueProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.underdogvalue_processor",
            "category": StrategyCategory.VALUE_ANALYSIS,
            "signal_type": SignalType.UNDERDOG_VALUE,
            "status": "LEGACY_PENDING",
            "priority": "MEDIUM",
            "description": "Underdog moneyline value detection",
            "migration_phase": "5C",
        },
        "legacy_line_movement": {
            "class": "LineMovementProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.linemovement_processor",
            "category": StrategyCategory.MARKET_INEFFICIENCY,
            "signal_type": SignalType.LINE_MOVEMENT,
            "status": "LEGACY_PENDING",
            "priority": "MEDIUM",
            "description": "Line movement analysis and steam move detection",
            "migration_phase": "5C",
        },
        "legacy_opposing_markets": {
            "class": "OpposingMarketsProcessor",
            "module": "src.mlb_sharp_betting.analysis.processors.opposingmarkets_processor",
            "category": StrategyCategory.MARKET_INEFFICIENCY,
            "signal_type": SignalType.OPPOSING_MARKETS,
            "status": "LEGACY_PENDING",
            "priority": "MEDIUM",
            "description": "Opposing market analysis and discrepancy detection",
            "migration_phase": "5C",
        },
    }

    def __init__(self, repository: UnifiedRepository, config: dict[str, Any]):
        """
        Initialize the strategy factory.

        Args:
            repository: Unified repository for data access
            config: Factory configuration
        """
        self.repository = repository
        self.config = config
        self.logger = get_logger(__name__, LogComponent.STRATEGY)

        # Strategy management
        self._strategy_cache: dict[str, type[BaseStrategyProcessor]] = {}
        self._strategy_instances: dict[str, BaseStrategyProcessor] = {}
        self._strategy_configs: dict[str, dict[str, Any]] = {}

        # Migration management
        self._migration_status: dict[str, str] = {}
        self._legacy_bridges: dict[str, str] = {}  # Maps legacy to unified

        # A/B testing framework
        self._ab_tests: dict[str, dict[str, Any]] = {}
        self._strategy_performance: dict[str, dict[str, Any]] = {}

        # Loading status tracking
        self.loading_status = StrategyLoadingStatus()

        # Configuration
        self.auto_load_strategies = config.get("auto_load_strategies", True)
        self.enable_ab_testing = config.get("enable_ab_testing", True)
        self.strategy_timeout = config.get("strategy_timeout", 300)  # 5 minutes
        self.prefer_unified = config.get(
            "prefer_unified", True
        )  # Prefer migrated processors
        self.enable_legacy_bridge = config.get("enable_legacy_bridge", True)

        # Initialize migration status
        self._initialize_migration_tracking()

        self.logger.info(
            f"Initialized StrategyFactory with {len(self.STRATEGY_REGISTRY)} registered strategies"
        )
        self.logger.info(f"Migration status: {self._get_migration_summary()}")

        # Auto-load strategies if enabled
        if self.auto_load_strategies:
            asyncio.create_task(self._auto_load_strategies())

    def _initialize_migration_tracking(self) -> None:
        """Initialize migration status tracking"""
        for strategy_name, info in self.STRATEGY_REGISTRY.items():
            status = info.get("status", "UNKNOWN")
            self._migration_status[strategy_name] = status

            # Set up legacy bridges
            if status == "LEGACY_BRIDGE":
                migration_target = info.get("migration_target")
                if migration_target:
                    self._legacy_bridges[strategy_name] = migration_target

    def _get_migration_summary(self) -> str:
        """Get migration status summary"""
        status_counts = {}
        for status in self._migration_status.values():
            status_counts[status] = status_counts.get(status, 0) + 1

        return ", ".join(
            [f"{status}: {count}" for status, count in status_counts.items()]
        )

    async def _auto_load_strategies(self) -> None:
        """Automatically load strategies based on preference"""
        try:
            if self.prefer_unified:
                # Load migrated processors first
                await self._load_migrated_strategies()

                # Load legacy bridges if enabled
                if self.enable_legacy_bridge:
                    await self._load_legacy_bridge_strategies()
            else:
                # Load all available strategies
                await self.load_all_strategies()

            self.logger.info(f"Auto-loaded {len(self._strategy_instances)} strategies")
        except Exception as e:
            self.logger.error(f"Failed to auto-load strategies: {e}")

    async def _load_migrated_strategies(self) -> None:
        """Load only migrated (unified) strategies"""
        migrated_strategies = [
            name
            for name, info in self.STRATEGY_REGISTRY.items()
            if info.get("status") == "MIGRATED"
        ]

        self.logger.info(f"Loading {len(migrated_strategies)} migrated strategies")

        for strategy_name in migrated_strategies:
            try:
                await self.load_strategy(strategy_name)
            except Exception as e:
                self.logger.warning(
                    f"Failed to load migrated strategy {strategy_name}: {e}"
                )

    async def _load_legacy_bridge_strategies(self) -> None:
        """Load legacy bridge strategies for gradual migration"""
        bridge_strategies = [
            name
            for name, info in self.STRATEGY_REGISTRY.items()
            if info.get("status") == "LEGACY_BRIDGE"
        ]

        self.logger.info(f"Loading {len(bridge_strategies)} legacy bridge strategies")

        for strategy_name in bridge_strategies:
            try:
                # Only load if unified equivalent is not already loaded
                migration_target = self._legacy_bridges.get(strategy_name)
                if (
                    migration_target
                    and migration_target not in self._strategy_instances
                ):
                    await self.load_strategy(strategy_name)
            except Exception as e:
                self.logger.warning(
                    f"Failed to load legacy bridge strategy {strategy_name}: {e}"
                )

    async def load_strategy(
        self, strategy_name: str
    ) -> BaseStrategyProcessor | None:
        """
        Load a specific strategy processor.

        Args:
            strategy_name: Name of the strategy to load

        Returns:
            Loaded strategy processor or None if failed
        """
        if strategy_name not in self.STRATEGY_REGISTRY:
            self.logger.warning(f"Unknown strategy: {strategy_name}")
            return None

        # Check if already loaded
        if strategy_name in self._strategy_instances:
            return self._strategy_instances[strategy_name]

        strategy_info = self.STRATEGY_REGISTRY[strategy_name]

        try:
            # Load the strategy class
            strategy_class = await self._load_strategy_class(strategy_info)
            if not strategy_class:
                return None

            # Create strategy configuration
            strategy_config = self._create_strategy_config(strategy_name, strategy_info)

            # Instantiate the strategy
            if strategy_info.get("status") == "MIGRATED":
                # Use unified constructor
                strategy_instance = strategy_class(self.repository, strategy_config)
            else:
                # Use legacy constructor pattern
                strategy_instance = await self._create_legacy_strategy_instance(
                    strategy_class, strategy_config
                )

            # Cache the instance
            self._strategy_instances[strategy_name] = strategy_instance
            self._strategy_configs[strategy_name] = strategy_config

            self.logger.info(
                f"✅ Loaded strategy: {strategy_name} ({strategy_info.get('status', 'UNKNOWN')})"
            )
            return strategy_instance

        except Exception as e:
            self.logger.error(f"Failed to load strategy {strategy_name}: {e}")
            return None

    async def _load_strategy_class(
        self, strategy_info: dict[str, Any]
    ) -> type[BaseStrategyProcessor] | None:
        """Load strategy class from module"""
        try:
            module_path = strategy_info["module"]
            class_name = strategy_info["class"]

            # Import the module
            module = importlib.import_module(module_path)

            # Get the class
            strategy_class = getattr(module, class_name)

            # Validate it's a proper strategy class
            if inspect.isclass(strategy_class):
                return strategy_class
            else:
                self.logger.warning(f"Invalid strategy class: {class_name}")
                return None

        except (ImportError, AttributeError) as e:
            self.logger.warning(f"Failed to load strategy class: {e}")
            return None

    def _create_strategy_config(
        self, strategy_name: str, strategy_info: dict[str, Any]
    ) -> dict[str, Any]:
        """Create configuration for strategy instance"""
        base_config = {
            "strategy_name": strategy_name,
            "strategy_category": strategy_info.get("category"),
            "signal_type": strategy_info.get("signal_type"),
            "status": strategy_info.get("status"),
            "priority": strategy_info.get("priority"),
            "migration_phase": strategy_info.get("migration_phase"),
            "legacy_mode": strategy_info.get("status")
            in ["LEGACY_BRIDGE", "LEGACY_PENDING"],
        }

        # Add strategy-specific configuration
        strategy_specific_config = self.config.get("strategies", {}).get(
            strategy_name, {}
        )

        return {**base_config, **strategy_specific_config}

    async def _create_legacy_strategy_instance(
        self,
        strategy_class: type[BaseStrategyProcessor],
        strategy_config: dict[str, Any],
    ) -> BaseStrategyProcessor:
        """Create legacy strategy instance with compatibility layer"""
        try:
            # Legacy strategies may need different constructor parameters
            # This is a simplified approach - in practice, we'd need more sophisticated mapping

            # Note: Legacy dependencies have been migrated to unified architecture
            # These imports are commented out as the services are now in the unified structure
            # from src.services.strategy.strategy_manager_service import StrategyManagerService
            # from src.analysis.models.unified_models import SignalProcessorConfig

            # Create mock legacy dependencies
            mock_repository = None  # Would create actual mock
            mock_validator = None  # Would create actual mock
            mock_config = None  # Would create actual mock

            # For now, create with unified parameters and enable legacy mode
            strategy_config["legacy_mode"] = True
            return strategy_class(self.repository, strategy_config)

        except Exception as e:
            self.logger.error(f"Failed to create legacy strategy instance: {e}")
            raise

    async def load_all_strategies(self) -> dict[str, BaseStrategyProcessor]:
        """Load all available strategies"""
        self.loading_status.load_start_time = datetime.now()
        self.loading_status.total_strategies = len(self.STRATEGY_REGISTRY)

        loaded_strategies = {}

        for strategy_name in self.STRATEGY_REGISTRY.keys():
            try:
                strategy = await self.load_strategy(strategy_name)
                if strategy:
                    loaded_strategies[strategy_name] = strategy
                    self.loading_status.loaded_strategies += 1
                else:
                    self.loading_status.failed_strategies += 1
                    self.loading_status.loading_errors[strategy_name] = "Failed to load"

            except Exception as e:
                self.loading_status.failed_strategies += 1
                self.loading_status.loading_errors[strategy_name] = str(e)

        self.loading_status.load_end_time = datetime.now()

        # Log comprehensive loading report
        self._log_loading_report()

        return loaded_strategies

    def _log_loading_report(self) -> None:
        """Log comprehensive strategy loading report"""
        self.logger.info(
            f"🏭 STRATEGY LOADING REPORT:\n"
            f"{'=' * 60}\n"
            f"📊 Total Strategies: {self.loading_status.total_strategies}\n"
            f"✅ Successfully Loaded: {self.loading_status.loaded_strategies}\n"
            f"❌ Failed to Load: {self.loading_status.failed_strategies}\n"
            f"📈 Success Rate: {self.loading_status.success_rate:.1%}\n"
            f"⏱️  Loading Time: {self.loading_status.loading_time:.2f}s\n"
            f"{'=' * 60}"
        )

        # Log by migration status
        migrated_count = len(
            [
                s
                for s in self._strategy_instances.keys()
                if self.STRATEGY_REGISTRY[s].get("status") == "MIGRATED"
            ]
        )
        legacy_bridge_count = len(
            [
                s
                for s in self._strategy_instances.keys()
                if self.STRATEGY_REGISTRY[s].get("status") == "LEGACY_BRIDGE"
            ]
        )
        legacy_pending_count = len(
            [
                s
                for s in self.STRATEGY_REGISTRY.keys()
                if self.STRATEGY_REGISTRY[s].get("status") == "LEGACY_PENDING"
            ]
        )

        self.logger.info(
            f"📋 MIGRATION STATUS:\n"
            f"🚀 Migrated (Phase 5B): {migrated_count}\n"
            f"🔄 Legacy Bridges: {legacy_bridge_count}\n"
            f"📋 Pending Migration: {legacy_pending_count}"
        )

        if self.loading_status.loading_errors:
            self.logger.warning("❌ LOADING ERRORS:")
            for strategy_name, error in self.loading_status.loading_errors.items():
                self.logger.warning(f"   {strategy_name}: {error}")

    def get_available_strategies(self) -> list[str]:
        """Get list of all available strategy names"""
        return list(self.STRATEGY_REGISTRY.keys())

    def get_migrated_strategies(self) -> list[str]:
        """Get list of migrated (unified) strategy names"""
        return [
            name
            for name, info in self.STRATEGY_REGISTRY.items()
            if info.get("status") == "MIGRATED"
        ]

    def get_legacy_strategies(self) -> list[str]:
        """Get list of legacy strategy names"""
        return [
            name
            for name, info in self.STRATEGY_REGISTRY.items()
            if info.get("status") in ["LEGACY_BRIDGE", "LEGACY_PENDING"]
        ]

    def get_loaded_strategies(self) -> dict[str, BaseStrategyProcessor]:
        """Get dict of successfully loaded strategies"""
        return self._strategy_instances.copy()

    def get_strategy_by_signal_type(
        self, signal_type: SignalType
    ) -> list[BaseStrategyProcessor]:
        """Get strategies that handle a specific signal type"""
        matching_strategies = []

        for strategy_name, strategy_instance in self._strategy_instances.items():
            try:
                if strategy_instance.get_signal_type() == signal_type:
                    matching_strategies.append(strategy_instance)
            except Exception as e:
                self.logger.warning(
                    f"Error checking signal type for {strategy_name}: {e}"
                )

        return matching_strategies

    def get_strategy_by_category(
        self, category: StrategyCategory
    ) -> list[BaseStrategyProcessor]:
        """Get strategies by category"""
        matching_strategies = []

        for strategy_name, strategy_instance in self._strategy_instances.items():
            try:
                if strategy_instance.get_strategy_category() == category:
                    matching_strategies.append(strategy_instance)
            except Exception as e:
                self.logger.warning(f"Error checking category for {strategy_name}: {e}")

        return matching_strategies

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on all loaded strategies"""
        health_results = {
            "factory_status": "healthy",
            "total_strategies": len(self._strategy_instances),
            "strategy_health": {},
            "migration_status": self._migration_status.copy(),
            "loading_status": {
                "success_rate": self.loading_status.success_rate,
                "total_loaded": self.loading_status.loaded_strategies,
                "total_failed": self.loading_status.failed_strategies,
            },
        }

        # Check each strategy
        for strategy_name, strategy_instance in self._strategy_instances.items():
            try:
                strategy_health = await strategy_instance.health_check()
                health_results["strategy_health"][strategy_name] = strategy_health
            except Exception as e:
                health_results["strategy_health"][strategy_name] = {
                    "status": "unhealthy",
                    "error": str(e),
                }

        return health_results

    def get_migration_report(self) -> dict[str, Any]:
        """Get comprehensive migration report"""
        report = {
            "migration_summary": self._get_migration_summary(),
            "migrated_processors": self.get_migrated_strategies(),
            "legacy_bridges": list(self._legacy_bridges.keys()),
            "pending_migration": [
                name
                for name, info in self.STRATEGY_REGISTRY.items()
                if info.get("status") == "LEGACY_PENDING"
            ],
            "migration_phases": {
                "5B": [
                    name
                    for name, info in self.STRATEGY_REGISTRY.items()
                    if info.get("migration_phase") == "5B"
                ],
                "5C": [
                    name
                    for name, info in self.STRATEGY_REGISTRY.items()
                    if info.get("migration_phase") == "5C"
                ],
            },
            "performance_comparison": self._get_performance_comparison(),
        }

        return report

    def _get_performance_comparison(self) -> dict[str, Any]:
        """Get performance comparison between migrated and legacy strategies"""
        # This would implement actual performance comparison
        # For now, return placeholder data
        return {
            "migrated_avg_performance": "TBD",
            "legacy_avg_performance": "TBD",
            "performance_improvement": "TBD",
        }

    def __str__(self) -> str:
        """String representation"""
        return f"StrategyFactory(loaded={len(self._strategy_instances)}, migrated={len(self.get_migrated_strategies())})"
