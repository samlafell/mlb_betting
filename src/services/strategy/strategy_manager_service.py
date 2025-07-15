#!/usr/bin/env python3
"""
Strategy Manager Service

Migrated and enhanced strategy management functionality from the legacy module.
Provides comprehensive strategy lifecycle management, configuration, validation,
and performance tracking with integration to the unified architecture.

Legacy Source: src/mlb_sharp_betting/services/strategy_manager.py
Enhanced Features:
- Unified architecture integration
- Enhanced strategy lifecycle management
- Improved performance tracking and analytics
- Better configuration management
- Advanced validation and quality assurance

Part of Phase 5D: Critical Business Logic Migration
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ...core.config import get_settings
from ...core.exceptions import StrategyError, ValidationError
from ...core.logging import get_logger

logger = get_logger(__name__)


class StrategyStatus(str, Enum):
    """Strategy status enumeration."""

    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"
    PROBATION = "probation"
    QUARANTINE = "quarantine"
    TESTING = "testing"


class PerformanceTrend(str, Enum):
    """Performance trend enumeration."""

    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class StrategyType(str, Enum):
    """Strategy type enumeration."""

    SHARP_ACTION = "sharp_action"
    CONSENSUS = "consensus"
    TIMING_BASED = "timing_based"
    UNDERDOG_VALUE = "underdog_value"
    PUBLIC_FADE = "public_fade"
    CUSTOM = "custom"


class SignalType(str, Enum):
    """Signal type enumeration."""

    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"
    PROPS = "props"


@dataclass
class StrategyConfiguration:
    """Enhanced strategy configuration with comprehensive settings."""

    strategy_id: str
    strategy_name: str
    strategy_type: StrategyType
    signal_type: SignalType

    # Status and enablement
    status: StrategyStatus = StrategyStatus.TESTING
    is_enabled: bool = False

    # Performance metrics
    win_rate: float = 0.0
    roi_percentage: float = 0.0
    total_bets: int = 0
    winning_bets: int = 0
    losing_bets: int = 0

    # Configuration parameters
    confidence_threshold: float = 0.6
    confidence_multiplier: float = 1.0
    weight_in_ensemble: float = 1.0
    max_recommendations_per_day: int = 10

    # Thresholds
    min_threshold: float = 0.5
    moderate_threshold: float = 0.7
    high_threshold: float = 0.8

    # Risk management
    max_drawdown_percentage: float = 0.0
    consecutive_losses: int = 0
    max_consecutive_losses: int = 0

    # Performance tracking
    performance_trend: PerformanceTrend = PerformanceTrend.UNKNOWN
    last_performance_update: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update_performance(self, win_rate: float, roi: float, total_bets: int):
        """Update performance metrics."""
        self.win_rate = win_rate
        self.roi_percentage = roi
        self.total_bets = total_bets
        self.last_performance_update = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def calculate_performance_trend(
        self, historical_performance: list[float]
    ) -> PerformanceTrend:
        """Calculate performance trend based on historical data."""
        if len(historical_performance) < 3:
            return PerformanceTrend.UNKNOWN

        recent_avg = sum(historical_performance[-3:]) / 3
        older_avg = (
            sum(historical_performance[:-3]) / len(historical_performance[:-3])
            if len(historical_performance) > 3
            else recent_avg
        )

        if recent_avg > older_avg * 1.1:
            return PerformanceTrend.IMPROVING
        elif recent_avg < older_avg * 0.9:
            return PerformanceTrend.DECLINING
        elif recent_avg < older_avg * 0.7:
            return PerformanceTrend.CRITICAL
        else:
            return PerformanceTrend.STABLE


@dataclass
class StrategyMetrics:
    """Comprehensive metrics for strategy management."""

    total_strategies: int = 0
    active_strategies: int = 0
    paused_strategies: int = 0
    deprecated_strategies: int = 0
    testing_strategies: int = 0

    total_signals_generated: int = 0
    successful_signals: int = 0
    failed_signals: int = 0

    average_win_rate: float = 0.0
    average_roi: float = 0.0
    total_profit: float = 0.0

    strategies_auto_disabled: int = 0
    strategies_promoted: int = 0
    strategies_demoted: int = 0

    last_performance_update: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def increment(self, metric: str, value: int = 1):
        """Increment a metric counter."""
        if hasattr(self, metric):
            current_value = getattr(self, metric)
            setattr(self, metric, current_value + value)

    def update(self, metric: str, value: Any):
        """Update a metric value."""
        if hasattr(self, metric):
            setattr(self, metric, value)

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }


@dataclass
class StrategyManagerConfig:
    """Configuration for strategy manager."""

    # Performance thresholds
    min_win_rate_threshold: float = 0.50
    min_roi_threshold: float = 0.0
    auto_disable_roi_threshold: float = -10.0
    probation_roi_threshold: float = -5.0

    # Sample size requirements
    min_sample_size: int = 5
    robust_sample_size: int = 50

    # Update intervals
    performance_update_interval_minutes: int = 15
    cache_refresh_interval_minutes: int = 5

    # Lifecycle management
    max_consecutive_losses: int = 8
    grace_period_days: int = 7
    performance_window_days: int = 14

    # Ensemble settings
    enable_ensemble_logic: bool = True
    max_strategies_in_ensemble: int = 5

    # Validation settings
    enable_validation: bool = True
    enable_performance_monitoring: bool = True


class StrategyManagerService:
    """
    Strategy Manager Service

    Provides comprehensive strategy lifecycle management including configuration,
    validation, performance tracking, and automatic optimization.

    Features:
    - Strategy lifecycle management (testing -> active -> deprecated)
    - Performance monitoring and trend analysis
    - Automatic strategy enabling/disabling based on performance
    - Ensemble strategy coordination
    - Configuration management and validation
    - Integration with unified architecture
    """

    def __init__(self, config: StrategyManagerConfig | None = None):
        """Initialize the strategy manager service."""
        self.config = config or StrategyManagerConfig()
        self.settings = get_settings()
        self.logger = logger.bind(service="StrategyManagerService")

        # State management
        self.metrics = StrategyMetrics()
        self.strategies: dict[str, StrategyConfiguration] = {}
        self.performance_history: dict[str, list[float]] = {}

        # Cache management
        self.cache_last_updated = datetime.now(timezone.utc)
        self.performance_last_updated = datetime.now(timezone.utc)

        self.logger.info(
            "StrategyManagerService initialized",
            min_win_rate=self.config.min_win_rate_threshold,
            min_roi=self.config.min_roi_threshold,
            auto_disable_threshold=self.config.auto_disable_roi_threshold,
        )

    async def get_active_strategies(
        self, strategy_type: StrategyType | None = None
    ) -> list[StrategyConfiguration]:
        """
        Get all active strategies, optionally filtered by type.

        Args:
            strategy_type: Optional strategy type filter

        Returns:
            List of active strategy configurations
        """
        await self._refresh_cache_if_needed()

        active_strategies = [
            strategy
            for strategy in self.strategies.values()
            if strategy.status == StrategyStatus.ACTIVE and strategy.is_enabled
        ]

        if strategy_type:
            active_strategies = [
                strategy
                for strategy in active_strategies
                if strategy.strategy_type == strategy_type
            ]

        self.logger.debug(
            "Retrieved active strategies",
            total_count=len(active_strategies),
            strategy_type=strategy_type.value if strategy_type else "all",
        )

        return active_strategies

    async def get_strategy_by_id(
        self, strategy_id: str
    ) -> StrategyConfiguration | None:
        """
        Get a strategy by its ID.

        Args:
            strategy_id: Strategy identifier

        Returns:
            Strategy configuration or None if not found
        """
        await self._refresh_cache_if_needed()
        return self.strategies.get(strategy_id)

    async def get_strategy_by_name(
        self, strategy_name: str
    ) -> StrategyConfiguration | None:
        """
        Get a strategy by its name.

        Args:
            strategy_name: Strategy name

        Returns:
            Strategy configuration or None if not found
        """
        await self._refresh_cache_if_needed()

        for strategy in self.strategies.values():
            if strategy.strategy_name == strategy_name:
                return strategy

        return None

    async def create_strategy(
        self, strategy_config: dict[str, Any]
    ) -> StrategyConfiguration:
        """
        Create a new strategy configuration.

        Args:
            strategy_config: Strategy configuration dictionary

        Returns:
            Created strategy configuration

        Raises:
            ValidationError: If configuration is invalid
            StrategyError: If strategy creation fails
        """
        try:
            # Validate required fields
            required_fields = ["strategy_name", "strategy_type", "signal_type"]
            for field in required_fields:
                if field not in strategy_config:
                    raise ValidationError(f"Missing required field: {field}")

            # Create strategy configuration
            strategy = StrategyConfiguration(
                strategy_id=str(uuid.uuid4()),
                strategy_name=strategy_config["strategy_name"],
                strategy_type=StrategyType(strategy_config["strategy_type"]),
                signal_type=SignalType(strategy_config["signal_type"]),
                **{
                    k: v
                    for k, v in strategy_config.items()
                    if k not in ["strategy_name", "strategy_type", "signal_type"]
                },
            )

            # Store strategy
            await self._store_strategy(strategy)

            # Update cache
            self.strategies[strategy.strategy_id] = strategy
            self.metrics.increment("total_strategies")
            self.metrics.increment("testing_strategies")

            self.logger.info(
                "Strategy created successfully",
                strategy_id=strategy.strategy_id,
                strategy_name=strategy.strategy_name,
                strategy_type=strategy.strategy_type.value,
            )

            return strategy

        except Exception as e:
            self.logger.error(
                "Failed to create strategy",
                strategy_name=strategy_config.get("strategy_name"),
                error=str(e),
            )
            raise StrategyError(f"Strategy creation failed: {str(e)}") from e

    async def update_strategy_performance(
        self, strategy_id: str, performance_data: dict[str, Any]
    ) -> bool:
        """
        Update strategy performance metrics.

        Args:
            strategy_id: Strategy identifier
            performance_data: Performance data dictionary

        Returns:
            True if update successful, False otherwise
        """
        try:
            strategy = await self.get_strategy_by_id(strategy_id)
            if not strategy:
                self.logger.warning(
                    "Strategy not found for performance update", strategy_id=strategy_id
                )
                return False

            # Update performance metrics
            win_rate = performance_data.get("win_rate", strategy.win_rate)
            roi = performance_data.get("roi_percentage", strategy.roi_percentage)
            total_bets = performance_data.get("total_bets", strategy.total_bets)

            strategy.update_performance(win_rate, roi, total_bets)

            # Update performance history
            if strategy_id not in self.performance_history:
                self.performance_history[strategy_id] = []

            self.performance_history[strategy_id].append(roi)

            # Keep only recent history (last 20 data points)
            if len(self.performance_history[strategy_id]) > 20:
                self.performance_history[strategy_id] = self.performance_history[
                    strategy_id
                ][-20:]

            # Calculate performance trend
            strategy.performance_trend = strategy.calculate_performance_trend(
                self.performance_history[strategy_id]
            )

            # Check if strategy should be auto-disabled
            await self._check_auto_disable_conditions(strategy)

            # Store updated strategy
            await self._store_strategy(strategy)

            self.logger.info(
                "Strategy performance updated",
                strategy_id=strategy_id,
                win_rate=win_rate,
                roi=roi,
                total_bets=total_bets,
                trend=strategy.performance_trend.value,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to update strategy performance",
                strategy_id=strategy_id,
                error=str(e),
            )
            return False

    async def enable_strategy(self, strategy_id: str) -> bool:
        """
        Enable a strategy for active use.

        Args:
            strategy_id: Strategy identifier

        Returns:
            True if enabled successfully, False otherwise
        """
        try:
            strategy = await self.get_strategy_by_id(strategy_id)
            if not strategy:
                return False

            # Validate strategy before enabling
            if self.config.enable_validation:
                validation_result = await self._validate_strategy_for_activation(
                    strategy
                )
                if not validation_result["valid"]:
                    self.logger.warning(
                        "Strategy validation failed for enablement",
                        strategy_id=strategy_id,
                        reasons=validation_result["reasons"],
                    )
                    return False

            # Enable strategy
            strategy.is_enabled = True
            strategy.status = StrategyStatus.ACTIVE
            strategy.updated_at = datetime.now(timezone.utc)

            # Store updated strategy
            await self._store_strategy(strategy)

            # Update metrics
            self.metrics.increment("active_strategies")
            if strategy.status != StrategyStatus.ACTIVE:
                self.metrics.increment("strategies_promoted")

            self.logger.info(
                "Strategy enabled successfully",
                strategy_id=strategy_id,
                strategy_name=strategy.strategy_name,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to enable strategy", strategy_id=strategy_id, error=str(e)
            )
            return False

    async def disable_strategy(self, strategy_id: str, reason: str = "manual") -> bool:
        """
        Disable a strategy.

        Args:
            strategy_id: Strategy identifier
            reason: Reason for disabling

        Returns:
            True if disabled successfully, False otherwise
        """
        try:
            strategy = await self.get_strategy_by_id(strategy_id)
            if not strategy:
                return False

            # Disable strategy
            strategy.is_enabled = False
            strategy.status = StrategyStatus.PAUSED
            strategy.updated_at = datetime.now(timezone.utc)

            # Store updated strategy
            await self._store_strategy(strategy)

            # Update metrics
            if strategy.status == StrategyStatus.ACTIVE:
                self.metrics.increment("active_strategies", -1)
                self.metrics.increment("paused_strategies")

            if reason == "auto":
                self.metrics.increment("strategies_auto_disabled")

            self.logger.info(
                "Strategy disabled",
                strategy_id=strategy_id,
                strategy_name=strategy.strategy_name,
                reason=reason,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to disable strategy", strategy_id=strategy_id, error=str(e)
            )
            return False

    async def get_strategy_performance_summary(self) -> dict[str, Any]:
        """
        Get comprehensive performance summary for all strategies.

        Returns:
            Performance summary dictionary
        """
        await self._refresh_cache_if_needed()

        # Calculate aggregate metrics
        total_strategies = len(self.strategies)
        active_strategies = len(
            [s for s in self.strategies.values() if s.status == StrategyStatus.ACTIVE]
        )

        if total_strategies == 0:
            return {
                "total_strategies": 0,
                "active_strategies": 0,
                "average_win_rate": 0.0,
                "average_roi": 0.0,
                "strategy_breakdown": {},
            }

        # Calculate averages
        total_win_rate = sum(s.win_rate for s in self.strategies.values())
        total_roi = sum(s.roi_percentage for s in self.strategies.values())

        average_win_rate = total_win_rate / total_strategies
        average_roi = total_roi / total_strategies

        # Strategy breakdown by status
        status_breakdown = {}
        for status in StrategyStatus:
            count = len([s for s in self.strategies.values() if s.status == status])
            status_breakdown[status.value] = count

        # Top performing strategies
        top_strategies = sorted(
            self.strategies.values(), key=lambda s: s.roi_percentage, reverse=True
        )[:5]

        return {
            "total_strategies": total_strategies,
            "active_strategies": active_strategies,
            "average_win_rate": average_win_rate,
            "average_roi": average_roi,
            "status_breakdown": status_breakdown,
            "top_strategies": [
                {
                    "strategy_id": s.strategy_id,
                    "strategy_name": s.strategy_name,
                    "win_rate": s.win_rate,
                    "roi_percentage": s.roi_percentage,
                    "total_bets": s.total_bets,
                }
                for s in top_strategies
            ],
            "metrics": self.metrics.to_dict(),
        }

    async def run_performance_monitoring(self) -> dict[str, Any]:
        """
        Run comprehensive performance monitoring for all strategies.

        Returns:
            Monitoring results dictionary
        """
        if not self.config.enable_performance_monitoring:
            return {"monitoring_disabled": True}

        monitoring_results = {
            "strategies_checked": 0,
            "strategies_auto_disabled": 0,
            "strategies_promoted": 0,
            "performance_alerts": [],
        }

        try:
            await self._refresh_cache_if_needed()

            for strategy in self.strategies.values():
                monitoring_results["strategies_checked"] += 1

                # Check auto-disable conditions
                disabled = await self._check_auto_disable_conditions(strategy)
                if disabled:
                    monitoring_results["strategies_auto_disabled"] += 1

                # Check for performance alerts
                if strategy.performance_trend == PerformanceTrend.CRITICAL:
                    monitoring_results["performance_alerts"].append(
                        {
                            "strategy_id": strategy.strategy_id,
                            "strategy_name": strategy.strategy_name,
                            "alert_type": "critical_performance",
                            "win_rate": strategy.win_rate,
                            "roi_percentage": strategy.roi_percentage,
                        }
                    )

            self.performance_last_updated = datetime.now(timezone.utc)

            self.logger.info(
                "Performance monitoring completed",
                strategies_checked=monitoring_results["strategies_checked"],
                auto_disabled=monitoring_results["strategies_auto_disabled"],
                alerts=len(monitoring_results["performance_alerts"]),
            )

            return monitoring_results

        except Exception as e:
            self.logger.error("Performance monitoring failed", error=str(e))
            return {"error": str(e)}

    # Private helper methods

    async def _refresh_cache_if_needed(self):
        """Refresh strategy cache if needed."""
        now = datetime.now(timezone.utc)
        if (now - self.cache_last_updated).total_seconds() > (
            self.config.cache_refresh_interval_minutes * 60
        ):
            await self._load_strategies_from_database()
            self.cache_last_updated = now

    async def _load_strategies_from_database(self):
        """Load strategies from database."""
        try:
            # This would implement actual database loading
            # For now, we'll use a placeholder
            self.logger.debug("Loading strategies from database")

            # Placeholder implementation
            await asyncio.sleep(0.01)  # Simulate database query

        except Exception as e:
            self.logger.error("Failed to load strategies from database", error=str(e))

    async def _store_strategy(self, strategy: StrategyConfiguration):
        """Store strategy configuration in database."""
        try:
            # This would implement actual database storage
            # For now, we'll use a placeholder
            self.logger.debug(
                "Storing strategy to database", strategy_id=strategy.strategy_id
            )

            # Placeholder implementation
            await asyncio.sleep(0.01)  # Simulate database write

        except Exception as e:
            self.logger.error(
                "Failed to store strategy",
                strategy_id=strategy.strategy_id,
                error=str(e),
            )
            raise

    async def _validate_strategy_for_activation(
        self, strategy: StrategyConfiguration
    ) -> dict[str, Any]:
        """Validate strategy before activation."""
        validation_result = {"valid": True, "reasons": []}

        # Check minimum sample size
        if strategy.total_bets < self.config.min_sample_size:
            validation_result["valid"] = False
            validation_result["reasons"].append(
                f"Insufficient sample size: {strategy.total_bets} < {self.config.min_sample_size}"
            )

        # Check performance thresholds
        if strategy.win_rate < self.config.min_win_rate_threshold:
            validation_result["valid"] = False
            validation_result["reasons"].append(
                f"Win rate below threshold: {strategy.win_rate:.2%} < {self.config.min_win_rate_threshold:.2%}"
            )

        if strategy.roi_percentage < self.config.min_roi_threshold:
            validation_result["valid"] = False
            validation_result["reasons"].append(
                f"ROI below threshold: {strategy.roi_percentage:.2f}% < {self.config.min_roi_threshold:.2f}%"
            )

        return validation_result

    async def _check_auto_disable_conditions(
        self, strategy: StrategyConfiguration
    ) -> bool:
        """Check if strategy should be auto-disabled."""
        if not strategy.is_enabled:
            return False

        # Hard ROI threshold
        if strategy.roi_percentage < self.config.auto_disable_roi_threshold:
            await self.disable_strategy(strategy.strategy_id, "auto")
            self.logger.warning(
                "Strategy auto-disabled due to poor ROI",
                strategy_id=strategy.strategy_id,
                roi=strategy.roi_percentage,
                threshold=self.config.auto_disable_roi_threshold,
            )
            return True

        # Conditional thresholds
        if (strategy.win_rate < 0.50 and strategy.roi_percentage < 0.0) or (
            strategy.win_rate < 0.55 and strategy.roi_percentage < -5.0
        ):
            await self.disable_strategy(strategy.strategy_id, "auto")
            self.logger.warning(
                "Strategy auto-disabled due to poor performance",
                strategy_id=strategy.strategy_id,
                win_rate=strategy.win_rate,
                roi=strategy.roi_percentage,
            )
            return True

        # Consecutive losses
        if strategy.consecutive_losses >= self.config.max_consecutive_losses:
            await self.disable_strategy(strategy.strategy_id, "auto")
            self.logger.warning(
                "Strategy auto-disabled due to consecutive losses",
                strategy_id=strategy.strategy_id,
                consecutive_losses=strategy.consecutive_losses,
            )
            return True

        return False

    def get_metrics(self) -> dict[str, Any]:
        """Get comprehensive strategy manager metrics."""
        return self.metrics.to_dict()


# Service instance for easy importing
strategy_manager_service = StrategyManagerService()


# Convenience functions
async def get_active_strategies(
    strategy_type: StrategyType | None = None,
) -> list[StrategyConfiguration]:
    """Convenience function to get active strategies."""
    return await strategy_manager_service.get_active_strategies(strategy_type)


async def create_strategy(strategy_config: dict[str, Any]) -> StrategyConfiguration:
    """Convenience function to create a strategy."""
    return await strategy_manager_service.create_strategy(strategy_config)


async def get_strategy_performance_summary() -> dict[str, Any]:
    """Convenience function to get performance summary."""
    return await strategy_manager_service.get_strategy_performance_summary()


if __name__ == "__main__":
    # Example usage
    async def main():
        try:
            # Test strategy creation
            strategy_config = {
                "strategy_name": "Test Sharp Action",
                "strategy_type": "sharp_action",
                "signal_type": "moneyline",
                "confidence_threshold": 0.7,
            }

            strategy = await create_strategy(strategy_config)
            print(f"Created strategy: {strategy.strategy_name}")

            # Test performance summary
            summary = await get_strategy_performance_summary()
            print(f"Total strategies: {summary['total_strategies']}")

        except Exception as e:
            print(f"Error: {e}")

    asyncio.run(main())
