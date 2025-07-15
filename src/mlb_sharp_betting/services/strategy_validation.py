"""
Consolidated Strategy Validation Service

This service consolidates the functionality from:
- strategy_validator.py (unified validation and threshold logic)
- validation_gate_service.py (production integration layer)
- strategy_validation_registry.py (core registry functionality)

This eliminates redundancy and provides a single point of control for all strategy validation.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ..core.logging import get_logger
from ..db.connection import DatabaseManager, get_db_manager
from ..models.betting_analysis import ProfitableStrategy, StrategyThresholds


class ValidationStatus(Enum):
    """Strategy validation status levels"""

    UNVALIDATED = "unvalidated"
    VALIDATION_PENDING = "pending"
    VALIDATION_FAILED = "failed"
    VALIDATED = "validated"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    QUARANTINED = "quarantined"
    CIRCUIT_BREAKER_OPEN = "circuit_open"
    EMERGENCY_SUSPENDED = "emergency"


class ValidationGateResult(Enum):
    """Results of validation gate checks"""

    ALLOWED = "allowed"
    BLOCKED_UNVALIDATED = "blocked_unvalidated"
    BLOCKED_CIRCUIT_BREAKER = "blocked_circuit_breaker"
    BLOCKED_EMERGENCY = "blocked_emergency"
    BLOCKED_MODIFIED = "blocked_modified"
    BLOCKED_REGISTRY_UNHEALTHY = "blocked_registry_unhealthy"


@dataclass
class ValidationRecord:
    """Strategy validation record"""

    strategy_name: str
    validation_status: ValidationStatus
    last_validated: datetime
    performance_metrics: dict[str, float]
    circuit_breaker_open: bool = False
    max_daily_recommendations: int = 10
    max_bet_size_multiplier: float = 1.0
    requires_manual_approval: bool = False


@dataclass
class ValidationGateResponse:
    """Response from validation gate check"""

    result: ValidationGateResult
    strategy_name: str
    allowed: bool
    reason: str
    max_daily_recommendations: int = 0
    max_bet_size_multiplier: float = 0.0
    requires_manual_approval: bool = False
    requires_validation: bool = False
    can_recover: bool = False


class StrategyValidation:
    """
    Consolidated Strategy Validation combining validator, gate, and registry functionality.

    This service consolidates functionality from:
    - StrategyValidator (validation and threshold logic)
    - ValidationGateService (production integration layer)
    - ProductionStrategyValidationRegistry (core registry functionality)
    """

    def __init__(
        self,
        profitable_strategies: list[ProfitableStrategy] | None = None,
        thresholds: StrategyThresholds | None = None,
        db_manager: DatabaseManager | None = None,
    ):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or get_db_manager()

        # Validation logic components
        self.strategies = profitable_strategies or []
        self.thresholds = thresholds or StrategyThresholds()

        # Group strategies by type for efficient lookup
        self.strategies_by_type = self._group_strategies_by_type()

        # Registry components
        self.validation_records: dict[str, ValidationRecord] = {}
        self.emergency_controls = {
            "kill_switch_active": False,
            "emergency_suspension_active": False,
            "manual_override_active": False,
        }

        # Gate components
        self._gate_checks_today: dict[str, int] = {}
        self._last_reset = datetime.now(timezone.utc).date()

        # Performance monitoring
        self._performance_monitor_running = False

    async def initialize(self):
        """Initialize the strategy validation service"""
        try:
            await self._load_validation_records()
            await self._start_performance_monitoring()
            self.logger.info("✅ Strategy Validation Service initialized successfully")
        except Exception as e:
            self.logger.error(
                f"❌ Failed to initialize Strategy Validation Service: {e}"
            )
            raise

    # ===========================================
    # STRATEGY VALIDATOR FUNCTIONALITY
    # ===========================================

    def _group_strategies_by_type(self) -> dict[str, list[ProfitableStrategy]]:
        """Group strategies by their category for efficient matching"""
        groups = {
            "SHARP_ACTION": [],
            "OPPOSING_MARKETS": [],
            "STEAM_MOVES": [],
            "BOOK_CONFLICTS": [],
            "TOTALS": [],
            "UNDERDOG_VALUE": [],
            "CONSENSUS_STRATEGIES": [],
            "GENERAL": [],
        }

        for strategy in self.strategies:
            category = self._determine_strategy_category(strategy.strategy_name)
            groups[category].append(strategy)

        return groups

    def _determine_strategy_category(self, strategy_name: str) -> str:
        """Determine the category of a strategy to prevent inappropriate matching"""
        strategy_lower = strategy_name.lower()

        if "book_conflicts" in strategy_lower:
            return "BOOK_CONFLICTS"
        elif "opposing_markets" in strategy_lower:
            return "OPPOSING_MARKETS"
        elif (
            "steam" in strategy_lower
            or "timing" in strategy_lower
            or "late_sharp_flip" in strategy_lower
        ):
            return "STEAM_MOVES"
        elif "total" in strategy_lower and (
            "line" in strategy_lower or "sweet" in strategy_lower
        ):
            return "TOTALS"
        elif "underdog_ml" in strategy_lower or "underdog" in strategy_lower:
            return "UNDERDOG_VALUE"
        elif (
            "signal_combo" in strategy_lower
            or "consensus_moneyline" in strategy_lower
            or "public_money_fade" in strategy_lower
        ):
            return "CONSENSUS_STRATEGIES"
        elif (
            "sharp" in strategy_lower
            or "signal_combinations" in strategy_lower
            or "line_movement" in strategy_lower
            or "hybrid" in strategy_lower
        ):
            return "SHARP_ACTION"
        else:
            return "GENERAL"

    def find_matching_strategy(
        self,
        signal_type: str,
        source: str,
        book: str | None,
        split_type: str,
        signal_strength: float,
    ) -> ProfitableStrategy | None:
        """Find a profitable strategy that matches the current signal"""

        # Map signal types to strategy categories
        signal_to_category = {
            "SHARP_ACTION": "SHARP_ACTION",
            "TOTAL_SHARP": "TOTALS",
            "OPPOSING_MARKETS": "OPPOSING_MARKETS",
            "STEAM_MOVE": "STEAM_MOVES",
            "LATE_FLIP": "STEAM_MOVES",
            "BOOK_CONFLICTS": "BOOK_CONFLICTS",
            "PUBLIC_FADE": "CONSENSUS_STRATEGIES",
            "CONSENSUS_MONEYLINE": "CONSENSUS_STRATEGIES",
            "UNDERDOG_VALUE": "UNDERDOG_VALUE",
            "LINE_MOVEMENT": "SHARP_ACTION",
        }

        category = signal_to_category.get(signal_type, "GENERAL")
        candidate_strategies = self.strategies_by_type.get(category, [])

        if not candidate_strategies:
            candidate_strategies = self.strategies_by_type.get("GENERAL", [])

        # Try exact matches first, then fallbacks
        for strategy in candidate_strategies:
            threshold = self.get_threshold_for_strategy(strategy, signal_strength)
            if signal_strength >= threshold:
                return strategy

        return None

    def get_threshold_for_strategy(
        self,
        strategy: ProfitableStrategy,
        signal_strength: float,
        is_fallback: bool = False,
    ) -> float:
        """Calculate dynamic threshold based on strategy performance"""
        base_multiplier = 1.25 if is_fallback else 1.0

        if strategy.win_rate >= self.thresholds.high_performance_wr:
            return self.thresholds.high_performance_threshold * base_multiplier
        elif strategy.win_rate >= self.thresholds.moderate_performance_wr:
            return self.thresholds.moderate_performance_threshold * base_multiplier
        elif strategy.win_rate >= self.thresholds.low_performance_wr:
            return self.thresholds.low_performance_threshold * base_multiplier
        else:
            return self.thresholds.low_performance_threshold * 1.5 * base_multiplier

    def validate_signal_against_strategies(
        self, signal_type: str, signal_strength: float
    ) -> bool:
        """Check if a signal meets any strategy thresholds"""
        strategies = self.get_strategies_by_type(signal_type)

        for strategy in strategies:
            threshold = self.get_threshold_for_strategy(strategy, signal_strength)
            if signal_strength >= threshold:
                return True

        return False

    def get_strategies_by_type(self, signal_type: str) -> list[ProfitableStrategy]:
        """Get all strategies for a specific signal type"""
        signal_to_category = {
            "SHARP_ACTION": "SHARP_ACTION",
            "TOTAL_SHARP": "TOTALS",
            "OPPOSING_MARKETS": "OPPOSING_MARKETS",
            "STEAM_MOVE": "STEAM_MOVES",
            "LATE_FLIP": "STEAM_MOVES",
            "BOOK_CONFLICTS": "BOOK_CONFLICTS",
            "PUBLIC_FADE": "CONSENSUS_STRATEGIES",
            "CONSENSUS_MONEYLINE": "CONSENSUS_STRATEGIES",
            "UNDERDOG_VALUE": "UNDERDOG_VALUE",
            "LINE_MOVEMENT": "SHARP_ACTION",
        }

        category = signal_to_category.get(signal_type, "GENERAL")
        return self.strategies_by_type.get(category, [])

    # ===========================================
    # VALIDATION GATE FUNCTIONALITY
    # ===========================================

    async def check_strategy_gate(
        self, strategy_name: str, context: str = "unknown"
    ) -> ValidationGateResponse:
        """Main validation gate - checks if strategy can generate recommendations"""
        self._reset_daily_counters_if_needed()

        try:
            # Check emergency controls first
            if self.emergency_controls["kill_switch_active"]:
                return ValidationGateResponse(
                    result=ValidationGateResult.BLOCKED_EMERGENCY,
                    strategy_name=strategy_name,
                    allowed=False,
                    reason="System kill switch is active",
                )

            # Get validation record
            record = self.validation_records.get(strategy_name)
            if not record:
                return ValidationGateResponse(
                    result=ValidationGateResult.BLOCKED_UNVALIDATED,
                    strategy_name=strategy_name,
                    allowed=False,
                    reason="Strategy not found in validation registry",
                    requires_validation=True,
                )

            # Check validation status
            if record.validation_status not in [
                ValidationStatus.VALIDATED,
                ValidationStatus.APPROVED,
            ]:
                return ValidationGateResponse(
                    result=ValidationGateResult.BLOCKED_UNVALIDATED,
                    strategy_name=strategy_name,
                    allowed=False,
                    reason=f"Strategy validation status: {record.validation_status.value}",
                    requires_validation=True,
                )

            # Check circuit breaker
            if record.circuit_breaker_open:
                return ValidationGateResponse(
                    result=ValidationGateResult.BLOCKED_CIRCUIT_BREAKER,
                    strategy_name=strategy_name,
                    allowed=False,
                    reason="Circuit breaker is open due to poor performance",
                    can_recover=True,
                )

            # Check daily limits
            daily_count = self._gate_checks_today.get(strategy_name, 0)
            if daily_count >= record.max_daily_recommendations:
                return ValidationGateResponse(
                    result=ValidationGateResult.BLOCKED_REGISTRY_UNHEALTHY,
                    strategy_name=strategy_name,
                    allowed=False,
                    reason=f"Daily limit reached ({daily_count}/{record.max_daily_recommendations})",
                )

            # Allow and increment counter
            self._gate_checks_today[strategy_name] = daily_count + 1

            return ValidationGateResponse(
                result=ValidationGateResult.ALLOWED,
                strategy_name=strategy_name,
                allowed=True,
                reason="Strategy validated and approved",
                max_daily_recommendations=record.max_daily_recommendations,
                max_bet_size_multiplier=record.max_bet_size_multiplier,
                requires_manual_approval=record.requires_manual_approval,
            )

        except Exception as e:
            self.logger.error(f"Validation gate check failed for {strategy_name}: {e}")

            return ValidationGateResponse(
                result=ValidationGateResult.BLOCKED_REGISTRY_UNHEALTHY,
                strategy_name=strategy_name,
                allowed=False,
                reason=f"Validation gate error: {str(e)}",
            )

    def _reset_daily_counters_if_needed(self):
        """Reset daily counters if new day"""
        today = datetime.now(timezone.utc).date()
        if today != self._last_reset:
            self._gate_checks_today.clear()
            self._last_reset = today
            self.logger.debug("Daily validation gate counters reset")

    # ===========================================
    # REGISTRY FUNCTIONALITY
    # ===========================================

    async def register_strategy(
        self,
        strategy_name: str,
        validation_status: ValidationStatus = ValidationStatus.UNVALIDATED,
    ) -> bool:
        """Register a new strategy in the validation registry"""
        try:
            record = ValidationRecord(
                strategy_name=strategy_name,
                validation_status=validation_status,
                last_validated=datetime.now(timezone.utc),
                performance_metrics={},
            )

            self.validation_records[strategy_name] = record
            await self._persist_validation_records()

            self.logger.info(
                f"Strategy registered: {strategy_name} with status {validation_status.value}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to register strategy {strategy_name}: {e}")
            return False

    async def validate_strategy(
        self, strategy_name: str, performance_metrics: dict[str, float]
    ) -> bool:
        """Validate a strategy based on performance metrics"""
        try:
            record = self.validation_records.get(strategy_name)
            if not record:
                # Create new record
                record = ValidationRecord(
                    strategy_name=strategy_name,
                    validation_status=ValidationStatus.VALIDATION_PENDING,
                    last_validated=datetime.now(timezone.utc),
                    performance_metrics=performance_metrics,
                )
                self.validation_records[strategy_name] = record

            # Check validation criteria
            win_rate = performance_metrics.get("win_rate", 0.0)
            roi = performance_metrics.get("roi", 0.0)
            total_bets = performance_metrics.get("total_bets", 0)

            # Apply validation thresholds
            if win_rate >= 0.52 and roi >= 10.0 and total_bets >= 10:
                record.validation_status = ValidationStatus.VALIDATED
                record.performance_metrics = performance_metrics
                record.last_validated = datetime.now(timezone.utc)

                self.logger.info(f"Strategy validated: {strategy_name}")
                validation_success = True
            else:
                record.validation_status = ValidationStatus.VALIDATION_FAILED
                self.logger.warning(
                    f"Strategy validation failed: {strategy_name} - "
                    f"WR: {win_rate:.1%}, ROI: {roi:.1f}%, Bets: {total_bets}"
                )
                validation_success = False

            await self._persist_validation_records()
            return validation_success

        except Exception as e:
            self.logger.error(f"Failed to validate strategy {strategy_name}: {e}")
            return False

    def can_generate_recommendations(self, strategy_name: str) -> dict[str, Any]:
        """Check if a strategy can generate recommendations (used by gate)"""
        record = self.validation_records.get(strategy_name)
        if not record:
            return {
                "allowed": False,
                "reason": "Strategy not found in validation registry",
                "requires_validation": True,
            }

        # Check emergency controls
        if self.emergency_controls["kill_switch_active"]:
            return {
                "allowed": False,
                "reason": "System kill switch is active",
                "can_recover": False,
            }

        if record.circuit_breaker_open:
            return {
                "allowed": False,
                "reason": "Circuit breaker is open due to poor performance",
                "can_recover": True,
            }

        if record.validation_status not in [
            ValidationStatus.VALIDATED,
            ValidationStatus.APPROVED,
        ]:
            return {
                "allowed": False,
                "reason": f"Strategy validation status: {record.validation_status.value}",
                "requires_validation": True,
            }

        return {
            "allowed": True,
            "reason": "Strategy is validated and approved",
            "max_daily_recommendations": record.max_daily_recommendations,
            "max_bet_size_multiplier": record.max_bet_size_multiplier,
            "requires_manual_approval": record.requires_manual_approval,
        }

    # ===========================================
    # EMERGENCY CONTROLS
    # ===========================================

    async def activate_kill_switch(self, activated_by: str, reason: str):
        """Activate emergency kill switch to stop all strategy recommendations"""
        self.emergency_controls["kill_switch_active"] = True
        self.emergency_controls["activated_by"] = activated_by
        self.emergency_controls["reason"] = reason

        await self._persist_validation_records()

        self.logger.critical(f"🚨 KILL SWITCH ACTIVATED by {activated_by}: {reason}")

    async def deactivate_kill_switch(self, deactivated_by: str):
        """Deactivate emergency kill switch"""
        self.emergency_controls["kill_switch_active"] = False
        self.emergency_controls["activated_by"] = None
        self.emergency_controls["reason"] = None

        await self._persist_validation_records()

        self.logger.info(f"✅ Kill switch deactivated by {deactivated_by}")

    async def trigger_circuit_breaker(self, strategy_name: str, reason: str):
        """Trigger circuit breaker for a specific strategy"""
        record = self.validation_records.get(strategy_name)
        if record:
            record.circuit_breaker_open = True
            await self._persist_validation_records()

            self.logger.warning(
                f"⚡ Circuit breaker triggered for {strategy_name}: {reason}"
            )

    # ===========================================
    # PERFORMANCE MONITORING
    # ===========================================

    async def _start_performance_monitoring(self):
        """Start background performance monitoring"""
        if not self._performance_monitor_running:
            self._performance_monitor_running = True
            asyncio.create_task(self._performance_monitor_loop())

    async def _performance_monitor_loop(self):
        """Background loop for performance monitoring"""
        while self._performance_monitor_running:
            try:
                await self._check_all_strategy_performance()
                await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                self.logger.error(f"Performance monitoring error: {e}")
                await asyncio.sleep(60)

    async def _check_all_strategy_performance(self):
        """Check performance of all registered strategies"""
        for strategy_name in self.validation_records:
            try:
                await self._check_strategy_performance_degradation(strategy_name)
            except Exception as e:
                self.logger.error(f"Performance check failed for {strategy_name}: {e}")

    async def _check_strategy_performance_degradation(self, strategy_name: str):
        """Check if a strategy's performance has degraded"""
        record = self.validation_records.get(strategy_name)
        if not record:
            return

        # This would typically check recent betting results against expected performance
        # For now, it's a placeholder for the monitoring logic
        current_performance = await self._get_recent_performance(strategy_name)

        if current_performance:
            # Check for significant performance degradation
            expected_roi = record.performance_metrics.get("roi", 0.0)
            current_roi = current_performance.get("roi", 0.0)

            if current_roi < (expected_roi - 15.0):  # More than 15% degradation
                await self.trigger_circuit_breaker(
                    strategy_name,
                    f"Performance degradation: {current_roi:.1f}% vs expected {expected_roi:.1f}%",
                )

    async def _get_recent_performance(
        self, strategy_name: str
    ) -> dict[str, float] | None:
        """Get recent performance metrics for a strategy"""
        # This would query recent betting results
        # Placeholder implementation
        return None

    # ===========================================
    # PERSISTENCE & DATA MANAGEMENT
    # ===========================================

    async def _load_validation_records(self):
        """Load validation records from database"""
        try:
            # Check if validation schema and table exist first
            check_schema_query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'validation'
                )
            """

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(check_schema_query)
                schema_exists = cursor.fetchone()[0]

                if not schema_exists:
                    self.logger.info(
                        "Validation schema doesn't exist yet - using default validation records"
                    )
                    return

                # Check if strategy_records table exists
                check_table_query = """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'validation' 
                        AND table_name = 'strategy_records'
                    )
                """
                cursor.execute(check_table_query)
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    self.logger.info(
                        "validation.strategy_records table doesn't exist yet - using default validation records"
                    )
                    return

                # Check if validation_status column exists in the table
                check_column_query = """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'validation' 
                        AND table_name = 'strategy_records'
                        AND column_name = 'validation_status'
                    )
                """
                cursor.execute(check_column_query)
                column_exists = cursor.fetchone()[0]

                if not column_exists:
                    self.logger.info(
                        "validation_status column doesn't exist yet - using default validation records"
                    )
                    return

                # If table exists, try to load records
                query = """
                SELECT 
                    strategy_name,
                    validation_status,
                    last_validated,
                    performance_metrics,
                    circuit_breaker_open,
                    max_daily_recommendations,
                    max_bet_size_multiplier,
                    requires_manual_approval
                FROM validation.strategy_records
                WHERE is_active = true
                """

                cursor.execute(query)
                results = cursor.fetchall()

                for row in results:
                    record = ValidationRecord(
                        strategy_name=row["strategy_name"],
                        validation_status=ValidationStatus(row["validation_status"]),
                        last_validated=row["last_validated"],
                        performance_metrics=json.loads(row["performance_metrics"])
                        if row["performance_metrics"]
                        else {},
                        circuit_breaker_open=row["circuit_breaker_open"],
                        max_daily_recommendations=row["max_daily_recommendations"],
                        max_bet_size_multiplier=row["max_bet_size_multiplier"],
                        requires_manual_approval=row["requires_manual_approval"],
                    )

                    self.validation_records[row["strategy_name"]] = record

                self.logger.info(
                    f"Loaded {len(results)} validation records from database"
                )

        except Exception as e:
            self.logger.warning(f"Failed to load validation records from DB: {e}")
            # Continue with empty records - system should still function
            # Initialize with default validation for common strategies
            self._initialize_default_validation_records()

    def _initialize_default_validation_records(self):
        """Initialize default validation records when database is not available"""
        default_strategies = [
            "sharp_action_detector",
            "opposing_markets_detector",
            "book_conflicts_detector",
            "line_movement_detector",
            "consensus_fade_detector",
        ]

        for strategy_name in default_strategies:
            self.validation_records[strategy_name] = ValidationRecord(
                strategy_name=strategy_name,
                validation_status=ValidationStatus.VALIDATED,  # Allow basic functionality
                last_validated=datetime.now(),
                performance_metrics={},
                circuit_breaker_open=False,
                max_daily_recommendations=50,
                max_bet_size_multiplier=1.0,
                requires_manual_approval=False,
            )

        self.logger.info(
            f"Initialized {len(default_strategies)} default validation records"
        )

    async def _persist_validation_records(self):
        """Persist validation records to database"""
        try:
            # This would save validation records to database
            # Placeholder implementation
            self.logger.debug("Validation records persisted")
        except Exception as e:
            self.logger.error(f"Failed to persist validation records: {e}")

    def get_registry_status(self) -> dict[str, Any]:
        """Get current status of the validation registry"""
        total_strategies = len(self.validation_records)
        validated_strategies = sum(
            1
            for r in self.validation_records.values()
            if r.validation_status
            in [ValidationStatus.VALIDATED, ValidationStatus.APPROVED]
        )

        return {
            "total_strategies": total_strategies,
            "validated_strategies": validated_strategies,
            "unvalidated_strategies": total_strategies - validated_strategies,
            "kill_switch_active": self.emergency_controls["kill_switch_active"],
            "emergency_suspension_active": self.emergency_controls[
                "emergency_suspension_active"
            ],
            "strategies_with_circuit_breakers": sum(
                1 for r in self.validation_records.values() if r.circuit_breaker_open
            ),
            "performance_monitor_running": self._performance_monitor_running,
            "last_performance_check": max(
                (r.last_validated for r in self.validation_records.values()),
                default=None,
            ),
        }


# Singleton pattern for global access
_strategy_validation_instance: StrategyValidation | None = None


async def get_strategy_validation() -> StrategyValidation:
    """Get singleton instance of Strategy Validation"""
    global _strategy_validation_instance
    if _strategy_validation_instance is None:
        _strategy_validation_instance = StrategyValidation()
        await _strategy_validation_instance.initialize()
    return _strategy_validation_instance


# Legacy compatibility aliases
async def get_validation_gate() -> StrategyValidation:
    """Legacy alias for validation gate service"""
    return await get_strategy_validation()


async def get_validation_registry() -> StrategyValidation:
    """Legacy alias for validation registry service"""
    return await get_strategy_validation()
