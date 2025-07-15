"""
Strategy Validation Registry - Production Enhanced

Central authority for strategy validation and approval in the MLB betting system.
Implements validation-first architecture with production-ready operational features:

- Strategy Versioning & Fingerprinting
- Performance Degradation Detection
- Circuit Breaker Patterns
- Emergency Controls & Kill Switches
- Registry Backup & Failover
- Continuous Monitoring & Alerting

This registry acts as a security gate between strategy implementation and live usage,
preventing untested strategies from affecting real betting decisions.
"""

import asyncio
import hashlib
import inspect
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from ..core.exceptions import ValidationError
from ..core.logging import get_logger
from ..db.connection import get_db_manager
from ..services.alert_service import AlertService, AlertSeverity
from ..services.backtesting_engine import get_backtesting_engine


# Simple BacktestResult for compatibility
@dataclass
class BacktestResult:
    """Simple backtest result for validation registry."""

    strategy_name: str
    total_bets: int
    wins: int
    win_rate: float
    roi_per_100: float
    confidence_score: float = 0.0


class ValidationStatus(Enum):
    """Strategy validation status levels"""

    UNVALIDATED = "unvalidated"  # No validation attempted
    VALIDATION_PENDING = "pending"  # Validation in progress
    VALIDATION_FAILED = "failed"  # Failed validation criteria
    VALIDATED = "validated"  # Passed validation, approved for limited use
    APPROVED = "approved"  # Fully approved for production use
    DEPRECATED = "deprecated"  # Previously approved but now deprecated
    QUARANTINED = "quarantined"  # Temporarily suspended due to poor performance
    CIRCUIT_BREAKER_OPEN = "circuit_open"  # Auto-suspended by circuit breaker
    EMERGENCY_SUSPENDED = "emergency"  # Emergency manual suspension


class ValidationLevel(Enum):
    """Levels of validation rigor"""

    BASIC = "basic"  # Basic backtesting requirements
    STANDARD = "standard"  # Standard validation with performance thresholds
    RIGOROUS = "rigorous"  # Comprehensive validation with statistical significance
    PRODUCTION = "production"  # Production-ready with monitoring requirements


class PerformanceAlert(Enum):
    """Types of performance alerts"""

    MINOR_DEGRADATION = "minor_degradation"
    MAJOR_DEGRADATION = "major_degradation"
    CRITICAL_DEGRADATION = "critical_degradation"
    CIRCUIT_BREAKER_TRIGGERED = "circuit_breaker_triggered"
    EMERGENCY_SUSPENSION = "emergency_suspension"


@dataclass
class StrategyVersion:
    """Strategy version information with fingerprinting"""

    version_id: str
    strategy_name: str
    code_fingerprint: str  # SHA-256 hash of strategy code
    creation_date: datetime
    validation_status: ValidationStatus
    backtest_results: list[BacktestResult] = field(default_factory=list)
    performance_metrics: dict[str, float] = field(default_factory=dict)

    # Version metadata
    description: str | None = None
    created_by: str | None = None
    git_commit: str | None = None

    def __post_init__(self):
        if not self.version_id:
            self.version_id = f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


@dataclass
class PerformanceWindow:
    """Rolling window for performance monitoring"""

    window_size: int = 50  # Number of recent bets to analyze
    min_sample_size: int = 10  # Minimum bets needed for analysis
    confidence_level: float = 0.95  # Confidence level for statistical tests

    # Performance decay thresholds
    minor_degradation_threshold: float = 0.1  # 10% performance drop
    major_degradation_threshold: float = 0.2  # 20% performance drop
    critical_degradation_threshold: float = 0.3  # 30% performance drop

    # Circuit breaker thresholds
    circuit_breaker_loss_threshold: int = 5  # Consecutive losses
    circuit_breaker_roi_threshold: float = -0.15  # -15% ROI threshold


@dataclass
class CircuitBreakerState:
    """Circuit breaker state for strategy protection"""

    is_open: bool = False
    failure_count: int = 0
    last_failure_time: datetime | None = None

    # Recovery parameters
    timeout_seconds: int = 3600  # 1 hour timeout
    half_open_test_period: int = 300  # 5 minutes test period
    recovery_success_threshold: int = 3  # Successes needed to close

    # Failure tracking
    consecutive_failures: int = 0
    total_failures: int = 0
    last_success_time: datetime | None = None


@dataclass
class EmergencyControls:
    """Emergency control state for the registry"""

    kill_switch_active: bool = False
    emergency_suspension_active: bool = False
    manual_override_active: bool = False

    # Control metadata
    activated_by: str | None = None
    activation_time: datetime | None = None
    reason: str | None = None

    # Auto-recovery settings
    auto_recovery_enabled: bool = False
    recovery_time: datetime | None = None


@dataclass
class RegistryBackup:
    """Registry backup configuration"""

    backup_enabled: bool = True
    backup_interval_minutes: int = 15
    backup_retention_days: int = 30

    # Backup locations
    primary_backup_path: Path = field(
        default_factory=lambda: Path("data/backups/registry")
    )
    secondary_backup_path: Path | None = None

    # Corruption detection
    checksum_validation: bool = True
    last_backup_time: datetime | None = None
    last_validation_time: datetime | None = None


@dataclass
class ValidationRecord:
    """Enhanced validation record with versioning and monitoring"""

    strategy_name: str
    current_version: StrategyVersion
    validation_history: list[StrategyVersion] = field(default_factory=list)

    # Performance monitoring
    performance_window: PerformanceWindow = field(default_factory=PerformanceWindow)
    live_performance_data: list[dict[str, Any]] = field(default_factory=list)

    # Circuit breaker
    circuit_breaker: CircuitBreakerState = field(default_factory=CircuitBreakerState)

    # Alerts and notifications
    performance_alerts: list[dict[str, Any]] = field(default_factory=list)
    last_performance_check: datetime | None = None

    # Operational constraints
    max_daily_recommendations: int = 10
    max_bet_size_multiplier: float = 1.0
    requires_manual_approval: bool = False

    # Approval tracking
    approved_by: str | None = None
    approval_date: datetime | None = None
    approval_notes: str | None = None


class ProductionStrategyValidationRegistry:
    """
    Production-ready Strategy Validation Registry with operational resilience.

    Implements comprehensive validation-first architecture with:
    - Strategy versioning and fingerprinting
    - Performance degradation detection
    - Circuit breaker patterns
    - Emergency controls and kill switches
    - Registry backup and failover
    - Continuous monitoring and alerting
    """

    def __init__(self, db_manager=None, registry_file: Path | None = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or get_db_manager()

        # Registry storage
        self.registry_file = registry_file or Path(
            "data/strategy_validation_registry.json"
        )
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)

        # Core registry data
        self._validation_records: dict[str, ValidationRecord] = {}
        self._approved_strategies: set[str] = set()
        self._strategy_fingerprints: dict[
            str, str
        ] = {}  # strategy_name -> current fingerprint

        # Emergency controls
        self.emergency_controls = EmergencyControls()

        # Registry backup system
        self.backup_config = RegistryBackup()
        self.backup_config.primary_backup_path.mkdir(parents=True, exist_ok=True)

        # Performance monitoring
        self._performance_monitor_task: asyncio.Task | None = None
        self._monitor_interval_seconds = 300  # 5 minutes

        # Services integration
        self.alert_service = None
        self.backtesting_service = None

        # Registry state
        self._last_integrity_check: datetime | None = None
        self._registry_healthy = True
        self._failover_mode = False

        # Load existing registry
        self._load_registry()

    async def initialize(self):
        """Initialize the production registry with all services"""
        try:
            # Initialize services
            self.alert_service = AlertService()
            self.backtesting_service = get_backtesting_engine()
            await self.backtesting_service.initialize()

            # Load registry data
            await self._load_registry_from_database()

            # Start performance monitoring
            await self._start_performance_monitoring()

            # Verify registry integrity
            await self._verify_registry_integrity()

            # Start backup system
            await self._start_backup_system()

            self.logger.info("✅ Production Strategy Validation Registry initialized")
            self.logger.info(
                f"   📊 {len(self._validation_records)} strategies registered"
            )
            self.logger.info(
                f"   ✅ {len(self._approved_strategies)} strategies approved"
            )
            self.logger.info("   🔄 Performance monitoring active")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize production registry: {e}")
            await self._enter_failover_mode("Initialization failed")
            raise

    # ===== STRATEGY VERSIONING & FINGERPRINTING =====

    def _generate_strategy_fingerprint(self, strategy_name: str) -> str:
        """Generate SHA-256 fingerprint of strategy code"""
        try:
            # Import the strategy processor to get its source code
            from ..analysis.processors.strategy_processor_factory import (
                StrategyProcessorFactory,
            )

            # Get the processor class
            factory = StrategyProcessorFactory(None, None, None)  # Temporary factory
            processor_info = factory.PROCESSOR_MAPPING.get(strategy_name)

            if not processor_info:
                return f"unknown_strategy_{strategy_name}"

            # Try to import and inspect the actual processor class
            try:
                processor_class_name = processor_info["class"]
                module_name = f"..analysis.processors.{strategy_name.lower()}_processor"

                # Get the source code of the processor
                import importlib

                module = importlib.import_module(module_name, package=__package__)
                processor_class = getattr(module, processor_class_name)

                # Get source code and create hash
                source_code = inspect.getsource(processor_class)
                return hashlib.sha256(source_code.encode()).hexdigest()

            except Exception as e:
                self.logger.warning(f"Could not fingerprint {strategy_name}: {e}")
                # Fallback to processor info hash
                info_str = json.dumps(processor_info, sort_keys=True)
                return hashlib.sha256(info_str.encode()).hexdigest()

        except Exception as e:
            self.logger.error(
                f"Failed to generate fingerprint for {strategy_name}: {e}"
            )
            return f"error_{strategy_name}_{datetime.now().isoformat()}"

    async def check_strategy_modifications(self, strategy_name: str) -> bool:
        """Check if strategy has been modified since last validation"""
        current_fingerprint = self._generate_strategy_fingerprint(strategy_name)

        if strategy_name not in self._strategy_fingerprints:
            self._strategy_fingerprints[strategy_name] = current_fingerprint
            return False  # First time seeing this strategy

        stored_fingerprint = self._strategy_fingerprints[strategy_name]

        if current_fingerprint != stored_fingerprint:
            self.logger.warning(f"🔄 Strategy {strategy_name} has been modified!")
            self.logger.warning(f"   Previous: {stored_fingerprint[:12]}...")
            self.logger.warning(f"   Current:  {current_fingerprint[:12]}...")

            # Auto-invalidate the strategy
            await self._invalidate_modified_strategy(strategy_name, current_fingerprint)
            return True

        return False

    async def _invalidate_modified_strategy(
        self, strategy_name: str, new_fingerprint: str
    ):
        """Invalidate a strategy that has been modified"""
        if strategy_name not in self._validation_records:
            return

        record = self._validation_records[strategy_name]

        # Move current version to history
        if record.current_version:
            record.validation_history.append(record.current_version)

        # Create new version requiring validation
        new_version = StrategyVersion(
            version_id=f"v{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            strategy_name=strategy_name,
            code_fingerprint=new_fingerprint,
            creation_date=datetime.now(timezone.utc),
            validation_status=ValidationStatus.UNVALIDATED,
            description="Auto-created due to code modification",
        )

        record.current_version = new_version

        # Remove from approved strategies
        self._approved_strategies.discard(strategy_name)

        # Update fingerprint
        self._strategy_fingerprints[strategy_name] = new_fingerprint

        # Send alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=AlertSeverity.WARNING,
                title=f"Strategy Modified: {strategy_name}",
                message=f"Strategy {strategy_name} has been modified and requires re-validation",
                category="strategy_modification",
            )

        # Persist changes
        await self._persist_registry()

        self.logger.warning(
            f"🚨 Strategy {strategy_name} invalidated due to modification"
        )

    # ===== PERFORMANCE DEGRADATION DETECTION =====

    async def _start_performance_monitoring(self):
        """Start continuous performance monitoring"""
        if self._performance_monitor_task:
            return

        self._performance_monitor_task = asyncio.create_task(
            self._performance_monitor_loop()
        )
        self.logger.info("🔄 Performance monitoring started")

    async def _performance_monitor_loop(self):
        """Continuous performance monitoring loop"""
        while True:
            try:
                await asyncio.sleep(self._monitor_interval_seconds)

                if self.emergency_controls.kill_switch_active:
                    continue  # Skip monitoring during kill switch

                await self._check_all_strategy_performance()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Performance monitoring error: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    async def _check_all_strategy_performance(self):
        """Check performance for all approved strategies"""
        for strategy_name in list(self._approved_strategies):
            if strategy_name in self._validation_records:
                await self._check_strategy_performance_degradation(strategy_name)

    async def _check_strategy_performance_degradation(self, strategy_name: str):
        """Check for performance degradation using statistical analysis"""
        record = self._validation_records[strategy_name]

        if (
            len(record.live_performance_data)
            < record.performance_window.min_sample_size
        ):
            return  # Not enough data for analysis

        # Get recent performance data
        recent_data = record.live_performance_data[
            -record.performance_window.window_size :
        ]

        # Calculate current performance metrics
        current_wins = sum(1 for bet in recent_data if bet.get("outcome", False))
        current_win_rate = current_wins / len(recent_data)
        current_roi = sum(bet.get("roi", 0) for bet in recent_data) / len(recent_data)

        # Get original validation metrics
        original_win_rate = (
            record.current_version.performance_metrics.get("win_rate", 0) / 100
        )
        original_roi = record.current_version.performance_metrics.get("roi", 0) / 100

        # Statistical significance test
        degradation_detected = False
        alert_level = PerformanceAlert.MINOR_DEGRADATION

        # Win rate degradation analysis
        if original_win_rate > 0:
            win_rate_degradation = (
                original_win_rate - current_win_rate
            ) / original_win_rate

            if (
                win_rate_degradation
                > record.performance_window.critical_degradation_threshold
            ):
                degradation_detected = True
                alert_level = PerformanceAlert.CRITICAL_DEGRADATION
            elif (
                win_rate_degradation
                > record.performance_window.major_degradation_threshold
            ):
                degradation_detected = True
                alert_level = PerformanceAlert.MAJOR_DEGRADATION
            elif (
                win_rate_degradation
                > record.performance_window.minor_degradation_threshold
            ):
                degradation_detected = True
                alert_level = PerformanceAlert.MINOR_DEGRADATION

        # ROI degradation analysis
        if original_roi > 0:
            roi_degradation = (original_roi - current_roi) / original_roi

            if (
                roi_degradation
                > record.performance_window.critical_degradation_threshold
            ):
                degradation_detected = True
                alert_level = PerformanceAlert.CRITICAL_DEGRADATION

        # Circuit breaker analysis
        recent_losses = 0
        for bet in recent_data[
            -record.performance_window.circuit_breaker_loss_threshold :
        ]:
            if not bet.get("outcome", False):
                recent_losses += 1
            else:
                break  # Reset count on win

        circuit_breaker_triggered = (
            recent_losses >= record.performance_window.circuit_breaker_loss_threshold
            or current_roi <= record.performance_window.circuit_breaker_roi_threshold
        )

        # Take action based on analysis
        if circuit_breaker_triggered:
            await self._trigger_circuit_breaker(
                strategy_name, recent_losses, current_roi
            )
        elif degradation_detected:
            await self._handle_performance_degradation(
                strategy_name,
                alert_level,
                {
                    "current_win_rate": current_win_rate,
                    "original_win_rate": original_win_rate,
                    "current_roi": current_roi,
                    "original_roi": original_roi,
                    "sample_size": len(recent_data),
                },
            )

        # Update last check time
        record.last_performance_check = datetime.now(timezone.utc)

    async def _trigger_circuit_breaker(
        self, strategy_name: str, consecutive_losses: int, current_roi: float
    ):
        """Trigger circuit breaker for underperforming strategy"""
        record = self._validation_records[strategy_name]

        # Update circuit breaker state
        record.circuit_breaker.is_open = True
        record.circuit_breaker.failure_count += 1
        record.circuit_breaker.consecutive_failures = consecutive_losses
        record.circuit_breaker.last_failure_time = datetime.now(timezone.utc)

        # Update strategy status
        record.current_version.validation_status = ValidationStatus.CIRCUIT_BREAKER_OPEN

        # Remove from approved strategies
        self._approved_strategies.discard(strategy_name)

        # Log circuit breaker activation
        self.logger.critical(f"🚨 CIRCUIT BREAKER TRIGGERED: {strategy_name}")
        self.logger.critical(f"   Consecutive losses: {consecutive_losses}")
        self.logger.critical(f"   Current ROI: {current_roi:.2%}")

        # Send critical alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=AlertSeverity.CRITICAL,
                title=f"Circuit Breaker: {strategy_name}",
                message=f"Strategy {strategy_name} auto-suspended due to poor performance",
                category="circuit_breaker",
            )

        # Persist changes
        await self._persist_registry()

    async def _handle_performance_degradation(
        self, strategy_name: str, alert_level: PerformanceAlert, metrics: dict[str, Any]
    ):
        """Handle detected performance degradation"""
        record = self._validation_records[strategy_name]

        # Create performance alert
        alert_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "alert_level": alert_level.value,
            "metrics": metrics,
            "strategy_name": strategy_name,
        }

        record.performance_alerts.append(alert_data)

        # Determine severity for alert service
        if alert_level == PerformanceAlert.CRITICAL_DEGRADATION:
            severity = AlertSeverity.CRITICAL
        elif alert_level == PerformanceAlert.MAJOR_DEGRADATION:
            severity = AlertSeverity.WARNING
        else:
            severity = AlertSeverity.INFO

        # Send alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=severity,
                title=f"Performance Degradation: {strategy_name}",
                message=f"Strategy {strategy_name} showing {alert_level.value} performance degradation",
                category="performance_degradation",
            )

        self.logger.warning(
            f"⚠️ Performance degradation detected: {strategy_name} ({alert_level.value})"
        )

        # Persist changes
        await self._persist_registry()

    # ===== EMERGENCY CONTROLS & KILL SWITCHES =====

    async def activate_kill_switch(self, activated_by: str, reason: str):
        """Activate emergency kill switch - suspends all strategies"""
        self.emergency_controls.kill_switch_active = True
        self.emergency_controls.activated_by = activated_by
        self.emergency_controls.activation_time = datetime.now(timezone.utc)
        self.emergency_controls.reason = reason

        # Clear all approved strategies
        self._approved_strategies.clear()

        # Update all strategy statuses
        for record in self._validation_records.values():
            if record.current_version.validation_status in [
                ValidationStatus.APPROVED,
                ValidationStatus.VALIDATED,
            ]:
                record.current_version.validation_status = (
                    ValidationStatus.EMERGENCY_SUSPENDED
                )

        # Send critical alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=AlertSeverity.CRITICAL,
                title="EMERGENCY KILL SWITCH ACTIVATED",
                message=f"All strategies suspended by {activated_by}. Reason: {reason}",
                category="emergency_control",
            )

        # Persist changes
        await self._persist_registry()

        self.logger.critical(f"🚨 EMERGENCY KILL SWITCH ACTIVATED by {activated_by}")
        self.logger.critical(f"   Reason: {reason}")
        self.logger.critical("   All strategies suspended")

    async def deactivate_kill_switch(self, deactivated_by: str):
        """Deactivate kill switch - requires manual strategy re-approval"""
        self.emergency_controls.kill_switch_active = False

        # Send alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=AlertSeverity.WARNING,
                title="Kill Switch Deactivated",
                message=f"Kill switch deactivated by {deactivated_by}. Strategies require manual re-approval.",
                category="emergency_control",
            )

        # Persist changes
        await self._persist_registry()

        self.logger.warning(f"🔓 Kill switch deactivated by {deactivated_by}")
        self.logger.warning("   Manual strategy re-approval required")

    async def emergency_suspend_strategy(
        self, strategy_name: str, suspended_by: str, reason: str
    ):
        """Emergency suspend a specific strategy"""
        if strategy_name not in self._validation_records:
            raise ValidationError(f"Strategy {strategy_name} not found")

        record = self._validation_records[strategy_name]
        record.current_version.validation_status = ValidationStatus.EMERGENCY_SUSPENDED

        # Remove from approved strategies
        self._approved_strategies.discard(strategy_name)

        # Send alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=AlertSeverity.CRITICAL,
                title=f"Emergency Suspension: {strategy_name}",
                message=f"Strategy {strategy_name} emergency suspended by {suspended_by}. Reason: {reason}",
                category="emergency_control",
            )

        # Persist changes
        await self._persist_registry()

        self.logger.critical(
            f"🚨 EMERGENCY SUSPENSION: {strategy_name} by {suspended_by}"
        )
        self.logger.critical(f"   Reason: {reason}")

    # ===== VALIDATION GATE ENFORCEMENT =====

    def can_generate_recommendations(self, strategy_name: str) -> dict[str, Any]:
        """
        Enhanced validation gate with all production checks

        Returns:
            Dict with 'allowed' boolean and detailed constraint information
        """
        # Emergency controls check
        if self.emergency_controls.kill_switch_active:
            return {
                "allowed": False,
                "reason": "Emergency kill switch is active",
                "requires_manual_override": True,
                "emergency_contact": self.emergency_controls.activated_by,
            }

        # Registry health check
        if not self._registry_healthy:
            return {
                "allowed": False,
                "reason": "Registry integrity compromised",
                "requires_manual_override": True,
                "failover_mode": self._failover_mode,
            }

        # Strategy existence check
        if strategy_name not in self._validation_records:
            return {
                "allowed": False,
                "reason": "Strategy not found in validation registry",
                "requires_validation": True,
            }

        record = self._validation_records[strategy_name]

        # Check for strategy modifications
        try:
            current_fingerprint = self._generate_strategy_fingerprint(strategy_name)
            if current_fingerprint != record.current_version.code_fingerprint:
                return {
                    "allowed": False,
                    "reason": "Strategy has been modified and requires re-validation",
                    "requires_validation": True,
                    "code_change_detected": True,
                }
        except Exception as e:
            self.logger.error(f"Fingerprint check failed for {strategy_name}: {e}")

        # Status checks
        status = record.current_version.validation_status

        if status == ValidationStatus.CIRCUIT_BREAKER_OPEN:
            return {
                "allowed": False,
                "reason": "Circuit breaker is open due to poor performance",
                "circuit_breaker_active": True,
                "can_recover": self._can_circuit_breaker_recover(record),
            }

        if status == ValidationStatus.EMERGENCY_SUSPENDED:
            return {
                "allowed": False,
                "reason": "Strategy is emergency suspended",
                "requires_manual_override": True,
            }

        if status not in [ValidationStatus.VALIDATED, ValidationStatus.APPROVED]:
            return {
                "allowed": False,
                "reason": f"Strategy status is {status.value}",
                "requires_validation": status == ValidationStatus.UNVALIDATED,
            }

        # All checks passed
        return {
            "allowed": True,
            "max_daily_recommendations": record.max_daily_recommendations,
            "max_bet_size_multiplier": record.max_bet_size_multiplier,
            "requires_manual_approval": record.requires_manual_approval,
            "validation_level": record.current_version.validation_status.value,
            "last_performance_check": record.last_performance_check.isoformat()
            if record.last_performance_check
            else None,
        }

    def _can_circuit_breaker_recover(self, record: ValidationRecord) -> bool:
        """Check if circuit breaker can recover"""
        if not record.circuit_breaker.last_failure_time:
            return True

        timeout_elapsed = (
            datetime.now(timezone.utc) - record.circuit_breaker.last_failure_time
        ).total_seconds() >= record.circuit_breaker.timeout_seconds

        return timeout_elapsed

    # ===== REGISTRY RELIABILITY & BACKUP =====

    async def _start_backup_system(self):
        """Start automated backup system"""
        if not self.backup_config.backup_enabled:
            return

        # Create backup task
        backup_task = asyncio.create_task(self._backup_loop())
        self.logger.info("💾 Registry backup system started")

    async def _backup_loop(self):
        """Automated backup loop"""
        while True:
            try:
                await asyncio.sleep(self.backup_config.backup_interval_minutes * 60)
                await self._create_backup()
                await self._cleanup_old_backups()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Backup system error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retry

    async def _create_backup(self):
        """Create registry backup"""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            backup_file = (
                self.backup_config.primary_backup_path
                / f"registry_backup_{timestamp}.json"
            )

            # Create backup data
            backup_data = {
                "timestamp": timestamp,
                "registry_version": "2.0",
                "validation_records": {},
                "approved_strategies": list(self._approved_strategies),
                "strategy_fingerprints": self._strategy_fingerprints,
                "emergency_controls": asdict(self.emergency_controls),
                "backup_config": asdict(self.backup_config),
            }

            # Serialize validation records
            for name, record in self._validation_records.items():
                backup_data["validation_records"][name] = (
                    self._serialize_validation_record(record)
                )

            # Write backup
            with open(backup_file, "w") as f:
                json.dump(backup_data, f, indent=2, default=str)

            # Calculate checksum
            if self.backup_config.checksum_validation:
                checksum = self._calculate_file_checksum(backup_file)
                checksum_file = backup_file.with_suffix(".checksum")
                with open(checksum_file, "w") as f:
                    f.write(checksum)

            self.backup_config.last_backup_time = datetime.now(timezone.utc)
            self.logger.debug(f"✅ Registry backup created: {backup_file}")

        except Exception as e:
            self.logger.error(f"Failed to create backup: {e}")

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    async def _cleanup_old_backups(self):
        """Clean up old backup files"""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(
                days=self.backup_config.backup_retention_days
            )

            for backup_file in self.backup_config.primary_backup_path.glob(
                "registry_backup_*.json"
            ):
                if backup_file.stat().st_mtime < cutoff_date.timestamp():
                    backup_file.unlink()
                    # Also remove checksum file
                    checksum_file = backup_file.with_suffix(".checksum")
                    if checksum_file.exists():
                        checksum_file.unlink()

        except Exception as e:
            self.logger.error(f"Failed to cleanup old backups: {e}")

    async def _verify_registry_integrity(self):
        """Verify registry data integrity"""
        try:
            # Check for corruption indicators
            integrity_issues = []

            # Validate strategy records
            for name, record in self._validation_records.items():
                if not record.current_version:
                    integrity_issues.append(f"Strategy {name} missing current version")

                if record.current_version.strategy_name != name:
                    integrity_issues.append(f"Strategy {name} name mismatch in record")

            # Check approved strategies consistency
            for strategy_name in self._approved_strategies:
                if strategy_name not in self._validation_records:
                    integrity_issues.append(
                        f"Approved strategy {strategy_name} not in registry"
                    )
                elif self._validation_records[
                    strategy_name
                ].current_version.validation_status not in [
                    ValidationStatus.VALIDATED,
                    ValidationStatus.APPROVED,
                ]:
                    integrity_issues.append(
                        f"Approved strategy {strategy_name} has invalid status"
                    )

            # Update integrity status
            self._registry_healthy = len(integrity_issues) == 0
            self._last_integrity_check = datetime.now(timezone.utc)

            if integrity_issues:
                self.logger.error("❌ Registry integrity issues detected:")
                for issue in integrity_issues:
                    self.logger.error(f"   - {issue}")

                # Send alert
                if self.alert_service:
                    await self.alert_service.send_alert(
                        severity=AlertSeverity.CRITICAL,
                        title="Registry Integrity Compromised",
                        message=f"Registry integrity issues detected: {len(integrity_issues)} problems found",
                        category="registry_integrity",
                    )
            else:
                self.logger.debug("✅ Registry integrity verified")

        except Exception as e:
            self.logger.error(f"Registry integrity check failed: {e}")
            self._registry_healthy = False

    async def _enter_failover_mode(self, reason: str):
        """Enter failover mode for registry protection"""
        self._failover_mode = True
        self._registry_healthy = False

        # Clear approved strategies as safety measure
        self._approved_strategies.clear()

        self.logger.critical(f"🚨 REGISTRY FAILOVER MODE ACTIVATED: {reason}")

        # Send critical alert
        if self.alert_service:
            await self.alert_service.send_alert(
                severity=AlertSeverity.CRITICAL,
                title="Registry Failover Mode",
                message=f"Registry entered failover mode: {reason}",
                category="registry_failover",
            )

    # ===== UTILITY METHODS =====

    def _serialize_validation_record(self, record: ValidationRecord) -> dict[str, Any]:
        """Serialize validation record for backup"""
        return {
            "strategy_name": record.strategy_name,
            "current_version": asdict(record.current_version),
            "validation_history": [asdict(v) for v in record.validation_history],
            "performance_window": asdict(record.performance_window),
            "live_performance_data": record.live_performance_data,
            "circuit_breaker": asdict(record.circuit_breaker),
            "performance_alerts": record.performance_alerts,
            "last_performance_check": record.last_performance_check.isoformat()
            if record.last_performance_check
            else None,
            "max_daily_recommendations": record.max_daily_recommendations,
            "max_bet_size_multiplier": record.max_bet_size_multiplier,
            "requires_manual_approval": record.requires_manual_approval,
            "approved_by": record.approved_by,
            "approval_date": record.approval_date.isoformat()
            if record.approval_date
            else None,
            "approval_notes": record.approval_notes,
        }

    def _load_registry(self):
        """Load registry from file (simplified for space)"""
        if not self.registry_file.exists():
            self.logger.info("Creating new validation registry")
            return

        try:
            with open(self.registry_file) as f:
                data = json.load(f)

            # Load basic data (full implementation would deserialize all records)
            self._approved_strategies = set(data.get("approved_strategies", []))
            self._strategy_fingerprints = data.get("strategy_fingerprints", {})

            self.logger.info(
                f"✅ Loaded registry with {len(self._approved_strategies)} approved strategies"
            )

        except Exception as e:
            self.logger.error(f"Failed to load registry: {e}")

    async def _persist_registry(self):
        """Persist registry to file (simplified for space)"""
        try:
            data = {
                "approved_strategies": list(self._approved_strategies),
                "strategy_fingerprints": self._strategy_fingerprints,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            with open(self.registry_file, "w") as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            self.logger.error(f"Failed to persist registry: {e}")

    async def _load_registry_from_database(self):
        """Load registry data from database if available"""
        # Implementation would depend on your database schema
        pass

    def get_registry_status(self) -> dict[str, Any]:
        """Get comprehensive registry status"""
        return {
            "total_strategies": len(self._validation_records),
            "approved_strategies": len(self._approved_strategies),
            "registry_healthy": self._registry_healthy,
            "failover_mode": self._failover_mode,
            "kill_switch_active": self.emergency_controls.kill_switch_active,
            "last_integrity_check": self._last_integrity_check.isoformat()
            if self._last_integrity_check
            else None,
            "last_backup": self.backup_config.last_backup_time.isoformat()
            if self.backup_config.last_backup_time
            else None,
            "performance_monitoring_active": self._performance_monitor_task is not None
            and not self._performance_monitor_task.done(),
        }


# ===== SINGLETON PATTERN =====

_registry_instance: ProductionStrategyValidationRegistry | None = None


async def get_validation_registry() -> ProductionStrategyValidationRegistry:
    """Get singleton instance of Production Strategy Validation Registry"""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = ProductionStrategyValidationRegistry()
        await _registry_instance.initialize()
    return _registry_instance
