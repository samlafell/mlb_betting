#!/usr/bin/env python3
"""
Feature Flags Service for MLB Sharp Betting System

Provides feature flag infrastructure for safe betting system updates during
refactoring. Enables quick rollback of changes if betting performance degrades
or system issues arise.

Production Features:
- Thread-safe feature flag management
- Dynamic flag updates without restart
- Performance-based automatic rollback
- Integration with betting accuracy monitoring
- Persistent flag state storage
- Flag dependency management
- A/B testing capabilities for betting strategies
"""

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import structlog

from ..core.logging import get_logger
from .config_service import get_config_service
from .backtesting_engine import get_backtesting_engine

logger = get_logger(__name__)


class FlagType(Enum):
    """Types of feature flags."""
    BOOLEAN = "boolean"
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    JSON = "json"


class FlagCategory(Enum):
    """Categories of feature flags."""
    REFACTORING = "refactoring"  # Refactoring-related flags
    STRATEGY = "strategy"  # Betting strategy flags
    PERFORMANCE = "performance"  # Performance optimization flags
    INTEGRATION = "integration"  # Service integration flags
    EXPERIMENT = "experiment"  # A/B testing flags


class RollbackTrigger(Enum):
    """Triggers for automatic rollback."""
    PERFORMANCE_DEGRADATION = "performance_degradation"
    ERROR_RATE_SPIKE = "error_rate_spike"
    MANUAL = "manual"
    TIMEOUT = "timeout"


@dataclass
class FlagRule:
    """Rule for automatic flag management."""
    trigger: RollbackTrigger
    threshold: float
    action: str  # "disable", "enable", "rollback"
    description: str


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    name: str
    flag_type: FlagType
    category: FlagCategory
    default_value: Any
    current_value: Any
    description: str
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    dependencies: List[str] = field(default_factory=list)
    rollback_rules: List[FlagRule] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_value(self) -> Any:
        """Get the current flag value."""
        if not self.enabled:
            return self.default_value
        return self.current_value


class BettingFeatureFlags:
    """
    Feature flags for safe betting system updates.
    
    Provides infrastructure for safely rolling out refactoring changes
    with quick rollback capability if betting performance degrades.
    """
    
    # Default feature flags for refactoring phases
    DEFAULT_FLAGS = {
        # Phase 1: Configuration consolidation flags
        "use_config_service": FeatureFlag(
            name="use_config_service",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.REFACTORING,
            default_value=False,
            current_value=False,
            description="Use centralized ConfigurationService instead of scattered config access"
        ),
        
        "use_unified_retry": FeatureFlag(
            name="use_unified_retry",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.REFACTORING,
            default_value=False,
            current_value=False,
            description="Use unified RetryService instead of scattered retry implementations"
        ),
        
        "use_unified_rate_limiting": FeatureFlag(
            name="use_unified_rate_limiting",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.REFACTORING,
            default_value=False,
            current_value=False,
            description="Use UnifiedRateLimiter instead of scattered rate limiting"
        ),
        
        # Phase 2: Base service pattern flags
        "use_base_service": FeatureFlag(
            name="use_base_service",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.REFACTORING,
            default_value=False,
            current_value=False,
            description="Use BaseService pattern for all services"
        ),
        
        "enhanced_monitoring": FeatureFlag(
            name="enhanced_monitoring",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.PERFORMANCE,
            default_value=False,
            current_value=False,
            description="Enable enhanced service monitoring and circuit breakers"
        ),
        
        # Phase 3: Scheduler consolidation flags
        "use_scheduler_orchestrator": FeatureFlag(
            name="use_scheduler_orchestrator",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.REFACTORING,
            default_value=False,
            current_value=False,
            description="Use SchedulerOrchestrator instead of individual schedulers"
        ),
        
        # Betting strategy flags
        "enable_new_processors": FeatureFlag(
            name="enable_new_processors",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.STRATEGY,
            default_value=True,
            current_value=True,
            description="Enable new betting strategy processors"
        ),
        
        "processor_parallel_execution": FeatureFlag(
            name="processor_parallel_execution",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.PERFORMANCE,
            default_value=True,
            current_value=True,
            description="Execute strategy processors in parallel"
        ),
        
        # Performance flags
        "aggressive_caching": FeatureFlag(
            name="aggressive_caching",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.PERFORMANCE,
            default_value=False,
            current_value=False,
            description="Enable aggressive caching for betting analysis"
        ),
        
        "database_optimization": FeatureFlag(
            name="database_optimization",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.PERFORMANCE,
            default_value=False,
            current_value=False,
            description="Enable database query optimizations"
        ),
        
        # Integration flags
        "odds_api_v2": FeatureFlag(
            name="odds_api_v2",
            flag_type=FlagType.BOOLEAN,
            category=FlagCategory.INTEGRATION,
            default_value=False,
            current_value=False,
            description="Use Odds API v2 integration"
        )
    }
    
    def __init__(self, data_dir: Optional[Path] = None):
        """Initialize feature flags service."""
        self.logger = get_logger(__name__)
        self.config_service = get_config_service()
        self.backtesting_engine = get_backtesting_engine()
        
        # Data directory for persistent storage
        self.data_dir = data_dir or Path("data/feature_flags")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Feature flags storage
        self.flags: Dict[str, FeatureFlag] = {}
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Automatic rollback tracking
        self.rollback_checks_enabled = True
        self.last_performance_check = datetime.now()
        self.performance_check_interval = timedelta(minutes=5)
        
        # Initialize flags
        self._initialize_flags()
        
        # Load persistent state
        self._load_flag_state()
        
        self.logger.info("BettingFeatureFlags initialized with safe rollback capability")
    
    def _initialize_flags(self) -> None:
        """Initialize default feature flags."""
        with self._lock:
            for flag_name, flag in self.DEFAULT_FLAGS.items():
                self.flags[flag_name] = flag
                
                # Add performance-based rollback rules for refactoring flags
                if flag.category == FlagCategory.REFACTORING:
                    flag.rollback_rules.extend([
                        FlagRule(
                            trigger=RollbackTrigger.PERFORMANCE_DEGRADATION,
                            threshold=15.0,  # 15% performance degradation
                            action="disable",
                            description="Disable if betting performance degrades by 15%"
                        ),
                        FlagRule(
                            trigger=RollbackTrigger.ERROR_RATE_SPIKE,
                            threshold=0.10,  # 10% error rate
                            action="disable",
                            description="Disable if error rate exceeds 10%"
                        )
                    ])
    
    def _load_flag_state(self) -> None:
        """Load persistent flag state from storage."""
        try:
            flag_file = self.data_dir / "feature_flags.json"
            if flag_file.exists():
                with open(flag_file, 'r') as f:
                    data = json.load(f)
                
                # Update flag values from stored state
                for flag_name, flag_data in data.get("flags", {}).items():
                    if flag_name in self.flags:
                        flag = self.flags[flag_name]
                        flag.current_value = flag_data.get("current_value", flag.default_value)
                        flag.enabled = flag_data.get("enabled", True)
                        flag.updated_at = datetime.fromisoformat(flag_data.get("updated_at", datetime.now().isoformat()))
                
                self.logger.info(f"Loaded feature flag state for {len(data.get('flags', {}))} flags")
                
        except Exception as e:
            self.logger.error(f"Failed to load feature flag state: {e}")
    
    def _save_flag_state(self) -> None:
        """Save current flag state to persistent storage."""
        try:
            flag_file = self.data_dir / "feature_flags.json"
            
            data = {
                "flags": {
                    flag_name: {
                        "current_value": flag.current_value,
                        "enabled": flag.enabled,
                        "updated_at": flag.updated_at.isoformat()
                    }
                    for flag_name, flag in self.flags.items()
                },
                "last_updated": datetime.now().isoformat()
            }
            
            with open(flag_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save feature flag state: {e}")
    
    def is_enabled(self, flag_name: str) -> bool:
        """Check if a feature flag is enabled."""
        with self._lock:
            flag = self.flags.get(flag_name)
            if not flag:
                self.logger.warning(f"Unknown feature flag: {flag_name}")
                return False
            
            return flag.enabled and flag.get_value()
    
    def get_flag_value(self, flag_name: str, default: Any = None) -> Any:
        """Get the value of a feature flag."""
        with self._lock:
            flag = self.flags.get(flag_name)
            if not flag:
                self.logger.warning(f"Unknown feature flag: {flag_name}")
                return default
            
            return flag.get_value()
    
    def set_flag(self, flag_name: str, value: Any, reason: str = "") -> bool:
        """
        Set a feature flag value.
        
        Args:
            flag_name: Name of the flag to set
            value: New value for the flag
            reason: Reason for the change
            
        Returns:
            True if flag was set successfully
        """
        with self._lock:
            flag = self.flags.get(flag_name)
            if not flag:
                self.logger.error(f"Cannot set unknown feature flag: {flag_name}")
                return False
            
            # Validate value type
            if not self._validate_flag_value(flag, value):
                self.logger.error(f"Invalid value type for flag {flag_name}: {type(value)}")
                return False
            
            # Check dependencies
            if not self._check_flag_dependencies(flag_name, value):
                self.logger.error(f"Flag dependencies not met for {flag_name}")
                return False
            
            old_value = flag.current_value
            flag.current_value = value
            flag.updated_at = datetime.now()
            
            # Save state
            self._save_flag_state()
            
            self.logger.info(f"Feature flag updated: {flag_name} = {value} (was {old_value}). Reason: {reason}")
            
            return True
    
    def enable_flag(self, flag_name: str, reason: str = "") -> bool:
        """Enable a feature flag."""
        with self._lock:
            flag = self.flags.get(flag_name)
            if not flag:
                return False
            
            flag.enabled = True
            flag.updated_at = datetime.now()
            self._save_flag_state()
            
            self.logger.info(f"Feature flag enabled: {flag_name}. Reason: {reason}")
            return True
    
    def disable_flag(self, flag_name: str, reason: str = "") -> bool:
        """Disable a feature flag."""
        with self._lock:
            flag = self.flags.get(flag_name)
            if not flag:
                return False
            
            flag.enabled = False
            flag.updated_at = datetime.now()
            self._save_flag_state()
            
            self.logger.warning(f"Feature flag disabled: {flag_name}. Reason: {reason}")
            return True
    
    def rollback_flag(self, flag_name: str, reason: str = "") -> bool:
        """Rollback a feature flag to its default value."""
        with self._lock:
            flag = self.flags.get(flag_name)
            if not flag:
                return False
            
            old_value = flag.current_value
            flag.current_value = flag.default_value
            flag.enabled = True  # Assume rollback means enable with default
            flag.updated_at = datetime.now()
            self._save_flag_state()
            
            self.logger.warning(f"Feature flag rolled back: {flag_name} = {flag.default_value} (was {old_value}). Reason: {reason}")
            return True
    
    def _validate_flag_value(self, flag: FeatureFlag, value: Any) -> bool:
        """Validate flag value against expected type."""
        if flag.flag_type == FlagType.BOOLEAN:
            return isinstance(value, bool)
        elif flag.flag_type == FlagType.STRING:
            return isinstance(value, str)
        elif flag.flag_type == FlagType.INTEGER:
            return isinstance(value, int)
        elif flag.flag_type == FlagType.FLOAT:
            return isinstance(value, (int, float))
        elif flag.flag_type == FlagType.JSON:
            return True  # JSON can be any serializable type
        
        return False
    
    def _check_flag_dependencies(self, flag_name: str, value: Any) -> bool:
        """Check if flag dependencies are satisfied."""
        flag = self.flags.get(flag_name)
        if not flag or not flag.dependencies:
            return True
        
        for dependency in flag.dependencies:
            if not self.is_enabled(dependency):
                self.logger.warning(f"Dependency not met for {flag_name}: {dependency} is disabled")
                return False
        
        return True
    
    async def check_automatic_rollback(self) -> None:
        """Check if any flags should be automatically rolled back."""
        if not self.rollback_checks_enabled:
            return
        
        # Only check periodically
        now = datetime.now()
        if now - self.last_performance_check < self.performance_check_interval:
            return
        
        self.last_performance_check = now
        
        # Get current accuracy status
        try:
            accuracy_status = self.backtesting_engine.accuracy_monitor.get_current_status()
            
            if not accuracy_status.get("monitoring_active"):
                return
            
            # Check for performance degradation
            if accuracy_status.get("baseline_established"):
                await self._check_performance_based_rollback(accuracy_status)
                
        except Exception as e:
            self.logger.error(f"Failed to check automatic rollback: {e}")
    
    async def _check_performance_based_rollback(self, accuracy_status: Dict[str, Any]) -> None:
        """Check for performance-based rollback triggers."""
        baseline_metrics = accuracy_status.get("baseline_metrics", {})
        latest_snapshot = accuracy_status.get("latest_snapshot")
        
        if not latest_snapshot or not baseline_metrics:
            return
        
        # Calculate performance degradation
        win_rate_baseline = baseline_metrics.get("win_rate", 0)
        roi_baseline = baseline_metrics.get("roi", 0)
        
        current_win_rate = latest_snapshot.get("win_rate", 0)
        current_roi = latest_snapshot.get("roi", 0)
        
        # Check win rate degradation
        if win_rate_baseline > 0:
            win_rate_degradation = (win_rate_baseline - current_win_rate) / win_rate_baseline
            if win_rate_degradation > 0.15:  # 15% degradation
                await self._trigger_automatic_rollback("performance_degradation", 
                                                     f"Win rate degraded by {win_rate_degradation*100:.1f}%")
        
        # Check ROI degradation
        if roi_baseline > 0:
            roi_degradation = (roi_baseline - current_roi) / roi_baseline
            if roi_degradation > 0.15:  # 15% degradation
                await self._trigger_automatic_rollback("performance_degradation",
                                                     f"ROI degraded by {roi_degradation*100:.1f}%")
    
    async def _trigger_automatic_rollback(self, trigger_type: str, reason: str) -> None:
        """Trigger automatic rollback of refactoring flags."""
        self.logger.critical(f"Triggering automatic rollback: {trigger_type} - {reason}")
        
        # Rollback all refactoring flags
        refactoring_flags = [
            flag_name for flag_name, flag in self.flags.items()
            if flag.category == FlagCategory.REFACTORING and flag.enabled
        ]
        
        for flag_name in refactoring_flags:
            self.disable_flag(flag_name, f"Automatic rollback: {reason}")
        
        # Send alert
        try:
            from .alert_service import AlertService, AlertSeverity
            alert_service = AlertService()
            await alert_service.send_alert(
                title="Automatic Feature Flag Rollback Triggered",
                message=f"""
                Automatic rollback of refactoring features has been triggered!
                
                Trigger: {trigger_type}
                Reason: {reason}
                Flags Rolled Back: {', '.join(refactoring_flags)}
                
                System has been reverted to stable configuration.
                Please investigate the performance degradation before re-enabling features.
                """,
                severity=AlertSeverity.CRITICAL,
                alert_type="automatic_rollback"
            )
        except Exception as e:
            self.logger.error(f"Failed to send rollback alert: {e}")
    
    def get_all_flags(self) -> Dict[str, Dict[str, Any]]:
        """Get all feature flags and their current state."""
        with self._lock:
            return {
                flag_name: {
                    "value": flag.get_value(),
                    "enabled": flag.enabled,
                    "default_value": flag.default_value,
                    "description": flag.description,
                    "category": flag.category.value,
                    "type": flag.flag_type.value,
                    "updated_at": flag.updated_at.isoformat(),
                    "dependencies": flag.dependencies
                }
                for flag_name, flag in self.flags.items()
            }
    
    def get_flags_by_category(self, category: FlagCategory) -> Dict[str, Dict[str, Any]]:
        """Get all flags in a specific category."""
        all_flags = self.get_all_flags()
        return {
            flag_name: flag_data
            for flag_name, flag_data in all_flags.items()
            if flag_data["category"] == category.value
        }
    
    def create_flag_group_transaction(self, flag_changes: Dict[str, Any], reason: str = "") -> bool:
        """
        Make multiple flag changes as an atomic transaction.
        
        Args:
            flag_changes: Dictionary of flag_name -> new_value
            reason: Reason for the changes
            
        Returns:
            True if all changes were applied successfully
        """
        with self._lock:
            # Validate all changes first
            original_values = {}
            for flag_name, new_value in flag_changes.items():
                flag = self.flags.get(flag_name)
                if not flag:
                    self.logger.error(f"Unknown flag in transaction: {flag_name}")
                    return False
                
                if not self._validate_flag_value(flag, new_value):
                    self.logger.error(f"Invalid value in transaction for {flag_name}: {new_value}")
                    return False
                
                original_values[flag_name] = flag.current_value
            
            # Apply all changes
            try:
                for flag_name, new_value in flag_changes.items():
                    flag = self.flags[flag_name]
                    flag.current_value = new_value
                    flag.updated_at = datetime.now()
                
                # Save state
                self._save_flag_state()
                
                self.logger.info(f"Flag group transaction completed: {len(flag_changes)} flags updated. Reason: {reason}")
                return True
                
            except Exception as e:
                # Rollback changes
                for flag_name, original_value in original_values.items():
                    self.flags[flag_name].current_value = original_value
                
                self.logger.error(f"Flag group transaction failed, rolled back: {e}")
                return False


# Global feature flags instance
_feature_flags = None
_feature_flags_lock = threading.Lock()


def get_feature_flags() -> BettingFeatureFlags:
    """Get the global feature flags instance."""
    global _feature_flags
    if _feature_flags is None:
        with _feature_flags_lock:
            if _feature_flags is None:
                _feature_flags = BettingFeatureFlags()
    return _feature_flags


# Convenience functions for common flag operations
def is_enabled(flag_name: str) -> bool:
    """Check if a feature flag is enabled."""
    return get_feature_flags().is_enabled(flag_name)


def get_flag_value(flag_name: str, default: Any = None) -> Any:
    """Get the value of a feature flag."""
    return get_feature_flags().get_flag_value(flag_name, default)


# Context manager for temporary flag changes
class TemporaryFlagOverride:
    """Context manager for temporarily overriding flag values."""
    
    def __init__(self, flag_overrides: Dict[str, Any]):
        self.flag_overrides = flag_overrides
        self.original_values = {}
        self.feature_flags = get_feature_flags()
    
    def __enter__(self):
        # Save original values and set overrides
        for flag_name, override_value in self.flag_overrides.items():
            self.original_values[flag_name] = self.feature_flags.get_flag_value(flag_name)
            self.feature_flags.set_flag(flag_name, override_value, "Temporary override")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original values
        for flag_name, original_value in self.original_values.items():
            self.feature_flags.set_flag(flag_name, original_value, "Restore from temporary override") 