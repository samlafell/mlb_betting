#!/usr/bin/env python3
"""
Enhanced Collection Orchestrator with Silent Failure Resolution

Extended orchestrator that integrates health monitoring, circuit breakers, and automatic
recovery to address silent failure modes. Part of solution for GitHub Issue #36.
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

import structlog

from ...core.config import get_settings
from ...core.logging import get_logger, LogComponent
from .base import BaseCollector, CollectionRequest, CollectionResult, CollectionStatus
from .health_monitoring import (
    AlertSeverity,
    CollectionAlert,
    CollectionConfidenceAnalyzer,
    CollectionHealthMetrics,
    CollectionHealthResult,
    FailurePattern,
    HealthStatus
)
from .alert_manager import CollectionAlertManager
from .circuit_breaker import (
    CircuitBreakerConfig,
    CircuitBreakerManager,
    CircuitBreakerOpenError,
    RecoveryStrategy,
    circuit_breaker_manager
)
from .orchestrator import CollectionOrchestrator, CollectionPlan, CollectionTask

logger = get_logger(__name__, LogComponent.CORE)


class RecoveryAction(Enum):
    """Types of recovery actions."""
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    SWITCH_TO_FALLBACK = "switch_to_fallback"
    ENABLE_DEGRADED_MODE = "enable_degraded_mode"
    RESTART_COLLECTOR = "restart_collector"
    ALERT_MANUAL_INTERVENTION = "alert_manual_intervention"


@dataclass
class RecoveryPlan:
    """Plan for recovering from collection failures."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    source: str = ""
    failure_patterns: List[FailurePattern] = field(default_factory=list)
    
    # Recovery actions in order of preference
    recovery_actions: List[RecoveryAction] = field(default_factory=list)
    
    # Current recovery state
    current_action_index: int = 0
    attempts_for_current_action: int = 0
    max_attempts_per_action: int = 3
    
    # Timing
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Status
    is_active: bool = False
    is_successful: bool = False
    
    @property
    def current_action(self) -> Optional[RecoveryAction]:
        """Get current recovery action."""
        if self.current_action_index < len(self.recovery_actions):
            return self.recovery_actions[self.current_action_index]
        return None
    
    @property
    def has_more_actions(self) -> bool:
        """Check if there are more recovery actions to try."""
        return self.current_action_index < len(self.recovery_actions)
    
    def advance_to_next_action(self) -> None:
        """Advance to the next recovery action."""
        self.current_action_index += 1
        self.attempts_for_current_action = 0


class EnhancedCollectionOrchestrator(CollectionOrchestrator):
    """
    Enhanced orchestrator with silent failure resolution capabilities.
    
    Features:
    - Health monitoring with confidence scoring
    - Circuit breaker integration with automatic recovery
    - Multi-level recovery strategies
    - Real-time gap detection and alerting
    - Comprehensive failure pattern analysis
    """
    
    def __init__(self, settings=None, repository=None):
        super().__init__(settings, repository)
        
        # Enhanced monitoring components
        self.confidence_analyzer = CollectionConfidenceAnalyzer()
        self.alert_manager = CollectionAlertManager()
        self.circuit_breaker_manager = circuit_breaker_manager
        
        # Health monitoring
        self.health_metrics: Dict[str, CollectionHealthMetrics] = {}
        self.recovery_plans: Dict[str, RecoveryPlan] = {}
        
        # Settings
        self.enhanced_settings = {
            "enable_health_monitoring": True,
            "enable_circuit_breakers": True,
            "enable_automatic_recovery": True,
            "gap_detection_threshold_hours": 4.0,
            "confidence_threshold": 0.7,
            "max_consecutive_failures": 5
        }
        
        # Initialize circuit breakers for all sources
        self._initialize_circuit_breakers()
        
        self.logger.info("Enhanced collection orchestrator initialized")
    
    async def initialize(self) -> None:
        """Initialize async components that require database connections."""
        try:
            await self.alert_manager.initialize_db_connection()
            self.logger.info("Enhanced orchestrator async initialization completed")
        except Exception as e:
            self.logger.error(f"Failed to initialize async components: {e}")
            raise
    
    def _initialize_circuit_breakers(self) -> None:
        """Initialize circuit breakers for all data sources."""
        # Load configuration from config.toml
        settings = get_settings()
        cb_config_data = getattr(settings, 'collection', {}).get('circuit_breaker', {})
        
        for source_name, config in self.source_configs.items():
            # Map recovery strategy from config string
            strategy_mapping = {
                'immediate': RecoveryStrategy.IMMEDIATE,
                'exponential_backoff': RecoveryStrategy.EXPONENTIAL_BACKOFF,
                'scheduled': RecoveryStrategy.SCHEDULED
            }
            recovery_strategy_str = cb_config_data.get('recovery_strategy', 'exponential_backoff')
            recovery_strategy = strategy_mapping.get(recovery_strategy_str, RecoveryStrategy.EXPONENTIAL_BACKOFF)
            
            # Create circuit breaker configuration from config.toml with fallback defaults
            cb_config = CircuitBreakerConfig(
                failure_threshold=cb_config_data.get('failure_threshold', 3),
                timeout_duration_seconds=cb_config_data.get('timeout_duration_seconds', 300),
                recovery_strategy=recovery_strategy,
                enable_automatic_recovery=cb_config_data.get('enable_automatic_recovery', True),
                enable_degraded_mode=cb_config_data.get('enable_degraded_mode', True)
            )
            
            # Create health checker for this source
            health_checker = self._create_health_checker(source_name)
            
            # Create circuit breaker
            circuit_breaker = self.circuit_breaker_manager.create_circuit_breaker(
                name=source_name,
                config=cb_config,
                fallback_handler=self._create_fallback_handler(source_name),
                health_checker=health_checker
            )
            
            # Add alert callback
            circuit_breaker.add_alert_callback(self.alert_manager.send_alert)
            
            self.logger.info("Circuit breaker initialized", source=source_name)
    
    def _create_health_checker(self, source_name: str) -> Callable:
        """Create health checker function for a source."""
        async def health_check() -> bool:
            try:
                # Get collector for health check
                collector = await self._get_collector(source_name)
                
                # Perform basic connection test
                return await collector.test_connection()
                
            except Exception as e:
                self.logger.debug("Health check failed", source=source_name, error=str(e))
                return False
        
        return health_check
    
    def _create_fallback_handler(self, source_name: str) -> Callable:
        """Create fallback handler for a source."""
        async def fallback_handler(*args, **kwargs) -> CollectionResult:
            self.logger.info("Using fallback handler", source=source_name)
            
            # Return minimal valid result for degraded mode
            return CollectionResult(
                success=True,
                data=[],
                source=f"{source_name}_fallback",
                timestamp=datetime.now(),
                metadata={"degraded_mode": True, "fallback_active": True}
            )
        
        return fallback_handler
    
    async def _execute_single_task_with_recovery(self, task: CollectionTask) -> None:
        """Enhanced single task execution with recovery strategies."""
        circuit_breaker = self.circuit_breaker_manager.get_circuit_breaker(task.source_name)
        
        task.status = CollectionStatus.RUNNING
        task.started_at = datetime.now()
        
        # Track this collection attempt
        if task.source_name not in self.health_metrics:
            self.health_metrics[task.source_name] = CollectionHealthMetrics(
                source=task.source_name
            )
        
        health_metrics = self.health_metrics[task.source_name]
        health_metrics.total_collections += 1
        
        try:
            # Execute through circuit breaker if available
            if circuit_breaker:
                result = await circuit_breaker.call(
                    self._execute_collection_with_health_monitoring,
                    task,
                    fallback_enabled=True
                )
            else:
                result = await self._execute_collection_with_health_monitoring(task)
            
            # Analyze result health
            enhanced_result = await self._analyze_collection_health(result, health_metrics)
            
            # Update task with enhanced result
            task.result = enhanced_result
            
            # Update health metrics
            await self._update_health_metrics(task.source_name, enhanced_result)
            
            # Check for alerts
            alerts = await self.alert_manager.evaluate_health_result(enhanced_result)
            for alert in alerts:
                await self.alert_manager.send_alert(alert)
            
            # Determine final task status
            if enhanced_result.success and enhanced_result.confidence_score >= self.enhanced_settings["confidence_threshold"]:
                task.status = CollectionStatus.SUCCESS
                health_metrics.successful_collections += 1
                health_metrics.consecutive_failures = 0
            else:
                task.status = CollectionStatus.FAILED
                health_metrics.failed_collections += 1
                health_metrics.consecutive_failures += 1
                
                # Start recovery if needed
                await self._initiate_recovery_if_needed(task.source_name, enhanced_result)
        
        except CircuitBreakerOpenError:
            task.status = CollectionStatus.FAILED
            task.last_error = "Circuit breaker is open"
            health_metrics.failed_collections += 1
            health_metrics.consecutive_failures += 1
            
        except Exception as e:
            task.status = CollectionStatus.FAILED
            task.last_error = str(e)
            health_metrics.failed_collections += 1
            health_metrics.consecutive_failures += 1
            
            self.logger.error("Task execution failed", 
                            task_id=task.id, 
                            source=task.source_name, 
                            error=str(e))
        
        finally:
            task.completed_at = datetime.now()
    
    async def _execute_collection_with_health_monitoring(self, task: CollectionTask) -> CollectionResult:
        """Execute collection with enhanced health monitoring."""
        start_time = time.time()
        
        try:
            # Get collector
            collector = await self._get_collector(task.source_name)
            
            # Execute collection with timeout
            result = await asyncio.wait_for(
                collector.collect(**task.params),
                timeout=task.timeout_seconds
            )
            
            # Calculate response time
            response_time_ms = (time.time() - start_time) * 1000
            result.response_time_ms = response_time_ms
            
            return result
            
        except asyncio.TimeoutError:
            response_time_ms = (time.time() - start_time) * 1000
            return CollectionResult(
                success=False,
                data=[],
                source=task.source_name,
                timestamp=datetime.now(),
                errors=["Collection timed out"],
                response_time_ms=response_time_ms
            )
    
    async def _analyze_collection_health(
        self, 
        result: CollectionResult, 
        historical_metrics: CollectionHealthMetrics
    ) -> CollectionHealthResult:
        """Convert and analyze collection result for health assessment."""
        
        # Convert to health result
        health_result = CollectionHealthResult(
            success=result.success,
            data=result.data,
            source=result.source,
            timestamp=result.timestamp,
            errors=result.errors,
            response_time_ms=getattr(result, 'response_time_ms', 0.0),
            request_count=getattr(result, 'request_count', 1)
        )
        
        # Analyze with confidence analyzer
        enhanced_result = self.confidence_analyzer.analyze_result(health_result, historical_metrics)
        
        return enhanced_result
    
    async def _update_health_metrics(self, source: str, result: CollectionHealthResult) -> None:
        """Update health metrics for a source."""
        metrics = self.health_metrics[source]
        
        # Update basic metrics
        metrics.records_collected += result.data_count
        metrics.confidence_score = result.confidence_score
        metrics.avg_response_time_ms = (
            (metrics.avg_response_time_ms + result.response_time_ms) / 2
            if metrics.avg_response_time_ms > 0 else result.response_time_ms
        )
        
        # Update success metrics
        if result.success:
            metrics.last_successful_collection = datetime.now()
            metrics.gap_duration_hours = 0.0
        else:
            # Calculate gap duration
            if metrics.last_successful_collection:
                gap_duration = datetime.now() - metrics.last_successful_collection
                metrics.gap_duration_hours = gap_duration.total_seconds() / 3600
        
        # Update success rate
        if metrics.total_collections > 0:
            metrics.success_rate = (metrics.successful_collections / metrics.total_collections) * 100
        
        # Update failure patterns
        metrics.failure_patterns = list(set(metrics.failure_patterns + result.detected_patterns))
        
        # Determine alert level
        if metrics.consecutive_failures >= self.enhanced_settings["max_consecutive_failures"]:
            metrics.alert_level = "critical"
        elif metrics.consecutive_failures >= 3:
            metrics.alert_level = "warning"
        else:
            metrics.alert_level = "normal"
        
        # Store to database
        await self._store_health_metrics(metrics)
    
    async def _store_health_metrics(self, metrics: CollectionHealthMetrics) -> None:
        """Store health metrics to database."""
        try:
            if not hasattr(self, 'db_pool') or not self.db_pool:
                await self.alert_manager.initialize_db_connection()
                self.db_pool = self.alert_manager.db_pool
            
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO operational.collection_health_monitoring (
                            source, collection_timestamp, records_collected,
                            success_rate, confidence_score, avg_response_time_ms,
                            last_successful_collection, gap_duration_hours,
                            consecutive_failures, health_status, alert_level,
                            metadata, failure_patterns
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    """, 
                    metrics.source, metrics.timestamp, metrics.records_collected,
                    metrics.success_rate, metrics.confidence_score, metrics.avg_response_time_ms,
                    metrics.last_successful_collection, metrics.gap_duration_hours,
                    metrics.consecutive_failures, 
                    "healthy" if metrics.is_healthy else ("degraded" if metrics.is_degraded else "critical"),
                    metrics.alert_level,
                    {"total_collections": metrics.total_collections, "failed_collections": metrics.failed_collections},
                    [fp.value for fp in metrics.failure_patterns]
                    )
        except Exception as e:
            self.logger.error("Failed to store health metrics", source=metrics.source, error=str(e))
    
    async def _initiate_recovery_if_needed(self, source: str, result: CollectionHealthResult) -> None:
        """Initiate recovery process if needed based on failure patterns."""
        metrics = self.health_metrics[source]
        
        # Check if recovery is needed
        should_recover = (
            metrics.consecutive_failures >= 3 or
            result.confidence_score < 0.5 or
            metrics.gap_duration_hours >= 2.0
        )
        
        if not should_recover:
            return
        
        # Check if recovery is already in progress
        if source in self.recovery_plans and self.recovery_plans[source].is_active:
            return
        
        # Create recovery plan based on failure patterns
        recovery_plan = self._create_recovery_plan(source, result.detected_patterns, metrics)
        self.recovery_plans[source] = recovery_plan
        
        # Start recovery process
        await self._execute_recovery_plan(recovery_plan)
    
    def _create_recovery_plan(
        self, 
        source: str, 
        failure_patterns: List[FailurePattern],
        metrics: CollectionHealthMetrics
    ) -> RecoveryPlan:
        """Create a recovery plan based on failure patterns."""
        
        recovery_actions = []
        
        # Determine recovery actions based on failure patterns
        if FailurePattern.RATE_LIMITING in failure_patterns:
            recovery_actions.extend([
                RecoveryAction.RETRY_WITH_BACKOFF,
                RecoveryAction.ENABLE_DEGRADED_MODE
            ])
        
        if FailurePattern.NETWORK_TIMEOUT in failure_patterns:
            recovery_actions.extend([
                RecoveryAction.RETRY_WITH_BACKOFF,
                RecoveryAction.RESTART_COLLECTOR,
                RecoveryAction.SWITCH_TO_FALLBACK
            ])
        
        if FailurePattern.SCHEMA_CHANGE in failure_patterns:
            recovery_actions.extend([
                RecoveryAction.ALERT_MANUAL_INTERVENTION,
                RecoveryAction.ENABLE_DEGRADED_MODE
            ])
        
        if FailurePattern.SYSTEMATIC_FAILURE in failure_patterns:
            recovery_actions.extend([
                RecoveryAction.RESTART_COLLECTOR,
                RecoveryAction.SWITCH_TO_FALLBACK,
                RecoveryAction.ALERT_MANUAL_INTERVENTION
            ])
        
        # Default recovery actions if no specific patterns
        if not recovery_actions:
            recovery_actions = [
                RecoveryAction.RETRY_WITH_BACKOFF,
                RecoveryAction.RESTART_COLLECTOR,
                RecoveryAction.ENABLE_DEGRADED_MODE,
                RecoveryAction.ALERT_MANUAL_INTERVENTION
            ]
        
        return RecoveryPlan(
            source=source,
            failure_patterns=failure_patterns,
            recovery_actions=recovery_actions
        )
    
    async def _execute_recovery_plan(self, plan: RecoveryPlan) -> None:
        """Execute a recovery plan."""
        plan.is_active = True
        plan.started_at = datetime.now()
        
        self.logger.info("Starting recovery plan", 
                        source=plan.source, 
                        plan_id=plan.id,
                        actions=len(plan.recovery_actions))
        
        try:
            while plan.has_more_actions and plan.attempts_for_current_action < plan.max_attempts_per_action:
                action = plan.current_action
                plan.attempts_for_current_action += 1
                
                self.logger.info("Executing recovery action", 
                               source=plan.source,
                               action=action.value,
                               attempt=plan.attempts_for_current_action)
                
                success = await self._execute_recovery_action(plan.source, action)
                
                if success:
                    plan.is_successful = True
                    break
                elif plan.attempts_for_current_action >= plan.max_attempts_per_action:
                    plan.advance_to_next_action()
        
        except Exception as e:
            self.logger.error("Recovery plan execution failed", 
                            source=plan.source, 
                            plan_id=plan.id, 
                            error=str(e))
        
        finally:
            plan.is_active = False
            plan.completed_at = datetime.now()
            
            if plan.is_successful:
                self.logger.info("Recovery plan completed successfully", 
                               source=plan.source, plan_id=plan.id)
            else:
                self.logger.error("Recovery plan failed", 
                                source=plan.source, plan_id=plan.id)
    
    async def _execute_recovery_action(self, source: str, action: RecoveryAction) -> bool:
        """Execute a specific recovery action."""
        try:
            if action == RecoveryAction.RETRY_WITH_BACKOFF:
                # Wait with exponential backoff then test
                await asyncio.sleep(2 ** min(3, self.recovery_plans[source].attempts_for_current_action))
                return await self._test_source_health(source)
            
            elif action == RecoveryAction.RESTART_COLLECTOR:
                # Restart the collector
                if source in self.collectors:
                    await self.collectors[source].cleanup()
                    del self.collectors[source]
                
                # Create new collector
                await self._get_collector(source)
                return await self._test_source_health(source)
            
            elif action == RecoveryAction.SWITCH_TO_FALLBACK:
                # Enable fallback mode for this source
                circuit_breaker = self.circuit_breaker_manager.get_circuit_breaker(source)
                if circuit_breaker:
                    await circuit_breaker.force_open()
                return True
            
            elif action == RecoveryAction.ENABLE_DEGRADED_MODE:
                # Enable degraded mode (always succeeds)
                return True
            
            elif action == RecoveryAction.ALERT_MANUAL_INTERVENTION:
                # Send alert for manual intervention
                alert = CollectionAlert(
                    source=source,
                    alert_type="manual_intervention_required",
                    severity=AlertSeverity.CRITICAL,
                    message=f"Manual intervention required for {source} - automatic recovery failed",
                    recovery_suggestions=[
                        "Check collector configuration and source availability",
                        "Review collector logs for detailed error information",
                        "Contact development team if issue persists"
                    ],
                    is_auto_recoverable=False
                )
                await self.alert_manager.send_alert(alert)
                return False  # Manual intervention required
            
            return False
            
        except Exception as e:
            self.logger.error("Recovery action failed", 
                            source=source, 
                            action=action.value, 
                            error=str(e))
            return False
    
    async def _test_source_health(self, source: str) -> bool:
        """Test if a source is healthy after recovery."""
        try:
            # Get collector and test connection
            collector = await self._get_collector(source)
            return await collector.test_connection()
        except Exception:
            return False
    
    async def check_collection_gaps_all_sources(self) -> List[CollectionAlert]:
        """Check for collection gaps across all sources."""
        alerts = []
        
        for source_name in self.source_configs.keys():
            gap_alert = await self.alert_manager.check_collection_gaps(
                source_name, 
                self.enhanced_settings["gap_detection_threshold_hours"]
            )
            if gap_alert:
                alerts.append(gap_alert)
                await self.alert_manager.send_alert(gap_alert)
        
        return alerts
    
    async def check_dead_tuple_accumulation(self) -> List[CollectionAlert]:
        """Check for dead tuple accumulation across database tables."""
        alerts = await self.alert_manager.check_dead_tuple_accumulation()
        
        for alert in alerts:
            await self.alert_manager.send_alert(alert)
        
        return alerts
    
    def get_enhanced_metrics(self) -> Dict[str, Any]:
        """Get enhanced orchestrator metrics including health monitoring."""
        base_metrics = super().get_metrics()
        
        # Add health monitoring metrics
        health_summary = {}
        for source, metrics in self.health_metrics.items():
            health_summary[source] = {
                "health_status": "healthy" if metrics.is_healthy else ("degraded" if metrics.is_degraded else "critical"),
                "success_rate": metrics.success_rate,
                "confidence_score": metrics.confidence_score,
                "consecutive_failures": metrics.consecutive_failures,
                "gap_duration_hours": metrics.gap_duration_hours,
                "last_successful_collection": metrics.last_successful_collection.isoformat() if metrics.last_successful_collection else None
            }
        
        # Add circuit breaker status
        circuit_breaker_status = self.circuit_breaker_manager.get_all_status()
        
        # Add recovery plan status
        recovery_status = {}
        for source, plan in self.recovery_plans.items():
            recovery_status[source] = {
                "is_active": plan.is_active,
                "is_successful": plan.is_successful,
                "current_action": plan.current_action.value if plan.current_action else None,
                "attempts": plan.attempts_for_current_action
            }
        
        return {
            **base_metrics,
            "health_monitoring": {
                "enabled": self.enhanced_settings["enable_health_monitoring"],
                "source_health": health_summary,
                "total_sources_monitored": len(self.health_metrics),
                "healthy_sources": len([m for m in self.health_metrics.values() if m.is_healthy]),
                "degraded_sources": len([m for m in self.health_metrics.values() if m.is_degraded]),
                "critical_sources": len([m for m in self.health_metrics.values() if m.is_critical])
            },
            "circuit_breakers": circuit_breaker_status,
            "recovery_plans": recovery_status,
            "alert_manager": self.alert_manager.get_alert_summary()
        }
    
    async def force_health_check_all_sources(self) -> Dict[str, bool]:
        """Force health check on all sources."""
        results = {}
        
        for source_name in self.source_configs.keys():
            results[source_name] = await self._test_source_health(source_name)
        
        return results


__all__ = [
    "RecoveryAction",
    "RecoveryPlan", 
    "EnhancedCollectionOrchestrator"
]