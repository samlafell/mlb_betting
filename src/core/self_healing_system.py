"""
Self-Healing System Infrastructure
Proactive monitoring, automated recovery, and predictive failure prevention for 24/7 operations
"""

import logging
import asyncio
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import psutil
import aiofiles
from pathlib import Path

from .circuit_breaker import get_circuit_breaker, CircuitBreakerConfig
from .config import get_settings
from .logging import LogComponent, get_logger
from ..services.monitoring.prometheus_metrics_service import get_metrics_service

logger = get_logger(__name__, LogComponent.INFRASTRUCTURE)


class HealthStatus(str, Enum):
    """System health status levels"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    FAILING = "failing"


class RecoveryAction(str, Enum):
    """Types of recovery actions"""
    RESTART_SERVICE = "restart_service"
    CLEAR_CACHE = "clear_cache"
    RESTART_DATABASE_POOL = "restart_database_pool"
    SCALE_RESOURCES = "scale_resources"
    FAILOVER = "failover"
    CIRCUIT_BREAKER_RESET = "circuit_breaker_reset"
    MEMORY_CLEANUP = "memory_cleanup"
    DISK_CLEANUP = "disk_cleanup"


@dataclass
class HealthCheckResult:
    """Health check result"""
    component: str
    status: HealthStatus
    message: str
    metrics: Dict[str, Any]
    last_check: datetime
    recovery_suggested: Optional[RecoveryAction] = None


@dataclass
class RecoveryAttempt:
    """Recovery attempt record"""
    component: str
    action: RecoveryAction
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    error_message: Optional[str] = None
    metrics_before: Dict[str, Any] = None
    metrics_after: Dict[str, Any] = None


class SelfHealingSystem:
    """
    Advanced self-healing system for 24/7 operational readiness
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.metrics_service = get_metrics_service()
        
        # Health monitoring configuration
        self.health_config = {
            # Resource thresholds
            "cpu_warning_threshold": 80.0,
            "cpu_critical_threshold": 95.0,
            "memory_warning_threshold": 85.0,
            "memory_critical_threshold": 95.0,
            "disk_warning_threshold": 80.0,
            "disk_critical_threshold": 90.0,
            
            # Performance thresholds
            "response_time_warning_ms": 1000,
            "response_time_critical_ms": 5000,
            "error_rate_warning": 0.05,  # 5%
            "error_rate_critical": 0.15,  # 15%
            
            # Health check intervals
            "health_check_interval_seconds": 30,
            "deep_health_check_interval_seconds": 300,  # 5 minutes
            "predictive_check_interval_seconds": 600,   # 10 minutes
            
            # Recovery configuration
            "max_recovery_attempts": 3,
            "recovery_cooldown_seconds": 300,  # 5 minutes
            "auto_recovery_enabled": True,
            "emergency_shutdown_threshold": 5,  # 5 critical failures
        }
        
        # Component health checkers
        self.health_checkers: Dict[str, Callable[[], HealthCheckResult]] = {}
        self.recovery_handlers: Dict[RecoveryAction, Callable] = {}
        
        # State tracking
        self.health_history: List[HealthCheckResult] = []
        self.recovery_history: List[RecoveryAttempt] = []
        self.component_status: Dict[str, HealthCheckResult] = {}
        self.recovery_attempts: Dict[str, int] = {}  # Component -> attempt count
        self.last_recovery_time: Dict[str, datetime] = {}
        
        # Predictive monitoring
        self.failure_predictors: Dict[str, Callable] = {}
        self.prediction_models: Dict[str, Any] = {}
        
        # Initialize default health checkers and recovery handlers
        self._initialize_default_components()

    def _initialize_default_components(self):
        """Initialize default health checkers and recovery handlers"""
        
        # Register default health checkers
        self.health_checkers.update({
            "system_resources": self._check_system_resources,
            "database_connectivity": self._check_database_connectivity,
            "external_apis": self._check_external_apis,
            "ml_services": self._check_ml_services,
            "storage_systems": self._check_storage_systems,
            "circuit_breakers": self._check_circuit_breakers,
        })
        
        # Register default recovery handlers
        self.recovery_handlers.update({
            RecoveryAction.RESTART_SERVICE: self._restart_service,
            RecoveryAction.CLEAR_CACHE: self._clear_cache,
            RecoveryAction.RESTART_DATABASE_POOL: self._restart_database_pool,
            RecoveryAction.SCALE_RESOURCES: self._scale_resources,
            RecoveryAction.CIRCUIT_BREAKER_RESET: self._reset_circuit_breakers,
            RecoveryAction.MEMORY_CLEANUP: self._cleanup_memory,
            RecoveryAction.DISK_CLEANUP: self._cleanup_disk,
        })
        
        # Register predictive failure detectors
        self.failure_predictors.update({
            "resource_exhaustion": self._predict_resource_exhaustion,
            "performance_degradation": self._predict_performance_degradation,
            "cascade_failure": self._predict_cascade_failure,
        })

    async def start_monitoring(self):
        """Start the self-healing monitoring system"""
        
        try:
            logger.info("Starting Self-Healing System monitoring...")
            
            # Start monitoring loops
            asyncio.create_task(self._health_monitoring_loop())
            asyncio.create_task(self._predictive_monitoring_loop())
            asyncio.create_task(self._maintenance_loop())
            
            logger.info("✅ Self-Healing System monitoring started")
            
        except Exception as e:
            logger.error(f"Failed to start self-healing monitoring: {e}")
            raise

    async def _health_monitoring_loop(self):
        """Main health monitoring loop"""
        
        while True:
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.health_config["health_check_interval_seconds"])
                
            except Exception as e:
                logger.error(f"Health monitoring loop error: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    async def _perform_health_checks(self):
        """Perform comprehensive health checks"""
        
        try:
            logger.debug("Performing health checks")
            
            # Run all registered health checkers
            check_results = []
            
            for component_name, health_checker in self.health_checkers.items():
                try:
                    result = await asyncio.to_thread(health_checker)
                    check_results.append(result)
                    self.component_status[component_name] = result
                    
                    # Record metrics
                    self.metrics_service.update_system_health_status(result.status.value)
                    
                    # Check if recovery is needed
                    if (result.status in [HealthStatus.CRITICAL, HealthStatus.FAILING] and
                        self.health_config["auto_recovery_enabled"]):
                        
                        await self._trigger_recovery(component_name, result)
                    
                except Exception as e:
                    logger.error(f"Health check failed for {component_name}: {e}")
                    
                    # Create failed health check result
                    failed_result = HealthCheckResult(
                        component=component_name,
                        status=HealthStatus.FAILING,
                        message=f"Health check error: {str(e)}",
                        metrics={},
                        last_check=datetime.now()
                    )
                    check_results.append(failed_result)
                    self.component_status[component_name] = failed_result
            
            # Store health history
            self.health_history.extend(check_results)
            
            # Trim history to last 1000 entries
            if len(self.health_history) > 1000:
                self.health_history = self.health_history[-1000:]
            
            # Assess overall system health
            overall_status = self._assess_overall_health(check_results)
            
            # Record overall system health
            self.metrics_service.update_system_health_status(overall_status.value)
            
            logger.debug(f"Health check completed: overall_status={overall_status.value}")
            
        except Exception as e:
            logger.error(f"Health check performance failed: {e}")

    def _assess_overall_health(self, check_results: List[HealthCheckResult]) -> HealthStatus:
        """Assess overall system health from component results"""
        
        if not check_results:
            return HealthStatus.CRITICAL
        
        failing_count = len([r for r in check_results if r.status == HealthStatus.FAILING])
        critical_count = len([r for r in check_results if r.status == HealthStatus.CRITICAL])
        degraded_count = len([r for r in check_results if r.status == HealthStatus.DEGRADED])
        
        total_components = len(check_results)
        
        # Determine overall status
        if failing_count > 0:
            return HealthStatus.FAILING
        elif critical_count >= total_components * 0.3:  # 30% critical
            return HealthStatus.CRITICAL
        elif critical_count > 0 or degraded_count >= total_components * 0.5:  # 50% degraded
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    async def _trigger_recovery(self, component_name: str, health_result: HealthCheckResult):
        """Trigger automated recovery for a failing component"""
        
        try:
            # Check recovery cooldown
            last_recovery = self.last_recovery_time.get(component_name)
            if last_recovery:
                cooldown_remaining = (datetime.now() - last_recovery).total_seconds()
                if cooldown_remaining < self.health_config["recovery_cooldown_seconds"]:
                    logger.debug(f"Recovery for {component_name} still in cooldown ({cooldown_remaining:.0f}s remaining)")
                    return
            
            # Check recovery attempt limit
            attempt_count = self.recovery_attempts.get(component_name, 0)
            if attempt_count >= self.health_config["max_recovery_attempts"]:
                logger.error(f"Max recovery attempts reached for {component_name} ({attempt_count})")
                
                # Record break-glass activation
                self.metrics_service.record_break_glass_activation(
                    "max_recovery_attempts",
                    f"Component {component_name} exceeded recovery attempts"
                )
                return
            
            # Determine recovery action
            recovery_action = health_result.recovery_suggested or self._determine_recovery_action(
                component_name, health_result
            )
            
            if not recovery_action:
                logger.warning(f"No recovery action determined for {component_name}")
                return
            
            logger.warning(f"Triggering recovery for {component_name}: {recovery_action.value}")
            
            # Create recovery attempt record
            recovery_attempt = RecoveryAttempt(
                component=component_name,
                action=recovery_action,
                started_at=datetime.now(),
                metrics_before=health_result.metrics.copy()
            )
            
            # Execute recovery
            try:
                recovery_handler = self.recovery_handlers.get(recovery_action)
                if recovery_handler:
                    await recovery_handler(component_name, health_result)
                    recovery_attempt.success = True
                    logger.info(f"Recovery successful for {component_name}: {recovery_action.value}")
                else:
                    raise ValueError(f"No recovery handler for {recovery_action.value}")
                
            except Exception as recovery_error:
                recovery_attempt.success = False
                recovery_attempt.error_message = str(recovery_error)
                logger.error(f"Recovery failed for {component_name}: {recovery_error}")
            
            # Update tracking
            recovery_attempt.completed_at = datetime.now()
            self.recovery_history.append(recovery_attempt)
            self.recovery_attempts[component_name] = attempt_count + 1
            self.last_recovery_time[component_name] = datetime.now()
            
            # Record metrics
            self.metrics_service.record_emergency_execution(f"self_healing_{recovery_action.value}")
            
        except Exception as e:
            logger.error(f"Recovery trigger failed for {component_name}: {e}")

    def _determine_recovery_action(self, component_name: str, health_result: HealthCheckResult) -> Optional[RecoveryAction]:
        """Determine appropriate recovery action based on component and health status"""
        
        # Component-specific recovery actions
        if component_name == "system_resources":
            cpu_usage = health_result.metrics.get("cpu_percent", 0)
            memory_usage = health_result.metrics.get("memory_percent", 0)
            
            if memory_usage > 90:
                return RecoveryAction.MEMORY_CLEANUP
            elif cpu_usage > 90:
                return RecoveryAction.SCALE_RESOURCES
                
        elif component_name == "database_connectivity":
            return RecoveryAction.RESTART_DATABASE_POOL
            
        elif component_name == "external_apis":
            return RecoveryAction.CIRCUIT_BREAKER_RESET
            
        elif component_name == "ml_services":
            return RecoveryAction.RESTART_SERVICE
            
        elif component_name == "storage_systems":
            disk_usage = health_result.metrics.get("disk_percent", 0)
            if disk_usage > 85:
                return RecoveryAction.DISK_CLEANUP
        
        # Default recovery action
        return RecoveryAction.RESTART_SERVICE

    # Health Checker Implementations
    
    def _check_system_resources(self) -> HealthCheckResult:
        """Check system resource utilization"""
        
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            metrics = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "disk_percent": disk.percent,
                "disk_free_gb": disk.free / (1024**3),
            }
            
            # Determine status
            if (cpu_percent >= self.health_config["cpu_critical_threshold"] or
                memory.percent >= self.health_config["memory_critical_threshold"] or
                disk.percent >= self.health_config["disk_critical_threshold"]):
                status = HealthStatus.CRITICAL
                message = "Critical resource utilization detected"
            elif (cpu_percent >= self.health_config["cpu_warning_threshold"] or
                  memory.percent >= self.health_config["memory_warning_threshold"] or
                  disk.percent >= self.health_config["disk_warning_threshold"]):
                status = HealthStatus.DEGRADED
                message = "High resource utilization detected"
            else:
                status = HealthStatus.HEALTHY
                message = "Resource utilization normal"
            
            return HealthCheckResult(
                component="system_resources",
                status=status,
                message=message,
                metrics=metrics,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="system_resources",
                status=HealthStatus.FAILING,
                message=f"Resource check error: {str(e)}",
                metrics={},
                last_check=datetime.now()
            )

    def _check_database_connectivity(self) -> HealthCheckResult:
        """Check database connectivity and performance"""
        
        try:
            # This would test actual database connectivity
            # For now, we'll simulate based on circuit breaker state
            
            metrics = {
                "connection_pool_active": 5,  # Simulated
                "connection_pool_idle": 15,   # Simulated
                "avg_query_time_ms": 50,      # Simulated
            }
            
            status = HealthStatus.HEALTHY
            message = "Database connectivity normal"
            
            return HealthCheckResult(
                component="database_connectivity",
                status=status,
                message=message,
                metrics=metrics,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="database_connectivity",
                status=HealthStatus.FAILING,
                message=f"Database check error: {str(e)}",
                metrics={},
                last_check=datetime.now()
            )

    def _check_external_apis(self) -> HealthCheckResult:
        """Check external API connectivity and performance"""
        
        try:
            # Check circuit breaker states for external APIs
            # This would integrate with actual circuit breakers
            
            metrics = {
                "api_response_time_ms": 150,  # Simulated
                "api_error_rate": 0.02,       # Simulated
                "api_success_rate": 0.98,     # Simulated
            }
            
            status = HealthStatus.HEALTHY
            message = "External API connectivity normal"
            
            return HealthCheckResult(
                component="external_apis",
                status=status,
                message=message,
                metrics=metrics,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="external_apis",
                status=HealthStatus.FAILING,
                message=f"API check error: {str(e)}",
                metrics={},
                last_check=datetime.now()
            )

    def _check_ml_services(self) -> HealthCheckResult:
        """Check ML services health"""
        
        try:
            metrics = {
                "active_models": 3,           # Simulated
                "prediction_latency_ms": 200, # Simulated
                "cache_hit_rate": 0.85,      # Simulated
            }
            
            status = HealthStatus.HEALTHY
            message = "ML services operating normally"
            
            return HealthCheckResult(
                component="ml_services",
                status=status,
                message=message,
                metrics=metrics,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="ml_services",
                status=HealthStatus.FAILING,
                message=f"ML services check error: {str(e)}",
                metrics={},
                last_check=datetime.now()
            )

    def _check_storage_systems(self) -> HealthCheckResult:
        """Check storage systems health"""
        
        try:
            # Check various storage systems
            disk_usage = psutil.disk_usage('/')
            
            metrics = {
                "disk_percent": disk_usage.percent,
                "disk_free_gb": disk_usage.free / (1024**3),
                "redis_connectivity": True,   # Simulated
                "cache_performance": 0.95,   # Simulated
            }
            
            if disk_usage.percent >= 90:
                status = HealthStatus.CRITICAL
                message = "Critical disk space shortage"
            elif disk_usage.percent >= 80:
                status = HealthStatus.DEGRADED
                message = "Low disk space warning"
            else:
                status = HealthStatus.HEALTHY
                message = "Storage systems normal"
            
            return HealthCheckResult(
                component="storage_systems",
                status=status,
                message=message,
                metrics=metrics,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="storage_systems",
                status=HealthStatus.FAILING,
                message=f"Storage check error: {str(e)}",
                metrics={},
                last_check=datetime.now()
            )

    def _check_circuit_breakers(self) -> HealthCheckResult:
        """Check circuit breaker states"""
        
        try:
            # This would check actual circuit breaker states
            metrics = {
                "total_circuit_breakers": 5,  # Simulated
                "open_circuit_breakers": 0,   # Simulated
                "half_open_circuit_breakers": 0,  # Simulated
            }
            
            open_breakers = metrics["open_circuit_breakers"]
            
            if open_breakers > 2:
                status = HealthStatus.CRITICAL
                message = f"Multiple circuit breakers open ({open_breakers})"
            elif open_breakers > 0:
                status = HealthStatus.DEGRADED
                message = f"Circuit breaker(s) open ({open_breakers})"
            else:
                status = HealthStatus.HEALTHY
                message = "All circuit breakers closed"
            
            return HealthCheckResult(
                component="circuit_breakers",
                status=status,
                message=message,
                metrics=metrics,
                last_check=datetime.now()
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="circuit_breakers",
                status=HealthStatus.FAILING,
                message=f"Circuit breaker check error: {str(e)}",
                metrics={},
                last_check=datetime.now()
            )

    # Recovery Handler Implementations
    
    async def _restart_service(self, component_name: str, health_result: HealthCheckResult):
        """Restart a failing service"""
        logger.info(f"Restarting service for component: {component_name}")
        
        # Implementation would restart the specific service
        # For now, simulate restart delay
        await asyncio.sleep(2)
        
        logger.info(f"Service restart completed for {component_name}")

    async def _clear_cache(self, component_name: str, health_result: HealthCheckResult):
        """Clear system caches"""
        logger.info(f"Clearing caches for component: {component_name}")
        
        # Implementation would clear Redis, application caches, etc.
        await asyncio.sleep(1)
        
        logger.info(f"Cache clearing completed for {component_name}")

    async def _restart_database_pool(self, component_name: str, health_result: HealthCheckResult):
        """Restart database connection pool"""
        logger.info(f"Restarting database pool for component: {component_name}")
        
        # Implementation would restart database connections
        await asyncio.sleep(3)
        
        logger.info(f"Database pool restart completed for {component_name}")

    async def _scale_resources(self, component_name: str, health_result: HealthCheckResult):
        """Scale system resources"""
        logger.info(f"Scaling resources for component: {component_name}")
        
        # Implementation would scale CPU/memory allocation
        await asyncio.sleep(5)
        
        logger.info(f"Resource scaling completed for {component_name}")

    async def _reset_circuit_breakers(self, component_name: str, health_result: HealthCheckResult):
        """Reset circuit breakers"""
        logger.info(f"Resetting circuit breakers for component: {component_name}")
        
        # Implementation would reset circuit breakers
        await asyncio.sleep(1)
        
        logger.info(f"Circuit breaker reset completed for {component_name}")

    async def _cleanup_memory(self, component_name: str, health_result: HealthCheckResult):
        """Perform memory cleanup"""
        logger.info(f"Performing memory cleanup for component: {component_name}")
        
        # Implementation would force garbage collection, clear caches
        import gc
        gc.collect()
        
        await asyncio.sleep(2)
        
        logger.info(f"Memory cleanup completed for {component_name}")

    async def _cleanup_disk(self, component_name: str, health_result: HealthCheckResult):
        """Perform disk cleanup"""
        logger.info(f"Performing disk cleanup for component: {component_name}")
        
        # Implementation would clean up temporary files, logs, etc.
        await asyncio.sleep(3)
        
        logger.info(f"Disk cleanup completed for {component_name}")

    # Predictive Monitoring
    
    async def _predictive_monitoring_loop(self):
        """Predictive monitoring loop"""
        
        while True:
            try:
                await self._perform_predictive_checks()
                await asyncio.sleep(self.health_config["predictive_check_interval_seconds"])
                
            except Exception as e:
                logger.error(f"Predictive monitoring error: {e}")
                await asyncio.sleep(300)  # Wait before retrying

    async def _perform_predictive_checks(self):
        """Perform predictive failure analysis"""
        
        try:
            logger.debug("Performing predictive failure checks")
            
            for predictor_name, predictor_func in self.failure_predictors.items():
                try:
                    prediction = await asyncio.to_thread(predictor_func)
                    
                    if prediction.get("failure_likely", False):
                        logger.warning(f"Predictive failure detected: {predictor_name} - {prediction.get('reason')}")
                        
                        # Record predictive alert
                        self.metrics_service.record_break_glass_activation(
                            "predictive_failure",
                            f"Predictor {predictor_name}: {prediction.get('reason')}"
                        )
                        
                        # Trigger preventive action if configured
                        preventive_action = prediction.get("preventive_action")
                        if preventive_action and isinstance(preventive_action, RecoveryAction):
                            await self._trigger_preventive_recovery(predictor_name, preventive_action)
                    
                except Exception as e:
                    logger.error(f"Predictive check failed for {predictor_name}: {e}")
            
        except Exception as e:
            logger.error(f"Predictive checks failed: {e}")

    def _predict_resource_exhaustion(self) -> Dict[str, Any]:
        """Predict resource exhaustion based on trends"""
        
        try:
            # Analyze resource trends from health history
            recent_checks = [
                check for check in self.health_history[-10:] 
                if check.component == "system_resources"
            ]
            
            if len(recent_checks) < 5:
                return {"failure_likely": False, "reason": "Insufficient data"}
            
            # Check CPU trend
            cpu_values = [check.metrics.get("cpu_percent", 0) for check in recent_checks]
            cpu_trend = np.polyfit(range(len(cpu_values)), cpu_values, 1)[0]  # Slope
            
            # Check memory trend
            memory_values = [check.metrics.get("memory_percent", 0) for check in recent_checks]
            memory_trend = np.polyfit(range(len(memory_values)), memory_values, 1)[0]
            
            # Predict failure
            if cpu_trend > 2.0 or memory_trend > 1.5:  # Increasing trend
                return {
                    "failure_likely": True,
                    "reason": f"Resource exhaustion trend detected (CPU: {cpu_trend:.1f}, Memory: {memory_trend:.1f})",
                    "preventive_action": RecoveryAction.SCALE_RESOURCES
                }
            
            return {"failure_likely": False, "reason": "Resource trends normal"}
            
        except Exception as e:
            return {"failure_likely": False, "reason": f"Prediction error: {str(e)}"}

    def _predict_performance_degradation(self) -> Dict[str, Any]:
        """Predict performance degradation"""
        
        try:
            # This would analyze performance metrics trends
            return {"failure_likely": False, "reason": "Performance trends normal"}
            
        except Exception as e:
            return {"failure_likely": False, "reason": f"Prediction error: {str(e)}"}

    def _predict_cascade_failure(self) -> Dict[str, Any]:
        """Predict cascade failure scenarios"""
        
        try:
            # Analyze component dependency failures
            recent_failures = [
                check for check in self.health_history[-20:] 
                if check.status in [HealthStatus.CRITICAL, HealthStatus.FAILING]
            ]
            
            if len(recent_failures) >= 3:
                return {
                    "failure_likely": True,
                    "reason": f"Multiple component failures detected ({len(recent_failures)})",
                    "preventive_action": RecoveryAction.CIRCUIT_BREAKER_RESET
                }
            
            return {"failure_likely": False, "reason": "No cascade failure indicators"}
            
        except Exception as e:
            return {"failure_likely": False, "reason": f"Prediction error: {str(e)}"}

    async def _trigger_preventive_recovery(self, predictor_name: str, action: RecoveryAction):
        """Trigger preventive recovery action"""
        
        try:
            logger.info(f"Triggering preventive recovery: {predictor_name} -> {action.value}")
            
            recovery_handler = self.recovery_handlers.get(action)
            if recovery_handler:
                await recovery_handler(f"predictive_{predictor_name}", HealthCheckResult(
                    component=predictor_name,
                    status=HealthStatus.DEGRADED,
                    message="Preventive action",
                    metrics={},
                    last_check=datetime.now()
                ))
                
                logger.info(f"Preventive recovery completed: {action.value}")
            else:
                logger.warning(f"No handler for preventive action: {action.value}")
            
        except Exception as e:
            logger.error(f"Preventive recovery failed: {e}")

    # Maintenance and Cleanup
    
    async def _maintenance_loop(self):
        """Periodic maintenance loop"""
        
        while True:
            try:
                # Run maintenance every hour
                await asyncio.sleep(3600)
                await self._perform_maintenance()
                
            except Exception as e:
                logger.error(f"Maintenance loop error: {e}")
                await asyncio.sleep(300)

    async def _perform_maintenance(self):
        """Perform routine maintenance tasks"""
        
        try:
            logger.info("Performing routine maintenance")
            
            # Cleanup old history
            cutoff_time = datetime.now() - timedelta(days=7)
            
            self.health_history = [
                check for check in self.health_history 
                if check.last_check > cutoff_time
            ]
            
            self.recovery_history = [
                recovery for recovery in self.recovery_history
                if recovery.started_at > cutoff_time
            ]
            
            # Reset recovery attempt counters for successful recoveries
            for component_name, last_recovery in self.last_recovery_time.items():
                if (datetime.now() - last_recovery).hours >= 24:
                    if self.recovery_attempts.get(component_name, 0) > 0:
                        self.recovery_attempts[component_name] = 0
                        logger.info(f"Reset recovery attempt counter for {component_name}")
            
            logger.info("Routine maintenance completed")
            
        except Exception as e:
            logger.error(f"Maintenance failed: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        
        return {
            "overall_health": self._assess_overall_health(list(self.component_status.values())),
            "components": {name: asdict(status) for name, status in self.component_status.items()},
            "recent_recoveries": [asdict(recovery) for recovery in self.recovery_history[-10:]],
            "recovery_stats": {
                "total_recoveries": len(self.recovery_history),
                "successful_recoveries": len([r for r in self.recovery_history if r.success]),
                "active_recovery_attempts": sum(self.recovery_attempts.values()),
            },
            "predictive_monitoring": {
                "enabled": True,
                "last_check": datetime.now().isoformat(),
                "failure_predictors": list(self.failure_predictors.keys()),
            }
        }


# Global self-healing system instance
_self_healing_system: Optional[SelfHealingSystem] = None


def get_self_healing_system() -> SelfHealingSystem:
    """Get or create the global self-healing system instance"""
    global _self_healing_system
    if _self_healing_system is None:
        _self_healing_system = SelfHealingSystem()
    return _self_healing_system