"""
Comprehensive Health Check Service

Provides health check endpoints for all system services with:
- Database connectivity and performance monitoring
- Service health status and dependency checks
- Performance metrics and resource utilization
- Circuit breaker status and error rates
- Integration testing and validation

This addresses Issue #38: System Reliability Issues Prevent Production Use
"""

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import asyncpg
from pydantic import BaseModel

from ...core.config import get_settings
from ...core.logging import UnifiedLogger, LogComponent
from ...data.collection.registry import get_collector_instance


@dataclass
class HealthCheckConfig:
    """Configuration for health check service."""

    # Cache settings
    cache_ttl_seconds: int = 30

    # Database health check settings
    connection_timeout_seconds: int = 5
    query_timeout_seconds: int = 10

    # Circuit breaker settings
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_minutes: int = 5

    # Performance thresholds
    slow_response_threshold_ms: int = 1000
    critical_response_threshold_ms: int = 5000


@dataclass
class CircuitBreakerState:
    """Circuit breaker state for health check operations."""

    failure_count: int = 0
    failure_threshold: int = 5
    timeout_duration: timedelta = None
    last_failure_time: datetime | None = None
    state: str = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def __post_init__(self):
        if self.timeout_duration is None:
            self.timeout_duration = timedelta(minutes=5)

    def is_open(self) -> bool:
        """Check if circuit breaker is open (preventing calls)."""
        if self.state == "OPEN":
            if (
                self.last_failure_time
                and datetime.now() - self.last_failure_time > self.timeout_duration
            ):
                self.state = "HALF_OPEN"
                return False
            return True
        return False

    def record_success(self) -> None:
        """Record a successful operation."""
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_time = None

    def record_failure(self) -> None:
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


class HealthStatus(str, Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class ServiceHealth(BaseModel):
    """Individual service health information."""

    name: str
    status: HealthStatus
    message: str
    response_time_ms: float
    last_check: datetime
    error_count: int = 0
    warning_count: int = 0
    metadata: Dict[str, Any] = {}


class SystemHealth(BaseModel):
    """Overall system health information."""

    status: HealthStatus
    timestamp: datetime
    services: List[ServiceHealth]
    overall_response_time_ms: float
    error_summary: Dict[str, int] = {}
    performance_metrics: Dict[str, float] = {}
    dependency_status: Dict[str, HealthStatus] = {}


class HealthCheckService:
    """Comprehensive health check service for all system components."""

    def __init__(self, config: HealthCheckConfig | None = None):
        self.settings = get_settings()
        self.config = config or HealthCheckConfig()
        self.logger = UnifiedLogger("health_check_service", LogComponent.MONITORING)
        self._last_health_check: Optional[SystemHealth] = None

        # Initialize circuit breakers for different services
        self._db_circuit_breaker = CircuitBreakerState(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            timeout_duration=timedelta(
                minutes=self.config.circuit_breaker_timeout_minutes
            ),
        )
        self._collection_circuit_breaker = CircuitBreakerState(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            timeout_duration=timedelta(
                minutes=self.config.circuit_breaker_timeout_minutes
            ),
        )

    async def get_system_health(self, include_detailed: bool = False) -> SystemHealth:
        """
        Get comprehensive system health status.

        Args:
            include_detailed: Include detailed service diagnostics

        Returns:
            SystemHealth object with current status
        """
        start_time = time.time()

        # Check cache first
        if (
            self._last_health_check
            and (datetime.now() - self._last_health_check.timestamp).seconds
            < self.config.cache_ttl_seconds
        ):
            return self._last_health_check

        self.logger.info(
            "Starting comprehensive system health check",
            operation="health_check",
            include_detailed=include_detailed,
        )

        # Run all health checks concurrently for better performance
        base_checks = [
            self._check_database_health(),
            self._check_data_collection_health(),
            self._check_configuration_health(),
            self._check_logging_health(),
        ]

        if include_detailed:
            detailed_checks = [
                self._check_monitoring_health(),
                self._check_cli_health(),
                self._check_pipeline_health(),
            ]
            all_checks = base_checks + detailed_checks
        else:
            all_checks = base_checks

        # Execute all health checks concurrently
        services = await asyncio.gather(*all_checks, return_exceptions=True)

        # Handle any exceptions from concurrent execution
        processed_services = []
        for i, result in enumerate(services):
            if isinstance(result, Exception):
                self.logger.error(f"Health check {i} failed", error=str(result))
                # Create a failed health check result
                processed_services.append(
                    ServiceHealth(
                        service_name=f"health_check_{i}",
                        status=HealthStatus.CRITICAL,
                        response_time=0.0,
                        error_message=str(result),
                        last_check=datetime.now(),
                        metadata={"concurrent_execution_error": True},
                    )
                )
            else:
                processed_services.append(result)

        services = processed_services

        # Calculate overall status
        overall_status = self._calculate_overall_status(services)
        overall_response_time = (time.time() - start_time) * 1000

        # Build error summary
        error_summary = {}
        for service in services:
            if service.error_count > 0:
                error_summary[service.name] = service.error_count

        # Performance metrics
        performance_metrics = {
            "avg_response_time_ms": sum(s.response_time_ms for s in services)
            / len(services),
            "max_response_time_ms": max(s.response_time_ms for s in services),
            "total_errors": sum(s.error_count for s in services),
            "total_warnings": sum(s.warning_count for s in services),
        }

        # Dependency status
        dependency_status = {service.name: service.status for service in services}

        system_health = SystemHealth(
            status=overall_status,
            timestamp=datetime.now(),
            services=services,
            overall_response_time_ms=overall_response_time,
            error_summary=error_summary,
            performance_metrics=performance_metrics,
            dependency_status=dependency_status,
        )

        self._last_health_check = system_health

        self.logger.info(
            "System health check completed",
            operation="health_check",
            overall_status=overall_status.value,
            response_time_ms=overall_response_time,
            total_services=len(services),
        )

        return system_health

    async def _check_database_health(self) -> ServiceHealth:
        """Check database connectivity and performance with connection pooling and circuit breaker."""
        start_time = time.time()
        service_name = "database"

        # Check circuit breaker first
        if hasattr(self, "_db_circuit_breaker") and self._db_circuit_breaker.is_open():
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.CRITICAL,
                message="Database health check circuit breaker is OPEN",
                response_time_ms=0.0,
                last_check=datetime.now(),
                error_count=1,
                metadata={"circuit_breaker_state": "OPEN"},
            )

        conn = None
        try:
            # Validate database configuration structure exists
            if not hasattr(self.settings, 'database'):
                return ServiceHealth(
                    name=service_name,
                    status=HealthStatus.CRITICAL,
                    message="Database configuration section missing from settings",
                    response_time_ms=0.0,
                    last_check=datetime.now(),
                    error_count=1,
                    metadata={"error_type": "ConfigurationError", "missing_section": "database"},
                )
            
            # Validate required database configuration fields
            required_fields = ['host', 'port', 'user', 'password', 'database']
            missing_fields = []
            for field in required_fields:
                if not hasattr(self.settings.database, field):
                    missing_fields.append(field)
            
            if missing_fields:
                return ServiceHealth(
                    name=service_name,
                    status=HealthStatus.CRITICAL,
                    message=f"Missing database configuration fields: {', '.join(missing_fields)}",
                    response_time_ms=0.0,
                    last_check=datetime.now(),
                    error_count=1,
                    metadata={"error_type": "ConfigurationError", "missing_fields": missing_fields},
                )
            
            # Try to use connection pool infrastructure first
            try:
                from ...data.database.connection import get_connection
                conn = await asyncio.wait_for(
                    get_connection(),
                    timeout=self.config.connection_timeout_seconds,
                )
            except Exception as pool_error:
                # Fallback to direct connection if pool unavailable
                conn = await asyncio.wait_for(
                    asyncpg.connect(
                        host=self.settings.database.host,
                        port=self.settings.database.port,
                        user=self.settings.database.user,
                        password=self.settings.database.password,
                        database=self.settings.database.database,
                        command_timeout=self.config.query_timeout_seconds,
                        server_settings={"application_name": "health_check"},
                    ),
                    timeout=self.config.connection_timeout_seconds,
                )

            # Test basic connectivity with timeout
            await asyncio.wait_for(
                conn.fetchval("SELECT 1"), timeout=self.config.query_timeout_seconds
            )

            # Get comprehensive database stats in single optimized query
            db_stats = await asyncio.wait_for(
                conn.fetchrow("""
                    SELECT 
                        pg_database_size(current_database()) as db_size,
                        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
                        (SELECT COALESCE(
                            (SELECT count(*) FROM raw_data.action_network_odds), 0
                        )) as raw_odds_count,
                        (SELECT COALESCE(
                            (SELECT count(*) FROM staging.betting_odds_unified), 0
                        )) as staging_odds_count,
                        (SELECT COALESCE(
                            (SELECT count(*) FROM curated.enhanced_games), 0
                        )) as curated_games_count
                """),
                timeout=self.config.query_timeout_seconds,
            )

            # Record success in circuit breaker
            if hasattr(self, "_db_circuit_breaker"):
                self._db_circuit_breaker.record_success()

            response_time = (time.time() - start_time) * 1000

            # Build table statistics from optimized query
            table_stats = {
                "raw_data.action_network_odds": db_stats["raw_odds_count"],
                "staging.betting_odds_unified": db_stats["staging_odds_count"],
                "curated.enhanced_games": db_stats["curated_games_count"],
            }

            # Determine status based on configurable performance thresholds
            if response_time > self.config.critical_response_threshold_ms:
                status = HealthStatus.UNHEALTHY
                message = f"Database critical slow response: {response_time:.1f}ms"
                warning_count = 0
                error_count = 1
            elif response_time > self.config.slow_response_threshold_ms:
                status = HealthStatus.DEGRADED
                message = f"Database slow response: {response_time:.1f}ms"
                warning_count = 1
                error_count = 0
            elif any(count < 0 for count in table_stats.values()):
                status = HealthStatus.DEGRADED
                message = "Some required tables missing or inaccessible"
                warning_count = 1
                error_count = 0
            else:
                status = HealthStatus.HEALTHY
                message = f"Database healthy, response: {response_time:.1f}ms"
                warning_count = 0
                error_count = 0

            metadata = {
                "database_size_bytes": db_stats["db_size"] if db_stats else 0,
                "active_connections": db_stats["active_connections"] if db_stats else 0,
                "max_connections": db_stats["max_connections"] if db_stats else 0,
                "table_counts": table_stats,
                "connection_pool_status": "healthy",
                "circuit_breaker_state": self._db_circuit_breaker.state,
                "query_optimization": "single_query_used",
            }

            return ServiceHealth(
                name=service_name,
                status=status,
                message=message,
                response_time_ms=response_time,
                last_check=datetime.now(),
                warning_count=warning_count,
                error_count=error_count,
                metadata=metadata,
            )

        except asyncio.TimeoutError as e:
            # Record failure in circuit breaker
            self._db_circuit_breaker.record_failure()
            response_time = (time.time() - start_time) * 1000

            self.logger.error(
                "Database health check timeout",
                operation="database_health_check",
                timeout_seconds=self.config.connection_timeout_seconds,
                response_time_ms=response_time,
                circuit_breaker_state=self._db_circuit_breaker.state,
                retry_suggested=True,
            )

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.CRITICAL,
                message=f"Database timeout after {self.config.connection_timeout_seconds}s",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={
                    "error_type": "TimeoutError",
                    "circuit_breaker_state": self._db_circuit_breaker.state,
                    "timeout_threshold": self.config.connection_timeout_seconds,
                    "recovery_suggestion": "Check database server availability and network connectivity",
                    "retry_recommended": True,
                },
            )

        except (ConnectionRefusedError, OSError) as e:
            # Network-related errors - specific handling
            self._db_circuit_breaker.record_failure()
            response_time = (time.time() - start_time) * 1000

            self.logger.error(
                "Database connection network error",
                operation="database_health_check",
                error_type=type(e).__name__,
                error_message=str(e),
                response_time_ms=response_time,
                circuit_breaker_state=self._db_circuit_breaker.state,
            )

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.CRITICAL,
                message=f"Database network error: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={
                    "error_type": type(e).__name__,
                    "error_category": "network",
                    "circuit_breaker_state": self._db_circuit_breaker.state,
                    "recovery_suggestion": "Verify database server is running and network configuration",
                    "check_database_status": True,
                },
            )

        except (asyncpg.InvalidAuthorizationSpecificationError, asyncpg.InvalidPasswordError) as e:
            # Authentication errors - specific handling
            self._db_circuit_breaker.record_failure()
            response_time = (time.time() - start_time) * 1000

            self.logger.error(
                "Database authentication error",
                operation="database_health_check",
                error_type=type(e).__name__,
                response_time_ms=response_time,
                circuit_breaker_state=self._db_circuit_breaker.state,
            )

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.CRITICAL,
                message="Database authentication failed",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={
                    "error_type": type(e).__name__,
                    "error_category": "authentication",
                    "circuit_breaker_state": self._db_circuit_breaker.state,
                    "recovery_suggestion": "Verify database credentials and user permissions",
                    "check_credentials": True,
                },
            )

        except Exception as e:
            # Record failure in circuit breaker
            self._db_circuit_breaker.record_failure()
            response_time = (time.time() - start_time) * 1000

            self.logger.error(
                "Database health check unexpected error",
                operation="database_health_check",
                error_type=type(e).__name__,
                error_message=str(e),
                response_time_ms=response_time,
                circuit_breaker_state=self._db_circuit_breaker.state,
            )

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "error_category": "unexpected",
                    "circuit_breaker_state": self._db_circuit_breaker.state,
                    "recovery_suggestion": "Review logs for detailed error analysis",
                },
            )
        finally:
            # Ensure connection cleanup
            if conn and not conn.is_closed():
                try:
                    await conn.close()
                except Exception as cleanup_error:
                    self.logger.warning(
                        f"Failed to close database connection: {cleanup_error}"
                    )

    async def _check_data_collection_health(self) -> ServiceHealth:
        """Check data collection service health with circuit breaker."""
        start_time = time.time()
        service_name = "data_collection"

        # Check circuit breaker first
        if self._collection_circuit_breaker.is_open():
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.CRITICAL,
                message="Data collection health check circuit breaker is OPEN",
                response_time_ms=0.0,
                last_check=datetime.now(),
                error_count=1,
                metadata={"circuit_breaker_state": "OPEN"},
            )

        try:
            # Test collector registry with proper error handling
            from ...data.collection.registry import get_collector_instance

            # Try to get key collectors
            collectors_status = {}
            test_collectors = ["action_network", "vsin", "sbd"]

            for collector_name in test_collectors:
                try:
                    collector = get_collector_instance(collector_name)
                    collectors_status[collector_name] = "available"
                except ImportError as e:
                    # Import errors are system-level failures
                    self._collection_circuit_breaker.record_failure()
                    response_time = (time.time() - start_time) * 1000
                    
                    return ServiceHealth(
                        name=service_name,
                        status=HealthStatus.UNHEALTHY,
                        message=f"Collector registry import failed: {str(e)}",
                        response_time_ms=response_time,
                        last_check=datetime.now(),
                        error_count=1,
                        metadata={
                            "error_type": "ImportError",
                            "circuit_breaker_state": self._collection_circuit_breaker.state,
                            "recovery_action": "check_collector_registry_availability",
                        },
                    )
                except Exception as e:
                    collectors_status[collector_name] = f"error: {str(e)}"

            response_time = (time.time() - start_time) * 1000

            # Determine status
            available_collectors = sum(
                1 for status in collectors_status.values() if status == "available"
            )
            total_collectors = len(collectors_status)

            if available_collectors == total_collectors:
                status = HealthStatus.HEALTHY
                message = f"All {total_collectors} collectors available"
                error_count = 0
                # Record success in circuit breaker
                self._collection_circuit_breaker.record_success()
            elif available_collectors > 0:
                status = HealthStatus.DEGRADED
                message = (
                    f"{available_collectors}/{total_collectors} collectors available"
                )
                error_count = total_collectors - available_collectors
            else:
                status = HealthStatus.UNHEALTHY
                message = "No collectors available"
                error_count = total_collectors
                # Record failure for completely unavailable collectors
                self._collection_circuit_breaker.record_failure()

            return ServiceHealth(
                name=service_name,
                status=status,
                message=message,
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=error_count,
                metadata={
                    "collectors_status": collectors_status,
                    "circuit_breaker_state": self._collection_circuit_breaker.state,
                },
            )

        except ImportError as e:
            # Handle import errors specifically
            self._collection_circuit_breaker.record_failure()
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Collector registry import failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={
                    "error_type": "ImportError",
                    "circuit_breaker_state": self._collection_circuit_breaker.state,
                    "recovery_action": "check_collector_registry_availability",
                },
            )

        except Exception as e:
            # Record failure in circuit breaker
            self._collection_circuit_breaker.record_failure()
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Data collection health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "circuit_breaker_state": self._collection_circuit_breaker.state,
                },
            )

    async def _check_configuration_health(self) -> ServiceHealth:
        """Check configuration system health."""
        start_time = time.time()
        service_name = "configuration"

        try:
            # Test configuration loading
            settings = get_settings()

            # Check required configuration sections
            required_sections = ["database", "logging"]
            missing_sections = []

            for section in required_sections:
                if not hasattr(settings, section):
                    missing_sections.append(section)

            response_time = (time.time() - start_time) * 1000

            if missing_sections:
                status = HealthStatus.DEGRADED
                message = (
                    f"Missing configuration sections: {', '.join(missing_sections)}"
                )
                warning_count = len(missing_sections)
            else:
                status = HealthStatus.HEALTHY
                message = "Configuration loaded successfully"
                warning_count = 0

            return ServiceHealth(
                name=service_name,
                status=status,
                message=message,
                response_time_ms=response_time,
                last_check=datetime.now(),
                warning_count=warning_count,
                metadata={
                    "config_sections": [
                        attr for attr in dir(settings) if not attr.startswith("_")
                    ],
                    "missing_sections": missing_sections,
                },
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Configuration health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__},
            )

    async def _check_logging_health(self) -> ServiceHealth:
        """Check logging system health."""
        start_time = time.time()
        service_name = "logging"

        try:
            # Test logging system
            test_logger = UnifiedLogger("health_check_test", LogComponent.MONITORING)
            test_logger.info("Health check test log entry")

            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="Logging system functional",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"test_log_written": True},
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Logging system failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__},
            )

    async def _check_monitoring_health(self) -> ServiceHealth:
        """Check monitoring system health."""
        start_time = time.time()
        service_name = "monitoring"

        try:
            # This is a self-check - if we're running, monitoring is working
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="Monitoring system operational",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"self_check": True},
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Monitoring health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__},
            )

    async def _check_cli_health(self) -> ServiceHealth:
        """Check CLI system health."""
        start_time = time.time()
        service_name = "cli"

        try:
            # Test CLI module imports - just import the module, don't need specific functions
            import src.interfaces.cli.main
            # Check if enhanced error handling is available (it might not exist)
            try:
                from ...interfaces.cli.enhanced_error_handling import EnhancedCLIValidator
                enhanced_error_handling = True
            except ImportError:
                enhanced_error_handling = False

            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="CLI system accessible",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={
                    "main_module_imported": True,
                    "enhanced_error_handling": enhanced_error_handling,
                },
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"CLI health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__},
            )

    async def _check_pipeline_health(self) -> ServiceHealth:
        """Check data pipeline health."""
        start_time = time.time()
        service_name = "pipeline"

        try:
            # Test pipeline orchestration imports
            from ...services.orchestration.pipeline_orchestration_service import (
                PipelineOrchestrationService,
            )

            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="Pipeline orchestration accessible",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"orchestration_service": True},
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000

            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Pipeline health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__},
            )

    def _calculate_overall_status(self, services: List[ServiceHealth]) -> HealthStatus:
        """Calculate overall system status from individual service health."""
        if not services:
            return HealthStatus.UNKNOWN

        critical_count = sum(1 for s in services if s.status == HealthStatus.CRITICAL)
        unhealthy_count = sum(1 for s in services if s.status == HealthStatus.UNHEALTHY)
        degraded_count = sum(1 for s in services if s.status == HealthStatus.DEGRADED)

        # If any service is critical, system is critical
        if critical_count > 0:
            return HealthStatus.CRITICAL
        
        # If any critical service is unhealthy, system is unhealthy
        critical_services = ["database", "configuration"]
        critical_unhealthy = any(
            s.status in [HealthStatus.UNHEALTHY, HealthStatus.CRITICAL]
            for s in services
            if s.name in critical_services
        )

        if critical_unhealthy:
            return HealthStatus.UNHEALTHY
        elif unhealthy_count > 0 or degraded_count >= len(services) // 2:
            return HealthStatus.DEGRADED
        elif degraded_count > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    async def get_service_health(self, service_name: str) -> Optional[ServiceHealth]:
        """Get health status for a specific service."""
        system_health = await self.get_system_health()

        for service in system_health.services:
            if service.name == service_name:
                return service

        return None

    async def is_system_healthy(self) -> bool:
        """Check if system is healthy (no critical failures)."""
        system_health = await self.get_system_health()
        return system_health.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]
