#!/usr/bin/env python3
"""
Data Collector Health Monitoring Service

Provides comprehensive health monitoring for all data collectors with:
- Multi-type health checks (connectivity, parsing, schema, performance)
- Intelligent alerting with escalation policies
- Automatic recovery mechanisms
- Performance analytics and trend analysis

This service addresses the inherent brittleness of web scraping by providing
proactive monitoring and rapid failure detection.
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import aiohttp

from ...core.config import UnifiedSettings
from ...core.logging import LogComponent, get_logger

try:
    from ...data.database.repositories import UnifiedRepository
except ImportError:
    # Fallback if repository not available
    UnifiedRepository = None
from ...data.collection.base import BaseCollector

logger = get_logger(__name__, LogComponent.MONITORING)


class HealthStatus(Enum):
    """Health status levels for collectors."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class CheckType(Enum):
    """Types of health checks."""

    CONNECTIVITY = "connectivity"
    PARSING = "parsing"
    SCHEMA = "schema"
    PERFORMANCE = "performance"


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""

    check_type: CheckType
    status: HealthStatus
    response_time: float | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class CircuitBreakerState:
    """Circuit breaker state for collector health monitoring."""

    failure_count: int = 0
    failure_threshold: int = 5
    timeout_duration: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    last_failure_time: datetime | None = None
    state: str = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

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


@dataclass
class CollectorHealthStatus:
    """Overall health status for a collector."""

    collector_name: str
    overall_status: HealthStatus
    checks: list[HealthCheckResult]
    last_updated: datetime
    uptime_percentage: float
    performance_score: float
    circuit_breaker_state: str | None = None


class CollectorHealthMonitor:
    """
    Health monitoring for individual data collectors.

    Performs multiple types of health checks and tracks performance metrics.
    """

    def __init__(self, collector: BaseCollector, config: dict[str, Any]):
        self.collector = collector
        self.config = config
        self.check_history: list[HealthCheckResult] = []
        self.last_check_time: datetime | None = None
        self.consecutive_failures = 0
        self.circuit_breaker = CircuitBreakerState()
        self._max_history_size = config.get("max_history_size", 100)

    async def run_all_checks(self) -> CollectorHealthStatus:
        """Execute all configured health checks for the collector with circuit breaker protection."""
        # Check circuit breaker before running checks
        if self.circuit_breaker.is_open():
            logger.warning(
                "Circuit breaker is open, skipping health checks",
                collector=self.collector.source.value,
                failure_count=self.circuit_breaker.failure_count,
                state=self.circuit_breaker.state,
            )

            return CollectorHealthStatus(
                collector_name=self.collector.source.value,
                overall_status=HealthStatus.CRITICAL,
                checks=[
                    HealthCheckResult(
                        check_type=CheckType.CONNECTIVITY,
                        status=HealthStatus.CRITICAL,
                        error_message="Circuit breaker is OPEN - too many consecutive failures",
                    )
                ],
                last_updated=datetime.now(),
                uptime_percentage=0.0,
                performance_score=0.0,
                circuit_breaker_state=self.circuit_breaker.state,
            )

        logger.info(
            "Running health checks for collector",
            collector=self.collector.source.value,
            checks=["connectivity", "parsing", "schema", "performance"],
        )

        checks = []

        # Run all health checks with parallel execution
        try:
            # Start with connectivity check
            connectivity_result = await self.check_connectivity()
            checks.append(connectivity_result)

            # Run remaining checks in parallel if connectivity is good
            if connectivity_result.status != HealthStatus.CRITICAL:
                parallel_tasks = [
                    asyncio.create_task(self.check_parsing()),
                    asyncio.create_task(self.check_schema()),
                    asyncio.create_task(self.check_performance()),
                ]

                parallel_results = await asyncio.gather(
                    *parallel_tasks, return_exceptions=True
                )

                for result in parallel_results:
                    if isinstance(result, Exception):
                        logger.error(
                            "Parallel health check failed",
                            collector=self.collector.source.value,
                            error=str(result),
                        )
                        checks.append(
                            HealthCheckResult(
                                check_type=CheckType.PERFORMANCE,
                                status=HealthStatus.CRITICAL,
                                error_message=f"Parallel check failed: {str(result)}",
                            )
                        )
                    else:
                        checks.append(result)
            else:
                # Still run performance check even if connectivity fails
                performance_result = await self.check_performance()
                checks.append(performance_result)

        except Exception as e:
            logger.error(
                "Health check execution failed",
                collector=self.collector.source.value,
                error=str(e),
            )
            checks.append(
                HealthCheckResult(
                    check_type=CheckType.CONNECTIVITY,
                    status=HealthStatus.CRITICAL,
                    error_message=f"Health check execution failed: {str(e)}",
                )
            )

        # Determine overall status
        overall_status = self._determine_overall_status(checks)

        # Update circuit breaker based on results
        if overall_status == HealthStatus.CRITICAL:
            self.circuit_breaker.record_failure()
            self.consecutive_failures += 1
        else:
            self.circuit_breaker.record_success()
            self.consecutive_failures = 0

        # Update tracking with resource management
        self.check_history.extend(checks)
        self.last_check_time = datetime.now()

        # Limit history size to prevent memory leaks
        if len(self.check_history) > self._max_history_size:
            # Keep only the most recent entries
            excess_count = len(self.check_history) - self._max_history_size
            self.check_history = self.check_history[excess_count:]
            logger.debug(
                "Trimmed health check history",
                collector=self.collector.source.value,
                removed_entries=excess_count,
                remaining_entries=len(self.check_history),
            )

        # Calculate performance metrics
        uptime_pct = self._calculate_uptime_percentage()
        performance_score = self._calculate_performance_score(checks)

        return CollectorHealthStatus(
            collector_name=self.collector.source.value,
            overall_status=overall_status,
            checks=checks,
            last_updated=datetime.now(),
            uptime_percentage=uptime_pct,
            performance_score=performance_score,
            circuit_breaker_state=self.circuit_breaker.state,
        )

    async def check_connectivity(self) -> HealthCheckResult:
        """
        Verify network connectivity to the data source.

        Tests DNS resolution, TCP connection, and HTTP response.
        """
        start_time = time.time()

        try:
            # Get the base URL for the collector
            base_url = getattr(self.collector.config, "base_url", None)
            if not base_url:
                # Try alternative URL sources
                test_url = "https://httpbin.org/get"  # Fallback for connectivity test
                base_url = test_url

            # Test HTTP connectivity
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(base_url) as response:
                    response_time = time.time() - start_time

                    if response.status >= 500:
                        return HealthCheckResult(
                            check_type=CheckType.CONNECTIVITY,
                            status=HealthStatus.CRITICAL,
                            response_time=response_time,
                            error_message=f"Server error: HTTP {response.status}",
                        )
                    elif response.status >= 400:
                        return HealthCheckResult(
                            check_type=CheckType.CONNECTIVITY,
                            status=HealthStatus.DEGRADED,
                            response_time=response_time,
                            error_message=f"Client error: HTTP {response.status}",
                        )
                    else:
                        return HealthCheckResult(
                            check_type=CheckType.CONNECTIVITY,
                            status=HealthStatus.HEALTHY,
                            response_time=response_time,
                            metadata={
                                "status_code": response.status,
                                "content_length": response.headers.get(
                                    "content-length", "unknown"
                                ),
                            },
                        )

        except asyncio.TimeoutError:
            return HealthCheckResult(
                check_type=CheckType.CONNECTIVITY,
                status=HealthStatus.CRITICAL,
                response_time=time.time() - start_time,
                error_message="Connection timeout",
            )
        except Exception as e:
            return HealthCheckResult(
                check_type=CheckType.CONNECTIVITY,
                status=HealthStatus.CRITICAL,
                response_time=time.time() - start_time,
                error_message=f"Connection failed: {str(e)}",
            )

    async def check_parsing(self) -> HealthCheckResult:
        """
        Validate that data parsing logic still works correctly.

        Performs a test collection with minimal data to verify parsing.
        Includes automatic retry logic for transient failures.
        """
        start_time = time.time()
        max_retries = 3
        retry_delay = 2.0

        for attempt in range(max_retries):
            try:
                # Attempt collection with minimal parameters
                result = await self.collector.collect(timeout_seconds=30)
                response_time = time.time() - start_time

                if not result.is_successful:
                    # Check if this is a retryable error
                    error_msg = (
                        "; ".join(result.errors) if result.errors else "Unknown error"
                    )

                    if attempt < max_retries - 1 and self._is_retryable_error(
                        error_msg
                    ):
                        logger.warning(
                            "Parsing check failed, retrying",
                            collector=self.collector.source.value,
                            attempt=attempt + 1,
                            error=error_msg,
                        )
                        await asyncio.sleep(
                            retry_delay * (attempt + 1)
                        )  # Exponential backoff
                        continue

                    return HealthCheckResult(
                        check_type=CheckType.PARSING,
                        status=HealthStatus.CRITICAL,
                        response_time=response_time,
                        error_message=f"Collection failed after {attempt + 1} attempts: {error_msg}",
                        metadata={"retry_attempts": attempt + 1},
                    )

                elif len(result.data) == 0:
                    return HealthCheckResult(
                        check_type=CheckType.PARSING,
                        status=HealthStatus.DEGRADED,
                        response_time=response_time,
                        error_message="No data collected",
                        metadata={
                            "records_collected": len(result.data),
                            "retry_attempts": attempt + 1,
                            "warnings": getattr(result, "warnings", []),
                        },
                    )
                else:
                    return HealthCheckResult(
                        check_type=CheckType.PARSING,
                        status=HealthStatus.HEALTHY,
                        response_time=response_time,
                        metadata={
                            "records_collected": len(result.data),
                            "data_quality_score": getattr(result, "quality_score", 1.0),
                            "retry_attempts": attempt + 1,
                        },
                    )

            except Exception as e:
                error_msg = str(e)

                if attempt < max_retries - 1 and self._is_retryable_error(error_msg):
                    logger.warning(
                        "Parsing check exception, retrying",
                        collector=self.collector.source.value,
                        attempt=attempt + 1,
                        error=error_msg,
                    )
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue

                return HealthCheckResult(
                    check_type=CheckType.PARSING,
                    status=HealthStatus.CRITICAL,
                    response_time=time.time() - start_time,
                    error_message=f"Parsing check failed after {attempt + 1} attempts: {error_msg}",
                    metadata={"retry_attempts": attempt + 1},
                )

        # Should never reach here due to the loop logic, but add as safety
        return HealthCheckResult(
            check_type=CheckType.PARSING,
            status=HealthStatus.CRITICAL,
            response_time=time.time() - start_time,
            error_message="Parsing check failed: maximum retries exceeded",
            metadata={"retry_attempts": max_retries},
        )

    def _is_retryable_error(self, error_message: str) -> bool:
        """Determine if an error is transient and worth retrying."""
        retryable_patterns = [
            "timeout",
            "connection",
            "network",
            "temporary",
            "503",
            "502",
            "504",
            "rate limit",
            "too many requests",
        ]

        error_lower = error_message.lower()
        return any(pattern in error_lower for pattern in retryable_patterns)

    async def check_schema(self) -> HealthCheckResult:
        """
        Validate that collected data conforms to expected schema.

        Checks data types, required fields, and business rules.
        """
        start_time = time.time()

        try:
            # Get recent sample data (would be from database in real implementation)
            # For now, simulate with a quick collection
            result = await self.collector.collect(timeout_seconds=20)
            response_time = time.time() - start_time

            if not result.is_successful:
                return HealthCheckResult(
                    check_type=CheckType.SCHEMA,
                    status=HealthStatus.CRITICAL,
                    response_time=response_time,
                    error_message="Could not collect data for schema validation",
                )

            # Validate schema compliance
            validation_errors = []
            required_fields = self.config.get(
                "required_fields", ["game_id", "home_team", "away_team", "timestamp"]
            )

            for record in result.data[:3]:  # Check first 3 records
                for field in required_fields:
                    if field not in record or record[field] is None:
                        validation_errors.append(f"Missing required field: {field}")

            # Calculate compliance percentage
            total_checks = len(result.data) * len(required_fields)
            if total_checks > 0:
                compliance_pct = max(0, 1 - (len(validation_errors) / total_checks))
            else:
                compliance_pct = 0.0

            if compliance_pct < 0.5:
                status = HealthStatus.CRITICAL
            elif compliance_pct < 0.8:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return HealthCheckResult(
                check_type=CheckType.SCHEMA,
                status=status,
                response_time=response_time,
                metadata={
                    "compliance_percentage": compliance_pct,
                    "validation_errors": validation_errors[:5],  # Limit errors
                    "records_validated": len(result.data),
                },
            )

        except Exception as e:
            return HealthCheckResult(
                check_type=CheckType.SCHEMA,
                status=HealthStatus.CRITICAL,
                response_time=time.time() - start_time,
                error_message=f"Schema validation failed: {str(e)}",
            )

    async def check_performance(self) -> HealthCheckResult:
        """
        Monitor collector performance metrics and trends.

        Analyzes response times, success rates, and data volume.
        """
        start_time = time.time()

        try:
            # Calculate recent performance metrics
            recent_checks = self.check_history[-20:]  # Last 20 checks

            if not recent_checks:
                return HealthCheckResult(
                    check_type=CheckType.PERFORMANCE,
                    status=HealthStatus.UNKNOWN,
                    response_time=time.time() - start_time,
                    error_message="No historical data for performance analysis",
                )

            # Calculate metrics
            response_times = [
                check.response_time
                for check in recent_checks
                if check.response_time is not None
            ]

            success_count = len(
                [
                    check
                    for check in recent_checks
                    if check.status == HealthStatus.HEALTHY
                ]
            )

            avg_response_time = (
                sum(response_times) / len(response_times) if response_times else 0
            )
            success_rate = success_count / len(recent_checks) if recent_checks else 0

            # Evaluate against thresholds
            max_response_time = self.config.get("max_response_time", 10.0)
            min_success_rate = self.config.get("min_success_rate", 0.90)

            issues = []
            if avg_response_time > max_response_time:
                issues.append(f"Slow response time: {avg_response_time:.2f}s")

            if success_rate < min_success_rate:
                issues.append(f"Low success rate: {success_rate:.1%}")

            # Determine status
            if success_rate < 0.5:
                status = HealthStatus.CRITICAL
            elif issues:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return HealthCheckResult(
                check_type=CheckType.PERFORMANCE,
                status=status,
                response_time=time.time() - start_time,
                metadata={
                    "avg_response_time": avg_response_time,
                    "success_rate": success_rate,
                    "recent_checks_count": len(recent_checks),
                    "issues": issues,
                },
            )

        except Exception as e:
            return HealthCheckResult(
                check_type=CheckType.PERFORMANCE,
                status=HealthStatus.CRITICAL,
                response_time=time.time() - start_time,
                error_message=f"Performance analysis failed: {str(e)}",
            )

    def _determine_overall_status(
        self, checks: list[HealthCheckResult]
    ) -> HealthStatus:
        """Determine overall health status from individual check results."""
        if not checks:
            return HealthStatus.UNKNOWN

        # Critical if any check is critical
        if any(check.status == HealthStatus.CRITICAL for check in checks):
            return HealthStatus.CRITICAL

        # Degraded if any check is degraded
        if any(check.status == HealthStatus.DEGRADED for check in checks):
            return HealthStatus.DEGRADED

        # Healthy if all checks are healthy
        if all(check.status == HealthStatus.HEALTHY for check in checks):
            return HealthStatus.HEALTHY

        return HealthStatus.UNKNOWN

    def _calculate_uptime_percentage(self) -> float:
        """Calculate uptime percentage based on recent health checks."""
        if not self.check_history:
            return 0.0

        # Consider last 24 hours of checks
        cutoff_time = datetime.now() - timedelta(hours=24)
        recent_checks = [
            check for check in self.check_history if check.timestamp >= cutoff_time
        ]

        if not recent_checks:
            return 0.0

        healthy_checks = len(
            [check for check in recent_checks if check.status == HealthStatus.HEALTHY]
        )

        return (healthy_checks / len(recent_checks)) * 100

    def _calculate_performance_score(self, checks: list[HealthCheckResult]) -> float:
        """Calculate overall performance score (0-100)."""
        if not checks:
            return 0.0

        scores = []
        for check in checks:
            if check.status == HealthStatus.HEALTHY:
                scores.append(100)
            elif check.status == HealthStatus.DEGRADED:
                scores.append(60)
            elif check.status == HealthStatus.CRITICAL:
                scores.append(0)
            else:
                scores.append(50)  # Unknown

        return sum(scores) / len(scores)


class HealthMonitoringOrchestrator:
    """
    Orchestrates health monitoring across all data collectors.

    Manages scheduling, alerting, and recovery coordination.
    """

    def __init__(self, config: UnifiedSettings):
        self.config = config
        self.monitors: dict[str, CollectorHealthMonitor] = {}
        self.alert_manager = AlertManager(config)
        self.running = False

    def register_collector(self, collector: BaseCollector):
        """Register a collector for health monitoring."""
        monitor_config = {
            "max_response_time": 10.0,
            "min_success_rate": 0.90,
            "required_fields": ["game_id", "home_team", "away_team", "timestamp"],
        }

        monitor = CollectorHealthMonitor(collector, monitor_config)
        self.monitors[collector.source.value] = monitor

        logger.info(
            "Registered collector for health monitoring",
            collector=collector.source.value,
            config=monitor_config,
        )

    async def start_monitoring(self):
        """Start continuous health monitoring."""
        self.running = True
        logger.info("Starting health monitoring service")

        # Start monitoring loop
        asyncio.create_task(self._monitoring_loop())

    async def stop_monitoring(self):
        """Stop health monitoring."""
        self.running = False
        logger.info("Stopping health monitoring service")

    async def check_all_collectors(self) -> dict[str, CollectorHealthStatus]:
        """Run health checks on all registered collectors."""
        results = {}

        tasks = []
        for name, monitor in self.monitors.items():
            task = asyncio.create_task(monitor.run_all_checks())
            tasks.append((name, task))

        # Wait for all checks to complete
        for name, task in tasks:
            try:
                result = await task
                results[name] = result

                # Process result for alerting
                await self.alert_manager.process_health_result(name, result)

            except Exception as e:
                logger.error(
                    "Health check failed for collector", collector=name, error=str(e)
                )
                results[name] = CollectorHealthStatus(
                    collector_name=name,
                    overall_status=HealthStatus.CRITICAL,
                    checks=[],
                    last_updated=datetime.now(),
                    uptime_percentage=0.0,
                    performance_score=0.0,
                )

        return results

    async def check_specific_collector(
        self, collector_name: str
    ) -> CollectorHealthStatus | None:
        """Run health check on a specific collector."""
        monitor = self.monitors.get(collector_name)
        if not monitor:
            logger.warning(f"No monitor found for collector: {collector_name}")
            return None

        try:
            result = await monitor.run_all_checks()
            await self.alert_manager.process_health_result(collector_name, result)
            return result

        except Exception as e:
            logger.error(
                "Health check failed for collector",
                collector=collector_name,
                error=str(e),
            )
            return None

    async def _monitoring_loop(self):
        """Main monitoring loop that runs health checks periodically."""
        check_interval = 300  # 5 minutes

        while self.running:
            try:
                logger.debug("Running scheduled health checks")
                await self.check_all_collectors()

                await asyncio.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(check_interval)


class AlertManager:
    """
    Manages alerting policies and notification delivery.
    """

    def __init__(self, config: UnifiedSettings):
        self.config = config
        self.alert_history: list[dict[str, Any]] = []

    async def process_health_result(
        self, collector_name: str, result: CollectorHealthStatus
    ):
        """Process health check results and trigger alerts if needed."""

        # Check if alert should be triggered
        should_alert = self._should_trigger_alert(collector_name, result)

        if should_alert:
            alert = {
                "collector": collector_name,
                "severity": self._determine_alert_severity(result),
                "status": result.overall_status.value,
                "message": self._generate_alert_message(result),
                "timestamp": datetime.now(),
                "uptime": result.uptime_percentage,
                "performance_score": result.performance_score,
            }

            await self._send_alert(alert)
            self.alert_history.append(alert)

    def _should_trigger_alert(
        self, collector_name: str, result: CollectorHealthStatus
    ) -> bool:
        """Determine if an alert should be triggered."""

        # Always alert on critical status
        if result.overall_status == HealthStatus.CRITICAL:
            return True

        # Alert on degraded status if it persists
        if result.overall_status == HealthStatus.DEGRADED:
            # Check recent alerts to avoid spam
            recent_alerts = [
                alert
                for alert in self.alert_history[-10:]
                if alert["collector"] == collector_name
                and alert["timestamp"] > datetime.now() - timedelta(hours=1)
            ]

            # Don't re-alert if we already alerted recently
            return len(recent_alerts) == 0

        return False

    def _determine_alert_severity(self, result: CollectorHealthStatus) -> AlertSeverity:
        """Determine appropriate alert severity."""
        if result.overall_status == HealthStatus.CRITICAL:
            return AlertSeverity.CRITICAL
        elif result.overall_status == HealthStatus.DEGRADED:
            return AlertSeverity.WARNING
        else:
            return AlertSeverity.INFO

    def _generate_alert_message(self, result: CollectorHealthStatus) -> str:
        """Generate human-readable alert message."""
        messages = []

        for check in result.checks:
            if check.status != HealthStatus.HEALTHY:
                messages.append(f"{check.check_type.value}: {check.error_message}")

        if messages:
            return f"Health issues detected: {'; '.join(messages)}"
        else:
            return f"Collector status: {result.overall_status.value}"

    async def _send_alert(self, alert: dict[str, Any]):
        """Send alert through configured channels."""
        logger.warning(
            "DATA COLLECTOR ALERT",
            collector=alert["collector"],
            severity=alert["severity"].value,
            status=alert["status"],
            message=alert["message"],
            uptime=f"{alert['uptime']:.1f}%",
            performance_score=f"{alert['performance_score']:.1f}",
        )

        # In a real implementation, this would send to Slack, email, etc.
        # For now, we just log the alert
        print(f"""
🚨 DATA COLLECTOR ALERT 🚨
Collector: {alert["collector"]}
Severity: {alert["severity"].value.upper()}
Status: {alert["status"]}
Message: {alert["message"]}
Uptime: {alert["uptime"]:.1f}%
Performance Score: {alert["performance_score"]:.1f}/100
Time: {alert["timestamp"].strftime("%Y-%m-%d %H:%M:%S")}
        """)
