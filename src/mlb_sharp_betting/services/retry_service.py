#!/usr/bin/env python3
"""
Unified Retry Service for MLB Sharp Betting System

Standardizes retry handling across all services to eliminate the chaos of
inconsistent retry implementations. Provides production-grade retry strategies
for different operation types with proper exponential backoff, jitter, and
timeout handling.

Production Features:
- Unified retry logic for all services
- Operation-specific retry strategies
- Circuit breaker integration
- Retry metrics and monitoring
- Thread-safe operation tracking
- Graceful degradation
"""

import asyncio
import functools
import random
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar

from ..core.logging import get_logger
from .config_service import get_config_service, get_retry_config

logger = get_logger(__name__)

T = TypeVar("T")


class OperationType(Enum):
    """Operation types for retry strategy selection."""

    DATABASE = "database"
    API_CALL = "api_call"
    SCRAPING = "scraping"
    NOTIFICATION = "notification"
    FILE_IO = "file_io"
    NETWORK = "network"
    BETTING_ANALYSIS = "betting_analysis"


class RetryStatus(Enum):
    """Retry attempt status."""

    SUCCESS = "success"
    RETRY = "retry"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class RetryMetrics:
    """Metrics for retry operations."""

    operation_type: OperationType
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    total_retry_count: int = 0
    total_delay_time: float = 0.0
    circuit_breaker_trips: int = 0
    last_attempt: datetime | None = None


@dataclass
class RetryAttempt:
    """Information about a retry attempt."""

    attempt_number: int
    operation_name: str
    start_time: datetime
    end_time: datetime | None = None
    status: RetryStatus = RetryStatus.RETRY
    error: Exception | None = None
    delay_before_retry: float = 0.0


class CircuitBreaker:
    """Circuit breaker for retry operations."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_calls: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self.failure_count = 0
        self.last_failure_time: datetime | None = None
        self.state = "closed"  # closed, open, half-open
        self.half_open_calls = 0
        self._lock = threading.Lock()

    def can_proceed(self) -> bool:
        """Check if operation can proceed."""
        with self._lock:
            if self.state == "closed":
                return True
            elif self.state == "open":
                if (
                    self.last_failure_time
                    and datetime.now() - self.last_failure_time
                    > timedelta(seconds=self.recovery_timeout)
                ):
                    self.state = "half-open"
                    self.half_open_calls = 0
                    return True
                return False
            elif self.state == "half-open":
                return self.half_open_calls < self.half_open_max_calls
        return False

    def record_success(self) -> None:
        """Record successful operation."""
        with self._lock:
            if self.state == "half-open":
                self.half_open_calls += 1
                if self.half_open_calls >= self.half_open_max_calls:
                    self.state = "closed"
                    self.failure_count = 0
            elif self.state == "closed":
                self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self) -> None:
        """Record failed operation."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()

            if self.state == "half-open":
                self.state = "open"
            elif (
                self.state == "closed" and self.failure_count >= self.failure_threshold
            ):
                self.state = "open"


class RetryService:
    """
    Production-grade retry handling for betting operations.

    Unifies the three different retry implementations found in the system:
    1. Pre-game workflow: delay = retry_delay_base ** retry_count
    2. Pinnacle scraper: delay = retry_delay * (2 ** attempt)
    3. Alert service: simple retry count with fixed delay
    4. Database: exponential backoff with jitter

    Provides operation-specific retry strategies with circuit breaker protection.
    """

    def __init__(self):
        """Initialize retry service."""
        self.logger = get_logger(__name__)
        self.config_service = get_config_service()

        # Retry metrics by operation type
        self.metrics: dict[OperationType, RetryMetrics] = {
            op_type: RetryMetrics(op_type) for op_type in OperationType
        }

        # Circuit breakers by operation type
        self.circuit_breakers: dict[OperationType, CircuitBreaker] = {
            OperationType.DATABASE: CircuitBreaker(
                failure_threshold=3, recovery_timeout=30
            ),
            OperationType.API_CALL: CircuitBreaker(
                failure_threshold=5, recovery_timeout=60
            ),
            OperationType.SCRAPING: CircuitBreaker(
                failure_threshold=3, recovery_timeout=45
            ),
            OperationType.NOTIFICATION: CircuitBreaker(
                failure_threshold=2, recovery_timeout=120
            ),
            OperationType.FILE_IO: CircuitBreaker(
                failure_threshold=2, recovery_timeout=15
            ),
            OperationType.NETWORK: CircuitBreaker(
                failure_threshold=4, recovery_timeout=60
            ),
            OperationType.BETTING_ANALYSIS: CircuitBreaker(
                failure_threshold=2, recovery_timeout=30
            ),
        }

        # Active retry tracking
        self.active_retries: dict[str, RetryAttempt] = {}
        self._metrics_lock = threading.Lock()

        self.logger.info("RetryService initialized with unified retry strategies")

    async def execute_with_retry(
        self,
        operation: Callable[[], T] | Callable[[], Awaitable[T]],
        operation_name: str,
        operation_type: OperationType = OperationType.NETWORK,
        service_name: str | None = None,
        max_retries: int | None = None,
        base_delay: float | None = None,
    ) -> T:
        """
        Execute operation with appropriate retry strategy.

        Args:
            operation: Function to execute (sync or async)
            operation_name: Name for logging and metrics
            operation_type: Type of operation for strategy selection
            service_name: Service name for configuration lookup
            max_retries: Override max retries
            base_delay: Override base delay

        Returns:
            Result of the operation

        Raises:
            Exception: If all retries are exhausted
        """
        # Get retry configuration
        if service_name:
            retry_config = get_retry_config(service_name)
        else:
            retry_config = self._get_default_retry_config(operation_type)

        # Override with provided values
        max_attempts = max_retries or retry_config.get("max_attempts", 3)
        base_delay_seconds = base_delay or retry_config.get("base_delay_seconds", 1.0)
        exponential_base = retry_config.get("exponential_base", 2.0)
        max_delay_seconds = retry_config.get("max_delay_seconds", 60.0)
        jitter_enabled = retry_config.get("jitter_enabled", True)
        timeout_seconds = retry_config.get("timeout_seconds", 30)

        # Check circuit breaker
        circuit_breaker = self.circuit_breakers[operation_type]
        if not circuit_breaker.can_proceed():
            self._record_circuit_breaker_trip(operation_type)
            raise Exception(
                f"Circuit breaker open for operation type: {operation_type.value}"
            )

        last_exception = None
        retry_attempt = RetryAttempt(
            attempt_number=0, operation_name=operation_name, start_time=datetime.now()
        )

        for attempt in range(max_attempts):
            retry_attempt.attempt_number = attempt + 1

            try:
                # Execute operation with timeout
                if asyncio.iscoroutinefunction(operation):
                    result = await asyncio.wait_for(
                        operation(), timeout=timeout_seconds
                    )
                else:
                    # For sync operations, we run them in a thread pool with timeout
                    loop = asyncio.get_event_loop()
                    result = await asyncio.wait_for(
                        loop.run_in_executor(None, operation), timeout=timeout_seconds
                    )

                # Success!
                retry_attempt.status = RetryStatus.SUCCESS
                retry_attempt.end_time = datetime.now()

                circuit_breaker.record_success()
                self._record_successful_attempt(operation_type, retry_attempt)

                return result

            except asyncio.TimeoutError as e:
                last_exception = e
                retry_attempt.status = RetryStatus.TIMEOUT
                retry_attempt.error = e

                self.logger.warning(
                    f"Operation timed out: {operation_name}",
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    timeout=timeout_seconds,
                )

            except Exception as e:
                last_exception = e
                retry_attempt.error = e

                self.logger.warning(
                    f"Operation failed: {operation_name}",
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    error=str(e),
                )

            # If this was the last attempt, don't delay
            if attempt >= max_attempts - 1:
                break

            # Calculate delay with exponential backoff and jitter
            delay = self._calculate_delay(
                attempt=attempt,
                base_delay=base_delay_seconds,
                exponential_base=exponential_base,
                max_delay=max_delay_seconds,
                jitter_enabled=jitter_enabled,
                operation_type=operation_type,
            )

            retry_attempt.delay_before_retry = delay

            self.logger.info(
                f"Retrying {operation_name} in {delay:.2f} seconds",
                attempt=attempt + 1,
                max_attempts=max_attempts,
                delay=delay,
            )

            # Wait before retry
            await asyncio.sleep(delay)

        # All retries exhausted
        retry_attempt.status = RetryStatus.FAILED
        retry_attempt.end_time = datetime.now()

        circuit_breaker.record_failure()
        self._record_failed_attempt(operation_type, retry_attempt)

        raise last_exception or Exception(
            f"Operation failed after {max_attempts} attempts: {operation_name}"
        )

    def _calculate_delay(
        self,
        attempt: int,
        base_delay: float,
        exponential_base: float,
        max_delay: float,
        jitter_enabled: bool,
        operation_type: OperationType,
    ) -> float:
        """Calculate delay with appropriate strategy for operation type."""

        if operation_type == OperationType.DATABASE:
            # Database: Exponential backoff with jitter (matches current db/connection.py)
            delay = base_delay * (exponential_base**attempt)
            if jitter_enabled:
                delay += random.uniform(0, 0.1)

        elif operation_type == OperationType.NOTIFICATION:
            # Notifications: Linear retry (matches current alert_service.py)
            delay = base_delay  # Fixed delay for notifications

        elif operation_type in [OperationType.API_CALL, OperationType.SCRAPING]:
            # API/Scraping: Standard exponential backoff (matches pinnacle_scraper.py)
            delay = base_delay * (exponential_base**attempt)

        elif operation_type == OperationType.BETTING_ANALYSIS:
            # Betting analysis: Exponential with power base (matches pre_game_workflow.py)
            delay = base_delay ** (attempt + 1)

        else:
            # Default: Standard exponentially backoff
            delay = base_delay * (exponential_base**attempt)

        # Apply jitter if enabled (except for notifications)
        if jitter_enabled and operation_type != OperationType.NOTIFICATION:
            jitter = delay * 0.1 * random.uniform(-1, 1)
            delay += jitter

        # Ensure delay doesn't exceed maximum
        return min(delay, max_delay)

    def _get_default_retry_config(
        self, operation_type: OperationType
    ) -> dict[str, Any]:
        """Get default retry configuration for operation type."""
        defaults = {
            OperationType.DATABASE: {
                "max_attempts": 3,
                "base_delay_seconds": 0.1,
                "exponential_base": 2.0,
                "max_delay_seconds": 5.0,
                "jitter_enabled": True,
                "timeout_seconds": 10,
            },
            OperationType.API_CALL: {
                "max_attempts": 3,
                "base_delay_seconds": 1.0,
                "exponential_base": 2.0,
                "max_delay_seconds": 30.0,
                "jitter_enabled": True,
                "timeout_seconds": 30,
            },
            OperationType.SCRAPING: {
                "max_attempts": 3,
                "base_delay_seconds": 1.0,
                "exponential_base": 2.0,
                "max_delay_seconds": 30.0,
                "jitter_enabled": True,
                "timeout_seconds": 45,
            },
            OperationType.NOTIFICATION: {
                "max_attempts": 3,
                "base_delay_seconds": 30.0,
                "exponential_base": 1.0,  # Linear retry
                "max_delay_seconds": 30.0,
                "jitter_enabled": False,
                "timeout_seconds": 60,
            },
            OperationType.FILE_IO: {
                "max_attempts": 2,
                "base_delay_seconds": 0.5,
                "exponential_base": 2.0,
                "max_delay_seconds": 5.0,
                "jitter_enabled": False,
                "timeout_seconds": 15,
            },
            OperationType.NETWORK: {
                "max_attempts": 3,
                "base_delay_seconds": 1.0,
                "exponential_base": 2.0,
                "max_delay_seconds": 30.0,
                "jitter_enabled": True,
                "timeout_seconds": 30,
            },
            OperationType.BETTING_ANALYSIS: {
                "max_attempts": 3,
                "base_delay_seconds": 2.0,
                "exponential_base": 2.0,  # Will use power-based calculation
                "max_delay_seconds": 60.0,
                "jitter_enabled": True,
                "timeout_seconds": 90,
            },
        }

        return defaults.get(operation_type, defaults[OperationType.NETWORK])

    def _record_successful_attempt(
        self, operation_type: OperationType, attempt: RetryAttempt
    ) -> None:
        """Record successful retry attempt."""
        with self._metrics_lock:
            metrics = self.metrics[operation_type]
            metrics.total_attempts += 1
            metrics.successful_attempts += 1
            metrics.total_retry_count += attempt.attempt_number - 1
            metrics.last_attempt = attempt.end_time

            if attempt.attempt_number > 1:
                metrics.total_delay_time += attempt.delay_before_retry

    def _record_failed_attempt(
        self, operation_type: OperationType, attempt: RetryAttempt
    ) -> None:
        """Record failed retry attempt."""
        with self._metrics_lock:
            metrics = self.metrics[operation_type]
            metrics.total_attempts += 1
            metrics.failed_attempts += 1
            metrics.total_retry_count += attempt.attempt_number - 1
            metrics.last_attempt = attempt.end_time

            if attempt.attempt_number > 1:
                metrics.total_delay_time += attempt.delay_before_retry

    def _record_circuit_breaker_trip(self, operation_type: OperationType) -> None:
        """Record circuit breaker trip."""
        with self._metrics_lock:
            self.metrics[operation_type].circuit_breaker_trips += 1

        self.logger.warning(
            f"Circuit breaker tripped for operation type: {operation_type.value}"
        )

    def get_metrics(self) -> dict[str, Any]:
        """Get retry metrics."""
        with self._metrics_lock:
            return {
                "operation_metrics": {
                    op_type.value: {
                        "total_attempts": metrics.total_attempts,
                        "successful_attempts": metrics.successful_attempts,
                        "failed_attempts": metrics.failed_attempts,
                        "success_rate": (
                            metrics.successful_attempts / metrics.total_attempts
                            if metrics.total_attempts > 0
                            else 0
                        ),
                        "total_retries": metrics.total_retry_count,
                        "avg_retries_per_attempt": (
                            metrics.total_retry_count / metrics.total_attempts
                            if metrics.total_attempts > 0
                            else 0
                        ),
                        "total_delay_time": metrics.total_delay_time,
                        "circuit_breaker_trips": metrics.circuit_breaker_trips,
                        "last_attempt": metrics.last_attempt.isoformat()
                        if metrics.last_attempt
                        else None,
                    }
                    for op_type, metrics in self.metrics.items()
                },
                "circuit_breaker_states": {
                    op_type.value: {
                        "state": cb.state,
                        "failure_count": cb.failure_count,
                        "last_failure": cb.last_failure_time.isoformat()
                        if cb.last_failure_time
                        else None,
                    }
                    for op_type, cb in self.circuit_breakers.items()
                },
            }

    def reset_circuit_breaker(self, operation_type: OperationType) -> None:
        """Reset circuit breaker for operation type."""
        circuit_breaker = self.circuit_breakers[operation_type]
        with circuit_breaker._lock:
            circuit_breaker.state = "closed"
            circuit_breaker.failure_count = 0
            circuit_breaker.last_failure_time = None
            circuit_breaker.half_open_calls = 0

        self.logger.info(
            f"Circuit breaker reset for operation type: {operation_type.value}"
        )


# Global retry service instance
_retry_service = None
_retry_service_lock = threading.Lock()


def get_retry_service() -> RetryService:
    """Get the global retry service instance."""
    global _retry_service
    if _retry_service is None:
        with _retry_service_lock:
            if _retry_service is None:
                _retry_service = RetryService()
    return _retry_service


# Convenience decorator for retry functionality
def retry_operation(
    operation_type: OperationType = OperationType.NETWORK,
    service_name: str | None = None,
    max_retries: int | None = None,
    base_delay: float | None = None,
):
    """
    Decorator for adding retry functionality to operations.

    Args:
        operation_type: Type of operation for strategy selection
        service_name: Service name for configuration lookup
        max_retries: Override max retries
        base_delay: Override base delay
    """

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            retry_service = get_retry_service()

            # For async functions, the operation lambda needs to be async
            async def operation():
                return await func(*args, **kwargs)

            return await retry_service.execute_with_retry(
                operation=operation,
                operation_name=func.__name__,
                operation_type=operation_type,
                service_name=service_name,
                max_retries=max_retries,
                base_delay=base_delay,
            )

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For sync functions, we need to run in an async context
            async def run():
                retry_service = get_retry_service()
                operation = lambda: func(*args, **kwargs)
                return await retry_service.execute_with_retry(
                    operation=operation,
                    operation_name=func.__name__,
                    operation_type=operation_type,
                    service_name=service_name,
                    max_retries=max_retries,
                    base_delay=base_delay,
                )

            # Run in async context
            try:
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(run())
            except RuntimeError:
                # No event loop running, create one
                return asyncio.run(run())

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
