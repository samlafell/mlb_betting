#!/usr/bin/env python3
"""
Enhanced Circuit Breaker with Automatic Recovery

Provides intelligent circuit breaker implementation with automatic recovery strategies
for data collection operations. Part of solution for GitHub Issue #36.
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from uuid import uuid4

import structlog

from ...core.logging import get_logger, LogComponent
from .health_monitoring import (
    AlertSeverity,
    CollectionAlert,
    CollectionHealthResult,
    FailurePattern,
    HealthStatus
)

logger = get_logger(__name__, LogComponent.CIRCUIT_BREAKER)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, calls are blocked
    HALF_OPEN = "half_open"  # Testing if service has recovered


class RecoveryStrategy(Enum):
    """Recovery strategies for circuit breaker."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    IMMEDIATE_RETRY = "immediate_retry"
    FALLBACK_SOURCE = "fallback_source"
    DEGRADED_MODE = "degraded_mode"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    
    # Failure thresholds
    failure_threshold: int = 5
    timeout_duration_seconds: int = 300  # 5 minutes
    half_open_max_calls: int = 3
    
    # Recovery settings
    recovery_strategy: RecoveryStrategy = RecoveryStrategy.EXPONENTIAL_BACKOFF
    max_retry_attempts: int = 3
    base_retry_delay_seconds: float = 1.0
    max_retry_delay_seconds: float = 300.0
    
    # Health monitoring
    success_threshold: int = 3  # Consecutive successes needed to close circuit
    health_check_interval_seconds: int = 60
    
    # Advanced settings
    enable_automatic_recovery: bool = True
    enable_fallback_sources: bool = False
    enable_degraded_mode: bool = True
    
    # Alert integration
    alert_on_open: bool = True
    alert_on_recovery: bool = True


@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker operation."""
    
    # State tracking
    current_state: CircuitState = CircuitState.CLOSED
    state_change_count: int = 0
    last_state_change: Optional[datetime] = None
    
    # Failure tracking
    failure_count: int = 0
    consecutive_failures: int = 0
    last_failure_time: Optional[datetime] = None
    
    # Success tracking
    success_count: int = 0
    consecutive_successes: int = 0
    last_success_time: Optional[datetime] = None
    
    # Recovery tracking
    recovery_attempts: int = 0
    last_recovery_attempt: Optional[datetime] = None
    total_recovery_time_seconds: float = 0.0
    
    # Performance metrics
    total_calls: int = 0
    blocked_calls: int = 0
    avg_response_time_ms: float = 0.0
    
    # Time windows
    circuit_open_time: Optional[datetime] = None
    circuit_half_open_time: Optional[datetime] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_calls == 0:
            return 0.0
        return (self.success_count / self.total_calls) * 100
    
    @property
    def time_since_last_failure(self) -> Optional[float]:
        """Get seconds since last failure."""
        if self.last_failure_time:
            return (datetime.now() - self.last_failure_time).total_seconds()
        return None
    
    @property
    def circuit_open_duration(self) -> Optional[float]:
        """Get duration circuit has been open in seconds."""
        if self.circuit_open_time:
            return (datetime.now() - self.circuit_open_time).total_seconds()
        return None


class EnhancedCircuitBreaker:
    """
    Enhanced circuit breaker with automatic recovery and intelligent failure handling.
    
    Features:
    - Multiple recovery strategies
    - Automatic fallback to alternative data sources
    - Degraded mode operation
    - Health check monitoring
    - Integration with alert system
    """
    
    def __init__(
        self, 
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        fallback_handler: Optional[Callable] = None,
        health_checker: Optional[Callable] = None
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.fallback_handler = fallback_handler
        self.health_checker = health_checker
        
        # State management
        self.metrics = CircuitBreakerMetrics()
        self.state = CircuitState.CLOSED
        
        # Recovery management
        self.recovery_task: Optional[asyncio.Task] = None
        self.health_check_task: Optional[asyncio.Task] = None
        
        # Alert integration
        self.alert_callbacks: List[Callable] = []
        
        self.logger = logger.with_context(component="circuit_breaker", name=name)
        self.logger.info("Enhanced circuit breaker initialized")
    
    def add_alert_callback(self, callback: Callable[[CollectionAlert], None]) -> None:
        """Add callback for circuit breaker alerts."""
        self.alert_callbacks.append(callback)
    
    async def call(
        self, 
        func: Callable, 
        *args, 
        fallback_enabled: bool = True,
        **kwargs
    ) -> Any:
        """
        Execute a function call through the circuit breaker.
        
        Args:
            func: Function to execute
            *args: Function arguments
            fallback_enabled: Whether to use fallback on failure
            **kwargs: Function keyword arguments
            
        Returns:
            Function result or fallback result
            
        Raises:
            CircuitBreakerOpenError: When circuit is open and no fallback available
        """
        self.metrics.total_calls += 1
        
        # Check circuit state
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                await self._transition_to_half_open()
            else:
                return await self._handle_blocked_call(func, *args, fallback_enabled=fallback_enabled, **kwargs)
        
        elif self.state == CircuitState.HALF_OPEN:
            if self.metrics.consecutive_successes >= self.config.half_open_max_calls:
                return await self._handle_blocked_call(func, *args, fallback_enabled=fallback_enabled, **kwargs)
        
        # Execute the function call
        try:
            start_time = time.time()
            result = await func(*args, **kwargs)
            response_time = (time.time() - start_time) * 1000
            
            await self._record_success(response_time)
            return result
            
        except Exception as e:
            await self._record_failure(e)
            
            # Try fallback if enabled
            if fallback_enabled:
                return await self._try_fallback(func, *args, **kwargs)
            else:
                raise
    
    async def _record_success(self, response_time_ms: float) -> None:
        """Record a successful call."""
        self.metrics.success_count += 1
        self.metrics.consecutive_successes += 1
        self.metrics.consecutive_failures = 0
        self.metrics.last_success_time = datetime.now()
        
        # Update average response time
        if self.metrics.avg_response_time_ms == 0:
            self.metrics.avg_response_time_ms = response_time_ms
        else:
            self.metrics.avg_response_time_ms = (self.metrics.avg_response_time_ms + response_time_ms) / 2
        
        # Check if we should close the circuit
        if self.state == CircuitState.HALF_OPEN:
            if self.metrics.consecutive_successes >= self.config.success_threshold:
                await self._transition_to_closed()
        
        self.logger.debug("Success recorded", 
                         consecutive_successes=self.metrics.consecutive_successes,
                         response_time_ms=response_time_ms)
    
    async def _record_failure(self, error: Exception) -> None:
        """Record a failed call."""
        self.metrics.failure_count += 1
        self.metrics.consecutive_failures += 1
        self.metrics.consecutive_successes = 0
        self.metrics.last_failure_time = datetime.now()
        
        self.logger.warning("Failure recorded", 
                           consecutive_failures=self.metrics.consecutive_failures,
                           error=str(error))
        
        # Check if we should open the circuit
        if (self.state == CircuitState.CLOSED and 
            self.metrics.consecutive_failures >= self.config.failure_threshold):
            await self._transition_to_open()
        
        elif (self.state == CircuitState.HALF_OPEN and 
              self.metrics.consecutive_failures >= 1):
            await self._transition_to_open()
    
    async def _transition_to_open(self) -> None:
        """Transition circuit breaker to open state."""
        self.state = CircuitState.OPEN
        self.metrics.current_state = CircuitState.OPEN
        self.metrics.circuit_open_time = datetime.now()
        self.metrics.state_change_count += 1
        self.metrics.last_state_change = datetime.now()
        
        self.logger.warning("Circuit breaker opened", 
                           failure_count=self.metrics.failure_count,
                           consecutive_failures=self.metrics.consecutive_failures)
        
        # Send alert
        if self.config.alert_on_open:
            await self._send_alert(
                AlertSeverity.CRITICAL,
                "circuit_breaker_open",
                f"Circuit breaker {self.name} opened due to {self.metrics.consecutive_failures} consecutive failures"
            )
        
        # Start automatic recovery if enabled
        if self.config.enable_automatic_recovery:
            await self._start_recovery_process()
    
    async def _transition_to_half_open(self) -> None:
        """Transition circuit breaker to half-open state."""
        self.state = CircuitState.HALF_OPEN
        self.metrics.current_state = CircuitState.HALF_OPEN
        self.metrics.circuit_half_open_time = datetime.now()
        self.metrics.state_change_count += 1
        self.metrics.last_state_change = datetime.now()
        self.metrics.consecutive_successes = 0
        
        self.logger.info("Circuit breaker transitioned to half-open")
    
    async def _transition_to_closed(self) -> None:
        """Transition circuit breaker to closed state."""
        recovery_time = 0.0
        if self.metrics.circuit_open_time:
            recovery_time = (datetime.now() - self.metrics.circuit_open_time).total_seconds()
            self.metrics.total_recovery_time_seconds += recovery_time
        
        self.state = CircuitState.CLOSED
        self.metrics.current_state = CircuitState.CLOSED
        self.metrics.circuit_open_time = None
        self.metrics.circuit_half_open_time = None
        self.metrics.state_change_count += 1
        self.metrics.last_state_change = datetime.now()
        self.metrics.consecutive_failures = 0
        
        self.logger.info("Circuit breaker closed", 
                        recovery_time_seconds=recovery_time,
                        consecutive_successes=self.metrics.consecutive_successes)
        
        # Send recovery alert
        if self.config.alert_on_recovery:
            await self._send_alert(
                AlertSeverity.INFO,
                "circuit_breaker_recovered",
                f"Circuit breaker {self.name} recovered after {recovery_time:.1f} seconds"
            )
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit breaker."""
        if not self.metrics.circuit_open_time:
            return False
        
        time_open = (datetime.now() - self.metrics.circuit_open_time).total_seconds()
        return time_open >= self.config.timeout_duration_seconds
    
    async def _handle_blocked_call(
        self, 
        func: Callable, 
        *args, 
        fallback_enabled: bool = True,
        **kwargs
    ) -> Any:
        """Handle a call when circuit breaker is open."""
        self.metrics.blocked_calls += 1
        
        self.logger.debug("Call blocked by circuit breaker", state=self.state.value)
        
        # Try fallback if available and enabled
        if fallback_enabled:
            return await self._try_fallback(func, *args, **kwargs)
        else:
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is open")
    
    async def _try_fallback(self, func: Callable, *args, **kwargs) -> Any:
        """Attempt to use fallback handler."""
        if self.fallback_handler:
            try:
                self.logger.info("Attempting fallback handler")
                return await self.fallback_handler(*args, **kwargs)
            except Exception as e:
                self.logger.error("Fallback handler failed", error=str(e))
                raise
        
        # If no fallback handler, try degraded mode
        if self.config.enable_degraded_mode:
            self.logger.info("Operating in degraded mode")
            return await self._degraded_mode_response()
        
        raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is open and no fallback available")
    
    async def _degraded_mode_response(self) -> Any:
        """Provide a degraded mode response."""
        # Return empty result with metadata indicating degraded mode
        return {
            "data": [],
            "success": True,
            "degraded_mode": True,
            "timestamp": datetime.now().isoformat(),
            "source": f"{self.name}_degraded"
        }
    
    async def _start_recovery_process(self) -> None:
        """Start automatic recovery process."""
        if self.recovery_task and not self.recovery_task.done():
            self.recovery_task.cancel()
            try:
                await self.recovery_task
            except asyncio.CancelledError:
                pass  # Expected when cancelling
        
        self.recovery_task = asyncio.create_task(self._recovery_loop())
        self.logger.info("Started automatic recovery process")
    
    async def _recovery_loop(self) -> None:
        """Main recovery loop with different strategies."""
        strategy = self.config.recovery_strategy
        attempt = 0
        
        while (self.state == CircuitState.OPEN and 
               attempt < self.config.max_retry_attempts):
            
            attempt += 1
            self.metrics.recovery_attempts = attempt
            self.metrics.last_recovery_attempt = datetime.now()
            
            # Calculate delay based on strategy
            delay = self._calculate_recovery_delay(strategy, attempt)
            
            self.logger.info("Recovery attempt", 
                           attempt=attempt, 
                           strategy=strategy.value,
                           delay_seconds=delay)
            
            await asyncio.sleep(delay)
            
            # Perform health check
            if await self._perform_health_check():
                self.logger.info("Health check passed, transitioning to half-open")
                await self._transition_to_half_open()
                break
            else:
                self.logger.warning("Health check failed", attempt=attempt)
        
        if self.state == CircuitState.OPEN:
            self.logger.error("All recovery attempts exhausted")
    
    def _calculate_recovery_delay(self, strategy: RecoveryStrategy, attempt: int) -> float:
        """Calculate delay for recovery attempt based on strategy."""
        base_delay = self.config.base_retry_delay_seconds
        max_delay = self.config.max_retry_delay_seconds
        
        if strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF:
            delay = base_delay * (2 ** (attempt - 1))
        elif strategy == RecoveryStrategy.LINEAR_BACKOFF:
            delay = base_delay * attempt
        else:  # IMMEDIATE_RETRY
            delay = base_delay
        
        return min(delay, max_delay)
    
    async def _perform_health_check(self) -> bool:
        """Perform health check to determine if service has recovered."""
        if self.health_checker:
            try:
                result = await self.health_checker()
                return bool(result)
            except Exception as e:
                self.logger.debug("Health check failed", error=str(e))
                return False
        
        # Default health check - just wait for timeout
        return True
    
    async def _send_alert(self, severity: AlertSeverity, alert_type: str, message: str) -> None:
        """Send alert through registered callbacks."""
        alert = CollectionAlert(
            source=self.name,
            alert_type=alert_type,
            severity=severity,
            message=message,
            metadata={
                "circuit_state": self.state.value,
                "failure_count": self.metrics.failure_count,
                "consecutive_failures": self.metrics.consecutive_failures,
                "success_rate": self.metrics.success_rate,
                "recovery_attempts": self.metrics.recovery_attempts
            }
        )
        
        for callback in self.alert_callbacks:
            try:
                await callback(alert)
            except Exception as e:
                self.logger.error("Alert callback failed", error=str(e))
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status."""
        return {
            "name": self.name,
            "state": self.state.value,
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "success_count": self.metrics.success_count,
                "failure_count": self.metrics.failure_count,
                "blocked_calls": self.metrics.blocked_calls,
                "success_rate": self.metrics.success_rate,
                "consecutive_failures": self.metrics.consecutive_failures,
                "consecutive_successes": self.metrics.consecutive_successes,
                "avg_response_time_ms": self.metrics.avg_response_time_ms,
                "recovery_attempts": self.metrics.recovery_attempts,
                "circuit_open_duration": self.metrics.circuit_open_duration,
                "last_failure_time": self.metrics.last_failure_time.isoformat() if self.metrics.last_failure_time else None,
                "last_success_time": self.metrics.last_success_time.isoformat() if self.metrics.last_success_time else None
            },
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "timeout_duration_seconds": self.config.timeout_duration_seconds,
                "recovery_strategy": self.config.recovery_strategy.value,
                "enable_automatic_recovery": self.config.enable_automatic_recovery
            }
        }
    
    async def reset(self) -> None:
        """Manually reset the circuit breaker."""
        if self.recovery_task and not self.recovery_task.done():
            self.recovery_task.cancel()
        
        self.state = CircuitState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        
        self.logger.info("Circuit breaker manually reset")
    
    async def force_open(self) -> None:
        """Manually force the circuit breaker open."""
        await self._transition_to_open()
        self.logger.warning("Circuit breaker manually forced open")
    
    async def cleanup(self) -> None:
        """Clean up circuit breaker resources."""
        if self.recovery_task and not self.recovery_task.done():
            self.recovery_task.cancel()
        
        if self.health_check_task and not self.health_check_task.done():
            self.health_check_task.cancel()


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerManager:
    """Manages multiple circuit breakers for different services."""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, EnhancedCircuitBreaker] = {}
        self.logger = logger.with_context(component="circuit_breaker_manager")
    
    def create_circuit_breaker(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        fallback_handler: Optional[Callable] = None,
        health_checker: Optional[Callable] = None
    ) -> EnhancedCircuitBreaker:
        """Create a new circuit breaker."""
        if name in self.circuit_breakers:
            self.logger.warning("Circuit breaker already exists", name=name)
            return self.circuit_breakers[name]
        
        circuit_breaker = EnhancedCircuitBreaker(name, config, fallback_handler, health_checker)
        self.circuit_breakers[name] = circuit_breaker
        
        self.logger.info("Circuit breaker created", name=name)
        return circuit_breaker
    
    def get_circuit_breaker(self, name: str) -> Optional[EnhancedCircuitBreaker]:
        """Get an existing circuit breaker."""
        return self.circuit_breakers.get(name)
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get status of all circuit breakers."""
        return {
            name: cb.get_status() 
            for name, cb in self.circuit_breakers.items()
        }
    
    async def cleanup_all(self) -> None:
        """Clean up all circuit breakers."""
        for cb in self.circuit_breakers.values():
            await cb.cleanup()
        
        self.circuit_breakers.clear()
        self.logger.info("All circuit breakers cleaned up")


# Global circuit breaker manager instance
circuit_breaker_manager = CircuitBreakerManager()


__all__ = [
    "CircuitState",
    "RecoveryStrategy", 
    "CircuitBreakerConfig",
    "CircuitBreakerMetrics",
    "EnhancedCircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitBreakerManager",
    "circuit_breaker_manager"
]