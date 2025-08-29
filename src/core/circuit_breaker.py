"""
Circuit Breaker Pattern Implementation for System Reliability

Provides fault tolerance by preventing cascading failures when services are down.
Automatically opens circuit when failures exceed threshold, closes after recovery period.
"""

import asyncio
import time
from enum import Enum
from typing import Any, Callable, Optional
import structlog

logger = structlog.get_logger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"          # Blocking all calls
    HALF_OPEN = "half_open" # Testing if service is back


class CircuitBreaker:
    """
    Circuit breaker for fault tolerance.
    
    - CLOSED: Normal operation, failures counted
    - OPEN: All calls rejected immediately, preventing cascading failures  
    - HALF_OPEN: Testing if service recovered, limited calls allowed
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception
    ):
        """
        Initialize circuit breaker.
        
        Args:
            name: Circuit breaker identifier
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before trying half-open
            expected_exception: Exception type that counts as failure
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        # State tracking
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.success_count = 0  # For half-open state
        
        logger.info(f"Circuit breaker '{name}' initialized", 
                   threshold=failure_threshold, recovery_timeout=recovery_timeout)
    
    async def __call__(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result if successful
            
        Raises:
            CircuitBreakerOpenError: If circuit is open
            Original exception: If function fails
        """
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self._move_to_half_open()
            else:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker '{self.name}' is OPEN. "
                    f"Will retry after {self.recovery_timeout}s."
                )
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
            
        return time.time() - self.last_failure_time >= self.recovery_timeout
    
    def _move_to_half_open(self) -> None:
        """Move circuit to half-open state for testing."""
        self.state = CircuitBreakerState.HALF_OPEN
        self.success_count = 0
        logger.info(f"Circuit breaker '{self.name}' moved to HALF_OPEN")
    
    def _on_success(self) -> None:
        """Handle successful function execution."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            # Reset circuit after successful half-open test
            if self.success_count >= 1:  # Single success resets circuit
                self._reset()
        elif self.state == CircuitBreakerState.CLOSED:
            # Reset failure count on success
            self.failure_count = 0
    
    def _on_failure(self) -> None:
        """Handle failed function execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            # Failure in half-open moves back to open
            self._open_circuit()
        elif self.failure_count >= self.failure_threshold:
            self._open_circuit()
    
    def _open_circuit(self) -> None:
        """Open the circuit breaker."""
        self.state = CircuitBreakerState.OPEN
        logger.warning(f"Circuit breaker '{self.name}' OPENED", 
                      failure_count=self.failure_count)
    
    def _reset(self) -> None:
        """Reset circuit breaker to closed state."""
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        logger.info(f"Circuit breaker '{self.name}' RESET to CLOSED")
    
    def get_state(self) -> dict:
        """Get current circuit breaker state."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "last_failure_time": self.last_failure_time,
            "recovery_timeout": self.recovery_timeout
        }


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and blocking calls."""
    pass


# Global circuit breakers for common services
DATABASE_CIRCUIT_BREAKER = CircuitBreaker(
    name="database",
    failure_threshold=3,
    recovery_timeout=30
)

API_CIRCUIT_BREAKER = CircuitBreaker(
    name="external_api", 
    failure_threshold=5,
    recovery_timeout=60
)

ACTION_NETWORK_CIRCUIT_BREAKER = CircuitBreaker(
    name="action_network_api",
    failure_threshold=3,
    recovery_timeout=45
)