"""
Circuit Breaker pattern implementation.
"""
import time
from functools import wraps
import logging
import asyncio

logger = logging.getLogger(__name__)

class CircuitBreaker:
    """
    A Circuit Breaker implementation that can be used as a decorator for both sync and async functions.

    This pattern is used to prevent an application from repeatedly trying to
    execute an operation that is likely to fail.
    """
    def __init__(self, failure_threshold=5, recovery_timeout=60, expected_exception=Exception):
        """
        Initializes the Circuit Breaker.

        Args:
            failure_threshold (int): The number of failures to tolerate before opening the circuit.
            recovery_timeout (int): The number of seconds to wait before moving to HALF_OPEN state.
            expected_exception (Exception): The type of exception to count as a failure.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # Can be 'CLOSED', 'OPEN', 'HALF_OPEN'

    def __call__(self, func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if self.state == 'OPEN':
                if self._should_attempt_reset():
                    self.state = 'HALF_OPEN'
                    logger.info("Circuit breaker is now HALF_OPEN.")
                else:
                    logger.warning("Circuit breaker is OPEN. Call is blocked.")
                    raise CircuitBreakerOpenException("Circuit breaker is OPEN")
            
            try:
                result = await func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise e
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if self.state == 'OPEN':
                if self._should_attempt_reset():
                    self.state = 'HALF_OPEN'
                    logger.info("Circuit breaker is now HALF_OPEN.")
                else:
                    logger.warning("Circuit breaker is OPEN. Call is blocked.")
                    raise CircuitBreakerOpenException("Circuit breaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise e
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    def _should_attempt_reset(self):
        """Check if it's time to try resetting the circuit."""
        if self.last_failure_time is None:
            return False
        return (time.time() - self.last_failure_time) >= self.recovery_timeout

    def _on_success(self):
        """Handle a successful call."""
        if self.state == 'HALF_OPEN':
            self.state = 'CLOSED'
            logger.info("Circuit breaker is now CLOSED.")
        self.failure_count = 0
        self.last_failure_time = None

    def _on_failure(self):
        """Handle a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == 'HALF_OPEN':
            self.state = 'OPEN'
            logger.warning("Call failed in HALF_OPEN state. Circuit breaker is now OPEN.")

        elif self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            logger.error("Failure threshold reached. Circuit breaker is now OPEN.")

class CircuitBreakerOpenException(Exception):
    """Exception raised when the circuit breaker is open."""
    pass 