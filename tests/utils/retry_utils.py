"""
Retry utilities for testing.

Provides retry logic, exponential backoff, and circuit breaker patterns for test reliability.
"""

import asyncio
import time
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
from functools import wraps
from unittest.mock import Mock

from tests.utils.logging_utils import create_test_logger


T = TypeVar('T')


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    RANDOM_JITTER = "random_jitter"


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    
    max_attempts: int = 3
    initial_delay: float = 0.1  # 100ms
    max_delay: float = 10.0     # 10 seconds
    exponential_factor: float = 2.0
    jitter_factor: float = 0.1
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    
    # Circuit breaker settings
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    success_threshold: int = 2  # Successes needed to close circuit
    
    # Retry conditions
    retry_on_exceptions: tuple = (Exception,)
    retry_on_status_codes: List[int] = None
    
    @classmethod
    def for_api_calls(cls) -> 'RetryConfig':
        """Configuration optimized for API calls."""
        return cls(
            max_attempts=3,
            initial_delay=0.5,
            max_delay=30.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            retry_on_exceptions=(ConnectionError, TimeoutError, Exception)
        )
    
    @classmethod
    def for_database_operations(cls) -> 'RetryConfig':
        """Configuration optimized for database operations."""
        return cls(
            max_attempts=5,
            initial_delay=0.1,
            max_delay=5.0,
            strategy=RetryStrategy.LINEAR_BACKOFF,
            failure_threshold=10
        )
    
    @classmethod
    def for_integration_tests(cls) -> 'RetryConfig':
        """Configuration optimized for integration tests."""
        return cls(
            max_attempts=2,
            initial_delay=0.2,
            max_delay=2.0,
            strategy=RetryStrategy.FIXED_DELAY
        )


class RetryStatistics:
    """Tracks retry statistics."""
    
    def __init__(self):
        self.total_attempts = 0
        self.successful_attempts = 0
        self.failed_attempts = 0
        self.retry_attempts = 0
        self.circuit_breaker_opens = 0
        self.average_response_time = 0.0
        self.response_times: List[float] = []
    
    def record_attempt(self, success: bool, response_time: float, was_retry: bool = False):
        """Record an attempt."""
        self.total_attempts += 1
        self.response_times.append(response_time)
        
        if success:
            self.successful_attempts += 1
        else:
            self.failed_attempts += 1
        
        if was_retry:
            self.retry_attempts += 1
        
        self.average_response_time = sum(self.response_times) / len(self.response_times)
    
    def record_circuit_breaker_open(self):
        """Record circuit breaker opening."""
        self.circuit_breaker_opens += 1
    
    def get_success_rate(self) -> float:
        """Get success rate as percentage."""
        if self.total_attempts == 0:
            return 0.0
        return (self.successful_attempts / self.total_attempts) * 100
    
    def get_retry_rate(self) -> float:
        """Get retry rate as percentage."""
        if self.total_attempts == 0:
            return 0.0
        return (self.retry_attempts / self.total_attempts) * 100
    
    def get_summary(self) -> Dict[str, Any]:
        """Get statistics summary."""
        return {
            "total_attempts": self.total_attempts,
            "successful_attempts": self.successful_attempts,
            "failed_attempts": self.failed_attempts,
            "retry_attempts": self.retry_attempts,
            "success_rate_percent": self.get_success_rate(),
            "retry_rate_percent": self.get_retry_rate(),
            "circuit_breaker_opens": self.circuit_breaker_opens,
            "average_response_time_ms": self.average_response_time * 1000,
            "p95_response_time_ms": sorted(self.response_times)[int(0.95 * len(self.response_times))] * 1000 if self.response_times else 0
        }


class CircuitBreaker:
    """Circuit breaker implementation for preventing cascade failures."""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.logger = create_test_logger("circuit_breaker")
    
    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        
        if self.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has passed
            if time.time() - self.last_failure_time >= self.config.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
                self.logger.info("Circuit breaker moved to HALF_OPEN state")
                return True
            return False
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            return True
        
        return False
    
    def record_success(self):
        """Record successful execution."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.logger.info("Circuit breaker moved to CLOSED state")
        
        if self.state == CircuitBreakerState.CLOSED:
            self.failure_count = 0
    
    def record_failure(self):
        """Record failed execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.config.failure_threshold:
            if self.state != CircuitBreakerState.OPEN:
                self.state = CircuitBreakerState.OPEN
                self.logger.warning(f"Circuit breaker OPENED after {self.failure_count} failures")
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.success_count = 0


class RetryManager:
    """Manages retry behavior with circuit breaker integration."""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.circuit_breaker = CircuitBreaker(config)
        self.statistics = RetryStatistics()
        self.logger = create_test_logger("retry_manager")
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay based on retry strategy."""
        if self.config.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.config.initial_delay
        
        elif self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.config.initial_delay * (self.config.exponential_factor ** (attempt - 1))
        
        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.initial_delay * attempt
        
        elif self.config.strategy == RetryStrategy.RANDOM_JITTER:
            base_delay = self.config.initial_delay * (self.config.exponential_factor ** (attempt - 1))
            jitter = base_delay * self.config.jitter_factor * random.random()
            delay = base_delay + jitter
        
        else:
            delay = self.config.initial_delay
        
        return min(delay, self.config.max_delay)
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if operation should be retried."""
        if attempt >= self.config.max_attempts:
            return False
        
        if not isinstance(exception, self.config.retry_on_exceptions):
            return False
        
        return True
    
    async def execute_async(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute async function with retry logic."""
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is OPEN")
        
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                response_time = time.time() - start_time
                
                self.statistics.record_attempt(True, response_time, attempt > 1)
                self.circuit_breaker.record_success()
                
                if attempt > 1:
                    self.logger.info(f"✅ Operation succeeded on attempt {attempt}")
                
                return result
            
            except Exception as e:
                response_time = time.time() - start_time
                last_exception = e
                
                self.statistics.record_attempt(False, response_time, attempt > 1)
                self.circuit_breaker.record_failure()
                
                if not self.should_retry(e, attempt):
                    self.logger.error(f"❌ Operation failed permanently after {attempt} attempts: {e}")
                    break
                
                if attempt < self.config.max_attempts:
                    delay = self.calculate_delay(attempt)
                    self.logger.warning(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {delay:.2f}s")
                    await asyncio.sleep(delay)
        
        # If we get here, all attempts failed
        if self.circuit_breaker.state == CircuitBreakerState.OPEN:
            self.statistics.record_circuit_breaker_open()
        
        raise last_exception
    
    def execute_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute sync function with retry logic."""
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is OPEN")
        
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                response_time = time.time() - start_time
                
                self.statistics.record_attempt(True, response_time, attempt > 1)
                self.circuit_breaker.record_success()
                
                if attempt > 1:
                    self.logger.info(f"✅ Operation succeeded on attempt {attempt}")
                
                return result
            
            except Exception as e:
                response_time = time.time() - start_time
                last_exception = e
                
                self.statistics.record_attempt(False, response_time, attempt > 1)
                self.circuit_breaker.record_failure()
                
                if not self.should_retry(e, attempt):
                    self.logger.error(f"❌ Operation failed permanently after {attempt} attempts: {e}")
                    break
                
                if attempt < self.config.max_attempts:
                    delay = self.calculate_delay(attempt)
                    self.logger.warning(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {delay:.2f}s")
                    time.sleep(delay)
        
        # If we get here, all attempts failed
        if self.circuit_breaker.state == CircuitBreakerState.OPEN:
            self.statistics.record_circuit_breaker_open()
        
        raise last_exception


def retry_async(config: RetryConfig = None):
    """Async retry decorator."""
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retry_manager = RetryManager(config)
            return await retry_manager.execute_async(func, *args, **kwargs)
        
        # Attach retry manager for inspection
        wrapper._retry_manager = RetryManager(config)
        return wrapper
    
    return decorator


def retry_sync(config: RetryConfig = None):
    """Sync retry decorator."""
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_manager = RetryManager(config)
            return retry_manager.execute_sync(func, *args, **kwargs)
        
        # Attach retry manager for inspection
        wrapper._retry_manager = RetryManager(config)
        return wrapper
    
    return decorator


class TransientFailureSimulator:
    """Simulates transient failures for testing retry logic."""
    
    def __init__(self, failure_rate: float = 0.3, failure_count: int = 2):
        self.failure_rate = failure_rate  # Probability of failure (0.0 - 1.0)
        self.failure_count = failure_count  # Number of failures before success
        self.current_failures = 0
        self.total_calls = 0
    
    def should_fail(self) -> bool:
        """Determine if this call should fail."""
        self.total_calls += 1
        
        # Always fail for the first N calls if using failure_count
        if self.failure_count > 0 and self.current_failures < self.failure_count:
            self.current_failures += 1
            return True
        
        # Use probability-based failure
        return random.random() < self.failure_rate
    
    def reset(self):
        """Reset failure simulation."""
        self.current_failures = 0
        self.total_calls = 0


# Global retry managers for common scenarios
_api_retry_manager: Optional[RetryManager] = None
_db_retry_manager: Optional[RetryManager] = None


def get_api_retry_manager() -> RetryManager:
    """Get shared retry manager for API calls."""
    global _api_retry_manager
    if _api_retry_manager is None:
        _api_retry_manager = RetryManager(RetryConfig.for_api_calls())
    return _api_retry_manager


def get_db_retry_manager() -> RetryManager:
    """Get shared retry manager for database operations."""
    global _db_retry_manager
    if _db_retry_manager is None:
        _db_retry_manager = RetryManager(RetryConfig.for_database_operations())
    return _db_retry_manager