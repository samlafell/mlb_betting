"""
Circuit Breaker Pattern Implementation
Prevents cascading failures by monitoring service health and providing fail-fast behavior
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

try:
    from ...core.config import get_settings
except ImportError:
    def get_settings():
        raise ImportError("Unified config system not available. Please check src.core.config module.")

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitBreakerState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing fast, not calling service
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5          # Failures before opening
    recovery_timeout: int = 60          # Seconds before trying half-open
    success_threshold: int = 3          # Successes needed to close from half-open
    timeout_seconds: float = 10.0       # Operation timeout
    sliding_window_size: int = 100      # Size of the sliding window for failure rate
    minimum_throughput: int = 10        # Minimum requests before evaluating failure rate
    failure_rate_threshold: float = 0.5  # 50% failure rate triggers opening
    
    @classmethod
    def from_unified_config(cls, service_name: str) -> 'CircuitBreakerConfig':
        """Load configuration from unified config if available"""
        if get_settings:
            try:
                config = get_settings()
                ml_config = config.ml_pipeline
                
                return cls(
                    failure_threshold=getattr(ml_config, f'{service_name}_failure_threshold', 5),
                    recovery_timeout=getattr(ml_config, f'{service_name}_recovery_timeout', 60),
                    success_threshold=getattr(ml_config, f'{service_name}_success_threshold', 3),
                    timeout_seconds=getattr(ml_config, f'{service_name}_timeout_seconds', 10.0),
                    sliding_window_size=getattr(ml_config, f'{service_name}_window_size', 100),
                    minimum_throughput=getattr(ml_config, f'{service_name}_min_throughput', 10),
                    failure_rate_threshold=getattr(ml_config, f'{service_name}_failure_rate', 0.5),
                )
            except Exception as e:
                logger.warning(f"Failed to load circuit breaker config for {service_name}: {e}")
        
        return cls()


@dataclass
class CircuitBreakerMetrics:
    """Circuit breaker metrics and statistics"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeouts: int = 0
    circuit_opened_count: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    average_response_time: float = 0.0
    sliding_window: list = field(default_factory=list)  # Recent request results
    
    @property
    def failure_rate(self) -> float:
        """Calculate current failure rate"""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests
    
    @property
    def success_rate(self) -> float:
        """Calculate current success rate"""
        return 1.0 - self.failure_rate
    
    def add_result(self, success: bool, response_time: float, window_size: int = 100):
        """Add a request result to metrics"""
        self.total_requests += 1
        current_time = time.time()
        
        if success:
            self.successful_requests += 1
            self.last_success_time = current_time
        else:
            self.failed_requests += 1
            self.last_failure_time = current_time
        
        # Update sliding window
        self.sliding_window.append({'success': success, 'time': current_time})
        if len(self.sliding_window) > window_size:
            self.sliding_window.pop(0)
        
        # Update average response time (exponential moving average)
        alpha = 0.1  # Smoothing factor
        if self.average_response_time == 0.0:
            self.average_response_time = response_time
        else:
            self.average_response_time = (alpha * response_time + 
                                       (1 - alpha) * self.average_response_time)
    
    def get_sliding_failure_rate(self) -> float:
        """Get failure rate from sliding window"""
        if len(self.sliding_window) == 0:
            return 0.0
        
        failures = sum(1 for result in self.sliding_window if not result['success'])
        return failures / len(self.sliding_window)


class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open"""
    def __init__(self, service_name: str, state: CircuitBreakerState):
        self.service_name = service_name
        self.state = state
        super().__init__(f"Circuit breaker for {service_name} is {state.value}")


class CircuitBreaker:
    """
    Circuit Breaker implementation with sliding window failure detection
    Provides fail-fast behavior and automatic recovery testing
    """
    
    def __init__(self, service_name: str, config: Optional[CircuitBreakerConfig] = None):
        self.service_name = service_name
        self.config = config or CircuitBreakerConfig.from_unified_config(service_name)
        self.state = CircuitBreakerState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self.last_state_change = time.time()
        self.half_open_success_count = 0
        self._lock = asyncio.Lock()
        
        logger.info(f"Circuit breaker initialized for {service_name}: {self.config}")
    
    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """
        Execute function with circuit breaker protection
        
        Args:
            func: Async function to execute
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerError: When circuit is open
            asyncio.TimeoutError: When operation times out
        """
        async with self._lock:
            await self._check_state()
            
            if self.state == CircuitBreakerState.OPEN:
                raise CircuitBreakerError(self.service_name, self.state)
            
            # In CLOSED or HALF_OPEN state, attempt the call
            start_time = time.time()
            
            try:
                # Execute with timeout
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.config.timeout_seconds
                )
                
                # Record success
                response_time = time.time() - start_time
                await self._record_success(response_time)
                
                return result
                
            except asyncio.TimeoutError:
                response_time = time.time() - start_time
                self.metrics.timeouts += 1
                await self._record_failure(response_time, "timeout")
                raise
                
            except Exception as e:
                response_time = time.time() - start_time
                await self._record_failure(response_time, str(e))
                raise
    
    async def _check_state(self):
        """Check and update circuit breaker state based on current conditions"""
        current_time = time.time()
        
        if self.state == CircuitBreakerState.OPEN:
            # Check if we should transition to HALF_OPEN
            if current_time - self.last_state_change >= self.config.recovery_timeout:
                await self._transition_to_half_open()
                
        elif self.state == CircuitBreakerState.CLOSED:
            # Check if we should transition to OPEN based on failure rate
            if (len(self.metrics.sliding_window) >= self.config.minimum_throughput and
                self.metrics.get_sliding_failure_rate() >= self.config.failure_rate_threshold):
                await self._transition_to_open()
    
    async def _record_success(self, response_time: float):
        """Record a successful operation"""
        self.metrics.add_result(True, response_time, self.config.sliding_window_size)
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_success_count += 1
            if self.half_open_success_count >= self.config.success_threshold:
                await self._transition_to_closed()
        
        logger.debug(f"Circuit breaker {self.service_name}: Success recorded "
                    f"(response_time={response_time:.3f}s)")
    
    async def _record_failure(self, response_time: float, error: str):
        """Record a failed operation"""
        self.metrics.add_result(False, response_time, self.config.sliding_window_size)
        
        logger.warning(f"Circuit breaker {self.service_name}: Failure recorded "
                      f"(error={error}, response_time={response_time:.3f}s, "
                      f"failure_rate={self.metrics.get_sliding_failure_rate():.2f})")
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            # Any failure in half-open state transitions back to open
            await self._transition_to_open()
        elif self.state == CircuitBreakerState.CLOSED:
            # Check if failure threshold is reached
            if (len(self.metrics.sliding_window) >= self.config.minimum_throughput and
                self.metrics.get_sliding_failure_rate() >= self.config.failure_rate_threshold):
                await self._transition_to_open()
    
    async def _transition_to_open(self):
        """Transition circuit breaker to OPEN state"""
        old_state = self.state
        self.state = CircuitBreakerState.OPEN
        self.last_state_change = time.time()
        self.metrics.circuit_opened_count += 1
        self.half_open_success_count = 0
        
        logger.warning(f"Circuit breaker {self.service_name}: {old_state.value} -> OPEN "
                      f"(failure_rate={self.metrics.get_sliding_failure_rate():.2f}, "
                      f"total_failures={self.metrics.failed_requests})")
    
    async def _transition_to_half_open(self):
        """Transition circuit breaker to HALF_OPEN state"""
        old_state = self.state
        self.state = CircuitBreakerState.HALF_OPEN
        self.last_state_change = time.time()
        self.half_open_success_count = 0
        
        logger.info(f"Circuit breaker {self.service_name}: {old_state.value} -> HALF_OPEN "
                   f"(testing recovery after {self.config.recovery_timeout}s)")
    
    async def _transition_to_closed(self):
        """Transition circuit breaker to CLOSED state"""
        old_state = self.state
        self.state = CircuitBreakerState.CLOSED
        self.last_state_change = time.time()
        self.half_open_success_count = 0
        
        logger.info(f"Circuit breaker {self.service_name}: {old_state.value} -> CLOSED "
                   f"(recovered after {self.config.success_threshold} successes)")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status and metrics"""
        return {
            "service_name": self.service_name,
            "state": self.state.value,
            "last_state_change": datetime.fromtimestamp(self.last_state_change).isoformat(),
            "metrics": {
                "total_requests": self.metrics.total_requests,
                "successful_requests": self.metrics.successful_requests,
                "failed_requests": self.metrics.failed_requests,
                "timeouts": self.metrics.timeouts,
                "failure_rate": round(self.metrics.failure_rate, 3),
                "sliding_failure_rate": round(self.metrics.get_sliding_failure_rate(), 3),
                "success_rate": round(self.metrics.success_rate, 3),
                "average_response_time": round(self.metrics.average_response_time, 3),
                "circuit_opened_count": self.metrics.circuit_opened_count,
                "last_failure_time": (datetime.fromtimestamp(self.metrics.last_failure_time).isoformat() 
                                    if self.metrics.last_failure_time else None),
                "last_success_time": (datetime.fromtimestamp(self.metrics.last_success_time).isoformat() 
                                    if self.metrics.last_success_time else None),
            },
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "success_threshold": self.config.success_threshold,
                "timeout_seconds": self.config.timeout_seconds,
                "failure_rate_threshold": self.config.failure_rate_threshold,
            }
        }
    
    async def force_open(self):
        """Manually force circuit breaker to OPEN state (for testing/emergency)"""
        async with self._lock:
            await self._transition_to_open()
            logger.warning(f"Circuit breaker {self.service_name}: Manually forced to OPEN")
    
    async def force_closed(self):
        """Manually force circuit breaker to CLOSED state (for recovery)"""
        async with self._lock:
            await self._transition_to_closed()
            logger.info(f"Circuit breaker {self.service_name}: Manually forced to CLOSED")
    
    async def reset_metrics(self):
        """Reset all metrics (for testing purposes)"""
        async with self._lock:
            self.metrics = CircuitBreakerMetrics()
            logger.info(f"Circuit breaker {self.service_name}: Metrics reset")


class CircuitBreakerManager:
    """
    Manages multiple circuit breakers for different services
    """
    
    def __init__(self):
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()
    
    async def get_circuit_breaker(self, service_name: str, 
                                config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get or create a circuit breaker for a service"""
        async with self._lock:
            if service_name not in self._circuit_breakers:
                self._circuit_breakers[service_name] = CircuitBreaker(service_name, config)
                logger.info(f"Created new circuit breaker for service: {service_name}")
            
            return self._circuit_breakers[service_name]
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers"""
        return {
            name: breaker.get_status() 
            for name, breaker in self._circuit_breakers.items()
        }
    
    async def reset_all_metrics(self):
        """Reset metrics for all circuit breakers"""
        async with self._lock:
            for breaker in self._circuit_breakers.values():
                await breaker.reset_metrics()
            logger.info("All circuit breaker metrics reset")


# Global circuit breaker manager instance
circuit_breaker_manager = CircuitBreakerManager()


# Convenience functions
async def get_circuit_breaker(service_name: str, 
                            config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
    """Get circuit breaker for a service"""
    return await circuit_breaker_manager.get_circuit_breaker(service_name, config)


def circuit_breaker(service_name: str, config: Optional[CircuitBreakerConfig] = None):
    """Decorator for adding circuit breaker protection to async functions"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            breaker = await get_circuit_breaker(service_name, config)
            return await breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator