"""
Reliability utilities for production-grade error handling and fault tolerance.
"""

import functools
import time
from typing import Any, Callable, Optional, Union
import structlog
import asyncio

from .circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

logger = structlog.get_logger(__name__)


def with_retry(
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for automatic retry with exponential backoff.
    
    Args:
        retries: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Backoff multiplier for each retry
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(retries + 1):
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                        
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == retries:
                        logger.error(f"Function {func.__name__} failed after {retries} retries", 
                                   error=str(e), attempt=attempt + 1)
                        break
                    
                    logger.warning(f"Function {func.__name__} failed, retrying", 
                                 error=str(e), attempt=attempt + 1, delay=current_delay)
                    
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                        
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == retries:
                        logger.error(f"Function {func.__name__} failed after {retries} retries", 
                                   error=str(e), attempt=attempt + 1)
                        break
                    
                    logger.warning(f"Function {func.__name__} failed, retrying", 
                                 error=str(e), attempt=attempt + 1, delay=current_delay)
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def with_circuit_breaker(circuit_breaker: CircuitBreaker):
    """
    Decorator to apply circuit breaker pattern to functions.
    
    Args:
        circuit_breaker: CircuitBreaker instance to use
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await circuit_breaker(func, *args, **kwargs)
            except CircuitBreakerOpenError:
                logger.warning(f"Circuit breaker open for {func.__name__}", 
                             circuit_state=circuit_breaker.get_state())
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return circuit_breaker(func, *args, **kwargs)
            except CircuitBreakerOpenError:
                logger.warning(f"Circuit breaker open for {func.__name__}", 
                             circuit_state=circuit_breaker.get_state())
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


class GracefulDegradation:
    """
    Context manager for graceful degradation when services fail.
    """
    
    def __init__(self, fallback_value: Any = None, log_failure: bool = True):
        """
        Initialize graceful degradation context.
        
        Args:
            fallback_value: Value to return on failure
            log_failure: Whether to log failures
        """
        self.fallback_value = fallback_value
        self.log_failure = log_failure
        self.exception: Optional[Exception] = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.exception = exc_val
            if self.log_failure:
                logger.warning("Operation failed, using graceful degradation", 
                             error=str(exc_val), fallback=self.fallback_value)
            return True  # Suppress exception
        return False
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)
    
    def get_result(self, result: Any = None) -> Any:
        """Get result or fallback value if exception occurred."""
        return self.fallback_value if self.exception else result


def safe_database_query(fallback_value: Any = None):
    """
    Decorator for safe database queries with graceful degradation.
    
    Args:
        fallback_value: Value to return if database query fails
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Database query failed in {func.__name__}", 
                           error=str(e), fallback=fallback_value)
                
                # Check if it's a schema-related error
                if any(keyword in str(e).lower() for keyword in 
                       ['column', 'does not exist', 'relation', 'table']):
                    logger.error("Database schema mismatch detected", 
                               function=func.__name__, error=str(e))
                
                return fallback_value
        
        return wrapper
    
    return decorator


class HealthCheck:
    """System health monitoring utilities."""
    
    @staticmethod
    async def check_database_connection(connection) -> dict:
        """Check database connection health."""
        try:
            start_time = time.time()
            await connection.execute_async("SELECT 1", fetch="one")
            response_time = time.time() - start_time
            
            return {
                "status": "healthy",
                "response_time": response_time,
                "timestamp": time.time()
            }
        except Exception as e:
            return {
                "status": "unhealthy", 
                "error": str(e),
                "timestamp": time.time()
            }
    
    @staticmethod
    async def check_api_endpoint(url: str, timeout: int = 5) -> dict:
        """Check external API endpoint health."""
        try:
            import aiohttp
            
            start_time = time.time()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.get(url) as response:
                    response_time = time.time() - start_time
                    
                    return {
                        "status": "healthy",
                        "status_code": response.status,
                        "response_time": response_time,
                        "timestamp": time.time()
                    }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": time.time()
            }