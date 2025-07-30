"""
ML Pipeline Resilience Module
Provides circuit breaker patterns, graceful degradation, and fallback strategies
for external service failures in production environments.
"""

from .circuit_breaker import CircuitBreaker, CircuitBreakerState
from .degradation_manager import DegradationManager, ServiceStatus
from .fallback_strategies import FallbackStrategies

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerState", 
    "DegradationManager",
    "ServiceStatus",
    "FallbackStrategies",
]