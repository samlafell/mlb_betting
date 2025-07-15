"""
Unified Rate Limiting System for Data Collection

Consolidates rate limiting patterns from:
- mlb_sharp_betting/services/rate_limiter.py (quota tracking, request rate limiting)
- sportsbookreview/services/sportsbookreview_scraper.py (adaptive rate limiting)
- src/mlb_sharp_betting/scrapers/base.py (token bucket, circuit breakers)

Provides enterprise-grade rate limiting with:
- Token bucket algorithm for burst handling
- Circuit breaker pattern for fault tolerance
- Adaptive rate limiting based on success rates
- Per-source rate limit configuration
- Comprehensive monitoring and metrics
"""

import asyncio
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from ...core.logging import LogComponent, get_logger

logger = get_logger(__name__, LogComponent.RATE_LIMITER)


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    # Basic rate limits
    requests_per_second: float = 1.0
    requests_per_minute: int = 30
    requests_per_hour: int = 1000

    # Burst handling
    burst_limit: int = 5
    burst_window_seconds: int = 60

    # Strategy configuration
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET

    # Adaptive configuration
    adaptive_enabled: bool = True
    success_rate_threshold: float = 0.8
    adaptation_factor: float = 0.5

    # Circuit breaker configuration
    circuit_breaker_enabled: bool = True
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 60

    # Backoff configuration
    exponential_backoff: bool = True
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 300.0
    jitter: bool = True


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    wait_time_seconds: float = 0.0
    reason: str = ""
    current_rate: float = 0.0
    tokens_remaining: int = 0
    next_token_time: datetime | None = None

    # Metrics
    requests_in_window: int = 0
    success_rate: float = 1.0
    circuit_breaker_state: str = "closed"


class TokenBucket:
    """
    Token bucket implementation for rate limiting.

    Allows for burst traffic while maintaining average rate limits.
    Consolidates token bucket patterns from legacy scrapers.
    """

    def __init__(
        self, rate: float, capacity: int, initial_tokens: int | None = None
    ) -> None:
        """
        Initialize token bucket.

        Args:
            rate: Token refill rate (tokens per second)
            capacity: Maximum bucket capacity
            initial_tokens: Initial token count (defaults to capacity)
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = initial_tokens or capacity
        self.last_refill = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> tuple[bool, float]:
        """
        Attempt to acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            Tuple of (success, wait_time_seconds)
        """
        with self._lock:
            now = time.time()

            # Refill tokens based on elapsed time
            elapsed = now - self.last_refill
            tokens_to_add = elapsed * self.rate
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now

            # Check if we have enough tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True, 0.0
            else:
                # Calculate wait time for required tokens
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate
                return False, wait_time

    def get_status(self) -> dict[str, Any]:
        """Get current bucket status."""
        with self._lock:
            return {
                "tokens": self.tokens,
                "capacity": self.capacity,
                "rate": self.rate,
                "fill_percentage": (self.tokens / self.capacity) * 100,
            }


class CircuitBreaker:
    """
    Circuit breaker implementation for fault tolerance.

    Prevents cascading failures by temporarily blocking requests
    when error rates exceed thresholds.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        success_threshold: int = 3,
    ) -> None:
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening
            recovery_timeout: Seconds to wait before attempting recovery
            success_threshold: Consecutive successes needed to close
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self.failure_count = 0
        self.success_count = 0
        self.state = "closed"  # closed, open, half_open
        self.last_failure_time = None
        self._lock = threading.Lock()

    def can_proceed(self) -> bool:
        """Check if requests can proceed through the circuit breaker."""
        with self._lock:
            if self.state == "closed":
                return True
            elif self.state == "open":
                # Check if recovery timeout has passed
                if (
                    self.last_failure_time
                    and time.time() - self.last_failure_time >= self.recovery_timeout
                ):
                    self.state = "half_open"
                    self.success_count = 0
                    return True
                return False
            elif self.state == "half_open":
                return True

            return False

    def record_success(self) -> None:
        """Record a successful operation."""
        with self._lock:
            if self.state == "half_open":
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = "closed"
                    self.failure_count = 0
            elif self.state == "closed":
                # Reset failure count on success
                self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed operation."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == "closed" and self.failure_count >= self.failure_threshold:
                self.state = "open"
            elif self.state == "half_open":
                self.state = "open"
                self.success_count = 0

    def get_state(self) -> dict[str, Any]:
        """Get current circuit breaker state."""
        with self._lock:
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "last_failure_time": self.last_failure_time,
            }


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter implementation.

    Tracks requests in a sliding time window for more accurate
    rate limiting than fixed windows.
    """

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        """
        Initialize sliding window rate limiter.

        Args:
            max_requests: Maximum requests in window
            window_seconds: Window size in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: deque = deque()
        self._lock = threading.Lock()

    def can_proceed(self) -> tuple[bool, float]:
        """
        Check if a request can proceed.

        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        with self._lock:
            now = time.time()

            # Remove old requests outside the window
            while self.requests and now - self.requests[0] >= self.window_seconds:
                self.requests.popleft()

            # Check if we're under the limit
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True, 0.0
            else:
                # Calculate wait time until oldest request expires
                oldest_request = self.requests[0]
                wait_time = self.window_seconds - (now - oldest_request)
                return False, max(0.0, wait_time)

    def get_current_rate(self) -> float:
        """Get current request rate."""
        with self._lock:
            now = time.time()

            # Count requests in current window
            recent_requests = sum(
                1 for req_time in self.requests if now - req_time < self.window_seconds
            )

            return recent_requests / self.window_seconds


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts limits based on success rates.

    Consolidates adaptive patterns from sportsbookreview scraper.
    """

    def __init__(self, base_config: RateLimitConfig) -> None:
        """Initialize adaptive rate limiter."""
        self.base_config = base_config
        self.current_multiplier = 1.0
        self.success_history: deque = deque(maxlen=100)
        self._lock = threading.Lock()

    def record_request_result(self, success: bool) -> None:
        """Record the result of a request."""
        with self._lock:
            self.success_history.append(success)
            self._update_multiplier()

    def _update_multiplier(self) -> None:
        """Update rate limit multiplier based on success rate."""
        if len(self.success_history) < 10:
            return

        success_rate = sum(self.success_history) / len(self.success_history)

        if success_rate < self.base_config.success_rate_threshold:
            # Reduce rate on poor success rate
            self.current_multiplier *= self.base_config.adaptation_factor
            self.current_multiplier = max(0.1, self.current_multiplier)
        elif success_rate > 0.95:
            # Gradually increase rate on high success rate
            self.current_multiplier = min(2.0, self.current_multiplier * 1.1)

    def get_adjusted_config(self) -> RateLimitConfig:
        """Get rate limit config adjusted by current multiplier."""
        adjusted = RateLimitConfig(
            requests_per_second=self.base_config.requests_per_second
            * self.current_multiplier,
            requests_per_minute=int(
                self.base_config.requests_per_minute * self.current_multiplier
            ),
            requests_per_hour=int(
                self.base_config.requests_per_hour * self.current_multiplier
            ),
            burst_limit=max(
                1, int(self.base_config.burst_limit * self.current_multiplier)
            ),
            **{
                k: v
                for k, v in self.base_config.__dict__.items()
                if k
                not in [
                    "requests_per_second",
                    "requests_per_minute",
                    "requests_per_hour",
                    "burst_limit",
                ]
            },
        )
        return adjusted


class UnifiedRateLimiter:
    """
    Unified rate limiter consolidating all rate limiting strategies.

    Provides enterprise-grade rate limiting with multiple strategies,
    circuit breakers, and adaptive behavior.
    """

    def __init__(self) -> None:
        """Initialize unified rate limiter."""
        self.logger = get_logger(__name__)

        # Per-source configurations and state
        self.configs: dict[str, RateLimitConfig] = {}
        self.token_buckets: dict[str, TokenBucket] = {}
        self.sliding_windows: dict[str, SlidingWindowRateLimiter] = {}
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.adaptive_limiters: dict[str, AdaptiveRateLimiter] = {}

        # Global state
        self.request_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)

        self.logger.info("UnifiedRateLimiter initialized")

    def configure_source(self, source: str, config: RateLimitConfig) -> None:
        """
        Configure rate limiting for a specific source.

        Args:
            source: Source identifier
            config: Rate limit configuration
        """
        with self._locks[source]:
            self.configs[source] = config

            # Initialize components based on strategy
            if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                self.token_buckets[source] = TokenBucket(
                    rate=config.requests_per_second, capacity=config.burst_limit
                )

            elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
                self.sliding_windows[source] = SlidingWindowRateLimiter(
                    max_requests=config.requests_per_minute, window_seconds=60
                )

            # Always initialize circuit breaker if enabled
            if config.circuit_breaker_enabled:
                self.circuit_breakers[source] = CircuitBreaker(
                    failure_threshold=config.failure_threshold,
                    recovery_timeout=config.recovery_timeout_seconds,
                )

            # Initialize adaptive limiter if enabled
            if config.adaptive_enabled:
                self.adaptive_limiters[source] = AdaptiveRateLimiter(config)

            self.logger.info(
                "Rate limiter configured for source",
                source=source,
                strategy=config.strategy.value,
            )

    async def acquire(self, source: str, tokens: int = 1) -> RateLimitResult:
        """
        Acquire permission for requests from a source.

        Args:
            source: Source identifier
            tokens: Number of tokens/requests to acquire

        Returns:
            RateLimitResult with permission and timing information
        """
        if source not in self.configs:
            # Use default configuration
            self.configure_source(source, RateLimitConfig())

        config = self.configs[source]

        # Get current configuration (may be adapted)
        if source in self.adaptive_limiters:
            config = self.adaptive_limiters[source].get_adjusted_config()

        # Check circuit breaker first
        if source in self.circuit_breakers:
            circuit_breaker = self.circuit_breakers[source]
            if not circuit_breaker.can_proceed():
                return RateLimitResult(
                    allowed=False,
                    reason="Circuit breaker open",
                    circuit_breaker_state="open",
                )

        # Apply rate limiting strategy
        allowed = False
        wait_time = 0.0
        reason = ""

        if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            if source in self.token_buckets:
                allowed, wait_time = self.token_buckets[source].acquire(tokens)
                if not allowed:
                    reason = f"Token bucket depleted, wait {wait_time:.2f}s"

        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            if source in self.sliding_windows:
                allowed, wait_time = self.sliding_windows[source].can_proceed()
                if not allowed:
                    reason = f"Rate limit exceeded, wait {wait_time:.2f}s"

        else:
            # Default: simple rate limiting
            allowed = True

        # Record request attempt
        now = time.time()
        self.request_history[source].append(
            {"timestamp": now, "allowed": allowed, "tokens": tokens}
        )

        # Apply exponential backoff if configured and rate limited
        if not allowed and config.exponential_backoff:
            recent_failures = sum(
                1
                for req in list(self.request_history[source])[-10:]
                if not req["allowed"]
            )
            backoff_multiplier = 2 ** min(recent_failures, 5)
            wait_time = min(wait_time * backoff_multiplier, config.max_delay_seconds)

            # Add jitter if enabled
            if config.jitter:
                import random

                jitter = wait_time * 0.1 * random.uniform(-1, 1)
                wait_time = max(0.0, wait_time + jitter)

        # Get current metrics
        current_rate = self._calculate_current_rate(source)
        tokens_remaining = 0
        if source in self.token_buckets:
            status = self.token_buckets[source].get_status()
            tokens_remaining = int(status["tokens"])

        circuit_state = "closed"
        if source in self.circuit_breakers:
            circuit_state = self.circuit_breakers[source].get_state()["state"]

        result = RateLimitResult(
            allowed=allowed,
            wait_time_seconds=wait_time,
            reason=reason,
            current_rate=current_rate,
            tokens_remaining=tokens_remaining,
            circuit_breaker_state=circuit_state,
        )

        # If not allowed, wait the required time
        if not allowed and wait_time > 0:
            self.logger.debug(
                "Rate limited, waiting",
                source=source,
                wait_time=wait_time,
                reason=reason,
            )
            await asyncio.sleep(wait_time)

        return result

    def record_request_result(self, source: str, success: bool) -> None:
        """
        Record the result of a request for adaptive rate limiting.

        Args:
            source: Source identifier
            success: Whether the request was successful
        """
        # Update circuit breaker
        if source in self.circuit_breakers:
            if success:
                self.circuit_breakers[source].record_success()
            else:
                self.circuit_breakers[source].record_failure()

        # Update adaptive limiter
        if source in self.adaptive_limiters:
            self.adaptive_limiters[source].record_request_result(success)

    def _calculate_current_rate(self, source: str) -> float:
        """Calculate current request rate for a source."""
        if source not in self.request_history:
            return 0.0

        now = time.time()
        recent_requests = [
            req
            for req in self.request_history[source]
            if now - req["timestamp"] <= 60  # Last minute
        ]

        return len(recent_requests) / 60.0

    def get_source_metrics(self, source: str) -> dict[str, Any]:
        """Get comprehensive metrics for a source."""
        metrics = {
            "source": source,
            "current_rate": self._calculate_current_rate(source),
            "total_requests": len(self.request_history[source]),
        }

        # Token bucket metrics
        if source in self.token_buckets:
            metrics["token_bucket"] = self.token_buckets[source].get_status()

        # Circuit breaker metrics
        if source in self.circuit_breakers:
            metrics["circuit_breaker"] = self.circuit_breakers[source].get_state()

        # Adaptive limiter metrics
        if source in self.adaptive_limiters:
            adaptive = self.adaptive_limiters[source]
            metrics["adaptive"] = {
                "current_multiplier": adaptive.current_multiplier,
                "success_rate": (
                    sum(adaptive.success_history) / len(adaptive.success_history)
                    if adaptive.success_history
                    else 1.0
                ),
            }

        return metrics

    def get_global_metrics(self) -> dict[str, Any]:
        """Get global rate limiting metrics."""
        total_requests = sum(len(history) for history in self.request_history.values())

        return {
            "total_sources": len(self.configs),
            "total_requests": total_requests,
            "sources": list(self.configs.keys()),
            "active_circuit_breakers": len(
                [
                    source
                    for source, cb in self.circuit_breakers.items()
                    if cb.get_state()["state"] != "closed"
                ]
            ),
        }


# Global rate limiter instance
_rate_limiter: UnifiedRateLimiter | None = None
_rate_limiter_lock = threading.Lock()


def get_rate_limiter() -> UnifiedRateLimiter:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        with _rate_limiter_lock:
            if _rate_limiter is None:
                _rate_limiter = UnifiedRateLimiter()
    return _rate_limiter


__all__ = [
    "UnifiedRateLimiter",
    "RateLimitConfig",
    "RateLimitResult",
    "RateLimitStrategy",
    "TokenBucket",
    "CircuitBreaker",
    "SlidingWindowRateLimiter",
    "AdaptiveRateLimiter",
    "get_rate_limiter",
]
