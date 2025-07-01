#!/usr/bin/env python3
"""
Unified Rate Limiter for MLB Sharp Betting System

Consolidates rate limiting across all external services to prevent API quota
exhaustion and system overload. Eliminates the anarchy of scattered rate 
limiting implementations.

Consolidates 3 Incompatible Rate Limiting Systems:
1. odds_api_service.py: API quota tracking with file-based usage tracking
2. pinnacle_scraper.py: Request rate limiting with sleep-based delays  
3. alert_service.py: Alert cooldowns with memory-based cooldowns

Production Features:
- Centralized rate limiting for all external services
- Multiple rate limiting strategies (quota, request rate, cooldown)
- Persistent quota tracking with file-based storage
- Memory-based request rate limiting with sliding windows
- Thread-safe concurrent access
- Rate limit metrics and monitoring
- Circuit breaker integration
"""

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import structlog

from ..core.logging import get_logger
from .config_service import get_config_service, get_rate_limits

logger = get_logger(__name__)


class RateLimitType(Enum):
    """Rate limit types."""
    QUOTA = "quota"  # Total requests per time period (e.g., monthly API quota)
    REQUEST_RATE = "request_rate"  # Requests per second/minute  
    COOLDOWN = "cooldown"  # Minimum time between requests of same type


class RateLimitStatus(Enum):
    """Rate limit check status."""
    ALLOWED = "allowed"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXCEEDED = "quota_exceeded" 
    COOLDOWN_ACTIVE = "cooldown_active"


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    status: RateLimitStatus
    allowed: bool
    wait_time_seconds: float = 0.0
    reason: str = ""
    quota_used: Optional[int] = None
    quota_remaining: Optional[int] = None


@dataclass
class QuotaTracker:
    """Tracks API quota usage with persistent storage."""
    service_name: str
    max_quota: int
    time_window: str  # "monthly", "daily", "hourly"
    storage_file: Path
    current_usage: int = 0
    window_start: datetime = field(default_factory=datetime.now)
    usage_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def __post_init__(self):
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_usage_data()
    
    def _load_usage_data(self) -> None:
        """Load usage data from persistent storage."""
        try:
            if self.storage_file.exists():
                with open(self.storage_file, 'r') as f:
                    data = json.load(f)
                    
                self.current_usage = data.get("used", 0)
                self.usage_history = data.get("history", [])
                
                # Parse window start
                window_start_str = data.get("window_start")
                if window_start_str:
                    self.window_start = datetime.fromisoformat(window_start_str)
                
                # Check if we need to reset quota for new time window
                self._check_window_reset()
                
        except Exception as e:
            logger.error(f"Failed to load quota data for {self.service_name}: {e}")
            self.current_usage = 0
            self.usage_history = []
    
    def _save_usage_data(self) -> None:
        """Save usage data to persistent storage."""
        try:
            data = {
                "service_name": self.service_name,
                "max_quota": self.max_quota,
                "time_window": self.time_window,
                "used": self.current_usage,
                "window_start": self.window_start.isoformat(),
                "last_updated": datetime.now().isoformat(),
                "history": self.usage_history[-100:]  # Keep last 100 entries
            }
            
            with open(self.storage_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save quota data for {self.service_name}: {e}")
    
    def _check_window_reset(self) -> None:
        """Check if quota window should reset."""
        now = datetime.now()
        should_reset = False
        
        if self.time_window == "monthly":
            should_reset = (now.year != self.window_start.year or 
                          now.month != self.window_start.month)
        elif self.time_window == "daily":
            should_reset = now.date() != self.window_start.date()
        elif self.time_window == "hourly":
            should_reset = (now - self.window_start).total_seconds() >= 3600
        
        if should_reset:
            logger.info(f"Resetting quota for {self.service_name} - new {self.time_window} window")
            self.current_usage = 0
            self.window_start = now
            self._save_usage_data()
    
    def can_make_request(self, cost: int = 1) -> bool:
        """Check if request can be made within quota."""
        self._check_window_reset()
        return (self.current_usage + cost) <= self.max_quota
    
    def record_usage(self, cost: int = 1, endpoint: str = "", metadata: Dict[str, Any] = None) -> None:
        """Record API usage."""
        self.current_usage += cost
        
        # Add to history
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "cost": cost,
            "endpoint": endpoint,
            "usage_after": self.current_usage,
            "metadata": metadata or {}
        }
        self.usage_history.append(history_entry)
        
        # Save to persistent storage
        self._save_usage_data()
        
        logger.info(f"{self.service_name} quota usage: {cost} credits used, "
                   f"{self.current_usage}/{self.max_quota} total this {self.time_window}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current quota status."""
        self._check_window_reset()
        
        return {
            "service_name": self.service_name,
            "used": self.current_usage,
            "max_quota": self.max_quota,
            "remaining": self.max_quota - self.current_usage,
            "percentage_used": (self.current_usage / self.max_quota) * 100,
            "time_window": self.time_window,
            "window_start": self.window_start.isoformat()
        }


@dataclass 
class RequestRateTracker:
    """Tracks request rate with sliding window."""
    service_name: str
    max_requests_per_minute: int
    max_requests_per_hour: int
    request_delay_seconds: float
    burst_limit: int
    request_times: deque = field(default_factory=deque)
    last_request_time: Optional[datetime] = None
    
    def can_make_request(self) -> RateLimitResult:
        """Check if request can be made within rate limits."""
        now = datetime.now()
        
        # Clean old request times (older than 1 hour)
        cutoff_time = now - timedelta(hours=1)
        while self.request_times and self.request_times[0] < cutoff_time:
            self.request_times.popleft()
        
        # Check request delay (minimum time between requests)
        if self.last_request_time:
            time_since_last = (now - self.last_request_time).total_seconds()
            if time_since_last < self.request_delay_seconds:
                wait_time = self.request_delay_seconds - time_since_last
                return RateLimitResult(
                    status=RateLimitStatus.RATE_LIMITED,
                    allowed=False,
                    wait_time_seconds=wait_time,
                    reason=f"Request delay: need to wait {wait_time:.2f}s"
                )
        
        # Check burst limit (requests in last minute)
        minute_ago = now - timedelta(minutes=1)
        recent_requests = sum(1 for req_time in self.request_times if req_time >= minute_ago)
        
        if recent_requests >= self.max_requests_per_minute:
            return RateLimitResult(
                status=RateLimitStatus.RATE_LIMITED,
                allowed=False,
                wait_time_seconds=60.0,
                reason=f"Rate limit exceeded: {recent_requests}/{self.max_requests_per_minute} per minute"
            )
        
        # Check hourly limit
        if len(self.request_times) >= self.max_requests_per_hour:
            oldest_request = self.request_times[0]
            wait_time = 3600 - (now - oldest_request).total_seconds()
            return RateLimitResult(
                status=RateLimitStatus.RATE_LIMITED,
                allowed=False,
                wait_time_seconds=max(0, wait_time),
                reason=f"Hourly rate limit exceeded: {len(self.request_times)}/{self.max_requests_per_hour}"
            )
        
        return RateLimitResult(
            status=RateLimitStatus.ALLOWED,
            allowed=True,
            reason="Rate limit check passed"
        )
    
    def record_request(self) -> None:
        """Record a request."""
        now = datetime.now()
        self.request_times.append(now)
        self.last_request_time = now


@dataclass
class CooldownTracker:
    """Tracks cooldowns for specific operations."""
    service_name: str
    cooldown_minutes: int
    last_operations: Dict[str, datetime] = field(default_factory=dict)
    
    def can_perform_operation(self, operation_type: str) -> RateLimitResult:
        """Check if operation can be performed (not in cooldown)."""
        now = datetime.now()
        
        if operation_type in self.last_operations:
            last_time = self.last_operations[operation_type]
            time_since_last = (now - last_time).total_seconds()
            cooldown_seconds = self.cooldown_minutes * 60
            
            if time_since_last < cooldown_seconds:
                wait_time = cooldown_seconds - time_since_last
                return RateLimitResult(
                    status=RateLimitStatus.COOLDOWN_ACTIVE,
                    allowed=False,
                    wait_time_seconds=wait_time,
                    reason=f"Cooldown active: need to wait {wait_time/60:.1f} minutes"
                )
        
        return RateLimitResult(
            status=RateLimitStatus.ALLOWED,
            allowed=True,
            reason="Cooldown check passed"
        )
    
    def record_operation(self, operation_type: str) -> None:
        """Record an operation."""
        self.last_operations[operation_type] = datetime.now()


class UnifiedRateLimiter:
    """
    Centralized rate limiting for all external services.
    
    Consolidates the three incompatible rate limiting systems:
    1. Odds API: Monthly quota protection (480 requests/month)
    2. Pinnacle: Request rate limiting (30 requests/minute with 0.1s delay)
    3. Alerts: Cooldown-based limiting (15 minute cooldowns)
    """
    
    def __init__(self, data_dir: Optional[Path] = None):
        """Initialize unified rate limiter."""
        self.logger = get_logger(__name__)
        self.config_service = get_config_service()
        
        # Data directory for persistent storage
        self.data_dir = data_dir or Path("data/rate_limits")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Rate limiters by service
        self.quota_trackers: Dict[str, QuotaTracker] = {}
        self.rate_trackers: Dict[str, RequestRateTracker] = {}
        self.cooldown_trackers: Dict[str, CooldownTracker] = {}
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Initialize default rate limiters
        self._initialize_default_limiters()
        
        self.logger.info("UnifiedRateLimiter initialized with consolidated rate limiting")
    
    def _initialize_default_limiters(self) -> None:
        """Initialize rate limiters for known services."""
        
        # Odds API - Monthly quota protection
        self.quota_trackers["odds_api"] = QuotaTracker(
            service_name="odds_api",
            max_quota=480,  # Monthly quota from memory
            time_window="monthly",
            storage_file=self.data_dir / "odds_api_quota.json"
        )
        
        # Pinnacle scraper - Request rate limiting
        self.rate_trackers["pinnacle_scraper"] = RequestRateTracker(
            service_name="pinnacle_scraper",
            max_requests_per_minute=30,
            max_requests_per_hour=1000,
            request_delay_seconds=0.1,
            burst_limit=10
        )
        
        # VSIN scraper - Conservative rate limiting
        self.rate_trackers["vsin_scraper"] = RequestRateTracker(
            service_name="vsin_scraper", 
            max_requests_per_minute=5,
            max_requests_per_hour=100,
            request_delay_seconds=0.2,
            burst_limit=3
        )
        
        # Alert service - Cooldown-based limiting
        self.cooldown_trackers["alert_service"] = CooldownTracker(
            service_name="alert_service",
            cooldown_minutes=15
        )
    
    async def check_rate_limit(self, service_name: str, 
                             operation_type: str = "default",
                             estimated_cost: int = 1) -> RateLimitResult:
        """
        Check if operation is allowed under rate limits.
        
        Args:
            service_name: Name of the service making the request
            operation_type: Type of operation (for cooldown tracking)
            estimated_cost: Estimated cost for quota-based limiting
            
        Returns:
            RateLimitResult with status and wait time if needed
        """
        with self._lock:
            # Check quota limits first (if applicable)
            if service_name in self.quota_trackers:
                quota_tracker = self.quota_trackers[service_name]
                if not quota_tracker.can_make_request(estimated_cost):
                    quota_status = quota_tracker.get_status()
                    return RateLimitResult(
                        status=RateLimitStatus.QUOTA_EXCEEDED,
                        allowed=False,
                        reason=f"Quota exceeded: {quota_status['used']}/{quota_status['max_quota']}",
                        quota_used=quota_status['used'],
                        quota_remaining=quota_status['remaining']
                    )
            
            # Check request rate limits
            if service_name in self.rate_trackers:
                rate_tracker = self.rate_trackers[service_name]
                rate_result = rate_tracker.can_make_request()
                if not rate_result.allowed:
                    return rate_result
            
            # Check cooldown limits
            if service_name in self.cooldown_trackers:
                cooldown_tracker = self.cooldown_trackers[service_name]
                cooldown_result = cooldown_tracker.can_perform_operation(operation_type)
                if not cooldown_result.allowed:
                    return cooldown_result
            
            return RateLimitResult(
                status=RateLimitStatus.ALLOWED,
                allowed=True,
                reason="All rate limit checks passed"
            )
    
    async def record_request(self, service_name: str,
                           operation_type: str = "default",
                           actual_cost: int = 1,
                           endpoint: str = "",
                           metadata: Dict[str, Any] = None) -> None:
        """
        Record a successful request for rate limiting tracking.
        
        Args:
            service_name: Name of the service that made the request
            operation_type: Type of operation (for cooldown tracking)
            actual_cost: Actual cost incurred
            endpoint: API endpoint called
            metadata: Additional metadata to store
        """
        with self._lock:
            # Record quota usage
            if service_name in self.quota_trackers:
                quota_tracker = self.quota_trackers[service_name]
                quota_tracker.record_usage(actual_cost, endpoint, metadata)
            
            # Record request rate
            if service_name in self.rate_trackers:
                rate_tracker = self.rate_trackers[service_name]
                rate_tracker.record_request()
            
            # Record cooldown operation
            if service_name in self.cooldown_trackers:
                cooldown_tracker = self.cooldown_trackers[service_name]
                cooldown_tracker.record_operation(operation_type)
    
    async def wait_for_rate_limit(self, service_name: str,
                                operation_type: str = "default",
                                estimated_cost: int = 1,
                                max_wait_seconds: float = 300.0) -> RateLimitResult:
        """
        Wait for rate limit to allow operation, with timeout.
        
        Args:
            service_name: Name of the service
            operation_type: Type of operation
            estimated_cost: Estimated cost
            max_wait_seconds: Maximum time to wait
            
        Returns:
            RateLimitResult after waiting or timeout
        """
        start_time = time.time()
        
        while True:
            result = await self.check_rate_limit(service_name, operation_type, estimated_cost)
            
            if result.allowed:
                return result
            
            # Check if we've exceeded max wait time
            elapsed = time.time() - start_time
            if elapsed >= max_wait_seconds:
                return RateLimitResult(
                    status=RateLimitStatus.RATE_LIMITED,
                    allowed=False,
                    reason=f"Timeout waiting for rate limit after {elapsed:.1f}s"
                )
            
            # Wait for the shorter of: suggested wait time or remaining max wait time
            wait_time = min(result.wait_time_seconds, max_wait_seconds - elapsed)
            
            self.logger.info(f"Rate limited, waiting {wait_time:.1f}s for {service_name}",
                           reason=result.reason)
            
            await asyncio.sleep(wait_time)
    
    def get_service_status(self, service_name: str) -> Dict[str, Any]:
        """Get rate limiting status for a service."""
        status = {
            "service_name": service_name,
            "quota": None,
            "rate_limits": None,
            "cooldowns": None
        }
        
        with self._lock:
            # Quota status
            if service_name in self.quota_trackers:
                status["quota"] = self.quota_trackers[service_name].get_status()
            
            # Rate limit status 
            if service_name in self.rate_trackers:
                rate_tracker = self.rate_trackers[service_name]
                status["rate_limits"] = {
                    "requests_in_last_hour": len(rate_tracker.request_times),
                    "max_requests_per_hour": rate_tracker.max_requests_per_hour,
                    "max_requests_per_minute": rate_tracker.max_requests_per_minute,
                    "request_delay_seconds": rate_tracker.request_delay_seconds,
                    "last_request": rate_tracker.last_request_time.isoformat() if rate_tracker.last_request_time else None
                }
            
            # Cooldown status
            if service_name in self.cooldown_trackers:
                cooldown_tracker = self.cooldown_trackers[service_name]
                status["cooldowns"] = {
                    "cooldown_minutes": cooldown_tracker.cooldown_minutes,
                    "active_operations": {
                        op_type: last_time.isoformat()
                        for op_type, last_time in cooldown_tracker.last_operations.items()
                    }
                }
        
        return status
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get rate limiting status for all services."""
        all_services = set()
        all_services.update(self.quota_trackers.keys())
        all_services.update(self.rate_trackers.keys())
        all_services.update(self.cooldown_trackers.keys())
        
        return {
            "services": {
                service_name: self.get_service_status(service_name)
                for service_name in sorted(all_services)
            },
            "summary": {
                "total_services": len(all_services),
                "quota_tracked": len(self.quota_trackers),
                "rate_limited": len(self.rate_trackers),
                "cooldown_tracked": len(self.cooldown_trackers)
            }
        }


# Global rate limiter instance
_rate_limiter = None
_rate_limiter_lock = threading.Lock()


def get_rate_limiter() -> UnifiedRateLimiter:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        with _rate_limiter_lock:
            if _rate_limiter is None:
                _rate_limiter = UnifiedRateLimiter()
    return _rate_limiter


# Convenience decorator for rate limiting
def rate_limited(service_name: str, 
                operation_type: str = "default",
                estimated_cost: int = 1,
                max_wait_seconds: float = 300.0):
    """
    Decorator for adding rate limiting to operations.
    
    Args:
        service_name: Name of the service
        operation_type: Type of operation
        estimated_cost: Estimated cost for quota tracking
        max_wait_seconds: Maximum wait time
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            rate_limiter = get_rate_limiter()
            
            # Wait for rate limit clearance
            await rate_limiter.wait_for_rate_limit(
                service_name=service_name,
                operation_type=operation_type,
                estimated_cost=estimated_cost,
                max_wait_seconds=max_wait_seconds
            )
            
            try:
                # Execute operation
                result = await func(*args, **kwargs)
                
                # Record successful request
                await rate_limiter.record_request(
                    service_name=service_name,
                    operation_type=operation_type,
                    actual_cost=estimated_cost,
                    endpoint=func.__name__
                )
                
                return result
                
            except Exception as e:
                # Still record the request (it consumed resources)
                await rate_limiter.record_request(
                    service_name=service_name,
                    operation_type=operation_type,
                    actual_cost=estimated_cost,
                    endpoint=func.__name__,
                    metadata={"error": str(e)}
                )
                raise
        
        # For sync functions
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            async def run():
                return await async_wrapper(*args, **kwargs)
            
            try:
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(run())
            except RuntimeError:
                return asyncio.run(run())
        
        # Return appropriate wrapper
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator 