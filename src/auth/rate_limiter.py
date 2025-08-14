"""
Rate Limiting Implementation

Comprehensive rate limiting system for authentication endpoints and general API access.
Supports sliding window, fixed window, and token bucket algorithms with Redis backend.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum
import json
from urllib.parse import quote

from ..core.config import get_settings
from ..core.logging import get_logger, LogComponent
from ..data.database.connection import get_connection
from .exceptions import RateLimitExceededError

logger = get_logger(__name__, LogComponent.AUTH)


class RateLimitAlgorithm(str, Enum):
    """Rate limiting algorithms."""
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    TOKEN_BUCKET = "token_bucket"


class RateLimitScope(str, Enum):
    """Rate limiting scopes."""
    IP_ADDRESS = "ip"
    USER = "user"
    API_KEY = "api_key"
    ENDPOINT = "endpoint"
    GLOBAL = "global"


@dataclass
class RateLimitRule:
    """Rate limiting rule configuration."""
    scope: RateLimitScope
    algorithm: RateLimitAlgorithm
    max_requests: int
    window_seconds: int
    burst_allowance: int = 0
    penalty_seconds: int = 0
    enabled: bool = True


@dataclass
class RateLimitStatus:
    """Rate limit status for a request."""
    allowed: bool
    remaining: int
    reset_time: datetime
    retry_after_seconds: Optional[int] = None
    limit: int = 0
    used: int = 0


class DatabaseRateLimiter:
    """
    Database-backed rate limiter implementation.
    
    Uses PostgreSQL for persistence with optional Redis acceleration.
    Implements multiple algorithms for different use cases.
    """
    
    def __init__(self):
        """Initialize rate limiter."""
        self.settings = get_settings()
        
        # Default rate limit rules
        self.rules: Dict[str, RateLimitRule] = {
            "login": RateLimitRule(
                scope=RateLimitScope.IP_ADDRESS,
                algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
                max_requests=5,
                window_seconds=300,  # 5 minutes
                penalty_seconds=900  # 15 minutes lockout
            ),
            "password_reset": RateLimitRule(
                scope=RateLimitScope.IP_ADDRESS,
                algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
                max_requests=3,
                window_seconds=3600,  # 1 hour
                penalty_seconds=3600  # 1 hour lockout
            ),
            "api_general": RateLimitRule(
                scope=RateLimitScope.USER,
                algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
                max_requests=1000,
                window_seconds=3600,  # 1 hour
                burst_allowance=100
            ),
            "api_key": RateLimitRule(
                scope=RateLimitScope.API_KEY,
                algorithm=RateLimitAlgorithm.FIXED_WINDOW,
                max_requests=10000,
                window_seconds=3600  # 1 hour
            ),
            "mfa_verification": RateLimitRule(
                scope=RateLimitScope.IP_ADDRESS,
                algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
                max_requests=10,
                window_seconds=600,  # 10 minutes
                penalty_seconds=1800  # 30 minutes lockout
            )
        }
    
    async def check_rate_limit(
        self,
        rule_name: str,
        identifier: str,
        increment: bool = True,
        request_weight: int = 1
    ) -> RateLimitStatus:
        """
        Check rate limit for a request.
        
        Args:
            rule_name: Name of the rate limit rule
            identifier: Request identifier (IP, user ID, API key, etc.)
            increment: Whether to increment the counter
            request_weight: Weight of the request (for weighted rate limiting)
            
        Returns:
            RateLimitStatus with current limit state
            
        Raises:
            RateLimitExceededError: If rate limit is exceeded
        """
        if rule_name not in self.rules:
            logger.warning(f"Unknown rate limit rule: {rule_name}")
            return RateLimitStatus(
                allowed=True,
                remaining=999,
                reset_time=datetime.now(timezone.utc) + timedelta(hours=1),
                limit=1000,
                used=0
            )
        
        rule = self.rules[rule_name]
        
        if not rule.enabled:
            return RateLimitStatus(
                allowed=True,
                remaining=rule.max_requests,
                reset_time=datetime.now(timezone.utc) + timedelta(seconds=rule.window_seconds),
                limit=rule.max_requests,
                used=0
            )
        
        try:
            if rule.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                return await self._check_sliding_window(rule, rule_name, identifier, increment, request_weight)
            elif rule.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
                return await self._check_fixed_window(rule, rule_name, identifier, increment, request_weight)
            elif rule.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                return await self._check_token_bucket(rule, rule_name, identifier, increment, request_weight)
            else:
                raise ValueError(f"Unknown rate limit algorithm: {rule.algorithm}")
                
        except Exception as e:
            logger.error(f"Rate limit check failed for rule {rule_name}", error=e)
            # Fail open - allow request if rate limiting fails
            return RateLimitStatus(
                allowed=True,
                remaining=rule.max_requests,
                reset_time=datetime.now(timezone.utc) + timedelta(seconds=rule.window_seconds),
                limit=rule.max_requests,
                used=0
            )
    
    async def _check_sliding_window(
        self,
        rule: RateLimitRule,
        rule_name: str,
        identifier: str,
        increment: bool,
        request_weight: int
    ) -> RateLimitStatus:
        """Check sliding window rate limit."""
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(seconds=rule.window_seconds)
        cache_key = f"rate_limit:{rule_name}:{quote(identifier)}"
        
        async with get_connection() as conn:
            # Check for penalty period (if applicable)
            if rule.penalty_seconds > 0:
                penalty_check = await conn.fetchrow(
                    """
                    SELECT penalty_until 
                    FROM auth.rate_limit_penalties 
                    WHERE cache_key = $1 AND penalty_until > $2
                    """,
                    cache_key, now
                )
                
                if penalty_check:
                    return RateLimitStatus(
                        allowed=False,
                        remaining=0,
                        reset_time=penalty_check['penalty_until'],
                        retry_after_seconds=int((penalty_check['penalty_until'] - now).total_seconds()),
                        limit=rule.max_requests,
                        used=rule.max_requests
                    )
            
            # Count requests in sliding window
            request_count = await conn.fetchval(
                """
                SELECT COALESCE(SUM(weight), 0)
                FROM auth.rate_limit_requests 
                WHERE cache_key = $1 AND timestamp > $2
                """,
                cache_key, window_start
            ) or 0
            
            # Check if limit would be exceeded
            new_count = request_count + (request_weight if increment else 0)
            allowed = new_count <= rule.max_requests
            
            if increment and allowed:
                # Record this request
                await conn.execute(
                    """
                    INSERT INTO auth.rate_limit_requests (cache_key, timestamp, weight)
                    VALUES ($1, $2, $3)
                    """,
                    cache_key, now, request_weight
                )
                
                # Clean up old requests
                await conn.execute(
                    """
                    DELETE FROM auth.rate_limit_requests 
                    WHERE cache_key = $1 AND timestamp <= $2
                    """,
                    cache_key, window_start
                )
            
            elif increment and not allowed and rule.penalty_seconds > 0:
                # Apply penalty
                penalty_until = now + timedelta(seconds=rule.penalty_seconds)
                await conn.execute(
                    """
                    INSERT INTO auth.rate_limit_penalties (cache_key, penalty_until)
                    VALUES ($1, $2)
                    ON CONFLICT (cache_key) 
                    DO UPDATE SET penalty_until = $2
                    """,
                    cache_key, penalty_until
                )
            
            return RateLimitStatus(
                allowed=allowed,
                remaining=max(0, rule.max_requests - int(new_count)),
                reset_time=now + timedelta(seconds=rule.window_seconds),
                retry_after_seconds=rule.penalty_seconds if not allowed and rule.penalty_seconds > 0 else None,
                limit=rule.max_requests,
                used=int(new_count)
            )
    
    async def _check_fixed_window(
        self,
        rule: RateLimitRule,
        rule_name: str,
        identifier: str,
        increment: bool,
        request_weight: int
    ) -> RateLimitStatus:
        """Check fixed window rate limit."""
        now = datetime.now(timezone.utc)
        window_start = datetime(
            now.year, now.month, now.day, now.hour,
            (now.minute // (rule.window_seconds // 60)) * (rule.window_seconds // 60),
            tzinfo=timezone.utc
        )
        window_end = window_start + timedelta(seconds=rule.window_seconds)
        cache_key = f"rate_limit:{rule_name}:{quote(identifier)}:{int(window_start.timestamp())}"
        
        async with get_connection() as conn:
            # Get current count for this window
            current_count = await conn.fetchval(
                """
                SELECT COALESCE(request_count, 0)
                FROM auth.rate_limit_windows 
                WHERE cache_key = $1
                """,
                cache_key
            ) or 0
            
            # Check if limit would be exceeded
            new_count = current_count + (request_weight if increment else 0)
            allowed = new_count <= rule.max_requests
            
            if increment and allowed:
                # Update counter
                await conn.execute(
                    """
                    INSERT INTO auth.rate_limit_windows (cache_key, request_count, window_start, window_end)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (cache_key) 
                    DO UPDATE SET request_count = auth.rate_limit_windows.request_count + $2
                    """,
                    cache_key, request_weight, window_start, window_end
                )
            
            return RateLimitStatus(
                allowed=allowed,
                remaining=max(0, rule.max_requests - new_count),
                reset_time=window_end,
                limit=rule.max_requests,
                used=new_count
            )
    
    async def _check_token_bucket(
        self,
        rule: RateLimitRule,
        rule_name: str,
        identifier: str,
        increment: bool,
        request_weight: int
    ) -> RateLimitStatus:
        """Check token bucket rate limit."""
        now = datetime.now(timezone.utc)
        cache_key = f"rate_limit:{rule_name}:{quote(identifier)}"
        
        async with get_connection() as conn:
            # Get current bucket state
            bucket = await conn.fetchrow(
                """
                SELECT tokens, last_refill
                FROM auth.rate_limit_buckets 
                WHERE cache_key = $1
                """,
                cache_key
            )
            
            if not bucket:
                # Initialize bucket
                initial_tokens = rule.max_requests
                await conn.execute(
                    """
                    INSERT INTO auth.rate_limit_buckets (cache_key, tokens, last_refill)
                    VALUES ($1, $2, $3)
                    """,
                    cache_key, initial_tokens, now
                )
                tokens = initial_tokens
                last_refill = now
            else:
                tokens = bucket['tokens']
                last_refill = bucket['last_refill']
            
            # Calculate token refill
            seconds_passed = (now - last_refill).total_seconds()
            refill_rate = rule.max_requests / rule.window_seconds  # tokens per second
            tokens_to_add = int(seconds_passed * refill_rate)
            
            if tokens_to_add > 0:
                tokens = min(rule.max_requests + rule.burst_allowance, tokens + tokens_to_add)
                last_refill = now
            
            # Check if request can be fulfilled
            allowed = tokens >= request_weight
            
            if increment and allowed:
                tokens -= request_weight
                
                # Update bucket
                await conn.execute(
                    """
                    UPDATE auth.rate_limit_buckets 
                    SET tokens = $1, last_refill = $2
                    WHERE cache_key = $3
                    """,
                    tokens, last_refill, cache_key
                )
            
            # Calculate next refill time
            next_refill = last_refill + timedelta(seconds=1/refill_rate) if tokens < rule.max_requests else now
            
            return RateLimitStatus(
                allowed=allowed,
                remaining=tokens,
                reset_time=next_refill,
                limit=rule.max_requests + rule.burst_allowance,
                used=(rule.max_requests + rule.burst_allowance) - tokens
            )
    
    async def reset_rate_limit(self, rule_name: str, identifier: str) -> None:
        """Reset rate limit for specific identifier."""
        cache_key = f"rate_limit:{rule_name}:{quote(identifier)}"
        
        async with get_connection() as conn:
            await conn.execute(
                "DELETE FROM auth.rate_limit_requests WHERE cache_key LIKE $1",
                f"{cache_key}%"
            )
            await conn.execute(
                "DELETE FROM auth.rate_limit_windows WHERE cache_key LIKE $1",
                f"{cache_key}%"
            )
            await conn.execute(
                "DELETE FROM auth.rate_limit_buckets WHERE cache_key = $1",
                cache_key
            )
            await conn.execute(
                "DELETE FROM auth.rate_limit_penalties WHERE cache_key = $1",
                cache_key
            )
        
        logger.info(f"Reset rate limit for {rule_name}:{identifier}")
    
    async def get_rate_limit_info(self, rule_name: str, identifier: str) -> Dict[str, Any]:
        """Get detailed rate limit information."""
        return {
            "rule": self.rules.get(rule_name),
            "status": await self.check_rate_limit(rule_name, identifier, increment=False)
        }
    
    async def cleanup_expired_data(self) -> None:
        """Clean up expired rate limit data."""
        now = datetime.now(timezone.utc)
        
        async with get_connection() as conn:
            # Clean up old requests (older than longest window)
            max_window = max(rule.window_seconds for rule in self.rules.values())
            cutoff = now - timedelta(seconds=max_window * 2)
            
            deleted_requests = await conn.execute(
                "DELETE FROM auth.rate_limit_requests WHERE timestamp < $1",
                cutoff
            )
            
            # Clean up expired windows
            deleted_windows = await conn.execute(
                "DELETE FROM auth.rate_limit_windows WHERE window_end < $1",
                now
            )
            
            # Clean up expired penalties
            deleted_penalties = await conn.execute(
                "DELETE FROM auth.rate_limit_penalties WHERE penalty_until < $1",
                now
            )
            
            # Clean up stale buckets (not updated in 24 hours)
            stale_cutoff = now - timedelta(hours=24)
            deleted_buckets = await conn.execute(
                "DELETE FROM auth.rate_limit_buckets WHERE last_refill < $1",
                stale_cutoff
            )
            
            logger.info(
                "Rate limit cleanup completed",
                extra={
                    "deleted_requests": deleted_requests,
                    "deleted_windows": deleted_windows,
                    "deleted_penalties": deleted_penalties,
                    "deleted_buckets": deleted_buckets
                }
            )


# Global rate limiter instance
_rate_limiter = DatabaseRateLimiter()


def get_rate_limiter() -> DatabaseRateLimiter:
    """Get the global rate limiter instance."""
    return _rate_limiter


async def check_rate_limit(
    rule_name: str,
    identifier: str,
    increment: bool = True,
    request_weight: int = 1
) -> RateLimitStatus:
    """Convenience function for rate limit checking."""
    return await _rate_limiter.check_rate_limit(rule_name, identifier, increment, request_weight)


async def require_rate_limit(
    rule_name: str,
    identifier: str,
    request_weight: int = 1
) -> RateLimitStatus:
    """
    Check rate limit and raise exception if exceeded.
    
    Args:
        rule_name: Name of the rate limit rule
        identifier: Request identifier
        request_weight: Weight of the request
        
    Returns:
        RateLimitStatus if allowed
        
    Raises:
        RateLimitExceededError: If rate limit is exceeded
    """
    status = await check_rate_limit(rule_name, identifier, increment=True, request_weight=request_weight)
    
    if not status.allowed:
        raise RateLimitExceededError(
            f"Rate limit exceeded for {rule_name}",
            retry_after_seconds=status.retry_after_seconds,
            limit=status.limit,
            remaining=status.remaining,
            reset_time=status.reset_time
        )
    
    return status