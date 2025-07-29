"""
Security utilities for API endpoints and authentication.

Provides:
- API key authentication for break-glass endpoints
- Rate limiting for sensitive operations
- Security headers and CORS configuration
"""

import time
from collections import defaultdict
from typing import Dict, Optional, Set
import ipaddress

from fastapi import HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .logging import get_logger, LogComponent

logger = get_logger(__name__, LogComponent.SECURITY)
security_scheme = HTTPBearer(auto_error=False)


class RateLimiter:
    """Enhanced rate limiter with Redis support for production deployments."""
    
    def __init__(self, use_redis: bool = False, redis_client=None):
        self._use_redis = use_redis
        self._redis_client = redis_client
        self._requests: Dict[str, list] = defaultdict(list)
        self._window_size = 3600  # 1 hour in seconds
        
        if use_redis and redis_client is None:
            logger.warning("Redis requested but no client provided, falling back to in-memory")
            self._use_redis = False
    
    def is_allowed(self, key: str, limit: int) -> bool:
        """Check if request is within rate limit."""
        if self._use_redis:
            return self._is_allowed_redis(key, limit)
        else:
            return self._is_allowed_memory(key, limit)
    
    def _is_allowed_memory(self, key: str, limit: int) -> bool:
        """In-memory rate limiting (development/single instance)."""
        now = time.time()
        window_start = now - self._window_size
        
        # Clean old requests
        self._requests[key] = [
            req_time for req_time in self._requests[key] 
            if req_time > window_start
        ]
        
        # Check if under limit
        if len(self._requests[key]) >= limit:
            logger.warning(
                "Rate limit exceeded (memory)",
                extra={
                    "rate_limit_key": key,
                    "current_count": len(self._requests[key]),
                    "limit": limit,
                    "window_seconds": self._window_size
                }
            )
            return False
        
        # Record this request
        self._requests[key].append(now)
        return True
    
    def _is_allowed_redis(self, key: str, limit: int) -> bool:
        """Redis-based rate limiting (production/multi-instance)."""
        try:
            # Use Redis sliding window counter
            now = int(time.time())
            pipeline = self._redis_client.pipeline()
            
            # Add current request
            pipeline.zadd(key, {now: now})
            
            # Remove old entries
            window_start = now - self._window_size
            pipeline.zremrangebyscore(key, 0, window_start)
            
            # Count current requests
            pipeline.zcard(key)
            
            # Set expiration
            pipeline.expire(key, self._window_size)
            
            results = pipeline.execute()
            current_count = results[2]  # Result from zcard
            
            if current_count > limit:
                logger.warning(
                    "Rate limit exceeded (Redis)",
                    extra={
                        "rate_limit_key": key,
                        "current_count": current_count,
                        "limit": limit,
                        "window_seconds": self._window_size
                    }
                )
                return False
            
            return True
            
        except Exception as e:
            logger.error(
                "Redis rate limiter error, falling back to allow",
                error=e,
                extra={"rate_limit_key": key}
            )
            # Fail open for availability
            return True


# Global rate limiter instance - will be configured on first use
rate_limiter = None


def get_rate_limiter():
    """Get configured rate limiter instance."""
    global rate_limiter
    if rate_limiter is None:
        from .config import get_settings
        settings = get_settings()
        
        # Initialize Redis client if configured
        redis_client = None
        if settings.security.enable_redis_rate_limiting and settings.security.redis_url:
            try:
                import redis
                redis_client = redis.from_url(settings.security.redis_url)
                # Test connection
                redis_client.ping()
                logger.info("Redis rate limiter initialized", redis_url=settings.security.redis_url)
            except ImportError:
                logger.warning("Redis not available, install redis package for production rate limiting")
            except Exception as e:
                logger.error("Failed to connect to Redis, falling back to in-memory", error=e)
        
        rate_limiter = RateLimiter(
            use_redis=settings.security.enable_redis_rate_limiting and redis_client is not None,
            redis_client=redis_client
        )
    
    return rate_limiter


def check_ip_whitelist(request: Request) -> bool:
    """
    Check if client IP is in the whitelist for break-glass endpoints.
    
    Args:
        request: FastAPI request object
        
    Returns:
        bool: True if IP is whitelisted or whitelisting is disabled
        
    Raises:
        HTTPException: If IP is not whitelisted
    """
    from .config import get_settings
    settings = get_settings()
    
    # Skip if IP whitelisting is disabled
    if not settings.security.enable_ip_whitelisting:
        return True
    
    # Get client IP
    client_ip = request.client.host if request.client else "unknown"
    
    # Check for forwarded headers (behind proxy)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Take the first IP in the chain
        client_ip = forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        client_ip = real_ip.strip()
    
    # Validate IP format and check whitelist
    try:
        client_addr = ipaddress.ip_address(client_ip)
        
        for allowed_ip in settings.security.break_glass_ip_whitelist:
            try:
                # Support both single IPs and CIDR ranges
                if "/" in allowed_ip:
                    allowed_network = ipaddress.ip_network(allowed_ip, strict=False)
                    if client_addr in allowed_network:
                        logger.info(
                            "Break-glass IP whitelist check passed",
                            client_ip=str(client_ip),
                            allowed_network=allowed_ip
                        )
                        return True
                else:
                    allowed_addr = ipaddress.ip_address(allowed_ip)
                    if client_addr == allowed_addr:
                        logger.info(
                            "Break-glass IP whitelist check passed",
                            client_ip=str(client_ip),
                            allowed_ip=allowed_ip
                        )
                        return True
            except ValueError:
                logger.warning(
                    "Invalid IP in whitelist configuration",
                    invalid_ip=allowed_ip
                )
                continue
        
        # IP not in whitelist
        logger.warning(
            "Break-glass access denied - IP not whitelisted",
            client_ip=str(client_ip),
            whitelist=settings.security.break_glass_ip_whitelist,
            endpoint=request.url.path
        )
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: IP address not authorized for break-glass operations"
        )
        
    except ValueError:
        logger.error(
            "Invalid client IP address format",
            client_ip=client_ip,
            endpoint=request.url.path
        )
        
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid IP address format"
        )


async def verify_api_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = None
) -> bool:
    """
    Verify API key for break-glass endpoints.
    
    Args:
        request: FastAPI request object
        credentials: Bearer token credentials
        
    Returns:
        bool: True if authenticated, raises HTTPException if not
        
    Raises:
        HTTPException: If authentication fails
    """
    # Import here to avoid circular imports
    from .config import get_settings
    settings = get_settings()
    
    # Generate correlation ID for audit trail
    correlation_id = request.headers.get("X-Correlation-ID", f"auth_{int(time.time() * 1000000)}")
    client_ip = request.client.host if request.client else "unknown"
    
    # Check for forwarded headers (behind proxy)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        client_ip = real_ip.strip()
    
    # Check if authentication is enabled
    if not settings.security.enable_authentication:
        logger.warning(
            "Authentication disabled - allowing request",
            extra={
                "correlation_id": correlation_id,
                "endpoint": request.url.path,
                "client_ip": client_ip,
                "user_agent": request.headers.get("User-Agent", "unknown"),
                "security_bypass": "authentication_disabled"
            }
        )
        return True
    
    # Check if API key is configured
    if not settings.security.dashboard_api_key:
        logger.error(
            "Dashboard API key not configured",
            extra={
                "correlation_id": correlation_id,
                "endpoint": request.url.path,
                "client_ip": client_ip,
                "security_error": "api_key_not_configured"
            }
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication not properly configured"
        )
    
    # Extract API key from Authorization header
    api_key = None
    if credentials and credentials.scheme.lower() == "bearer":
        api_key = credentials.credentials
    
    # Also check X-API-Key header as fallback
    if not api_key:
        api_key = request.headers.get("X-API-Key")
    
    if not api_key:
        logger.warning(
            "Missing API key for break-glass endpoint",
            extra={
                "correlation_id": correlation_id,
                "endpoint": request.url.path,
                "client_ip": client_ip,
                "user_agent": request.headers.get("User-Agent", "unknown"),
                "security_violation": "missing_api_key",
                "auth_headers": list(request.headers.keys())
            }
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required for break-glass operations",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Verify API key
    if api_key != settings.security.dashboard_api_key:
        logger.warning(
            "Invalid API key for break-glass endpoint",
            extra={
                "correlation_id": correlation_id,
                "endpoint": request.url.path,
                "client_ip": client_ip,
                "user_agent": request.headers.get("User-Agent", "unknown"),
                "security_violation": "invalid_api_key",
                "api_key_prefix": api_key[:8] + "..." if len(api_key) > 8 else "too_short"
            }
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    logger.info(
        "Break-glass endpoint authenticated successfully",
        extra={
            "correlation_id": correlation_id,
            "endpoint": request.url.path,
            "client_ip": client_ip,
            "user_agent": request.headers.get("User-Agent", "unknown"),
            "security_event": "successful_authentication",
            "auth_method": "bearer" if credentials else "x_api_key"
        }
    )
    
    return True


async def check_rate_limit(request: Request) -> bool:
    """
    Check rate limit for break-glass endpoints.
    
    Args:
        request: FastAPI request object
        
    Returns:
        bool: True if allowed, raises HTTPException if rate limited
        
    Raises:
        HTTPException: If rate limit exceeded
    """
    # Import here to avoid circular imports
    from .config import get_settings
    settings = get_settings()
    
    # Check if rate limiting is enabled
    if not settings.security.enable_rate_limiting:
        return True
    
    # Create rate limit key from client IP with forwarded header support
    client_ip = request.client.host if request.client else "unknown"
    
    # Check for forwarded headers (behind proxy)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        client_ip = real_ip.strip()
    
    rate_limit_key = f"break_glass:{client_ip}"
    
    # Get configured rate limiter
    limiter = get_rate_limiter()
    
    # Check rate limit
    if not limiter.is_allowed(rate_limit_key, settings.security.break_glass_rate_limit):
        logger.warning(
            "Rate limit exceeded for break-glass endpoint",
            endpoint=request.url.path,
            client_ip=client_ip,
            rate_limit=settings.security.break_glass_rate_limit
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Maximum {settings.security.break_glass_rate_limit} requests per hour."
        )
    
    return True


class SecurityHeaders:
    """Security headers middleware for API responses."""
    
    @staticmethod
    def get_security_headers() -> Dict[str, str]:
        """Get standard security headers."""
        return {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Content-Security-Policy": "default-src 'self'",
        }


def create_break_glass_dependency():
    """
    Create FastAPI dependency for break-glass endpoint authentication.
    
    Returns:
        Callable dependency function
    """
    async def break_glass_auth(
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = None
    ) -> bool:
        """FastAPI dependency for break-glass authentication."""
        # Check IP whitelist first (fastest check)
        check_ip_whitelist(request)
        
        # Check rate limit
        await check_rate_limit(request)
        
        # Finally verify API key
        await verify_api_key(request, credentials)
        
        logger.info(
            "Break-glass authentication successful",
            endpoint=request.url.path,
            client_ip=request.client.host if request.client else "unknown",
            user_agent=request.headers.get("User-Agent", "unknown")
        )
        
        return True
    
    return break_glass_auth


# Create the dependency instance
require_break_glass_auth = create_break_glass_dependency()