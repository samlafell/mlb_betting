"""
Security utilities for API endpoints and authentication.

Provides:
- API key authentication for break-glass endpoints
- Rate limiting for sensitive operations
- Security headers and CORS configuration
"""

import time
from collections import defaultdict
from typing import Dict, Optional

from fastapi import HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .logging import get_logger, LogComponent

logger = get_logger(__name__, LogComponent.SECURITY)
security_scheme = HTTPBearer(auto_error=False)


class RateLimiter:
    """Simple in-memory rate limiter for break-glass endpoints."""
    
    def __init__(self):
        self._requests: Dict[str, list] = defaultdict(list)
        self._window_size = 3600  # 1 hour in seconds
    
    def is_allowed(self, key: str, limit: int) -> bool:
        """Check if request is within rate limit."""
        now = time.time()
        window_start = now - self._window_size
        
        # Clean old requests
        self._requests[key] = [
            req_time for req_time in self._requests[key] 
            if req_time > window_start
        ]
        
        # Check if under limit
        if len(self._requests[key]) >= limit:
            return False
        
        # Record this request
        self._requests[key].append(now)
        return True


# Global rate limiter instance
rate_limiter = RateLimiter()


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
    
    # Check if authentication is enabled
    if not settings.security.enable_authentication:
        logger.warning("Authentication disabled - allowing request")
        return True
    
    # Check if API key is configured
    if not settings.security.dashboard_api_key:
        logger.error("Dashboard API key not configured")
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
            endpoint=request.url.path,
            client_ip=request.client.host if request.client else "unknown"
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
            endpoint=request.url.path,
            client_ip=request.client.host if request.client else "unknown"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    logger.info(
        "Break-glass endpoint authenticated successfully",
        endpoint=request.url.path,
        client_ip=request.client.host if request.client else "unknown"
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
    
    # Create rate limit key from client IP
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_key = f"break_glass:{client_ip}"
    
    # Check rate limit
    if not rate_limiter.is_allowed(rate_limit_key, settings.security.break_glass_rate_limit):
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
        # Check rate limit first
        await check_rate_limit(request)
        
        # Then verify API key
        await verify_api_key(request, credentials)
        
        return True
    
    return break_glass_auth


# Create the dependency instance
require_break_glass_auth = create_break_glass_dependency()