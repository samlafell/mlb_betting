"""
ML API Security Middleware and Authentication
Enhanced security for production MLB prediction API
"""

import os
import time
import hmac
import hashlib
from typing import Optional, List, Dict, Any
from functools import wraps

from fastapi import HTTPException, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import redis.asyncio as redis
from pydantic import BaseModel

# Security configuration
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "dev-secret-change-in-production")
RATE_LIMIT_REQUESTS_PER_MINUTE = int(os.getenv("RATE_LIMIT_REQUESTS_PER_MINUTE", "60"))
PREDICTION_RATE_LIMIT = int(os.getenv("PREDICTION_RATE_LIMIT", "10"))

# Production CORS origins (empty in dev for flexibility)
ALLOWED_ORIGINS = []
if ENVIRONMENT == "production":
    origins_env = os.getenv("ALLOWED_ORIGINS", "")
    ALLOWED_ORIGINS = [origin.strip() for origin in origins_env.split(",") if origin.strip()]

security = HTTPBearer(auto_error=False)


class SecurityConfig(BaseModel):
    """Security configuration model"""
    environment: str = ENVIRONMENT
    require_auth: bool = ENVIRONMENT == "production"
    allowed_origins: List[str] = ALLOWED_ORIGINS
    rate_limit_per_minute: int = RATE_LIMIT_REQUESTS_PER_MINUTE
    prediction_rate_limit: int = PREDICTION_RATE_LIMIT


class RateLimiter:
    """Redis-based rate limiter with sliding window"""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.fallback_memory: Dict[str, List[float]] = {}
        
    async def is_allowed(self, key: str, limit: int, window_seconds: int = 60) -> bool:
        """Check if request is within rate limit"""
        current_time = time.time()
        
        if self.redis_client:
            try:
                # Use Redis for distributed rate limiting
                pipe = self.redis_client.pipeline()
                
                # Remove old entries
                pipe.zremrangebyscore(key, 0, current_time - window_seconds)
                
                # Count current requests
                pipe.zcard(key)
                
                # Add current request
                pipe.zadd(key, {str(current_time): current_time})
                
                # Set expiration
                pipe.expire(key, window_seconds)
                
                results = await pipe.execute()
                current_count = results[1]  # Count from zcard
                
                return current_count < limit
                
            except Exception:
                # Fallback to memory-based rate limiting
                return self._memory_rate_limit(key, limit, window_seconds, current_time)
        else:
            return self._memory_rate_limit(key, limit, window_seconds, current_time)
    
    def _memory_rate_limit(self, key: str, limit: int, window_seconds: int, current_time: float) -> bool:
        """Memory-based rate limiting fallback"""
        if key not in self.fallback_memory:
            self.fallback_memory[key] = []
        
        # Clean old entries
        cutoff_time = current_time - window_seconds
        self.fallback_memory[key] = [t for t in self.fallback_memory[key] if t > cutoff_time]
        
        # Check limit
        if len(self.fallback_memory[key]) >= limit:
            return False
        
        # Add current request
        self.fallback_memory[key].append(current_time)
        return True


# Global rate limiter instance
rate_limiter = RateLimiter()


def get_security_config() -> SecurityConfig:
    """Get current security configuration"""
    return SecurityConfig()


async def get_rate_limiter(request: Request) -> RateLimiter:
    """Get rate limiter with Redis client if available"""
    if hasattr(request.app.state, 'redis_client'):
        rate_limiter.redis_client = request.app.state.redis_client
    return rate_limiter


def verify_api_key(api_key: str) -> bool:
    """Verify API key using HMAC comparison"""
    if not api_key or not API_SECRET_KEY:
        return False
    
    # Time-safe comparison to prevent timing attacks
    return hmac.compare_digest(api_key, API_SECRET_KEY)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    config: SecurityConfig = Depends(get_security_config)
) -> Optional[Dict[str, Any]]:
    """Extract and validate current user from API key"""
    
    # Skip authentication in development
    if not config.require_auth:
        return {"user_id": "dev_user", "environment": "development"}
    
    # Require authentication in production
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not verify_api_key(credentials.credentials):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return {
        "user_id": "authenticated_user",
        "environment": config.environment,
        "auth_method": "api_key"
    }


async def rate_limit_check(
    request: Request,
    rate_limiter: RateLimiter = Depends(get_rate_limiter),
    config: SecurityConfig = Depends(get_security_config)
) -> None:
    """Check rate limits for API endpoints"""
    
    # Get client identifier
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    client_id = f"{client_ip}:{hash(user_agent) % 10000}"
    
    # Different limits for different endpoints
    if "/predict" in str(request.url.path):
        limit = config.prediction_rate_limit
        rate_key = f"prediction_rate_limit:{client_id}"
    else:
        limit = config.rate_limit_per_minute
        rate_key = f"general_rate_limit:{client_id}"
    
    # Check rate limit
    if not await rate_limiter.is_allowed(rate_key, limit):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Maximum {limit} requests per minute.",
            headers={
                "Retry-After": "60",
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Reset": str(int(time.time()) + 60)
            }
        )


def require_auth(f):
    """Decorator to require authentication for specific endpoints"""
    @wraps(f)
    async def wrapper(*args, **kwargs):
        # Extract user from kwargs (injected by Depends)
        user = kwargs.get('current_user')
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required"
            )
        return await f(*args, **kwargs)
    return wrapper


def get_cors_origins() -> List[str]:
    """Get allowed CORS origins based on environment"""
    if ENVIRONMENT == "production":
        return ALLOWED_ORIGINS if ALLOWED_ORIGINS else ["https://yourdomain.com"]
    else:
        # Development: allow localhost and common dev ports
        return [
            "http://localhost:3000",
            "http://localhost:8080",
            "http://localhost:8000",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:8080",
            "http://127.0.0.1:8000"
        ]


# Security headers middleware
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses"""
    response = await call_next(request)
    
    # Security headers
    security_headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Content-Security-Policy": "default-src 'self'",
    }
    
    # Add HSTS in production
    if ENVIRONMENT == "production":
        security_headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    
    for header, value in security_headers.items():
        response.headers[header] = value
    
    return response