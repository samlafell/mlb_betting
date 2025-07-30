"""
Enhanced ML API Security Components
Production-grade security implementations for MLB prediction API
"""

import os
import time
import hmac
import hashlib
import jwt
import asyncio
import logging
from typing import Optional, List, Dict, Any, Callable
from functools import wraps
from dataclasses import dataclass
from enum import Enum

from fastapi import HTTPException, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import redis.asyncio as redis
from pydantic import BaseModel
import aiohttp


class UserRole(str, Enum):
    """User role enumeration"""

    PUBLIC = "public"
    CONSUMER = "consumer"
    PREMIUM = "premium"
    ADMIN = "admin"


class SecurityEvent(str, Enum):
    """Security event types"""

    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    RATE_LIMIT_VIOLATION = "rate_limit_violation"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    DATA_ACCESS = "data_access"
    API_ABUSE = "api_abuse"


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""

    requests_per_minute: int
    burst_limit: int
    window_seconds: int = 60


class EnhancedSecurityConfig(BaseModel):
    """Enhanced security configuration"""

    environment: str = os.getenv("ENVIRONMENT", "development")
    require_auth: bool = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
    api_secret_key: str = os.getenv("API_SECRET_KEY", "dev-secret-change-in-production")
    token_expiry_hours: int = int(os.getenv("TOKEN_EXPIRY_HOURS", "24"))

    # Rate limiting by role
    rate_limits: Dict[UserRole, RateLimitConfig] = {
        UserRole.PUBLIC: RateLimitConfig(100, 150),
        UserRole.CONSUMER: RateLimitConfig(300, 400),
        UserRole.PREMIUM: RateLimitConfig(1000, 1200),
        UserRole.ADMIN: RateLimitConfig(2000, 2500),
    }

    # Security thresholds
    max_failed_attempts: int = 5
    lockout_duration_minutes: int = 15
    suspicious_activity_threshold: int = 10

    # TLS settings
    require_tls: bool = environment == "production"
    min_tls_version: str = "1.2"


class SecurityLogger:
    """Enhanced security event logging"""

    def __init__(self):
        self.logger = logging.getLogger("security")
        self.logger.setLevel(logging.INFO)

        # Create handler if not exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s SECURITY [%(levelname)s] %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def log_security_event(
        self,
        event_type: SecurityEvent,
        user_id: str = None,
        ip_address: str = None,
        details: Dict[str, Any] = None,
    ):
        """Log security events with structured data"""
        event_data = {
            "event_type": event_type.value,
            "timestamp": time.time(),
            "user_id": user_id,
            "ip_address": ip_address,
            "details": details or {},
        }

        if event_type in [
            SecurityEvent.AUTH_FAILURE,
            SecurityEvent.RATE_LIMIT_VIOLATION,
            SecurityEvent.SUSPICIOUS_ACTIVITY,
            SecurityEvent.API_ABUSE,
        ]:
            self.logger.warning(f"SECURITY_ALERT: {event_data}")
        else:
            self.logger.info(f"SECURITY_EVENT: {event_data}")


class CircuitBreaker:
    """Circuit breaker for external service protection"""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: Exception = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""

        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
            else:
                raise HTTPException(
                    status_code=503, detail="Service temporarily unavailable"
                )

        try:
            result = await func(*args, **kwargs)

            # Reset on success
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0

            return result

        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"

            raise HTTPException(status_code=503, detail=f"Service error: {str(e)}")


class EnhancedAuthenticator:
    """Production-grade JWT authentication"""

    def __init__(self, config: EnhancedSecurityConfig):
        self.config = config
        self.security_logger = SecurityLogger()

    def generate_token(self, user_id: str, role: UserRole) -> str:
        """Generate JWT token with role-based access"""
        payload = {
            "user_id": user_id,
            "role": role.value,
            "issued_at": time.time(),
            "expires_at": time.time() + (self.config.token_expiry_hours * 3600),
            "api_version": "v1",
        }

        return jwt.encode(payload, self.config.api_secret_key, algorithm="HS256")

    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(
                token, self.config.api_secret_key, algorithms=["HS256"]
            )

            # Check expiration
            if time.time() > payload.get("expires_at", 0):
                raise jwt.ExpiredSignatureError("Token expired")

            return payload

        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")


class AdvancedRateLimiter:
    """Advanced rate limiter with role-based limits and sliding window"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.memory_cache: Dict[str, List[float]] = {}
        self.security_logger = SecurityLogger()

    async def is_allowed(
        self, key: str, config: RateLimitConfig, user_role: UserRole = UserRole.PUBLIC
    ) -> bool:
        """Check if request is within rate limit"""
        current_time = time.time()

        # Use Redis for distributed rate limiting
        if self.redis_client:
            try:
                return await self._redis_rate_limit(key, config, current_time)
            except Exception as e:
                self.security_logger.log_security_event(
                    SecurityEvent.SUSPICIOUS_ACTIVITY,
                    details={
                        "error": "Redis rate limiter failure",
                        "fallback": "memory",
                    },
                )
                return self._memory_rate_limit(key, config, current_time)

        return self._memory_rate_limit(key, config, current_time)

    async def _redis_rate_limit(
        self, key: str, config: RateLimitConfig, current_time: float
    ) -> bool:
        """Redis-based sliding window rate limiting"""
        pipe = self.redis_client.pipeline()

        # Remove old entries
        pipe.zremrangebyscore(key, 0, current_time - config.window_seconds)

        # Count current requests
        pipe.zcard(key)

        # Add current request
        pipe.zadd(key, {str(current_time): current_time})

        # Set expiration
        pipe.expire(key, config.window_seconds)

        results = await pipe.execute()
        current_count = results[1]  # Count from zcard

        return current_count < config.requests_per_minute

    def _memory_rate_limit(
        self, key: str, config: RateLimitConfig, current_time: float
    ) -> bool:
        """Memory-based rate limiting fallback"""
        if key not in self.memory_cache:
            self.memory_cache[key] = []

        # Clean old entries
        cutoff_time = current_time - config.window_seconds
        self.memory_cache[key] = [t for t in self.memory_cache[key] if t > cutoff_time]

        # Check limit
        if len(self.memory_cache[key]) >= config.requests_per_minute:
            return False

        # Add current request
        self.memory_cache[key].append(current_time)
        return True


class ThreatDetector:
    """Real-time threat detection system"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.security_logger = SecurityLogger()

    async def analyze_request(
        self, request: Request, user_id: str = None
    ) -> Dict[str, Any]:
        """Analyze request for suspicious patterns"""

        ip_address = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")

        threat_score = 0
        threats = []

        # Check for suspicious patterns
        if await self._check_ip_reputation(ip_address):
            threat_score += 30
            threats.append("suspicious_ip")

        if self._check_suspicious_user_agent(user_agent):
            threat_score += 20
            threats.append("suspicious_user_agent")

        if await self._check_request_frequency(ip_address):
            threat_score += 25
            threats.append("high_frequency_requests")

        if self._check_payload_anomalies(request):
            threat_score += 15
            threats.append("anomalous_payload")

        # Log if threats detected
        if threat_score > 50:
            self.security_logger.log_security_event(
                SecurityEvent.SUSPICIOUS_ACTIVITY,
                user_id=user_id,
                ip_address=ip_address,
                details={
                    "threat_score": threat_score,
                    "threats": threats,
                    "user_agent": user_agent,
                },
            )

        return {
            "threat_score": threat_score,
            "threats": threats,
            "action": "block" if threat_score > 80 else "monitor",
        }

    async def _check_ip_reputation(self, ip_address: str) -> bool:
        """Check IP against threat intelligence feeds"""
        # In production, integrate with threat intel APIs
        # For now, basic checks

        # Check against known bad ranges
        suspicious_ranges = [
            "10.0.0.0/8",  # Private networks shouldn't access public API
            "172.16.0.0/12",  # Private networks
            "192.168.0.0/16",  # Private networks
        ]

        # Simple check - in production use proper IP analysis
        for range_check in suspicious_ranges:
            if ip_address.startswith(range_check.split("/")[0].rsplit(".", 1)[0]):
                return True

        return False

    def _check_suspicious_user_agent(self, user_agent: str) -> bool:
        """Check for suspicious user agent patterns"""
        suspicious_patterns = [
            "bot",
            "crawler",
            "spider",
            "scraper",
            "python-requests",
            "curl",
            "wget",
            "scanner",
            "exploit",
            "attack",
        ]

        user_agent_lower = user_agent.lower()
        return any(pattern in user_agent_lower for pattern in suspicious_patterns)

    async def _check_request_frequency(self, ip_address: str) -> bool:
        """Check for unusually high request frequency"""
        if not self.redis_client:
            return False

        key = f"frequency:{ip_address}"
        current_time = time.time()

        # Count requests in last minute
        count = await self.redis_client.zcount(key, current_time - 60, current_time)

        return count > 100  # More than 100 requests per minute

    def _check_payload_anomalies(self, request: Request) -> bool:
        """Check for anomalous request payloads"""
        # Check for unusually large requests
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > 1024 * 1024:  # 1MB
            return True

        # Check for suspicious headers
        suspicious_headers = ["x-forwarded-for", "x-real-ip", "x-cluster-client-ip"]

        for header in suspicious_headers:
            if header in request.headers:
                # Check for header injection attempts
                value = request.headers.get(header, "")
                if any(char in value for char in ["\n", "\r", "\x00"]):
                    return True

        return False


# Enhanced middleware and dependencies
security_config = EnhancedSecurityConfig()
authenticator = EnhancedAuthenticator(security_config)
rate_limiter = AdvancedRateLimiter()
threat_detector = ThreatDetector()
security_logger = SecurityLogger()

# Circuit breakers for external services
redis_circuit_breaker = CircuitBreaker(
    failure_threshold=3, recovery_timeout=30, expected_exception=redis.RedisError
)

database_circuit_breaker = CircuitBreaker(
    failure_threshold=5, recovery_timeout=60, expected_exception=Exception
)


async def enhanced_auth_dependency(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False)),
    request: Request = None,
) -> Dict[str, Any]:
    """Enhanced authentication with threat detection"""

    # Skip authentication in development if not required
    if (
        not security_config.require_auth
        and security_config.environment == "development"
    ):
        return {
            "user_id": "dev_user",
            "role": UserRole.ADMIN,
            "environment": "development",
        }

    # Require authentication in production
    if not credentials:
        security_logger.log_security_event(
            SecurityEvent.AUTH_FAILURE,
            ip_address=request.client.host if request.client else "unknown",
            details={"reason": "missing_credentials"},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify token
    try:
        payload = authenticator.verify_token(credentials.credentials)

        # Threat analysis
        threat_analysis = await threat_detector.analyze_request(
            request, user_id=payload.get("user_id")
        )

        if threat_analysis["action"] == "block":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Request blocked due to suspicious activity",
            )

        security_logger.log_security_event(
            SecurityEvent.AUTH_SUCCESS,
            user_id=payload.get("user_id"),
            ip_address=request.client.host if request.client else "unknown",
        )

        return {
            "user_id": payload.get("user_id"),
            "role": UserRole(payload.get("role", "consumer")),
            "threat_score": threat_analysis["threat_score"],
        }

    except HTTPException:
        raise
    except Exception as e:
        security_logger.log_security_event(
            SecurityEvent.AUTH_FAILURE,
            ip_address=request.client.host if request.client else "unknown",
            details={"reason": "token_verification_error", "error": str(e)},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )


async def enhanced_rate_limit_check(
    request: Request, user_data: Dict[str, Any] = Depends(enhanced_auth_dependency)
) -> None:
    """Enhanced rate limiting with role-based limits"""

    client_ip = request.client.host if request.client else "unknown"
    user_role = user_data.get("role", UserRole.PUBLIC)
    user_id = user_data.get("user_id", "anonymous")

    # Get rate limit configuration for user role
    rate_config = security_config.rate_limits.get(
        user_role, security_config.rate_limits[UserRole.PUBLIC]
    )

    # Create rate limit key
    rate_key = f"rate_limit:{user_role.value}:{client_ip}"

    # Check rate limit
    if not await rate_limiter.is_allowed(rate_key, rate_config, user_role):
        security_logger.log_security_event(
            SecurityEvent.RATE_LIMIT_VIOLATION,
            user_id=user_id,
            ip_address=client_ip,
            details={
                "role": user_role.value,
                "limit": rate_config.requests_per_minute,
                "endpoint": str(request.url.path),
            },
        )

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Maximum {rate_config.requests_per_minute} requests per minute for {user_role.value} role.",
            headers={
                "Retry-After": str(rate_config.window_seconds),
                "X-RateLimit-Limit": str(rate_config.requests_per_minute),
                "X-RateLimit-Reset": str(int(time.time()) + rate_config.window_seconds),
                "X-RateLimit-Role": user_role.value,
            },
        )


def require_role(required_role: UserRole):
    """Decorator to require specific role for endpoint access"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user_data = kwargs.get("user_data", {})
            user_role = user_data.get("role", UserRole.PUBLIC)

            # Role hierarchy check
            role_hierarchy = {
                UserRole.PUBLIC: 0,
                UserRole.CONSUMER: 1,
                UserRole.PREMIUM: 2,
                UserRole.ADMIN: 3,
            }

            if role_hierarchy.get(user_role, 0) < role_hierarchy.get(required_role, 0):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Role {required_role.value} or higher required",
                )

            return await func(*args, **kwargs)

        return wrapper

    return decorator


# Export the enhanced security components
__all__ = [
    "EnhancedSecurityConfig",
    "SecurityLogger",
    "CircuitBreaker",
    "EnhancedAuthenticator",
    "AdvancedRateLimiter",
    "ThreatDetector",
    "UserRole",
    "SecurityEvent",
    "enhanced_auth_dependency",
    "enhanced_rate_limit_check",
    "require_role",
    "security_config",
    "redis_circuit_breaker",
    "database_circuit_breaker",
]
