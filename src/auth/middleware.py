"""
Authentication Middleware

FastAPI middleware for authentication, authorization, session management,
and comprehensive security monitoring.
"""

import time
from datetime import datetime, timezone
from typing import Optional, Callable, List, Dict, Any
from uuid import uuid4

from fastapi import FastAPI, Request, Response, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from ..core.config import get_settings
from ..core.logging import get_logger, LogComponent
from .services import AuthenticationService
from .authorization import AuthorizationService
from .security import JWTManager, TokenType, SecurityValidator
from .models import User
from .exceptions import (
    AuthenticationError, AuthorizationError, SessionExpiredError,
    InvalidTokenError, TokenExpiredError, InsufficientPermissionsError,
    SuspiciousActivityError, RateLimitExceededError
)
from .rate_limiter import get_rate_limiter, require_rate_limit

logger = get_logger(__name__, LogComponent.AUTH)

# Global instances
security = HTTPBearer(auto_error=False)
jwt_manager = JWTManager()
auth_service = AuthenticationService()
authz_service = AuthorizationService()
security_validator = SecurityValidator()
rate_limiter = get_rate_limiter()


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Middleware for handling authentication and session management."""
    
    def __init__(
        self,
        app: FastAPI,
        excluded_paths: Optional[List[str]] = None,
        require_auth_by_default: bool = True
    ):
        """
        Initialize authentication middleware.
        
        Args:
            app: FastAPI application
            excluded_paths: Paths that don't require authentication
            require_auth_by_default: Whether to require auth by default
        """
        super().__init__(app)
        self.excluded_paths = excluded_paths or [
            "/health",
            "/metrics",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/auth/login",
            "/auth/register",
            "/auth/password-reset",
            "/auth/verify-email"
        ]
        self.require_auth_by_default = require_auth_by_default
        self.settings = get_settings()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request through authentication middleware."""
        # Generate correlation ID for tracking
        correlation_id = str(uuid4())
        request.state.correlation_id = correlation_id
        
        start_time = time.time()
        
        try:
            # Skip authentication for excluded paths
            if self._is_excluded_path(request.url.path):
                return await call_next(request)
            
            # Check if authentication is disabled globally
            if not self.settings.security.enable_authentication:
                logger.debug("Authentication disabled globally")
                return await call_next(request)
            
            # Extract and validate token
            user = await self._authenticate_request(request)
            
            if user:
                request.state.user = user
                request.state.authenticated = True
                
                # Update user activity
                await self._update_user_activity(user.id, request)
            else:
                request.state.user = None
                request.state.authenticated = False
                
                # Check if authentication is required for this endpoint
                if self.require_auth_by_default:
                    return self._create_auth_error_response(
                        "Authentication required",
                        status.HTTP_401_UNAUTHORIZED
                    )
            
            # Process request
            response = await call_next(request)
            
            # Add security headers
            self._add_security_headers(response)
            
            # Log successful request
            duration = time.time() - start_time
            await self._log_request(request, response, duration, True)
            
            return response
            
        except AuthenticationError as e:
            logger.warning(
                "Authentication error in middleware",
                error=e,
                extra={
                    "path": request.url.path,
                    "correlation_id": correlation_id
                }
            )
            return self._create_auth_error_response(str(e), status.HTTP_401_UNAUTHORIZED)
            
        except AuthorizationError as e:
            logger.warning(
                "Authorization error in middleware",
                error=e,
                extra={
                    "path": request.url.path,
                    "correlation_id": correlation_id
                }
            )
            return self._create_auth_error_response(str(e), status.HTTP_403_FORBIDDEN)
            
        except Exception as e:
            logger.error(
                "Unexpected error in authentication middleware",
                error=e,
                extra={
                    "path": request.url.path,
                    "correlation_id": correlation_id
                }
            )
            return self._create_auth_error_response(
                "Authentication service unavailable",
                status.HTTP_503_SERVICE_UNAVAILABLE
            )
    
    def _is_excluded_path(self, path: str) -> bool:
        """Check if path is excluded from authentication."""
        return any(path.startswith(excluded) for excluded in self.excluded_paths)
    
    async def _authenticate_request(self, request: Request) -> Optional[User]:
        """Authenticate request and return user if valid."""
        # Extract token from Authorization header
        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return None
        
        token = auth_header[7:]  # Remove "Bearer " prefix
        
        try:
            # Decode and validate JWT token
            claims = jwt_manager.decode_token(token)
            
            # Verify it's an access token
            if claims.token_type != TokenType.ACCESS:
                raise InvalidTokenError("Token is not an access token")
            
            # Get user information
            from .services import AuthenticationService
            auth_svc = AuthenticationService()
            user = await auth_svc._get_user_by_id(int(claims.sub))
            
            # Verify user is still active
            if not user.is_active:
                raise AuthenticationError("User account is inactive")
            
            # Verify session if present
            if claims.session_id:
                session = await auth_svc._get_session_by_id(claims.session_id)
                if not session.is_active or session.expires_at < datetime.now(timezone.utc):
                    raise SessionExpiredError()
            
            return user
            
        except (TokenExpiredError, InvalidTokenError, SessionExpiredError):
            return None
        except Exception as e:
            logger.error("Token authentication error", error=e)
            return None
    
    async def _update_user_activity(self, user_id: int, request: Request) -> None:
        """Update user's last activity timestamp."""
        try:
            from ..data.database.connection import get_connection
            
            async with get_connection() as conn:
                await conn.execute(
                    "UPDATE auth.users SET last_activity = NOW() WHERE id = $1",
                    user_id
                )
        except Exception as e:
            logger.error("Failed to update user activity", error=e)
    
    def _add_security_headers(self, response: Response) -> None:
        """Add security headers to response."""
        security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY", 
            "X-XSS-Protection": "1; mode=block",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Content-Security-Policy": "default-src 'self'",
        }
        
        if self.settings.environment == "production":
            security_headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        for header, value in security_headers.items():
            response.headers[header] = value
    
    def _create_auth_error_response(self, message: str, status_code: int) -> JSONResponse:
        """Create standardized authentication error response."""
        return JSONResponse(
            status_code=status_code,
            content={
                "error": "authentication_error",
                "message": message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            headers={"WWW-Authenticate": "Bearer"} if status_code == 401 else {}
        )
    
    async def _log_request(
        self, request: Request, response: Response, duration: float, success: bool
    ) -> None:
        """Log request details for security monitoring."""
        try:
            user_id = getattr(request.state, 'user', None)
            user_id = user_id.id if user_id else None
            
            # Extract client information
            client_ip = request.client.host if request.client else "unknown"
            user_agent = request.headers.get("user-agent", "unknown")
            
            # Log to audit system
            from .services import AuthenticationService
            auth_svc = AuthenticationService()
            
            await auth_svc._log_auth_event(
                user_id=user_id,
                session_id=None,
                event_type="api_request",
                event_category="session",
                event_description=f"{request.method} {request.url.path}",
                ip_address=client_ip,
                user_agent=user_agent,
                success=success,
                metadata={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": round(duration * 1000, 2),
                    "correlation_id": getattr(request.state, 'correlation_id', None)
                }
            )
        except Exception as e:
            logger.error("Failed to log request", error=e)


class AuthorizationMiddleware(BaseHTTPMiddleware):
    """Middleware for role-based authorization and permission checking."""
    
    def __init__(
        self,
        app: FastAPI,
        permission_map: Optional[Dict[str, List[str]]] = None
    ):
        """
        Initialize authorization middleware.
        
        Args:
            app: FastAPI application
            permission_map: Mapping of endpoints to required permissions
        """
        super().__init__(app)
        self.permission_map = permission_map or {}
        self.authz_service = AuthorizationService()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request through authorization middleware."""
        try:
            # Skip if user is not authenticated
            if not getattr(request.state, 'authenticated', False):
                return await call_next(request)
            
            user = getattr(request.state, 'user', None)
            if not user:
                return await call_next(request)
            
            # Check endpoint-specific permissions
            required_permissions = self._get_required_permissions(request)
            
            if required_permissions:
                await self._check_permissions(user.id, required_permissions)
            
            return await call_next(request)
            
        except AuthorizationError as e:
            logger.warning(
                "Authorization failed",
                error=e,
                extra={
                    "user_id": user.id if user else None,
                    "path": request.url.path,
                    "method": request.method
                }
            )
            
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    "error": "authorization_error",
                    "message": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
        
        except Exception as e:
            logger.error("Authorization middleware error", error=e)
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": "authorization_service_error",
                    "message": "Authorization service temporarily unavailable",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
    
    def _get_required_permissions(self, request: Request) -> List[str]:
        """Get required permissions for endpoint."""
        endpoint_key = f"{request.method} {request.url.path}"
        
        # Check exact match first
        if endpoint_key in self.permission_map:
            return self.permission_map[endpoint_key]
        
        # Check pattern matches
        for pattern, permissions in self.permission_map.items():
            if self._path_matches_pattern(request.url.path, pattern):
                return permissions
        
        return []
    
    def _path_matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if path matches permission pattern."""
        # Remove HTTP method if present in pattern
        if ' ' in pattern:
            pattern = pattern.split(' ', 1)[1]
        
        # Simple wildcard matching
        if pattern.endswith('*'):
            return path.startswith(pattern[:-1])
        
        return path == pattern
    
    async def _check_permissions(self, user_id: int, required_permissions: List[str]) -> None:
        """Check if user has required permissions."""
        if len(required_permissions) == 1:
            await self.authz_service.require_permission(user_id, required_permissions[0])
        else:
            await self.authz_service.require_any_permission(user_id, required_permissions)


class RateLimitingMiddleware(BaseHTTPMiddleware):
    """Middleware for API rate limiting."""
    
    def __init__(
        self,
        app: FastAPI,
        default_rule: str = "api_general",
        endpoint_rules: Optional[Dict[str, str]] = None
    ):
        """
        Initialize rate limiting middleware.
        
        Args:
            app: FastAPI application
            default_rule: Default rate limit rule name
            endpoint_rules: Mapping of endpoints to specific rate limit rules
        """
        super().__init__(app)
        self.default_rule = default_rule
        self.endpoint_rules = endpoint_rules or {
            "/auth/login": "login",
            "/auth/password-reset": "password_reset",
            "/auth/register": "user_registration",
            "/auth/mfa/verify": "mfa_verification",
            "/auth/verify-email": "email_verification"
        }
        self.rate_limiter = get_rate_limiter()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request through rate limiting middleware."""
        try:
            # Skip rate limiting for certain paths
            if self._should_skip_rate_limiting(request.url.path):
                return await call_next(request)
            
            # Determine rate limit rule and identifier
            rule_name = self._get_rate_limit_rule(request)
            identifier = self._get_rate_limit_identifier(request)
            
            # Check rate limit
            try:
                status = await require_rate_limit(rule_name, identifier)
                
                # Add rate limit headers to response
                response = await call_next(request)
                self._add_rate_limit_headers(response, status)
                
                return response
                
            except RateLimitExceededError as e:
                logger.warning(
                    "Rate limit exceeded",
                    extra={
                        "rule": rule_name,
                        "identifier": identifier,
                        "path": request.url.path,
                        "method": request.method,
                        "retry_after": e.retry_after_seconds
                    }
                )
                
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": "rate_limit_exceeded",
                        "message": str(e),
                        "retry_after_seconds": e.retry_after_seconds,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    },
                    headers={
                        "Retry-After": str(e.retry_after_seconds or 60),
                        "X-RateLimit-Limit": str(e.limit),
                        "X-RateLimit-Remaining": str(e.remaining),
                        "X-RateLimit-Reset": str(int(e.reset_time.timestamp())) if e.reset_time else ""
                    }
                )
                
        except Exception as e:
            logger.error("Rate limiting middleware error", error=e)
            # Fail open - allow request if rate limiting fails
            return await call_next(request)
    
    def _should_skip_rate_limiting(self, path: str) -> bool:
        """Check if path should skip rate limiting."""
        skip_paths = ["/health", "/metrics", "/docs", "/redoc", "/openapi.json"]
        return any(path.startswith(skip_path) for skip_path in skip_paths)
    
    def _get_rate_limit_rule(self, request: Request) -> str:
        """Get rate limit rule for request."""
        endpoint_key = f"{request.method} {request.url.path}"
        
        # Check exact match first
        if request.url.path in self.endpoint_rules:
            return self.endpoint_rules[request.url.path]
        
        if endpoint_key in self.endpoint_rules:
            return self.endpoint_rules[endpoint_key]
        
        # Check pattern matches
        for pattern, rule in self.endpoint_rules.items():
            if request.url.path.startswith(pattern.rstrip("*")):
                return rule
        
        return self.default_rule
    
    def _get_rate_limit_identifier(self, request: Request) -> str:
        """Get rate limit identifier for request."""
        # Try to get user ID if authenticated
        user = getattr(request.state, 'user', None)
        if user:
            return f"user:{user.id}"
        
        # Check for API key
        api_key = request.headers.get("x-api-key")
        if api_key:
            return f"api_key:{api_key[:8]}..."  # Use prefix for privacy
        
        # Fall back to IP address
        client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"
    
    def _add_rate_limit_headers(self, response: Response, status) -> None:
        """Add rate limit headers to response."""
        response.headers.update({
            "X-RateLimit-Limit": str(status.limit),
            "X-RateLimit-Remaining": str(status.remaining),
            "X-RateLimit-Reset": str(int(status.reset_time.timestamp())),
            "X-RateLimit-Used": str(status.used)
        })


class AuditMiddleware(BaseHTTPMiddleware):
    """Middleware for comprehensive security auditing and monitoring."""
    
    def __init__(self, app: FastAPI, enable_detailed_logging: bool = False):
        """
        Initialize audit middleware.
        
        Args:
            app: FastAPI application
            enable_detailed_logging: Enable detailed request/response logging
        """
        super().__init__(app)
        self.enable_detailed_logging = enable_detailed_logging
        self.security_validator = SecurityValidator()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request through audit middleware."""
        start_time = time.time()
        
        # Extract request metadata
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        user = getattr(request.state, 'user', None)
        
        try:
            # Assess request risk
            risk_assessment = await self._assess_request_risk(request, user)
            
            # Block high-risk requests
            if risk_assessment.risk_level == "critical" and not risk_assessment.allow_action:
                logger.warning(
                    "High-risk request blocked",
                    extra={
                        "ip_address": client_ip,
                        "user_agent": user_agent,
                        "risk_score": risk_assessment.risk_score,
                        "risk_factors": risk_assessment.risk_factors,
                        "path": request.url.path
                    }
                )
                
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": "suspicious_activity",
                        "message": "Request blocked due to suspicious activity",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                )
            
            # Process request
            response = await call_next(request)
            duration = time.time() - start_time
            
            # Log security event
            await self._log_security_event(
                request, response, user, duration, risk_assessment
            )
            
            return response
            
        except Exception as e:
            logger.error("Audit middleware error", error=e)
            # Continue processing even if audit fails
            return await call_next(request)
    
    async def _assess_request_risk(self, request: Request, user: Optional[User]) -> Any:
        """Assess security risk of request."""
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        # For now, return a basic assessment
        # In production, this would integrate with threat intelligence
        return type('SecurityAssessment', (), {
            'risk_score': 10,
            'risk_level': 'low',
            'risk_factors': [],
            'recommendations': [],
            'allow_action': True
        })()
    
    async def _log_security_event(
        self,
        request: Request,
        response: Response,
        user: Optional[User],
        duration: float,
        risk_assessment: Any
    ) -> None:
        """Log security event for monitoring."""
        try:
            if not self.enable_detailed_logging:
                return
            
            client_ip = request.client.host if request.client else "unknown"
            user_agent = request.headers.get("user-agent", "unknown")
            
            logger.info(
                "Request audit log",
                extra={
                    "user_id": user.id if user else None,
                    "username": user.username if user else None,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": round(duration * 1000, 2),
                    "ip_address": client_ip,
                    "user_agent": user_agent,
                    "risk_score": risk_assessment.risk_score,
                    "risk_level": risk_assessment.risk_level,
                    "correlation_id": getattr(request.state, 'correlation_id', None)
                }
            )
        except Exception as e:
            logger.error("Failed to log security event", error=e)


# FastAPI dependency functions
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """
    FastAPI dependency to get current authenticated user.
    
    Raises:
        HTTPException: If user is not authenticated
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    try:
        # Decode JWT token
        claims = jwt_manager.decode_token(credentials.credentials)
        
        if claims.token_type != TokenType.ACCESS:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        # Get user
        user = await auth_service._get_user_by_id(int(claims.sub))
        
        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User account is inactive",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return user
        
    except TokenExpiredError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as e:
        logger.error("Error getting current user", error=e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Optional[User]:
    """
    FastAPI dependency to get current user if authenticated, None otherwise.
    """
    if not credentials:
        return None
    
    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None


def require_permission(permission: str):
    """
    FastAPI dependency factory to require specific permission.
    
    Args:
        permission: Required permission string
        
    Returns:
        Dependency function
    """
    async def permission_dependency(user: User = Depends(get_current_user)) -> User:
        try:
            await authz_service.require_permission(user.id, permission)
            return user
        except InsufficientPermissionsError as e:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(e)
            )
    
    return permission_dependency


def require_any_permission(permissions: List[str]):
    """
    FastAPI dependency factory to require any of the specified permissions.
    
    Args:
        permissions: List of acceptable permissions
        
    Returns:
        Dependency function
    """
    async def permission_dependency(user: User = Depends(get_current_user)) -> User:
        try:
            await authz_service.require_any_permission(user.id, permissions)
            return user
        except InsufficientPermissionsError as e:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(e)
            )
    
    return permission_dependency


def require_role(role_name: str):
    """
    FastAPI dependency factory to require specific role.
    
    Args:
        role_name: Required role name
        
    Returns:
        Dependency function
    """
    async def role_dependency(user: User = Depends(get_current_user)) -> User:
        user_roles = await authz_service.get_user_roles(user.id)
        role_names = [role.name for role in user_roles]
        
        if role_name not in role_names:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{role_name}' required"
            )
        
        return user
    
    return role_dependency


# Convenience functions for common permission patterns
def require_admin() -> Callable:
    """Require admin permissions."""
    return require_any_permission(["system:admin", "system:*"])


def require_user_management() -> Callable:
    """Require user management permissions."""
    return require_permission("user:manage")


def require_data_access() -> Callable:
    """Require data access permissions."""
    return require_any_permission(["data:read", "data:write", "data:*"])


def require_analytics_access() -> Callable:
    """Require analytics access permissions."""
    return require_any_permission(["analytics:read", "analytics:write", "analytics:*"])


def require_ml_access() -> Callable:
    """Require ML access permissions."""
    return require_any_permission(["ml:read", "ml:write", "ml:*"])