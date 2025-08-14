"""
MLB Betting Program Authentication System

Comprehensive authentication and authorization system with:
- JWT token-based authentication
- Role-based access control (RBAC)  
- Multi-factor authentication (MFA)
- Session management with security monitoring
- Password security with hashing and complexity requirements
- User profile management and preferences
- API key management for programmatic access
- Comprehensive audit logging
"""

from .models import (
    User,
    Role,
    Session,
    UserRole,
    UserPreferences,
    APIKey,
    AuditLog,
    PasswordResetToken,
    EmailVerificationToken,
)

from .services import (
    AuthenticationService,
    AuthorizationService,
    SessionService,
    UserService,
    PasswordService,
    MFAService,
    TokenService,
)

from .security import (
    PasswordHasher,
    JWTManager,
    SecurityValidator,
    RiskAssessment,
)

from .middleware import (
    AuthenticationMiddleware,
    AuthorizationMiddleware,
    AuditMiddleware,
)

from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    SessionError,
    PasswordError,
    MFAError,
    TokenError,
    SecurityError,
)

__all__ = [
    # Models
    "User",
    "Role", 
    "Session",
    "UserRole",
    "UserPreferences",
    "APIKey",
    "AuditLog",
    "PasswordResetToken",
    "EmailVerificationToken",
    
    # Services
    "AuthenticationService",
    "AuthorizationService", 
    "SessionService",
    "UserService",
    "PasswordService",
    "MFAService",
    "TokenService",
    
    # Security
    "PasswordHasher",
    "JWTManager",
    "SecurityValidator",
    "RiskAssessment",
    
    # Middleware
    "AuthenticationMiddleware",
    "AuthorizationMiddleware",
    "AuditMiddleware",
    
    # Exceptions
    "AuthenticationError",
    "AuthorizationError",
    "SessionError",
    "PasswordError", 
    "MFAError",
    "TokenError",
    "SecurityError",
]