"""
Authentication System Exceptions

Comprehensive exception hierarchy for authentication and authorization errors.
"""

from typing import Optional, Dict, Any


class AuthError(Exception):
    """Base authentication system exception."""
    
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__.lower()
        self.details = details or {}


class AuthenticationError(AuthError):
    """Authentication failed - user could not be verified."""
    pass


class AuthorizationError(AuthError):
    """Authorization failed - user lacks required permissions."""
    pass


class SessionError(AuthError):
    """Session management error."""
    pass


class PasswordError(AuthError):
    """Password-related error."""
    pass


class MFAError(AuthError):
    """Multi-factor authentication error."""
    pass


class TokenError(AuthError):
    """JWT token or API key error."""
    pass


class SecurityError(AuthError):
    """General security violation."""
    pass


# Specific authentication errors
class InvalidCredentialsError(AuthenticationError):
    """Invalid username/password combination."""
    
    def __init__(self, attempts_remaining: Optional[int] = None):
        message = "Invalid username or password"
        details = {}
        if attempts_remaining is not None:
            details["attempts_remaining"] = attempts_remaining
        super().__init__(message, "invalid_credentials", details)


class AccountLockedError(AuthenticationError):
    """Account is temporarily locked due to failed login attempts."""
    
    def __init__(self, locked_until: Optional[str] = None):
        message = "Account is temporarily locked due to failed login attempts"
        details = {}
        if locked_until:
            details["locked_until"] = locked_until
        super().__init__(message, "account_locked", details)


class AccountDisabledError(AuthenticationError):
    """Account has been disabled."""
    
    def __init__(self):
        super().__init__(
            "Account has been disabled",
            "account_disabled"
        )


class EmailNotVerifiedError(AuthenticationError):
    """Email address has not been verified."""
    
    def __init__(self):
        super().__init__(
            "Email address must be verified before login",
            "email_not_verified"
        )


class PasswordExpiredError(AuthenticationError):
    """Password has expired and must be changed."""
    
    def __init__(self):
        super().__init__(
            "Password has expired and must be changed",
            "password_expired"
        )


class MFARequiredError(AuthenticationError):
    """Multi-factor authentication is required."""
    
    def __init__(self, session_token: Optional[str] = None):
        message = "Multi-factor authentication is required"
        details = {}
        if session_token:
            details["mfa_session_token"] = session_token
        super().__init__(message, "mfa_required", details)


# Authorization errors
class InsufficientPermissionsError(AuthorizationError):
    """User lacks required permissions for this operation."""
    
    def __init__(self, required_permission: str, user_permissions: Optional[list] = None):
        message = f"Operation requires '{required_permission}' permission"
        details = {
            "required_permission": required_permission,
            "user_permissions": user_permissions or []
        }
        super().__init__(message, "insufficient_permissions", details)


class RoleNotFoundError(AuthorizationError):
    """Requested role does not exist."""
    
    def __init__(self, role_name: str):
        super().__init__(
            f"Role '{role_name}' does not exist",
            "role_not_found",
            {"role_name": role_name}
        )


# Session errors
class SessionExpiredError(SessionError):
    """Session has expired."""
    
    def __init__(self):
        super().__init__(
            "Session has expired",
            "session_expired"
        )


class SessionNotFoundError(SessionError):
    """Session not found or invalid."""
    
    def __init__(self):
        super().__init__(
            "Session not found or invalid",
            "session_not_found"
        )


class SessionRevokedError(SessionError):
    """Session has been revoked."""
    
    def __init__(self, reason: Optional[str] = None):
        message = "Session has been revoked"
        details = {}
        if reason:
            details["reason"] = reason
        super().__init__(message, "session_revoked", details)


# Password errors
class WeakPasswordError(PasswordError):
    """Password does not meet security requirements."""
    
    def __init__(self, requirements: list):
        message = "Password does not meet security requirements"
        details = {"missing_requirements": requirements}
        super().__init__(message, "weak_password", details)


class PasswordReuseError(PasswordError):
    """Password has been used recently and cannot be reused."""
    
    def __init__(self, history_count: int):
        super().__init__(
            f"Password cannot be one of the last {history_count} passwords used",
            "password_reuse",
            {"history_count": history_count}
        )


class InvalidPasswordResetTokenError(PasswordError):
    """Password reset token is invalid or expired."""
    
    def __init__(self):
        super().__init__(
            "Password reset token is invalid or expired",
            "invalid_reset_token"
        )


# MFA errors
class InvalidMFACodeError(MFAError):
    """MFA code is invalid or expired."""
    
    def __init__(self, attempts_remaining: Optional[int] = None):
        message = "Multi-factor authentication code is invalid"
        details = {}
        if attempts_remaining is not None:
            details["attempts_remaining"] = attempts_remaining
        super().__init__(message, "invalid_mfa_code", details)


class MFANotSetupError(MFAError):
    """MFA is not set up for this user."""
    
    def __init__(self):
        super().__init__(
            "Multi-factor authentication is not set up",
            "mfa_not_setup"
        )


class MFAAlreadySetupError(MFAError):
    """MFA is already set up for this user."""
    
    def __init__(self):
        super().__init__(
            "Multi-factor authentication is already set up",
            "mfa_already_setup"
        )


class InvalidBackupCodeError(MFAError):
    """MFA backup code is invalid or already used."""
    
    def __init__(self):
        super().__init__(
            "Backup code is invalid or has already been used",
            "invalid_backup_code"
        )


# Token errors
class InvalidTokenError(TokenError):
    """Token is invalid, malformed, or expired."""
    
    def __init__(self, token_type: str = "token"):
        super().__init__(
            f"Invalid or expired {token_type}",
            "invalid_token",
            {"token_type": token_type}
        )


class TokenExpiredError(TokenError):
    """Token has expired."""
    
    def __init__(self, token_type: str = "token"):
        super().__init__(
            f"{token_type.title()} has expired",
            "token_expired",
            {"token_type": token_type}
        )


class TokenRevokedError(TokenError):
    """Token has been revoked."""
    
    def __init__(self, token_type: str = "token", reason: Optional[str] = None):
        message = f"{token_type.title()} has been revoked"
        details = {"token_type": token_type}
        if reason:
            details["reason"] = reason
        super().__init__(message, "token_revoked", details)


class InvalidAPIKeyError(TokenError):
    """API key is invalid or revoked."""
    
    def __init__(self):
        super().__init__(
            "Invalid or revoked API key",
            "invalid_api_key"
        )


# Security errors
class RateLimitExceededError(SecurityError):
    """Rate limit has been exceeded."""
    
    def __init__(self, retry_after: Optional[int] = None):
        message = "Rate limit exceeded"
        details = {}
        if retry_after:
            details["retry_after"] = retry_after
        super().__init__(message, "rate_limit_exceeded", details)


class SuspiciousActivityError(SecurityError):
    """Suspicious activity detected."""
    
    def __init__(self, risk_score: Optional[int] = None):
        message = "Suspicious activity detected"
        details = {}
        if risk_score:
            details["risk_score"] = risk_score
        super().__init__(message, "suspicious_activity", details)


class IPNotAllowedError(SecurityError):
    """IP address is not allowed to access this resource."""
    
    def __init__(self, ip_address: str):
        super().__init__(
            f"IP address {ip_address} is not allowed",
            "ip_not_allowed",
            {"ip_address": ip_address}
        )


class EmailVerificationRequiredError(SecurityError):
    """Email verification is required to proceed."""
    
    def __init__(self):
        super().__init__(
            "Email verification is required",
            "email_verification_required"
        )


class InvalidEmailVerificationTokenError(SecurityError):
    """Email verification token is invalid or expired."""
    
    def __init__(self):
        super().__init__(
            "Email verification token is invalid or expired",
            "invalid_email_verification_token"
        )


# User management errors
class UserNotFoundError(AuthError):
    """User does not exist."""
    
    def __init__(self, identifier: str, identifier_type: str = "username"):
        super().__init__(
            f"User not found: {identifier}",
            "user_not_found",
            {"identifier": identifier, "identifier_type": identifier_type}
        )


class UserAlreadyExistsError(AuthError):
    """User already exists."""
    
    def __init__(self, identifier: str, identifier_type: str = "username"):
        super().__init__(
            f"User already exists: {identifier}",
            "user_already_exists",
            {"identifier": identifier, "identifier_type": identifier_type}
        )


class EmailAlreadyExistsError(AuthError):
    """Email address is already registered."""
    
    def __init__(self, email: str):
        super().__init__(
            f"Email address {email} is already registered",
            "email_already_exists",
            {"email": email}
        )