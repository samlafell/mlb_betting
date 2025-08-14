"""
Authentication Services

Core authentication and authorization services for user management,
session handling, password security, and multi-factor authentication.
"""

import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from uuid import UUID, uuid4
import asyncio
import ipaddress

from ..core.config import get_settings
from ..core.logging import get_logger, LogComponent
from ..data.database.connection import get_database_connection
from .models import (
    User, UserCreate, UserUpdate, UserPasswordUpdate,
    Role, Session, SessionCreate,
    UserRole, UserPreferences, UserPreferencesUpdate,
    APIKey, APIKeyCreate, AuditLog, AuditLogCreate,
    LoginRequest, MFAVerificationRequest, TokenResponse,
    PasswordResetRequest, PasswordResetConfirm,
    TokenType, AuditEventCategory, UserStatus
)
from .security import (
    PasswordHasher, PasswordValidator, JWTManager, MFAManager,
    SecurityValidator, APIKeyManager, TokenClaims, SecurityAssessment
)
from .exceptions import (
    AuthenticationError, AuthorizationError, UserNotFoundError,
    InvalidCredentialsError, AccountLockedError, AccountDisabledError,
    EmailNotVerifiedError, PasswordExpiredError, MFARequiredError,
    SessionExpiredError, SessionNotFoundError, TokenExpiredError,
    InvalidTokenError, WeakPasswordError, PasswordReuseError,
    InvalidMFACodeError, MFANotSetupError, MFAAlreadySetupError,
    InsufficientPermissionsError, EmailAlreadyExistsError,
    UserAlreadyExistsError, SuspiciousActivityError
)

logger = get_logger(__name__, LogComponent.AUTH)


class AuthenticationService:
    """Core authentication service handling login, logout, and session management."""
    
    def __init__(self):
        """Initialize authentication service."""
        self.password_hasher = PasswordHasher()
        self.password_validator = PasswordValidator()
        self.jwt_manager = JWTManager()
        self.mfa_manager = MFAManager()
        self.security_validator = SecurityValidator()
        self.settings = get_settings()
    
    async def authenticate_user(
        self,
        login_request: LoginRequest,
        ip_address: str,
        user_agent: str
    ) -> TokenResponse:
        """
        Authenticate user with username/password.
        
        Args:
            login_request: Login request with credentials
            ip_address: Client IP address
            user_agent: Client user agent
            
        Returns:
            TokenResponse with access/refresh tokens and user info
            
        Raises:
            Various authentication exceptions based on failure type
        """
        correlation_id = str(uuid4())
        
        try:
            # Find user by username
            user = await self._get_user_by_username(login_request.username)
            
            # Check account status
            await self._check_account_status(user, ip_address)
            
            # Verify password
            await self._verify_password(user, login_request.password.get_secret_value())
            
            # Assess login risk
            risk_assessment = await self._assess_login_risk(user, ip_address, user_agent)
            
            # Check if MFA is required
            if user.mfa_enabled or risk_assessment.risk_level in ['high', 'critical']:
                return await self._handle_mfa_required(
                    user, ip_address, user_agent, login_request.device_fingerprint
                )
            
            # Create session and tokens
            session = await self._create_user_session(
                user, ip_address, user_agent, login_request.device_fingerprint,
                login_request.remember_me
            )
            
            # Generate JWT tokens
            access_token, refresh_token = await self._generate_token_pair(user, session)
            
            # Update user login information
            await self._update_user_login_info(user.id, ip_address)
            
            # Log successful authentication
            await self._log_auth_event(
                user.id, session.session_id, "login_success", "auth",
                "User successfully authenticated", ip_address, user_agent, True,
                metadata={"risk_score": risk_assessment.risk_score}
            )
            
            logger.info(
                "User authentication successful",
                extra={
                    "user_id": user.id,
                    "username": user.username,
                    "session_id": str(session.session_id),
                    "ip_address": ip_address,
                    "risk_score": risk_assessment.risk_score,
                    "correlation_id": correlation_id
                }
            )
            
            return TokenResponse(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_in=15 * 60,  # 15 minutes
                user=user
            )
            
        except AuthenticationError:
            # Log failed authentication attempt
            user_id = user.id if 'user' in locals() else None
            await self._log_auth_event(
                user_id, None, "login_failed", "auth",
                f"Authentication failed for username: {login_request.username}",
                ip_address, user_agent, False,
                failure_reason="invalid_credentials"
            )
            
            # Increment failed attempts if user exists
            if 'user' in locals():
                await self._increment_failed_attempts(user.id)
            
            raise
            
        except Exception as e:
            logger.error(
                "Unexpected error during authentication",
                error=e,
                extra={
                    "username": login_request.username,
                    "ip_address": ip_address,
                    "correlation_id": correlation_id
                }
            )
            
            await self._log_auth_event(
                None, None, "login_error", "auth",
                "Unexpected authentication error", ip_address, user_agent, False,
                failure_reason="internal_error"
            )
            
            raise AuthenticationError("Authentication service temporarily unavailable")
    
    async def verify_mfa(
        self,
        mfa_request: MFAVerificationRequest,
        ip_address: str,
        user_agent: str
    ) -> TokenResponse:
        """
        Verify MFA code and complete authentication.
        
        Args:
            mfa_request: MFA verification request
            ip_address: Client IP address
            user_agent: Client user agent
            
        Returns:
            TokenResponse with access/refresh tokens
        """
        try:
            # Decode MFA session token
            mfa_claims = self.jwt_manager.decode_token(mfa_request.mfa_token)
            
            if mfa_claims.token_type != TokenType.MFA_SESSION:
                raise InvalidTokenError("Invalid MFA session token")
            
            # Get user
            user = await self._get_user_by_id(int(mfa_claims.sub))
            
            # Verify MFA code
            is_valid = await self._verify_mfa_code(user, mfa_request.code)
            
            if not is_valid:
                await self._log_auth_event(
                    user.id, None, "mfa_failed", "auth",
                    "Invalid MFA code provided", ip_address, user_agent, False
                )
                raise InvalidMFACodeError()
            
            # Create session and tokens
            session = await self._create_user_session(
                user, ip_address, user_agent, mfa_request.device_fingerprint,
                remember_device=mfa_request.remember_device
            )
            
            access_token, refresh_token = await self._generate_token_pair(user, session)
            
            # Update login info
            await self._update_user_login_info(user.id, ip_address)
            
            # Log successful MFA
            await self._log_auth_event(
                user.id, session.session_id, "mfa_success", "auth",
                "MFA verification successful", ip_address, user_agent, True
            )
            
            return TokenResponse(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_in=15 * 60,
                user=user
            )
            
        except Exception as e:
            if isinstance(e, AuthenticationError):
                raise
            
            logger.error("MFA verification error", error=e)
            raise AuthenticationError("MFA verification failed")
    
    async def refresh_tokens(self, refresh_token: str) -> TokenResponse:
        """
        Refresh access and refresh tokens.
        
        Args:
            refresh_token: Valid refresh token
            
        Returns:
            TokenResponse with new tokens
        """
        try:
            # Decode refresh token
            claims = self.jwt_manager.decode_token(refresh_token)
            
            if claims.token_type != TokenType.REFRESH:
                raise InvalidTokenError("Token is not a refresh token")
            
            # Verify token is not revoked
            await self._verify_token_not_revoked(claims.jti)
            
            # Get user and session
            user = await self._get_user_by_id(int(claims.sub))
            
            if claims.session_id:
                session = await self._get_session_by_id(UUID(claims.session_id))
                
                # Verify session is still active
                if not session.is_active or session.expires_at < datetime.now(timezone.utc):
                    raise SessionExpiredError()
            else:
                session = None
            
            # Generate new token pair
            new_access_token, new_refresh_token = await self._generate_token_pair(user, session)
            
            # Revoke old refresh token
            await self._revoke_token(claims.jti, "token_refreshed")
            
            logger.info(
                "Tokens refreshed successfully",
                extra={
                    "user_id": user.id,
                    "session_id": str(session.session_id) if session else None,
                    "old_jti": claims.jti
                }
            )
            
            return TokenResponse(
                access_token=new_access_token,
                refresh_token=new_refresh_token,
                expires_in=15 * 60,
                user=user
            )
            
        except TokenExpiredError:
            logger.info("Refresh token expired")
            raise
        except InvalidTokenError:
            logger.warning("Invalid refresh token provided")
            raise
        except Exception as e:
            logger.error("Token refresh error", error=e)
            raise AuthenticationError("Token refresh failed")
    
    async def logout_user(
        self,
        user_id: int,
        session_id: Optional[UUID] = None,
        revoke_all_sessions: bool = False
    ) -> bool:
        """
        Logout user by revoking session(s).
        
        Args:
            user_id: User ID
            session_id: Specific session to revoke (optional)
            revoke_all_sessions: Whether to revoke all user sessions
            
        Returns:
            True if logout successful
        """
        try:
            if revoke_all_sessions:
                await self._revoke_all_user_sessions(user_id, "user_logout")
                logger.info(f"All sessions revoked for user {user_id}")
            elif session_id:
                await self._revoke_session(session_id, "user_logout")
                logger.info(f"Session {session_id} revoked for user {user_id}")
            
            await self._log_auth_event(
                user_id, session_id, "logout", "auth",
                "User logged out successfully", None, None, True
            )
            
            return True
            
        except Exception as e:
            logger.error("Logout error", error=e, extra={"user_id": user_id})
            return False
    
    # Private helper methods
    async def _get_user_by_username(self, username: str) -> User:
        """Get user by username."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                """
                SELECT u.*, COALESCE(array_agg(r.name) FILTER (WHERE r.name IS NOT NULL), ARRAY[]::text[]) as role_names
                FROM auth.users u
                LEFT JOIN auth.user_roles ur ON u.id = ur.user_id AND ur.is_active = true
                LEFT JOIN auth.roles r ON ur.role_id = r.id
                WHERE u.username = $1
                GROUP BY u.id
                """,
                username
            )
            
            if not result:
                raise UserNotFoundError(username, "username")
            
            user_dict = dict(result)
            role_names = user_dict.pop('role_names', [])
            
            # Get user preferences
            prefs_result = await conn.fetchrow(
                "SELECT * FROM auth.user_preferences WHERE user_id = $1",
                result['id']
            )
            
            user_dict['preferences'] = dict(prefs_result) if prefs_result else None
            user_dict['roles'] = [{'name': name} for name in role_names if name]
            
            return User(**user_dict)
    
    async def _get_user_by_id(self, user_id: int) -> User:
        """Get user by ID."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                """
                SELECT u.*, COALESCE(array_agg(r.name) FILTER (WHERE r.name IS NOT NULL), ARRAY[]::text[]) as role_names
                FROM auth.users u
                LEFT JOIN auth.user_roles ur ON u.id = ur.user_id AND ur.is_active = true
                LEFT JOIN auth.roles r ON ur.role_id = r.id
                WHERE u.id = $1
                GROUP BY u.id
                """,
                user_id
            )
            
            if not result:
                raise UserNotFoundError(str(user_id), "user_id")
            
            user_dict = dict(result)
            role_names = user_dict.pop('role_names', [])
            user_dict['roles'] = [{'name': name} for name in role_names if name]
            
            return User(**user_dict)
    
    async def _check_account_status(self, user: User, ip_address: str) -> None:
        """Check if account is in valid state for login."""
        if not user.is_active:
            await self._log_auth_event(
                user.id, None, "login_blocked_inactive", "security",
                "Login blocked - account inactive", ip_address, None, False
            )
            raise AccountDisabledError()
        
        if not user.is_verified:
            await self._log_auth_event(
                user.id, None, "login_blocked_unverified", "security", 
                "Login blocked - email not verified", ip_address, None, False
            )
            raise EmailNotVerifiedError()
        
        if user.is_locked:
            locked_until = user.locked_until.isoformat() if user.locked_until else None
            await self._log_auth_event(
                user.id, None, "login_blocked_locked", "security",
                "Login blocked - account locked", ip_address, None, False
            )
            raise AccountLockedError(locked_until)
        
        # Check if password has expired
        if user.require_password_change:
            await self._log_auth_event(
                user.id, None, "login_blocked_password_expired", "security",
                "Login blocked - password change required", ip_address, None, False
            )
            raise PasswordExpiredError()
    
    async def _verify_password(self, user: User, password: str) -> None:
        """Verify user password."""
        # Get password hash and salt
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT password_hash, password_salt FROM auth.users WHERE id = $1",
                user.id
            )
        
        if not result or not self.password_hasher.verify_password(
            password, result['password_hash'], result['password_salt']
        ):
            raise InvalidCredentialsError()
    
    async def _assess_login_risk(
        self, user: User, ip_address: str, user_agent: str
    ) -> SecurityAssessment:
        """Assess login risk factors."""
        return self.security_validator.assess_login_risk(
            ip_address=ip_address,
            user_agent=user_agent,
            failed_attempts=user.failed_login_attempts,
            last_login_ip=None,  # TODO: Get from previous session
            is_known_device=False  # TODO: Check device recognition
        )
    
    async def _handle_mfa_required(
        self, user: User, ip_address: str, user_agent: str, device_fingerprint: Optional[str]
    ) -> TokenResponse:
        """Handle MFA requirement by creating MFA session token."""
        # Create MFA session token
        mfa_token = self.jwt_manager.create_token(
            user_id=str(user.id),
            token_type=TokenType.MFA_SESSION,
            metadata={"ip_address": ip_address, "user_agent": user_agent}
        )
        
        await self._log_auth_event(
            user.id, None, "mfa_required", "auth",
            "MFA verification required", ip_address, user_agent, True
        )
        
        # Return special response indicating MFA is required
        from .exceptions import MFARequiredError
        raise MFARequiredError(mfa_token)
    
    async def _verify_mfa_code(self, user: User, code: str) -> bool:
        """Verify MFA TOTP code or backup code."""
        if not user.mfa_enabled or not user.mfa_secret:
            raise MFANotSetupError()
        
        # First try TOTP code
        if self.mfa_manager.verify_totp_code(user.mfa_secret, code):
            return True
        
        # If TOTP fails, try backup codes
        if user.mfa_backup_codes:
            is_valid, used_code = self.mfa_manager.verify_backup_code(code, user.mfa_backup_codes)
            if is_valid:
                # Remove used backup code
                await self._remove_used_backup_code(user.id, used_code)
                return True
        
        return False
    
    async def _create_user_session(
        self,
        user: User,
        ip_address: str,
        user_agent: str,
        device_fingerprint: Optional[str],
        remember_me: bool = False
    ) -> Session:
        """Create new user session."""
        session_duration = timedelta(hours=24 if remember_me else 8)
        session_id = uuid4()
        
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                """
                INSERT INTO auth.sessions (
                    session_id, user_id, device_fingerprint, user_agent, ip_address,
                    expires_at, is_mobile, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *
                """,
                session_id,
                user.id,
                device_fingerprint,
                user_agent,
                ip_address,
                datetime.now(timezone.utc) + session_duration,
                'Mobile' in user_agent if user_agent else False,
                {"remember_me": remember_me}
            )
        
        return Session(**dict(result))
    
    async def _generate_token_pair(self, user: User, session: Optional[Session]) -> Tuple[str, str]:
        """Generate access and refresh token pair."""
        # Get user permissions
        permissions = await self._get_user_permissions(user.id)
        
        # Create access token
        access_token = self.jwt_manager.create_token(
            user_id=str(user.id),
            token_type=TokenType.ACCESS,
            session_id=str(session.session_id) if session else None,
            permissions=permissions
        )
        
        # Create refresh token
        refresh_token = self.jwt_manager.create_token(
            user_id=str(user.id),
            token_type=TokenType.REFRESH,
            session_id=str(session.session_id) if session else None,
            permissions=permissions
        )
        
        # Store token hashes in database
        await self._store_token_hashes(user.id, access_token, refresh_token, session)
        
        return access_token, refresh_token
    
    async def _get_user_permissions(self, user_id: int) -> List[str]:
        """Get all user permissions from roles."""
        async with get_database_connection() as conn:
            result = await conn.fetch(
                """
                SELECT DISTINCT jsonb_array_elements_text(r.permissions) as permission
                FROM auth.user_roles ur
                JOIN auth.roles r ON ur.role_id = r.id
                WHERE ur.user_id = $1 AND ur.is_active = true
                  AND (ur.effective_from IS NULL OR ur.effective_from <= NOW())
                  AND (ur.effective_until IS NULL OR ur.effective_until > NOW())
                """,
                user_id
            )
        
        return [row['permission'] for row in result]
    
    async def _store_token_hashes(
        self, user_id: int, access_token: str, refresh_token: str, session: Optional[Session]
    ) -> None:
        """Store token hashes in database."""
        access_hash = self.jwt_manager.get_token_hash(access_token)
        refresh_hash = self.jwt_manager.get_token_hash(refresh_token)
        
        access_claims = self.jwt_manager.decode_token(access_token, verify_expiry=False)
        refresh_claims = self.jwt_manager.decode_token(refresh_token, verify_expiry=False)
        
        async with get_database_connection() as conn:
            await conn.execute(
                """
                INSERT INTO auth.jwt_tokens (token_id, user_id, session_id, token_type, token_hash, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)
                """,
                UUID(access_claims.jti), user_id, session.session_id if session else None,
                TokenType.ACCESS.value, access_hash, access_claims.exp,
                UUID(refresh_claims.jti), user_id, session.session_id if session else None,
                TokenType.REFRESH.value, refresh_hash, refresh_claims.exp
            )
    
    async def _update_user_login_info(self, user_id: int, ip_address: str) -> None:
        """Update user last login information."""
        async with get_database_connection() as conn:
            await conn.execute(
                """
                UPDATE auth.users 
                SET last_login = NOW(), last_activity = NOW(), failed_login_attempts = 0
                WHERE id = $1
                """,
                user_id
            )
    
    async def _increment_failed_attempts(self, user_id: int) -> None:
        """Increment failed login attempts and potentially lock account."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                """
                UPDATE auth.users 
                SET failed_login_attempts = failed_login_attempts + 1,
                    last_failed_login = NOW()
                WHERE id = $1
                RETURNING failed_login_attempts
                """,
                user_id
            )
            
            if result and result['failed_login_attempts'] >= 5:
                # Lock account for 30 minutes
                await conn.execute(
                    """
                    UPDATE auth.users 
                    SET is_locked = true, locked_until = NOW() + INTERVAL '30 minutes'
                    WHERE id = $1
                    """,
                    user_id
                )
    
    async def _verify_token_not_revoked(self, jti: str) -> None:
        """Verify token has not been revoked."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT revoked_at FROM auth.jwt_tokens WHERE token_id = $1",
                UUID(jti)
            )
            
            if result and result['revoked_at']:
                raise InvalidTokenError("Token has been revoked")
    
    async def _revoke_token(self, jti: str, reason: str) -> None:
        """Revoke JWT token."""
        async with get_database_connection() as conn:
            await conn.execute(
                """
                UPDATE auth.jwt_tokens 
                SET revoked_at = NOW(), revoked_reason = $2
                WHERE token_id = $1
                """,
                UUID(jti), reason
            )
    
    async def _get_session_by_id(self, session_id: UUID) -> Session:
        """Get session by ID."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT * FROM auth.sessions WHERE session_id = $1",
                session_id
            )
            
            if not result:
                raise SessionNotFoundError()
            
            return Session(**dict(result))
    
    async def _revoke_session(self, session_id: UUID, reason: str) -> None:
        """Revoke user session."""
        async with get_database_connection() as conn:
            await conn.execute(
                """
                UPDATE auth.sessions 
                SET is_active = false, revoked_at = NOW(), revoked_reason = $2
                WHERE session_id = $1
                """,
                session_id, reason
            )
    
    async def _revoke_all_user_sessions(self, user_id: int, reason: str) -> None:
        """Revoke all user sessions."""
        async with get_database_connection() as conn:
            await conn.execute(
                """
                UPDATE auth.sessions 
                SET is_active = false, revoked_at = NOW(), revoked_reason = $3
                WHERE user_id = $1 AND is_active = true
                """,
                user_id, reason
            )
    
    async def _remove_used_backup_code(self, user_id: int, used_code: str) -> None:
        """Remove used MFA backup code."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT mfa_backup_codes FROM auth.users WHERE id = $1",
                user_id
            )
            
            if result and result['mfa_backup_codes']:
                updated_codes = [code for code in result['mfa_backup_codes'] if code != used_code]
                
                await conn.execute(
                    "UPDATE auth.users SET mfa_backup_codes = $2 WHERE id = $1",
                    user_id, updated_codes
                )
    
    async def _log_auth_event(
        self,
        user_id: Optional[int],
        session_id: Optional[UUID],
        event_type: str,
        event_category: str,
        event_description: Optional[str],
        ip_address: Optional[str],
        user_agent: Optional[str],
        success: bool,
        failure_reason: Optional[str] = None,
        risk_score: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log authentication event to audit log."""
        try:
            async with get_database_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, session_id, event_type, event_category, event_description,
                        ip_address, user_agent, success, failure_reason, risk_score, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    user_id, session_id, event_type, event_category, event_description,
                    ip_address, user_agent, success, failure_reason, risk_score,
                    metadata or {}
                )
        except Exception as e:
            logger.error("Failed to log auth event", error=e)