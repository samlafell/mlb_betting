"""
User Management Service

Comprehensive user management including creation, updates, profile management,
password management, and user preferences.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
import secrets

from ..core.config import get_settings
from ..core.logging import get_logger, LogComponent
from ..data.database.connection import get_database_connection
from .models import (
    User, UserCreate, UserUpdate, UserPasswordUpdate,
    UserPreferences, UserPreferencesUpdate,
    PasswordResetRequest, PasswordResetConfirm,
    EmailVerificationToken, PasswordResetToken,
    AuditEventCategory
)
from .security import (
    PasswordHasher, PasswordValidator, JWTManager, TokenType
)
from .exceptions import (
    UserNotFoundError, UserAlreadyExistsError, EmailAlreadyExistsError,
    WeakPasswordError, PasswordReuseError, InvalidPasswordResetTokenError,
    InvalidEmailVerificationTokenError
)

logger = get_logger(__name__, LogComponent.AUTH)


class UserService:
    """User management and profile service."""
    
    def __init__(self):
        """Initialize user service."""
        self.password_hasher = PasswordHasher()
        self.password_validator = PasswordValidator()
        self.jwt_manager = JWTManager()
        self.settings = get_settings()
    
    async def create_user(
        self,
        user_data: UserCreate,
        created_by: Optional[int] = None,
        auto_verify: bool = False
    ) -> User:
        """
        Create a new user account.
        
        Args:
            user_data: User creation data
            created_by: ID of user creating this account
            auto_verify: Whether to automatically verify email
            
        Returns:
            Created User object
        """
        try:
            # Validate password strength
            is_valid, violations = self.password_validator.validate_password(
                user_data.password.get_secret_value(),
                user_data.username
            )
            
            if not is_valid:
                raise WeakPasswordError(violations)
            
            # Check if username or email already exists
            await self._check_user_uniqueness(user_data.username, user_data.email)
            
            # Hash password
            password_hash, password_salt = self.password_hasher.hash_password(
                user_data.password.get_secret_value()
            )
            
            async with get_database_connection() as conn:
                async with conn.transaction():
                    # Create user
                    user_result = await conn.fetchrow(
                        """
                        INSERT INTO auth.users (
                            username, email, password_hash, password_salt,
                            first_name, last_name, display_name, timezone, locale,
                            is_active, is_verified, created_by
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        RETURNING *
                        """,
                        user_data.username,
                        user_data.email,
                        password_hash,
                        password_salt,
                        user_data.first_name,
                        user_data.last_name,
                        user_data.display_name or f"{user_data.first_name} {user_data.last_name}".strip(),
                        user_data.timezone,
                        user_data.locale,
                        user_data.is_active,
                        auto_verify or user_data.is_verified,
                        created_by
                    )
                    
                    user_id = user_result['id']
                    
                    # Create default user preferences
                    await conn.execute(
                        """
                        INSERT INTO auth.user_preferences (user_id)
                        VALUES ($1)
                        """,
                        user_id
                    )
                    
                    # Assign default role (viewer)
                    default_role = await conn.fetchrow(
                        "SELECT id FROM auth.roles WHERE name = 'viewer'"
                    )
                    
                    if default_role:
                        await conn.execute(
                            """
                            INSERT INTO auth.user_roles (user_id, role_id, assigned_by)
                            VALUES ($1, $2, $3)
                            """,
                            user_id, default_role['id'], created_by
                        )
                    
                    # Add initial password to history
                    await conn.execute(
                        """
                        INSERT INTO auth.password_history (user_id, password_hash, password_salt)
                        VALUES ($1, $2, $3)
                        """,
                        user_id, password_hash, password_salt
                    )
                    
                    # Log user creation
                    await conn.execute(
                        """
                        INSERT INTO auth.audit_log (
                            user_id, event_type, event_category, event_description, success
                        ) VALUES ($1, $2, $3, $4, $5)
                        """,
                        user_id, "user_created", "user", "User account created", True
                    )
            
            # Get complete user object
            user = await self.get_user_by_id(user_id)
            
            # Send verification email if not auto-verified
            if not auto_verify:
                await self._send_email_verification(user)
            
            logger.info(
                "User created successfully",
                extra={
                    "user_id": user_id,
                    "username": user_data.username,
                    "email": user_data.email,
                    "created_by": created_by,
                    "auto_verified": auto_verify
                }
            )
            
            return user
            
        except (UserAlreadyExistsError, EmailAlreadyExistsError, WeakPasswordError):
            raise
        except Exception as e:
            logger.error("User creation failed", error=e)
            raise Exception("User creation failed")
    
    async def get_user_by_id(self, user_id: int) -> User:
        """
        Get user by ID with complete information.
        
        Args:
            user_id: User ID
            
        Returns:
            User object
        """
        async with get_database_connection() as conn:
            # Get user with roles
            user_result = await conn.fetchrow(
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
            
            if not user_result:
                raise UserNotFoundError(str(user_id), "user_id")
            
            # Get user preferences
            prefs_result = await conn.fetchrow(
                "SELECT * FROM auth.user_preferences WHERE user_id = $1",
                user_id
            )
            
            # Build user object
            user_dict = dict(user_result)
            role_names = user_dict.pop('role_names', [])
            user_dict['roles'] = [{'name': name} for name in role_names if name]
            user_dict['preferences'] = dict(prefs_result) if prefs_result else None
            
            return User(**user_dict)
    
    async def get_user_by_username(self, username: str) -> User:
        """Get user by username."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT id FROM auth.users WHERE username = $1",
                username
            )
            
            if not result:
                raise UserNotFoundError(username, "username")
            
            return await self.get_user_by_id(result['id'])
    
    async def get_user_by_email(self, email: str) -> User:
        """Get user by email."""
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT id FROM auth.users WHERE email = $1",
                email
            )
            
            if not result:
                raise UserNotFoundError(email, "email")
            
            return await self.get_user_by_id(result['id'])
    
    async def update_user(
        self,
        user_id: int,
        user_data: UserUpdate,
        updated_by: Optional[int] = None
    ) -> User:
        """
        Update user information.
        
        Args:
            user_id: User ID to update
            user_data: Updated user data
            updated_by: ID of user making the update
            
        Returns:
            Updated User object
        """
        try:
            # Verify user exists
            await self.get_user_by_id(user_id)
            
            # Build dynamic update query
            update_fields = []
            values = []
            param_count = 1
            
            if user_data.first_name is not None:
                update_fields.append(f"first_name = ${param_count}")
                values.append(user_data.first_name)
                param_count += 1
            
            if user_data.last_name is not None:
                update_fields.append(f"last_name = ${param_count}")
                values.append(user_data.last_name)
                param_count += 1
            
            if user_data.display_name is not None:
                update_fields.append(f"display_name = ${param_count}")
                values.append(user_data.display_name)
                param_count += 1
            
            if user_data.timezone is not None:
                update_fields.append(f"timezone = ${param_count}")
                values.append(user_data.timezone)
                param_count += 1
            
            if user_data.locale is not None:
                update_fields.append(f"locale = ${param_count}")
                values.append(user_data.locale)
                param_count += 1
            
            if user_data.metadata is not None:
                update_fields.append(f"metadata = ${param_count}")
                values.append(user_data.metadata)
                param_count += 1
            
            if update_fields:
                update_fields.append("updated_at = NOW()")
                values.append(user_id)
                
                query = f"""
                    UPDATE auth.users 
                    SET {', '.join(update_fields)}
                    WHERE id = ${param_count}
                """
                
                async with get_database_connection() as conn:
                    await conn.execute(query, *values)
                
                # Log update
                await self._log_user_event(
                    user_id, "user_updated", "User profile updated", True,
                    metadata={"updated_by": updated_by, "fields_updated": len(update_fields) - 1}
                )
            
            logger.info(
                "User updated successfully",
                extra={
                    "user_id": user_id,
                    "updated_by": updated_by,
                    "fields_updated": len(update_fields)
                }
            )
            
            return await self.get_user_by_id(user_id)
            
        except UserNotFoundError:
            raise
        except Exception as e:
            logger.error("User update failed", error=e, extra={"user_id": user_id})
            raise Exception("User update failed")
    
    async def update_password(
        self,
        user_id: int,
        password_data: UserPasswordUpdate
    ) -> bool:
        """
        Update user password with validation.
        
        Args:
            user_id: User ID
            password_data: Password update data
            
        Returns:
            True if password was updated
        """
        try:
            # Get user and current password
            async with get_database_connection() as conn:
                user_result = await conn.fetchrow(
                    "SELECT password_hash, password_salt FROM auth.users WHERE id = $1",
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                # Verify current password
                if not self.password_hasher.verify_password(
                    password_data.current_password.get_secret_value(),
                    user_result['password_hash'],
                    user_result['password_salt']
                ):
                    raise Exception("Current password is incorrect")
                
                # Validate new password strength
                user = await self.get_user_by_id(user_id)
                is_valid, violations = self.password_validator.validate_password(
                    password_data.new_password.get_secret_value(),
                    user.username
                )
                
                if not is_valid:
                    raise WeakPasswordError(violations)
                
                # Check password history (prevent reuse)
                await self._check_password_history(
                    user_id, password_data.new_password.get_secret_value()
                )
                
                # Hash new password
                new_hash, new_salt = self.password_hasher.hash_password(
                    password_data.new_password.get_secret_value()
                )
                
                async with conn.transaction():
                    # Update password
                    await conn.execute(
                        """
                        UPDATE auth.users 
                        SET password_hash = $2, password_salt = $3, 
                            password_updated_at = NOW(), updated_at = NOW(),
                            require_password_change = false
                        WHERE id = $1
                        """,
                        user_id, new_hash, new_salt
                    )
                    
                    # Add to password history
                    await conn.execute(
                        """
                        INSERT INTO auth.password_history (user_id, password_hash, password_salt)
                        VALUES ($1, $2, $3)
                        """,
                        user_id, new_hash, new_salt
                    )
                    
                    # Clean up old password history (keep last 5)
                    await conn.execute(
                        """
                        DELETE FROM auth.password_history 
                        WHERE user_id = $1 
                          AND id NOT IN (
                              SELECT id FROM auth.password_history 
                              WHERE user_id = $1 
                              ORDER BY created_at DESC 
                              LIMIT 5
                          )
                        """,
                        user_id
                    )
                    
                    # Revoke all existing sessions (force re-login)
                    await conn.execute(
                        """
                        UPDATE auth.sessions 
                        SET is_active = false, revoked_at = NOW(), revoked_reason = 'password_changed'
                        WHERE user_id = $1 AND is_active = true
                        """,
                        user_id
                    )
                    
                    # Log password change
                    await conn.execute(
                        """
                        INSERT INTO auth.audit_log (
                            user_id, event_type, event_category, event_description, success
                        ) VALUES ($1, $2, $3, $4, $5)
                        """,
                        user_id, "password_changed", "security", "User password changed", True
                    )
            
            logger.info(
                "Password updated successfully",
                extra={"user_id": user_id}
            )
            
            return True
            
        except (UserNotFoundError, WeakPasswordError, PasswordReuseError):
            raise
        except Exception as e:
            logger.error("Password update failed", error=e, extra={"user_id": user_id})
            await self._log_user_event(
                user_id, "password_change_failed", "Password change failed", False,
                failure_reason=str(e)
            )
            raise Exception("Password update failed")
    
    async def update_preferences(
        self,
        user_id: int,
        preferences_data: UserPreferencesUpdate
    ) -> UserPreferences:
        """
        Update user preferences.
        
        Args:
            user_id: User ID
            preferences_data: Updated preferences
            
        Returns:
            Updated UserPreferences object
        """
        try:
            # Verify user exists
            await self.get_user_by_id(user_id)
            
            # Build dynamic update query
            update_fields = []
            values = []
            param_count = 1
            
            for field_name in preferences_data.model_fields.keys():
                field_value = getattr(preferences_data, field_name)
                if field_value is not None:
                    update_fields.append(f"{field_name} = ${param_count}")
                    values.append(field_value)
                    param_count += 1
            
            if update_fields:
                update_fields.append("updated_at = NOW()")
                values.append(user_id)
                
                query = f"""
                    UPDATE auth.user_preferences 
                    SET {', '.join(update_fields)}
                    WHERE user_id = ${param_count}
                    RETURNING *
                """
                
                async with get_database_connection() as conn:
                    result = await conn.fetchrow(query, *values)
                    
                    if result:
                        logger.info(
                            "User preferences updated",
                            extra={"user_id": user_id, "fields_updated": len(update_fields) - 1}
                        )
                        return UserPreferences(**dict(result))
            
            # If no updates, return existing preferences
            async with get_database_connection() as conn:
                result = await conn.fetchrow(
                    "SELECT * FROM auth.user_preferences WHERE user_id = $1",
                    user_id
                )
                
                if result:
                    return UserPreferences(**dict(result))
                else:
                    # Create default preferences if none exist
                    result = await conn.fetchrow(
                        """
                        INSERT INTO auth.user_preferences (user_id)
                        VALUES ($1)
                        RETURNING *
                        """,
                        user_id
                    )
                    return UserPreferences(**dict(result))
                    
        except UserNotFoundError:
            raise
        except Exception as e:
            logger.error("Preferences update failed", error=e, extra={"user_id": user_id})
            raise Exception("Preferences update failed")
    
    async def request_password_reset(self, email: str, ip_address: Optional[str] = None) -> bool:
        """
        Request password reset for user.
        
        Args:
            email: User's email address
            ip_address: Client IP address
            
        Returns:
            True if reset email was sent
        """
        try:
            # Find user by email
            user = await self.get_user_by_email(email)
            
            # Generate reset token
            reset_token = secrets.token_urlsafe(32)
            token_hash = self.jwt_manager.get_token_hash(reset_token)
            expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
            
            async with get_database_connection() as conn:
                # Clean up any existing reset tokens
                await conn.execute(
                    "DELETE FROM auth.password_reset_tokens WHERE user_id = $1",
                    user.id
                )
                
                # Create new reset token
                await conn.fetchrow(
                    """
                    INSERT INTO auth.password_reset_tokens (
                        user_id, email, token_hash, expires_at, ip_address
                    ) VALUES ($1, $2, $3, $4, $5)
                    RETURNING token_id
                    """,
                    user.id, email, token_hash, expires_at, ip_address
                )
                
                # Log password reset request
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description, 
                        ip_address, success
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    user.id, "password_reset_requested", "security",
                    "Password reset requested", ip_address, True
                )
            
            # TODO: Send reset email
            logger.info(
                "Password reset requested",
                extra={"user_id": user.id, "email": email, "ip_address": ip_address}
            )
            
            return True
            
        except UserNotFoundError:
            # Don't reveal if email exists or not
            logger.warning(f"Password reset requested for unknown email: {email}")
            return True
        except Exception as e:
            logger.error("Password reset request failed", error=e)
            return False
    
    async def confirm_password_reset(
        self,
        token: str,
        reset_data: PasswordResetConfirm,
        ip_address: Optional[str] = None
    ) -> bool:
        """
        Confirm password reset with token.
        
        Args:
            token: Password reset token
            reset_data: New password data
            ip_address: Client IP address
            
        Returns:
            True if password was reset
        """
        try:
            token_hash = self.jwt_manager.get_token_hash(token)
            
            async with get_database_connection() as conn:
                # Find valid reset token
                token_result = await conn.fetchrow(
                    """
                    SELECT user_id, email, attempts FROM auth.password_reset_tokens
                    WHERE token_hash = $1 AND expires_at > NOW() AND used_at IS NULL
                    """,
                    token_hash
                )
                
                if not token_result:
                    raise InvalidPasswordResetTokenError()
                
                # Check attempts limit
                if token_result['attempts'] >= 3:
                    raise InvalidPasswordResetTokenError()
                
                user_id = token_result['user_id']
                user = await self.get_user_by_id(user_id)
                
                # Validate new password
                is_valid, violations = self.password_validator.validate_password(
                    reset_data.new_password.get_secret_value(),
                    user.username
                )
                
                if not is_valid:
                    # Increment attempts
                    await conn.execute(
                        """
                        UPDATE auth.password_reset_tokens 
                        SET attempts = attempts + 1 
                        WHERE token_hash = $1
                        """,
                        token_hash
                    )
                    raise WeakPasswordError(violations)
                
                # Check password history
                await self._check_password_history(
                    user_id, reset_data.new_password.get_secret_value()
                )
                
                # Hash new password
                new_hash, new_salt = self.password_hasher.hash_password(
                    reset_data.new_password.get_secret_value()
                )
                
                async with conn.transaction():
                    # Update password
                    await conn.execute(
                        """
                        UPDATE auth.users 
                        SET password_hash = $2, password_salt = $3, 
                            password_updated_at = NOW(), updated_at = NOW(),
                            require_password_change = false, failed_login_attempts = 0
                        WHERE id = $1
                        """,
                        user_id, new_hash, new_salt
                    )
                    
                    # Mark token as used
                    await conn.execute(
                        """
                        UPDATE auth.password_reset_tokens 
                        SET used_at = NOW() 
                        WHERE token_hash = $1
                        """,
                        token_hash
                    )
                    
                    # Add to password history
                    await conn.execute(
                        """
                        INSERT INTO auth.password_history (user_id, password_hash, password_salt)
                        VALUES ($1, $2, $3)
                        """,
                        user_id, new_hash, new_salt
                    )
                    
                    # Revoke all sessions
                    await conn.execute(
                        """
                        UPDATE auth.sessions 
                        SET is_active = false, revoked_at = NOW(), revoked_reason = 'password_reset'
                        WHERE user_id = $1 AND is_active = true
                        """,
                        user_id
                    )
                    
                    # Log password reset
                    await conn.execute(
                        """
                        INSERT INTO auth.audit_log (
                            user_id, event_type, event_category, event_description, 
                            ip_address, success
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        user_id, "password_reset_completed", "security",
                        "Password reset via token", ip_address, True
                    )
            
            logger.info(
                "Password reset completed",
                extra={"user_id": user_id, "ip_address": ip_address}
            )
            
            return True
            
        except (InvalidPasswordResetTokenError, WeakPasswordError, PasswordReuseError):
            raise
        except Exception as e:
            logger.error("Password reset confirmation failed", error=e)
            raise Exception("Password reset failed")
    
    # Private helper methods
    async def _check_user_uniqueness(self, username: str, email: str) -> None:
        """Check if username and email are unique."""
        async with get_database_connection() as conn:
            # Check username
            username_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM auth.users WHERE username = $1)",
                username
            )
            
            if username_exists:
                raise UserAlreadyExistsError(username, "username")
            
            # Check email
            email_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM auth.users WHERE email = $1)",
                email
            )
            
            if email_exists:
                raise EmailAlreadyExistsError(email)
    
    async def _check_password_history(self, user_id: int, new_password: str) -> None:
        """Check if password has been used recently."""
        async with get_database_connection() as conn:
            # Get recent password hashes
            history_results = await conn.fetch(
                """
                SELECT password_hash, password_salt FROM auth.password_history
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT 5
                """,
                user_id
            )
            
            # Check if new password matches any recent password
            for history_row in history_results:
                if self.password_hasher.verify_password(
                    new_password, history_row['password_hash'], history_row['password_salt']
                ):
                    raise PasswordReuseError(5)
    
    async def _send_email_verification(self, user: User) -> None:
        """Send email verification to user."""
        # TODO: Implement email verification
        logger.info(f"Email verification needed for user {user.id}")
    
    async def _log_user_event(
        self,
        user_id: int,
        event_type: str,
        description: str,
        success: bool,
        failure_reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log user-related event."""
        try:
            async with get_database_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description,
                        success, failure_reason, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    user_id, event_type, "user", description,
                    success, failure_reason, metadata or {}
                )
        except Exception as e:
            logger.error("Failed to log user event", error=e)