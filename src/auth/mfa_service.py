"""
Multi-Factor Authentication Service

Comprehensive MFA implementation with TOTP, backup codes, and device trust management.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import secrets
import qrcode
from io import BytesIO
import base64

from ..core.config import get_settings
from ..core.logging import get_logger, LogComponent
from ..data.database.connection import get_database_connection
from .models import User, MFASetupResponse
from .security import MFAManager
from .exceptions import (
    UserNotFoundError, MFANotSetupError, MFAAlreadySetupError,
    InvalidMFACodeError, InvalidBackupCodeError
)

logger = get_logger(__name__, LogComponent.AUTH)


class MFAService:
    """Multi-factor authentication management service."""
    
    def __init__(self):
        """Initialize MFA service."""
        self.mfa_manager = MFAManager()
        self.settings = get_settings()
    
    async def setup_mfa(self, user_id: int) -> MFASetupResponse:
        """
        Setup MFA for user.
        
        Args:
            user_id: User ID
            
        Returns:
            MFASetupResponse with secret, QR code, and backup codes
            
        Raises:
            UserNotFoundError: If user doesn't exist
            MFAAlreadySetupError: If MFA is already enabled
        """
        try:
            # Get user information
            async with get_database_connection() as conn:
                user_result = await conn.fetchrow(
                    "SELECT username, email, mfa_enabled FROM auth.users WHERE id = $1",
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                if user_result['mfa_enabled']:
                    raise MFAAlreadySetupError()
                
                # Generate MFA secret
                secret = self.mfa_manager.generate_secret()
                
                # Generate QR code URL
                qr_code_url = self.mfa_manager.generate_qr_code_url(
                    secret, user_result['username']
                )
                
                # Generate backup codes
                backup_codes = self.mfa_manager.generate_backup_codes(8)
                hashed_backup_codes = self.mfa_manager.hash_backup_codes(backup_codes)
                
                # Store MFA secret and backup codes (but don't enable yet)
                await conn.execute(
                    """
                    UPDATE auth.users 
                    SET mfa_secret = $2, mfa_backup_codes = $3, updated_at = NOW()
                    WHERE id = $1
                    """,
                    user_id, secret, hashed_backup_codes
                )
                
                # Log MFA setup initiation
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description, success
                    ) VALUES ($1, $2, $3, $4, $5)
                    """,
                    user_id, "mfa_setup_initiated", "security", 
                    "MFA setup process initiated", True
                )
            
            logger.info(
                "MFA setup initiated",
                extra={
                    "user_id": user_id,
                    "username": user_result['username']
                }
            )
            
            return MFASetupResponse(
                secret=secret,
                qr_code_url=qr_code_url,
                backup_codes=backup_codes
            )
            
        except (UserNotFoundError, MFAAlreadySetupError):
            raise
        except Exception as e:
            logger.error("MFA setup failed", error=e, extra={"user_id": user_id})
            raise Exception("MFA setup failed")
    
    async def verify_and_enable_mfa(
        self,
        user_id: int,
        verification_code: str
    ) -> bool:
        """
        Verify MFA setup and enable it for the user.
        
        Args:
            user_id: User ID
            verification_code: TOTP code for verification
            
        Returns:
            True if MFA was enabled successfully
            
        Raises:
            UserNotFoundError: If user doesn't exist
            MFANotSetupError: If MFA setup wasn't initiated
            InvalidMFACodeError: If verification code is invalid
        """
        try:
            async with get_database_connection() as conn:
                # Get user MFA information
                user_result = await conn.fetchrow(
                    """
                    SELECT mfa_enabled, mfa_secret FROM auth.users 
                    WHERE id = $1
                    """,
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                if user_result['mfa_enabled']:
                    logger.info(f"MFA already enabled for user {user_id}")
                    return True
                
                if not user_result['mfa_secret']:
                    raise MFANotSetupError()
                
                # Verify the TOTP code
                if not self.mfa_manager.verify_totp_code(
                    user_result['mfa_secret'], verification_code
                ):
                    await conn.execute(
                        """
                        INSERT INTO auth.audit_log (
                            user_id, event_type, event_category, event_description, success
                        ) VALUES ($1, $2, $3, $4, $5)
                        """,
                        user_id, "mfa_verification_failed", "security",
                        "MFA verification failed during setup", False
                    )
                    raise InvalidMFACodeError()
                
                # Enable MFA
                await conn.execute(
                    """
                    UPDATE auth.users 
                    SET mfa_enabled = true, updated_at = NOW()
                    WHERE id = $1
                    """,
                    user_id
                )
                
                # Log MFA enablement
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description, success
                    ) VALUES ($1, $2, $3, $4, $5)
                    """,
                    user_id, "mfa_enabled", "security", 
                    "MFA successfully enabled", True
                )
            
            logger.info(
                "MFA enabled successfully",
                extra={"user_id": user_id}
            )
            
            return True
            
        except (UserNotFoundError, MFANotSetupError, InvalidMFACodeError):
            raise
        except Exception as e:
            logger.error("MFA verification failed", error=e, extra={"user_id": user_id})
            raise Exception("MFA verification failed")
    
    async def disable_mfa(
        self,
        user_id: int,
        verification_code: str,
        disabled_by: Optional[int] = None
    ) -> bool:
        """
        Disable MFA for user after verification.
        
        Args:
            user_id: User ID
            verification_code: TOTP code or backup code for verification
            disabled_by: ID of user disabling MFA (for admin actions)
            
        Returns:
            True if MFA was disabled
        """
        try:
            async with get_database_connection() as conn:
                # Get user MFA information
                user_result = await conn.fetchrow(
                    """
                    SELECT mfa_enabled, mfa_secret, mfa_backup_codes FROM auth.users 
                    WHERE id = $1
                    """,
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                if not user_result['mfa_enabled']:
                    logger.info(f"MFA already disabled for user {user_id}")
                    return True
                
                # Verify code (unless disabled by admin)
                if disabled_by != user_id and disabled_by is not None:
                    # Admin disabling - check admin permissions would be done at API level
                    pass
                else:
                    # User disabling - verify their MFA code
                    code_valid = False
                    
                    # Try TOTP first
                    if user_result['mfa_secret']:
                        code_valid = self.mfa_manager.verify_totp_code(
                            user_result['mfa_secret'], verification_code
                        )
                    
                    # Try backup codes if TOTP failed
                    if not code_valid and user_result['mfa_backup_codes']:
                        code_valid, used_code = self.mfa_manager.verify_backup_code(
                            verification_code, user_result['mfa_backup_codes']
                        )
                        
                        if code_valid:
                            # Remove used backup code
                            updated_codes = [
                                code for code in user_result['mfa_backup_codes'] 
                                if code != used_code
                            ]
                            await conn.execute(
                                "UPDATE auth.users SET mfa_backup_codes = $2 WHERE id = $1",
                                user_id, updated_codes
                            )
                    
                    if not code_valid:
                        await conn.execute(
                            """
                            INSERT INTO auth.audit_log (
                                user_id, event_type, event_category, event_description, success
                            ) VALUES ($1, $2, $3, $4, $5)
                            """,
                            user_id, "mfa_disable_failed", "security",
                            "MFA disable failed - invalid code", False
                        )
                        raise InvalidMFACodeError()
                
                # Disable MFA
                await conn.execute(
                    """
                    UPDATE auth.users 
                    SET mfa_enabled = false, mfa_secret = NULL, mfa_backup_codes = NULL,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    user_id
                )
                
                # Log MFA disabling
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description, 
                        success, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    user_id, "mfa_disabled", "security", 
                    "MFA disabled", True,
                    {"disabled_by": disabled_by} if disabled_by else {}
                )
            
            logger.info(
                "MFA disabled successfully",
                extra={
                    "user_id": user_id,
                    "disabled_by": disabled_by
                }
            )
            
            return True
            
        except (UserNotFoundError, InvalidMFACodeError):
            raise
        except Exception as e:
            logger.error("MFA disable failed", error=e, extra={"user_id": user_id})
            raise Exception("MFA disable failed")
    
    async def verify_mfa_code(
        self,
        user_id: int,
        code: str,
        consume_backup_code: bool = True
    ) -> bool:
        """
        Verify MFA code for authentication.
        
        Args:
            user_id: User ID
            code: TOTP code or backup code
            consume_backup_code: Whether to consume backup code if used
            
        Returns:
            True if code is valid
        """
        try:
            async with get_database_connection() as conn:
                # Get user MFA information
                user_result = await conn.fetchrow(
                    """
                    SELECT mfa_enabled, mfa_secret, mfa_backup_codes FROM auth.users 
                    WHERE id = $1
                    """,
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                if not user_result['mfa_enabled']:
                    raise MFANotSetupError()
                
                # Try TOTP first
                if user_result['mfa_secret']:
                    if self.mfa_manager.verify_totp_code(user_result['mfa_secret'], code):
                        await self._log_mfa_verification(user_id, "totp", True)
                        return True
                
                # Try backup codes
                if user_result['mfa_backup_codes']:
                    is_valid, used_code = self.mfa_manager.verify_backup_code(
                        code, user_result['mfa_backup_codes']
                    )
                    
                    if is_valid:
                        if consume_backup_code:
                            # Remove used backup code
                            updated_codes = [
                                c for c in user_result['mfa_backup_codes'] 
                                if c != used_code
                            ]
                            await conn.execute(
                                "UPDATE auth.users SET mfa_backup_codes = $2 WHERE id = $1",
                                user_id, updated_codes
                            )
                        
                        await self._log_mfa_verification(user_id, "backup_code", True)
                        return True
                
                # Code is invalid
                await self._log_mfa_verification(user_id, "unknown", False)
                return False
                
        except (UserNotFoundError, MFANotSetupError):
            raise
        except Exception as e:
            logger.error("MFA code verification failed", error=e, extra={"user_id": user_id})
            return False
    
    async def regenerate_backup_codes(
        self,
        user_id: int,
        verification_code: str
    ) -> List[str]:
        """
        Regenerate backup codes for user.
        
        Args:
            user_id: User ID
            verification_code: TOTP code for verification
            
        Returns:
            List of new backup codes
        """
        try:
            async with get_database_connection() as conn:
                # Get user MFA information
                user_result = await conn.fetchrow(
                    """
                    SELECT mfa_enabled, mfa_secret FROM auth.users 
                    WHERE id = $1
                    """,
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                if not user_result['mfa_enabled'] or not user_result['mfa_secret']:
                    raise MFANotSetupError()
                
                # Verify TOTP code
                if not self.mfa_manager.verify_totp_code(
                    user_result['mfa_secret'], verification_code
                ):
                    raise InvalidMFACodeError()
                
                # Generate new backup codes
                backup_codes = self.mfa_manager.generate_backup_codes(8)
                hashed_backup_codes = self.mfa_manager.hash_backup_codes(backup_codes)
                
                # Update backup codes
                await conn.execute(
                    """
                    UPDATE auth.users 
                    SET mfa_backup_codes = $2, updated_at = NOW()
                    WHERE id = $1
                    """,
                    user_id, hashed_backup_codes
                )
                
                # Log backup code regeneration
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description, success
                    ) VALUES ($1, $2, $3, $4, $5)
                    """,
                    user_id, "mfa_backup_codes_regenerated", "security",
                    "MFA backup codes regenerated", True
                )
            
            logger.info(
                "MFA backup codes regenerated",
                extra={"user_id": user_id}
            )
            
            return backup_codes
            
        except (UserNotFoundError, MFANotSetupError, InvalidMFACodeError):
            raise
        except Exception as e:
            logger.error("Backup code regeneration failed", error=e, extra={"user_id": user_id})
            raise Exception("Backup code regeneration failed")
    
    async def get_mfa_status(self, user_id: int) -> Dict[str, Any]:
        """
        Get MFA status for user.
        
        Args:
            user_id: User ID
            
        Returns:
            Dictionary with MFA status information
        """
        try:
            async with get_database_connection() as conn:
                user_result = await conn.fetchrow(
                    """
                    SELECT mfa_enabled, mfa_secret IS NOT NULL as has_secret,
                           CASE WHEN mfa_backup_codes IS NOT NULL 
                                THEN array_length(mfa_backup_codes, 1) 
                                ELSE 0 
                           END as backup_codes_count
                    FROM auth.users 
                    WHERE id = $1
                    """,
                    user_id
                )
                
                if not user_result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                return {
                    "user_id": user_id,
                    "mfa_enabled": user_result['mfa_enabled'],
                    "has_secret": user_result['has_secret'],
                    "backup_codes_remaining": user_result['backup_codes_count'] or 0,
                    "setup_required": user_result['has_secret'] and not user_result['mfa_enabled']
                }
                
        except UserNotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to get MFA status", error=e, extra={"user_id": user_id})
            raise Exception("Failed to get MFA status")
    
    async def check_mfa_requirement(self, user_id: int) -> bool:
        """
        Check if user is required to use MFA.
        
        Args:
            user_id: User ID
            
        Returns:
            True if MFA is required for this user
        """
        try:
            async with get_database_connection() as conn:
                # Check user's roles for MFA requirements
                result = await conn.fetchrow(
                    """
                    SELECT u.mfa_enabled,
                           bool_or(r.name IN ('super_admin', 'admin')) as has_admin_role
                    FROM auth.users u
                    LEFT JOIN auth.user_roles ur ON u.id = ur.user_id AND ur.is_active = true
                    LEFT JOIN auth.roles r ON ur.role_id = r.id
                    WHERE u.id = $1
                    GROUP BY u.id, u.mfa_enabled
                    """,
                    user_id
                )
                
                if not result:
                    raise UserNotFoundError(str(user_id), "user_id")
                
                # Admin roles require MFA by default
                if result['has_admin_role']:
                    return True
                
                # Check global MFA requirement setting
                # This could be configured in security settings
                return False
                
        except UserNotFoundError:
            raise
        except Exception as e:
            logger.error("Failed to check MFA requirement", error=e, extra={"user_id": user_id})
            return False
    
    async def generate_qr_code_image(self, secret: str, username: str) -> str:
        """
        Generate QR code image as base64 string.
        
        Args:
            secret: MFA secret
            username: Username
            
        Returns:
            Base64 encoded QR code image
        """
        try:
            # Generate QR code URL
            qr_url = self.mfa_manager.generate_qr_code_url(secret, username)
            
            # Create QR code
            qr = qrcode.QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=10,
                border=4,
            )
            qr.add_data(qr_url)
            qr.make(fit=True)
            
            # Create image
            img = qr.make_image(fill_color="black", back_color="white")
            
            # Convert to base64
            buffer = BytesIO()
            img.save(buffer, format='PNG')
            buffer.seek(0)
            
            img_base64 = base64.b64encode(buffer.getvalue()).decode()
            return f"data:image/png;base64,{img_base64}"
            
        except Exception as e:
            logger.error("QR code generation failed", error=e)
            raise Exception("QR code generation failed")
    
    async def _log_mfa_verification(
        self, user_id: int, method: str, success: bool
    ) -> None:
        """Log MFA verification attempt."""
        try:
            async with get_database_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO auth.audit_log (
                        user_id, event_type, event_category, event_description, 
                        success, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    user_id, "mfa_verification", "security",
                    f"MFA verification attempt using {method}",
                    success, {"method": method}
                )
        except Exception as e:
            logger.error("Failed to log MFA verification", error=e)