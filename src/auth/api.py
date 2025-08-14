"""
Authentication API Endpoints

FastAPI endpoints for authentication, user management, and MFA operations.
"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials

from ..core.logging import get_logger, LogComponent
from .models import (
    User, UserCreate, UserUpdate, UserPasswordUpdate,
    LoginRequest, MFAVerificationRequest, TokenResponse, RefreshTokenRequest,
    PasswordResetRequest, PasswordResetConfirm,
    UserPreferencesUpdate, UserPreferences,
    MFASetupResponse, SecurityAuditResult
)
from .services import AuthenticationService
from .authorization import AuthorizationService
from .user_service import UserService
from .mfa_service import MFAService
from .middleware import (
    get_current_user, get_optional_user, require_permission,
    require_user_management, require_admin, security
)
from .exceptions import (
    AuthenticationError, AuthorizationError, UserNotFoundError,
    MFARequiredError, WeakPasswordError, InvalidTokenError,
    UserAlreadyExistsError, EmailAlreadyExistsError
)

logger = get_logger(__name__, LogComponent.AUTH)

# Create router
auth_router = APIRouter(prefix="/auth", tags=["Authentication"])
users_router = APIRouter(prefix="/users", tags=["User Management"])
mfa_router = APIRouter(prefix="/mfa", tags=["Multi-Factor Authentication"])


# Authentication endpoints
@auth_router.post("/login", response_model=TokenResponse)
async def login(
    login_request: LoginRequest,
    request: Request
) -> TokenResponse:
    """
    Authenticate user with username and password.
    
    Returns JWT tokens on successful authentication.
    May return MFA challenge if MFA is enabled.
    """
    try:
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        auth_service = AuthenticationService()
        
        return await auth_service.authenticate_user(
            login_request, client_ip, user_agent
        )
        
    except MFARequiredError as e:
        # Return special response for MFA requirement
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "mfa_required": True,
                "mfa_token": e.details.get("mfa_session_token"),
                "message": "Multi-factor authentication required"
            }
        )
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )
    except Exception as e:
        logger.error("Login error", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service unavailable"
        )


@auth_router.post("/mfa/verify", response_model=TokenResponse)
async def verify_mfa(
    mfa_request: MFAVerificationRequest,
    request: Request
) -> TokenResponse:
    """
    Complete authentication with MFA code verification.
    """
    try:
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        auth_service = AuthenticationService()
        
        return await auth_service.verify_mfa(
            mfa_request, client_ip, user_agent
        )
        
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )
    except Exception as e:
        logger.error("MFA verification error", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MFA verification failed"
        )


@auth_router.post("/refresh", response_model=TokenResponse)
async def refresh_tokens(
    refresh_request: RefreshTokenRequest
) -> TokenResponse:
    """
    Refresh access and refresh tokens using refresh token.
    """
    try:
        auth_service = AuthenticationService()
        
        return await auth_service.refresh_tokens(refresh_request.refresh_token)
        
    except InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )
    except Exception as e:
        logger.error("Token refresh error", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Token refresh failed"
        )


@auth_router.post("/logout")
async def logout(
    user: User = Depends(get_current_user),
    revoke_all: bool = False
) -> dict:
    """
    Logout user by revoking session tokens.
    
    Args:
        revoke_all: Whether to revoke all sessions or just current one
    """
    try:
        auth_service = AuthenticationService()
        
        success = await auth_service.logout_user(
            user.id,
            revoke_all_sessions=revoke_all
        )
        
        return {
            "message": "Logout successful",
            "revoked_all_sessions": revoke_all
        }
        
    except Exception as e:
        logger.error("Logout error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Logout failed"
        )


@auth_router.post("/password-reset/request")
async def request_password_reset(
    reset_request: PasswordResetRequest,
    request: Request
) -> dict:
    """
    Request password reset via email.
    """
    try:
        client_ip = request.client.host if request.client else "unknown"
        
        user_service = UserService()
        success = await user_service.request_password_reset(
            reset_request.email, client_ip
        )
        
        # Always return success to prevent email enumeration
        return {"message": "If the email exists, a password reset link has been sent"}
        
    except Exception as e:
        logger.error("Password reset request error", error=e)
        return {"message": "If the email exists, a password reset link has been sent"}


@auth_router.post("/password-reset/confirm")
async def confirm_password_reset(
    token: str,
    reset_data: PasswordResetConfirm,
    request: Request
) -> dict:
    """
    Confirm password reset with token.
    """
    try:
        client_ip = request.client.host if request.client else "unknown"
        
        user_service = UserService()
        success = await user_service.confirm_password_reset(
            token, reset_data, client_ip
        )
        
        return {"message": "Password reset successfully"}
        
    except WeakPasswordError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "weak_password", "violations": e.details.get("missing_requirements", [])}
        )
    except Exception as e:
        logger.error("Password reset confirmation error", error=e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password reset failed"
        )


# User management endpoints
@users_router.post("/", response_model=User)
async def create_user(
    user_data: UserCreate,
    current_user: User = Depends(require_user_management())
) -> User:
    """
    Create a new user account (admin only).
    """
    try:
        user_service = UserService()
        
        return await user_service.create_user(
            user_data, 
            created_by=current_user.id,
            auto_verify=True  # Admin-created users are auto-verified
        )
        
    except (UserAlreadyExistsError, EmailAlreadyExistsError) as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except WeakPasswordError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "weak_password", "violations": e.details.get("missing_requirements", [])}
        )
    except Exception as e:
        logger.error("User creation error", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User creation failed"
        )


@users_router.get("/me", response_model=User)
async def get_current_user_info(
    user: User = Depends(get_current_user)
) -> User:
    """Get current user information."""
    return user


@users_router.get("/{user_id}", response_model=User)
async def get_user(
    user_id: int,
    current_user: User = Depends(require_user_management())
) -> User:
    """Get user by ID (admin only)."""
    try:
        user_service = UserService()
        return await user_service.get_user_by_id(user_id)
        
    except UserNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@users_router.put("/me", response_model=User)
async def update_current_user(
    user_data: UserUpdate,
    user: User = Depends(get_current_user)
) -> User:
    """Update current user profile."""
    try:
        user_service = UserService()
        
        return await user_service.update_user(
            user.id, user_data, updated_by=user.id
        )
        
    except UserNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error("User update error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User update failed"
        )


@users_router.put("/me/password")
async def update_password(
    password_data: UserPasswordUpdate,
    user: User = Depends(get_current_user)
) -> dict:
    """Update current user password."""
    try:
        user_service = UserService()
        
        success = await user_service.update_password(user.id, password_data)
        
        return {"message": "Password updated successfully"}
        
    except WeakPasswordError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "weak_password", "violations": e.details.get("missing_requirements", [])}
        )
    except Exception as e:
        logger.error("Password update error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password update failed"
        )


@users_router.get("/me/preferences", response_model=UserPreferences)
async def get_user_preferences(
    user: User = Depends(get_current_user)
) -> UserPreferences:
    """Get current user preferences."""
    if user.preferences:
        return UserPreferences(**user.preferences)
    
    # Return default preferences if none set
    return UserPreferences(user_id=user.id)


@users_router.put("/me/preferences", response_model=UserPreferences)
async def update_user_preferences(
    preferences_data: UserPreferencesUpdate,
    user: User = Depends(get_current_user)
) -> UserPreferences:
    """Update current user preferences."""
    try:
        user_service = UserService()
        
        return await user_service.update_preferences(user.id, preferences_data)
        
    except Exception as e:
        logger.error("Preferences update error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Preferences update failed"
        )


# MFA endpoints
@mfa_router.post("/setup", response_model=MFASetupResponse)
async def setup_mfa(
    user: User = Depends(get_current_user)
) -> MFASetupResponse:
    """
    Setup MFA for current user.
    
    Returns secret key, QR code URL, and backup codes.
    """
    try:
        mfa_service = MFAService()
        
        return await mfa_service.setup_mfa(user.id)
        
    except Exception as e:
        logger.error("MFA setup error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MFA setup failed"
        )


@mfa_router.post("/enable")
async def enable_mfa(
    verification_code: str,
    user: User = Depends(get_current_user)
) -> dict:
    """
    Enable MFA after verifying setup code.
    """
    try:
        mfa_service = MFAService()
        
        success = await mfa_service.verify_and_enable_mfa(user.id, verification_code)
        
        return {"message": "MFA enabled successfully"}
        
    except Exception as e:
        logger.error("MFA enable error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MFA enable failed"
        )


@mfa_router.post("/disable")
async def disable_mfa(
    verification_code: str,
    user: User = Depends(get_current_user)
) -> dict:
    """
    Disable MFA after verifying code.
    """
    try:
        mfa_service = MFAService()
        
        success = await mfa_service.disable_mfa(user.id, verification_code)
        
        return {"message": "MFA disabled successfully"}
        
    except Exception as e:
        logger.error("MFA disable error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MFA disable failed"
        )


@mfa_router.post("/backup-codes/regenerate")
async def regenerate_backup_codes(
    verification_code: str,
    user: User = Depends(get_current_user)
) -> dict:
    """
    Regenerate MFA backup codes.
    """
    try:
        mfa_service = MFAService()
        
        backup_codes = await mfa_service.regenerate_backup_codes(
            user.id, verification_code
        )
        
        return {
            "message": "Backup codes regenerated",
            "backup_codes": backup_codes
        }
        
    except Exception as e:
        logger.error("Backup code regeneration error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Backup code regeneration failed"
        )


@mfa_router.get("/status")
async def get_mfa_status(
    user: User = Depends(get_current_user)
) -> dict:
    """
    Get MFA status for current user.
    """
    try:
        mfa_service = MFAService()
        
        return await mfa_service.get_mfa_status(user.id)
        
    except Exception as e:
        logger.error("MFA status error", error=e, extra={"user_id": user.id})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get MFA status"
        )


# Admin endpoints
@users_router.get("/", response_model=List[User])
async def list_users(
    current_user: User = Depends(require_user_management()),
    skip: int = 0,
    limit: int = 100
) -> List[User]:
    """
    List all users (admin only).
    """
    try:
        # TODO: Implement user listing with pagination
        # This would need to be implemented in UserService
        return []
        
    except Exception as e:
        logger.error("User listing error", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User listing failed"
        )


@auth_router.get("/security/audit", response_model=List[SecurityAuditResult])
async def get_security_audit(
    current_user: User = Depends(require_admin()),
    hours: int = 24,
    user_id: Optional[int] = None
) -> List[SecurityAuditResult]:
    """
    Get security audit information (super admin only).
    """
    try:
        # TODO: Implement security audit querying
        # This would query the audit_log table
        return []
        
    except Exception as e:
        logger.error("Security audit error", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Security audit failed"
        )


# Health check endpoint
@auth_router.get("/health")
async def auth_health_check():
    """Authentication system health check."""
    try:
        # TODO: Add actual health checks
        # - Database connectivity
        # - Token manager status
        # - MFA service status
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                "authentication": "operational",
                "authorization": "operational", 
                "mfa": "operational",
                "database": "operational"
            }
        }
        
    except Exception as e:
        logger.error("Auth health check failed", error=e)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": "Authentication service unavailable"
            }
        )


# Include all routers
def create_auth_routes() -> List[APIRouter]:
    """Create all authentication-related routers."""
    return [auth_router, users_router, mfa_router]