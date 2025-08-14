"""
Authentication System Models

Pydantic models for authentication, authorization, and user management.
These models represent the database entities and API request/response objects.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from uuid import UUID
from enum import Enum
import ipaddress

from pydantic import BaseModel, Field, EmailStr, validator, root_validator
from pydantic.types import SecretStr

from ..core.pydantic_compat import computed_field


class UserStatus(str, Enum):
    """User account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    LOCKED = "locked"
    SUSPENDED = "suspended"


class SessionStatus(str, Enum):
    """Session status."""
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


class TokenType(str, Enum):
    """Token types."""
    ACCESS = "access"
    REFRESH = "refresh" 
    RESET = "reset"
    MFA_SESSION = "mfa_session"
    EMAIL_VERIFICATION = "email_verification"


class AuditEventCategory(str, Enum):
    """Audit event categories."""
    AUTH = "auth"
    SESSION = "session"
    USER = "user"
    ROLE = "role"
    SECURITY = "security"


# Base models
class BaseAuthModel(BaseModel):
    """Base model for authentication system entities."""
    
    class Config:
        from_attributes = True
        use_enum_values = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None,
            UUID: str,
        }


# User models
class UserBase(BaseAuthModel):
    """Base user model with common fields."""
    username: str = Field(..., min_length=3, max_length=255)
    email: EmailStr
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    display_name: Optional[str] = Field(None, max_length=200)
    timezone: str = Field(default="America/New_York", max_length=50)
    locale: str = Field(default="en-US", max_length=10)
    is_active: bool = Field(default=True)
    is_verified: bool = Field(default=False)


class UserCreate(UserBase):
    """Model for creating a new user."""
    password: SecretStr = Field(..., min_length=12)
    confirm_password: SecretStr = Field(..., min_length=12)
    
    @root_validator
    def passwords_match(cls, values):
        """Ensure password and confirm_password match."""
        password = values.get('password')
        confirm_password = values.get('confirm_password')
        
        if password and confirm_password and password.get_secret_value() != confirm_password.get_secret_value():
            raise ValueError('Passwords do not match')
        
        return values


class UserUpdate(BaseAuthModel):
    """Model for updating user information."""
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    display_name: Optional[str] = Field(None, max_length=200)
    timezone: Optional[str] = Field(None, max_length=50)
    locale: Optional[str] = Field(None, max_length=10)
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


class UserPasswordUpdate(BaseAuthModel):
    """Model for updating user password."""
    current_password: SecretStr
    new_password: SecretStr = Field(..., min_length=12)
    confirm_password: SecretStr = Field(..., min_length=12)
    
    @root_validator
    def passwords_match(cls, values):
        """Ensure new passwords match."""
        new_password = values.get('new_password')
        confirm_password = values.get('confirm_password')
        
        if new_password and confirm_password and new_password.get_secret_value() != confirm_password.get_secret_value():
            raise ValueError('New passwords do not match')
        
        return values


class User(UserBase):
    """Complete user model with all fields."""
    id: int
    uuid: UUID
    password_created_at: datetime
    password_updated_at: datetime
    is_locked: bool = Field(default=False)
    locked_until: Optional[datetime] = None
    failed_login_attempts: int = Field(default=0)
    last_failed_login: Optional[datetime] = None
    require_password_change: bool = Field(default=False)
    mfa_enabled: bool = Field(default=False)
    last_login: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Relationships
    roles: List["Role"] = Field(default_factory=list)
    preferences: Optional["UserPreferences"] = None
    sessions: List["Session"] = Field(default_factory=list)
    
    @computed_field
    @property
    def status(self) -> UserStatus:
        """Determine user status based on flags."""
        if not self.is_active:
            return UserStatus.INACTIVE
        elif self.is_locked:
            return UserStatus.LOCKED
        else:
            return UserStatus.ACTIVE
    
    @computed_field
    @property
    def full_name(self) -> str:
        """User's full name."""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        elif self.display_name:
            return self.display_name
        else:
            return self.username


# Role models
class RoleBase(BaseAuthModel):
    """Base role model."""
    name: str = Field(..., max_length=255)
    display_name: str = Field(..., max_length=255)
    description: Optional[str] = None
    permissions: List[str] = Field(default_factory=list)
    is_system_role: bool = Field(default=False)
    parent_role_id: Optional[int] = None


class RoleCreate(RoleBase):
    """Model for creating a new role."""
    pass


class RoleUpdate(BaseAuthModel):
    """Model for updating role information."""
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    permissions: Optional[List[str]] = None
    parent_role_id: Optional[int] = None


class Role(RoleBase):
    """Complete role model."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    # Computed fields
    @computed_field
    @property
    def permission_count(self) -> int:
        """Number of permissions assigned to role."""
        return len(self.permissions)


class UserRoleBase(BaseAuthModel):
    """Base user role assignment model."""
    user_id: int
    role_id: int
    effective_from: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    effective_until: Optional[datetime] = None
    is_active: bool = Field(default=True)


class UserRoleCreate(UserRoleBase):
    """Model for creating user role assignment."""
    pass


class UserRole(UserRoleBase):
    """Complete user role assignment model."""
    id: int
    assigned_by: Optional[int] = None
    assigned_at: datetime
    
    # Relationships
    user: Optional[User] = None
    role: Optional[Role] = None


# Session models
class SessionBase(BaseAuthModel):
    """Base session model."""
    user_id: int
    device_fingerprint: Optional[str] = Field(None, max_length=255)
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    location_country: Optional[str] = Field(None, max_length=2)
    location_city: Optional[str] = Field(None, max_length=100)
    is_mobile: bool = Field(default=False)
    is_trusted_device: bool = Field(default=False)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('ip_address')
    def validate_ip_address(cls, v):
        """Validate IP address format."""
        if v is not None:
            try:
                ipaddress.ip_address(v)
            except ValueError:
                raise ValueError('Invalid IP address format')
        return v


class SessionCreate(SessionBase):
    """Model for creating a new session."""
    pass


class Session(SessionBase):
    """Complete session model."""
    id: int
    session_id: UUID
    created_at: datetime
    last_activity: datetime
    expires_at: datetime
    revoked_at: Optional[datetime] = None
    revoked_by: Optional[int] = None
    revoked_reason: Optional[str] = Field(None, max_length=255)
    is_active: bool = Field(default=True)
    
    # Relationships
    user: Optional[User] = None
    
    @computed_field
    @property
    def status(self) -> SessionStatus:
        """Determine session status."""
        now = datetime.now(timezone.utc)
        
        if self.revoked_at:
            return SessionStatus.REVOKED
        elif self.expires_at < now:
            return SessionStatus.EXPIRED
        elif self.is_active:
            return SessionStatus.ACTIVE
        else:
            return SessionStatus.EXPIRED


# JWT Token models
class JWTTokenBase(BaseAuthModel):
    """Base JWT token model."""
    user_id: int
    session_id: Optional[UUID] = None
    token_type: TokenType
    audience: Optional[str] = Field(None, max_length=255)
    scopes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class JWTTokenCreate(JWTTokenBase):
    """Model for creating JWT token record."""
    token_hash: str = Field(..., max_length=255)
    expires_at: datetime


class JWTToken(JWTTokenBase):
    """Complete JWT token model."""
    id: int
    token_id: UUID
    token_hash: str
    issued_at: datetime
    expires_at: datetime
    revoked_at: Optional[datetime] = None
    revoked_reason: Optional[str] = Field(None, max_length=255)
    
    # Relationships
    user: Optional[User] = None
    session: Optional[Session] = None


# User preferences models
class UserPreferencesBase(BaseAuthModel):
    """Base user preferences model."""
    theme: str = Field(default="light", max_length=20)
    dashboard_layout: Dict[str, Any] = Field(default_factory=dict)
    notification_settings: Dict[str, Any] = Field(
        default_factory=lambda: {"email": True, "browser": True, "mobile": False}
    )
    default_timezone: str = Field(default="America/New_York", max_length=50)
    date_format: str = Field(default="MM/DD/YYYY", max_length=20)
    time_format: str = Field(default="12h", max_length=10)
    currency: str = Field(default="USD", max_length=3)
    advanced_features_enabled: bool = Field(default=False)
    beta_features_enabled: bool = Field(default=False)
    session_timeout_minutes: int = Field(default=60, ge=5, le=480)
    require_mfa_for_sensitive_actions: bool = Field(default=True)
    custom_settings: Dict[str, Any] = Field(default_factory=dict)


class UserPreferencesUpdate(UserPreferencesBase):
    """Model for updating user preferences."""
    pass


class UserPreferences(UserPreferencesBase):
    """Complete user preferences model."""
    id: int
    user_id: int
    created_at: datetime
    updated_at: datetime
    
    # Relationships
    user: Optional[User] = None


# API Key models
class APIKeyBase(BaseAuthModel):
    """Base API key model."""
    user_id: int
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    permissions: List[str] = Field(default_factory=list)
    rate_limit_per_hour: int = Field(default=1000, ge=1)
    allowed_ip_addresses: List[str] = Field(default_factory=list)
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('allowed_ip_addresses')
    def validate_ip_addresses(cls, v):
        """Validate IP addresses in allowed list."""
        for ip in v:
            try:
                if '/' in ip:
                    ipaddress.ip_network(ip, strict=False)
                else:
                    ipaddress.ip_address(ip)
            except ValueError:
                raise ValueError(f'Invalid IP address or network: {ip}')
        return v


class APIKeyCreate(APIKeyBase):
    """Model for creating API key."""
    pass


class APIKey(APIKeyBase):
    """Complete API key model."""
    id: int
    key_id: UUID
    key_hash: str
    key_prefix: str
    created_at: datetime
    last_used_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    revoked_by: Optional[int] = None
    is_active: bool = Field(default=True)
    usage_count: int = Field(default=0)
    
    # Relationships
    user: Optional[User] = None


# Password reset token models
class PasswordResetTokenCreate(BaseAuthModel):
    """Model for creating password reset token."""
    user_id: int
    email: EmailStr
    token_hash: str
    expires_at: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None


class PasswordResetToken(BaseAuthModel):
    """Complete password reset token model."""
    id: int
    token_id: UUID
    user_id: int
    token_hash: str
    email: str
    created_at: datetime
    expires_at: datetime
    used_at: Optional[datetime] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    attempts: int = Field(default=0)
    
    # Relationships
    user: Optional[User] = None


# Email verification token models
class EmailVerificationTokenCreate(BaseAuthModel):
    """Model for creating email verification token."""
    user_id: int
    email: EmailStr
    token_hash: str
    expires_at: datetime
    ip_address: Optional[str] = None


class EmailVerificationToken(BaseAuthModel):
    """Complete email verification token model."""
    id: int
    token_id: UUID
    user_id: int
    token_hash: str
    email: str
    created_at: datetime
    expires_at: datetime
    verified_at: Optional[datetime] = None
    ip_address: Optional[str] = None
    attempts: int = Field(default=0)
    
    # Relationships
    user: Optional[User] = None


# Audit log models
class AuditLogBase(BaseAuthModel):
    """Base audit log model."""
    user_id: Optional[int] = None
    session_id: Optional[UUID] = None
    event_type: str = Field(..., max_length=50)
    event_category: AuditEventCategory
    event_description: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    success: bool
    failure_reason: Optional[str] = Field(None, max_length=255)
    risk_score: Optional[int] = Field(None, ge=0, le=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AuditLogCreate(AuditLogBase):
    """Model for creating audit log entry."""
    pass


class AuditLog(AuditLogBase):
    """Complete audit log model."""
    id: int
    request_id: Optional[UUID] = None
    correlation_id: Optional[str] = Field(None, max_length=255)
    timestamp: datetime
    
    # Relationships
    user: Optional[User] = None
    session: Optional[Session] = None


# Authentication request/response models
class LoginRequest(BaseAuthModel):
    """Login request model."""
    username: str = Field(..., min_length=3, max_length=255)
    password: SecretStr = Field(..., min_length=1)
    remember_me: bool = Field(default=False)
    device_fingerprint: Optional[str] = Field(None, max_length=255)
    
    class Config:
        schema_extra = {
            "example": {
                "username": "john_doe",
                "password": "secure_password_123",
                "remember_me": False,
                "device_fingerprint": "unique_device_id"
            }
        }


class MFAVerificationRequest(BaseAuthModel):
    """MFA verification request model."""
    mfa_token: str = Field(..., description="MFA session token")
    code: str = Field(..., min_length=6, max_length=8, description="TOTP or backup code")
    remember_device: bool = Field(default=False)
    device_fingerprint: Optional[str] = Field(None, max_length=255)


class TokenResponse(BaseAuthModel):
    """Token response model."""
    access_token: str
    refresh_token: str
    token_type: str = Field(default="bearer")
    expires_in: int
    user: User
    
    class Config:
        schema_extra = {
            "example": {
                "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
                "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
                "token_type": "bearer",
                "expires_in": 900,
                "user": {
                    "id": 1,
                    "username": "john_doe",
                    "email": "john@example.com",
                    "full_name": "John Doe"
                }
            }
        }


class RefreshTokenRequest(BaseAuthModel):
    """Refresh token request model."""
    refresh_token: str


class PasswordResetRequest(BaseAuthModel):
    """Password reset request model."""
    email: EmailStr
    
    class Config:
        schema_extra = {
            "example": {
                "email": "user@example.com"
            }
        }


class PasswordResetConfirm(BaseAuthModel):
    """Password reset confirmation model."""
    token: str = Field(..., description="Password reset token")
    new_password: SecretStr = Field(..., min_length=12)
    confirm_password: SecretStr = Field(..., min_length=12)
    
    @root_validator
    def passwords_match(cls, values):
        """Ensure passwords match."""
        new_password = values.get('new_password')
        confirm_password = values.get('confirm_password')
        
        if new_password and confirm_password and new_password.get_secret_value() != confirm_password.get_secret_value():
            raise ValueError('Passwords do not match')
        
        return values


class MFASetupResponse(BaseAuthModel):
    """MFA setup response model."""
    secret: str = Field(..., description="Base32-encoded secret key")
    qr_code_url: str = Field(..., description="QR code URL for authenticator apps")
    backup_codes: List[str] = Field(..., description="One-time backup codes")
    
    class Config:
        schema_extra = {
            "example": {
                "secret": "JBSWY3DPEHPK3PXP",
                "qr_code_url": "otpauth://totp/MLB%20Betting%20Program:john_doe?secret=JBSWY3DPEHPK3PXP&issuer=MLB%20Betting%20Program",
                "backup_codes": [
                    "12345-67890",
                    "98765-43210"
                ]
            }
        }


# Permission and role response models
class PermissionCheck(BaseAuthModel):
    """Permission check model."""
    permission: str
    has_permission: bool
    required_roles: List[str] = Field(default_factory=list)


class UserPermissions(BaseAuthModel):
    """User permissions model."""
    user_id: int
    permissions: List[str]
    roles: List[str]
    effective_permissions: List[PermissionCheck] = Field(default_factory=list)


# Security models
class SecurityAuditResult(BaseAuthModel):
    """Security audit result model."""
    user_id: int
    risk_score: int = Field(..., ge=0, le=100)
    risk_level: str
    risk_factors: List[str]
    recommendations: List[str]
    timestamp: datetime
    
    class Config:
        schema_extra = {
            "example": {
                "user_id": 1,
                "risk_score": 25,
                "risk_level": "low",
                "risk_factors": ["Login from new device"],
                "recommendations": ["Monitor for additional suspicious activity"],
                "timestamp": "2025-01-15T12:00:00Z"
            }
        }


# Update forward references
User.model_rebuild()
Role.model_rebuild()
UserRole.model_rebuild()
Session.model_rebuild()
UserPreferences.model_rebuild()
APIKey.model_rebuild()
PasswordResetToken.model_rebuild()
EmailVerificationToken.model_rebuild()
AuditLog.model_rebuild()