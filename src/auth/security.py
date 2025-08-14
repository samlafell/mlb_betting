"""
Authentication Security Components

Core security functionality for password hashing, JWT tokens, validation,
and risk assessment.
"""

import secrets
import hashlib
import hmac
import pyotp
import jwt
import bcrypt
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import re
import ipaddress

from ..core.config import get_settings
from ..core.logging import get_logger, LogComponent
from .exceptions import (
    WeakPasswordError,
    TokenError,
    InvalidTokenError,
    TokenExpiredError,
    SecurityError,
    SuspiciousActivityError,
)

logger = get_logger(__name__, LogComponent.SECURITY)


class TokenType(Enum):
    """JWT token types."""
    ACCESS = "access"
    REFRESH = "refresh"
    RESET = "reset"
    MFA_SESSION = "mfa_session"
    EMAIL_VERIFICATION = "email_verification"


@dataclass
class TokenClaims:
    """JWT token claims structure."""
    sub: str  # Subject (user ID)
    token_type: TokenType
    exp: datetime  # Expiration
    iat: datetime  # Issued at
    jti: str  # JWT ID
    session_id: Optional[str] = None
    permissions: Optional[List[str]] = None
    audience: Optional[str] = None
    scopes: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SecurityAssessment:
    """Security risk assessment result."""
    risk_score: int  # 0-100
    risk_level: str  # low, medium, high, critical
    risk_factors: List[str]
    recommendations: List[str]
    allow_action: bool


class PasswordHasher:
    """Secure password hashing using bcrypt with salt."""
    
    def __init__(self, rounds: int = 12):
        """
        Initialize password hasher.
        
        Args:
            rounds: Bcrypt cost factor (4-31, higher = more secure but slower)
        """
        self.rounds = max(4, min(31, rounds))
    
    def hash_password(self, password: str) -> Tuple[str, str]:
        """
        Hash password with salt.
        
        Args:
            password: Plain text password
            
        Returns:
            Tuple of (password_hash, salt)
        """
        # Generate salt
        salt = bcrypt.gensalt(rounds=self.rounds)
        
        # Hash password with salt
        password_bytes = password.encode('utf-8')
        hashed = bcrypt.hashpw(password_bytes, salt)
        
        # Return base64-encoded hash and salt
        password_hash = hashed.decode('utf-8')
        salt_str = salt.decode('utf-8')
        
        logger.debug(
            "Password hashed successfully",
            extra={
                "rounds": self.rounds,
                "salt_length": len(salt_str),
                "hash_length": len(password_hash)
            }
        )
        
        return password_hash, salt_str
    
    def verify_password(self, password: str, password_hash: str, salt: str = None) -> bool:
        """
        Verify password against hash.
        
        Args:
            password: Plain text password to verify
            password_hash: Stored password hash
            salt: Salt (for backward compatibility, not used with bcrypt)
            
        Returns:
            True if password matches
        """
        try:
            password_bytes = password.encode('utf-8')
            hash_bytes = password_hash.encode('utf-8')
            
            result = bcrypt.checkpw(password_bytes, hash_bytes)
            
            logger.debug(
                "Password verification completed",
                extra={"verification_result": result}
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "Password verification failed",
                error=e,
                extra={"error_type": type(e).__name__}
            )
            return False
    
    def needs_rehash(self, password_hash: str) -> bool:
        """
        Check if password hash needs to be updated (due to changed cost factor).
        
        Args:
            password_hash: Current password hash
            
        Returns:
            True if hash should be updated
        """
        try:
            hash_bytes = password_hash.encode('utf-8')
            
            # Extract cost factor from hash
            current_rounds = bcrypt.gensalt().decode('utf-8').split('$')[2]
            hash_rounds = hash_bytes.decode('utf-8').split('$')[2]
            
            return int(current_rounds) != int(hash_rounds)
            
        except Exception as e:
            logger.warning(
                "Could not determine if password hash needs rehash",
                error=e
            )
            return False


class PasswordValidator:
    """Password strength validation and policy enforcement."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize password validator with security policy.
        
        Args:
            config: Password policy configuration
        """
        settings = get_settings()
        
        # Default configuration
        self.min_length = config.get('min_length', 12) if config else 12
        self.require_uppercase = config.get('require_uppercase', True) if config else True
        self.require_lowercase = config.get('require_lowercase', True) if config else True
        self.require_numbers = config.get('require_numbers', True) if config else True
        self.require_special_chars = config.get('require_special_chars', True) if config else True
        self.forbidden_patterns = config.get('forbidden_patterns', []) if config else []
        
        # Common weak password patterns
        self.weak_patterns = [
            r'(.)\1{3,}',  # Repeated characters (aaaa, 1111)
            r'(?i)(password|admin|user|test|login)',  # Common words
            r'(123|abc|qwe)',  # Common sequences
            r'(?i)(company|mlb|betting)',  # Domain-specific words
        ]
        
        self.special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
    
    def validate_password(self, password: str, username: str = None) -> Tuple[bool, List[str]]:
        """
        Validate password strength and policy compliance.
        
        Args:
            password: Password to validate
            username: Username to check for similarity
            
        Returns:
            Tuple of (is_valid, list_of_violations)
        """
        violations = []
        
        # Check length
        if len(password) < self.min_length:
            violations.append(f"Password must be at least {self.min_length} characters long")
        
        # Check character requirements
        if self.require_uppercase and not re.search(r'[A-Z]', password):
            violations.append("Password must contain at least one uppercase letter")
        
        if self.require_lowercase and not re.search(r'[a-z]', password):
            violations.append("Password must contain at least one lowercase letter")
        
        if self.require_numbers and not re.search(r'\d', password):
            violations.append("Password must contain at least one number")
        
        if self.require_special_chars and not any(c in self.special_chars for c in password):
            violations.append("Password must contain at least one special character")
        
        # Check for weak patterns
        for pattern in self.weak_patterns:
            if re.search(pattern, password):
                violations.append("Password contains weak or common patterns")
                break
        
        # Check for forbidden patterns
        for pattern in self.forbidden_patterns:
            if re.search(pattern, password, re.IGNORECASE):
                violations.append("Password contains forbidden content")
                break
        
        # Check similarity to username
        if username and len(username) > 3:
            if username.lower() in password.lower():
                violations.append("Password must not contain username")
        
        is_valid = len(violations) == 0
        
        logger.debug(
            "Password validation completed",
            extra={
                "is_valid": is_valid,
                "violation_count": len(violations),
                "password_length": len(password)
            }
        )
        
        return is_valid, violations
    
    def get_password_strength_score(self, password: str) -> int:
        """
        Calculate password strength score (0-100).
        
        Args:
            password: Password to score
            
        Returns:
            Strength score from 0 (very weak) to 100 (very strong)
        """
        score = 0
        
        # Length bonus (up to 30 points)
        length_score = min(30, len(password) * 2)
        score += length_score
        
        # Character variety bonus (up to 40 points)
        variety_score = 0
        if re.search(r'[a-z]', password):
            variety_score += 10
        if re.search(r'[A-Z]', password):
            variety_score += 10
        if re.search(r'\d', password):
            variety_score += 10
        if any(c in self.special_chars for c in password):
            variety_score += 10
        
        score += variety_score
        
        # Uniqueness bonus (up to 30 points)
        unique_chars = len(set(password))
        uniqueness_score = min(30, unique_chars * 2)
        score += uniqueness_score
        
        # Penalty for weak patterns
        for pattern in self.weak_patterns:
            if re.search(pattern, password):
                score -= 20
                break
        
        # Ensure score is between 0 and 100
        score = max(0, min(100, score))
        
        return score


class JWTManager:
    """JWT token management with security features."""
    
    def __init__(self):
        """Initialize JWT manager with configuration."""
        settings = get_settings()
        
        # Get secret key from settings (should be set in environment)
        self.secret_key = getattr(settings.security, 'jwt_secret_key', None) or secrets.token_urlsafe(64)
        self.algorithm = 'HS256'
        
        # Token durations
        self.access_token_duration = timedelta(minutes=15)
        self.refresh_token_duration = timedelta(days=30)
        self.reset_token_duration = timedelta(hours=1)
        self.mfa_session_duration = timedelta(minutes=5)
        self.email_verification_duration = timedelta(hours=24)
        
        if not hasattr(settings.security, 'jwt_secret_key') or not settings.security.jwt_secret_key:
            logger.warning(
                "JWT secret key not configured, using generated key (not persistent across restarts)",
                extra={"generated_key_length": len(self.secret_key)}
            )
    
    def create_token(
        self,
        user_id: str,
        token_type: TokenType,
        session_id: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        audience: Optional[str] = None,
        scopes: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        custom_expiry: Optional[timedelta] = None
    ) -> str:
        """
        Create JWT token with specified claims.
        
        Args:
            user_id: User identifier
            token_type: Type of token
            session_id: Session identifier
            permissions: User permissions
            audience: Token audience
            scopes: Token scopes
            metadata: Additional metadata
            custom_expiry: Custom expiration time
            
        Returns:
            Encoded JWT token
        """
        now = datetime.now(timezone.utc)
        
        # Determine expiration based on token type
        if custom_expiry:
            expires_at = now + custom_expiry
        elif token_type == TokenType.ACCESS:
            expires_at = now + self.access_token_duration
        elif token_type == TokenType.REFRESH:
            expires_at = now + self.refresh_token_duration
        elif token_type == TokenType.RESET:
            expires_at = now + self.reset_token_duration
        elif token_type == TokenType.MFA_SESSION:
            expires_at = now + self.mfa_session_duration
        elif token_type == TokenType.EMAIL_VERIFICATION:
            expires_at = now + self.email_verification_duration
        else:
            expires_at = now + self.access_token_duration
        
        # Create token claims
        claims = {
            'sub': str(user_id),
            'token_type': token_type.value,
            'iat': now,
            'exp': expires_at,
            'jti': secrets.token_urlsafe(32),
        }
        
        # Add optional claims
        if session_id:
            claims['session_id'] = session_id
        if permissions:
            claims['permissions'] = permissions
        if audience:
            claims['aud'] = audience
        if scopes:
            claims['scopes'] = scopes
        if metadata:
            claims['metadata'] = metadata
        
        # Encode token
        token = jwt.encode(claims, self.secret_key, algorithm=self.algorithm)
        
        logger.debug(
            "JWT token created",
            extra={
                "user_id": user_id,
                "token_type": token_type.value,
                "session_id": session_id,
                "expires_at": expires_at.isoformat(),
                "jti": claims['jti']
            }
        )
        
        return token
    
    def decode_token(self, token: str, verify_expiry: bool = True) -> TokenClaims:
        """
        Decode and validate JWT token.
        
        Args:
            token: JWT token to decode
            verify_expiry: Whether to verify token expiration
            
        Returns:
            TokenClaims object with decoded claims
            
        Raises:
            InvalidTokenError: If token is invalid or malformed
            TokenExpiredError: If token has expired
        """
        try:
            # Decode token
            options = {"verify_exp": verify_expiry}
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                options=options
            )
            
            # Create TokenClaims object
            token_claims = TokenClaims(
                sub=payload['sub'],
                token_type=TokenType(payload['token_type']),
                exp=datetime.fromtimestamp(payload['exp'], timezone.utc),
                iat=datetime.fromtimestamp(payload['iat'], timezone.utc),
                jti=payload['jti'],
                session_id=payload.get('session_id'),
                permissions=payload.get('permissions'),
                audience=payload.get('aud'),
                scopes=payload.get('scopes'),
                metadata=payload.get('metadata')
            )
            
            logger.debug(
                "JWT token decoded successfully",
                extra={
                    "user_id": token_claims.sub,
                    "token_type": token_claims.token_type.value,
                    "jti": token_claims.jti
                }
            )
            
            return token_claims
            
        except jwt.ExpiredSignatureError:
            logger.warning("JWT token expired", extra={"token_prefix": token[:8]})
            raise TokenExpiredError("JWT token")
            
        except jwt.InvalidTokenError as e:
            logger.warning(
                "Invalid JWT token",
                error=e,
                extra={"token_prefix": token[:8], "error_type": type(e).__name__}
            )
            raise InvalidTokenError("JWT token")
    
    def refresh_token(self, refresh_token: str) -> Tuple[str, str]:
        """
        Create new access and refresh tokens using refresh token.
        
        Args:
            refresh_token: Valid refresh token
            
        Returns:
            Tuple of (new_access_token, new_refresh_token)
            
        Raises:
            InvalidTokenError: If refresh token is invalid
            TokenExpiredError: If refresh token has expired
        """
        # Decode refresh token
        claims = self.decode_token(refresh_token)
        
        if claims.token_type != TokenType.REFRESH:
            raise InvalidTokenError("Token is not a refresh token")
        
        # Create new tokens
        new_access_token = self.create_token(
            user_id=claims.sub,
            token_type=TokenType.ACCESS,
            session_id=claims.session_id,
            permissions=claims.permissions,
            audience=claims.audience,
            scopes=claims.scopes,
            metadata=claims.metadata
        )
        
        new_refresh_token = self.create_token(
            user_id=claims.sub,
            token_type=TokenType.REFRESH,
            session_id=claims.session_id,
            permissions=claims.permissions,
            audience=claims.audience,
            scopes=claims.scopes,
            metadata=claims.metadata
        )
        
        logger.info(
            "JWT tokens refreshed",
            extra={
                "user_id": claims.sub,
                "session_id": claims.session_id,
                "old_jti": claims.jti
            }
        )
        
        return new_access_token, new_refresh_token
    
    def get_token_hash(self, token: str) -> str:
        """
        Generate SHA-256 hash of token for storage.
        
        Args:
            token: JWT token
            
        Returns:
            Hexadecimal hash string
        """
        return hashlib.sha256(token.encode()).hexdigest()


class MFAManager:
    """Multi-factor authentication management."""
    
    def __init__(self):
        """Initialize MFA manager."""
        self.issuer_name = "MLB Betting Program"
    
    def generate_secret(self) -> str:
        """
        Generate MFA secret key.
        
        Returns:
            Base32-encoded secret key
        """
        return pyotp.random_base32()
    
    def generate_qr_code_url(self, secret: str, username: str) -> str:
        """
        Generate QR code URL for MFA setup.
        
        Args:
            secret: MFA secret key
            username: User's username
            
        Returns:
            QR code URL for authenticator apps
        """
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(
            name=username,
            issuer_name=self.issuer_name
        )
    
    def verify_totp_code(self, secret: str, code: str, valid_window: int = 1) -> bool:
        """
        Verify TOTP code.
        
        Args:
            secret: MFA secret key
            code: TOTP code to verify
            valid_window: Number of time windows to allow (default: 1)
            
        Returns:
            True if code is valid
        """
        try:
            totp = pyotp.TOTP(secret)
            return totp.verify(code, valid_window=valid_window)
        except Exception as e:
            logger.error("TOTP verification failed", error=e)
            return False
    
    def generate_backup_codes(self, count: int = 8) -> List[str]:
        """
        Generate MFA backup codes.
        
        Args:
            count: Number of backup codes to generate
            
        Returns:
            List of backup codes
        """
        return [
            f"{secrets.randbelow(100000):05d}-{secrets.randbelow(100000):05d}"
            for _ in range(count)
        ]
    
    def hash_backup_codes(self, codes: List[str]) -> List[str]:
        """
        Hash backup codes for secure storage.
        
        Args:
            codes: Plain text backup codes
            
        Returns:
            List of hashed backup codes
        """
        return [hashlib.sha256(code.encode()).hexdigest() for code in codes]
    
    def verify_backup_code(self, code: str, hashed_codes: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Verify backup code against hashed codes.
        
        Args:
            code: Backup code to verify
            hashed_codes: List of hashed backup codes
            
        Returns:
            Tuple of (is_valid, matching_hash)
        """
        code_hash = hashlib.sha256(code.encode()).hexdigest()
        
        for hashed_code in hashed_codes:
            if hmac.compare_digest(code_hash, hashed_code):
                return True, hashed_code
        
        return False, None


class SecurityValidator:
    """Security validation and risk assessment."""
    
    def __init__(self):
        """Initialize security validator."""
        self.max_login_attempts = 5
        self.lockout_duration = timedelta(minutes=30)
        self.password_age_limit = timedelta(days=90)
    
    def validate_ip_address(self, ip_address: str, allowed_ranges: List[str] = None) -> bool:
        """
        Validate IP address against allowed ranges.
        
        Args:
            ip_address: IP address to validate
            allowed_ranges: List of allowed CIDR ranges
            
        Returns:
            True if IP is allowed
        """
        if not allowed_ranges:
            return True
        
        try:
            client_ip = ipaddress.ip_address(ip_address)
            
            for allowed_range in allowed_ranges:
                try:
                    if "/" in allowed_range:
                        network = ipaddress.ip_network(allowed_range, strict=False)
                        if client_ip in network:
                            return True
                    else:
                        allowed_ip = ipaddress.ip_address(allowed_range)
                        if client_ip == allowed_ip:
                            return True
                except ValueError:
                    logger.warning(f"Invalid IP range in allowed list: {allowed_range}")
                    continue
            
            return False
            
        except ValueError:
            logger.warning(f"Invalid IP address: {ip_address}")
            return False
    
    def assess_login_risk(
        self,
        ip_address: str,
        user_agent: str,
        failed_attempts: int,
        last_login_ip: Optional[str] = None,
        last_login_location: Optional[str] = None,
        is_known_device: bool = False
    ) -> SecurityAssessment:
        """
        Assess login risk based on multiple factors.
        
        Args:
            ip_address: Client IP address
            user_agent: Client user agent
            failed_attempts: Number of recent failed login attempts
            last_login_ip: Previous login IP address
            last_login_location: Previous login location
            is_known_device: Whether device is recognized
            
        Returns:
            SecurityAssessment with risk score and recommendations
        """
        risk_score = 0
        risk_factors = []
        recommendations = []
        
        # Failed attempts risk
        if failed_attempts > 0:
            risk_score += min(50, failed_attempts * 10)
            risk_factors.append(f"Recent failed login attempts: {failed_attempts}")
            
            if failed_attempts >= 3:
                recommendations.append("Consider requiring MFA")
        
        # IP address change risk
        if last_login_ip and last_login_ip != ip_address:
            risk_score += 25
            risk_factors.append("Login from different IP address")
            recommendations.append("Verify user identity")
        
        # Unknown device risk
        if not is_known_device:
            risk_score += 20
            risk_factors.append("Login from unknown device")
            recommendations.append("Send device verification notification")
        
        # Suspicious user agent patterns
        suspicious_agents = [
            'bot', 'crawler', 'spider', 'scraper', 'curl', 'wget'
        ]
        
        if any(agent in user_agent.lower() for agent in suspicious_agents):
            risk_score += 40
            risk_factors.append("Suspicious user agent detected")
            recommendations.append("Block automated access")
        
        # Determine risk level
        if risk_score >= 75:
            risk_level = "critical"
            allow_action = False
        elif risk_score >= 50:
            risk_level = "high"
            allow_action = False
        elif risk_score >= 25:
            risk_level = "medium"
            allow_action = True
        else:
            risk_level = "low"
            allow_action = True
        
        # Add general recommendations based on risk level
        if risk_level == "critical":
            recommendations.extend([
                "Block login attempt",
                "Temporarily lock account",
                "Send security alert to user"
            ])
        elif risk_level == "high":
            recommendations.extend([
                "Require MFA verification",
                "Send security notification"
            ])
        elif risk_level == "medium":
            recommendations.append("Monitor for additional suspicious activity")
        
        return SecurityAssessment(
            risk_score=risk_score,
            risk_level=risk_level,
            risk_factors=risk_factors,
            recommendations=recommendations,
            allow_action=allow_action
        )


class APIKeyManager:
    """API key generation and validation."""
    
    def __init__(self):
        """Initialize API key manager."""
        self.key_prefix_length = 8
        self.key_length = 64
    
    def generate_api_key(self) -> Tuple[str, str, str]:
        """
        Generate new API key.
        
        Returns:
            Tuple of (full_key, key_hash, key_prefix)
        """
        # Generate random key
        key = secrets.token_urlsafe(self.key_length)
        
        # Create hash for storage
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        
        # Create prefix for identification
        key_prefix = key[:self.key_prefix_length]
        
        return key, key_hash, key_prefix
    
    def validate_api_key(self, key: str, stored_hash: str) -> bool:
        """
        Validate API key against stored hash.
        
        Args:
            key: API key to validate
            stored_hash: Stored hash to compare against
            
        Returns:
            True if key is valid
        """
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return hmac.compare_digest(key_hash, stored_hash)