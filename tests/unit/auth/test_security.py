"""
Unit tests for authentication security components

Tests for password hashing, JWT tokens, MFA, and security validation.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import secrets
import jwt

from src.auth.security import (
    PasswordHasher, PasswordValidator, JWTManager, MFAManager,
    SecurityValidator, APIKeyManager, TokenType, TokenClaims
)
from src.auth.exceptions import (
    WeakPasswordError, InvalidTokenError, TokenExpiredError
)


class TestPasswordHasher:
    """Test cases for PasswordHasher."""
    
    @pytest.fixture
    def password_hasher(self):
        """Create PasswordHasher instance."""
        return PasswordHasher(rounds=4)  # Lower rounds for faster tests
    
    def test_hash_password(self, password_hasher):
        """Test password hashing."""
        password = "test_password_123"
        
        hash1, salt1 = password_hasher.hash_password(password)
        hash2, salt2 = password_hasher.hash_password(password)
        
        # Hashes should be different due to different salts
        assert hash1 != hash2
        assert salt1 != salt2
        assert len(hash1) > 0
        assert len(salt1) > 0
    
    def test_verify_password_success(self, password_hasher):
        """Test successful password verification."""
        password = "test_password_123"
        password_hash, salt = password_hasher.hash_password(password)
        
        assert password_hasher.verify_password(password, password_hash, salt) is True
    
    def test_verify_password_failure(self, password_hasher):
        """Test failed password verification."""
        password = "test_password_123"
        wrong_password = "wrong_password"
        password_hash, salt = password_hasher.hash_password(password)
        
        assert password_hasher.verify_password(wrong_password, password_hash, salt) is False
    
    def test_verify_password_invalid_hash(self, password_hasher):
        """Test password verification with invalid hash."""
        password = "test_password_123"
        invalid_hash = "invalid_hash"
        salt = "salt"
        
        assert password_hasher.verify_password(password, invalid_hash, salt) is False


class TestPasswordValidator:
    """Test cases for PasswordValidator."""
    
    @pytest.fixture
    def password_validator(self):
        """Create PasswordValidator instance."""
        return PasswordValidator()
    
    def test_validate_strong_password(self, password_validator):
        """Test validation of strong password."""
        password = "StrongPassword123!"
        
        is_valid, violations = password_validator.validate_password(password)
        
        assert is_valid is True
        assert len(violations) == 0
    
    def test_validate_weak_password_short(self, password_validator):
        """Test validation of short password."""
        password = "short"
        
        is_valid, violations = password_validator.validate_password(password)
        
        assert is_valid is False
        assert any("at least 12 characters" in v for v in violations)
    
    def test_validate_weak_password_no_uppercase(self, password_validator):
        """Test validation of password without uppercase."""
        password = "nouppercase123!"
        
        is_valid, violations = password_validator.validate_password(password)
        
        assert is_valid is False
        assert any("uppercase letter" in v for v in violations)
    
    def test_validate_weak_password_no_lowercase(self, password_validator):
        """Test validation of password without lowercase."""
        password = "NOLOWERCASE123!"
        
        is_valid, violations = password_validator.validate_password(password)
        
        assert is_valid is False
        assert any("lowercase letter" in v for v in violations)
    
    def test_validate_weak_password_no_numbers(self, password_validator):
        """Test validation of password without numbers."""
        password = "NoNumbersHere!"
        
        is_valid, violations = password_validator.validate_password(password)
        
        assert is_valid is False
        assert any("number" in v for v in violations)
    
    def test_validate_weak_password_no_special_chars(self, password_validator):
        """Test validation of password without special characters."""
        password = "NoSpecialChars123"
        
        is_valid, violations = password_validator.validate_password(password)
        
        assert is_valid is False
        assert any("special character" in v for v in violations)
    
    def test_validate_password_with_username(self, password_validator):
        """Test validation of password containing username."""
        password = "MyUsernamePassword123!"
        username = "myusername"
        
        is_valid, violations = password_validator.validate_password(password, username)
        
        assert is_valid is False
        assert any("username" in v for v in violations)
    
    def test_validate_weak_patterns(self, password_validator):
        """Test validation of passwords with weak patterns."""
        weak_passwords = [
            "PasswordPassword123!",  # Contains "password"
            "AdminAdmin123!",        # Contains "admin"
            "Aaaaaaaa123!",         # Repeated characters
            "Qwerty123456!"         # Common sequence
        ]
        
        for password in weak_passwords:
            is_valid, violations = password_validator.validate_password(password)
            assert is_valid is False
    
    def test_password_strength_score(self, password_validator):
        """Test password strength scoring."""
        weak_password = "weak"
        medium_password = "MediumPass123"
        strong_password = "VeryStrongPassword123!@#"
        
        weak_score = password_validator.get_password_strength_score(weak_password)
        medium_score = password_validator.get_password_strength_score(medium_password)
        strong_score = password_validator.get_password_strength_score(strong_password)
        
        assert weak_score < medium_score < strong_score
        assert 0 <= weak_score <= 100
        assert 0 <= medium_score <= 100
        assert 0 <= strong_score <= 100


class TestJWTManager:
    """Test cases for JWTManager."""
    
    @pytest.fixture
    def jwt_manager(self):
        """Create JWTManager instance."""
        return JWTManager()
    
    def test_create_access_token(self, jwt_manager):
        """Test access token creation."""
        user_id = "123"
        permissions = ["user:read", "data:read"]
        
        token = jwt_manager.create_token(
            user_id=user_id,
            token_type=TokenType.ACCESS,
            permissions=permissions
        )
        
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_create_refresh_token(self, jwt_manager):
        """Test refresh token creation."""
        user_id = "123"
        session_id = "session_123"
        
        token = jwt_manager.create_token(
            user_id=user_id,
            token_type=TokenType.REFRESH,
            session_id=session_id
        )
        
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_decode_valid_token(self, jwt_manager):
        """Test decoding valid token."""
        user_id = "123"
        permissions = ["user:read"]
        
        token = jwt_manager.create_token(
            user_id=user_id,
            token_type=TokenType.ACCESS,
            permissions=permissions
        )
        
        claims = jwt_manager.decode_token(token)
        
        assert claims.sub == user_id
        assert claims.token_type == TokenType.ACCESS
        assert claims.permissions == permissions
        assert isinstance(claims.exp, datetime)
        assert isinstance(claims.iat, datetime)
        assert isinstance(claims.jti, str)
    
    def test_decode_expired_token(self, jwt_manager):
        """Test decoding expired token."""
        user_id = "123"
        
        # Create token with past expiration
        token = jwt_manager.create_token(
            user_id=user_id,
            token_type=TokenType.ACCESS,
            custom_expiry=timedelta(seconds=-1)  # Expired 1 second ago
        )
        
        with pytest.raises(TokenExpiredError):
            jwt_manager.decode_token(token)
    
    def test_decode_invalid_token(self, jwt_manager):
        """Test decoding invalid token."""
        invalid_token = "invalid.token.here"
        
        with pytest.raises(InvalidTokenError):
            jwt_manager.decode_token(invalid_token)
    
    def test_refresh_token_flow(self, jwt_manager):
        """Test token refresh functionality."""
        user_id = "123"
        permissions = ["user:read"]
        
        # Create refresh token
        refresh_token = jwt_manager.create_token(
            user_id=user_id,
            token_type=TokenType.REFRESH,
            permissions=permissions
        )
        
        # Refresh tokens
        new_access, new_refresh = jwt_manager.refresh_token(refresh_token)
        
        # Verify new tokens
        access_claims = jwt_manager.decode_token(new_access)
        refresh_claims = jwt_manager.decode_token(new_refresh)
        
        assert access_claims.sub == user_id
        assert access_claims.token_type == TokenType.ACCESS
        assert refresh_claims.sub == user_id
        assert refresh_claims.token_type == TokenType.REFRESH
    
    def test_token_hash_generation(self, jwt_manager):
        """Test token hash generation."""
        token = "test.jwt.token"
        
        hash1 = jwt_manager.get_token_hash(token)
        hash2 = jwt_manager.get_token_hash(token)
        
        assert hash1 == hash2  # Same token should produce same hash
        assert len(hash1) == 64  # SHA-256 produces 64-character hex string
    
    def test_token_expiration_times(self, jwt_manager):
        """Test different token expiration times."""
        user_id = "123"
        now = datetime.now(timezone.utc)
        
        # Test different token types
        access_token = jwt_manager.create_token(user_id, TokenType.ACCESS)
        refresh_token = jwt_manager.create_token(user_id, TokenType.REFRESH)
        reset_token = jwt_manager.create_token(user_id, TokenType.RESET)
        
        access_claims = jwt_manager.decode_token(access_token, verify_expiry=False)
        refresh_claims = jwt_manager.decode_token(refresh_token, verify_expiry=False)
        reset_claims = jwt_manager.decode_token(reset_token, verify_expiry=False)
        
        # Access token should expire in 15 minutes
        assert access_claims.exp - now < timedelta(minutes=16)
        assert access_claims.exp - now > timedelta(minutes=14)
        
        # Refresh token should expire in 30 days
        assert refresh_claims.exp - now < timedelta(days=31)
        assert refresh_claims.exp - now > timedelta(days=29)
        
        # Reset token should expire in 1 hour
        assert reset_claims.exp - now < timedelta(hours=1.1)
        assert reset_claims.exp - now > timedelta(minutes=59)


class TestMFAManager:
    """Test cases for MFAManager."""
    
    @pytest.fixture
    def mfa_manager(self):
        """Create MFAManager instance."""
        return MFAManager()
    
    def test_generate_secret(self, mfa_manager):
        """Test MFA secret generation."""
        secret = mfa_manager.generate_secret()
        
        assert isinstance(secret, str)
        assert len(secret) == 32  # Base32 encoded secret
    
    def test_generate_qr_code_url(self, mfa_manager):
        """Test QR code URL generation."""
        secret = "TESTSECRET123456"
        username = "testuser"
        
        qr_url = mfa_manager.generate_qr_code_url(secret, username)
        
        assert qr_url.startswith("otpauth://totp/")
        assert username in qr_url
        assert secret in qr_url
        assert "MLB%20Betting%20Program" in qr_url
    
    @patch('pyotp.TOTP.verify')
    def test_verify_totp_code_success(self, mock_verify, mfa_manager):
        """Test successful TOTP code verification."""
        mock_verify.return_value = True
        
        secret = "TESTSECRET123456"
        code = "123456"
        
        result = mfa_manager.verify_totp_code(secret, code)
        
        assert result is True
        mock_verify.assert_called_once()
    
    @patch('pyotp.TOTP.verify')
    def test_verify_totp_code_failure(self, mock_verify, mfa_manager):
        """Test failed TOTP code verification."""
        mock_verify.return_value = False
        
        secret = "TESTSECRET123456"
        code = "wrong123"
        
        result = mfa_manager.verify_totp_code(secret, code)
        
        assert result is False
    
    def test_generate_backup_codes(self, mfa_manager):
        """Test backup code generation."""
        codes = mfa_manager.generate_backup_codes(8)
        
        assert len(codes) == 8
        assert all(isinstance(code, str) for code in codes)
        assert all("-" in code for code in codes)  # Format: XXXXX-XXXXX
        assert len(set(codes)) == 8  # All codes should be unique
    
    def test_hash_backup_codes(self, mfa_manager):
        """Test backup code hashing."""
        codes = ["12345-67890", "98765-43210"]
        
        hashed = mfa_manager.hash_backup_codes(codes)
        
        assert len(hashed) == len(codes)
        assert all(len(h) == 64 for h in hashed)  # SHA-256 hex length
        assert hashed[0] != hashed[1]  # Different codes should have different hashes
    
    def test_verify_backup_code_success(self, mfa_manager):
        """Test successful backup code verification."""
        codes = ["12345-67890", "98765-43210"]
        hashed_codes = mfa_manager.hash_backup_codes(codes)
        
        is_valid, matched_hash = mfa_manager.verify_backup_code(
            "12345-67890", hashed_codes
        )
        
        assert is_valid is True
        assert matched_hash == hashed_codes[0]
    
    def test_verify_backup_code_failure(self, mfa_manager):
        """Test failed backup code verification."""
        codes = ["12345-67890", "98765-43210"]
        hashed_codes = mfa_manager.hash_backup_codes(codes)
        
        is_valid, matched_hash = mfa_manager.verify_backup_code(
            "wrong-code", hashed_codes
        )
        
        assert is_valid is False
        assert matched_hash is None


class TestSecurityValidator:
    """Test cases for SecurityValidator."""
    
    @pytest.fixture
    def security_validator(self):
        """Create SecurityValidator instance."""
        return SecurityValidator()
    
    def test_validate_ip_address_allowed(self, security_validator):
        """Test IP address validation with allowed ranges."""
        allowed_ranges = ["127.0.0.1", "192.168.1.0/24"]
        
        assert security_validator.validate_ip_address("127.0.0.1", allowed_ranges) is True
        assert security_validator.validate_ip_address("192.168.1.100", allowed_ranges) is True
    
    def test_validate_ip_address_denied(self, security_validator):
        """Test IP address validation with denied IP."""
        allowed_ranges = ["127.0.0.1", "192.168.1.0/24"]
        
        assert security_validator.validate_ip_address("10.0.0.1", allowed_ranges) is False
    
    def test_validate_ip_address_no_ranges(self, security_validator):
        """Test IP address validation with no ranges (allow all)."""
        assert security_validator.validate_ip_address("127.0.0.1", None) is True
        assert security_validator.validate_ip_address("10.0.0.1", []) is True
    
    def test_assess_login_risk_low(self, security_validator):
        """Test low-risk login assessment."""
        assessment = security_validator.assess_login_risk(
            ip_address="127.0.0.1",
            user_agent="Mozilla/5.0 (normal browser)",
            failed_attempts=0,
            is_known_device=True
        )
        
        assert assessment.risk_level == "low"
        assert assessment.allow_action is True
        assert assessment.risk_score < 25
    
    def test_assess_login_risk_high(self, security_validator):
        """Test high-risk login assessment."""
        assessment = security_validator.assess_login_risk(
            ip_address="1.2.3.4",
            user_agent="curl/7.68.0",
            failed_attempts=5,
            last_login_ip="127.0.0.1",
            is_known_device=False
        )
        
        assert assessment.risk_level in ["high", "critical"]
        assert assessment.risk_score >= 50
        assert len(assessment.risk_factors) > 0
        assert len(assessment.recommendations) > 0
    
    def test_assess_login_risk_suspicious_user_agent(self, security_validator):
        """Test risk assessment with suspicious user agent."""
        assessment = security_validator.assess_login_risk(
            ip_address="127.0.0.1",
            user_agent="python-requests/2.25.1",
            failed_attempts=0,
            is_known_device=True
        )
        
        assert "Suspicious user agent" in assessment.risk_factors
        assert assessment.risk_score > 25


class TestAPIKeyManager:
    """Test cases for APIKeyManager."""
    
    @pytest.fixture
    def api_key_manager(self):
        """Create APIKeyManager instance."""
        return APIKeyManager()
    
    def test_generate_api_key(self, api_key_manager):
        """Test API key generation."""
        key, key_hash, key_prefix = api_key_manager.generate_api_key()
        
        assert isinstance(key, str)
        assert isinstance(key_hash, str)
        assert isinstance(key_prefix, str)
        
        assert len(key) > 0
        assert len(key_hash) == 64  # SHA-256 hex length
        assert len(key_prefix) == 8
        assert key.startswith(key_prefix)
    
    def test_validate_api_key_success(self, api_key_manager):
        """Test successful API key validation."""
        key, key_hash, _ = api_key_manager.generate_api_key()
        
        result = api_key_manager.validate_api_key(key, key_hash)
        
        assert result is True
    
    def test_validate_api_key_failure(self, api_key_manager):
        """Test failed API key validation."""
        key, key_hash, _ = api_key_manager.generate_api_key()
        wrong_key = "wrong_key"
        
        result = api_key_manager.validate_api_key(wrong_key, key_hash)
        
        assert result is False
    
    def test_api_key_uniqueness(self, api_key_manager):
        """Test that generated API keys are unique."""
        key1, hash1, prefix1 = api_key_manager.generate_api_key()
        key2, hash2, prefix2 = api_key_manager.generate_api_key()
        
        assert key1 != key2
        assert hash1 != hash2
        # Prefixes might be the same (small chance), but keys should be different


if __name__ == "__main__":
    pytest.main([__file__, "-v"])