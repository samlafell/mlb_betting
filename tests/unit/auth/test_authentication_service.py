"""
Unit tests for AuthenticationService

Tests for user authentication, token management, and session handling.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from src.auth.services import AuthenticationService
from src.auth.models import LoginRequest, MFAVerificationRequest, User
from src.auth.security import TokenType
from src.auth.exceptions import (
    InvalidCredentialsError, AccountLockedError, AccountDisabledError,
    EmailNotVerifiedError, MFARequiredError, InvalidMFACodeError,
    TokenExpiredError, InvalidTokenError, SessionExpiredError
)


class TestAuthenticationService:
    """Test cases for AuthenticationService."""
    
    @pytest.fixture
    def auth_service(self):
        """Create AuthenticationService instance."""
        return AuthenticationService()
    
    @pytest.fixture
    def mock_user(self):
        """Create mock user for testing."""
        return User(
            id=1,
            uuid=uuid4(),
            username="testuser",
            email="test@example.com",
            password_created_at=datetime.now(timezone.utc),
            password_updated_at=datetime.now(timezone.utc),
            is_active=True,
            is_verified=True,
            is_locked=False,
            failed_login_attempts=0,
            mfa_enabled=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            roles=[],
            preferences=None,
            sessions=[]
        )
    
    @pytest.fixture
    def login_request(self):
        """Create login request for testing."""
        from pydantic import SecretStr
        return LoginRequest(
            username="testuser",
            password=SecretStr("secure_password_123"),
            remember_me=False
        )
    
    @pytest.mark.asyncio
    async def test_authenticate_user_success(self, auth_service, mock_user, login_request):
        """Test successful user authentication."""
        with patch.object(auth_service, '_get_user_by_username', return_value=mock_user), \
             patch.object(auth_service, '_check_account_status'), \
             patch.object(auth_service, '_verify_password'), \
             patch.object(auth_service, '_assess_login_risk') as mock_risk, \
             patch.object(auth_service, '_create_user_session') as mock_session, \
             patch.object(auth_service, '_generate_token_pair') as mock_tokens, \
             patch.object(auth_service, '_update_user_login_info'), \
             patch.object(auth_service, '_log_auth_event'):
            
            # Setup mocks
            mock_risk.return_value = MagicMock(risk_level="low", risk_score=10)
            mock_session.return_value = MagicMock(session_id=uuid4())
            mock_tokens.return_value = ("access_token", "refresh_token")
            
            # Test authentication
            result = await auth_service.authenticate_user(
                login_request, "127.0.0.1", "test-agent"
            )
            
            # Assertions
            assert result.access_token == "access_token"
            assert result.refresh_token == "refresh_token"
            assert result.user == mock_user
            assert result.expires_in == 15 * 60
    
    @pytest.mark.asyncio
    async def test_authenticate_user_invalid_credentials(self, auth_service, mock_user, login_request):
        """Test authentication with invalid credentials."""
        with patch.object(auth_service, '_get_user_by_username', return_value=mock_user), \
             patch.object(auth_service, '_check_account_status'), \
             patch.object(auth_service, '_verify_password', side_effect=InvalidCredentialsError()), \
             patch.object(auth_service, '_log_auth_event'), \
             patch.object(auth_service, '_increment_failed_attempts'):
            
            with pytest.raises(InvalidCredentialsError):
                await auth_service.authenticate_user(
                    login_request, "127.0.0.1", "test-agent"
                )
    
    @pytest.mark.asyncio
    async def test_authenticate_user_account_locked(self, auth_service, mock_user, login_request):
        """Test authentication with locked account."""
        mock_user.is_locked = True
        mock_user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=30)
        
        with patch.object(auth_service, '_get_user_by_username', return_value=mock_user), \
             patch.object(auth_service, '_check_account_status', side_effect=AccountLockedError()):
            
            with pytest.raises(AccountLockedError):
                await auth_service.authenticate_user(
                    login_request, "127.0.0.1", "test-agent"
                )
    
    @pytest.mark.asyncio
    async def test_authenticate_user_account_disabled(self, auth_service, mock_user, login_request):
        """Test authentication with disabled account."""
        mock_user.is_active = False
        
        with patch.object(auth_service, '_get_user_by_username', return_value=mock_user), \
             patch.object(auth_service, '_check_account_status', side_effect=AccountDisabledError()):
            
            with pytest.raises(AccountDisabledError):
                await auth_service.authenticate_user(
                    login_request, "127.0.0.1", "test-agent"
                )
    
    @pytest.mark.asyncio
    async def test_authenticate_user_mfa_required(self, auth_service, mock_user, login_request):
        """Test authentication when MFA is required."""
        mock_user.mfa_enabled = True
        
        with patch.object(auth_service, '_get_user_by_username', return_value=mock_user), \
             patch.object(auth_service, '_check_account_status'), \
             patch.object(auth_service, '_verify_password'), \
             patch.object(auth_service, '_assess_login_risk') as mock_risk, \
             patch.object(auth_service, '_handle_mfa_required', side_effect=MFARequiredError("mfa_token")):
            
            mock_risk.return_value = MagicMock(risk_level="low", risk_score=10)
            
            with pytest.raises(MFARequiredError) as exc_info:
                await auth_service.authenticate_user(
                    login_request, "127.0.0.1", "test-agent"
                )
            
            assert exc_info.value.details.get("mfa_session_token") == "mfa_token"
    
    @pytest.mark.asyncio
    async def test_verify_mfa_success(self, auth_service, mock_user):
        """Test successful MFA verification."""
        mfa_request = MFAVerificationRequest(
            mfa_token="mfa_session_token",
            code="123456",
            remember_device=False
        )
        
        with patch.object(auth_service.jwt_manager, 'decode_token') as mock_decode, \
             patch.object(auth_service, '_get_user_by_id', return_value=mock_user), \
             patch.object(auth_service, '_verify_mfa_code', return_value=True), \
             patch.object(auth_service, '_create_user_session') as mock_session, \
             patch.object(auth_service, '_generate_token_pair') as mock_tokens, \
             patch.object(auth_service, '_update_user_login_info'), \
             patch.object(auth_service, '_log_auth_event'):
            
            # Setup mocks
            mock_decode.return_value = MagicMock(
                token_type=TokenType.MFA_SESSION,
                sub="1"
            )
            mock_session.return_value = MagicMock(session_id=uuid4())
            mock_tokens.return_value = ("access_token", "refresh_token")
            
            # Test MFA verification
            result = await auth_service.verify_mfa(
                mfa_request, "127.0.0.1", "test-agent"
            )
            
            # Assertions
            assert result.access_token == "access_token"
            assert result.refresh_token == "refresh_token"
            assert result.user == mock_user
    
    @pytest.mark.asyncio
    async def test_verify_mfa_invalid_code(self, auth_service, mock_user):
        """Test MFA verification with invalid code."""
        mfa_request = MFAVerificationRequest(
            mfa_token="mfa_session_token",
            code="invalid",
            remember_device=False
        )
        
        with patch.object(auth_service.jwt_manager, 'decode_token') as mock_decode, \
             patch.object(auth_service, '_get_user_by_id', return_value=mock_user), \
             patch.object(auth_service, '_verify_mfa_code', return_value=False), \
             patch.object(auth_service, '_log_auth_event'):
            
            mock_decode.return_value = MagicMock(
                token_type=TokenType.MFA_SESSION,
                sub="1"
            )
            
            with pytest.raises(InvalidMFACodeError):
                await auth_service.verify_mfa(
                    mfa_request, "127.0.0.1", "test-agent"
                )
    
    @pytest.mark.asyncio
    async def test_refresh_tokens_success(self, auth_service, mock_user):
        """Test successful token refresh."""
        with patch.object(auth_service.jwt_manager, 'decode_token') as mock_decode, \
             patch.object(auth_service, '_verify_token_not_revoked'), \
             patch.object(auth_service, '_get_user_by_id', return_value=mock_user), \
             patch.object(auth_service, '_get_session_by_id') as mock_session, \
             patch.object(auth_service, '_generate_token_pair') as mock_tokens, \
             patch.object(auth_service, '_revoke_token'):
            
            # Setup mocks
            session_id = uuid4()
            mock_decode.return_value = MagicMock(
                token_type=TokenType.REFRESH,
                sub="1",
                session_id=str(session_id),
                jti="token_id"
            )
            mock_session.return_value = MagicMock(
                is_active=True,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
            )
            mock_tokens.return_value = ("new_access_token", "new_refresh_token")
            
            # Test token refresh
            result = await auth_service.refresh_tokens("refresh_token")
            
            # Assertions
            assert result.access_token == "new_access_token"
            assert result.refresh_token == "new_refresh_token"
            assert result.user == mock_user
    
    @pytest.mark.asyncio
    async def test_refresh_tokens_expired(self, auth_service):
        """Test token refresh with expired token."""
        with patch.object(auth_service.jwt_manager, 'decode_token', side_effect=TokenExpiredError("refresh")):
            
            with pytest.raises(TokenExpiredError):
                await auth_service.refresh_tokens("expired_token")
    
    @pytest.mark.asyncio
    async def test_refresh_tokens_invalid(self, auth_service):
        """Test token refresh with invalid token."""
        with patch.object(auth_service.jwt_manager, 'decode_token', side_effect=InvalidTokenError("refresh")):
            
            with pytest.raises(InvalidTokenError):
                await auth_service.refresh_tokens("invalid_token")
    
    @pytest.mark.asyncio
    async def test_logout_user_success(self, auth_service):
        """Test successful user logout."""
        with patch.object(auth_service, '_revoke_all_user_sessions') as mock_revoke, \
             patch.object(auth_service, '_log_auth_event'):
            
            # Test logout with revoke all sessions
            result = await auth_service.logout_user(1, revoke_all_sessions=True)
            
            # Assertions
            assert result is True
            mock_revoke.assert_called_once_with(1, "user_logout")
    
    @pytest.mark.asyncio
    async def test_logout_single_session(self, auth_service):
        """Test logout of single session."""
        session_id = uuid4()
        
        with patch.object(auth_service, '_revoke_session') as mock_revoke, \
             patch.object(auth_service, '_log_auth_event'):
            
            # Test logout of specific session
            result = await auth_service.logout_user(1, session_id=session_id)
            
            # Assertions
            assert result is True
            mock_revoke.assert_called_once_with(session_id, "user_logout")
    
    @pytest.mark.asyncio
    async def test_check_account_status_inactive(self, auth_service, mock_user):
        """Test account status check with inactive account."""
        mock_user.is_active = False
        
        with patch.object(auth_service, '_log_auth_event'):
            with pytest.raises(AccountDisabledError):
                await auth_service._check_account_status(mock_user, "127.0.0.1")
    
    @pytest.mark.asyncio
    async def test_check_account_status_unverified(self, auth_service, mock_user):
        """Test account status check with unverified email."""
        mock_user.is_verified = False
        
        with patch.object(auth_service, '_log_auth_event'):
            with pytest.raises(EmailNotVerifiedError):
                await auth_service._check_account_status(mock_user, "127.0.0.1")
    
    @pytest.mark.asyncio
    async def test_check_account_status_locked(self, auth_service, mock_user):
        """Test account status check with locked account."""
        mock_user.is_locked = True
        mock_user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=30)
        
        with patch.object(auth_service, '_log_auth_event'):
            with pytest.raises(AccountLockedError):
                await auth_service._check_account_status(mock_user, "127.0.0.1")
    
    @pytest.mark.asyncio
    async def test_verify_password_success(self, auth_service, mock_user):
        """Test successful password verification."""
        with patch('src.data.database.connection.get_database_connection') as mock_conn, \
             patch.object(auth_service.password_hasher, 'verify_password', return_value=True):
            
            mock_conn.return_value.__aenter__.return_value.fetchrow.return_value = {
                'password_hash': 'hashed_password',
                'password_salt': 'salt'
            }
            
            # Should not raise exception
            await auth_service._verify_password(mock_user, "correct_password")
    
    @pytest.mark.asyncio
    async def test_verify_password_failure(self, auth_service, mock_user):
        """Test failed password verification."""
        with patch('src.data.database.connection.get_database_connection') as mock_conn, \
             patch.object(auth_service.password_hasher, 'verify_password', return_value=False):
            
            mock_conn.return_value.__aenter__.return_value.fetchrow.return_value = {
                'password_hash': 'hashed_password',
                'password_salt': 'salt'
            }
            
            with pytest.raises(InvalidCredentialsError):
                await auth_service._verify_password(mock_user, "wrong_password")
    
    @pytest.mark.asyncio
    async def test_assess_login_risk(self, auth_service, mock_user):
        """Test login risk assessment."""
        with patch.object(auth_service.security_validator, 'assess_login_risk') as mock_assess:
            mock_assess.return_value = MagicMock(
                risk_score=25,
                risk_level="medium",
                allow_action=True
            )
            
            result = await auth_service._assess_login_risk(
                mock_user, "127.0.0.1", "test-agent"
            )
            
            assert result.risk_score == 25
            assert result.risk_level == "medium"
            assert result.allow_action is True
    
    @pytest.mark.asyncio
    async def test_verify_mfa_code_totp_success(self, auth_service, mock_user):
        """Test successful TOTP code verification."""
        mock_user.mfa_enabled = True
        mock_user.mfa_secret = "test_secret"
        
        with patch.object(auth_service.mfa_manager, 'verify_totp_code', return_value=True):
            result = await auth_service._verify_mfa_code(mock_user, "123456")
            assert result is True
    
    @pytest.mark.asyncio
    async def test_verify_mfa_code_backup_success(self, auth_service, mock_user):
        """Test successful backup code verification."""
        mock_user.mfa_enabled = True
        mock_user.mfa_secret = "test_secret"
        mock_user.mfa_backup_codes = ["hashed_code_1", "hashed_code_2"]
        
        with patch.object(auth_service.mfa_manager, 'verify_totp_code', return_value=False), \
             patch.object(auth_service.mfa_manager, 'verify_backup_code', return_value=(True, "hashed_code_1")), \
             patch.object(auth_service, '_remove_used_backup_code'):
            
            result = await auth_service._verify_mfa_code(mock_user, "backup_code")
            assert result is True
    
    @pytest.mark.asyncio
    async def test_verify_mfa_code_failure(self, auth_service, mock_user):
        """Test failed MFA code verification."""
        mock_user.mfa_enabled = True
        mock_user.mfa_secret = "test_secret"
        
        with patch.object(auth_service.mfa_manager, 'verify_totp_code', return_value=False):
            result = await auth_service._verify_mfa_code(mock_user, "invalid")
            assert result is False
    
    @pytest.mark.asyncio
    async def test_mfa_not_setup_error(self, auth_service, mock_user):
        """Test MFA verification when MFA is not set up."""
        mock_user.mfa_enabled = False
        
        with pytest.raises(MFARequiredError):
            await auth_service._verify_mfa_code(mock_user, "123456")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])