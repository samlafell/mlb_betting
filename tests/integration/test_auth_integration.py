"""
Comprehensive Authentication Integration Tests

Tests complete authentication flows including login, logout, MFA, 
password management, session handling, and rate limiting.
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from unittest.mock import AsyncMock, patch
import json

import asyncpg
from fastapi.testclient import TestClient
from fastapi import FastAPI, status

from src.auth.models import User, UserCreate, LoginRequest
from src.auth.services import AuthenticationService
from src.auth.middleware import AuthenticationMiddleware, RateLimitingMiddleware
from src.auth.rate_limiter import get_rate_limiter
from src.auth.security import PasswordHasher, JWTManager
from src.auth.exceptions import (
    AuthenticationError, RateLimitExceededError, AccountLockedError
)
from src.core.config import get_settings
from src.data.database.connection import get_connection


class TestAuthenticationIntegration:
    """Test complete authentication system integration."""
    
    @pytest.fixture
    async def db_connection(self):
        """Get test database connection."""
        async with get_connection() as conn:
            yield conn
    
    @pytest.fixture
    async def auth_service(self):
        """Get authentication service."""
        return AuthenticationService()
    
    @pytest.fixture
    async def test_user_data(self):
        """Test user data."""
        return {
            "username": "test_user",
            "email": "test@example.com",
            "password": "SecurePassword123!",
            "first_name": "Test",
            "last_name": "User"
        }
    
    @pytest.fixture
    async def test_app(self):
        """Create test FastAPI app with auth middleware."""
        app = FastAPI()
        
        # Add auth middleware
        app.add_middleware(RateLimitingMiddleware)
        app.add_middleware(AuthenticationMiddleware)
        
        @app.get("/test/public")
        async def public_endpoint():
            return {"message": "public"}
        
        @app.get("/test/protected")
        async def protected_endpoint():
            return {"message": "protected"}
        
        return app
    
    @pytest.fixture
    async def client(self, test_app):
        """Get test client."""
        return TestClient(test_app)
    
    async def test_user_registration_flow(self, auth_service, test_user_data, db_connection):
        """Test complete user registration flow."""
        # Create user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"],
            first_name=test_user_data["first_name"],
            last_name=test_user_data["last_name"]
        )
        
        user = await auth_service.create_user(user_create)
        
        # Verify user was created
        assert user.username == test_user_data["username"]
        assert user.email == test_user_data["email"]
        assert user.is_active is True
        assert user.is_verified is False
        
        # Verify password is hashed
        db_user = await db_connection.fetchrow(
            "SELECT password_hash FROM auth.users WHERE id = $1",
            user.id
        )
        assert db_user["password_hash"] != test_user_data["password"]
        
        # Verify password history
        history_count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.password_history WHERE user_id = $1",
            user.id
        )
        assert history_count == 1
    
    async def test_login_flow(self, auth_service, test_user_data, db_connection):
        """Test complete login flow."""
        # Create and verify user first
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        # Mark user as verified
        await db_connection.execute(
            "UPDATE auth.users SET is_verified = true WHERE id = $1",
            user.id
        )
        
        # Test login
        login_request = LoginRequest(
            username=test_user_data["username"],
            password=test_user_data["password"]
        )
        
        token_response = await auth_service.authenticate_user(
            login_request,
            ip_address="127.0.0.1",
            user_agent="test-agent"
        )
        
        # Verify response
        assert token_response.access_token is not None
        assert token_response.refresh_token is not None
        assert token_response.user.username == user.username
        assert token_response.expires_in > 0
        
        # Verify session was created
        session_count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.sessions WHERE user_id = $1 AND is_active = true",
            user.id
        )
        assert session_count == 1
        
        # Verify audit log
        audit_count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.audit_log WHERE user_id = $1 AND event_type = 'login_success'",
            user.id
        )
        assert audit_count == 1
    
    async def test_failed_login_attempts(self, auth_service, test_user_data, db_connection):
        """Test failed login attempts and account locking."""
        # Create user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        # Mark user as verified
        await db_connection.execute(
            "UPDATE auth.users SET is_verified = true WHERE id = $1",
            user.id
        )
        
        # Attempt multiple failed logins
        login_request = LoginRequest(
            username=test_user_data["username"],
            password="wrong_password"
        )
        
        # First 4 attempts should fail but not lock account
        for i in range(4):
            with pytest.raises(AuthenticationError):
                await auth_service.authenticate_user(
                    login_request,
                    ip_address="127.0.0.1",
                    user_agent="test-agent"
                )
        
        # Check failed attempt count
        failed_attempts = await db_connection.fetchval(
            "SELECT failed_login_attempts FROM auth.users WHERE id = $1",
            user.id
        )
        assert failed_attempts == 4
        
        # 5th attempt should lock account
        with pytest.raises(AccountLockedError):
            await auth_service.authenticate_user(
                login_request,
                ip_address="127.0.0.1",
                user_agent="test-agent"
            )
        
        # Verify account is locked
        user_status = await db_connection.fetchrow(
            "SELECT is_locked, locked_until FROM auth.users WHERE id = $1",
            user.id
        )
        assert user_status["is_locked"] is True
        assert user_status["locked_until"] is not None
    
    async def test_password_change_flow(self, auth_service, test_user_data, db_connection):
        """Test password change flow."""
        # Create user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        # Change password
        new_password = "NewSecurePassword456!"
        await auth_service.change_password(
            user.id,
            current_password=test_user_data["password"],
            new_password=new_password
        )
        
        # Verify password was changed
        db_user = await db_connection.fetchrow(
            "SELECT password_hash, password_updated_at FROM auth.users WHERE id = $1",
            user.id
        )
        
        # Verify password history
        history_count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.password_history WHERE user_id = $1",
            user.id
        )
        assert history_count == 2  # Original + new password
        
        # Test login with new password
        login_request = LoginRequest(
            username=test_user_data["username"],
            password=new_password
        )
        
        await db_connection.execute(
            "UPDATE auth.users SET is_verified = true WHERE id = $1",
            user.id
        )
        
        token_response = await auth_service.authenticate_user(
            login_request,
            ip_address="127.0.0.1",
            user_agent="test-agent"
        )
        assert token_response.access_token is not None
    
    async def test_session_management(self, auth_service, test_user_data, db_connection):
        """Test session creation, validation, and cleanup."""
        # Create and login user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        await db_connection.execute(
            "UPDATE auth.users SET is_verified = true WHERE id = $1",
            user.id
        )
        
        login_request = LoginRequest(
            username=test_user_data["username"],
            password=test_user_data["password"]
        )
        
        token_response = await auth_service.authenticate_user(
            login_request,
            ip_address="127.0.0.1",
            user_agent="test-agent"
        )
        
        # Verify session exists
        session = await db_connection.fetchrow(
            "SELECT * FROM auth.sessions WHERE user_id = $1 AND is_active = true",
            user.id
        )
        assert session is not None
        assert session["ip_address"] == "127.0.0.1"
        assert session["user_agent"] == "test-agent"
        
        # Test session logout
        await auth_service.logout_user(session["session_id"])
        
        # Verify session is revoked
        revoked_session = await db_connection.fetchrow(
            "SELECT is_active, revoked_at FROM auth.sessions WHERE session_id = $1",
            session["session_id"]
        )
        assert revoked_session["is_active"] is False
        assert revoked_session["revoked_at"] is not None
    
    async def test_rate_limiting_integration(self, client):
        """Test rate limiting integration."""
        rate_limiter = get_rate_limiter()
        
        # Test login rate limiting
        login_data = {
            "username": "test_user",
            "password": "wrong_password"
        }
        
        # Make requests up to limit
        for i in range(5):
            response = client.post("/auth/login", json=login_data)
            if i < 4:
                # Should be allowed (but fail authentication)
                assert response.status_code in [401, 422]  # Auth failure or validation
            else:
                # Should be rate limited
                assert response.status_code == 429
                assert "rate_limit_exceeded" in response.json()["error"]
    
    async def test_jwt_token_validation(self, auth_service, test_user_data):
        """Test JWT token creation and validation."""
        # Create user and login
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        # Login to get tokens
        login_request = LoginRequest(
            username=test_user_data["username"],
            password=test_user_data["password"]
        )
        
        with patch.object(auth_service, '_check_account_status', new_callable=AsyncMock):
            token_response = await auth_service.authenticate_user(
                login_request,
                ip_address="127.0.0.1",
                user_agent="test-agent"
            )
        
        # Validate access token
        jwt_manager = JWTManager()
        claims = jwt_manager.decode_token(token_response.access_token)
        
        assert claims.sub == str(user.id)
        assert claims.token_type == "access"
        assert claims.exp > datetime.now(timezone.utc)
        
        # Test token refresh
        new_tokens = await auth_service.refresh_token(token_response.refresh_token)
        assert new_tokens.access_token != token_response.access_token
        assert new_tokens.refresh_token is not None
    
    async def test_mfa_setup_flow(self, auth_service, test_user_data, db_connection):
        """Test MFA setup and verification flow."""
        # Create user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        # Setup MFA
        mfa_setup = await auth_service.setup_mfa(user.id)
        
        assert mfa_setup.secret is not None
        assert mfa_setup.qr_code_url is not None
        assert len(mfa_setup.backup_codes) > 0
        
        # Verify MFA is enabled in database
        mfa_enabled = await db_connection.fetchval(
            "SELECT mfa_enabled FROM auth.users WHERE id = $1",
            user.id
        )
        assert mfa_enabled is True
        
        # Test MFA verification during login
        login_request = LoginRequest(
            username=test_user_data["username"],
            password=test_user_data["password"]
        )
        
        await db_connection.execute(
            "UPDATE auth.users SET is_verified = true WHERE id = $1",
            user.id
        )
        
        # Login should require MFA
        with pytest.raises(Exception):  # Should require MFA token
            await auth_service.authenticate_user(
                login_request,
                ip_address="127.0.0.1",
                user_agent="test-agent"
            )
    
    async def test_audit_logging(self, auth_service, test_user_data, db_connection):
        """Test comprehensive audit logging."""
        # Create user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        # Check audit log for user creation
        creation_audit = await db_connection.fetchrow(
            """
            SELECT * FROM auth.audit_log 
            WHERE user_id = $1 AND event_type = 'user_created'
            ORDER BY timestamp DESC LIMIT 1
            """,
            user.id
        )
        assert creation_audit is not None
        assert creation_audit["success"] is True
        
        # Test failed login audit
        login_request = LoginRequest(
            username=test_user_data["username"],
            password="wrong_password"
        )
        
        with pytest.raises(AuthenticationError):
            await auth_service.authenticate_user(
                login_request,
                ip_address="127.0.0.1",
                user_agent="test-agent"
            )
        
        # Check audit log for failed login
        failed_login_audit = await db_connection.fetchrow(
            """
            SELECT * FROM auth.audit_log 
            WHERE user_id = $1 AND event_type = 'login_failed'
            ORDER BY timestamp DESC LIMIT 1
            """,
            user.id
        )
        assert failed_login_audit is not None
        assert failed_login_audit["success"] is False
        assert failed_login_audit["ip_address"] == "127.0.0.1"
    
    async def test_security_headers(self, client):
        """Test security headers are properly set."""
        response = client.get("/test/public")
        
        # Check security headers
        assert "X-Content-Type-Options" in response.headers
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert "X-Frame-Options" in response.headers
        assert response.headers["X-Frame-Options"] == "DENY"
        assert "X-XSS-Protection" in response.headers
        assert "Referrer-Policy" in response.headers
        assert "Content-Security-Policy" in response.headers
    
    async def test_password_strength_validation(self, auth_service):
        """Test password strength validation."""
        # Test weak passwords
        weak_passwords = [
            "password",  # Too common
            "12345678",  # Too simple
            "abc",       # Too short
            "PASSWORD123",  # No lowercase
            "password123",  # No uppercase
            "Password",     # No numbers
        ]
        
        for weak_password in weak_passwords:
            user_create = UserCreate(
                username="test_user",
                email="test@example.com",
                password=weak_password,
                confirm_password=weak_password
            )
            
            with pytest.raises(Exception):  # Should fail validation
                await auth_service.create_user(user_create)
    
    async def test_concurrent_login_sessions(self, auth_service, test_user_data, db_connection):
        """Test multiple concurrent login sessions."""
        # Create user
        user_create = UserCreate(
            username=test_user_data["username"],
            email=test_user_data["email"],
            password=test_user_data["password"],
            confirm_password=test_user_data["password"]
        )
        user = await auth_service.create_user(user_create)
        
        await db_connection.execute(
            "UPDATE auth.users SET is_verified = true WHERE id = $1",
            user.id
        )
        
        login_request = LoginRequest(
            username=test_user_data["username"],
            password=test_user_data["password"]
        )
        
        # Create multiple sessions
        sessions = []
        for i in range(3):
            token_response = await auth_service.authenticate_user(
                login_request,
                ip_address=f"127.0.0.{i+1}",
                user_agent=f"test-agent-{i}"
            )
            sessions.append(token_response)
        
        # Verify all sessions are active
        active_sessions = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.sessions WHERE user_id = $1 AND is_active = true",
            user.id
        )
        assert active_sessions == 3
        
        # Test logout all sessions
        await auth_service.logout_all_sessions(user.id)
        
        # Verify all sessions are revoked
        active_sessions = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.sessions WHERE user_id = $1 AND is_active = true",
            user.id
        )
        assert active_sessions == 0


class TestRateLimitingIntegration:
    """Test rate limiting system integration."""
    
    @pytest.fixture
    async def rate_limiter(self):
        """Get rate limiter instance."""
        return get_rate_limiter()
    
    async def test_sliding_window_rate_limiting(self, rate_limiter, db_connection):
        """Test sliding window rate limiting algorithm."""
        rule_name = "test_sliding"
        identifier = "test_user_1"
        
        # Add test rule
        await db_connection.execute(
            """
            INSERT INTO auth.rate_limit_rules (rule_name, scope, algorithm, max_requests, window_seconds)
            VALUES ($1, 'user', 'sliding_window', 3, 60)
            """,
            rule_name
        )
        
        rate_limiter.rules[rule_name] = type('Rule', (), {
            'algorithm': 'sliding_window',
            'max_requests': 3,
            'window_seconds': 60,
            'penalty_seconds': 0,
            'enabled': True
        })()
        
        # Make requests up to limit
        for i in range(3):
            status = await rate_limiter.check_rate_limit(rule_name, identifier)
            assert status.allowed is True
            assert status.remaining == 2 - i
        
        # Next request should be denied
        status = await rate_limiter.check_rate_limit(rule_name, identifier)
        assert status.allowed is False
        assert status.remaining == 0
    
    async def test_token_bucket_rate_limiting(self, rate_limiter, db_connection):
        """Test token bucket rate limiting algorithm."""
        rule_name = "test_bucket"
        identifier = "test_user_2"
        
        # Add test rule
        await db_connection.execute(
            """
            INSERT INTO auth.rate_limit_rules (rule_name, scope, algorithm, max_requests, window_seconds, burst_allowance)
            VALUES ($1, 'user', 'token_bucket', 5, 60, 2)
            """,
            rule_name
        )
        
        rate_limiter.rules[rule_name] = type('Rule', (), {
            'algorithm': 'token_bucket',
            'max_requests': 5,
            'window_seconds': 60,
            'burst_allowance': 2,
            'penalty_seconds': 0,
            'enabled': True
        })()
        
        # Should allow burst
        for i in range(7):  # 5 + 2 burst
            status = await rate_limiter.check_rate_limit(rule_name, identifier)
            assert status.allowed is True
        
        # Next request should be denied
        status = await rate_limiter.check_rate_limit(rule_name, identifier)
        assert status.allowed is False
    
    async def test_rate_limit_penalties(self, rate_limiter, db_connection):
        """Test rate limit penalty system."""
        rule_name = "test_penalty"
        identifier = "test_user_3"
        
        # Add test rule with penalty
        await db_connection.execute(
            """
            INSERT INTO auth.rate_limit_rules (rule_name, scope, algorithm, max_requests, window_seconds, penalty_seconds)
            VALUES ($1, 'user', 'sliding_window', 2, 60, 300)
            """,
            rule_name
        )
        
        rate_limiter.rules[rule_name] = type('Rule', (), {
            'algorithm': 'sliding_window',
            'max_requests': 2,
            'window_seconds': 60,
            'penalty_seconds': 300,
            'enabled': True
        })()
        
        # Exceed limit
        for i in range(3):
            status = await rate_limiter.check_rate_limit(rule_name, identifier)
            if i < 2:
                assert status.allowed is True
            else:
                assert status.allowed is False
        
        # Check penalty is applied
        penalty = await db_connection.fetchrow(
            "SELECT * FROM auth.rate_limit_penalties WHERE cache_key LIKE $1",
            f"rate_limit:{rule_name}:{identifier}"
        )
        assert penalty is not None
        assert penalty["penalty_until"] > datetime.now(timezone.utc)
    
    async def test_rate_limit_cleanup(self, rate_limiter, db_connection):
        """Test rate limit data cleanup."""
        # Insert old test data
        old_time = datetime.now(timezone.utc) - timedelta(days=2)
        
        await db_connection.execute(
            "INSERT INTO auth.rate_limit_requests (cache_key, timestamp) VALUES ($1, $2)",
            "test_key", old_time
        )
        
        await db_connection.execute(
            "INSERT INTO auth.rate_limit_penalties (cache_key, penalty_until) VALUES ($1, $2)",
            "test_penalty", old_time
        )
        
        # Run cleanup
        await rate_limiter.cleanup_expired_data()
        
        # Verify old data is removed
        old_requests = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.rate_limit_requests WHERE cache_key = $1",
            "test_key"
        )
        assert old_requests == 0
        
        old_penalties = await db_connection.fetchval(
            "SELECT COUNT(*) FROM auth.rate_limit_penalties WHERE cache_key = $1",
            "test_penalty"
        )
        assert old_penalties == 0


# Test configuration and fixtures
@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def setup_test_database():
    """Setup test database with clean state."""
    async with get_connection() as conn:
        # Clean up test data
        await conn.execute("DELETE FROM auth.users WHERE username LIKE 'test_%'")
        await conn.execute("DELETE FROM auth.rate_limit_requests WHERE cache_key LIKE 'test_%'")
        await conn.execute("DELETE FROM auth.rate_limit_penalties WHERE cache_key LIKE 'test_%'")
        await conn.execute("DELETE FROM auth.rate_limit_buckets WHERE cache_key LIKE 'test_%'")
        yield
        # Cleanup after test
        await conn.execute("DELETE FROM auth.users WHERE username LIKE 'test_%'")
        await conn.execute("DELETE FROM auth.rate_limit_requests WHERE cache_key LIKE 'test_%'")
        await conn.execute("DELETE FROM auth.rate_limit_penalties WHERE cache_key LIKE 'test_%'")
        await conn.execute("DELETE FROM auth.rate_limit_buckets WHERE cache_key LIKE 'test_%'")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])