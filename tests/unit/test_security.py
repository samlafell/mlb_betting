"""
Unit tests for security module.

Tests authentication, rate limiting, and security headers functionality.
"""

import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials

from src.core.security import (
    verify_api_key,
    check_rate_limit,
    check_ip_whitelist,
    RateLimiter,
    SecurityHeaders,
    create_break_glass_dependency,
)


class TestRateLimiter:
    """Test rate limiter functionality."""

    def test_memory_rate_limiter_allows_under_limit(self):
        """Test that memory rate limiter allows requests under limit."""
        limiter = RateLimiter(use_redis=False)

        # Should allow requests under limit
        assert limiter.is_allowed("test_key", 5) is True
        assert limiter.is_allowed("test_key", 5) is True
        assert limiter.is_allowed("test_key", 5) is True

    def test_memory_rate_limiter_blocks_over_limit(self):
        """Test that memory rate limiter blocks requests over limit."""
        limiter = RateLimiter(use_redis=False)

        # Fill up the limit
        for _ in range(5):
            assert limiter.is_allowed("test_key", 5) is True

        # Should block the next request
        assert limiter.is_allowed("test_key", 5) is False

    def test_memory_rate_limiter_different_keys(self):
        """Test that memory rate limiter treats different keys separately."""
        limiter = RateLimiter(use_redis=False)

        # Fill up one key
        for _ in range(5):
            assert limiter.is_allowed("key1", 5) is True

        # Other key should still work
        assert limiter.is_allowed("key2", 5) is True

    def test_redis_fallback_when_no_client(self):
        """Test that Redis rate limiter falls back to memory when no client provided."""
        limiter = RateLimiter(use_redis=True, redis_client=None)

        # Should fall back to memory mode
        assert limiter._use_redis is False

    def test_redis_rate_limiter_with_mock_client(self):
        """Test Redis rate limiter with mock client."""
        from unittest.mock import Mock

        mock_redis = Mock()
        mock_pipeline = Mock()
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.return_value = [None, None, 3, None]  # zcard returns 3

        limiter = RateLimiter(use_redis=True, redis_client=mock_redis)

        # Should allow request under limit
        result = limiter.is_allowed("test_key", 5)
        assert result is True

        # Verify Redis operations were called
        mock_redis.pipeline.assert_called_once()
        mock_pipeline.zadd.assert_called_once()
        mock_pipeline.zremrangebyscore.assert_called_once()
        mock_pipeline.zcard.assert_called_once()
        mock_pipeline.expire.assert_called_once()


class TestAPIKeyVerification:
    """Test API key verification functionality."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request object."""
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "127.0.0.1"
        request.url = Mock()
        request.url.path = "/api/control/test"
        request.headers = {}
        return request

    @pytest.mark.asyncio
    async def test_verify_api_key_disabled_auth(self, mock_request):
        """Test API key verification when authentication is disabled."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_authentication = False

            result = await verify_api_key(mock_request, None)
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_api_key_no_config(self, mock_request):
        """Test API key verification when no API key is configured."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = None

            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(mock_request, None)

            assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_verify_api_key_missing_key(self, mock_request):
        """Test API key verification when no key is provided."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "test-key"

            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(mock_request, None)

            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_verify_api_key_invalid_key(self, mock_request):
        """Test API key verification with invalid key."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"

            credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials="wrong-key"
            )

            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(mock_request, credentials)

            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_verify_api_key_valid_key(self, mock_request):
        """Test API key verification with valid key."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"

            credentials = HTTPAuthorizationCredentials(
                scheme="Bearer", credentials="correct-key"
            )

            result = await verify_api_key(mock_request, credentials)
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_api_key_x_api_key_header(self, mock_request):
        """Test API key verification using X-API-Key header."""
        mock_request.headers = Mock()
        mock_request.headers.get = Mock(
            side_effect=lambda key, default=None: {"X-API-Key": "correct-key"}.get(
                key, default
            )
        )

        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"

            result = await verify_api_key(mock_request, None)
            assert result is True


class TestRateLimiting:
    """Test rate limiting functionality."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request object."""
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "127.0.0.1"
        request.url = Mock()
        request.url.path = "/api/control/test"
        request.headers = Mock()
        request.headers.get = Mock(return_value=None)
        return request

    @pytest.mark.asyncio
    async def test_check_rate_limit_disabled(self, mock_request):
        """Test rate limiting when disabled."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_rate_limiting = False

            result = await check_rate_limit(mock_request)
            assert result is True

    @pytest.mark.asyncio
    async def test_check_rate_limit_under_limit(self, mock_request):
        """Test rate limiting under limit."""
        with (
            patch("src.core.config.get_settings") as mock_settings,
            patch("src.core.security.get_rate_limiter") as mock_get_limiter,
        ):
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter = Mock()
            mock_limiter.is_allowed.return_value = True
            mock_get_limiter.return_value = mock_limiter

            result = await check_rate_limit(mock_request)
            assert result is True

    @pytest.mark.asyncio
    async def test_check_rate_limit_over_limit(self, mock_request):
        """Test rate limiting over limit."""
        with (
            patch("src.core.config.get_settings") as mock_settings,
            patch("src.core.security.get_rate_limiter") as mock_get_limiter,
        ):
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter = Mock()
            mock_limiter.is_allowed.return_value = False
            mock_get_limiter.return_value = mock_limiter

            with pytest.raises(HTTPException) as exc_info:
                await check_rate_limit(mock_request)

            assert exc_info.value.status_code == 429


class TestIPWhitelist:
    """Test IP whitelisting functionality."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request object."""
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "192.168.1.100"
        request.url = Mock()
        request.url.path = "/api/control/test"
        request.headers = Mock()
        request.headers.get = Mock(return_value=None)
        return request

    def test_check_ip_whitelist_disabled(self, mock_request):
        """Test IP whitelist when disabled."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_ip_whitelisting = False

            result = check_ip_whitelist(mock_request)
            assert result is True

    def test_check_ip_whitelist_allowed_ip(self, mock_request):
        """Test IP whitelist with allowed IP."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_ip_whitelisting = True
            mock_settings.return_value.security.break_glass_ip_whitelist = [
                "192.168.1.100",
                "127.0.0.1",
            ]

            result = check_ip_whitelist(mock_request)
            assert result is True

    def test_check_ip_whitelist_blocked_ip(self, mock_request):
        """Test IP whitelist with blocked IP."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_ip_whitelisting = True
            mock_settings.return_value.security.break_glass_ip_whitelist = [
                "127.0.0.1",
                "10.0.0.1",
            ]

            with pytest.raises(HTTPException) as exc_info:
                check_ip_whitelist(mock_request)

            assert exc_info.value.status_code == 403

    def test_check_ip_whitelist_cidr_range(self, mock_request):
        """Test IP whitelist with CIDR range."""
        with patch("src.core.config.get_settings") as mock_settings:
            mock_settings.return_value.security.enable_ip_whitelisting = True
            mock_settings.return_value.security.break_glass_ip_whitelist = [
                "192.168.1.0/24"
            ]

            result = check_ip_whitelist(mock_request)
            assert result is True


class TestSecurityHeaders:
    """Test security headers functionality."""

    def test_get_security_headers(self):
        """Test security headers generation."""
        headers = SecurityHeaders.get_security_headers()

        expected_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Content-Security-Policy": "default-src 'self'",
        }

        assert headers == expected_headers


class TestBreakGlassDependency:
    """Test break-glass dependency functionality."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock request object."""
        request = Mock(spec=Request)
        request.client = Mock()
        request.client.host = "127.0.0.1"
        request.url = Mock()
        request.url.path = "/api/control/test"
        request.headers = Mock()
        request.headers.get = Mock(
            side_effect=lambda key, default=None: {"X-API-Key": "correct-key"}.get(
                key, default
            )
        )
        return request

    @pytest.mark.asyncio
    async def test_break_glass_dependency_success(self, mock_request):
        """Test successful break-glass authentication."""
        dependency = create_break_glass_dependency()

        with (
            patch("src.core.config.get_settings") as mock_settings,
            patch("src.core.security.get_rate_limiter") as mock_get_limiter,
        ):
            # Setup mocks
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.enable_ip_whitelisting = False
            mock_settings.return_value.security.dashboard_api_key = "correct-key"
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter = Mock()
            mock_limiter.is_allowed.return_value = True
            mock_get_limiter.return_value = mock_limiter

            result = await dependency(mock_request, None)
            assert result is True

    @pytest.mark.asyncio
    async def test_break_glass_dependency_rate_limited(self, mock_request):
        """Test break-glass dependency with rate limiting."""
        dependency = create_break_glass_dependency()

        with (
            patch("src.core.config.get_settings") as mock_settings,
            patch("src.core.security.get_rate_limiter") as mock_get_limiter,
        ):
            # Setup mocks
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.enable_ip_whitelisting = False
            mock_settings.return_value.security.dashboard_api_key = "correct-key"
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter = Mock()
            mock_limiter.is_allowed.return_value = False
            mock_get_limiter.return_value = mock_limiter

            with pytest.raises(HTTPException) as exc_info:
                await dependency(mock_request, None)

            assert exc_info.value.status_code == 429
