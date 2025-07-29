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
    RateLimiter,
    SecurityHeaders,
    create_break_glass_dependency
)


class TestRateLimiter:
    """Test rate limiter functionality."""
    
    def test_rate_limiter_allows_under_limit(self):
        """Test that rate limiter allows requests under limit."""
        limiter = RateLimiter()
        
        # Should allow requests under limit
        assert limiter.is_allowed("test_key", 5) is True
        assert limiter.is_allowed("test_key", 5) is True
        assert limiter.is_allowed("test_key", 5) is True
        
    def test_rate_limiter_blocks_over_limit(self):
        """Test that rate limiter blocks requests over limit."""
        limiter = RateLimiter()
        
        # Fill up the limit
        for _ in range(5):
            assert limiter.is_allowed("test_key", 5) is True
            
        # Should block the next request
        assert limiter.is_allowed("test_key", 5) is False
        
    def test_rate_limiter_different_keys(self):
        """Test that rate limiter treats different keys separately."""
        limiter = RateLimiter()
        
        # Fill up one key
        for _ in range(5):
            assert limiter.is_allowed("key1", 5) is True
            
        # Other key should still work
        assert limiter.is_allowed("key2", 5) is True


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
        with patch('src.core.security.get_settings') as mock_settings:
            mock_settings.return_value.security.enable_authentication = False
            
            result = await verify_api_key(mock_request, None)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_verify_api_key_no_config(self, mock_request):
        """Test API key verification when no API key is configured."""
        with patch('src.core.security.get_settings') as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = None
            
            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(mock_request, None)
            
            assert exc_info.value.status_code == 503
    
    @pytest.mark.asyncio
    async def test_verify_api_key_missing_key(self, mock_request):
        """Test API key verification when no key is provided."""
        with patch('src.core.security.get_settings') as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "test-key"
            
            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(mock_request, None)
            
            assert exc_info.value.status_code == 401
    
    @pytest.mark.asyncio
    async def test_verify_api_key_invalid_key(self, mock_request):
        """Test API key verification with invalid key."""
        with patch('src.core.security.get_settings') as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"
            
            credentials = HTTPAuthorizationCredentials(
                scheme="Bearer",
                credentials="wrong-key"
            )
            
            with pytest.raises(HTTPException) as exc_info:
                await verify_api_key(mock_request, credentials)
            
            assert exc_info.value.status_code == 401
    
    @pytest.mark.asyncio
    async def test_verify_api_key_valid_key(self, mock_request):
        """Test API key verification with valid key."""
        with patch('src.core.security.get_settings') as mock_settings:
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"
            
            credentials = HTTPAuthorizationCredentials(
                scheme="Bearer",
                credentials="correct-key"
            )
            
            result = await verify_api_key(mock_request, credentials)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_verify_api_key_x_api_key_header(self, mock_request):
        """Test API key verification using X-API-Key header."""
        mock_request.headers = {"X-API-Key": "correct-key"}
        
        with patch('src.core.security.get_settings') as mock_settings:
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
        return request
    
    @pytest.mark.asyncio
    async def test_check_rate_limit_disabled(self, mock_request):
        """Test rate limiting when disabled."""
        with patch('src.core.security.get_settings') as mock_settings:
            mock_settings.return_value.security.enable_rate_limiting = False
            
            result = await check_rate_limit(mock_request)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_check_rate_limit_under_limit(self, mock_request):
        """Test rate limiting under limit."""
        with patch('src.core.security.get_settings') as mock_settings, \
             patch('src.core.security.rate_limiter') as mock_limiter:
            
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter.is_allowed.return_value = True
            
            result = await check_rate_limit(mock_request)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_check_rate_limit_over_limit(self, mock_request):
        """Test rate limiting over limit."""
        with patch('src.core.security.get_settings') as mock_settings, \
             patch('src.core.security.rate_limiter') as mock_limiter:
            
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter.is_allowed.return_value = False
            
            with pytest.raises(HTTPException) as exc_info:
                await check_rate_limit(mock_request)
            
            assert exc_info.value.status_code == 429


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
        request.headers = {"X-API-Key": "correct-key"}
        return request
    
    @pytest.mark.asyncio
    async def test_break_glass_dependency_success(self, mock_request):
        """Test successful break-glass authentication."""
        dependency = create_break_glass_dependency()
        
        with patch('src.core.security.get_settings') as mock_settings, \
             patch('src.core.security.rate_limiter') as mock_limiter:
            
            # Setup mocks
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter.is_allowed.return_value = True
            
            result = await dependency(mock_request, None)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_break_glass_dependency_rate_limited(self, mock_request):
        """Test break-glass dependency with rate limiting."""
        dependency = create_break_glass_dependency()
        
        with patch('src.core.security.get_settings') as mock_settings, \
             patch('src.core.security.rate_limiter') as mock_limiter:
            
            # Setup mocks
            mock_settings.return_value.security.enable_rate_limiting = True
            mock_settings.return_value.security.enable_authentication = True
            mock_settings.return_value.security.dashboard_api_key = "correct-key"
            mock_settings.return_value.security.break_glass_rate_limit = 5
            mock_limiter.is_allowed.return_value = False
            
            with pytest.raises(HTTPException) as exc_info:
                await dependency(mock_request, None)
            
            assert exc_info.value.status_code == 429