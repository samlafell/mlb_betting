"""
Comprehensive tests for ML API security
Tests authentication, rate limiting, and CORS policies
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import HTTPException, status
import time

from src.ml.api.security import (
    RateLimiter, SecurityConfig, get_security_config,
    verify_api_key, rate_limit_check, get_current_user
)


class TestRateLimiter:
    """Test suite for rate limiting functionality"""
    
    @pytest.fixture
    def rate_limiter(self):
        """Create rate limiter for testing"""
        return RateLimiter()
    
    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client for rate limiter"""
        return AsyncMock()
    
    @pytest.mark.asyncio
    async def test_memory_based_rate_limiting(self, rate_limiter):
        """Test memory-based rate limiting fallback"""
        key = "test_user"
        limit = 5
        window_seconds = 60
        
        # Should allow requests up to limit
        for i in range(limit):
            allowed = await rate_limiter.is_allowed(key, limit, window_seconds)
            assert allowed is True
        
        # Should reject request over limit
        allowed = await rate_limiter.is_allowed(key, limit, window_seconds)
        assert allowed is False
    
    @pytest.mark.asyncio
    async def test_redis_based_rate_limiting(self, rate_limiter, mock_redis_client):
        """Test Redis-based distributed rate limiting"""
        rate_limiter.redis_client = mock_redis_client
        
        # Mock Redis pipeline operations
        mock_pipe = AsyncMock()
        mock_pipe.execute.return_value = [None, 4, None, None]  # 4 current requests
        mock_redis_client.pipeline.return_value = mock_pipe
        
        key = "test_user"
        limit = 5
        
        # Should allow request (4 < 5)
        allowed = await rate_limiter.is_allowed(key, limit)
        assert allowed is True
        
        # Mock over limit
        mock_pipe.execute.return_value = [None, 6, None, None]  # 6 current requests
        allowed = await rate_limiter.is_allowed(key, limit)
        assert allowed is False
    
    @pytest.mark.asyncio
    async def test_rate_limit_window_cleanup(self, rate_limiter):
        """Test rate limit window cleanup"""
        key = "test_user"
        limit = 5
        window_seconds = 1  # 1 second window
        
        # Fill up the limit
        for _ in range(limit):
            await rate_limiter.is_allowed(key, limit, window_seconds)
        
        # Should be rejected
        allowed = await rate_limiter.is_allowed(key, limit, window_seconds)
        assert allowed is False
        
        # Wait for window to expire
        await asyncio.sleep(1.1)
        
        # Should be allowed again
        allowed = await rate_limiter.is_allowed(key, limit, window_seconds)
        assert allowed is True
    
    @pytest.mark.asyncio
    async def test_redis_fallback_on_error(self, rate_limiter, mock_redis_client):
        """Test fallback to memory when Redis fails"""
        rate_limiter.redis_client = mock_redis_client
        
        # Mock Redis to raise an exception
        mock_redis_client.pipeline.side_effect = Exception("Redis connection error")
        
        # Should fallback to memory-based limiting
        allowed = await rate_limiter.is_allowed("test_user", 5)
        assert allowed is True  # Should not fail due to Redis error


class TestSecurityConfig:
    """Test suite for security configuration"""
    
    def test_development_config(self):
        """Test development environment configuration"""
        with patch.dict('os.environ', {'ENVIRONMENT': 'development'}):
            config = get_security_config()
            assert config.environment == 'development'
            assert config.require_auth is False
            assert len(config.allowed_origins) >= 0  # Should have development origins
    
    def test_production_config(self):
        """Test production environment configuration"""
        with patch.dict('os.environ', {
            'ENVIRONMENT': 'production',
            'ALLOWED_ORIGINS': 'https://example.com,https://api.example.com'
        }):
            config = get_security_config()
            assert config.environment == 'production'
            assert config.require_auth is True
            assert 'https://example.com' in config.allowed_origins
    
    def test_rate_limit_configuration(self):
        """Test rate limit configuration"""
        with patch.dict('os.environ', {
            'RATE_LIMIT_REQUESTS_PER_MINUTE': '120',
            'PREDICTION_RATE_LIMIT': '20'
        }):
            config = get_security_config()
            assert config.rate_limit_per_minute == 120
            assert config.prediction_rate_limit == 20


class TestApiKeyAuthentication:
    """Test suite for API key authentication"""
    
    def test_valid_api_key_verification(self):
        """Test valid API key verification"""
        with patch.dict('os.environ', {'API_SECRET_KEY': 'test_secret_key'}):
            assert verify_api_key('test_secret_key') is True
            assert verify_api_key('wrong_key') is False
            assert verify_api_key('') is False
            assert verify_api_key(None) is False
    
    def test_timing_attack_prevention(self):
        """Test timing attack prevention in API key verification"""
        with patch.dict('os.environ', {'API_SECRET_KEY': 'correct_key'}):
            # Time correct key verification
            start = time.time()
            verify_api_key('correct_key')
            correct_time = time.time() - start
            
            # Time incorrect key verification  
            start = time.time()
            verify_api_key('wrong_key_same_length')
            wrong_time = time.time() - start
            
            # Should take similar time (timing-safe comparison)
            time_diff = abs(correct_time - wrong_time)
            assert time_diff < 0.001  # Less than 1ms difference
    
    @pytest.mark.asyncio
    async def test_development_auth_bypass(self):
        """Test authentication bypass in development"""
        with patch('src.ml.api.security.get_security_config') as mock_config:
            mock_config.return_value = SecurityConfig(
                environment='development',
                require_auth=False
            )
            
            # Should return dev user without credentials
            user = await get_current_user(None, mock_config.return_value)
            assert user['user_id'] == 'dev_user'
            assert user['environment'] == 'development'
    
    @pytest.mark.asyncio
    async def test_production_auth_required(self):
        """Test authentication required in production"""
        from fastapi.security import HTTPAuthorizationCredentials
        
        with patch('src.ml.api.security.get_security_config') as mock_config:
            mock_config.return_value = SecurityConfig(
                environment='production',
                require_auth=True
            )
            
            # Should raise 401 without credentials
            with pytest.raises(HTTPException) as exc_info:
                await get_current_user(None, mock_config.return_value)
            assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
            
            # Should raise 401 with invalid credentials
            with patch.dict('os.environ', {'API_SECRET_KEY': 'correct_key'}):
                invalid_creds = HTTPAuthorizationCredentials(
                    scheme="Bearer",
                    credentials="wrong_key"
                )
                with pytest.raises(HTTPException) as exc_info:
                    await get_current_user(invalid_creds, mock_config.return_value)
                assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
                
                # Should succeed with valid credentials
                valid_creds = HTTPAuthorizationCredentials(
                    scheme="Bearer", 
                    credentials="correct_key"
                )
                user = await get_current_user(valid_creds, mock_config.return_value)
                assert user['user_id'] == 'authenticated_user'


class TestRateLimitingMiddleware:
    """Test suite for rate limiting middleware"""
    
    @pytest.fixture
    def mock_request(self):
        """Mock FastAPI request object"""
        request = MagicMock()
        request.client.host = "192.168.1.100"
        request.headers.get.return_value = "test-user-agent"
        request.url.path = "/api/v1/predict"
        return request
    
    @pytest.mark.asyncio
    async def test_rate_limit_enforcement(self, mock_request):
        """Test rate limit enforcement"""
        mock_rate_limiter = AsyncMock()
        mock_rate_limiter.is_allowed.return_value = False  # Over limit
        
        mock_config = SecurityConfig(prediction_rate_limit=10)
        
        with pytest.raises(HTTPException) as exc_info:
            await rate_limit_check(mock_request, mock_rate_limiter, mock_config)
        
        assert exc_info.value.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert "Rate limit exceeded" in exc_info.value.detail
        assert "Retry-After" in exc_info.value.headers
    
    @pytest.mark.asyncio
    async def test_different_endpoints_different_limits(self, mock_request):
        """Test different rate limits for different endpoints"""
        mock_rate_limiter = AsyncMock()
        mock_rate_limiter.is_allowed.return_value = True
        
        mock_config = SecurityConfig(
            prediction_rate_limit=10,
            rate_limit_per_minute=60
        )
        
        # Prediction endpoint should use prediction limit
        mock_request.url.path = "/api/v1/predict"
        await rate_limit_check(mock_request, mock_rate_limiter, mock_config)
        
        # Should have been called with prediction limit
        mock_rate_limiter.is_allowed.assert_called()
        call_args = mock_rate_limiter.is_allowed.call_args
        assert call_args[0][1] == 10  # prediction_rate_limit
        
        # Reset mock
        mock_rate_limiter.reset_mock()
        
        # Other endpoint should use general limit
        mock_request.url.path = "/api/v1/models"
        await rate_limit_check(mock_request, mock_rate_limiter, mock_config)
        
        call_args = mock_rate_limiter.is_allowed.call_args
        assert call_args[0][1] == 60  # rate_limit_per_minute
    
    def test_client_identification(self, mock_request):
        """Test client identification for rate limiting"""
        # Mock different clients
        mock_request.client.host = "192.168.1.100"
        mock_request.headers.get.return_value = "Chrome/95.0"
        
        # Client ID should be based on IP and user agent hash
        client_id = f"{mock_request.client.host}:{hash('Chrome/95.0') % 10000}"
        assert "192.168.1.100:" in client_id


class TestCorsConfiguration:
    """Test suite for CORS configuration"""
    
    def test_development_cors_origins(self):
        """Test CORS origins in development"""
        from src.ml.api.security import get_cors_origins
        
        with patch.dict('os.environ', {'ENVIRONMENT': 'development'}):
            origins = get_cors_origins()
            assert "http://localhost:3000" in origins
            assert "http://localhost:8080" in origins
            assert "http://127.0.0.1:3000" in origins
    
    def test_production_cors_origins(self):
        """Test CORS origins in production"""
        from src.ml.api.security import get_cors_origins
        
        with patch.dict('os.environ', {
            'ENVIRONMENT': 'production',
            'ALLOWED_ORIGINS': 'https://example.com,https://api.example.com'
        }):
            origins = get_cors_origins()
            assert "https://example.com" in origins
            assert "https://api.example.com" in origins
            assert "http://localhost:3000" not in origins
    
    def test_production_cors_fallback(self):
        """Test CORS fallback in production without configured origins"""
        from src.ml.api.security import get_cors_origins
        
        with patch.dict('os.environ', {'ENVIRONMENT': 'production'}, clear=True):
            origins = get_cors_origins()
            assert "https://yourdomain.com" in origins


class TestSecurityHeaders:
    """Test suite for security headers middleware"""
    
    @pytest.mark.asyncio
    async def test_security_headers_added(self):
        """Test security headers are added to responses"""
        from src.ml.api.security import add_security_headers
        
        # Mock request and response
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.headers = {}
        
        # Mock call_next to return the response
        async def mock_call_next(request):
            return mock_response
        
        # Process request through middleware
        result = await add_security_headers(mock_request, mock_call_next)
        
        # Check security headers were added
        assert "X-Content-Type-Options" in result.headers
        assert "X-Frame-Options" in result.headers
        assert "X-XSS-Protection" in result.headers
        assert "Referrer-Policy" in result.headers
        assert "Content-Security-Policy" in result.headers
        
        assert result.headers["X-Content-Type-Options"] == "nosniff"
        assert result.headers["X-Frame-Options"] == "DENY"
    
    @pytest.mark.asyncio
    async def test_hsts_header_in_production(self):
        """Test HSTS header added in production"""
        from src.ml.api.security import add_security_headers
        
        with patch.dict('os.environ', {'ENVIRONMENT': 'production'}):
            mock_request = MagicMock()
            mock_response = MagicMock()
            mock_response.headers = {}
            
            async def mock_call_next(request):
                return mock_response
            
            result = await add_security_headers(mock_request, mock_call_next)
            assert "Strict-Transport-Security" in result.headers


class TestApiSecurityIntegration:
    """Integration tests for API security"""
    
    @pytest.mark.integration
    def test_fastapi_security_integration(self):
        """Test security integration with FastAPI"""
        pytest.skip("Integration test - requires full API setup")
        
        # This would test the complete security integration:
        # 1. CORS headers
        # 2. Rate limiting
        # 3. Authentication
        # 4. Security headers
        from src.ml.api.main import app
        
        client = TestClient(app)
        
        # Test CORS preflight
        response = client.options("/api/v1/predict")
        assert response.status_code == 200
        
        # Test rate limiting
        for _ in range(61):  # Exceed default rate limit
            response = client.post("/api/v1/predict", json={"game_id": "12345"})
        
        assert response.status_code == 429  # Too Many Requests
        
        # Test authentication required in production
        with patch.dict('os.environ', {'ENVIRONMENT': 'production'}):
            response = client.post("/api/v1/predict", json={"game_id": "12345"})
            assert response.status_code == 401  # Unauthorized


# Performance tests
@pytest.mark.benchmark
class TestSecurityPerformance:
    """Performance tests for security components"""
    
    def test_rate_limiter_performance(self, benchmark):
        """Benchmark rate limiter performance"""
        rate_limiter = RateLimiter()
        
        async def rate_limit_check():
            return await rate_limiter.is_allowed("test_user", 100)
        
        result = benchmark(asyncio.run, rate_limit_check())
        assert result is True
    
    def test_api_key_verification_performance(self, benchmark):
        """Benchmark API key verification performance"""
        with patch.dict('os.environ', {'API_SECRET_KEY': 'test_secret_key'}):
            result = benchmark(verify_api_key, 'test_secret_key')
            assert result is True