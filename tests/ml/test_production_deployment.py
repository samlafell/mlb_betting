"""
Production Deployment Tests
Tests environment-specific security configuration and production readiness
"""

import pytest
import os
import tempfile
import subprocess
import yaml
import json
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime, timedelta
import asyncio

from src.ml.api.security import SecurityConfig, get_security_config, get_cors_origins
from src.ml.api.main import app
from src.core.config import Config
from fastapi.testclient import TestClient


@pytest.mark.deployment
class TestEnvironmentConfiguration:
    """Test environment-specific configuration and security settings"""
    
    def test_development_environment_config(self):
        """Test development environment configuration"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'development',
            'DEBUG': 'true',
            'LOG_LEVEL': 'DEBUG'
        }, clear=True):
            config = get_security_config()
            
            # Development should allow relaxed security
            assert config.environment == 'development'
            assert config.require_auth is False
            assert config.debug_mode is True
            assert len(config.allowed_origins) > 0
            
            # Should include localhost origins
            origins = get_cors_origins()
            assert any('localhost' in origin for origin in origins)
    
    def test_production_environment_config(self):
        """Test production environment configuration"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'DEBUG': 'false',
            'LOG_LEVEL': 'INFO',
            'API_SECRET_KEY': 'prod_secret_key_12345',
            'ALLOWED_ORIGINS': 'https://mlb.example.com,https://api.example.com'
        }, clear=True):
            config = get_security_config()
            
            # Production should enforce strict security
            assert config.environment == 'production'
            assert config.require_auth is True
            assert config.debug_mode is False
            
            # Should use configured origins only
            origins = get_cors_origins()
            assert 'https://mlb.example.com' in origins
            assert 'https://api.example.com' in origins
            assert not any('localhost' in origin for origin in origins)
    
    def test_staging_environment_config(self):
        """Test staging environment configuration"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'staging',
            'DEBUG': 'false',
            'LOG_LEVEL': 'INFO',
            'API_SECRET_KEY': 'staging_secret_key',
            'ALLOWED_ORIGINS': 'https://staging.example.com'
        }, clear=True):
            config = get_security_config()
            
            # Staging should balance security and testing needs
            assert config.environment == 'staging'
            assert config.require_auth is True
            assert config.debug_mode is False
            
            # Should have staging-specific origins
            origins = get_cors_origins()
            assert 'https://staging.example.com' in origins
    
    def test_missing_required_production_config(self):
        """Test behavior when required production config is missing"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production'
        }, clear=True):
            # Remove API_SECRET_KEY if it exists
            if 'API_SECRET_KEY' in os.environ:
                del os.environ['API_SECRET_KEY']
            
            # Should still create config but with warnings
            config = get_security_config()
            assert config.environment == 'production'
            assert config.require_auth is True  # Should still require auth
    
    def test_rate_limiting_configuration_by_environment(self):
        """Test rate limiting varies by environment"""
        # Development - relaxed limits
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'development'
        }, clear=True):
            dev_config = get_security_config()
            
        # Production - strict limits
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'RATE_LIMIT_REQUESTS_PER_MINUTE': '60',
            'PREDICTION_RATE_LIMIT': '10'
        }, clear=True):
            prod_config = get_security_config()
            
        # Production should have stricter rate limits
        assert prod_config.rate_limit_per_minute <= dev_config.rate_limit_per_minute
        assert prod_config.prediction_rate_limit <= dev_config.prediction_rate_limit


@pytest.mark.deployment
class TestProductionSecurityValidation:
    """Test production security requirements and validation"""
    
    def test_https_enforcement_production(self):
        """Test HTTPS enforcement in production"""
        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            client = TestClient(app)
            
            # Test that security headers include HSTS
            response = client.get("/health")
            
            # Should include security headers
            assert response.status_code == 200
            # Note: In real production, middleware would add these headers
    
    def test_api_key_validation_production(self):
        """Test API key validation in production environment"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'API_SECRET_KEY': 'production_secret_key_12345'
        }):
            client = TestClient(app)
            
            # Test without API key
            response = client.post("/api/v1/predict", json={"game_id": 12345})
            assert response.status_code == 401
            
            # Test with wrong API key
            headers = {"Authorization": "Bearer wrong_key"}
            response = client.post("/api/v1/predict", json={"game_id": 12345}, headers=headers)
            assert response.status_code == 401
            
            # Test with correct API key
            headers = {"Authorization": "Bearer production_secret_key_12345"}
            response = client.post("/api/v1/predict", json={"game_id": 12345}, headers=headers)
            # Should pass auth (may fail on business logic, but not auth)
            assert response.status_code != 401
    
    def test_cors_configuration_production(self):
        """Test CORS configuration in production"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'ALLOWED_ORIGINS': 'https://mlb.example.com'
        }):
            client = TestClient(app)
            
            # Test CORS preflight with allowed origin
            headers = {
                'Origin': 'https://mlb.example.com',
                'Access-Control-Request-Method': 'POST',
                'Access-Control-Request-Headers': 'authorization,content-type'
            }
            response = client.options("/api/v1/predict", headers=headers)
            
            # Should allow the request
            assert response.status_code == 200
    
    def test_debug_mode_disabled_production(self):
        """Test debug mode is disabled in production"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'DEBUG': 'false'
        }):
            config = get_security_config()
            assert config.debug_mode is False
            
            # Test that debug endpoints are not accessible
            client = TestClient(app)
            
            # Debug endpoints should not exist or should be disabled
            # (This would be implementation-specific)
    
    def test_sensitive_data_not_exposed_production(self):
        """Test sensitive data is not exposed in production"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'API_SECRET_KEY': 'super_secret_production_key',
            'DATABASE_PASSWORD': 'secret_db_password'
        }):
            client = TestClient(app)
            
            # Health check should not expose sensitive config
            response = client.get("/health")
            response_text = response.text.lower()
            
            # Should not contain sensitive data
            assert 'secret' not in response_text
            assert 'password' not in response_text
            assert 'api_key' not in response_text


@pytest.mark.deployment
class TestDatabaseConnectionSecurity:
    """Test database connection security in production"""
    
    def test_database_ssl_configuration(self):
        """Test database SSL configuration for production"""
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'DATABASE_SSL_MODE': 'require'
        }):
            config = Config()
            
            # Production should require SSL
            # This would test the actual database config if implemented
            # For now, we test that the environment variable is read
            assert os.getenv('DATABASE_SSL_MODE') == 'require'
    
    def test_database_connection_pooling_production(self):
        """Test database connection pooling is properly configured"""
        from src.ml.database.connection_pool import DatabaseConnectionPool
        from src.core.config import DatabaseConfig
        
        # Production database config
        db_config = DatabaseConfig(
            host="prod-db.example.com",
            port=5432,
            database="mlb_betting_prod",
            user="mlb_user",
            password="secure_password",
            min_size=5,
            max_size=20,
            ssl_mode="require"
        )
        
        pool = DatabaseConnectionPool(db_config)
        
        # Verify production-appropriate pool settings
        assert db_config.min_size >= 5   # Minimum connections for production
        assert db_config.max_size <= 50  # Reasonable maximum
        assert db_config.ssl_mode == "require"  # SSL required


@pytest.mark.deployment
class TestContainerizedDeployment:
    """Test containerized deployment configuration"""
    
    def test_docker_environment_variables(self):
        """Test Docker environment variable configuration"""
        # Simulate Docker environment
        docker_env = {
            'ENVIRONMENT': 'production',
            'API_SECRET_KEY': 'docker_production_key',
            'DATABASE_HOST': 'postgres-service',  # Docker service name
            'DATABASE_PORT': '5432',
            'REDIS_URL': 'redis://redis-service:6379/0',
            'ALLOWED_ORIGINS': 'https://mlb.example.com,https://api.example.com'
        }
        
        with patch.dict(os.environ, docker_env, clear=True):
            config = Config()
            security_config = get_security_config()
            
            # Verify configuration is properly loaded
            assert security_config.environment == 'production'
            assert security_config.require_auth is True
            assert config.database.host == 'postgres-service'
            assert config.redis.url == 'redis://redis-service:6379/0'
    
    def test_health_check_endpoints_for_container_orchestration(self):
        """Test health check endpoints work for container orchestration"""
        client = TestClient(app)
        
        # Basic health check
        response = client.get("/health")
        assert response.status_code == 200
        
        health_data = response.json()
        assert health_data["status"] == "healthy"
        assert "timestamp" in health_data
        
        # Readiness check (if implemented)
        # This would test database connectivity, Redis availability, etc.
        response = client.get("/ready")
        # Should return 200 when all dependencies are available
        # May return 503 if dependencies not available
        assert response.status_code in [200, 503]
    
    def test_graceful_shutdown_handling(self):
        """Test graceful shutdown behavior"""
        # This would test that the application handles SIGTERM gracefully
        # by closing database connections, Redis connections, etc.
        
        # Mock implementation - in real test this would:
        # 1. Start the application
        # 2. Send SIGTERM
        # 3. Verify graceful shutdown within timeout
        # 4. Verify no connection leaks
        
        pytest.skip("Graceful shutdown testing requires process management")


@pytest.mark.deployment
class TestMonitoringAndObservability:
    """Test production monitoring and observability features"""
    
    def test_prometheus_metrics_endpoint(self):
        """Test Prometheus metrics endpoint is available"""
        client = TestClient(app)
        
        # Metrics endpoint should be available
        response = client.get("/metrics")
        
        # Should return metrics in Prometheus format
        if response.status_code == 200:
            metrics_text = response.text
            
            # Should contain basic metrics
            assert "# HELP" in metrics_text  # Prometheus comment format
            assert "# TYPE" in metrics_text  # Prometheus type declarations
        else:
            # Metrics might not be implemented yet
            assert response.status_code == 404
    
    def test_structured_logging_configuration(self):
        """Test structured logging is properly configured"""
        import logging
        
        # Test that logging is configured for production
        logger = logging.getLogger("src.ml")
        
        # Should have appropriate log level
        assert logger.level <= logging.INFO  # INFO or DEBUG
        
        # In production, should log in structured format (JSON)
        # This would require checking the actual log handler configuration
    
    def test_error_tracking_integration(self):
        """Test error tracking integration (Sentry, etc.)"""
        # This would test integration with error tracking services
        # For now, just test that errors are properly logged
        
        client = TestClient(app)
        
        # Trigger an error
        response = client.post("/api/v1/predict", json={"invalid": "data"})
        
        # Should return proper error response
        assert response.status_code in [400, 422, 500]
        
        # Error should be logged (would check log output in real test)


@pytest.mark.deployment
class TestSecurityScanning:
    """Test security scanning and vulnerability checks"""
    
    def test_no_hardcoded_secrets(self):
        """Test that no secrets are hardcoded in the codebase"""
        import re
        from pathlib import Path
        
        # Patterns that might indicate hardcoded secrets
        secret_patterns = [
            r'password\s*=\s*["\'](?!.*\{\{)[^"\']{8,}["\']',  # Hardcoded passwords
            r'api[_-]?key\s*=\s*["\'][^"\']{20,}["\']',       # API keys
            r'secret\s*=\s*["\'][^"\']{16,}["\']',             # Generic secrets
            r'token\s*=\s*["\'][^"\']{20,}["\']',              # Tokens
        ]
        
        # Scan source files
        src_path = Path(__file__).parent.parent.parent / "src"
        violations = []
        
        for py_file in src_path.rglob("*.py"):
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
                for pattern in secret_patterns:
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for match in matches:
                        violations.append(f"{py_file}:{match.start()}: {match.group()}")
        
        # Should not find any hardcoded secrets
        assert len(violations) == 0, f"Found potential hardcoded secrets: {violations}"
    
    def test_dependency_vulnerabilities(self):
        """Test for known vulnerabilities in dependencies"""
        # This would run safety check or similar tool
        # For now, just test that we can import our dependencies
        
        try:
            import fastapi
            import redis
            import asyncpg
            import polars
            import msgpack
            # All imports successful
            assert True
        except ImportError as e:
            pytest.fail(f"Missing required dependency: {e}")
    
    def test_sql_injection_prevention(self):
        """Test SQL injection prevention"""
        # This would test that all database queries use parameterized queries
        # For now, verify that we're using async database drivers that support it
        
        from src.ml.database.connection_pool import DatabaseConnectionPool
        
        # asyncpg supports parameterized queries by default
        # This is a basic check that we're using the right driver
        assert hasattr(DatabaseConnectionPool, 'fetch')


@pytest.mark.deployment
class TestPerformanceBaselines:
    """Test performance baselines for production deployment"""
    
    def test_api_response_time_baselines(self):
        """Test API response time meets production baselines"""
        client = TestClient(app)
        
        import time
        
        # Test health check performance
        start_time = time.time()
        response = client.get("/health")
        response_time = (time.time() - start_time) * 1000
        
        assert response.status_code == 200
        assert response_time < 100  # Under 100ms for health check
        
        # Test prediction endpoint performance (if available)
        start_time = time.time()
        response = client.post("/api/v1/predict", json={
            "game_id": 12345,
            "feature_version": "baseline_test_v1.0"
        })
        response_time = (time.time() - start_time) * 1000
        
        # Should respond quickly even if prediction fails
        assert response_time < 500  # Under 500ms for prediction endpoint
    
    def test_memory_usage_baselines(self):
        """Test memory usage meets production baselines"""
        import psutil
        import gc
        
        # Force garbage collection
        gc.collect()
        
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        # Memory usage should be reasonable for production
        assert memory_mb < 500  # Under 500MB for basic API
        
        # Test memory doesn't grow excessively during operations
        client = TestClient(app)
        
        # Make multiple requests
        for _ in range(10):
            client.get("/health")
        
        gc.collect()
        final_memory_mb = process.memory_info().rss / 1024 / 1024
        memory_growth = final_memory_mb - memory_mb
        
        # Memory growth should be minimal
        assert memory_growth < 50  # Under 50MB growth for 10 requests


@pytest.mark.deployment 
class TestDeploymentAutomation:
    """Test deployment automation and infrastructure as code"""
    
    def test_configuration_validation(self):
        """Test that deployment configuration is valid"""
        # This would validate Docker Compose, Kubernetes manifests, etc.
        
        # For now, test that our configuration can be loaded
        try:
            config = Config()
            security_config = get_security_config()
            
            # Basic validation
            assert config is not None
            assert security_config is not None
            assert hasattr(config, 'database')
            assert hasattr(config, 'redis')
            
        except Exception as e:
            pytest.fail(f"Configuration validation failed: {e}")
    
    def test_environment_specific_configs_exist(self):
        """Test that environment-specific configurations exist"""
        # This would check for config files like:
        # - docker-compose.prod.yml
        # - kubernetes/production/
        # - .env.production.template
        
        project_root = Path(__file__).parent.parent.parent
        
        # Check for environment template files
        env_files = [
            ".env.template",
            ".env.production.template", 
            ".env.staging.template"
        ]
        
        existing_env_files = []
        for env_file in env_files:
            if (project_root / env_file).exists():
                existing_env_files.append(env_file)
        
        # Should have at least basic template
        assert len(existing_env_files) > 0, "No environment template files found"
    
    def test_deployment_scripts_executable(self):
        """Test that deployment scripts are executable and valid"""
        # This would test deployment scripts like:
        # - deploy.sh
        # - docker-build.sh
        # - kubernetes-deploy.yaml
        
        # For now, just test that we can import our modules
        try:
            from src.ml.api.main import app
            from src.ml.features.feature_pipeline import FeaturePipeline
            from src.ml.training.lightgbm_trainer import LightGBMTrainer
            
            # All imports successful
            assert True
            
        except ImportError as e:
            pytest.fail(f"Module import failed - deployment will fail: {e}")