#!/usr/bin/env python3
"""
Unit Tests for MLflow Configuration

Tests MLflow configuration settings without importing the full service.
"""

import pytest
from unittest.mock import MagicMock, patch

from src.core.config import get_settings, MLflowSettings


class TestMLflowConfiguration:
    """Unit tests for MLflow configuration settings."""

    def test_mlflow_settings_defaults(self):
        """Test MLflow settings have correct default values"""
        mlflow_settings = MLflowSettings()
        
        assert mlflow_settings.tracking_uri == "http://localhost:5001"
        assert mlflow_settings.max_retries == 3
        assert mlflow_settings.retry_delay == 1.0
        assert mlflow_settings.connection_timeout == 30
        assert mlflow_settings.default_experiment_name == "mlb_betting_experiments"
        assert mlflow_settings.artifact_root is None
        assert mlflow_settings.backend_store_uri is None

    def test_mlflow_effective_tracking_uri(self):
        """Test effective tracking URI computation"""
        # Test with only tracking_uri
        mlflow_settings = MLflowSettings()
        assert mlflow_settings.effective_tracking_uri == "http://localhost:5001"
        
        # Test with backend_store_uri override
        mlflow_settings = MLflowSettings(
            tracking_uri="http://localhost:5001",
            backend_store_uri="postgresql://user:pass@host:5432/db"
        )
        assert mlflow_settings.effective_tracking_uri == "postgresql://user:pass@host:5432/db"

    def test_mlflow_settings_environment_variables(self):
        """Test MLflow settings can be configured via environment variables"""
        import os
        from unittest.mock import patch
        
        env_vars = {
            "MLFLOW_TRACKING_URI": "http://test-mlflow:8080",
            "MLFLOW_MAX_RETRIES": "5",
            "MLFLOW_RETRY_DELAY": "2.0",
            "MLFLOW_CONNECTION_TIMEOUT": "60",
            "MLFLOW_DEFAULT_ARTIFACT_ROOT": "/test/artifacts"
        }
        
        with patch.dict(os.environ, env_vars):
            mlflow_settings = MLflowSettings()
            
            assert mlflow_settings.tracking_uri == "http://test-mlflow:8080"
            assert mlflow_settings.max_retries == 5
            assert mlflow_settings.retry_delay == 2.0
            assert mlflow_settings.connection_timeout == 60
            assert mlflow_settings.artifact_root == "/test/artifacts"

    def test_mlflow_settings_validation(self):
        """Test MLflow settings validation"""
        # Test valid settings
        valid_settings = MLflowSettings(
            max_retries=5,
            retry_delay=0.5,
            connection_timeout=120
        )
        assert valid_settings.max_retries == 5
        assert valid_settings.retry_delay == 0.5
        assert valid_settings.connection_timeout == 120
        
        # Test invalid retry count
        with pytest.raises(ValueError):
            MLflowSettings(max_retries=0)  # Below minimum
            
        with pytest.raises(ValueError):
            MLflowSettings(max_retries=15)  # Above maximum
            
        # Test invalid retry delay
        with pytest.raises(ValueError):
            MLflowSettings(retry_delay=0.05)  # Below minimum
            
        with pytest.raises(ValueError):
            MLflowSettings(retry_delay=15.0)  # Above maximum
            
        # Test invalid connection timeout
        with pytest.raises(ValueError):
            MLflowSettings(connection_timeout=1)  # Below minimum
            
        with pytest.raises(ValueError):
            MLflowSettings(connection_timeout=200)  # Above maximum

    def test_unified_settings_includes_mlflow(self):
        """Test that unified settings includes MLflow configuration"""
        settings = get_settings()
        
        assert hasattr(settings, 'mlflow')
        assert isinstance(settings.mlflow, MLflowSettings)
        assert settings.mlflow.tracking_uri == "http://localhost:5001"

    def test_mlflow_config_integration_with_settings(self):
        """Test that MLflow configuration integrates properly with settings"""
        settings = get_settings()
        
        # Test that MLflow settings exist
        assert hasattr(settings, 'mlflow')
        assert hasattr(settings.mlflow, 'tracking_uri')
        assert hasattr(settings.mlflow, 'max_retries')
        assert hasattr(settings.mlflow, 'retry_delay')
        assert hasattr(settings.mlflow, 'connection_timeout')
        assert hasattr(settings.mlflow, 'effective_tracking_uri')
        
        # Test that values are reasonable (may be overridden by environment)
        assert isinstance(settings.mlflow.tracking_uri, str)
        assert settings.mlflow.tracking_uri.startswith('http')  # Should be a valid HTTP URL
        assert settings.mlflow.max_retries >= 1
        assert settings.mlflow.retry_delay > 0
        assert settings.mlflow.connection_timeout > 0
        
        # Test computed field exists and works
        assert hasattr(settings.mlflow, 'effective_tracking_uri')
        assert isinstance(settings.mlflow.effective_tracking_uri, str)
        assert settings.mlflow.effective_tracking_uri.startswith('http') or settings.mlflow.effective_tracking_uri.startswith('postgresql')

    def test_mlflow_settings_serialization(self):
        """Test MLflow settings can be serialized properly"""
        mlflow_settings = MLflowSettings(
            tracking_uri="http://custom:5001",
            max_retries=5,
            retry_delay=2.0
        )
        
        # Test that settings can be converted to dict
        settings_dict = mlflow_settings.model_dump()
        
        assert settings_dict['tracking_uri'] == "http://custom:5001"
        assert settings_dict['max_retries'] == 5
        assert settings_dict['retry_delay'] == 2.0
        assert 'effective_tracking_uri' in settings_dict  # Computed field included

    def test_mlflow_config_with_custom_values(self):
        """Test MLflow configuration with custom values"""
        custom_mlflow = MLflowSettings(
            tracking_uri="http://production-mlflow:5001",
            max_retries=5,
            retry_delay=0.5,
            connection_timeout=60,
            default_experiment_name="production_experiments",
            artifact_root="/prod/artifacts",
            backend_store_uri="postgresql://mlflow:password@db:5432/mlflow"
        )
        
        assert custom_mlflow.tracking_uri == "http://production-mlflow:5001"
        assert custom_mlflow.max_retries == 5
        assert custom_mlflow.retry_delay == 0.5
        assert custom_mlflow.connection_timeout == 60
        assert custom_mlflow.default_experiment_name == "production_experiments"
        assert custom_mlflow.artifact_root == "/prod/artifacts"
        assert custom_mlflow.backend_store_uri == "postgresql://mlflow:password@db:5432/mlflow"
        
        # Test effective URI uses backend store when provided
        assert custom_mlflow.effective_tracking_uri == "postgresql://mlflow:password@db:5432/mlflow"