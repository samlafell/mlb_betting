#!/usr/bin/env python3
"""
Integration Tests for MLflow Configuration and Service Integration

Tests MLflow service connects with new config format and retry logic works properly.
"""

import pytest
from unittest.mock import MagicMock, patch, Mock
import time
from mlflow.exceptions import MlflowException

from src.ml.services.mlflow_integration import MLflowService
from src.core.config import get_settings


class TestMLflowIntegration:
    """Integration tests for MLflow service and configuration."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings with test MLflow configuration."""
        mock_settings = MagicMock()
        # Database config
        mock_settings.database.host = "test-host"
        mock_settings.database.port = 5433
        mock_settings.database.database = "test-database"
        mock_settings.database.user = "test-user"
        mock_settings.database.password = "test-password"
        
        # MLflow config
        mock_settings.mlflow.tracking_uri = "http://test-mlflow:5001"
        mock_settings.mlflow.max_retries = 3
        mock_settings.mlflow.retry_delay = 0.1  # Fast retries for tests
        mock_settings.mlflow.connection_timeout = 30
        mock_settings.mlflow.artifact_root = "./test-mlruns"
        mock_settings.mlflow.backend_store_uri = None
        mock_settings.mlflow.effective_tracking_uri = "http://test-mlflow:5001"
        
        return mock_settings

    @pytest.fixture
    def mock_mlflow_client(self):
        """Mock MLflow client for testing."""
        mock_client = MagicMock()
        mock_client.search_experiments.return_value = []
        mock_client.get_experiment_by_name.return_value = None
        mock_client.create_experiment.return_value = "test-experiment-id"
        return mock_client

    def test_mlflow_service_initialization(self, mock_settings):
        """Test MLflow service connects with new config format"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri') as mock_set_uri, \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class:
            
            mock_client = mock_client_class.return_value
            mock_client.search_experiments.return_value = []
            
            service = MLflowService()
            
            # Verify MLflow service is properly initialized
            assert service is not None
            assert service.client is not None
            assert service.max_retries == 3
            assert service.retry_delay == 0.1
            
            # Verify tracking URI was set correctly
            mock_set_uri.assert_called_with("http://test-mlflow:5001")

    def test_mlflow_service_initialization_with_backend_store(self, mock_settings):
        """Test MLflow service with backend store URI override"""
        mock_settings.mlflow.backend_store_uri = "postgresql://test-user:test-password@test-host:5433/test-database"
        mock_settings.mlflow.effective_tracking_uri = "postgresql://test-user:test-password@test-host:5433/test-database"
        
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri') as mock_set_uri, \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class:
            
            mock_client = mock_client_class.return_value
            mock_client.search_experiments.return_value = []
            
            service = MLflowService()
            
            # Verify backend store URI is used instead of tracking URI
            expected_uri = "postgresql://test-user:test-password@test-host:5433/test-database"
            mock_set_uri.assert_called_with(expected_uri)

    def test_mlflow_service_retry_logic_success_after_failure(self, mock_settings):
        """Test that retry logic works correctly on connection failures"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class, \
             patch('src.ml.services.mlflow_integration.time.sleep') as mock_sleep:
            
            mock_client = mock_client_class.return_value
            # Fail twice, then succeed
            mock_client.search_experiments.side_effect = [
                MlflowException("Connection failed"),
                MlflowException("Still failing"),
                []  # Success on third attempt
            ]
            
            service = MLflowService(max_retries=3, retry_delay=0.1)
            
            # Verify service was eventually initialized successfully
            assert service.client is not None
            assert mock_client.search_experiments.call_count == 3
            assert mock_sleep.call_count == 2  # Two retries

    def test_mlflow_service_retry_logic_max_retries_exceeded(self, mock_settings):
        """Test that service fails after max retries are exceeded"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class, \
             patch('src.ml.services.mlflow_integration.time.sleep'):
            
            mock_client = mock_client_class.return_value
            # Always fail
            mock_client.search_experiments.side_effect = MlflowException("Persistent connection failure")
            
            with pytest.raises(MlflowException, match="Persistent connection failure"):
                MLflowService(max_retries=2, retry_delay=0.01)

    def test_create_experiment_with_retry_logic(self, mock_settings):
        """Test experiment creation with retry logic"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class, \
             patch('src.ml.services.mlflow_integration.time.sleep') as mock_sleep:
            
            mock_client = mock_client_class.return_value
            mock_client.search_experiments.return_value = []
            mock_client.get_experiment_by_name.side_effect = [
                MlflowException("Temporary failure"),
                None  # Success on second attempt
            ]
            mock_client.create_experiment.return_value = "test-experiment-id"
            
            service = MLflowService(max_retries=3, retry_delay=0.01)
            
            experiment_id = service.create_experiment("test-experiment")
            
            assert experiment_id == "test-experiment-id"
            assert mock_client.get_experiment_by_name.call_count == 2
            assert mock_sleep.call_count >= 1  # At least one retry

    def test_log_metrics_with_retry_logic(self, mock_settings):
        """Test metric logging with retry logic"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class, \
             patch('src.ml.services.mlflow_integration.mlflow.log_metric') as mock_log_metric, \
             patch('src.ml.services.mlflow_integration.time.sleep'):
            
            mock_client = mock_client_class.return_value
            mock_client.search_experiments.return_value = []
            
            # Fail once, then succeed
            mock_log_metric.side_effect = [
                MlflowException("Temporary failure"),
                None,
                None  # Success for both metrics
            ]
            
            service = MLflowService(max_retries=3, retry_delay=0.01)
            
            metrics = {"accuracy": 0.85, "f1_score": 0.82}
            service.log_model_metrics(metrics)
            
            # Should have retried and succeeded
            assert mock_log_metric.call_count >= 2  # At least one retry

    def test_log_params_with_retry_logic(self, mock_settings):
        """Test parameter logging with retry logic"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class, \
             patch('src.ml.services.mlflow_integration.mlflow.log_param') as mock_log_param, \
             patch('src.ml.services.mlflow_integration.time.sleep'):
            
            mock_client = mock_client_class.return_value
            mock_client.search_experiments.return_value = []
            
            # Fail once, then succeed for all params
            mock_log_param.side_effect = [
                MlflowException("Temporary failure"),
                None,
                None,
                None  # Success for all params
            ]
            
            service = MLflowService(max_retries=3, retry_delay=0.01)
            
            params = {"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1}
            service.log_model_params(params)
            
            # Should have retried and succeeded
            assert mock_log_param.call_count >= 3  # At least one retry

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
        
        # Test default values
        assert settings.mlflow.tracking_uri == "http://localhost:5001"
        assert settings.mlflow.max_retries >= 1
        assert settings.mlflow.retry_delay > 0
        assert settings.mlflow.connection_timeout > 0
        
        # Test computed field
        assert callable(getattr(type(settings.mlflow), 'effective_tracking_uri', None))

    def test_non_retryable_errors_fail_immediately(self, mock_settings):
        """Test that non-retryable errors fail immediately without retries"""
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class, \
             patch('src.ml.services.mlflow_integration.time.sleep') as mock_sleep:
            
            mock_client = mock_client_class.return_value
            # Non-retryable error (not MlflowException, ConnectionError, or TimeoutError)
            mock_client.search_experiments.side_effect = ValueError("Invalid configuration")
            
            with pytest.raises(ValueError, match="Invalid configuration"):
                MLflowService(max_retries=3, retry_delay=0.01)
            
            # Should not have retried
            assert mock_sleep.call_count == 0

    def test_mlflow_service_custom_retry_settings(self, mock_settings):
        """Test MLflow service with custom retry settings"""
        custom_retries = 5
        custom_delay = 0.5
        
        with patch('src.ml.services.mlflow_integration.get_settings', return_value=mock_settings), \
             patch('src.ml.services.mlflow_integration.mlflow.set_tracking_uri'), \
             patch('src.ml.services.mlflow_integration.MlflowClient') as mock_client_class:
            
            mock_client = mock_client_class.return_value
            mock_client.search_experiments.return_value = []
            
            service = MLflowService(max_retries=custom_retries, retry_delay=custom_delay)
            
            # Verify custom settings override config defaults
            assert service.max_retries == custom_retries
            assert service.retry_delay == custom_delay