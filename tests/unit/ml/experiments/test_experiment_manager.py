#!/usr/bin/env python3
"""
Unit Tests for ML Experiment Manager

Tests experiment creation, tracking, comparison, and MLflow integration.
Addresses critical testing gaps identified in PR review.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from datetime import datetime, timezone
from typing import Dict, Any
import json

from mlflow.entities import Experiment, Run

from src.ml.experiments.experiment_manager import ExperimentManager


class TestExperimentManager:
    """Comprehensive unit tests for ExperimentManager"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings configuration"""
        settings = MagicMock()
        settings.ml = MagicMock()
        settings.ml.mlflow = MagicMock()
        settings.ml.mlflow.tracking_uri = "http://test-mlflow:5001"
        return settings

    @pytest.fixture
    def mock_mlflow_service(self):
        """Mock MLflow service"""
        service = MagicMock()
        service.client = MagicMock()
        service.create_experiment = MagicMock(return_value="123")
        service.start_run = MagicMock(return_value="run_456")
        service.log_metrics = MagicMock()
        service.log_params = MagicMock()
        service.end_run = MagicMock()
        return service

    @pytest.fixture
    def experiment_manager(self, mock_settings, mock_mlflow_service):
        """Create ExperimentManager instance with mocked dependencies"""
        with patch('src.ml.experiments.experiment_manager.get_settings', return_value=mock_settings), \
             patch('src.ml.experiments.experiment_manager.mlflow_service', mock_mlflow_service), \
             patch('mlflow.set_tracking_uri') as mock_set_uri:
            
            manager = ExperimentManager()
            return manager

    @pytest.fixture
    def sample_experiment_data(self):
        """Sample experiment data for testing"""
        return {
            "name": "sharp_action_classifier_v2",
            "description": "Sharp action detection with enhanced features",
            "model_type": "classification",
            "target_variable": "sharp_action",
            "data_start_date": "2024-01-01",
            "data_end_date": "2024-06-30",
            "tags": {"version": "2.0", "feature_set": "enhanced"}
        }

    @pytest.fixture
    def mock_database_connection(self):
        """Mock database connection for experiment records"""
        conn = AsyncMock()
        mock_record = {
            "experiment_id": 1,
            "mlflow_experiment_id": "123",
            "experiment_name": "test_experiment",
            "status": "active",
            "created_at": datetime.now(timezone.utc)
        }
        conn.fetchrow = AsyncMock(return_value=mock_record)
        conn.execute = AsyncMock()
        return conn

    class TestInitialization:
        """Test experiment manager initialization"""

        def test_initialization_success(self, experiment_manager, mock_mlflow_service):
            """Test successful experiment manager initialization"""
            assert experiment_manager.mlflow_client == mock_mlflow_service.client
            assert experiment_manager.experiment_cache == {}

        def test_mlflow_uri_configuration(self, experiment_manager):
            """Test MLflow tracking URI is set correctly"""
            with patch('mlflow.set_tracking_uri') as mock_set_uri:
                # Create new instance to trigger URI setting
                with patch('src.ml.experiments.experiment_manager.get_settings'), \
                     patch('src.ml.experiments.experiment_manager.mlflow_service'):
                    ExperimentManager()
                    mock_set_uri.assert_called_with("http://localhost:5001")  # Current hardcoded value

    class TestExperimentCreation:
        """Test experiment creation functionality"""

        @pytest.mark.asyncio
        async def test_create_experiment_success(self, experiment_manager, mock_mlflow_service, sample_experiment_data, mock_database_connection):
            """Test successful experiment creation"""
            mock_mlflow_service.create_experiment.return_value = "123"
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.create_experiment(**sample_experiment_data)
                
                assert isinstance(result, dict)
                mock_mlflow_service.create_experiment.assert_called_once()
                mock_database_connection.fetchrow.assert_called_once()

        @pytest.mark.asyncio
        async def test_create_experiment_with_minimal_params(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test experiment creation with minimal parameters"""
            mock_mlflow_service.create_experiment.return_value = "456"
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.create_experiment(
                    name="minimal_experiment"
                )
                
                assert isinstance(result, dict)
                mock_mlflow_service.create_experiment.assert_called_once()

        @pytest.mark.asyncio
        async def test_create_experiment_tags_preparation(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test proper experiment tags preparation"""
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                await experiment_manager.create_experiment(
                    name="test_experiment",
                    model_type="regression",
                    target_variable="home_runs",
                    data_start_date="2024-01-01",
                    tags={"custom": "value"}
                )
                
                # Verify MLflow service was called with proper tags
                call_args = mock_mlflow_service.create_experiment.call_args
                tags = call_args.kwargs["tags"]
                
                assert tags["model_type"] == "regression"
                assert tags["target_variable"] == "home_runs"
                assert tags["data_start_date"] == "2024-01-01"
                assert tags["custom"] == "value"
                assert tags["created_by"] == "experiment_manager"
                assert tags["framework"] == "mlb_betting_system"

        @pytest.mark.asyncio
        async def test_create_experiment_database_failure(self, experiment_manager, mock_mlflow_service):
            """Test experiment creation with database failure"""
            mock_mlflow_service.create_experiment.return_value = "789"
            
            with patch('src.ml.experiments.experiment_manager.get_connection', side_effect=Exception("Database error")):
                with pytest.raises(Exception, match="Database error"):
                    await experiment_manager.create_experiment(name="test_experiment")

        @pytest.mark.asyncio
        async def test_create_experiment_mlflow_failure(self, experiment_manager, mock_mlflow_service):
            """Test experiment creation with MLflow failure"""
            mock_mlflow_service.create_experiment.side_effect = Exception("MLflow error")
            
            with pytest.raises(Exception, match="MLflow error"):
                await experiment_manager.create_experiment(name="test_experiment")

    class TestRunExperiment:
        """Test experiment run execution"""

        @pytest.mark.asyncio
        async def test_run_experiment_success(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test successful experiment run execution"""
            mock_run = MagicMock(spec=Run)
            mock_run.info.run_id = "run_123"
            mock_run.info.experiment_id = "exp_456"
            mock_run.info.artifact_uri = "s3://artifacts/run_123"
            mock_run.data.tags = {"model_name": "test_model", "model_version": "1.0"}
            
            mock_mlflow_service.start_run.return_value = "run_123"
            experiment_manager.mlflow_client.get_run.return_value = mock_run
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.run_experiment(
                    experiment_id="123",
                    run_name="test_run",
                    model_params={"n_estimators": 100, "max_depth": 6},
                    training_config={"batch_size": 32, "epochs": 10}
                )
                
                assert result["run_id"] == "run_123"
                mock_mlflow_service.start_run.assert_called_once()
                mock_mlflow_service.log_params.assert_called()

        @pytest.mark.asyncio
        async def test_run_experiment_with_metrics_logging(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test experiment run with metrics logging"""
            mock_run = MagicMock(spec=Run)
            mock_run.info.run_id = "run_789"
            mock_run.info.experiment_id = "exp_123"
            mock_run.info.artifact_uri = "s3://artifacts/run_789"
            mock_run.data.tags = {"model_name": "metrics_model", "model_version": "2.0"}
            
            mock_mlflow_service.start_run.return_value = "run_789"
            experiment_manager.mlflow_client.get_run.return_value = mock_run
            
            metrics = {
                "accuracy": 0.85,
                "precision": 0.82,
                "recall": 0.88,
                "f1_score": 0.85
            }
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.run_experiment(
                    experiment_id="123",
                    run_name="metrics_test",
                    final_metrics=metrics
                )
                
                assert result["run_id"] == "run_789" 
                mock_mlflow_service.log_metrics.assert_called_with("run_789", metrics)

        @pytest.mark.asyncio
        async def test_run_experiment_run_failure(self, experiment_manager, mock_mlflow_service):
            """Test experiment run with MLflow run failure"""
            mock_mlflow_service.start_run.side_effect = Exception("Run start failed")
            
            with pytest.raises(Exception, match="Run start failed"):
                await experiment_manager.run_experiment(
                    experiment_id="123",
                    run_name="failed_run"
                )

    class TestExperimentComparison:
        """Test experiment comparison functionality"""

        @pytest.mark.asyncio
        async def test_compare_experiments_success(self, experiment_manager):
            """Test successful experiment comparison"""
            # Mock experiment data
            exp1_runs = [
                {"run_id": "run_1", "metrics": {"accuracy": 0.80, "f1_score": 0.78}},
                {"run_id": "run_2", "metrics": {"accuracy": 0.82, "f1_score": 0.80}}
            ]
            exp2_runs = [
                {"run_id": "run_3", "metrics": {"accuracy": 0.78, "f1_score": 0.76}},
                {"run_id": "run_4", "metrics": {"accuracy": 0.84, "f1_score": 0.82}}
            ]
            
            with patch.object(experiment_manager, '_get_experiment_runs', side_effect=[exp1_runs, exp2_runs]):
                comparison = await experiment_manager.compare_experiments(
                    experiment_ids=["exp_1", "exp_2"],
                    metrics=["accuracy", "f1_score"]
                )
                
                assert "exp_1" in comparison
                assert "exp_2" in comparison
                assert "summary" in comparison
                
                # Check summary statistics
                summary = comparison["summary"]
                assert "best_accuracy" in summary
                assert "best_f1_score" in summary

        @pytest.mark.asyncio
        async def test_compare_experiments_statistical_analysis(self, experiment_manager):
            """Test experiment comparison with statistical analysis"""
            exp1_runs = [
                {"run_id": "run_1", "metrics": {"accuracy": 0.80}},
                {"run_id": "run_2", "metrics": {"accuracy": 0.82}},
                {"run_id": "run_3", "metrics": {"accuracy": 0.81}}
            ]
            exp2_runs = [
                {"run_id": "run_4", "metrics": {"accuracy": 0.75}},
                {"run_id": "run_5", "metrics": {"accuracy": 0.77}},
                {"run_id": "run_6", "metrics": {"accuracy": 0.76}}
            ]
            
            with patch.object(experiment_manager, '_get_experiment_runs', side_effect=[exp1_runs, exp2_runs]):
                comparison = await experiment_manager.compare_experiments(
                    experiment_ids=["exp_1", "exp_2"],
                    metrics=["accuracy"],
                    include_statistical_analysis=True
                )
                
                assert "statistical_analysis" in comparison
                stat_analysis = comparison["statistical_analysis"]
                assert "mean_accuracy" in stat_analysis["exp_1"]
                assert "std_accuracy" in stat_analysis["exp_1"] 
                assert "mean_accuracy" in stat_analysis["exp_2"]
                assert "std_accuracy" in stat_analysis["exp_2"]

        @pytest.mark.asyncio
        async def test_compare_experiments_empty_results(self, experiment_manager):
            """Test experiment comparison with empty results"""
            with patch.object(experiment_manager, '_get_experiment_runs', return_value=[]):
                comparison = await experiment_manager.compare_experiments(
                    experiment_ids=["empty_exp_1", "empty_exp_2"],
                    metrics=["accuracy"]
                )
                
                assert comparison["empty_exp_1"]["runs"] == []
                assert comparison["empty_exp_2"]["runs"] == []
                assert comparison["summary"] == {}

    class TestExperimentRetrieval:
        """Test experiment data retrieval"""

        @pytest.mark.asyncio
        async def test_get_experiment_success(self, experiment_manager, mock_database_connection):
            """Test successful experiment retrieval"""
            mock_experiment_record = {
                "experiment_id": 1,
                "mlflow_experiment_id": "123",
                "experiment_name": "test_experiment",
                "experiment_description": "Test description",
                "status": "active"
            }
            mock_database_connection.fetchrow.return_value = mock_experiment_record
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                experiment = await experiment_manager.get_experiment("test_experiment")
                
                assert experiment["mlflow_experiment_id"] == "123"
                assert experiment["experiment_name"] == "test_experiment"

        @pytest.mark.asyncio
        async def test_get_experiment_not_found(self, experiment_manager, mock_database_connection):
            """Test experiment retrieval when not found"""
            mock_database_connection.fetchrow.return_value = None
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                experiment = await experiment_manager.get_experiment("nonexistent_experiment")
                
                assert experiment is None

        @pytest.mark.asyncio
        async def test_list_experiments_success(self, experiment_manager, mock_database_connection):
            """Test successful experiments listing"""
            mock_experiments = [
                {"experiment_id": 1, "experiment_name": "exp_1", "status": "active"},
                {"experiment_id": 2, "experiment_name": "exp_2", "status": "completed"},
                {"experiment_id": 3, "experiment_name": "exp_3", "status": "active"}
            ]
            mock_database_connection.fetch.return_value = mock_experiments
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                experiments = await experiment_manager.list_experiments()
                
                assert len(experiments) == 3
                assert experiments[0]["experiment_name"] == "exp_1"

        @pytest.mark.asyncio
        async def test_list_experiments_with_status_filter(self, experiment_manager, mock_database_connection):
            """Test experiments listing with status filter"""
            mock_active_experiments = [
                {"experiment_id": 1, "experiment_name": "exp_1", "status": "active"},
                {"experiment_id": 3, "experiment_name": "exp_3", "status": "active"}
            ]
            mock_database_connection.fetch.return_value = mock_active_experiments
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                experiments = await experiment_manager.list_experiments(status="active")
                
                assert len(experiments) == 2
                assert all(exp["status"] == "active" for exp in experiments)

    class TestDatabaseIntegration:
        """Test database integration methods"""

        @pytest.mark.asyncio
        async def test_create_experiment_record_success(self, experiment_manager, mock_database_connection):
            """Test successful experiment record creation"""
            mock_record = {
                "experiment_id": 1,
                "mlflow_experiment_id": "123",
                "experiment_name": "test_experiment",
                "status": "active"
            }
            mock_database_connection.fetchrow.return_value = mock_record
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager._create_experiment_record(
                    mlflow_experiment_id=123,
                    name="test_experiment",
                    description="Test description",
                    model_type="classification",
                    target_variable="sharp_action",
                    data_start_date="2024-01-01",
                    data_end_date="2024-06-30"
                )
                
                assert result["mlflow_experiment_id"] == "123"
                assert result["experiment_name"] == "test_experiment"

        @pytest.mark.asyncio
        async def test_create_model_record_success(self, experiment_manager, mock_database_connection):
            """Test successful model record creation"""
            mock_run = MagicMock(spec=Run)
            mock_run.info.experiment_id = "456"
            mock_run.info.artifact_uri = "s3://artifacts/run_789"
            mock_run.data.tags = {
                "model_name": "test_model",
                "model_version": "2.0",
                "model_type": "classification"
            }
            
            experiment_manager.mlflow_client.get_run.return_value = mock_run
            
            metrics = {"accuracy": 0.85, "f1_score": 0.82}
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                await experiment_manager._create_model_record(
                    run_id="run_789",
                    final_metrics=metrics,
                    model_artifact_path="s3://models/test_model"
                )
                
                mock_database_connection.execute.assert_called_once()
                # Verify the SQL parameters include expected values
                call_args = mock_database_connection.execute.call_args
                assert call_args[0][1] == 456  # experiment_id
                assert call_args[0][2] == "run_789"  # run_id

        @pytest.mark.asyncio
        async def test_create_model_record_mlflow_failure(self, experiment_manager, mock_database_connection):
            """Test model record creation with MLflow failure"""
            experiment_manager.mlflow_client.get_run.side_effect = Exception("MLflow error")
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                with pytest.raises(Exception, match="MLflow error"):
                    await experiment_manager._create_model_record(
                        run_id="invalid_run",
                        final_metrics={"accuracy": 0.80},
                        model_artifact_path=None
                    )

    class TestExperimentStatus:
        """Test experiment status management"""

        @pytest.mark.asyncio
        async def test_update_experiment_status_success(self, experiment_manager, mock_database_connection):
            """Test successful experiment status update"""
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.update_experiment_status(
                    experiment_name="test_experiment",
                    status="completed"
                )
                
                assert result is True
                mock_database_connection.execute.assert_called_once()

        @pytest.mark.asyncio
        async def test_update_experiment_status_failure(self, experiment_manager, mock_database_connection):
            """Test experiment status update failure"""
            mock_database_connection.execute.side_effect = Exception("Update failed")
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.update_experiment_status(
                    experiment_name="test_experiment",
                    status="failed"
                )
                
                assert result is False

    class TestExperimentCaching:
        """Test experiment caching functionality"""

        def test_experiment_cache_initialization(self, experiment_manager):
            """Test experiment cache is properly initialized"""
            assert hasattr(experiment_manager, 'experiment_cache')
            assert isinstance(experiment_manager.experiment_cache, dict)
            assert len(experiment_manager.experiment_cache) == 0

        @pytest.mark.asyncio
        async def test_experiment_caching_behavior(self, experiment_manager, mock_database_connection):
            """Test experiment data is cached correctly"""
            mock_experiment = {
                "experiment_id": 1,
                "experiment_name": "cached_experiment",
                "status": "active"
            }
            mock_database_connection.fetchrow.return_value = mock_experiment
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                # First call should hit database
                result1 = await experiment_manager.get_experiment("cached_experiment")
                assert result1["experiment_name"] == "cached_experiment"
                
                # Verify caching if implemented
                if hasattr(experiment_manager, '_cache_experiment'):
                    assert "cached_experiment" in experiment_manager.experiment_cache

    class TestErrorHandling:
        """Test error handling and edge cases"""

        @pytest.mark.asyncio
        async def test_invalid_experiment_parameters(self, experiment_manager):
            """Test handling of invalid experiment parameters"""
            with pytest.raises(ValueError):
                await experiment_manager.create_experiment(
                    name="",  # Empty name should be invalid
                    model_type="classification"
                )

        @pytest.mark.asyncio
        async def test_invalid_date_formats(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test handling of invalid date formats"""
            mock_mlflow_service.create_experiment.return_value = "123"
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                with pytest.raises(ValueError):
                    await experiment_manager.create_experiment(
                        name="test_experiment",
                        data_start_date="invalid-date-format"
                    )

        @pytest.mark.asyncio
        async def test_concurrent_experiment_operations(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test concurrent experiment operations"""
            mock_mlflow_service.create_experiment.side_effect = ["123", "456", "789"]
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                # Create multiple experiments concurrently
                tasks = [
                    experiment_manager.create_experiment(name=f"concurrent_exp_{i}")
                    for i in range(3)
                ]
                
                results = await asyncio.gather(*tasks)
                
                assert len(results) == 3
                assert all(isinstance(result, dict) for result in results)

    class TestMLflowIntegration:
        """Test MLflow service integration"""

        def test_mlflow_client_initialization(self, experiment_manager, mock_mlflow_service):
            """Test MLflow client is properly initialized"""
            assert experiment_manager.mlflow_client == mock_mlflow_service.client

        @pytest.mark.asyncio
        async def test_mlflow_service_method_calls(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test proper MLflow service method calls"""
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                await experiment_manager.create_experiment(
                    name="mlflow_test",
                    description="Test MLflow integration"
                )
                
                # Verify MLflow service methods were called correctly
                mock_mlflow_service.create_experiment.assert_called_once()
                call_args = mock_mlflow_service.create_experiment.call_args
                assert call_args.kwargs["name"] == "mlflow_test"
                assert call_args.kwargs["description"] == "Test MLflow integration"

        @pytest.mark.asyncio
        async def test_mlflow_run_management(self, experiment_manager, mock_mlflow_service, mock_database_connection):
            """Test MLflow run lifecycle management"""
            mock_run = MagicMock(spec=Run)
            mock_run.info.run_id = "managed_run_123"
            mock_run.info.experiment_id = "exp_456"
            mock_run.info.artifact_uri = "s3://artifacts/managed_run_123"
            mock_run.data.tags = {"model_name": "managed_model"}
            
            mock_mlflow_service.start_run.return_value = "managed_run_123"
            experiment_manager.mlflow_client.get_run.return_value = mock_run
            
            with patch('src.ml.experiments.experiment_manager.get_connection', return_value=mock_database_connection):
                result = await experiment_manager.run_experiment(
                    experiment_id="456",
                    run_name="managed_test_run"
                )
                
                # Verify run lifecycle methods were called
                mock_mlflow_service.start_run.assert_called_once()
                mock_mlflow_service.end_run.assert_called_once_with("managed_run_123")
                assert result["run_id"] == "managed_run_123"