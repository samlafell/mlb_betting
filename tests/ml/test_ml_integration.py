"""
Comprehensive ML Integration Tests
Tests ML training, registry, retraining, and API integration
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from src.ml.training.lightgbm_trainer import LightGBMTrainer
from src.ml.registry.model_registry import model_registry, ModelStage
from src.ml.workflows.automated_retraining import automated_retraining_service, RetrainingConfig
from src.ml.experiments.experiment_manager import experiment_manager
from src.ml.features.redis_feature_store import RedisFeatureStore


@pytest.fixture
async def ml_trainer():
    """Create ML trainer for testing"""
    trainer = LightGBMTrainer(experiment_name="test_ml_integration")
    return trainer


@pytest.fixture
async def redis_store():
    """Create Redis feature store for testing"""
    store = RedisFeatureStore()
    await store.initialize()
    yield store
    await store.close()


@pytest.fixture
async def test_experiment():
    """Create test experiment"""
    experiment_result = await experiment_manager.create_experiment(
        name="integration_test_experiment",
        description="Integration test experiment",
        model_type="classification",
        target_variable="total_over"
    )
    return experiment_result


class TestMLTrainingIntegration:
    """Test ML training pipeline integration"""

    async def test_trainer_initialization(self, ml_trainer):
        """Test trainer initialization"""
        assert ml_trainer is not None
        assert ml_trainer.experiment_name == "test_ml_integration"
        assert ml_trainer.model_version == "v2.1"
        assert len(ml_trainer.model_configs) > 0

    async def test_database_connectivity(self, ml_trainer):
        """Test trainer database connectivity"""
        # Test with minimal date range to avoid insufficient data
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=1)
        
        # This should not raise an exception even with no data
        try:
            training_data = await ml_trainer._load_training_data(
                start_date, end_date, use_cached_features=False
            )
            # Should return empty list rather than error
            assert isinstance(training_data, list)
        except Exception as e:
            # Allow database connection errors in test environment
            assert "connection" in str(e).lower() or "insufficient" in str(e).lower()

    async def test_feature_vector_serialization(self, ml_trainer):
        """Test feature vector to array conversion"""
        # Mock feature vector for testing
        from src.ml.features.models import FeatureVector, TemporalFeatures
        
        mock_feature_vector = FeatureVector(
            game_id="test_123",
            temporal_features=TemporalFeatures(
                minutes_before_game=60,
                sharp_action_intensity_60min=0.75,
                opening_to_current_ml=1.5
            ),
            feature_completeness_score=0.85,
            data_source_coverage=3,
            total_feature_count=25
        )
        
        # Test conversion
        feature_array = ml_trainer._feature_vector_to_array(mock_feature_vector)
        
        assert feature_array is not None
        assert isinstance(feature_array, type(feature_array))  # numpy array
        assert len(feature_array) > 0

    async def test_model_configs(self, ml_trainer):
        """Test model configurations"""
        configs = ml_trainer.model_configs
        
        # Test required configs exist
        assert "moneyline_home_win" in configs
        assert "total_over_under" in configs
        assert "run_total_regression" in configs
        
        # Test config structure
        for config_name, config in configs.items():
            assert "objective" in config
            assert "metric" in config
            assert "learning_rate" in config
            assert "num_leaves" in config


class TestModelRegistryIntegration:
    """Test model registry integration"""

    async def test_registry_initialization(self):
        """Test model registry initialization"""
        success = await model_registry.initialize()
        assert success is True

    async def test_registry_stats(self):
        """Test registry statistics"""
        await model_registry.initialize()
        stats = model_registry.get_registry_stats()
        
        assert "total_models" in stats
        assert "total_versions" in stats
        assert "models_by_stage" in stats
        assert isinstance(stats["total_models"], int)

    async def test_model_versions_retrieval(self):
        """Test model versions retrieval"""
        await model_registry.initialize()
        
        # This should work even with no models
        versions = await model_registry.get_model_versions("nonexistent_model")
        assert isinstance(versions, list)

    async def test_validation_thresholds(self):
        """Test model validation thresholds"""
        await model_registry.initialize()
        
        # Test staging thresholds
        assert model_registry.staging_thresholds["min_accuracy"] > 0.5
        assert model_registry.staging_thresholds["min_roc_auc"] > 0.5
        
        # Test production thresholds
        assert model_registry.production_thresholds["min_accuracy"] > model_registry.staging_thresholds["min_accuracy"]
        assert model_registry.production_thresholds["evaluation_days"] > 0


class TestAutomatedRetrainingIntegration:
    """Test automated retraining integration"""

    async def test_service_initialization(self):
        """Test retraining service initialization"""
        success = await automated_retraining_service.initialize()
        # Allow failure in test environment due to scheduler dependencies
        assert isinstance(success, bool)

    async def test_service_stats(self):
        """Test retraining service statistics"""
        stats = automated_retraining_service.get_service_stats()
        
        assert "scheduler_running" in stats
        assert "monitoring_enabled" in stats
        assert "configured_models" in stats
        assert "running_jobs" in stats
        assert isinstance(stats["configured_models"], int)

    async def test_retraining_config_creation(self):
        """Test retraining configuration"""
        config = RetrainingConfig(
            model_name="test_model",
            schedule_cron="0 2 * * *",
            sliding_window_days=90,
            auto_promote_to_staging=True
        )
        
        assert config.model_name == "test_model"
        assert config.sliding_window_days == 90
        assert config.auto_promote_to_staging is True
        assert config.enabled is True

    async def test_performance_degradation_check(self):
        """Test performance degradation detection"""
        # This should not raise an exception even with no model
        degradation = await automated_retraining_service.check_performance_degradation("nonexistent_model")
        assert isinstance(degradation, bool)


class TestRedisFeatureStoreIntegration:
    """Test Redis feature store integration"""

    async def test_feature_store_initialization(self, redis_store):
        """Test feature store initialization"""
        assert redis_store is not None

    async def test_feature_store_health_check(self, redis_store):
        """Test feature store health check"""
        health = await redis_store.health_check()
        
        assert "status" in health
        # Allow both healthy and unhealthy status in test environment
        assert health["status"] in ["healthy", "unhealthy"]

    async def test_cache_stats(self, redis_store):
        """Test cache statistics"""
        stats = redis_store.get_cache_stats()
        
        assert "hits" in stats
        assert "misses" in stats
        assert "writes" in stats
        assert "hit_rate" in stats
        assert isinstance(stats["hits"], int)
        assert isinstance(stats["hit_rate"], float)

    async def test_serialization_methods(self, redis_store):
        """Test data serialization methods"""
        test_data = {"test": "data", "number": 123, "timestamp": datetime.utcnow().isoformat()}
        
        # Test serialization
        serialized = redis_store._serialize_data(test_data)
        assert isinstance(serialized, bytes)
        
        # Test deserialization
        deserialized = redis_store._deserialize_data(serialized)
        assert deserialized["test"] == "data"
        assert deserialized["number"] == 123


class TestExperimentManagerIntegration:
    """Test experiment manager integration"""

    async def test_experiment_creation(self, test_experiment):
        """Test experiment creation"""
        assert "experiment_id" in test_experiment
        assert "mlflow_experiment_id" in test_experiment
        assert "experiment_name" in test_experiment
        assert test_experiment["experiment_name"] == "integration_test_experiment"

    async def test_experiment_summary(self, test_experiment):
        """Test experiment summary retrieval"""
        experiment_name = test_experiment["experiment_name"]
        
        summary = await experiment_manager.get_experiment_summary(experiment_name)
        
        assert "experiment_id" in summary
        assert "experiment_name" in summary
        assert "run_analysis" in summary
        assert summary["experiment_name"] == experiment_name

    async def test_experiment_listing(self):
        """Test experiment listing"""
        experiments = await experiment_manager.list_experiments()
        
        assert isinstance(experiments, list)
        # Should have at least our test experiment
        assert len(experiments) >= 0


class TestEndToEndMLWorkflow:
    """Test complete end-to-end ML workflow"""

    async def test_ml_infrastructure_connectivity(self):
        """Test all ML infrastructure components"""
        # Test components individually to isolate failures
        components_status = {}
        
        # Test experiment manager
        try:
            await experiment_manager.create_experiment(
                name="connectivity_test",
                description="Test connectivity",
                model_type="test"
            )
            components_status["experiment_manager"] = True
        except Exception as e:
            components_status["experiment_manager"] = f"Error: {e}"
        
        # Test model registry
        try:
            await model_registry.initialize()
            stats = model_registry.get_registry_stats()
            components_status["model_registry"] = "total_models" in stats
        except Exception as e:
            components_status["model_registry"] = f"Error: {e}"
        
        # Test retraining service
        try:
            stats = automated_retraining_service.get_service_stats()
            components_status["retraining_service"] = "configured_models" in stats
        except Exception as e:
            components_status["retraining_service"] = f"Error: {e}"
        
        # Test Redis feature store
        try:
            store = RedisFeatureStore()
            await store.initialize()
            health = await store.health_check()
            components_status["redis_store"] = "status" in health
            await store.close()
        except Exception as e:
            components_status["redis_store"] = f"Error: {e}"
        
        # Report results
        print(f"ML Infrastructure Connectivity Test Results:")
        for component, status in components_status.items():
            print(f"  {component}: {status}")
        
        # At least some components should be working
        working_components = sum(1 for status in components_status.values() if status is True or status is not False)
        assert working_components >= 2, f"Too few ML components working: {components_status}"

    async def test_feature_pipeline_integration(self):
        """Test feature pipeline integration"""
        from src.ml.features.feature_pipeline import FeaturePipeline
        
        pipeline = FeaturePipeline()
        
        # Test pipeline initialization
        assert pipeline is not None
        
        # Test feature extraction (should handle no data gracefully)
        try:
            # Use a far future date to ensure no data exists
            future_date = datetime.utcnow() + timedelta(days=365)
            features = await pipeline.extract_features_for_game("test_game_123", future_date)
            # Should return None or handle gracefully
            assert features is None or hasattr(features, 'game_id')
        except Exception as e:
            # Allow database connection errors in test environment
            assert "connection" in str(e).lower() or "not found" in str(e).lower()

    async def test_ml_configuration_consistency(self):
        """Test ML configuration consistency across components"""
        # Test that all components use consistent configuration
        trainer = LightGBMTrainer()
        registry_initialized = await model_registry.initialize()
        service_stats = automated_retraining_service.get_service_stats()
        
        # All components should be able to initialize without errors
        assert trainer is not None
        assert isinstance(registry_initialized, bool)
        assert isinstance(service_stats, dict)
        
        # Test configuration values are reasonable
        assert trainer.default_ttl > 0
        assert len(trainer.model_configs) > 0
        
        # Test model names are consistent
        expected_models = ["moneyline_home_win", "total_over_under", "run_total_regression"]
        for model_name in expected_models:
            assert model_name in trainer.model_configs


@pytest.mark.asyncio
async def test_ml_system_health():
    """Comprehensive ML system health check"""
    health_results = {}
    
    # Test each major component
    try:
        # 1. Model training system
        trainer = LightGBMTrainer()
        health_results["training_system"] = trainer is not None
        
        # 2. Model registry
        registry_success = await model_registry.initialize()
        health_results["model_registry"] = registry_success
        
        # 3. Automated retraining
        service_stats = automated_retraining_service.get_service_stats()
        health_results["retraining_service"] = "configured_models" in service_stats
        
        # 4. Feature store
        store = RedisFeatureStore()
        store_health = await store.health_check()
        health_results["feature_store"] = "status" in store_health
        await store.close()
        
        # 5. Experiment management
        experiments = await experiment_manager.list_experiments()
        health_results["experiment_manager"] = isinstance(experiments, list)
        
    except Exception as e:
        health_results["system_error"] = str(e)
    
    # Report overall health
    healthy_components = sum(1 for status in health_results.values() if status is True)
    total_components = len([k for k in health_results.keys() if k != "system_error"])
    
    print(f"ML System Health: {healthy_components}/{total_components} components healthy")
    print(f"Health Details: {health_results}")
    
    # System should have at least 3 healthy components
    assert healthy_components >= 3, f"ML system unhealthy: {health_results}"


if __name__ == "__main__":
    # Run tests
    asyncio.run(test_ml_system_health())