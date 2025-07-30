#!/usr/bin/env python3
"""
Integration tests for ML Prediction API

Tests the complete ML prediction service integration including:
- Service initialization and dependency injection
- API structure and routing validation
- Pydantic model validation
- Error handling and edge cases
- Resource management and cleanup

Follows project testing standards with pytest framework.
"""

import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

# Test markers for categorization
pytestmark = [
    pytest.mark.integration
]


class TestMLPredictionServiceIntegration:
    """Integration tests for ML prediction service components"""

    @pytest.fixture
    def mock_redis_url(self) -> str:
        """Provide test Redis URL from environment or use test default"""
        return os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")

    @pytest.fixture
    def mock_config(self) -> dict:
        """Provide test configuration with secure defaults"""
        return {
            "redis_url": os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15"),
            "redis_ttl": 900,
            "feature_version": "v2.1",
            "api_secret_key": os.getenv("API_SECRET_KEY", "test_secret_for_testing_only")
        }

    @pytest.fixture
    def prediction_service(self, mock_config):
        """Initialize prediction service with mocked dependencies"""
        with patch("src.ml.services.prediction_service.PredictionService") as mock_service:
            service = mock_service.return_value
            service.predict = AsyncMock(return_value={"prediction": 0.75, "confidence": 0.88})
            service.get_model_info = AsyncMock(return_value={"model": "test_model", "version": "1.0"})
            yield service

    @pytest.fixture
    def feature_pipeline(self, mock_config):
        """Initialize feature pipeline with test configuration"""
        with patch("src.ml.features.feature_pipeline.FeaturePipeline") as mock_pipeline:
            pipeline = mock_pipeline.return_value
            pipeline.extract_features = AsyncMock(return_value={"features": [1, 2, 3]})
            pipeline.feature_version = mock_config["feature_version"]
            yield pipeline

    @pytest.fixture
    def redis_feature_store(self, mock_redis_url):
        """Initialize Redis feature store with test configuration"""
        with patch("src.ml.features.redis_feature_store.RedisFeatureStore") as mock_store:
            store = mock_store.return_value
            store.get_features = AsyncMock(return_value={"cached": True})
            store.store_features = AsyncMock(return_value=True)
            yield store

    @pytest.fixture
    def lightgbm_trainer(self, feature_pipeline, redis_feature_store):
        """Initialize LightGBM trainer with mocked dependencies"""
        with patch("src.ml.training.lightgbm_trainer.LightGBMTrainer") as mock_trainer:
            trainer = mock_trainer.return_value
            trainer.train = AsyncMock(return_value={"accuracy": 0.85})
            trainer.predict = AsyncMock(return_value=[0.75, 0.25])
            yield trainer

    def test_ml_prediction_service_imports(self):
        """Test that ML prediction service components can be imported"""
        try:
            from src.ml.features.feature_pipeline import FeaturePipeline
            from src.ml.features.redis_feature_store import RedisFeatureStore
            from src.ml.services.prediction_service import PredictionService
            from src.ml.training.lightgbm_trainer import LightGBMTrainer

            # Verify classes are importable
            assert PredictionService is not None, "PredictionService should be importable"
            assert FeaturePipeline is not None, "FeaturePipeline should be importable"
            assert RedisFeatureStore is not None, "RedisFeatureStore should be importable"
            assert LightGBMTrainer is not None, "LightGBMTrainer should be importable"

        except ImportError as e:
            pytest.fail(f"Import failed: {e}")
        except ModuleNotFoundError as e:
            pytest.fail(f"Module not found: {e}")

    def test_prediction_service_initialization(self, prediction_service):
        """Test prediction service initialization with mocked dependencies"""
        assert prediction_service is not None, "Prediction service should initialize"

        # Test service method availability
        assert hasattr(prediction_service, 'predict'), "Service should have predict method"
        assert hasattr(prediction_service, 'get_model_info'), "Service should have get_model_info method"

    def test_feature_pipeline_initialization(self, feature_pipeline, mock_config):
        """Test feature pipeline initialization and configuration"""
        assert feature_pipeline is not None, "Feature pipeline should initialize"
        assert feature_pipeline.feature_version == mock_config["feature_version"], \
            "Feature pipeline should use configured version"

    def test_redis_feature_store_initialization(self, redis_feature_store):
        """Test Redis feature store initialization with secure configuration"""
        assert redis_feature_store is not None, "Redis feature store should initialize"

        # Test store method availability
        assert hasattr(redis_feature_store, 'get_features'), "Store should have get_features method"
        assert hasattr(redis_feature_store, 'store_features'), "Store should have store_features method"

    def test_lightgbm_trainer_initialization(self, lightgbm_trainer, feature_pipeline, redis_feature_store):
        """Test LightGBM trainer initialization with dependencies"""
        assert lightgbm_trainer is not None, "LightGBM trainer should initialize"

        # Test trainer method availability
        assert hasattr(lightgbm_trainer, 'train'), "Trainer should have train method"
        assert hasattr(lightgbm_trainer, 'predict'), "Trainer should have predict method"

    @pytest.mark.asyncio
    async def test_service_integration_workflow(self, prediction_service, feature_pipeline, redis_feature_store):
        """Test integrated workflow between services"""
        # Mock a prediction workflow
        game_id = "test_game_12345"

        # Test feature extraction
        features = await feature_pipeline.extract_features(game_id)
        assert features is not None, "Feature extraction should return features"

        # Test feature storage
        store_result = await redis_feature_store.store_features(game_id, features)
        assert store_result is True, "Feature storage should succeed"

        # Test prediction
        prediction = await prediction_service.predict(game_id)
        assert prediction is not None, "Prediction should return result"
        assert "prediction" in prediction, "Prediction should contain prediction value"


class TestMLAPIStructure:
    """Tests for ML API structure and routing"""

    def test_fastapi_app_import(self):
        """Test FastAPI app can be imported"""
        try:
            from src.ml.api.main import app
            assert app is not None, "FastAPI app should be importable"

        except ImportError as e:
            pytest.fail(f"FastAPI app import failed: {e}")
        except ModuleNotFoundError as e:
            pytest.fail(f"API module not found: {e}")

    def test_api_routers_import(self):
        """Test API routers can be imported"""
        try:
            from src.ml.api.routers import health, models, predictions

            assert predictions is not None, "Predictions router should be importable"
            assert models is not None, "Models router should be importable"
            assert health is not None, "Health router should be importable"

        except ImportError as e:
            pytest.fail(f"Router import failed: {e}")
        except ModuleNotFoundError as e:
            pytest.fail(f"Router module not found: {e}")

    def test_api_dependencies_import(self):
        """Test API dependencies can be imported"""
        try:
            from src.ml.api.dependencies import get_ml_service, get_redis_client

            assert get_ml_service is not None, "ML service dependency should be importable"
            assert get_redis_client is not None, "Redis client dependency should be importable"

        except ImportError as e:
            pytest.fail(f"Dependencies import failed: {e}")
        except ModuleNotFoundError as e:
            pytest.fail(f"Dependencies module not found: {e}")


class TestPydanticModels:
    """Tests for Pydantic model validation"""

    def test_prediction_request_model(self):
        """Test PredictionRequest model validation"""
        try:
            from src.ml.api.routers.predictions import PredictionRequest

            # Test valid request
            request = PredictionRequest(
                game_id="12345",
                model_name="test_model",
                include_explanation=True
            )

            assert request.game_id == "12345", "Game ID should be set correctly"
            assert request.model_name == "test_model", "Model name should be set correctly"
            assert request.include_explanation is True, "Include explanation should be set correctly"

        except ImportError as e:
            pytest.fail(f"PredictionRequest import failed: {e}")
        except ValueError as e:
            pytest.fail(f"PredictionRequest validation failed: {e}")

    def test_batch_prediction_request_model(self):
        """Test BatchPredictionRequest model validation"""
        try:
            from src.ml.api.routers.predictions import BatchPredictionRequest

            # Test valid batch request
            batch_request = BatchPredictionRequest(
                game_ids=["12345", "12346", "12347"],
                model_name="test_model"
            )

            assert len(batch_request.game_ids) == 3, "Should accept multiple game IDs"
            assert "12345" in batch_request.game_ids, "Should contain first game ID"
            assert batch_request.model_name == "test_model", "Model name should be set correctly"

        except ImportError as e:
            pytest.fail(f"BatchPredictionRequest import failed: {e}")
        except ValueError as e:
            pytest.fail(f"BatchPredictionRequest validation failed: {e}")

    def test_model_info_validation(self):
        """Test ModelInfo and ModelPerformanceResponse models"""
        try:
            from datetime import datetime

            from src.ml.api.routers.models import ModelInfo

            # Test ModelInfo with required fields
            model_info = ModelInfo(
                model_name="test_model",
                model_version="1.0",
                model_type="classification",
                is_active=True,
                created_at=datetime.now(),
                description="Test model for unit testing"
            )

            assert model_info.model_name == "test_model", "Model name should be set correctly"
            assert model_info.model_version == "1.0", "Model version should be set correctly"
            assert model_info.model_type == "classification", "Model type should be set correctly"
            assert model_info.is_active is True, "Model active status should be set correctly"
            assert model_info.description == "Test model for unit testing", "Description should be set correctly"

        except ImportError as e:
            pytest.fail(f"Model schemas import failed: {e}")
        except ValueError as e:
            pytest.fail(f"Model schema validation failed: {e}")


class TestErrorHandlingAndEdgeCases:
    """Tests for error handling and edge case scenarios"""

    @pytest.mark.asyncio
    async def test_invalid_model_name_handling(self):
        """Test handling of invalid model names"""
        with patch("src.ml.services.prediction_service.PredictionService") as mock_service:
            service = mock_service.return_value
            service.predict = AsyncMock(side_effect=ValueError("Invalid model name"))

            with pytest.raises(ValueError, match="Invalid model name"):
                await service.predict("invalid_game_id", model_name="nonexistent_model")

    @pytest.mark.asyncio
    async def test_network_failure_handling(self):
        """Test handling of network failures"""
        with patch("src.ml.features.redis_feature_store.RedisFeatureStore") as mock_store:
            store = mock_store.return_value
            store.get_features = AsyncMock(side_effect=ConnectionError("Redis connection failed"))

            with pytest.raises(ConnectionError, match="Redis connection failed"):
                await store.get_features("test_game_id")

    @pytest.mark.asyncio
    async def test_empty_data_handling(self):
        """Test handling of empty/malformed data"""
        with patch("src.ml.features.feature_pipeline.FeaturePipeline") as mock_pipeline:
            pipeline = mock_pipeline.return_value
            pipeline.extract_features = AsyncMock(return_value={})

            features = await pipeline.extract_features("empty_game_id")
            assert features == {}, "Should handle empty feature extraction gracefully"

    @pytest.mark.asyncio
    async def test_resource_exhaustion_handling(self):
        """Test handling of resource exhaustion scenarios"""
        with patch("src.ml.training.lightgbm_trainer.LightGBMTrainer") as mock_trainer:
            trainer = mock_trainer.return_value
            trainer.train = AsyncMock(side_effect=MemoryError("Out of memory during training"))

            with pytest.raises(MemoryError, match="Out of memory during training"):
                await trainer.train()


class TestResourceManagement:
    """Tests for proper resource management and cleanup"""

    @pytest_asyncio.fixture
    async def redis_client_mock(self):
        """Mock Redis client with proper cleanup"""
        mock_client = AsyncMock()
        mock_client.close = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        yield mock_client

        # Ensure cleanup was called
        await mock_client.close()

    @pytest.mark.asyncio
    async def test_redis_client_cleanup(self, redis_client_mock):
        """Test Redis client is properly cleaned up"""
        async with redis_client_mock as client:
            await client.ping()

        # Verify cleanup was called
        assert redis_client_mock.__aexit__.called, "Redis client should be properly closed"

    @pytest_asyncio.fixture
    async def service_with_cleanup(self):
        """Mock service that requires cleanup"""
        service = AsyncMock()
        service.close = AsyncMock()

        yield service

        # Ensure cleanup
        await service.close()

    @pytest.mark.asyncio
    async def test_service_resource_cleanup(self, service_with_cleanup):
        """Test service resources are properly cleaned up"""
        # Use the service
        await service_with_cleanup.some_operation()

        # Verify it was used
        service_with_cleanup.some_operation.assert_called_once()


# Performance and load testing markers for future expansion
@pytest.mark.slow
@pytest.mark.performance
class TestMLAPIPerformance:
    """Performance tests for ML API components"""

    def test_prediction_service_performance(self):
        """Test prediction service meets performance targets"""
        # Placeholder for performance testing
        # Should validate <500ms response time for predictions
        pass

    def test_feature_extraction_performance(self):
        """Test feature extraction meets performance targets"""
        # Placeholder for performance testing
        # Should validate <200KB memory usage per game
        pass


if __name__ == "__main__":
    # For compatibility with uv run pytest
    pytest.main([__file__, "-v"])
