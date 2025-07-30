"""
End-to-End Integration Tests for ML Pipeline
Tests complete workflow from feature extraction to model training and prediction
"""

import pytest
import asyncio
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, AsyncMock
import polars as pl
import mlflow
import os

from src.ml.features.feature_pipeline import FeaturePipeline
from src.ml.features.redis_atomic_store import RedisAtomicStore
from src.ml.training.lightgbm_trainer import LightGBMTrainer
from src.ml.training.ml_training_service import MLTrainingService
from src.ml.api.main import app
from src.ml.database.connection_pool import DatabaseConnectionPool
from src.core.config import Config
from fastapi.testclient import TestClient


@pytest.mark.integration
class TestEndToEndMLPipeline:
    """End-to-end integration tests for complete ML pipeline"""
    
    @pytest.fixture(scope="class")
    async def temp_mlflow_dir(self):
        """Create temporary MLflow tracking directory"""
        temp_dir = tempfile.mkdtemp(prefix="mlflow_test_")
        mlflow.set_tracking_uri(f"file://{temp_dir}")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture(scope="class")
    async def test_config(self):
        """Test configuration with overrides"""
        config = Config()
        # Override for testing
        config.database.host = os.getenv("TEST_DB_HOST", "localhost")
        config.database.database = os.getenv("TEST_DB_NAME", "mlb_betting_test")
        config.redis.url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
        return config
    
    @pytest.fixture
    async def feature_pipeline(self, test_config):
        """Feature pipeline instance"""
        return FeaturePipeline(
            config=test_config,
            feature_version="integration_test_v1.0"
        )
    
    @pytest.fixture
    async def redis_store(self, test_config):
        """Redis store instance"""
        store = RedisAtomicStore(
            redis_url=test_config.redis.url,
            use_msgpack=True
        )
        await store.initialize()
        yield store
        await store.close()
    
    @pytest.fixture
    async def database_pool(self, test_config):
        """Database connection pool"""
        pool = DatabaseConnectionPool(test_config.database)
        await pool.initialize()
        yield pool
        await pool.close()
    
    @pytest.fixture
    def sample_game_data(self):
        """Sample game data for testing"""
        base_time = datetime.now() - timedelta(hours=3)
        return pl.DataFrame({
            'game_id': [12345] * 20,
            'timestamp': [base_time + timedelta(minutes=i*5) for i in range(20)],
            'sportsbook_name': ['DraftKings', 'FanDuel', 'BetMGM', 'Caesars'] * 5,
            'market_type': ['moneyline'] * 20,
            'home_team': ['Yankees'] * 20,
            'away_team': ['Red Sox'] * 20,
            'home_ml_odds': [-150 + i for i in range(20)],
            'away_ml_odds': [130 + i for i in range(20)],
            'home_score': [0] * 20,
            'away_score': [0] * 20,
            'game_status': ['scheduled'] * 20
        })
    
    @pytest.mark.asyncio
    async def test_complete_feature_extraction_workflow(
        self, 
        feature_pipeline, 
        redis_store, 
        database_pool, 
        sample_game_data
    ):
        """Test complete feature extraction from database to Redis cache"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        # Mock database data loading
        with patch.object(feature_pipeline, '_load_game_data') as mock_load:
            mock_load.return_value = {
                'temporal_data': sample_game_data,
                'market_data': sample_game_data,
                'team_data': sample_game_data,
                'betting_splits_data': sample_game_data
            }
            
            # Extract features
            feature_vector = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )
            
            assert feature_vector is not None
            assert feature_vector.game_id == game_id
            assert feature_vector.minutes_before_game >= 60
            
            # Cache features in Redis
            cache_success = await redis_store.cache_feature_vector_atomic(
                game_id, feature_vector
            )
            assert cache_success is True
            
            # Retrieve from Redis
            cached_features = await redis_store.get_feature_vector_atomic(
                game_id, feature_vector.feature_version
            )
            assert cached_features is not None
            assert cached_features.game_id == game_id
            assert cached_features.feature_version == feature_vector.feature_version
    
    @pytest.mark.asyncio
    async def test_batch_feature_processing_integration(
        self, 
        feature_pipeline, 
        redis_store, 
        sample_game_data
    ):
        """Test batch processing of multiple games through complete pipeline"""
        game_ids = [12340 + i for i in range(10)]
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        # Mock data loading for multiple games
        def mock_load_data(game_id, cutoff):
            game_data = sample_game_data.with_columns(
                pl.col('game_id').map_elements(lambda x: game_id, return_dtype=pl.Int64)
            )
            return {
                'temporal_data': game_data,
                'market_data': game_data,
                'team_data': game_data,
                'betting_splits_data': game_data
            }
        
        with patch.object(feature_pipeline, '_load_game_data', side_effect=mock_load_data):
            # Extract features for all games
            feature_vectors = []
            for game_id in game_ids:
                feature_vector = await feature_pipeline.extract_features_for_game(
                    game_id, cutoff_time
                )
                if feature_vector:
                    feature_vectors.append((game_id, feature_vector))
            
            assert len(feature_vectors) == 10
            
            # Batch cache all features
            cached_count = await redis_store.cache_batch_features_atomic(
                feature_vectors, ttl=600
            )
            assert cached_count == 10
            
            # Verify all cached
            for game_id, original_fv in feature_vectors:
                cached_fv = await redis_store.get_feature_vector_atomic(
                    game_id, original_fv.feature_version
                )
                assert cached_fv is not None
                assert cached_fv.game_id == game_id
    
    @pytest.mark.asyncio
    async def test_model_training_integration(
        self, 
        temp_mlflow_dir,
        feature_pipeline,
        sample_game_data
    ):
        """Test complete model training workflow with MLflow tracking"""
        # Prepare training data
        training_data = sample_game_data.with_columns([
            pl.col('home_ml_odds').alias('home_moneyline'),
            pl.col('away_ml_odds').alias('away_moneyline'),
            (pl.col('home_ml_odds') < pl.col('away_ml_odds')).alias('home_favorite'),
            pl.lit(1).alias('home_win'),  # Mock outcome
            pl.lit(8.5).alias('total_score')  # Mock total
        ])
        
        # Initialize trainer
        trainer = LightGBMTrainer(
            experiment_name="integration_test",
            model_registry_name="test_model"
        )
        
        # Mock feature preparation
        with patch.object(trainer, '_prepare_features') as mock_prepare:
            mock_prepare.return_value = (
                training_data.to_pandas()[['home_moneyline', 'away_moneyline', 'home_favorite']],
                training_data.to_pandas()['home_win'],
                ['home_moneyline', 'away_moneyline', 'home_favorite']
            )
            
            # Train model
            model_info = await trainer.train_model(
                training_data=training_data,
                target_column='home_win',
                model_type='classification'
            )
            
            assert model_info is not None
            assert 'model_uri' in model_info
            assert 'experiment_id' in model_info
            assert 'run_id' in model_info
            
            # Verify MLflow tracking
            experiment = mlflow.get_experiment_by_name("integration_test")
            assert experiment is not None
    
    @pytest.mark.asyncio
    async def test_prediction_api_integration(self, test_config):
        """Test prediction API with complete security integration"""
        # Create test client
        client = TestClient(app)
        
        # Test health check (no auth required)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
        
        # Test prediction endpoint (requires auth in production)
        with patch.dict(os.environ, {'ENVIRONMENT': 'development'}):
            prediction_request = {
                "game_id": 12345,
                "feature_version": "integration_test_v1.0"
            }
            
            response = client.post("/api/v1/predict", json=prediction_request)
            # Should work in development without auth
            assert response.status_code in [200, 422, 503]  # 422 if validation fails, 503 if model not available
        
        # Test production auth requirement
        with patch.dict(os.environ, {'ENVIRONMENT': 'production', 'API_SECRET_KEY': 'test_key'}):
            # Without auth header
            response = client.post("/api/v1/predict", json=prediction_request)
            assert response.status_code == 401
            
            # With valid auth header
            headers = {"Authorization": "Bearer test_key"}
            response = client.post("/api/v1/predict", json=prediction_request, headers=headers)
            assert response.status_code in [200, 422, 503]
    
    @pytest.mark.asyncio
    async def test_ml_training_service_integration(
        self, 
        temp_mlflow_dir,
        test_config,
        sample_game_data
    ):
        """Test ML training service with complete workflow"""
        # Initialize training service
        training_service = MLTrainingService(config=test_config)
        
        # Mock database queries
        with patch.object(training_service, '_load_training_data') as mock_load:
            mock_load.return_value = sample_game_data.with_columns([
                pl.lit(1).alias('home_win'),
                pl.lit(8.5).alias('total_runs'),
                pl.col('home_ml_odds').alias('home_moneyline')
            ])
            
            # Test automated training
            training_results = await training_service.train_models_automated(
                start_date=datetime.now() - timedelta(days=30),
                end_date=datetime.now() - timedelta(days=1),
                models=['moneyline']
            )
            
            assert len(training_results) >= 1
            assert 'moneyline' in training_results
            
            moneyline_result = training_results['moneyline']
            assert 'model_uri' in moneyline_result
            assert 'metrics' in moneyline_result
            assert 'feature_importance' in moneyline_result
    
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, feature_pipeline, redis_store):
        """Test error handling and recovery in complete pipeline"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        # Test database connection failure
        with patch.object(feature_pipeline, '_load_game_data') as mock_load:
            mock_load.side_effect = Exception("Database connection failed")
            
            feature_vector = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )
            assert feature_vector is None  # Should handle gracefully
        
        # Test Redis failure with graceful degradation
        await redis_store.close()  # Simulate Redis failure
        
        # Feature extraction should still work (just no caching)
        with patch.object(feature_pipeline, '_load_game_data') as mock_load:
            mock_load.return_value = {'temporal_data': pl.DataFrame()}
            
            # Should not crash due to Redis being unavailable
            try:
                feature_vector = await feature_pipeline.extract_features_for_game(
                    game_id, cutoff_time
                )
                # May return None due to empty data, but shouldn't crash
            except Exception as e:
                # Should not be Redis-related
                assert "redis" not in str(e).lower()
    
    @pytest.mark.asyncio
    async def test_performance_benchmarks_integration(
        self, 
        feature_pipeline, 
        redis_store, 
        sample_game_data
    ):
        """Test performance benchmarks for complete pipeline"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        # Mock data loading
        with patch.object(feature_pipeline, '_load_game_data') as mock_load:
            mock_load.return_value = {
                'temporal_data': sample_game_data,
                'market_data': sample_game_data,
                'team_data': sample_game_data,
                'betting_splits_data': sample_game_data
            }
            
            # Benchmark complete feature extraction
            start_time = asyncio.get_event_loop().time()
            feature_vector = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )
            extraction_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            assert feature_vector is not None
            assert extraction_time < 300  # Should be under 300ms for integration test
            
            # Benchmark Redis caching
            start_time = asyncio.get_event_loop().time()
            cache_success = await redis_store.cache_feature_vector_atomic(
                game_id, feature_vector
            )
            cache_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            assert cache_success is True
            assert cache_time < 100  # Should be under 100ms
            
            # Benchmark Redis retrieval
            start_time = asyncio.get_event_loop().time()
            cached_features = await redis_store.get_feature_vector_atomic(
                game_id, feature_vector.feature_version
            )
            retrieval_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            assert cached_features is not None
            assert retrieval_time < 50  # Should be under 50ms
    
    @pytest.mark.asyncio
    async def test_data_consistency_across_pipeline(
        self, 
        feature_pipeline, 
        redis_store, 
        sample_game_data
    ):
        """Test data consistency throughout complete pipeline"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        with patch.object(feature_pipeline, '_load_game_data') as mock_load:
            mock_load.return_value = {
                'temporal_data': sample_game_data,
                'market_data': sample_game_data,
                'team_data': sample_game_data,
                'betting_splits_data': sample_game_data
            }
            
            # Extract features
            original_features = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )
            assert original_features is not None
            
            # Cache and retrieve
            await redis_store.cache_feature_vector_atomic(game_id, original_features)
            cached_features = await redis_store.get_feature_vector_atomic(
                game_id, original_features.feature_version
            )
            
            # Verify data consistency
            assert cached_features.game_id == original_features.game_id
            assert cached_features.feature_version == original_features.feature_version
            assert cached_features.minutes_before_game == original_features.minutes_before_game
            
            # Verify temporal features consistency
            if original_features.temporal_features and cached_features.temporal_features:
                assert (
                    cached_features.temporal_features.feature_cutoff_time == 
                    original_features.temporal_features.feature_cutoff_time
                )
                assert (
                    cached_features.temporal_features.minutes_before_game == 
                    original_features.temporal_features.minutes_before_game
                )


@pytest.mark.integration
class TestConcurrentPipelineOperations:
    """Test concurrent operations in ML pipeline"""
    
    @pytest.mark.asyncio
    async def test_concurrent_feature_extraction(self):
        """Test concurrent feature extraction for multiple games"""
        pytest.skip("Requires full database setup - placeholder for concurrent testing")
        
        # This would test:
        # 1. Multiple simultaneous feature extractions
        # 2. Database connection pool handling
        # 3. Redis atomic operations under load
        # 4. No race conditions or data corruption
        
        game_ids = list(range(10000, 10100))  # 100 games
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        # Concurrent extraction tasks
        async def extract_features(game_id):
            pipeline = FeaturePipeline()
            return await pipeline.extract_features_for_game(game_id, cutoff_time)
        
        # Run concurrent extractions
        tasks = [extract_features(gid) for gid in game_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify no exceptions and consistent results
        successful_results = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_results) >= len(game_ids) * 0.8  # At least 80% success rate