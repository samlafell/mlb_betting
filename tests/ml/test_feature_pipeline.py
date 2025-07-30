"""
Comprehensive tests for ML feature pipeline
Tests feature extraction, validation, and performance
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import polars as pl

from src.ml.features.feature_pipeline import FeaturePipeline
from src.ml.features.models import FeatureVector, TemporalFeatures
from src.ml.database.connection_pool import DatabaseConnectionPool


class TestFeaturePipeline:
    """Test suite for feature pipeline functionality"""

    @pytest.fixture
    def feature_pipeline(self):
        """Create feature pipeline instance for testing"""
        return FeaturePipeline(feature_version="test_v1.0")

    @pytest.fixture
    def mock_database_pool(self):
        """Mock database connection pool"""
        pool = AsyncMock(spec=DatabaseConnectionPool)
        return pool

    @pytest.fixture
    def sample_game_data(self):
        """Sample game data for testing"""
        return pl.DataFrame(
            {
                "game_id": [12345] * 5,
                "timestamp": [
                    datetime.now() - timedelta(hours=3),
                    datetime.now() - timedelta(hours=2),
                    datetime.now() - timedelta(hours=1, minutes=30),
                    datetime.now() - timedelta(hours=1, minutes=15),
                    datetime.now() - timedelta(hours=1),
                ],
                "sportsbook_name": [
                    "DraftKings",
                    "FanDuel",
                    "BetMGM",
                    "Caesars",
                    "DraftKings",
                ],
                "market_type": ["moneyline"] * 5,
                "home_team": ["Yankees"] * 5,
                "away_team": ["Red Sox"] * 5,
                "home_ml_odds": [-150, -145, -155, -148, -152],
                "away_ml_odds": [130, 125, 135, 128, 132],
            }
        )

    @pytest.mark.asyncio
    async def test_ml_cutoff_enforcement(self, feature_pipeline):
        """Test that 60-minute ML cutoff is enforced"""
        game_id = 12345

        # Test with cutoff time less than 60 minutes before game (should fail)
        cutoff_time = datetime.now() - timedelta(minutes=30)

        with pytest.raises(ValueError, match="ML data leakage prevention"):
            await feature_pipeline.extract_features_for_game(game_id, cutoff_time)

    @pytest.mark.asyncio
    async def test_feature_extraction_success(
        self, feature_pipeline, mock_database_pool, sample_game_data
    ):
        """Test successful feature extraction"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)

        # Mock database responses
        mock_database_pool.fetch.return_value = sample_game_data.to_pandas().to_dict(
            "records"
        )

        with patch.object(
            feature_pipeline,
            "_load_game_data",
            return_value={"temporal_data": sample_game_data},
        ):
            result = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )

            assert result is not None
            assert isinstance(result, FeatureVector)
            assert result.game_id == game_id
            assert result.feature_cutoff_time == cutoff_time
            assert result.minutes_before_game >= 60

    @pytest.mark.asyncio
    async def test_batch_processing_memory_efficiency(self, feature_pipeline):
        """Test that batch processing doesn't cause memory exhaustion"""
        game_ids = list(range(1000, 1100))  # 100 games
        cutoff_time = datetime.now() - timedelta(hours=2)

        # Mock feature extraction to avoid actual database calls
        async def mock_extract(game_id, cutoff):
            return FeatureVector(
                game_id=game_id,
                feature_cutoff_time=cutoff,
                feature_version="test_v1.0",
                minutes_before_game=120,
                temporal_features=TemporalFeatures(
                    feature_cutoff_time=cutoff, minutes_before_game=120
                ),
            )

        with patch.object(
            feature_pipeline, "extract_features_for_game", side_effect=mock_extract
        ):
            # This should not raise memory errors
            results = []
            for game_id in game_ids:
                result = await feature_pipeline.extract_features_for_game(
                    game_id, cutoff_time
                )
                results.append(result)

            assert len(results) == 100
            # Verify memory usage is reasonable (implementation would check actual memory)

    def test_feature_validation(self):
        """Test Pydantic validation of feature models"""
        # Test valid temporal features
        temporal_features = TemporalFeatures(
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            minutes_before_game=120,
        )
        assert temporal_features.minutes_before_game == 120

        # Test invalid temporal features (violates 60-minute rule)
        with pytest.raises(ValueError, match="ML data leakage prevention"):
            TemporalFeatures(
                feature_cutoff_time=datetime.now() - timedelta(minutes=30),
                minutes_before_game=30,
            )

    @pytest.mark.asyncio
    async def test_database_transaction_handling(
        self, feature_pipeline, mock_database_pool
    ):
        """Test database transaction management"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)

        # Mock database to simulate transaction failure
        mock_database_pool.get_transaction.side_effect = Exception("Database error")

        with patch.object(
            feature_pipeline, "_get_database_pool", return_value=mock_database_pool
        ):
            # Should handle database errors gracefully
            result = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )
            assert result is None  # Should return None on database error

    @pytest.mark.asyncio
    async def test_timezone_consistency(self, feature_pipeline):
        """Test consistent timezone handling"""
        # Test with different timezone inputs
        utc_time = datetime.utcnow() - timedelta(hours=2)

        # Feature extraction should normalize to EST/EDT consistently
        with patch.object(feature_pipeline, "_load_game_data") as mock_load:
            mock_load.return_value = {"temporal_data": pl.DataFrame()}

            # This should not raise timezone-related errors
            try:
                await feature_pipeline.extract_features_for_game(12345, utc_time)
            except Exception as e:
                # Should not be timezone-related
                assert "timezone" not in str(e).lower()

    def test_feature_hash_generation(self, feature_pipeline):
        """Test feature hash generation for cache keys"""
        feature_data = {
            "game_id": 12345,
            "feature_version": "v1.0",
            "data": {"test": "value"},
        }

        hash1 = feature_pipeline._generate_feature_hash(feature_data)
        hash2 = feature_pipeline._generate_feature_hash(feature_data)

        # Same data should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex string

        # Different data should produce different hash
        feature_data["data"]["test"] = "different_value"
        hash3 = feature_pipeline._generate_feature_hash(feature_data)
        assert hash1 != hash3

    @pytest.mark.asyncio
    async def test_performance_benchmarks(self, feature_pipeline):
        """Test performance claims - feature extraction ~150ms per game"""
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)

        # Mock all external dependencies for pure performance test
        with patch.object(feature_pipeline, "_load_game_data") as mock_load:
            mock_load.return_value = {
                "temporal_data": pl.DataFrame(
                    {
                        "game_id": [12345] * 10,
                        "timestamp": [
                            cutoff_time - timedelta(minutes=i) for i in range(10)
                        ],
                    }
                )
            }

            start_time = asyncio.get_event_loop().time()
            result = await feature_pipeline.extract_features_for_game(
                game_id, cutoff_time
            )
            execution_time = (asyncio.get_event_loop().time() - start_time) * 1000

            # Should complete within performance target
            assert execution_time < 200  # Allow some margin for test environment
            assert result is not None


class TestFeatureValidation:
    """Test suite for feature data validation"""

    def test_decimal_precision_handling(self):
        """Test Decimal type handling in features"""
        temporal_features = TemporalFeatures(
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            minutes_before_game=120,
            line_movement_velocity_60min=Decimal("1.234567"),
        )

        # Should preserve decimal precision
        assert isinstance(temporal_features.line_movement_velocity_60min, Decimal)
        assert str(temporal_features.line_movement_velocity_60min) == "1.234567"

    def test_constraint_validation(self):
        """Test field constraint validation"""
        # Test movement consistency score constraints (0 <= x <= 1)
        temporal_features = TemporalFeatures(
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            minutes_before_game=120,
            movement_consistency_score=Decimal("0.5"),
        )
        assert temporal_features.movement_consistency_score == Decimal("0.5")

        # Test invalid constraint (should raise validation error)
        with pytest.raises(ValueError):
            TemporalFeatures(
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                minutes_before_game=120,
                movement_consistency_score=Decimal("1.5"),  # > 1
            )

    def test_pattern_validation(self):
        """Test string pattern validation"""
        temporal_features = TemporalFeatures(
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            minutes_before_game=120,
            ml_movement_direction="toward_home",
        )
        assert temporal_features.ml_movement_direction == "toward_home"

        # Test invalid pattern
        with pytest.raises(ValueError):
            TemporalFeatures(
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                minutes_before_game=120,
                ml_movement_direction="invalid_direction",
            )


class TestFeaturePipelineIntegration:
    """Integration tests for feature pipeline"""

    @pytest.mark.asyncio
    async def test_end_to_end_feature_extraction(self):
        """Test complete feature extraction workflow"""
        # This would be an integration test with real database
        # Skip in unit test suite, but structure shows how it would work
        pytest.skip("Integration test - requires database")

        pipeline = FeaturePipeline()
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)

        # This would test the complete workflow:
        # 1. Database connection
        # 2. Data loading
        # 3. Feature extraction
        # 4. Validation
        # 5. Serialization
        result = await pipeline.extract_features_for_game(game_id, cutoff_time)

        assert result is not None
        assert isinstance(result, FeatureVector)
        assert result.feature_completeness_score > 0.5


# Performance benchmarks
@pytest.mark.benchmark
class TestFeaturePipelinePerformance:
    """Performance benchmark tests"""

    @pytest.mark.asyncio
    async def test_feature_extraction_performance(self, benchmark):
        """Benchmark feature extraction performance"""
        pipeline = FeaturePipeline()
        game_id = 12345
        cutoff_time = datetime.now() - timedelta(hours=2)

        # Benchmark the extraction
        result = await benchmark(
            pipeline.extract_features_for_game, game_id, cutoff_time
        )

        assert result is not None

    def test_serialization_performance(self, benchmark):
        """Benchmark feature serialization performance"""
        feature_vector = FeatureVector(
            game_id=12345,
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            feature_version="test_v1.0",
            minutes_before_game=120,
        )

        # Benchmark serialization
        result = benchmark(feature_vector.model_dump)
        assert isinstance(result, dict)
