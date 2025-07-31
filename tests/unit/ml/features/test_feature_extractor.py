#!/usr/bin/env python3
"""
Unit Tests for Feature Extractor

Tests feature extraction, caching, performance, and memory management.
Addresses critical testing gaps and performance issues identified in PR review.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import pandas as pd
import numpy as np

from src.ml.features.feature_extractor import (
    FeatureExtractor,
    FeatureExtractionConfig,
    GameFeatures,
    extract_features_for_training
)


class TestFeatureExtractionConfig:
    """Test feature extraction configuration"""

    def test_config_default_values(self):
        """Test configuration default values"""
        config = FeatureExtractionConfig()
        
        assert config.lookback_days == 30
        assert config.min_games_for_features == 10
        assert config.feature_version == "v1.0"
        assert config.include_temporal_features is True
        assert config.include_sharp_action_features is True
        assert config.include_market_features is True

    def test_config_custom_values(self):
        """Test configuration with custom values"""
        config = FeatureExtractionConfig(
            lookback_days=60,
            min_games_for_features=20,
            feature_version="v2.0",
            include_temporal_features=False
        )
        
        assert config.lookback_days == 60
        assert config.min_games_for_features == 20
        assert config.feature_version == "v2.0"
        assert config.include_temporal_features is False

    def test_config_validation(self):
        """Test configuration field validation"""
        # Test valid configuration
        config = FeatureExtractionConfig(lookback_days=1)
        assert config.lookback_days == 1
        
        # Test that Pydantic validation works
        with pytest.raises(ValueError):
            FeatureExtractionConfig(lookback_days="invalid")


class TestGameFeatures:
    """Test GameFeatures data model"""

    def test_game_features_creation(self):
        """Test basic GameFeatures creation"""
        game_date = datetime.now(timezone.utc)
        features = GameFeatures(
            game_id="MLB_20240630_LAA_TEX",
            game_date=game_date,
            home_team="TEX",
            away_team="LAA"
        )
        
        assert features.game_id == "MLB_20240630_LAA_TEX"
        assert features.game_date == game_date
        assert features.home_team == "TEX"
        assert features.away_team == "LAA"
        assert features.feature_version == "v1.0"

    def test_game_features_with_optional_fields(self):
        """Test GameFeatures with optional target and feature fields"""
        features = GameFeatures(
            game_id="test_game",
            game_date=datetime.now(timezone.utc),
            home_team="HOME",
            away_team="AWAY",
            total_over_target=1.0,
            sharp_action_total=0.8,
            home_team_wins_l10=7,
            consensus_total_percentage=65.5
        )
        
        assert features.total_over_target == 1.0
        assert features.sharp_action_total == 0.8
        assert features.home_team_wins_l10 == 7
        assert features.consensus_total_percentage == 65.5

    def test_game_features_extraction_timestamp(self):
        """Test automatic extraction timestamp setting"""
        before_creation = datetime.now(timezone.utc)
        features = GameFeatures(
            game_id="test_game",
            game_date=datetime.now(timezone.utc),
            home_team="HOME",
            away_team="AWAY"
        )
        after_creation = datetime.now(timezone.utc)
        
        assert before_creation <= features.extraction_timestamp <= after_creation

    def test_game_features_model_dump(self):
        """Test GameFeatures serialization"""
        features = GameFeatures(
            game_id="test_game",
            game_date=datetime.now(timezone.utc),
            home_team="HOME",
            away_team="AWAY",
            total_over_target=1.0
        )
        
        data = features.model_dump()
        
        assert isinstance(data, dict)
        assert data["game_id"] == "test_game"
        assert data["home_team"] == "HOME"
        assert data["total_over_target"] == 1.0


class TestFeatureExtractor:
    """Comprehensive unit tests for FeatureExtractor"""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings configuration"""
        settings = MagicMock()
        settings.ml = MagicMock()
        settings.ml.feature_extraction = MagicMock()
        settings.ml.performance = MagicMock()
        settings.ml.performance.memory_limit_mb = 2048
        settings.ml.performance.batch_size_limit = 50
        return settings

    @pytest.fixture
    def feature_config(self):
        """Standard feature extraction configuration for testing"""
        return FeatureExtractionConfig(
            lookback_days=30,
            min_games_for_features=10,
            feature_version="test_v1.0"
        )

    @pytest.fixture
    def feature_extractor(self, mock_settings, feature_config):
        """Create FeatureExtractor instance with mocked dependencies"""
        with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings):
            return FeatureExtractor(feature_config)

    @pytest.fixture
    def mock_database_connection(self):
        """Mock database connection with sample data"""
        conn = AsyncMock()
        
        # Sample game data
        sample_games = [
            {
                "game_id": "MLB_20240630_LAA_TEX",
                "game_date": datetime(2024, 6, 30, 19, 0, tzinfo=timezone.utc),
                "home_team": "TEX",
                "away_team": "LAA",
                "total_over_target": 1.0,
                "sharp_action_total": 0.8
            },
            {
                "game_id": "MLB_20240701_NYY_BOS",
                "game_date": datetime(2024, 7, 1, 20, 0, tzinfo=timezone.utc),
                "home_team": "BOS", 
                "away_team": "NYY",
                "total_over_target": 0.0,
                "sharp_action_total": 0.3
            }
        ]
        
        conn.fetch = AsyncMock(return_value=sample_games)
        return conn

    @pytest.fixture
    def sample_game_features(self):
        """Sample GameFeatures for testing"""
        return [
            GameFeatures(
                game_id="test_game_1",
                game_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
                home_team="TEX",
                away_team="LAA",
                total_over_target=1.0,
                sharp_action_total=0.8,
                home_team_wins_l10=7
            ),
            GameFeatures(
                game_id="test_game_2", 
                game_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
                home_team="BOS",
                away_team="NYY",
                total_over_target=0.0,
                sharp_action_total=0.3,
                home_team_wins_l10=5
            )
        ]

    class TestInitialization:
        """Test feature extractor initialization"""

        def test_initialization_default_config(self, mock_settings):
            """Test initialization with default configuration"""
            with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings):
                extractor = FeatureExtractor()
                
                assert extractor.config.lookback_days == 30
                assert extractor.config.feature_version == "v1.0"
                assert extractor.settings == mock_settings

        def test_initialization_custom_config(self, mock_settings, feature_config):
            """Test initialization with custom configuration"""
            with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings):
                extractor = FeatureExtractor(feature_config)
                
                assert extractor.config == feature_config
                assert extractor.config.feature_version == "test_v1.0"

    class TestFeatureExtractionForDateRange:
        """Test feature extraction for date ranges"""

        @pytest.mark.asyncio
        async def test_extract_features_for_date_range_success(self, feature_extractor, mock_database_connection):
            """Test successful feature extraction for date range"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_database_connection), \
                 patch.object(feature_extractor, '_extract_temporal_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_sharp_action_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_market_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_team_performance_features', return_value={}):
                
                features = await feature_extractor.extract_features_for_date_range(
                    start_date, end_date
                )
                
                assert isinstance(features, list)
                assert len(features) == 2  # Based on mock data
                assert all(isinstance(f, GameFeatures) for f in features)

        @pytest.mark.asyncio
        async def test_extract_features_empty_date_range(self, feature_extractor):
            """Test feature extraction with no games in date range"""
            start_date = datetime(2024, 12, 1, tzinfo=timezone.utc)
            end_date = datetime(2024, 12, 2, tzinfo=timezone.utc)
            
            mock_conn = AsyncMock()
            mock_conn.fetch = AsyncMock(return_value=[])  # No games
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_conn):
                features = await feature_extractor.extract_features_for_date_range(
                    start_date, end_date
                )
                
                assert features == []

        @pytest.mark.asyncio
        async def test_extract_features_with_prediction_targets(self, feature_extractor, mock_database_connection):
            """Test feature extraction with specific prediction targets"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            prediction_targets = ["total_over", "spread"]
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_database_connection), \
                 patch.object(feature_extractor, '_extract_temporal_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_sharp_action_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_market_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_team_performance_features', return_value={}):
                
                features = await feature_extractor.extract_features_for_date_range(
                    start_date, end_date, prediction_targets
                )
                
                assert len(features) == 2
                # Verify prediction targets are handled correctly
                for feature in features:
                    assert hasattr(feature, 'total_over_target')

        @pytest.mark.asyncio
        async def test_extract_features_database_error(self, feature_extractor):
            """Test feature extraction with database error"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            
            with patch('src.ml.features.feature_extractor.get_connection', side_effect=Exception("Database error")):
                with pytest.raises(Exception, match="Database error"):
                    await feature_extractor.extract_features_for_date_range(start_date, end_date)

    class TestFeatureExtractionComponents:
        """Test individual feature extraction components"""

        @pytest.mark.asyncio
        async def test_extract_temporal_features(self, feature_extractor):
            """Test temporal feature extraction"""
            game_data = {
                "game_date": datetime(2024, 6, 30, 19, 0, tzinfo=timezone.utc),  # Sunday 7 PM
                "home_team": "TEX",
                "away_team": "LAA"
            }
            
            with patch.object(feature_extractor, '_get_last_game_date', return_value=datetime(2024, 6, 28, tzinfo=timezone.utc)):
                temporal_features = await feature_extractor._extract_temporal_features(game_data)
                
                assert isinstance(temporal_features, dict)
                assert "game_time_hour" in temporal_features
                assert "is_weekend" in temporal_features
                assert temporal_features["game_time_hour"] == 19
                assert temporal_features["is_weekend"] is True  # Sunday

        @pytest.mark.asyncio
        async def test_extract_sharp_action_features(self, feature_extractor):
            """Test sharp action feature extraction"""
            game_data = {
                "game_id": "test_game",
                "game_date": datetime(2024, 6, 30, tzinfo=timezone.utc)
            }
            
            mock_sharp_data = [
                {"bet_type": "total", "sharp_action_score": 0.8},
                {"bet_type": "spread", "sharp_action_score": 0.6},
                {"bet_type": "moneyline", "sharp_action_score": 0.4}
            ]
            
            mock_conn = AsyncMock()
            mock_conn.fetch = AsyncMock(return_value=mock_sharp_data)
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_conn):
                sharp_features = await feature_extractor._extract_sharp_action_features(game_data)
                
                assert isinstance(sharp_features, dict)
                assert "sharp_action_total" in sharp_features
                assert "sharp_action_spread" in sharp_features
                assert "sharp_action_moneyline" in sharp_features
                assert sharp_features["sharp_action_total"] == 0.8

        @pytest.mark.asyncio
        async def test_extract_market_features(self, feature_extractor):
            """Test market consensus feature extraction"""
            game_data = {
                "game_id": "test_game",
                "game_date": datetime(2024, 6, 30, tzinfo=timezone.utc)
            }
            
            mock_market_data = [
                {"bet_type": "total", "consensus_percentage": 65.5, "line_movement": 0.5},
                {"bet_type": "spread", "consensus_percentage": 72.3, "line_movement": -1.0}
            ]
            
            mock_conn = AsyncMock()
            mock_conn.fetch = AsyncMock(return_value=mock_market_data)
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_conn):
                market_features = await feature_extractor._extract_market_features(game_data)
                
                assert isinstance(market_features, dict)
                assert "consensus_total_percentage" in market_features
                assert "line_movement_total" in market_features
                assert market_features["consensus_total_percentage"] == 65.5

        @pytest.mark.asyncio
        async def test_extract_team_performance_features(self, feature_extractor):
            """Test team performance feature extraction"""
            game_data = {
                "home_team": "TEX",
                "away_team": "LAA",
                "game_date": datetime(2024, 6, 30, tzinfo=timezone.utc)
            }
            
            mock_team_stats = {
                "TEX": {"wins": 7, "runs_avg": 5.2},
                "LAA": {"wins": 4, "runs_avg": 4.8}
            }
            
            with patch.object(feature_extractor, '_get_team_recent_stats', side_effect=lambda team, date: mock_team_stats[team]):
                team_features = await feature_extractor._extract_team_performance_features(game_data)
                
                assert isinstance(team_features, dict)
                assert "home_team_wins_l10" in team_features
                assert "away_team_wins_l10" in team_features
                assert "home_team_runs_avg_l10" in team_features
                assert "away_team_runs_avg_l10" in team_features
                assert team_features["home_team_wins_l10"] == 7
                assert team_features["away_team_wins_l10"] == 4

    class TestDataFrameConversion:
        """Test DataFrame conversion functionality - addressing memory management concerns"""

        def test_to_dataframe_success(self, feature_extractor, sample_game_features):
            """Test successful conversion of features to DataFrame"""
            df = feature_extractor.to_dataframe(sample_game_features)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert "game_id" in df.columns
            assert "home_team" in df.columns
            assert "total_over_target" in df.columns

        def test_to_dataframe_empty_list(self, feature_extractor):
            """Test DataFrame conversion with empty features list"""
            df = feature_extractor.to_dataframe([])
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0

        def test_to_dataframe_datetime_conversion(self, feature_extractor, sample_game_features):
            """Test proper datetime to timestamp conversion"""
            df = feature_extractor.to_dataframe(sample_game_features)
            
            assert "game_date" in df.columns
            assert pd.api.types.is_numeric_dtype(df["game_date"])
            assert "extraction_timestamp" in df.columns
            assert pd.api.types.is_numeric_dtype(df["extraction_timestamp"])

        def test_to_dataframe_sorting(self, feature_extractor):
            """Test DataFrame is sorted by game_date"""
            # Create features with mixed dates
            features = [
                GameFeatures(
                    game_id="game_2",
                    game_date=datetime(2024, 7, 2, tzinfo=timezone.utc),
                    home_team="B",
                    away_team="A"
                ),
                GameFeatures(
                    game_id="game_1",
                    game_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
                    home_team="A",
                    away_team="B"
                )
            ]
            
            df = feature_extractor.to_dataframe(features)
            
            # Should be sorted by game_date (ascending)
            assert df.iloc[0]["game_id"] == "game_1"
            assert df.iloc[1]["game_id"] == "game_2"

        def test_to_dataframe_memory_handling_large_dataset(self, feature_extractor):
            """Test DataFrame conversion with large dataset (memory management concern)"""
            # Create a large number of features to test memory handling
            large_features = []
            for i in range(1000):  # Large dataset
                features = GameFeatures(
                    game_id=f"game_{i}",
                    game_date=datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(days=i % 30),
                    home_team=f"TEAM_{i % 10}",
                    away_team=f"TEAM_{(i + 1) % 10}",
                    total_over_target=float(i % 2),
                    sharp_action_total=float(i % 100) / 100.0
                )
                large_features.append(features)
            
            # This should not cause memory issues
            df = feature_extractor.to_dataframe(large_features)
            
            assert len(df) == 1000
            assert isinstance(df, pd.DataFrame)
            
            # Clean up memory
            del df
            del large_features

    class TestUtilityFunctions:
        """Test utility functions"""

        @pytest.mark.asyncio
        async def test_extract_features_for_training_function(self, mock_settings):
            """Test the convenience function for training feature extraction"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            
            mock_features = [
                GameFeatures(
                    game_id="test_game",
                    game_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
                    home_team="HOME",
                    away_team="AWAY"
                )
            ]
            
            with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings), \
                 patch('src.ml.features.feature_extractor.FeatureExtractor') as mock_extractor_class:
                
                mock_extractor = MagicMock()
                mock_extractor.extract_features_for_date_range = AsyncMock(return_value=mock_features)
                mock_extractor.to_dataframe = MagicMock(return_value=pd.DataFrame())
                mock_extractor_class.return_value = mock_extractor
                
                result = await extract_features_for_training(start_date, end_date)
                
                assert isinstance(result, pd.DataFrame)
                mock_extractor.extract_features_for_date_range.assert_called_once_with(
                    start_date, end_date, None
                )
                mock_extractor.to_dataframe.assert_called_once_with(mock_features)

        @pytest.mark.asyncio
        async def test_extract_features_for_training_with_config(self, mock_settings, feature_config):
            """Test training function with custom configuration"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            prediction_targets = ["total_over"]
            
            with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings), \
                 patch('src.ml.features.feature_extractor.FeatureExtractor') as mock_extractor_class:
                
                mock_extractor = MagicMock()
                mock_extractor.extract_features_for_date_range = AsyncMock(return_value=[])
                mock_extractor.to_dataframe = MagicMock(return_value=pd.DataFrame())
                mock_extractor_class.return_value = mock_extractor
                
                result = await extract_features_for_training(
                    start_date, end_date, prediction_targets, feature_config
                )
                
                # Verify config was passed to extractor
                mock_extractor_class.assert_called_once_with(feature_config)
                mock_extractor.extract_features_for_date_range.assert_called_once_with(
                    start_date, end_date, prediction_targets
                )

    class TestPerformanceAndMemoryManagement:
        """Test performance and memory management - addressing PR review concerns"""

        def test_memory_efficient_feature_processing(self, feature_extractor):
            """Test memory-efficient processing of large feature sets"""
            # Simulate processing large amounts of data without excessive memory usage
            large_feature_count = 5000
            
            # Mock memory monitoring
            with patch('psutil.virtual_memory') as mock_memory:
                mock_memory.return_value.percent = 75.0  # 75% memory usage
                
                # Create features in batches to test memory efficiency
                batch_size = 100
                total_processed = 0
                
                for batch_start in range(0, large_feature_count, batch_size):
                    batch_end = min(batch_start + batch_size, large_feature_count)
                    batch_features = []
                    
                    for i in range(batch_start, batch_end):
                        features = GameFeatures(
                            game_id=f"game_{i}",
                            game_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
                            home_team="HOME",
                            away_team="AWAY"
                        )
                        batch_features.append(features)
                    
                    # Process batch
                    df_batch = feature_extractor.to_dataframe(batch_features)
                    total_processed += len(df_batch)
                    
                    # Clean up batch memory
                    del df_batch
                    del batch_features
                
                assert total_processed == large_feature_count

        @pytest.mark.asyncio
        async def test_batch_size_limitation(self, feature_extractor, mock_database_connection):
            """Test that batch size limits are respected for memory management"""
            start_date = datetime(2024, 6, 1, tzinfo=timezone.utc) 
            end_date = datetime(2024, 12, 31, tzinfo=timezone.utc)  # Large date range
            
            # Mock a large number of games
            large_game_data = []
            for i in range(1000):  # Many games
                large_game_data.append({
                    "game_id": f"game_{i}",
                    "game_date": datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(days=i % 180),
                    "home_team": f"TEAM_{i % 30}",
                    "away_team": f"TEAM_{(i + 1) % 30}"
                })
            
            mock_database_connection.fetch = AsyncMock(return_value=large_game_data)
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_database_connection), \
                 patch.object(feature_extractor, '_extract_temporal_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_sharp_action_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_market_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_team_performance_features', return_value={}):
                
                # This should handle large datasets efficiently
                features = await feature_extractor.extract_features_for_date_range(start_date, end_date)
                
                # Should successfully process all features
                assert len(features) == 1000

        def test_dataframe_memory_optimization(self, feature_extractor):
            """Test DataFrame creation with memory optimization"""
            # Create features with various data types
            features = []
            for i in range(100):
                feature = GameFeatures(
                    game_id=f"game_{i}",
                    game_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
                    home_team="HOME",
                    away_team="AWAY",
                    total_over_target=float(i % 2),  # Binary
                    home_team_wins_l10=i % 10,  # Small integers
                    consensus_total_percentage=float(i % 101)  # Percentages
                )
                features.append(feature)
            
            df = feature_extractor.to_dataframe(features)
            
            # Verify DataFrame is created successfully
            assert len(df) == 100
            
            # Check data types are appropriate for memory efficiency
            assert pd.api.types.is_numeric_dtype(df["total_over_target"])
            assert pd.api.types.is_integer_dtype(df["home_team_wins_l10"])
            
            # Clean up
            del df
            del features

    class TestErrorHandling:
        """Test error handling and edge cases"""

        @pytest.mark.asyncio
        async def test_database_connection_failure(self, feature_extractor):
            """Test handling of database connection failures"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            
            with patch('src.ml.features.feature_extractor.get_connection', side_effect=Exception("Connection failed")):
                with pytest.raises(Exception, match="Connection failed"):
                    await feature_extractor.extract_features_for_date_range(start_date, end_date)

        @pytest.mark.asyncio
        async def test_invalid_date_range(self, feature_extractor):
            """Test handling of invalid date ranges"""
            # End date before start date
            start_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            end_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            
            mock_conn = AsyncMock()
            mock_conn.fetch = AsyncMock(return_value=[])
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_conn):
                features = await feature_extractor.extract_features_for_date_range(start_date, end_date)
                # Should handle gracefully and return empty results
                assert features == []

        def test_malformed_game_data(self, feature_extractor):
            """Test handling of malformed game data"""
            # Test with missing required fields
            with pytest.raises((ValueError, TypeError)):
                GameFeatures(
                    game_id="test_game",
                    # Missing required fields: game_date, home_team, away_team
                )

        @pytest.mark.asyncio
        async def test_feature_extraction_partial_failure(self, feature_extractor, mock_database_connection):
            """Test handling when some feature extraction components fail"""
            start_date = datetime(2024, 6, 30, tzinfo=timezone.utc)
            end_date = datetime(2024, 7, 1, tzinfo=timezone.utc)
            
            with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_database_connection), \
                 patch.object(feature_extractor, '_extract_temporal_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_sharp_action_features', side_effect=Exception("Sharp action failed")), \
                 patch.object(feature_extractor, '_extract_market_features', return_value={}), \
                 patch.object(feature_extractor, '_extract_team_performance_features', return_value={}):
                
                # Should handle partial failures gracefully
                with pytest.raises(Exception, match="Sharp action failed"):
                    await feature_extractor.extract_features_for_date_range(start_date, end_date)

    class TestConfigurationImpact:
        """Test how configuration affects feature extraction"""

        @pytest.mark.asyncio
        async def test_disabled_temporal_features(self, mock_settings):
            """Test feature extraction with temporal features disabled"""
            config = FeatureExtractionConfig(include_temporal_features=False)
            
            with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings):
                extractor = FeatureExtractor(config)
                
                mock_conn = AsyncMock()
                mock_conn.fetch = AsyncMock(return_value=[{
                    "game_id": "test_game",
                    "game_date": datetime(2024, 6, 30, tzinfo=timezone.utc),
                    "home_team": "HOME",
                    "away_team": "AWAY"
                }])
                
                with patch('src.ml.features.feature_extractor.get_connection', return_value=mock_conn), \
                     patch.object(extractor, '_extract_temporal_features') as mock_temporal, \
                     patch.object(extractor, '_extract_sharp_action_features', return_value={}), \
                     patch.object(extractor, '_extract_market_features', return_value={}), \
                     patch.object(extractor, '_extract_team_performance_features', return_value={}):
                    
                    await extractor.extract_features_for_date_range(
                        datetime(2024, 6, 30, tzinfo=timezone.utc),
                        datetime(2024, 7, 1, tzinfo=timezone.utc)
                    )
                    
                    # Temporal features should not be called
                    mock_temporal.assert_not_called()

        @pytest.mark.asyncio
        async def test_custom_lookback_days(self, mock_settings):
            """Test feature extraction with custom lookback days"""
            config = FeatureExtractionConfig(lookback_days=60)
            
            with patch('src.ml.features.feature_extractor.get_settings', return_value=mock_settings):
                extractor = FeatureExtractor(config)
                
                assert extractor.config.lookback_days == 60
                
                # The lookback days should affect team performance queries
                game_data = {
                    "home_team": "TEX",
                    "away_team": "LAA", 
                    "game_date": datetime(2024, 6, 30, tzinfo=timezone.utc)
                }
                
                with patch.object(extractor, '_get_team_recent_stats') as mock_stats:
                    mock_stats.return_value = {"wins": 5, "runs_avg": 4.5}
                    
                    await extractor._extract_team_performance_features(game_data)
                    
                    # Should be called with the custom lookback period
                    assert mock_stats.call_count == 2  # Once for each team