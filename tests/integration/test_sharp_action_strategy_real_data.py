"""
Integration Tests for Sharp Action Strategy with Real Data

Tests the Sharp Action Strategy processors after removing mock data dependencies.
Validates that all processors work correctly with actual database queries and 
handle both populated and empty data scenarios gracefully.

Key test areas:
1. Database connectivity and configuration
2. Real data querying and processing
3. Empty data scenario handling
4. Sharp action detection with actual patterns
5. Error handling and recovery
6. Performance and reliability

Created for GitHub issue #68: Remove Mock Data from Sharp Action Strategy
"""

import asyncio
import pytest
from datetime import date, datetime
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock

from src.analysis.processors.sharp_action_processor import UnifiedSharpActionProcessor
from src.analysis.processors.hybrid_sharp_processor import UnifiedHybridSharpProcessor
from src.services.sharp_action_detection_service import SharpActionDetectionService
from src.core.config import get_settings
from src.data.database.connection import DatabaseConnection


class TestSharpActionStrategyRealData:
    """Integration tests for Sharp Action Strategy with real data."""

    @pytest.fixture(scope="class")
    def config(self):
        """Get application configuration."""
        return get_settings()

    @pytest.fixture(scope="class")
    def db_connection(self, config):
        """Create database connection for testing."""
        return DatabaseConnection(config.database.connection_string)

    @pytest.fixture
    def mock_repository(self):
        """Create mock repository for processor initialization."""
        return Mock()

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration for processor initialization."""
        return {
            "min_differential_threshold": 10.0,
            "high_confidence_threshold": 20.0,
            "volume_weight_factor": 1.5,
            "min_volume_threshold": 100,
        }

    @pytest.fixture
    def sharp_processor(self, mock_repository, mock_config):
        """Create UnifiedSharpActionProcessor instance."""
        return UnifiedSharpActionProcessor(mock_repository, mock_config)

    @pytest.fixture
    def hybrid_processor(self, mock_repository, mock_config):
        """Create UnifiedHybridSharpProcessor instance."""
        return UnifiedHybridSharpProcessor(mock_repository, mock_config)

    @pytest.fixture
    def detection_service(self, db_connection):
        """Create SharpActionDetectionService instance."""
        return SharpActionDetectionService(db_connection)

    @pytest.mark.asyncio
    async def test_database_connectivity(self, db_connection):
        """Test basic database connectivity."""
        async with db_connection.get_async_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1, "Database connection should return 1"

    @pytest.mark.asyncio
    async def test_required_tables_exist(self, db_connection):
        """Test that required database tables exist."""
        async with db_connection.get_async_connection() as conn:
            # Test unified_betting_splits table
            try:
                count = await conn.fetchval("SELECT COUNT(*) FROM curated.unified_betting_splits")
                assert count >= 0, "unified_betting_splits table should be accessible"
            except Exception as e:
                pytest.fail(f"unified_betting_splits table not accessible: {e}")

            # Test enhanced_games table
            try:
                count = await conn.fetchval("SELECT COUNT(*) FROM curated.enhanced_games")
                assert count >= 0, "enhanced_games table should be accessible"
            except Exception as e:
                pytest.fail(f"enhanced_games table not accessible: {e}")

    @pytest.mark.asyncio
    async def test_sharp_processor_real_game_data(self, sharp_processor):
        """Test Sharp Action Processor can query real game data."""
        # Test with various time windows
        for minutes_ahead in [60, 180, 1440]:  # 1 hour, 3 hours, 24 hours
            try:
                game_data = await sharp_processor._get_real_game_data(minutes_ahead)
                
                assert isinstance(game_data, list), f"Should return list for {minutes_ahead} minutes"
                
                # If we have games, validate the structure
                if game_data:
                    for game in game_data:
                        assert "game_id" in game, "Game should have game_id"
                        assert "home_team" in game, "Game should have home_team"
                        assert "away_team" in game, "Game should have away_team"
                        assert "game_datetime" in game, "Game should have game_datetime"
                        
                        # Validate data types
                        assert isinstance(game["game_id"], int), "game_id should be integer"
                        assert isinstance(game["home_team"], str), "home_team should be string"
                        assert isinstance(game["away_team"], str), "away_team should be string"
                        
            except Exception as e:
                pytest.fail(f"Sharp processor failed to get real game data: {e}")

    @pytest.mark.asyncio
    async def test_sharp_processor_betting_splits_data(self, sharp_processor):
        """Test Sharp Action Processor can query real betting splits data."""
        # First get some game data
        game_data = await sharp_processor._get_real_game_data(1440)  # 24 hours
        
        try:
            splits_data = await sharp_processor._get_betting_splits_data(game_data, 180)
            
            assert isinstance(splits_data, list), "Should return list of splits data"
            
            # If we have splits data, validate the structure
            if splits_data:
                for split in splits_data:
                    assert "game_id" in split, "Split should have game_id"
                    assert "split_type" in split, "Split should have split_type"
                    assert "source" in split, "Split should have source"
                    assert "book" in split, "Split should have book"
                    
                    # Validate split type is valid
                    assert split["split_type"] in ["moneyline", "spread", "total_over"], \
                        f"Invalid split_type: {split['split_type']}"
                    
                    # If we have percentages, they should be valid
                    if split.get("money_percentage") is not None:
                        assert 0 <= split["money_percentage"] <= 100, \
                            "money_percentage should be between 0 and 100"
                    
                    if split.get("bet_percentage") is not None:
                        assert 0 <= split["bet_percentage"] <= 100, \
                            "bet_percentage should be between 0 and 100"
            
        except Exception as e:
            pytest.fail(f"Sharp processor failed to get betting splits data: {e}")

    @pytest.mark.asyncio
    async def test_hybrid_processor_real_game_data(self, hybrid_processor):
        """Test Hybrid Sharp Processor can query real game data."""
        try:
            game_data = await hybrid_processor._get_real_game_data(180)  # 3 hours
            
            assert isinstance(game_data, list), "Should return list of games"
            
            # If we have games, validate the structure
            if game_data:
                for game in game_data:
                    assert "game_id" in game, "Game should have game_id"
                    assert "home_team" in game, "Game should have home_team"
                    assert "away_team" in game, "Game should have away_team"
                    assert "game_datetime" in game, "Game should have game_datetime"
                    
        except Exception as e:
            pytest.fail(f"Hybrid processor failed to get real game data: {e}")

    @pytest.mark.asyncio
    async def test_hybrid_processor_hybrid_sharp_data(self, hybrid_processor):
        """Test Hybrid Sharp Processor can query real hybrid data."""
        # First get some game data
        game_data = await hybrid_processor._get_real_game_data(1440)  # 24 hours
        
        try:
            hybrid_data = await hybrid_processor._get_hybrid_sharp_data(game_data, 180)
            
            assert isinstance(hybrid_data, list), "Should return list of hybrid data"
            
            # If we have hybrid data, validate the structure
            if hybrid_data:
                for hybrid in hybrid_data:
                    assert "game_id" in hybrid, "Hybrid data should have game_id"
                    assert "split_type" in hybrid, "Hybrid data should have split_type"
                    assert "source" in hybrid, "Hybrid data should have source"
                    assert "book" in hybrid, "Hybrid data should have book"
                    
                    # Check for specific hybrid fields
                    expected_fields = [
                        "money_pct", "bet_pct", "sharp_differential",
                        "sharp_direction", "line_sharp_correlation"
                    ]
                    
                    for field in expected_fields:
                        if field in hybrid:
                            # Validate field values where applicable
                            if field in ["money_pct", "bet_pct"] and hybrid[field] is not None:
                                assert 0 <= hybrid[field] <= 100, \
                                    f"{field} should be between 0 and 100"
                            
                            if field == "sharp_differential" and hybrid[field] is not None:
                                assert hybrid[field] >= 0, \
                                    "sharp_differential should be non-negative"
            
        except Exception as e:
            pytest.fail(f"Hybrid processor failed to get hybrid sharp data: {e}")

    @pytest.mark.asyncio
    async def test_detection_service_real_patterns(self, detection_service, db_connection):
        """Test Sharp Action Detection Service can detect real patterns."""
        async with db_connection.get_async_connection() as conn:
            try:
                # Test with a non-existent game ID (should handle gracefully)
                result = await detection_service._analyze_game_for_sharp_action(
                    conn, 99999, date.today()
                )
                
                assert isinstance(result, dict), "Should return dictionary"
                
                # Should have indicators for all market types
                expected_markets = ["moneyline", "spread", "total"]
                for market in expected_markets:
                    assert market in result, f"Should have {market} indicators"
                    
                    market_data = result[market]
                    assert "detected" in market_data, f"{market} should have detected field"
                    assert "confidence" in market_data, f"{market} should have confidence field"
                    assert "patterns" in market_data, f"{market} should have patterns field"
                    
                    # Validate data types
                    assert isinstance(market_data["detected"], bool), \
                        f"{market} detected should be boolean"
                    assert isinstance(market_data["confidence"], (int, float)), \
                        f"{market} confidence should be numeric"
                    assert isinstance(market_data["patterns"], list), \
                        f"{market} patterns should be list"
                    
                    # Validate confidence range
                    assert 0 <= market_data["confidence"] <= 1, \
                        f"{market} confidence should be between 0 and 1"
                
            except Exception as e:
                pytest.fail(f"Detection service failed to analyze patterns: {e}")

    @pytest.mark.asyncio
    async def test_empty_data_scenarios(self, sharp_processor, hybrid_processor):
        """Test that processors handle empty data scenarios gracefully."""
        # Test with empty game data
        empty_game_data = []
        
        try:
            # Test sharp processor with empty data
            splits_data = await sharp_processor._get_betting_splits_data(empty_game_data, 180)
            assert isinstance(splits_data, list), "Should return empty list for empty input"
            assert len(splits_data) == 0, "Should return empty list for empty input"
            
            # Test hybrid processor with empty data
            hybrid_data = await hybrid_processor._get_hybrid_sharp_data(empty_game_data, 180)
            assert isinstance(hybrid_data, list), "Should return empty list for empty input"
            assert len(hybrid_data) == 0, "Should return empty list for empty input"
            
        except Exception as e:
            pytest.fail(f"Processors should handle empty data gracefully: {e}")

    @pytest.mark.asyncio
    async def test_database_error_handling(self, sharp_processor):
        """Test that processors handle database errors gracefully."""
        # Test with very short time window that might cause issues
        try:
            game_data = await sharp_processor._get_real_game_data(0)  # 0 minutes
            assert isinstance(game_data, list), "Should return list even for edge cases"
            
        except Exception as e:
            # Should not raise unhandled exceptions
            pytest.fail(f"Processor should handle edge cases gracefully: {e}")

    @pytest.mark.asyncio
    async def test_no_mock_data_references(self):
        """Test that no mock data references remain in the code."""
        import inspect
        
        # Check sharp action processor
        sharp_source = inspect.getsource(UnifiedSharpActionProcessor._get_betting_splits_data)
        assert "mock" not in sharp_source.lower(), \
            "Sharp action processor should not contain mock data references"
        assert "unified_betting_splits" in sharp_source, \
            "Sharp action processor should query real data tables"
        
        # Check hybrid processor
        hybrid_source = inspect.getsource(UnifiedHybridSharpProcessor._get_hybrid_sharp_data)
        assert "mock" not in hybrid_source.lower(), \
            "Hybrid processor should not contain mock data references"
        assert "unified_betting_splits" in hybrid_source, \
            "Hybrid processor should query real data tables"
        
        # Check detection service
        service_source = inspect.getsource(SharpActionDetectionService._detect_real_sharp_patterns)
        assert "real" in service_source.lower(), \
            "Detection service should use real pattern detection"
        assert "unified_betting_splits" in service_source, \
            "Detection service should query real data tables"

    @pytest.mark.asyncio
    async def test_performance_benchmarks(self, sharp_processor, hybrid_processor):
        """Test performance of real data queries."""
        import time
        
        # Test sharp processor performance
        start_time = time.time()
        game_data = await sharp_processor._get_real_game_data(180)
        query_time = time.time() - start_time
        
        # Should complete within reasonable time (10 seconds)
        assert query_time < 10.0, f"Game data query took too long: {query_time:.2f}s"
        
        if game_data:
            start_time = time.time()
            splits_data = await sharp_processor._get_betting_splits_data(game_data[:5], 180)
            splits_time = time.time() - start_time
            
            # Should complete within reasonable time (15 seconds for 5 games)
            assert splits_time < 15.0, f"Splits data query took too long: {splits_time:.2f}s"

    def test_processor_initialization(self, sharp_processor, hybrid_processor):
        """Test that processors initialize correctly with real data dependencies."""
        # Test sharp processor
        assert sharp_processor is not None, "Sharp processor should initialize"
        assert hasattr(sharp_processor, "_get_real_game_data"), \
            "Sharp processor should have real game data method"
        assert hasattr(sharp_processor, "_get_betting_splits_data"), \
            "Sharp processor should have betting splits method"
        
        # Test hybrid processor
        assert hybrid_processor is not None, "Hybrid processor should initialize"
        assert hasattr(hybrid_processor, "_get_real_game_data"), \
            "Hybrid processor should have real game data method"
        assert hasattr(hybrid_processor, "_get_hybrid_sharp_data"), \
            "Hybrid processor should have hybrid data method"

    @pytest.mark.asyncio
    async def test_data_validation_and_quality(self, sharp_processor):
        """Test data validation and quality checks."""
        # Get real game data
        game_data = await sharp_processor._get_real_game_data(1440)
        
        if game_data:
            # Test with first game
            test_game = [game_data[0]]
            splits_data = await sharp_processor._get_betting_splits_data(test_game, 180)
            
            if splits_data:
                for split in splits_data:
                    # Validate required fields exist
                    required_fields = ["game_id", "split_type", "source", "book"]
                    for field in required_fields:
                        assert field in split, f"Split data missing required field: {field}"
                    
                    # Validate data quality
                    if "differential" in split and split["differential"] is not None:
                        assert split["differential"] >= 0, \
                            "Differential should be non-negative"
                    
                    if "last_updated" in split:
                        assert split["last_updated"] is not None, \
                            "Last updated should not be None"

    @pytest.mark.asyncio
    async def test_multiple_market_types(self, sharp_processor):
        """Test handling of different market types (moneyline, spread, totals)."""
        game_data = await sharp_processor._get_real_game_data(1440)
        
        if game_data:
            splits_data = await sharp_processor._get_betting_splits_data(game_data, 180)
            
            if splits_data:
                # Check for different market types
                market_types = set()
                for split in splits_data:
                    if "split_type" in split:
                        market_types.add(split["split_type"])
                
                # Validate market types are valid
                valid_types = {"moneyline", "spread", "total_over", "total_under"}
                for market_type in market_types:
                    assert market_type in valid_types, \
                        f"Invalid market type: {market_type}"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])