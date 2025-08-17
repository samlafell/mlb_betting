"""
Integration Tests for Enhanced Games Outcome Sync Service

Tests the complete sync workflow with real database operations
to verify the ML Training Pipeline fix works end-to-end.

Reference: GitHub Issue #67 - ML Training Pipeline Has Zero Real Data
"""

import pytest
import asyncio
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any

from src.core.config import get_settings
from src.data.database.connection import get_connection, initialize_connections
from src.services.curated_zone.enhanced_games_outcome_sync_service import (
    EnhancedGamesOutcomeSyncService,
    sync_all_missing_outcomes,
    sync_recent_outcomes
)


@pytest.mark.integration
class TestEnhancedGamesOutcomeSyncIntegration:
    """Integration tests for the sync service with real database."""
    
    @pytest.fixture(scope="class", autouse=True)
    def setup_database(self):
        """Setup database connections for integration tests."""
        settings = get_settings()
        initialize_connections(settings)
        yield
        # Cleanup is handled by connection manager
    
    @pytest.fixture
    def service(self):
        """Create service instance for integration testing."""
        return EnhancedGamesOutcomeSyncService()
    
    @pytest.fixture
    def clean_test_data(self):
        """Clean any test data before and after tests."""
        # This fixture would clean test data if needed
        # For now, we'll use read-only operations
        yield
    
    @pytest.mark.asyncio
    async def test_database_connectivity(self, service):
        """Test that service can connect to database."""
        health = await service.health_check()
        
        assert health["database_connection"] == "ok"
        assert "enhanced_games_with_scores" in health
        assert "missing_enhanced_games" in health
    
    @pytest.mark.asyncio
    async def test_get_sync_stats_real_data(self, service):
        """Test getting sync stats with real database data."""
        stats = await service.get_sync_stats()
        
        # Verify stats structure
        assert "enhanced_games_with_scores" in stats
        assert "total_game_outcomes" in stats
        assert "missing_enhanced_games" in stats
        assert "sync_completion_rate" in stats
        assert "ml_training_ready" in stats
        
        # Verify data types
        assert isinstance(stats["enhanced_games_with_scores"], int)
        assert isinstance(stats["total_game_outcomes"], int)
        assert isinstance(stats["missing_enhanced_games"], int)
        assert isinstance(stats["ml_training_ready"], bool)
    
    @pytest.mark.asyncio
    async def test_query_performance_with_large_dataset(self, service):
        """Test query performance with realistic dataset size."""
        start_time = datetime.now()
        
        # Test paginated query performance
        missing_outcomes = await service._get_missing_enhanced_game_outcomes_paginated(
            page_size=100, offset=0
        )
        
        query_time = (datetime.now() - start_time).total_seconds()
        
        # Query should complete within reasonable time
        assert query_time < 5.0  # Should complete within 5 seconds
        assert isinstance(missing_outcomes, list)
    
    @pytest.mark.asyncio
    async def test_dry_run_sync_integration(self, service):
        """Test dry run sync with real database queries."""
        result = await service.sync_all_missing_outcomes(
            dry_run=True, 
            limit=10,  # Small limit for testing
            page_size=5
        )
        
        # Verify result structure
        assert hasattr(result, "outcomes_found")
        assert hasattr(result, "enhanced_games_created") 
        assert hasattr(result, "enhanced_games_updated")
        assert hasattr(result, "sync_failures")
        assert hasattr(result, "processing_time_seconds")
        assert hasattr(result, "errors")
        assert hasattr(result, "metadata")
        
        # Verify metadata
        assert result.metadata["dry_run"] is True
        assert result.metadata["sync_type"] == "all_missing_outcomes"
        assert result.processing_time_seconds > 0
    
    @pytest.mark.asyncio
    async def test_recent_sync_integration(self, service):
        """Test recent outcomes sync with real database."""
        result = await service.sync_recent_outcomes(
            days_back=1,  # Small window for testing
            dry_run=True  # Use dry run to avoid data changes
        )
        
        assert hasattr(result, "outcomes_found")
        assert result.metadata["sync_type"] == "recent_outcomes"
        assert result.metadata["days_back"] == 1
        assert result.metadata["dry_run"] is True
    
    @pytest.mark.asyncio
    async def test_pagination_across_pages(self, service):
        """Test pagination works correctly across multiple pages."""
        page_size = 5
        pages_to_test = 3
        all_results = []
        
        for page in range(pages_to_test):
            offset = page * page_size
            page_results = await service._get_missing_enhanced_game_outcomes_paginated(
                page_size=page_size, 
                offset=offset
            )
            
            # Verify page structure
            assert isinstance(page_results, list)
            assert len(page_results) <= page_size
            
            # Check for unique results across pages
            page_ids = [result.get("game_id") for result in page_results if result.get("game_id")]
            existing_ids = [result.get("game_id") for result in all_results if result.get("game_id")]
            
            # Should not have duplicates across pages
            assert not set(page_ids).intersection(set(existing_ids))
            
            all_results.extend(page_results)
            
            # If we got fewer results than page_size, we've reached the end
            if len(page_results) < page_size:
                break
    
    @pytest.mark.asyncio
    async def test_validation_with_real_data_structure(self, service):
        """Test that service handles real database schema correctly."""
        try:
            # Test that queries work with actual schema
            missing_outcomes = await service._get_missing_enhanced_game_outcomes(limit=1)
            
            if missing_outcomes:
                sample_outcome = missing_outcomes[0]
                
                # Verify expected fields exist
                expected_fields = [
                    "game_id", "home_team", "away_team", "home_score", "away_score",
                    "home_win", "over", "winning_team", "game_datetime"
                ]
                
                for field in expected_fields:
                    assert field in sample_outcome, f"Missing expected field: {field}"
                
                # Test creating enhanced game from real data
                enhanced_game = await service._create_enhanced_game_with_outcome(sample_outcome)
                assert enhanced_game.home_team == sample_outcome["home_team"]
                assert enhanced_game.away_team == sample_outcome["away_team"]
                
        except Exception as e:
            pytest.fail(f"Service failed to handle real database schema: {e}")
    
    @pytest.mark.asyncio
    async def test_error_handling_with_invalid_queries(self, service):
        """Test error handling with invalid query parameters."""
        # Test invalid limit
        with pytest.raises(ValueError):
            await service._get_missing_enhanced_game_outcomes(-1)
        
        # Test invalid pagination
        with pytest.raises(ValueError):
            await service._get_missing_enhanced_game_outcomes_paginated(-1, 0)
        
        with pytest.raises(ValueError):
            await service._get_missing_enhanced_game_outcomes_paginated(10, -1)
        
        # Test invalid days_back
        with pytest.raises(ValueError):
            await service._get_recent_outcomes_for_sync(-1)


@pytest.mark.integration
class TestConvenienceFunctions:
    """Test the convenience functions for sync operations."""
    
    @pytest.mark.asyncio
    async def test_sync_all_missing_outcomes_function(self):
        """Test the convenience function for syncing all missing outcomes."""
        result = await sync_all_missing_outcomes(dry_run=True, limit=5)
        
        assert hasattr(result, "outcomes_found")
        assert hasattr(result, "processing_time_seconds")
        assert result.processing_time_seconds > 0
    
    @pytest.mark.asyncio
    async def test_sync_recent_outcomes_function(self):
        """Test the convenience function for syncing recent outcomes."""
        result = await sync_recent_outcomes(days_back=1, dry_run=True)
        
        assert hasattr(result, "outcomes_found")
        assert hasattr(result, "metadata")
        assert result.metadata["days_back"] == 1


@pytest.mark.integration 
class TestMLTrainingPipelineValidation:
    """Test ML Training Pipeline integration after sync."""
    
    @pytest.fixture
    def service(self):
        """Create service instance.""" 
        return EnhancedGamesOutcomeSyncService()
    
    @pytest.mark.asyncio
    async def test_ml_pipeline_data_availability(self, service):
        """Test that synced data is available for ML training pipeline."""
        # Check current state
        stats = await service.get_sync_stats()
        
        enhanced_games_count = stats.get("enhanced_games_with_scores", 0)
        ml_training_ready = stats.get("ml_training_ready", False)
        
        # Log current state for debugging
        print(f"Enhanced games with scores: {enhanced_games_count}")
        print(f"ML training ready: {ml_training_ready}")
        
        # If we have data, verify it's accessible
        if enhanced_games_count > 0:
            async with get_connection() as conn:
                # Test query that ML trainer would use
                sample_games = await conn.fetch("""
                    SELECT id, home_team, away_team, home_score, away_score, game_datetime
                    FROM curated.enhanced_games
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                    LIMIT 5
                """)
                
                assert len(sample_games) > 0
                for game in sample_games:
                    assert game["home_score"] is not None
                    assert game["away_score"] is not None
                    assert game["home_team"] is not None
                    assert game["away_team"] is not None
    
    @pytest.mark.asyncio
    async def test_data_quality_for_ml_training(self, service):
        """Test that synced data meets ML training quality requirements."""
        async with get_connection() as conn:
            # Check data quality metrics
            quality_check = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 1 END) as games_with_scores,
                    COUNT(DISTINCT home_team) as unique_home_teams,
                    COUNT(DISTINCT away_team) as unique_away_teams,
                    MIN(game_datetime) as earliest_game,
                    MAX(game_datetime) as latest_game
                FROM curated.enhanced_games
            """)
            
            if quality_check["total_games"] > 0:
                # Verify data quality requirements
                assert quality_check["games_with_scores"] > 0
                assert quality_check["unique_home_teams"] > 0
                assert quality_check["unique_away_teams"] > 0
                assert quality_check["earliest_game"] is not None
                assert quality_check["latest_game"] is not None
                
                # Calculate quality score
                quality_score = quality_check["games_with_scores"] / quality_check["total_games"]
                print(f"Data quality score: {quality_score:.2%}")
                
                # For ML training, we want high quality data
                if quality_check["total_games"] >= 10:  # Only check if we have meaningful data
                    assert quality_score >= 0.8  # At least 80% of games should have scores


if __name__ == "__main__":
    # Allow running integration tests directly
    pytest.main([__file__, "-v", "-m", "integration"])