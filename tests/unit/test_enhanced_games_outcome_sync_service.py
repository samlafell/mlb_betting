"""
Unit Tests for Enhanced Games Outcome Sync Service

Tests for the critical ML Training Pipeline sync service that resolves
the zero data issue by syncing game outcomes to enhanced_games table.

Reference: GitHub Issue #67 - ML Training Pipeline Has Zero Real Data
"""

import pytest
from datetime import datetime, timezone, date
from unittest.mock import AsyncMock, Mock, patch
from typing import Dict, Any, List

from src.services.curated_zone.enhanced_games_outcome_sync_service import (
    EnhancedGamesOutcomeSyncService,
    GameOutcomeSyncResult,
    EnhancedGameWithOutcome
)


class TestEnhancedGamesOutcomeSyncService:
    """Test suite for EnhancedGamesOutcomeSyncService."""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing."""
        return EnhancedGamesOutcomeSyncService()
    
    @pytest.fixture
    def mock_outcome_data(self):
        """Sample outcome data for testing."""
        return {
            "game_id": 123,
            "home_team": "NYY",
            "away_team": "BOS",
            "home_score": 7,
            "away_score": 4,
            "home_win": True,
            "over": True,
            "home_cover_spread": True,
            "total_line": 9.5,
            "home_spread_line": -1.5,
            "outcome_game_date": date(2024, 8, 15),
            "mlb_stats_api_game_id": "game_123",
            "action_network_game_id": 456,
            "game_datetime": datetime(2024, 8, 15, 19, 0, tzinfo=timezone.utc),
            "game_date": date(2024, 8, 15),
            "season": 2024,
            "venue_name": "Yankee Stadium",
            "game_status": "final",
            "winning_team": "NYY"
        }
    
    @pytest.fixture
    def mock_missing_outcomes(self, mock_outcome_data):
        """List of missing outcomes for testing."""
        return [
            mock_outcome_data,
            {**mock_outcome_data, "game_id": 124, "home_score": 3, "away_score": 8, "home_win": False, "winning_team": "BOS"},
            {**mock_outcome_data, "game_id": 125, "home_score": 5, "away_score": 2, "home_win": True, "winning_team": "NYY"}
        ]

    def test_service_initialization(self, service):
        """Test service initializes with correct default values."""
        assert service.sync_stats["total_synced"] == 0
        assert service.sync_stats["total_created"] == 0
        assert service.sync_stats["total_updated"] == 0
        assert service.sync_stats["last_sync"] is None

    @pytest.mark.asyncio
    async def test_create_enhanced_game_with_outcome(self, service, mock_outcome_data):
        """Test creating enhanced game object from outcome data."""
        enhanced_game = await service._create_enhanced_game_with_outcome(mock_outcome_data)
        
        assert isinstance(enhanced_game, EnhancedGameWithOutcome)
        assert enhanced_game.home_team == "NYY"
        assert enhanced_game.away_team == "BOS"
        assert enhanced_game.home_score == 7
        assert enhanced_game.away_score == 4
        assert enhanced_game.home_win is True
        assert enhanced_game.winning_team == "NYY"
        assert enhanced_game.data_quality_score == 1.0
        assert enhanced_game.ml_metadata["has_complete_outcome"] is True
        assert enhanced_game.ml_metadata["quality_checks"]["ml_training_ready"] is True

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_validate_sql_injection_protection(self, mock_get_connection, service):
        """Test that SQL injection protection works for user inputs."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Test limit validation - should fail before database call
        with pytest.raises(ValueError, match="Invalid limit value"):
            await service._get_missing_enhanced_game_outcomes(-1)
        
        # Test string injection in limit - should fail validation
        with pytest.raises((ValueError, TypeError)):
            await service._get_missing_enhanced_game_outcomes("'; DROP TABLE test; --")
        
        # Test days_back validation
        with pytest.raises(ValueError, match="Invalid days_back value"):
            await service._get_recent_outcomes_for_sync(-5)
        
        # Test string injection in days_back - should fail validation  
        with pytest.raises((ValueError, TypeError)):
            await service._get_recent_outcomes_for_sync("'; DROP TABLE test; --")
        
        # Test pagination validation
        with pytest.raises(ValueError, match="Invalid page_size"):
            await service._get_missing_enhanced_game_outcomes_paginated(-1, 0)
        
        with pytest.raises(ValueError, match="Invalid offset"):
            await service._get_missing_enhanced_game_outcomes_paginated(10, -1)

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_get_missing_outcomes_with_limit(self, mock_get_connection, service):
        """Test getting missing outcomes with limit parameter."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Mock database response
        mock_rows = [
            {"game_id": 1, "home_team": "NYY", "away_team": "BOS"},
            {"game_id": 2, "home_team": "LAA", "away_team": "HOU"}
        ]
        mock_conn.fetch.return_value = mock_rows
        
        result = await service._get_missing_enhanced_game_outcomes(limit=2)
        
        # Verify parameterized query was called
        mock_conn.fetch.assert_called_once()
        call_args = mock_conn.fetch.call_args
        assert "$1" in call_args[0][0]  # Query should contain parameter placeholder
        assert call_args[0][1] == 2  # Limit should be passed as parameter
        
        assert len(result) == 2
        assert result[0]["game_id"] == 1

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_pagination_functionality(self, mock_get_connection, service):
        """Test pagination works correctly for large datasets."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Mock paginated response
        mock_rows = [{"game_id": i} for i in range(10)]
        mock_conn.fetch.return_value = mock_rows
        
        result = await service._get_missing_enhanced_game_outcomes_paginated(10, 20)
        
        # Verify parameterized pagination query
        mock_conn.fetch.assert_called_once()
        call_args = mock_conn.fetch.call_args
        assert "LIMIT $1 OFFSET $2" in call_args[0][0]
        assert call_args[0][1] == 10  # page_size
        assert call_args[0][2] == 20  # offset

    @pytest.mark.asyncio
    async def test_batch_processing_performance(self, service, mock_missing_outcomes):
        """Test batch processing handles multiple outcomes efficiently."""
        # Mock the _batch_upsert_enhanced_games method directly
        with patch.object(service, '_batch_upsert_enhanced_games') as mock_batch_upsert:
            mock_batch_upsert.return_value = (2, 1)  # 2 created, 1 updated
            
            result = GameOutcomeSyncResult()
            created, updated = await service._batch_sync_outcomes(
                mock_missing_outcomes, dry_run=False, result=result
            )
            
            assert created == 2  # Two new records created
            assert updated == 1   # One record updated
            assert mock_batch_upsert.call_count == 1  # Called once for the batch

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_dry_run_mode(self, mock_get_connection, service, mock_missing_outcomes):
        """Test dry run mode doesn't modify database."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        with patch.object(service, '_upsert_enhanced_game_with_outcome') as mock_upsert:
            result = GameOutcomeSyncResult()
            created, updated = await service._batch_sync_outcomes(
                mock_missing_outcomes, dry_run=True, result=result
            )
            
            # In dry run, should simulate counts but not call upsert
            assert created == 3  # Simulated count
            assert updated == 0  # No updates in dry run simulation
            mock_upsert.assert_not_called()

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_error_handling_in_batch(self, mock_get_connection, service):
        """Test error handling during batch processing."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Create invalid outcome data to trigger error
        invalid_outcome = {"game_id": None, "invalid": "data"}
        
        result = GameOutcomeSyncResult()
        created, updated = await service._batch_sync_outcomes(
            [invalid_outcome], dry_run=False, result=result
        )
        
        assert created == 0
        assert updated == 0
        assert result.sync_failures == 1
        assert len(result.errors) == 1
        assert "Failed to create enhanced game" in result.errors[0]

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_sync_all_missing_outcomes_integration(self, mock_get_connection, service):
        """Test complete sync workflow integration."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Mock paginated responses
        mock_conn.fetch.side_effect = [
            [{"game_id": 1, "home_team": "NYY", "away_team": "BOS", "home_score": 7, "away_score": 4, 
              "home_win": True, "over": True, "winning_team": "NYY", "game_datetime": datetime.now(timezone.utc),
              "outcome_game_date": date.today()}],
            []  # Empty page indicates end
        ]
        
        with patch.object(service, '_batch_sync_outcomes') as mock_batch_sync:
            mock_batch_sync.return_value = (1, 0)  # 1 created, 0 updated
            
            result = await service.sync_all_missing_outcomes(
                dry_run=False, limit=None, page_size=1000
            )
            
            assert result.outcomes_found == 1
            assert result.enhanced_games_created == 1
            assert result.enhanced_games_updated == 0
            assert result.sync_failures == 0
            assert result.processing_time_seconds > 0

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_health_check_functionality(self, mock_get_connection, service):
        """Test health check returns correct status."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Mock database responses for health check
        mock_conn.fetchval.side_effect = [
            1,   # Database connectivity test
            50,  # Enhanced games count
            0    # Missing games count
        ]
        
        health = await service.health_check()
        
        assert health["status"] == "healthy"
        assert health["database_connection"] == "ok"
        assert health["enhanced_games_with_scores"] == 50
        assert health["missing_enhanced_games"] == 0
        assert health["ml_training_ready"] is True
        assert health["sync_needed"] is False

    @pytest.mark.asyncio
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_health_check_unhealthy_status(self, mock_get_connection, service):
        """Test health check detects unhealthy conditions."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Mock responses indicating unhealthy state
        mock_conn.fetchval.side_effect = [
            1,   # Database connectivity test
            10,  # Low enhanced games count (< 50)
            25   # High missing games count
        ]
        
        health = await service.health_check()
        
        assert health["status"] == "needs_sync"
        assert health["ml_training_ready"] is False
        assert health["sync_needed"] is True

    @pytest.mark.asyncio
    async def test_sync_stats_tracking(self, service):
        """Test sync statistics are properly tracked."""
        initial_stats = await service.get_sync_stats()
        
        # Verify initial state
        assert initial_stats["total_synced"] == 0
        assert initial_stats["total_created"] == 0
        assert initial_stats["total_updated"] == 0
        assert initial_stats["last_sync"] is None

    @pytest.mark.asyncio 
    @patch('src.services.curated_zone.enhanced_games_outcome_sync_service.get_connection')
    async def test_recent_outcomes_sync(self, mock_get_connection, service):
        """Test syncing recent outcomes works correctly."""
        mock_conn = AsyncMock()
        mock_get_connection.return_value.__aenter__.return_value = mock_conn
        
        # Mock recent outcomes query
        mock_conn.fetch.return_value = [
            {"game_id": 1, "home_team": "NYY", "away_team": "BOS", "home_score": 5, "away_score": 3,
             "home_win": True, "over": False, "winning_team": "NYY", "game_datetime": datetime.now(timezone.utc),
             "outcome_game_date": date.today()}
        ]
        
        with patch.object(service, '_batch_sync_outcomes') as mock_batch_sync:
            mock_batch_sync.return_value = (0, 1)  # 0 created, 1 updated
            
            result = await service.sync_recent_outcomes(days_back=7, dry_run=False)
            
            assert result.outcomes_found == 1
            assert result.enhanced_games_created == 0
            assert result.enhanced_games_updated == 1
            assert result.metadata["sync_type"] == "recent_outcomes"
            assert result.metadata["days_back"] == 7


class TestGameOutcomeSyncResult:
    """Test suite for GameOutcomeSyncResult model."""
    
    def test_result_model_initialization(self):
        """Test result model initializes with correct defaults."""
        result = GameOutcomeSyncResult()
        
        assert result.outcomes_found == 0
        assert result.enhanced_games_updated == 0
        assert result.enhanced_games_created == 0
        assert result.sync_failures == 0
        assert result.processing_time_seconds == 0.0
        assert result.errors == []
        assert result.metadata == {}

    def test_result_model_with_data(self):
        """Test result model handles data correctly."""
        result = GameOutcomeSyncResult(
            outcomes_found=10,
            enhanced_games_created=8,
            enhanced_games_updated=2,
            processing_time_seconds=1.5,
            errors=["Error 1", "Error 2"],
            metadata={"dry_run": False}
        )
        
        assert result.outcomes_found == 10
        assert result.enhanced_games_created == 8
        assert result.enhanced_games_updated == 2
        assert result.processing_time_seconds == 1.5
        assert len(result.errors) == 2
        assert result.metadata["dry_run"] is False


class TestEnhancedGameWithOutcome:
    """Test suite for EnhancedGameWithOutcome model."""
    
    def test_enhanced_game_model_creation(self):
        """Test enhanced game model creation with outcome data."""
        game = EnhancedGameWithOutcome(
            home_team="NYY",
            away_team="BOS", 
            home_score=7,
            away_score=4,
            winning_team="NYY",
            home_win=True,
            over=True,
            game_datetime=datetime(2024, 8, 15, 19, 0, tzinfo=timezone.utc)
        )
        
        assert game.home_team == "NYY"
        assert game.away_team == "BOS"
        assert game.home_score == 7
        assert game.away_score == 4
        assert game.winning_team == "NYY"
        assert game.home_win is True
        assert game.over is True
        assert game.data_quality_score == 1.0

    def test_enhanced_game_model_validation(self):
        """Test enhanced game model validation."""
        # Test that required fields are enforced
        with pytest.raises(Exception):  # Pydantic validation error
            EnhancedGameWithOutcome()  # Missing required fields