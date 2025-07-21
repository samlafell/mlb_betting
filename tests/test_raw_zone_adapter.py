#!/usr/bin/env python3
"""
Test RAW Zone Adapter

Comprehensive tests for the RAW zone adapter functionality including:
- Action Network game and odds storage
- SBD betting splits storage
- VSIN data storage
- MLB Stats API data storage
- Generic betting lines storage
- Line movement storage
- Health check and status monitoring

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import sys
sys.path.insert(0, '/Users/samlafell/Documents/programming_projects/mlb_betting_program')

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any, List

def test_raw_zone_adapter_initialization():
    """Test RAW zone adapter initialization."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    
    print("\n⚙️ Testing RAW zone adapter initialization...")
    
    # Create adapter
    adapter = RawZoneAdapter()
    
    assert adapter is not None
    assert adapter.raw_processor is not None
    assert adapter.settings is not None
    print("  ✅ RAW zone adapter initialization successful")

async def test_store_action_network_games():
    """Test storing Action Network game data."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n🎮 Testing Action Network games storage...")
    
    # Mock the raw processor
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=2,
            records_successful=2,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test data
        games_data = [
            {
                "id": "game_123",
                "home_team": "Yankees",
                "away_team": "Red Sox",
                "game_date": "2025-07-21",
                "status": "upcoming"
            },
            {
                "id": "game_456", 
                "home_team": "Dodgers",
                "away_team": "Giants",
                "game_date": "2025-07-21",
                "status": "upcoming"
            }
        ]
        
        source_info = {
            "endpoint_url": "https://api.actionnetwork.com/web/v1/games",
            "response_status": 200
        }
        
        # Store games
        result = await adapter.store_action_network_games(games_data, source_info)
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 2
        assert result.records_successful == 2
        
        # Verify process_batch was called with correct data
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 2
        assert call_args[0].external_id == "game_123"
        assert call_args[0].source == "action_network"
        assert call_args[0].data_type == "game"
        assert call_args[1].external_id == "game_456"
        
        print("  ✅ Action Network games storage successful")

async def test_store_action_network_odds():
    """Test storing Action Network odds data."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n💰 Testing Action Network odds storage...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=3,
            records_successful=3,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test odds data
        odds_data = [
            {
                "sportsbook_key": "draftkings",
                "markets": {
                    "moneyline": {"home": -150, "away": +130}
                }
            },
            {
                "key": "fanduel", 
                "markets": {
                    "moneyline": {"home": -145, "away": +125}
                }
            },
            {
                "sportsbook_key": "betmgm",
                "markets": {
                    "spread": {"home": -1.5, "away": +1.5}
                }
            }
        ]
        
        # Store odds
        result = await adapter.store_action_network_odds(
            odds_data, 
            game_id="game_123",
            source_info={"endpoint": "odds_api"}
        )
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 3
        assert result.records_successful == 3
        
        # Verify records created correctly
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 3
        assert call_args[0].source == "action_network"
        assert call_args[0].data_type == "odds"
        assert call_args[0].game_external_id == "game_123"
        assert call_args[0].sportsbook_name == "draftkings"
        assert call_args[1].sportsbook_name == "fanduel"  # Uses "key" field
        
        print("  ✅ Action Network odds storage successful")

async def test_store_sbd_betting_splits():
    """Test storing SBD betting splits data."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n📊 Testing SBD betting splits storage...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=1,
            records_successful=1,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test SBD splits data
        splits_data = [
            {
                "matchup_id": "sbd_match_789",
                "game_id": "game_123",
                "public_bets": {"moneyline": {"home": 65, "away": 35}},
                "public_money": {"moneyline": {"home": 58, "away": 42}},
                "timestamp": "2025-07-21T14:00:00Z"
            }
        ]
        
        source_info = {
            "api_endpoint": "https://api.sbd.com/betting-splits"
        }
        
        # Store splits
        result = await adapter.store_sbd_betting_splits(splits_data, source_info)
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 1
        assert result.records_successful == 1
        
        # Verify record structure
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 1
        assert call_args[0].external_id == "sbd_match_789"
        assert call_args[0].source == "sbd"
        assert call_args[0].data_type == "betting_splits"
        assert call_args[0].game_external_id == "game_123"
        
        print("  ✅ SBD betting splits storage successful")

async def test_store_vsin_data():
    """Test storing VSIN data."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n📈 Testing VSIN data storage...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=2,
            records_successful=2,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test VSIN data
        vsin_data = [
            {
                "id": "vsin_analysis_1",
                "game_id": "game_123",
                "analysis_type": "sharp_action",
                "content": "Heavy sharp action on Yankees -150"
            },
            {
                "id": "vsin_steam_2",
                "game_id": "game_456", 
                "analysis_type": "steam_move",
                "content": "Line moved from -3 to -4.5"
            }
        ]
        
        source_info = {
            "source_feed": "vsin_premium"
        }
        
        # Store VSIN data
        result = await adapter.store_vsin_data(
            vsin_data, 
            data_type="analysis",
            source_info=source_info
        )
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 2
        assert result.records_successful == 2
        
        # Verify record structure
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 2
        assert call_args[0].external_id == "vsin_analysis_1"
        assert call_args[0].source == "vsin"
        assert call_args[0].data_type == "analysis"
        assert call_args[0].game_external_id == "game_123"
        assert call_args[1].external_id == "vsin_steam_2"
        
        print("  ✅ VSIN data storage successful")

async def test_store_mlb_stats_data():
    """Test storing MLB Stats API data."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n⚾ Testing MLB Stats API data storage...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=1,
            records_successful=1,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test MLB Stats API data
        mlb_data = [
            {
                "gamePk": 789456,
                "gameDate": "2025-07-21",
                "teams": {
                    "home": {"name": "New York Yankees"},
                    "away": {"name": "Boston Red Sox"}
                },
                "status": {"detailedState": "Scheduled"},
                "venue": {"name": "Yankee Stadium"}
            }
        ]
        
        source_info = {
            "api_endpoint": "https://statsapi.mlb.com/api/v1/games"
        }
        
        # Store MLB data
        result = await adapter.store_mlb_stats_data(mlb_data, source_info)
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 1
        assert result.records_successful == 1
        
        # Verify record structure
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 1
        assert call_args[0].external_id == "789456"
        assert call_args[0].source == "mlb_stats_api"
        assert call_args[0].game_external_id == "789456"
        
        print("  ✅ MLB Stats API data storage successful")

async def test_store_betting_lines():
    """Test storing generic betting lines."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n💸 Testing generic betting lines storage...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=2,
            records_successful=2,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test betting lines data
        lines_data = [
            {
                "id": "line_123",
                "game_id": "game_123",
                "sportsbook": "DraftKings",
                "odds": -110,
                "game_date": "2025-07-21"
            },
            {
                "id": "line_456",
                "game_id": "game_456", 
                "sportsbook": "FanDuel",
                "odds": +105,
                "game_date": "2025-07-21"
            }
        ]
        
        # Store betting lines
        result = await adapter.store_betting_lines(
            lines_data,
            bet_type="moneyline",
            source="oddsapi"
        )
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 2
        assert result.records_successful == 2
        
        # Verify record structure
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 2
        assert call_args[0].external_id == "line_123"
        assert call_args[0].source == "oddsapi"
        assert call_args[0].bet_type == "moneyline"
        assert call_args[0].sportsbook_name == "DraftKings"
        assert call_args[1].sportsbook_name == "FanDuel"
        
        print("  ✅ Generic betting lines storage successful")

async def test_store_line_movements():
    """Test storing line movement data."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    from src.data.pipeline.zone_interface import ProcessingResult, ProcessingStatus
    
    print("\n📉 Testing line movement storage...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.return_value = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=1,
            records_successful=1,
            records_failed=0
        )
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test line movement data
        movements_data = [
            {
                "id": "movement_789",
                "game_id": "game_123",
                "old_line": -3.0,
                "new_line": -4.5,
                "movement_time": "2025-07-21T13:30:00Z",
                "sportsbook": "DraftKings"
            }
        ]
        
        # Store line movements
        result = await adapter.store_line_movements(movements_data, source="movements_api")
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 1
        assert result.records_successful == 1
        
        # Verify record structure
        mock_processor.process_batch.assert_called_once()
        call_args = mock_processor.process_batch.call_args[0][0]
        
        assert len(call_args) == 1
        assert call_args[0].external_id == "movement_789"
        assert call_args[0].source == "movements_api"
        assert call_args[0].bet_type == "line_movement"
        assert call_args[0].game_external_id == "game_123"
        
        print("  ✅ Line movement storage successful")

async def test_health_check():
    """Test RAW zone adapter health check."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    
    print("\n🏥 Testing health check...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.health_check.return_value = {
            "zone_type": "raw",
            "status": "healthy",
            "connection_status": "connected",
            "metrics": {
                "records_processed": 1000,
                "records_successful": 980,
                "records_failed": 20
            }
        }
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Get health status
        status = await adapter.get_raw_zone_status()
        
        assert status["zone_type"] == "raw"
        assert status["status"] == "healthy"
        assert status["connection_status"] == "connected"
        assert status["metrics"]["records_processed"] == 1000
        
        print("  ✅ Health check successful")

async def test_error_handling():
    """Test adapter error handling."""
    from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
    
    print("\n❌ Testing error handling...")
    
    with patch('src.data.pipeline.raw_zone_adapter.RawZoneProcessor') as mock_processor_class:
        mock_processor = AsyncMock()
        mock_processor.process_batch.side_effect = Exception("Database connection failed")
        mock_processor_class.return_value = mock_processor
        
        adapter = RawZoneAdapter()
        
        # Test error propagation
        games_data = [{"id": "game_123", "home_team": "Yankees"}]
        
        try:
            await adapter.store_action_network_games(games_data)
            assert False, "Expected exception to be raised"
        except Exception as e:
            assert "Database connection failed" in str(e)
            print("  ✅ Error handling works correctly")

def run_raw_zone_adapter_tests():
    """Run all RAW zone adapter tests."""
    print("🚀 Starting RAW Zone Adapter Tests")
    print("=" * 60)
    
    try:
        # Run sync tests
        test_raw_zone_adapter_initialization()
        
        # Run async tests
        async def run_async_tests():
            await test_store_action_network_games()
            await test_store_action_network_odds()
            await test_store_sbd_betting_splits()
            await test_store_vsin_data()
            await test_store_mlb_stats_data()
            await test_store_betting_lines()
            await test_store_line_movements()
            await test_health_check()
            await test_error_handling()
        
        asyncio.run(run_async_tests())
        
        print("\n" + "=" * 60)
        print("🎉 ALL RAW ZONE ADAPTER TESTS PASSED!")
        print("✅ Action Network game and odds storage working")
        print("✅ SBD betting splits storage operational")
        print("✅ VSIN and MLB Stats API integration functional")
        print("✅ Generic betting lines and movements handling working")
        print("✅ Health monitoring and error handling operational")
        return True
        
    except Exception as e:
        print(f"\n❌ RAW ZONE ADAPTER TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_raw_zone_adapter_tests()
    sys.exit(0 if success else 1)