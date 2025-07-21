#!/usr/bin/env python3
"""
Test RAW Zone Processor

Comprehensive tests for the RAW zone processor functionality including:
- Record processing and metadata extraction
- Table name determination and routing
- Database storage operations
- Validation logic
- Promotion to STAGING zone

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import pytest
import json
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any, List

# Import pipeline components directly
import sys
sys.path.insert(0, '/Users/samlafell/Documents/programming_projects/mlb_betting_program')

from src.data.pipeline.zone_interface import (
    ZoneType,
    ProcessingStatus,
    DataRecord,
    ProcessingResult,
    create_zone_config
)


def test_raw_data_record_structure():
    """Test RawDataRecord structure and field validation."""
    from src.data.pipeline.raw_zone import RawDataRecord
    
    print("\n🧪 Testing RawDataRecord structure...")
    
    # Test basic record creation
    record = RawDataRecord(
        external_id="test_123",
        source="action_network",
        raw_data={"game_id": "123", "test": "data"}
    )
    
    assert record.external_id == "test_123"
    assert record.source == "action_network"
    assert record.raw_data["game_id"] == "123"
    assert record.validation_status == ProcessingStatus.PENDING
    print("  ✅ Basic RawDataRecord creation works")
    
    # Test additional RAW-specific fields
    record_with_metadata = RawDataRecord(
        external_id="test_456",
        source="action_network",
        raw_data={"test": "data"},
        game_external_id="game_123",
        sportsbook_name="DraftKings",
        sportsbook_id=1,
        bet_type="moneyline",
        game_date="2025-07-21",
        endpoint_url="https://api.example.com/games",
        response_status=200,
        data_type="game"
    )
    
    assert record_with_metadata.game_external_id == "game_123"
    assert record_with_metadata.sportsbook_name == "DraftKings"
    assert record_with_metadata.bet_type == "moneyline"
    print("  ✅ RawDataRecord with metadata fields works")


def test_raw_zone_processor_initialization():
    """Test RAW zone processor initialization and configuration."""
    from src.data.pipeline.raw_zone import RawZoneProcessor
    
    print("\n⚙️ Testing RAW zone processor initialization...")
    
    # Create RAW zone configuration
    config = create_zone_config(
        ZoneType.RAW,
        "raw_data",
        batch_size=100,
        validation_enabled=True,
        quality_threshold=0.6
    )
    
    # Initialize processor
    processor = RawZoneProcessor(config)
    
    assert processor.zone_type == ZoneType.RAW
    assert processor.schema_name == "raw_data"
    assert processor.config.batch_size == 100
    assert processor.config.validation_enabled is True
    assert processor.config.quality_threshold == 0.6
    print("  ✅ RAW zone processor initialization successful")
    
    # Test table mappings
    expected_tables = [
        'raw_data.action_network_games',
        'raw_data.betting_lines_raw',
        'raw_data.moneylines_raw',
        'raw_data.spreads_raw',
        'raw_data.totals_raw'
    ]
    
    for table in expected_tables:
        assert any(table in mapping for mapping in processor.table_mappings.values())
    print("  ✅ Table mappings configured correctly")


@pytest.mark.asyncio
async def test_metadata_extraction():
    """Test metadata extraction from raw data."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    
    print("\n🔍 Testing metadata extraction...")
    
    # Create processor
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = RawZoneProcessor(config)
    
    # Test Action Network game data structure
    action_network_data = {
        "game_id": "12345",
        "home_team": "Yankees", 
        "away_team": "Red Sox",
        "game_date": "2025-07-21",
        "sportsbook": {
            "name": "DraftKings",
            "id": 1
        },
        "bet_type": "moneyline"
    }
    
    record = RawDataRecord(
        external_id="test_extraction",
        source="action_network",
        raw_data=action_network_data
    )
    
    # Extract metadata
    extracted_record = await processor._extract_metadata(record)
    
    assert extracted_record.game_external_id == "12345"
    assert extracted_record.sportsbook_name == "DraftKings"
    assert extracted_record.sportsbook_id == 1
    assert extracted_record.bet_type == "moneyline"
    assert extracted_record.game_date == "2025-07-21"
    print("  ✅ Action Network metadata extraction works")
    
    # Test alternative field formats
    alternative_data = {
        "gameId": "67890",  # Alternative naming
        "betType": "spread",  # Alternative naming
        "gameDate": "2025-07-22",  # Alternative naming
        "sportsbook": "FanDuel"  # String instead of object
    }
    
    record2 = RawDataRecord(
        external_id="test_alternative",
        source="action_network",
        raw_data=alternative_data
    )
    
    extracted_record2 = await processor._extract_metadata(record2)
    
    assert extracted_record2.game_external_id == "67890"
    assert extracted_record2.bet_type == "spread"
    assert extracted_record2.game_date == "2025-07-22"
    assert extracted_record2.sportsbook_name == "FanDuel"
    print("  ✅ Alternative field format extraction works")


@pytest.mark.asyncio 
async def test_record_processing():
    """Test individual record processing functionality."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    
    print("\n⚙️ Testing record processing...")
    
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = RawZoneProcessor(config)
    
    # Test valid JSON processing
    test_data = {
        "game_id": "test_game_123",
        "home_team": "Yankees",
        "away_team": "Red Sox",
        "odds": -110
    }
    
    record = RawDataRecord(
        external_id="process_test_1",
        source="action_network",
        raw_data=test_data
    )
    
    processed = await processor.process_record(record)
    
    assert processed is not None
    assert processed.external_id == "process_test_1"
    assert processed.validation_status == ProcessingStatus.COMPLETED
    assert processed.processed_at is not None
    assert processed.game_external_id == "test_game_123"
    print("  ✅ Valid record processing works")
    
    # Test dictionary data processing
    dict_data = {"game_id": "string_test", "data": "test"}
    record_dict = RawDataRecord(
        external_id="process_test_2",
        source="test",
        raw_data=dict_data
    )
    
    processed_dict = await processor.process_record(record_dict)
    
    assert processed_dict is not None
    assert isinstance(processed_dict.raw_data, dict)
    assert processed_dict.raw_data["game_id"] == "string_test"
    print("  ✅ Dictionary data processing works")
    
    # Test processing with missing external_id (should still work with raw_data)
    record_no_id = RawDataRecord(
        source="test",
        raw_data={"game_id": "no_id_test", "data": "test"}
    )
    
    processed_no_id = await processor.process_record(record_no_id)
    
    assert processed_no_id is not None
    assert processed_no_id.raw_data["game_id"] == "no_id_test"
    print("  ✅ Record processing without external_id works")


@pytest.mark.asyncio
async def test_table_name_determination():
    """Test table name determination logic based on source and data type."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    
    print("\n🗂️ Testing table name determination...")
    
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = RawZoneProcessor(config)
    
    # Test Action Network games
    an_game_record = RawDataRecord(
        external_id="an_game",
        source="action_network",
        data_type="game"
    )
    table_name = await processor._determine_table_name(an_game_record)
    assert table_name == "raw_data.action_network_games"
    print("  ✅ Action Network games routing works")
    
    # Test Action Network odds
    an_odds_record = RawDataRecord(
        external_id="an_odds",
        source="action_network",
        data_type="odds"
    )
    table_name = await processor._determine_table_name(an_odds_record)
    assert table_name == "raw_data.action_network_odds"
    print("  ✅ Action Network odds routing works")
    
    # Test SBD data
    sbd_record = RawDataRecord(
        external_id="sbd_test",
        source="sbd"
    )
    table_name = await processor._determine_table_name(sbd_record)
    assert table_name == "raw_data.sbd_betting_splits"
    print("  ✅ SBD data routing works")
    
    # Test VSIN data
    vsin_record = RawDataRecord(
        external_id="vsin_test",
        source="vsin"
    )
    table_name = await processor._determine_table_name(vsin_record)
    assert table_name == "raw_data.vsin_data"
    print("  ✅ VSIN data routing works")
    
    # Test generic bet type routing
    moneyline_record = RawDataRecord(
        external_id="ml_test",
        source="generic",
        bet_type="moneyline"
    )
    table_name = await processor._determine_table_name(moneyline_record)
    assert table_name == "raw_data.moneylines_raw"
    print("  ✅ Moneyline bet type routing works")
    
    spread_record = RawDataRecord(
        external_id="spread_test",
        source="generic",
        bet_type="spread"
    )
    table_name = await processor._determine_table_name(spread_record)
    assert table_name == "raw_data.spreads_raw"
    print("  ✅ Spread bet type routing works")
    
    # Test default fallback
    unknown_record = RawDataRecord(
        external_id="unknown_test",
        source="unknown_source"
    )
    table_name = await processor._determine_table_name(unknown_record)
    assert table_name == "raw_data.betting_lines_raw"
    print("  ✅ Default fallback routing works")


@pytest.mark.asyncio
async def test_record_validation():
    """Test RAW zone record validation logic."""
    from src.data.pipeline.raw_zone import RawZoneProcessor
    
    print("\n✅ Testing record validation...")
    
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = RawZoneProcessor(config)
    
    # Test valid record with external_id
    valid_record = DataRecord(
        external_id="valid_test",
        source="test_source",
        raw_data={"test": "data"}
    )
    is_valid = await processor.validate_record_custom(valid_record)
    assert is_valid is True
    print("  ✅ Valid record with external_id passes validation")
    
    # Test valid record with only raw_data (no external_id)
    valid_record_no_id = DataRecord(
        source="test_source",
        raw_data={"test": "data"}
    )
    is_valid = await processor.validate_record_custom(valid_record_no_id)
    assert is_valid is True
    print("  ✅ Valid record with only raw_data passes validation")
    
    # Test invalid record (no external_id and no raw_data)
    invalid_record = DataRecord(
        source="test_source"
    )
    is_valid = await processor.validate_record_custom(invalid_record)
    assert is_valid is False
    assert "RAW record must have either external_id or raw_data" in invalid_record.validation_errors
    print("  ✅ Invalid record fails validation with correct error message")
    
    # Test record with empty raw_data (should pass for RAW zone)
    empty_data_record = DataRecord(
        external_id="empty_data_test",
        source="test_source",
        raw_data={}
    )
    is_valid = await processor.validate_record_custom(empty_data_record)
    assert is_valid is True
    print("  ✅ Record with empty raw_data passes validation")


@pytest.mark.asyncio
async def test_database_insert_methods():
    """Test database insert method calls without actual database."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    
    print("\n💾 Testing database insert methods...")
    
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = RawZoneProcessor(config)
    
    # Mock connection
    mock_connection = AsyncMock()
    
    # Test Action Network games insert
    an_games = [
        RawDataRecord(
            external_id="game_123",
            source="action_network",
            raw_data={"game_id": "123"},
            endpoint_url="https://api.actionnetwork.com/games/123",
            response_status=200,
            game_date="2025-07-21"
        )
    ]
    
    await processor._insert_action_network_games(mock_connection, an_games)
    mock_connection.execute.assert_called()
    
    # Verify the SQL query structure
    call_args = mock_connection.execute.call_args
    query = call_args[0][0]
    assert "INSERT INTO raw_data.action_network_games" in query
    assert "ON CONFLICT (external_game_id) DO UPDATE" in query
    print("  ✅ Action Network games insert method works")
    
    # Test generic betting lines insert
    mock_connection.reset_mock()
    
    betting_lines = [
        RawDataRecord(
            external_id="line_456",
            source="test_source",
            raw_data={"odds": -110},
            game_external_id="game_789",
            sportsbook_name="DraftKings",
            bet_type="moneyline"
        )
    ]
    
    await processor._insert_betting_lines_raw(mock_connection, betting_lines)
    mock_connection.execute.assert_called()
    
    call_args = mock_connection.execute.call_args
    query = call_args[0][0]
    assert "INSERT INTO raw_data.betting_lines_raw" in query
    print("  ✅ Generic betting lines insert method works")


@pytest.mark.asyncio
async def test_batch_processing_with_mocks():
    """Test batch processing functionality with mocked dependencies."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    
    print("\n🔄 Testing batch processing...")
    
    config = create_zone_config(
        ZoneType.RAW, 
        "raw_data",
        batch_size=10,
        validation_enabled=True,
        quality_threshold=0.5
    )
    
    # Create processor and mock dependencies
    with patch.object(RawZoneProcessor, 'get_connection') as mock_get_conn, \
         patch.object(RawZoneProcessor, 'store_records') as mock_store:
        
        mock_connection = AsyncMock()
        mock_get_conn.return_value = mock_connection
        mock_store.return_value = None
        
        processor = RawZoneProcessor(config)
        
        # Create test batch
        records = [
            RawDataRecord(
                external_id=f"batch_test_{i}",
                source="test_source",
                raw_data={"test": f"data_{i}", "quality": "high"}
            ) for i in range(5)
        ]
        
        # Process batch
        result = await processor.process_batch(records)
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 5
        assert result.records_successful == 5
        assert result.records_failed == 0
        print("  ✅ Successful batch processing works")
        
        # Test batch with some failures
        mixed_records = [
            RawDataRecord(
                external_id="good_record",
                source="test_source", 
                raw_data={"test": "good_data"}
            ),
            DataRecord(
                source="test_source"  # Missing external_id and raw_data
            ),
            RawDataRecord(
                external_id="another_good_record",
                source="test_source",
                raw_data={"test": "more_good_data"}
            )
        ]
        
        result_mixed = await processor.process_batch(mixed_records)
        
        assert result_mixed.records_processed == 3
        assert result_mixed.records_successful == 2  # Only the valid ones
        assert result_mixed.records_failed == 1     # The invalid one
        print("  ✅ Mixed success/failure batch processing works")


def test_zone_factory_registration():
    """Test that RAW zone processor is properly registered with ZoneFactory."""
    from src.data.pipeline.zone_interface import ZoneFactory
    from src.data.pipeline.raw_zone import RawZoneProcessor
    
    print("\n🏭 Testing ZoneFactory registration...")
    
    # Test that RAW zone is in registered zones
    registered_zones = ZoneFactory.list_registered_zones()
    assert ZoneType.RAW in registered_zones
    print("  ✅ RAW zone processor is registered with ZoneFactory")
    
    # Test zone creation through factory
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = ZoneFactory.create_zone(ZoneType.RAW, config)
    
    assert isinstance(processor, RawZoneProcessor)
    assert processor.zone_type == ZoneType.RAW
    print("  ✅ RAW zone processor creation through factory works")


def run_raw_zone_tests():
    """Run all RAW zone processor tests."""
    print("🚀 Starting RAW Zone Processor Tests")
    print("=" * 60)
    
    try:
        # Run all tests
        test_raw_data_record_structure()
        test_raw_zone_processor_initialization()
        
        # Run async tests (need to handle differently)
        import asyncio
        
        async def run_async_tests():
            await test_metadata_extraction()
            await test_record_processing()
            await test_table_name_determination()
            await test_record_validation()
            await test_database_insert_methods()
            await test_batch_processing_with_mocks()
        
        asyncio.run(run_async_tests())
        
        test_zone_factory_registration()
        
        print("\n" + "=" * 60)
        print("🎉 ALL RAW ZONE PROCESSOR TESTS PASSED!")
        print("✅ Record structure and metadata extraction working")
        print("✅ Table routing and database operations functional")
        print("✅ Validation logic and error handling operational")
        print("✅ Batch processing with quality control working")
        print("✅ Zone factory integration successful")
        return True
        
    except Exception as e:
        print(f"\n❌ RAW ZONE TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_raw_zone_tests()
    sys.exit(0 if success else 1)