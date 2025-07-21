#!/usr/bin/env python3
"""
Test RAW Zone Processor Direct

Direct tests for RAW zone processor without external dependencies.
"""

import sys
sys.path.insert(0, '/Users/samlafell/Documents/programming_projects/mlb_betting_program')

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch

def test_raw_data_record_structure():
    """Test RawDataRecord structure and field validation."""
    from src.data.pipeline.raw_zone import RawDataRecord
    from src.data.pipeline.zone_interface import ProcessingStatus
    
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
        game_date="2025-07-21"
    )
    
    assert record_with_metadata.game_external_id == "game_123"
    assert record_with_metadata.sportsbook_name == "DraftKings"
    assert record_with_metadata.bet_type == "moneyline"
    print("  ✅ RawDataRecord with metadata fields works")

def test_raw_zone_processor_initialization():
    """Test RAW zone processor initialization and configuration."""
    from src.data.pipeline.raw_zone import RawZoneProcessor
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
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
        'action_network_games',
        'betting_lines_raw', 
        'moneylines_raw',
        'spreads_raw',
        'totals_raw'
    ]
    
    for table in expected_tables:
        assert any(table in mapping for mapping in processor.table_mappings.values())
    print("  ✅ Table mappings configured correctly")

async def test_metadata_extraction():
    """Test metadata extraction from raw data."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
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

async def test_table_name_determination():
    """Test table name determination logic."""
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    from src.data.pipeline.zone_interface import create_zone_config, ZoneType
    
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
    
    # Test SBD data
    sbd_record = RawDataRecord(
        external_id="sbd_test",
        source="sbd"
    )
    table_name = await processor._determine_table_name(sbd_record)
    assert table_name == "raw_data.sbd_betting_splits" 
    print("  ✅ SBD data routing works")
    
    # Test generic bet types
    moneyline_record = RawDataRecord(
        external_id="ml_test",
        source="generic",
        bet_type="moneyline"
    )
    table_name = await processor._determine_table_name(moneyline_record)
    assert table_name == "raw_data.moneylines_raw"
    print("  ✅ Moneyline bet type routing works")

async def test_record_validation():
    """Test RAW zone record validation logic."""
    from src.data.pipeline.raw_zone import RawZoneProcessor
    from src.data.pipeline.zone_interface import DataRecord, create_zone_config, ZoneType
    
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
    
    # Test invalid record (no external_id and no raw_data)
    invalid_record = DataRecord(
        source="test_source"
    )
    is_valid = await processor.validate_record_custom(invalid_record)
    assert is_valid is False
    assert "RAW record must have either external_id or raw_data" in invalid_record.validation_errors
    print("  ✅ Invalid record fails validation with correct error message")

async def test_record_processing():
    """Test individual record processing functionality.""" 
    from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
    from src.data.pipeline.zone_interface import DataRecord, ProcessingStatus, create_zone_config, ZoneType
    
    print("\n⚙️ Testing record processing...")
    
    config = create_zone_config(ZoneType.RAW, "raw_data")
    processor = RawZoneProcessor(config)
    
    # Test valid record processing
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

def test_zone_factory_registration():
    """Test that RAW zone processor is properly registered with ZoneFactory."""
    from src.data.pipeline.zone_interface import ZoneFactory, ZoneType, create_zone_config
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
        # Run sync tests
        test_raw_data_record_structure()
        test_raw_zone_processor_initialization()
        test_zone_factory_registration()
        
        # Run async tests
        async def run_async_tests():
            await test_metadata_extraction()
            await test_table_name_determination()
            await test_record_validation()
            await test_record_processing()
        
        asyncio.run(run_async_tests())
        
        print("\n" + "=" * 60)
        print("🎉 ALL RAW ZONE PROCESSOR TESTS PASSED!")
        print("✅ Record structure and metadata extraction working")
        print("✅ Table routing and validation logic operational") 
        print("✅ Record processing functionality successful")
        print("✅ Zone factory integration working")
        return True
        
    except Exception as e:
        print(f"\n❌ RAW ZONE TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_raw_zone_tests()
    sys.exit(0 if success else 1)