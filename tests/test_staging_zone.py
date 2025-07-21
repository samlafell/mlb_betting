#!/usr/bin/env python3
"""
Test STAGING Zone Processor

Comprehensive tests for the STAGING zone processor functionality including:
- Data cleaning and normalization 
- Team name normalization
- Sportsbook name mapping
- Numeric field cleaning
- Data quality scoring
- Consistency validation
- Raw data extraction and processing

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import sys
sys.path.insert(0, '/Users/samlafell/Documents/programming_projects/mlb_betting_program')

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, AsyncMock, patch, MagicMock

def test_staging_data_record_structure():
    """Test StagingDataRecord structure and field validation."""
    from src.data.pipeline.staging_zone import StagingDataRecord
    from src.data.pipeline.zone_interface import ProcessingStatus
    
    print("\n🧪 Testing StagingDataRecord structure...")
    
    # Test basic staging record creation
    record = StagingDataRecord(
        external_id="staging_test_123",
        source="action_network",
        raw_data={"game_id": "123", "test": "data"},
        home_team_normalized="New York Yankees",
        away_team_normalized="Boston Red Sox",
        sportsbook_name="DraftKings",
        bet_type="moneyline",
        line_value=Decimal("-1.5"),
        odds_american=-150,
        team_type="home",
        data_completeness_score=0.95,
        data_accuracy_score=0.90,
        data_consistency_score=0.85
    )
    
    assert record.external_id == "staging_test_123"
    assert record.source == "action_network"
    assert record.home_team_normalized == "New York Yankees"
    assert record.away_team_normalized == "Boston Red Sox"
    assert record.sportsbook_name == "DraftKings"
    assert record.bet_type == "moneyline"
    assert record.line_value == Decimal("-1.5")
    assert record.odds_american == -150
    assert record.team_type == "home"
    assert record.data_completeness_score == 0.95
    print("  ✅ StagingDataRecord creation works")

def test_staging_zone_processor_initialization():
    """Test STAGING zone processor initialization."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n⚙️ Testing STAGING zone processor initialization...")
    
    # Create STAGING zone configuration
    config = create_zone_config(
        ZoneType.STAGING,
        "staging",
        batch_size=100,
        validation_enabled=True,
        quality_threshold=0.7
    )
    
    # Initialize processor
    processor = StagingZoneProcessor(config)
    
    assert processor.zone_type == ZoneType.STAGING
    assert processor.schema_name == "staging"
    assert processor.config.batch_size == 100
    assert processor.config.validation_enabled is True
    assert processor.config.quality_threshold == 0.7
    print("  ✅ STAGING zone processor initialization successful")
    
    # Test sportsbook mapping loaded
    assert 'draftkings' in processor.sportsbook_mapping
    assert processor.sportsbook_mapping['draftkings'] == 'DraftKings'
    assert 'fanduel' in processor.sportsbook_mapping
    assert processor.sportsbook_mapping['fanduel'] == 'FanDuel'
    print("  ✅ Sportsbook mapping loaded correctly")

async def test_raw_data_normalization():
    """Test raw data extraction and normalization."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n🔄 Testing raw data normalization...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    processor = StagingZoneProcessor(config)
    
    # Test Action Network game data format
    raw_data = {
        "home_team": "New York Yankees",
        "away_team": "Boston Red Sox",
        "sportsbook": {
            "name": "DraftKings",
            "id": 1
        },
        "outcomes": [
            {
                "price": -150,
                "point": -1.5,
                "team": "home"
            }
        ],
        "moneyline": {
            "home": -150,
            "away": 130
        }
    }
    
    record = StagingDataRecord(
        external_id="norm_test",
        source="action_network",
        raw_data=raw_data
    )
    
    # Normalize the record
    normalized = await processor._normalize_from_raw_data(record)
    
    assert normalized.home_team_normalized == "New York Yankees"
    assert normalized.away_team_normalized == "Boston Red Sox"
    assert normalized.sportsbook_name == "DraftKings"
    assert normalized.sportsbook_id == 1
    assert normalized.odds_american == -150
    assert normalized.line_value == Decimal("-1.5")
    print("  ✅ Action Network data normalization works")
    
    # Test alternative field formats
    alt_raw_data = {
        "homeTeam": "Los Angeles Dodgers",
        "awayTeam": "San Francisco Giants",
        "sportsbook": "FanDuel",
        "spread": {
            "home": {"odds": -110, "line": -2.5},
            "away": {"odds": -110, "line": 2.5}
        }
    }
    
    record2 = StagingDataRecord(
        external_id="alt_test",
        source="generic",
        raw_data=alt_raw_data
    )
    
    normalized2 = await processor._normalize_from_raw_data(record2)
    
    assert normalized2.home_team_normalized == "Los Angeles Dodgers"
    assert normalized2.away_team_normalized == "San Francisco Giants"
    assert normalized2.sportsbook_name == "FanDuel"
    print("  ✅ Alternative data format normalization works")

async def test_team_name_normalization():
    """Test team name normalization functionality."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n🏟️ Testing team name normalization...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    
    with patch('src.data.pipeline.staging_zone.normalize_team_name') as mock_normalize:
        # Mock team name normalization
        mock_normalize.side_effect = lambda x: x.replace("NY", "New York").replace("LA", "Los Angeles")
        
        processor = StagingZoneProcessor(config)
        
        record = StagingDataRecord(
            external_id="team_test",
            source="test",
            home_team_normalized="NY Yankees",
            away_team_normalized="LA Dodgers"
        )
        
        normalized = await processor._normalize_team_names(record)
        
        assert normalized.home_team_normalized == "New York Yankees"
        assert normalized.away_team_normalized == "Los Angeles Dodgers"
        
        # Verify normalize_team_name was called
        assert mock_normalize.call_count == 2
        print("  ✅ Team name normalization works")

async def test_sportsbook_name_normalization():
    """Test sportsbook name normalization."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n📊 Testing sportsbook name normalization...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    processor = StagingZoneProcessor(config)
    
    # Test known sportsbook mapping
    record = StagingDataRecord(
        external_id="sb_test",
        source="test",
        sportsbook_name="draftkings"
    )
    
    normalized = await processor._normalize_sportsbook_names(record)
    
    assert normalized.sportsbook_name == "DraftKings"
    print("  ✅ Known sportsbook name normalization works")
    
    # Test unknown sportsbook (should remain unchanged but cleaned)
    record2 = StagingDataRecord(
        external_id="sb_test2",
        source="test",
        sportsbook_name="unknown_book"
    )
    
    normalized2 = await processor._normalize_sportsbook_names(record2)
    
    assert normalized2.sportsbook_name == "Unknown_Book"  # Title case applied to original
    print("  ✅ Unknown sportsbook name handling works")

async def test_numeric_field_cleaning():
    """Test numeric field cleaning and conversion."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n🔢 Testing numeric field cleaning...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    processor = StagingZoneProcessor(config)
    
    # Test raw data with various numeric formats
    raw_data = {
        "odds": "-150",  # String number
        "line": "2.5",   # String decimal
        "invalid_odds": "invalid",  # Invalid string
        "null_line": None  # Null value
    }
    
    record = StagingDataRecord(
        external_id="numeric_test",
        source="test",
        raw_data=raw_data,
        odds_american="-110",  # String that should convert to int
        line_value="1.5"       # String that should convert to Decimal
    )
    
    cleaned = await processor._clean_numeric_fields(record)
    
    assert isinstance(cleaned.odds_american, int)
    assert cleaned.odds_american == -110
    assert isinstance(cleaned.line_value, Decimal)
    assert cleaned.line_value == Decimal("1.5")
    print("  ✅ Numeric field cleaning works")
    
    # Test invalid numeric data handling by modifying a valid record
    record2 = StagingDataRecord(
        external_id="invalid_numeric_test",
        source="test",
        odds_american=100,  # Valid initially
        line_value=Decimal("1.0")  # Valid initially
    )
    
    # Manually set invalid string values (bypassing validation)
    record2.__dict__['odds_american'] = "not_a_number"
    record2.__dict__['line_value'] = "also_not_a_number"
    
    cleaned2 = await processor._clean_numeric_fields(record2)
    
    # Invalid values should be set to None
    assert cleaned2.odds_american is None
    assert cleaned2.line_value is None
    print("  ✅ Invalid numeric data handling works")

async def test_quality_score_calculation():
    """Test data quality score calculation."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n📈 Testing quality score calculation...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    processor = StagingZoneProcessor(config)
    
    # Test high-quality record
    high_quality_record = StagingDataRecord(
        external_id="quality_test",
        source="action_network",
        raw_data={"complete": "data"},
        home_team_normalized="New York Yankees",
        away_team_normalized="Boston Red Sox",
        sportsbook_name="DraftKings",
        bet_type="moneyline",
        odds_american=-150,
        line_value=Decimal("1.5")
    )
    
    completeness = await processor._calculate_completeness_score(high_quality_record)
    accuracy = await processor._calculate_accuracy_score(high_quality_record)
    consistency = await processor._calculate_consistency_score(high_quality_record)
    
    assert completeness >= 0.8  # Should have high completeness
    assert accuracy >= 0.8      # Should have high accuracy
    assert consistency >= 0.8   # Should have high consistency
    print("  ✅ High quality record scoring works")
    
    # Test low-quality record
    low_quality_record = StagingDataRecord(
        external_id="low_quality_test",
        source="unknown",
        raw_data=None  # Missing raw data
        # Missing most fields
    )
    
    completeness_low = await processor._calculate_completeness_score(low_quality_record)
    
    assert completeness_low < completeness  # Should be lower than high quality record
    print("  ✅ Low quality record scoring works (relative comparison)")

async def test_data_consistency_validation():
    """Test data consistency validation logic."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config
    
    print("\n✅ Testing data consistency validation...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    processor = StagingZoneProcessor(config)
    
    # Test consistent record
    consistent_record = StagingDataRecord(
        external_id="consistent_test",
        source="action_network",
        bet_type="moneyline",
        odds_american=-150,  # Consistent with moneyline
        line_value=None,     # No line for moneyline (consistent)
        team_type="home"
    )
    
    is_consistent = await processor._validate_data_consistency(consistent_record)
    assert is_consistent is True
    print("  ✅ Consistent record validation works")
    
    # Test inconsistent record - extreme odds values should trigger inconsistency
    inconsistent_record = StagingDataRecord(
        external_id="inconsistent_test",
        source="action_network",
        bet_type="moneyline",
        odds_american=10000,  # Extreme odds value should be flagged
        team_type="home"
    )
    
    is_inconsistent = await processor._validate_data_consistency(inconsistent_record)
    assert is_inconsistent is False
    print("  ✅ Inconsistent record validation works")

async def test_full_record_processing():
    """Test complete record processing workflow."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config, ProcessingStatus, DataRecord
    
    print("\n🔄 Testing full record processing...")
    
    config = create_zone_config(ZoneType.STAGING, "staging")
    
    with patch('src.data.pipeline.staging_zone.normalize_team_name') as mock_normalize:
        mock_normalize.side_effect = lambda x: x.upper() if x else x
        
        processor = StagingZoneProcessor(config)
        
        # Create raw record for processing
        raw_record = DataRecord(
            external_id="full_test",
            source="action_network",
            raw_data={
                "home_team": "Yankees",
                "away_team": "Red Sox",
                "sportsbook": {
                    "name": "DraftKings",
                    "id": 1
                },
                "moneyline": {
                    "home": -150,
                    "away": 130
                },
                "bet_type": "moneyline"
            }
        )
        
        # Process the record
        processed = await processor.process_record(raw_record)
        
        assert processed is not None
        assert isinstance(processed, StagingDataRecord)
        assert processed.external_id == "full_test"
        assert processed.validation_status == ProcessingStatus.COMPLETED
        assert processed.processed_at is not None
        
        # Check normalization occurred
        assert processed.home_team_normalized == "YANKEES"  # Mocked to uppercase
        assert processed.away_team_normalized == "RED SOX"
        assert processed.sportsbook_name == "DraftKings"
        assert processed.odds_american == -150
        
        # Check quality scores were calculated
        assert processed.data_completeness_score is not None
        assert processed.data_accuracy_score is not None
        assert processed.data_consistency_score is not None
        assert processed.quality_score is not None
        assert 0.0 <= processed.quality_score <= 1.0
        
        print("  ✅ Full record processing workflow works")

async def test_batch_processing():
    """Test staging zone batch processing."""
    from src.data.pipeline.staging_zone import StagingZoneProcessor
    from src.data.pipeline.zone_interface import ZoneType, create_zone_config, ProcessingStatus, DataRecord
    
    print("\n📦 Testing batch processing...")
    
    config = create_zone_config(
        ZoneType.STAGING,
        "staging",
        batch_size=10,
        validation_enabled=True,
        quality_threshold=0.5
    )
    
    with patch('src.data.pipeline.staging_zone.normalize_team_name') as mock_normalize, \
         patch.object(StagingZoneProcessor, 'get_connection') as mock_get_conn, \
         patch.object(StagingZoneProcessor, 'store_records') as mock_store:
        
        mock_normalize.side_effect = lambda x: x
        mock_connection = AsyncMock()
        mock_get_conn.return_value = mock_connection
        mock_store.return_value = None
        
        processor = StagingZoneProcessor(config)
        
        # Create test batch
        records = []
        for i in range(3):
            record = DataRecord(
                external_id=f"batch_test_{i}",
                source="action_network",
                raw_data={
                    "home_team": f"Team_Home_{i}",
                    "away_team": f"Team_Away_{i}",
                    "sportsbook": "DraftKings",
                    "moneyline": {"home": -150, "away": 130}
                }
            )
            records.append(record)
        
        # Process batch
        result = await processor.process_batch(records)
        
        assert result.status == ProcessingStatus.COMPLETED
        assert result.records_processed == 3
        assert result.records_successful == 3
        assert result.records_failed == 0
        
        print("  ✅ Batch processing works")

def test_zone_factory_registration():
    """Test that STAGING zone processor is registered with ZoneFactory."""
    from src.data.pipeline.zone_interface import ZoneFactory, ZoneType, create_zone_config
    from src.data.pipeline.staging_zone import StagingZoneProcessor
    
    print("\n🏭 Testing ZoneFactory registration...")
    
    # Test that STAGING zone is in registered zones
    registered_zones = ZoneFactory.list_registered_zones()
    assert ZoneType.STAGING in registered_zones
    print("  ✅ STAGING zone processor is registered with ZoneFactory")
    
    # Test zone creation through factory
    config = create_zone_config(ZoneType.STAGING, "staging")
    processor = ZoneFactory.create_zone(ZoneType.STAGING, config)
    
    assert isinstance(processor, StagingZoneProcessor)
    assert processor.zone_type == ZoneType.STAGING
    print("  ✅ STAGING zone processor creation through factory works")

def run_staging_zone_tests():
    """Run all STAGING zone processor tests."""
    print("🚀 Starting STAGING Zone Processor Tests")
    print("=" * 60)
    
    try:
        # Run sync tests
        test_staging_data_record_structure()
        test_staging_zone_processor_initialization()
        test_zone_factory_registration()
        
        # Run async tests
        async def run_async_tests():
            await test_raw_data_normalization()
            await test_team_name_normalization()
            await test_sportsbook_name_normalization()
            await test_numeric_field_cleaning()
            await test_quality_score_calculation()
            await test_data_consistency_validation()
            await test_full_record_processing()
            await test_batch_processing()
        
        asyncio.run(run_async_tests())
        
        print("\n" + "=" * 60)
        print("🎉 ALL STAGING ZONE PROCESSOR TESTS PASSED!")
        print("✅ Data cleaning and normalization working")
        print("✅ Team and sportsbook name processing operational")
        print("✅ Numeric field cleaning and validation functional")
        print("✅ Quality scoring and consistency validation working")
        print("✅ Batch processing with quality control operational")
        print("✅ Zone factory integration successful")
        return True
        
    except Exception as e:
        print(f"\n❌ STAGING ZONE TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_staging_zone_tests()
    sys.exit(0 if success else 1)