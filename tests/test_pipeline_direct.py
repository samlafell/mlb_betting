#!/usr/bin/env python3
"""
Test Pipeline Direct

Direct imports test for pipeline zone interface without going through
the complex import chain that includes pydantic models.
"""

import sys

sys.path.insert(
    0, "/Users/samlafell/Documents/programming_projects/mlb_betting_program"
)


# Test basic functionality of the pipeline implementation
def test_successful_pydantic_fix():
    """Test that our Pydantic compatibility fixes work."""
    from src.core.pydantic_compat import (
        COMPUTED_FIELD_AVAILABLE,
        FIELD_VALIDATOR_AVAILABLE,
        MODEL_VALIDATOR_AVAILABLE,
        VALIDATION_INFO_AVAILABLE,
        computed_field,
        field_validator,
        model_validator,
    )

    print("Pydantic compatibility status:")
    print(f"  computed_field: {COMPUTED_FIELD_AVAILABLE}")
    print(f"  field_validator: {FIELD_VALIDATOR_AVAILABLE}")
    print(f"  model_validator: {MODEL_VALIDATOR_AVAILABLE}")
    print(f"  ValidationInfo: {VALIDATION_INFO_AVAILABLE}")

    # Test that the decorators work
    @computed_field
    @property
    def test_computed(self):
        return "test"

    @field_validator("test_field")
    @classmethod
    def test_field_validator(cls, v):
        return v

    @model_validator(mode="before")
    @classmethod
    def test_model_validator(cls, v):
        return v

    print("✅ All Pydantic compatibility decorators work")


def test_zone_interface_direct():
    """Test zone interface components directly."""
    from src.data.pipeline.zone_interface import (
        ProcessingStatus,
        ZoneType,
        create_zone_config,
        get_next_zone,
        validate_zone_progression,
    )

    print("\n🧪 Testing zone interface components:")

    # Test enums
    assert ZoneType.RAW.value == "raw"
    assert ZoneType.STAGING.value == "staging"
    assert ZoneType.CURATED.value == "curated"
    print("  ✅ ZoneType enum values correct")

    assert ProcessingStatus.PENDING.value == "pending"
    assert ProcessingStatus.COMPLETED.value == "completed"
    print("  ✅ ProcessingStatus enum values correct")

    # Test progression logic
    assert validate_zone_progression(ZoneType.RAW, ZoneType.STAGING) is True
    assert validate_zone_progression(ZoneType.STAGING, ZoneType.CURATED) is True
    assert validate_zone_progression(ZoneType.RAW, ZoneType.CURATED) is False
    print("  ✅ Zone progression validation works")

    # Test next zone logic
    assert get_next_zone(ZoneType.RAW) == ZoneType.STAGING
    assert get_next_zone(ZoneType.STAGING) == ZoneType.CURATED
    assert get_next_zone(ZoneType.CURATED) is None
    print("  ✅ Get next zone logic works")

    # Test config creation
    config = create_zone_config(ZoneType.RAW, "raw_data")
    assert config.zone_type == ZoneType.RAW
    assert config.schema_name == "raw_data"
    print("  ✅ Zone config creation works")


def test_data_models():
    """Test data models work with our compatibility fixes."""
    from src.data.pipeline.zone_interface import (
        DataRecord,
        ProcessingResult,
        ProcessingStatus,
        ZoneMetrics,
    )

    print("\n📊 Testing data models:")

    # Test DataRecord
    record = DataRecord(
        external_id="test_123", source="action_network", raw_data={"test": "data"}
    )
    assert record.external_id == "test_123"
    assert record.source == "action_network"
    print("  ✅ DataRecord model works")

    # Test ProcessingResult
    result = ProcessingResult(
        status=ProcessingStatus.COMPLETED,
        records_processed=100,
        records_successful=95,
        records_failed=5,
    )
    assert result.records_processed == 100
    assert result.records_successful == 95
    print("  ✅ ProcessingResult model works")

    # Test ZoneMetrics
    metrics = ZoneMetrics(
        records_processed=1000,
        records_successful=950,
        records_failed=50,
        processing_time_seconds=25.5,
        quality_score=0.85,
        error_rate=0.05,
    )
    assert metrics.records_processed == 1000
    assert metrics.quality_score == 0.85
    print("  ✅ ZoneMetrics dataclass works")


def test_progression_workflow():
    """Test complete workflow through all zones."""
    from src.data.pipeline.zone_interface import (
        DataRecord,
        ZoneType,
        get_next_zone,
        validate_zone_progression,
    )

    print("\n🔄 Testing progression workflow:")

    # Create a record and process through zones
    record = DataRecord(
        external_id="workflow_test",
        source="test_source",
        raw_data={"game_id": "123", "data": "test"},
    )

    # Start with RAW zone
    current_zone = ZoneType.RAW
    zones_processed = [current_zone]

    # Progress through all zones
    while True:
        next_zone = get_next_zone(current_zone)
        if next_zone is None:
            break

        # Validate progression
        assert validate_zone_progression(current_zone, next_zone)
        zones_processed.append(next_zone)
        current_zone = next_zone

    # Verify we processed through all zones
    expected_zones = [ZoneType.RAW, ZoneType.STAGING, ZoneType.CURATED]
    assert zones_processed == expected_zones
    print("  ✅ Full zone progression workflow completed")
    print(f"  📈 Zones processed: {[z.value for z in zones_processed]}")


def run_all_tests():
    """Run all direct tests and report results."""
    print("🚀 Starting Pipeline Direct Tests")
    print("=" * 50)

    try:
        test_successful_pydantic_fix()
        test_zone_interface_direct()
        test_data_models()
        test_progression_workflow()

        print("\n" + "=" * 50)
        print("🎉 ALL TESTS PASSED!")
        print("✅ Pydantic v2 compatibility fixes successful")
        print("✅ Zone interface components working")
        print("✅ Data models functioning correctly")
        print("✅ Pipeline progression workflow operational")
        return True

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
