#!/usr/bin/env python3
"""
Test Pipeline Zone Interface

Tests for zone interface and progression logic.
Tests the first phase pipeline implementation components.
"""

import pytest
from datetime import datetime
from enum import Enum

# Direct imports with proper error handling
def test_zone_type_enum():
    """Test ZoneType enumeration basic functionality."""
    from src.data.pipeline.zone_interface import ZoneType
    
    # Test enum values
    assert ZoneType.RAW.value == "raw"
    assert ZoneType.STAGING.value == "staging"  
    assert ZoneType.CURATED.value == "curated"
    
    # Test enum functionality
    zones = list(ZoneType)
    assert len(zones) == 3
    assert ZoneType.RAW in zones
    assert ZoneType.STAGING in zones
    assert ZoneType.CURATED in zones

def test_processing_status_enum():
    """Test ProcessingStatus enumeration basic functionality."""
    from src.data.pipeline.zone_interface import ProcessingStatus
    
    # Test enum values  
    assert ProcessingStatus.PENDING.value == "pending"
    assert ProcessingStatus.IN_PROGRESS.value == "in_progress"
    assert ProcessingStatus.COMPLETED.value == "completed"
    assert ProcessingStatus.FAILED.value == "failed"
    assert ProcessingStatus.SKIPPED.value == "skipped"
    
    # Test enum functionality
    statuses = list(ProcessingStatus)
    assert len(statuses) == 5

def test_zone_progression_validation():
    """Test zone progression validation logic."""
    from src.data.pipeline.zone_interface import (
        ZoneType, 
        validate_zone_progression
    )
    
    # Test valid progressions
    assert validate_zone_progression(ZoneType.RAW, ZoneType.STAGING) is True
    assert validate_zone_progression(ZoneType.STAGING, ZoneType.CURATED) is True
    
    # Test invalid progressions 
    assert validate_zone_progression(ZoneType.RAW, ZoneType.CURATED) is False  # Skip staging
    assert validate_zone_progression(ZoneType.STAGING, ZoneType.RAW) is False  # Backward
    assert validate_zone_progression(ZoneType.CURATED, ZoneType.RAW) is False  # Backward
    assert validate_zone_progression(ZoneType.RAW, ZoneType.RAW) is False  # Same zone

def test_get_next_zone():
    """Test getting next zone in progression."""
    from src.data.pipeline.zone_interface import (
        ZoneType,
        get_next_zone
    )
    
    # Test progression
    assert get_next_zone(ZoneType.RAW) == ZoneType.STAGING
    assert get_next_zone(ZoneType.STAGING) == ZoneType.CURATED
    assert get_next_zone(ZoneType.CURATED) is None  # Final zone

def test_zone_config_creation():
    """Test zone configuration creation."""
    from src.data.pipeline.zone_interface import (
        ZoneType,
        create_zone_config
    )
    
    # Test config creation with defaults
    config = create_zone_config(ZoneType.RAW, "raw_data")
    
    assert config.zone_type == ZoneType.RAW
    assert config.schema_name == "raw_data"
    assert config.batch_size == 1000  # Default
    assert config.quality_threshold == 0.8  # Default
    assert config.validation_enabled is True  # Default
    
def test_zone_config_custom_values():
    """Test zone configuration with custom values."""
    from src.data.pipeline.zone_interface import (
        ZoneType,
        create_zone_config
    )
    
    # Test config creation with custom values
    config = create_zone_config(
        ZoneType.STAGING, 
        "staging",
        batch_size=500,
        quality_threshold=0.9,
        validation_enabled=False,
        auto_promotion=False
    )
    
    assert config.zone_type == ZoneType.STAGING
    assert config.schema_name == "staging"
    assert config.batch_size == 500
    assert config.quality_threshold == 0.9
    assert config.validation_enabled is False
    assert config.auto_promotion is False

def test_data_record_structure():
    """Test DataRecord model structure."""
    from src.data.pipeline.zone_interface import (
        DataRecord,
        ProcessingStatus
    )
    
    # Test basic record creation
    record = DataRecord(
        external_id="test_123",
        source="action_network",
        raw_data={"test": "data"}
    )
    
    assert record.external_id == "test_123"
    assert record.source == "action_network"
    assert record.raw_data == {"test": "data"}
    assert record.validation_status == ProcessingStatus.PENDING
    assert record.quality_score is None
    assert record.created_at is not None

def test_processing_result_structure():
    """Test ProcessingResult model structure."""
    from src.data.pipeline.zone_interface import (
        ProcessingResult,
        ProcessingStatus
    )
    
    # Test result creation
    result = ProcessingResult(
        status=ProcessingStatus.COMPLETED,
        records_processed=100,
        records_successful=95,
        records_failed=5,
        processing_time=10.5
    )
    
    assert result.status == ProcessingStatus.COMPLETED
    assert result.records_processed == 100
    assert result.records_successful == 95
    assert result.records_failed == 5
    assert result.processing_time == 10.5
    assert isinstance(result.errors, list)
    assert len(result.errors) == 0
    assert isinstance(result.metadata, dict)

def test_zone_metrics_structure():
    """Test ZoneMetrics dataclass structure."""
    from src.data.pipeline.zone_interface import ZoneMetrics
    
    # Test metrics creation
    metrics = ZoneMetrics(
        records_processed=1000,
        records_successful=950,
        records_failed=50,
        processing_time_seconds=25.5,
        quality_score=0.85,
        error_rate=0.05
    )
    
    assert metrics.records_processed == 1000
    assert metrics.records_successful == 950
    assert metrics.records_failed == 50
    assert metrics.processing_time_seconds == 25.5
    assert metrics.quality_score == 0.85
    assert metrics.error_rate == 0.05

def test_zone_progression_workflow():
    """Test complete zone progression workflow."""
    from src.data.pipeline.zone_interface import (
        ZoneType,
        ProcessingStatus,
        validate_zone_progression,
        get_next_zone,
        create_zone_config,
        DataRecord
    )
    
    # Test workflow through all zones
    zones = [ZoneType.RAW, ZoneType.STAGING, ZoneType.CURATED]
    
    current_zone = zones[0]
    
    # Process through each zone
    for i, next_zone in enumerate(zones[1:], 1):
        # Validate progression
        assert validate_zone_progression(current_zone, next_zone) is True
        
        # Get next zone
        calculated_next = get_next_zone(current_zone)
        assert calculated_next == next_zone
        
        # Move to next zone
        current_zone = next_zone
    
    # Final zone should have no next zone
    assert get_next_zone(ZoneType.CURATED) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])