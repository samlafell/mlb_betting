#!/usr/bin/env python3
"""
Test Pipeline Core Components

Tests for the pipeline implementation that don't require full system imports.
Focuses on testing the core pipeline components in isolation.

Reference: docs/PIPELINE_IMPLEMENTATION_GUIDE.md
"""

import pytest
import json
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, List
from unittest.mock import Mock, AsyncMock, patch

# Test the zone interface components directly
from src.data.pipeline.zone_interface import (
    ZoneType, 
    ZoneConfig, 
    DataRecord, 
    ProcessingResult, 
    ProcessingStatus,
    create_zone_config,
    validate_zone_progression,
    get_next_zone,
    ZoneMetrics
)


class TestZoneInterface:
    """Test zone interface and base classes."""
    
    def test_zone_type_enum(self):
        """Test ZoneType enumeration."""
        assert ZoneType.RAW == "raw"
        assert ZoneType.STAGING == "staging"
        assert ZoneType.CURATED == "curated"
        
    def test_processing_status_enum(self):
        """Test ProcessingStatus enumeration."""
        assert ProcessingStatus.PENDING == "pending"
        assert ProcessingStatus.IN_PROGRESS == "in_progress"
        assert ProcessingStatus.COMPLETED == "completed"
        assert ProcessingStatus.FAILED == "failed"
        assert ProcessingStatus.SKIPPED == "skipped"
    
    def test_zone_config_creation(self):
        """Test zone configuration creation and validation."""
        config = ZoneConfig(
            zone_type=ZoneType.RAW,
            schema_name="raw_data",
            batch_size=1000,
            validation_enabled=True,
            auto_promotion=True,
            quality_threshold=0.8
        )
        
        assert config.zone_type == ZoneType.RAW
        assert config.schema_name == "raw_data"
        assert config.batch_size == 1000
        assert config.validation_enabled is True
        assert config.auto_promotion is True
        assert config.quality_threshold == 0.8
        
    def test_zone_config_defaults(self):
        """Test zone configuration default values."""
        config = ZoneConfig(
            zone_type=ZoneType.STAGING,
            schema_name="staging"
        )
        
        assert config.enabled is True
        assert config.quality_threshold == 0.8
        assert config.batch_size == 1000
        assert config.retry_attempts == 3
        assert config.timeout_seconds == 300
        assert config.validation_enabled is True
        assert config.auto_promotion is True
        
    def test_zone_config_validation(self):
        """Test zone configuration validation."""
        # Valid configurations
        config1 = ZoneConfig(zone_type=ZoneType.RAW, schema_name="raw_data", quality_threshold=0.5)
        assert config1.quality_threshold == 0.5
        
        config2 = ZoneConfig(zone_type=ZoneType.RAW, schema_name="raw_data", quality_threshold=1.0)
        assert config2.quality_threshold == 1.0
        
        # Invalid quality threshold (outside 0.0-1.0 range)
        with pytest.raises(ValueError):
            ZoneConfig(zone_type=ZoneType.RAW, schema_name="raw_data", quality_threshold=1.5)
            
        with pytest.raises(ValueError):
            ZoneConfig(zone_type=ZoneType.RAW, schema_name="raw_data", quality_threshold=-0.1)
    
    def test_data_record_creation(self):
        """Test DataRecord model creation and validation."""
        record = DataRecord(
            external_id="test_123",
            source="action_network",
            raw_data={"game_id": "123", "odds": -110}
        )
        
        assert record.external_id == "test_123"
        assert record.source == "action_network"
        assert record.raw_data["game_id"] == "123"
        assert record.validation_status == ProcessingStatus.PENDING
        assert record.quality_score is None
        assert record.validation_errors is None
        assert record.processed_at is None
        
    def test_data_record_with_quality_score(self):
        """Test DataRecord with quality score validation."""
        record = DataRecord(
            external_id="test_456",
            source="sbd",
            quality_score=0.95
        )
        
        assert record.quality_score == 0.95
        
        # Test invalid quality scores
        with pytest.raises(ValueError):
            DataRecord(external_id="test", source="test", quality_score=1.5)
            
        with pytest.raises(ValueError):
            DataRecord(external_id="test", source="test", quality_score=-0.1)
    
    def test_processing_result_creation(self):
        """Test ProcessingResult model creation."""
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
        assert len(result.errors) == 0
        assert isinstance(result.metadata, dict)
        
    def test_zone_metrics(self):
        """Test ZoneMetrics dataclass."""
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


class TestZoneProgression:
    """Test zone progression and validation logic."""
    
    def test_valid_zone_progression(self):
        """Test valid zone progressions."""
        assert validate_zone_progression(ZoneType.RAW, ZoneType.STAGING) is True
        assert validate_zone_progression(ZoneType.STAGING, ZoneType.CURATED) is True
        
    def test_invalid_zone_progression(self):
        """Test invalid zone progressions."""
        # Skipping zones
        assert validate_zone_progression(ZoneType.RAW, ZoneType.CURATED) is False
        
        # Backward progression
        assert validate_zone_progression(ZoneType.STAGING, ZoneType.RAW) is False
        assert validate_zone_progression(ZoneType.CURATED, ZoneType.RAW) is False
        assert validate_zone_progression(ZoneType.CURATED, ZoneType.STAGING) is False
        
        # Same zone progression
        assert validate_zone_progression(ZoneType.RAW, ZoneType.RAW) is False
        assert validate_zone_progression(ZoneType.STAGING, ZoneType.STAGING) is False
        assert validate_zone_progression(ZoneType.CURATED, ZoneType.CURATED) is False
        
    def test_get_next_zone(self):
        """Test getting the next zone in progression."""
        assert get_next_zone(ZoneType.RAW) == ZoneType.STAGING
        assert get_next_zone(ZoneType.STAGING) == ZoneType.CURATED
        assert get_next_zone(ZoneType.CURATED) is None  # Final zone


class TestConfigurationHelpers:
    """Test configuration helper functions."""
    
    def test_create_zone_config(self):
        """Test zone configuration creation helper."""
        config = create_zone_config(
            ZoneType.RAW,
            "raw_data",
            batch_size=500,
            quality_threshold=0.7,
            validation_enabled=False
        )
        
        assert config.zone_type == ZoneType.RAW
        assert config.schema_name == "raw_data"
        assert config.batch_size == 500
        assert config.quality_threshold == 0.7
        assert config.validation_enabled is False
        
    def test_create_zone_config_defaults(self):
        """Test zone configuration creation with defaults."""
        config = create_zone_config(ZoneType.STAGING, "staging")
        
        assert config.zone_type == ZoneType.STAGING
        assert config.schema_name == "staging"
        assert config.batch_size == 1000  # default
        assert config.quality_threshold == 0.8  # default
        assert config.validation_enabled is True  # default


class TestDataValidation:
    """Test data validation and quality scoring."""
    
    def test_record_validation_with_errors(self):
        """Test record with validation errors."""
        record = DataRecord(
            external_id="test_error",
            source="test_source",
            validation_errors=["Missing required field", "Invalid format"]
        )
        
        assert len(record.validation_errors) == 2
        assert "Missing required field" in record.validation_errors
        assert "Invalid format" in record.validation_errors
        
    def test_record_quality_score_range(self):
        """Test quality score must be between 0 and 1."""
        # Valid quality scores
        record1 = DataRecord(external_id="test1", source="test", quality_score=0.0)
        assert record1.quality_score == 0.0
        
        record2 = DataRecord(external_id="test2", source="test", quality_score=1.0)
        assert record2.quality_score == 1.0
        
        record3 = DataRecord(external_id="test3", source="test", quality_score=0.5)
        assert record3.quality_score == 0.5


class TestProcessingResultAnalysis:
    """Test processing result analysis and calculations."""
    
    def test_processing_result_success_rate(self):
        """Test calculating success rate from processing results."""
        result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=1000,
            records_successful=850,
            records_failed=150
        )
        
        # Calculate success rate manually to verify
        success_rate = result.records_successful / result.records_processed
        assert success_rate == 0.85
        
        # Calculate error rate
        error_rate = result.records_failed / result.records_processed
        assert error_rate == 0.15
        
    def test_processing_result_with_errors(self):
        """Test processing result with error messages."""
        errors = [
            "Database connection failed",
            "Invalid JSON format in record 123",
            "Missing required field: external_id"
        ]
        
        result = ProcessingResult(
            status=ProcessingStatus.FAILED,
            records_processed=100,
            records_successful=0,
            records_failed=100,
            errors=errors
        )
        
        assert result.status == ProcessingStatus.FAILED
        assert len(result.errors) == 3
        assert "Database connection failed" in result.errors
        
    def test_processing_result_metadata(self):
        """Test processing result with metadata."""
        metadata = {
            "source": "action_network",
            "batch_id": "batch_123",
            "collection_timestamp": "2025-07-21T10:30:00Z",
            "api_response_time_ms": 450
        }
        
        result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=50,
            records_successful=48,
            records_failed=2,
            metadata=metadata
        )
        
        assert result.metadata["source"] == "action_network"
        assert result.metadata["batch_id"] == "batch_123"
        assert result.metadata["api_response_time_ms"] == 450


class TestPipelineConstants:
    """Test pipeline constants and enumerations."""
    
    def test_all_zone_types_defined(self):
        """Test that all expected zone types are defined."""
        zone_types = [ZoneType.RAW, ZoneType.STAGING, ZoneType.CURATED]
        assert len(zone_types) == 3
        
        # Test values
        assert "raw" in [zt.value for zt in zone_types]
        assert "staging" in [zt.value for zt in zone_types] 
        assert "curated" in [zt.value for zt in zone_types]
        
    def test_all_processing_statuses_defined(self):
        """Test that all expected processing statuses are defined."""
        statuses = [
            ProcessingStatus.PENDING,
            ProcessingStatus.IN_PROGRESS,
            ProcessingStatus.COMPLETED,
            ProcessingStatus.FAILED,
            ProcessingStatus.SKIPPED
        ]
        assert len(statuses) == 5
        
        # Test values
        status_values = [s.value for s in statuses]
        assert "pending" in status_values
        assert "in_progress" in status_values
        assert "completed" in status_values
        assert "failed" in status_values
        assert "skipped" in status_values


# Simple integration test for core components
class TestCoreIntegration:
    """Integration tests for core pipeline components."""
    
    def test_zone_config_to_metrics_workflow(self):
        """Test workflow from configuration to metrics."""
        # Create configuration
        config = create_zone_config(
            ZoneType.RAW,
            "raw_data",
            batch_size=100,
            quality_threshold=0.8
        )
        
        # Create some records
        records = [
            DataRecord(external_id=f"record_{i}", source="test", quality_score=0.9)
            for i in range(5)
        ]
        
        # Create processing result
        result = ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            records_processed=len(records),
            records_successful=len(records),
            records_failed=0,
            processing_time=2.5
        )
        
        # Verify the workflow
        assert config.batch_size >= len(records)  # Batch size sufficient
        assert all(r.quality_score >= config.quality_threshold for r in records)  # Quality check
        assert result.status == ProcessingStatus.COMPLETED  # Successful processing
        
    def test_progressive_zone_workflow(self):
        """Test progressive workflow through zones."""
        zones = [ZoneType.RAW, ZoneType.STAGING, ZoneType.CURATED]
        
        current_zone = zones[0]  # Start with RAW
        
        # Process through each zone
        for i, zone in enumerate(zones[1:], 1):
            # Validate progression is valid
            assert validate_zone_progression(current_zone, zone)
            
            # Get next zone
            next_zone = get_next_zone(current_zone)
            assert next_zone == zone
            
            current_zone = zone
        
        # Final zone should have no next zone
        assert get_next_zone(ZoneType.CURATED) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])