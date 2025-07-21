#!/usr/bin/env python3
"""
Test Pipeline Implementation - Phase 1

Tests for the RAW → STAGING → CURATED pipeline implementation.
Validates core functionality, configuration, and data flow.

Reference: docs/PIPELINE_IMPLEMENTATION_GUIDE.md
"""

import pytest
import json
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, List
from unittest.mock import Mock, AsyncMock, patch

# Pipeline components to test
from src.core.config import get_settings
from src.data.pipeline.zone_interface import (
    ZoneType, 
    ZoneConfig, 
    DataRecord, 
    ProcessingResult, 
    ProcessingStatus,
    create_zone_config,
    validate_zone_progression,
    get_next_zone
)
from src.data.pipeline.raw_zone import RawZoneProcessor, RawDataRecord
from src.data.pipeline.staging_zone import StagingZoneProcessor, StagingDataRecord
from src.data.pipeline.raw_zone_adapter import RawZoneAdapter
from src.data.pipeline.pipeline_orchestrator import (
    DataPipelineOrchestrator, 
    PipelineMode,
    PipelineExecution
)


class TestPipelineConfiguration:
    """Test pipeline configuration loading and validation."""
    
    def test_pipeline_settings_loading(self):
        """Test that pipeline settings load correctly from config.toml."""
        settings = get_settings()
        
        # Test schema settings
        assert hasattr(settings, 'schemas')
        assert settings.schemas.raw == "raw_data"
        assert settings.schemas.staging == "staging"
        assert settings.schemas.curated == "curated"
        
        # Test pipeline settings
        assert hasattr(settings, 'pipeline')
        assert settings.pipeline.enable_staging is True
        assert settings.pipeline.enable_curated is True
        assert settings.pipeline.auto_promotion is True
        assert settings.pipeline.validation_enabled is True
        assert settings.pipeline.quality_threshold == 0.8
        
        # Test zone settings
        assert hasattr(settings.pipeline, 'zones')
        assert settings.pipeline.zones.raw_enabled is True
        assert settings.pipeline.zones.staging_enabled is True
        
    def test_zone_config_creation(self):
        """Test zone configuration creation and validation."""
        config = create_zone_config(
            ZoneType.RAW,
            "raw_data",
            batch_size=1000,
            validation_enabled=True,
            auto_promotion=True
        )
        
        assert config.zone_type == ZoneType.RAW
        assert config.schema_name == "raw_data"
        assert config.batch_size == 1000
        assert config.validation_enabled is True
        assert config.auto_promotion is True
        assert config.quality_threshold == 0.8  # default
        
    def test_zone_progression_validation(self):
        """Test zone progression validation logic."""
        # Valid progressions
        assert validate_zone_progression(ZoneType.RAW, ZoneType.STAGING) is True
        assert validate_zone_progression(ZoneType.STAGING, ZoneType.CURATED) is True
        
        # Invalid progressions
        assert validate_zone_progression(ZoneType.RAW, ZoneType.CURATED) is False
        assert validate_zone_progression(ZoneType.STAGING, ZoneType.RAW) is False
        assert validate_zone_progression(ZoneType.CURATED, ZoneType.RAW) is False
        
        # Next zone logic
        assert get_next_zone(ZoneType.RAW) == ZoneType.STAGING
        assert get_next_zone(ZoneType.STAGING) == ZoneType.CURATED
        assert get_next_zone(ZoneType.CURATED) is None


class TestZoneInterface:
    """Test zone interface and base classes."""
    
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


class TestRawZoneProcessor:
    """Test RAW zone processor functionality."""
    
    @pytest.fixture
    def raw_config(self):
        """Create RAW zone configuration for testing."""
        return create_zone_config(
            ZoneType.RAW,
            "raw_data",
            batch_size=100,
            validation_enabled=True
        )
    
    @pytest.fixture
    def raw_processor(self, raw_config):
        """Create RAW zone processor for testing."""
        return RawZoneProcessor(raw_config)
    
    def test_raw_processor_initialization(self, raw_processor):
        """Test RAW zone processor initialization."""
        assert raw_processor.zone_type == ZoneType.RAW
        assert raw_processor.schema_name == "raw_data"
        assert raw_processor.config.validation_enabled is True
    
    @pytest.mark.asyncio
    async def test_raw_record_processing(self, raw_processor):
        """Test processing individual RAW records."""
        # Mock database connection
        with patch.object(raw_processor, 'get_connection') as mock_conn:
            mock_conn.return_value = Mock()
            
            # Create test record
            record = RawDataRecord(
                external_id="game_123",
                source="action_network",
                raw_data={
                    "game_id": "123",
                    "home_team": "Yankees",
                    "away_team": "Red Sox",
                    "sportsbook": {"name": "DraftKings", "id": 1},
                    "moneyline": {"home": -150, "away": 130}
                }
            )
            
            # Process record
            processed = await raw_processor.process_record(record)
            
            assert processed is not None
            assert processed.external_id == "game_123"
            assert processed.source == "action_network"
            assert processed.processed_at is not None
            assert processed.validation_status == ProcessingStatus.COMPLETED
            
            # Check metadata extraction
            assert processed.game_external_id == "123"
            assert processed.sportsbook_name == "DraftKings"
    
    @pytest.mark.asyncio
    async def test_raw_validation(self, raw_processor):
        """Test RAW zone validation logic."""
        # Valid record
        valid_record = DataRecord(
            external_id="test_123",
            source="action_network",
            raw_data={"valid": "data"}
        )
        
        is_valid = await raw_processor.validate_record_custom(valid_record)
        assert is_valid is True
        
        # Invalid record - no external_id or raw_data
        invalid_record = DataRecord(
            source="action_network"
        )
        
        is_valid = await raw_processor.validate_record_custom(invalid_record)
        assert is_valid is False
        assert "RAW record must have either external_id or raw_data" in invalid_record.validation_errors


class TestStagingZoneProcessor:
    """Test STAGING zone processor functionality."""
    
    @pytest.fixture
    def staging_config(self):
        """Create STAGING zone configuration for testing."""
        return create_zone_config(
            ZoneType.STAGING,
            "staging",
            batch_size=50,
            quality_threshold=0.7,
            validation_enabled=True
        )
    
    @pytest.fixture
    def staging_processor(self, staging_config):
        """Create STAGING zone processor for testing."""
        return StagingZoneProcessor(staging_config)
    
    def test_staging_processor_initialization(self, staging_processor):
        """Test STAGING zone processor initialization."""
        assert staging_processor.zone_type == ZoneType.STAGING
        assert staging_processor.schema_name == "staging"
        assert staging_processor.config.quality_threshold == 0.7
    
    @pytest.mark.asyncio
    async def test_staging_record_processing(self, staging_processor):
        """Test processing individual STAGING records."""
        # Mock database connection
        with patch.object(staging_processor, 'get_connection') as mock_conn:
            mock_conn.return_value = Mock()
            
            # Create test record with raw data
            record = StagingDataRecord(
                external_id="game_123",
                source="action_network",
                raw_data={
                    "home_team": "New York Yankees",
                    "away_team": "Boston Red Sox",
                    "sportsbook": {"name": "draftkings", "id": 1},
                    "moneyline": {"home": -150, "away": 130},
                    "spread": {"point": -1.5, "price": -110}
                }
            )
            
            # Process record
            processed = await staging_processor.process_record(record)
            
            assert processed is not None
            assert processed.external_id == "game_123"
            assert processed.home_team_normalized is not None  # Team names normalized
            assert processed.away_team_normalized is not None
            assert processed.sportsbook_name == "DraftKings"  # Sportsbook normalized
            assert processed.quality_score is not None
            assert 0 <= processed.quality_score <= 1
    
    def test_team_name_normalization(self, staging_processor):
        """Test team name normalization."""
        # This would test the actual team normalization logic
        # For now, just ensure the method exists
        assert hasattr(staging_processor, '_normalize_team_names')
    
    def test_sportsbook_normalization(self, staging_processor):
        """Test sportsbook name normalization."""
        assert staging_processor.sportsbook_mapping['draftkings'] == 'DraftKings'
        assert staging_processor.sportsbook_mapping['fanduel'] == 'FanDuel'
        assert staging_processor.sportsbook_mapping['betmgm'] == 'BetMGM'
    
    def test_numeric_field_cleaning(self, staging_processor):
        """Test numeric field cleaning methods."""
        # Test safe integer conversion
        assert staging_processor._safe_int_convert(-110) == -110
        assert staging_processor._safe_int_convert("-110") == -110
        assert staging_processor._safe_int_convert("invalid") is None
        
        # Test safe decimal conversion
        assert staging_processor._safe_decimal_convert(1.5) == Decimal('1.5')
        assert staging_processor._safe_decimal_convert("1.5") == Decimal('1.5')
        assert staging_processor._safe_decimal_convert("invalid") is None


class TestRawZoneAdapter:
    """Test RAW zone adapter functionality."""
    
    @pytest.fixture
    def raw_adapter(self):
        """Create RAW zone adapter for testing."""
        with patch('src.data.pipeline.raw_zone_adapter.get_settings') as mock_settings:
            mock_settings.return_value.schemas.raw = "raw_data"
            return RawZoneAdapter()
    
    @pytest.mark.asyncio
    async def test_action_network_games_storage(self, raw_adapter):
        """Test storing Action Network games through adapter."""
        # Mock the raw processor
        with patch.object(raw_adapter.raw_processor, 'process_batch') as mock_process:
            mock_process.return_value = ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                records_processed=2,
                records_successful=2,
                records_failed=0
            )
            
            games_data = [
                {
                    "id": "game_123",
                    "home_team": "Yankees",
                    "away_team": "Red Sox",
                    "game_date": "2025-07-21"
                },
                {
                    "id": "game_456", 
                    "home_team": "Dodgers",
                    "away_team": "Giants",
                    "game_date": "2025-07-21"
                }
            ]
            
            result = await raw_adapter.store_action_network_games(games_data)
            
            assert result.status == ProcessingStatus.COMPLETED
            assert result.records_successful == 2
            mock_process.assert_called_once()
            
            # Verify the records passed to processor
            call_args = mock_process.call_args[0][0]
            assert len(call_args) == 2
            assert call_args[0].external_id == "game_123"
            assert call_args[0].source == "action_network"
            assert call_args[0].data_type == "game"
    
    @pytest.mark.asyncio
    async def test_betting_lines_storage(self, raw_adapter):
        """Test storing generic betting lines through adapter."""
        with patch.object(raw_adapter.raw_processor, 'process_batch') as mock_process:
            mock_process.return_value = ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                records_processed=1,
                records_successful=1,
                records_failed=0
            )
            
            lines_data = [
                {
                    "id": "line_123",
                    "game_id": "game_456",
                    "sportsbook": "DraftKings",
                    "odds": -110,
                    "line": -1.5,
                    "game_date": "2025-07-21"
                }
            ]
            
            result = await raw_adapter.store_betting_lines(lines_data, "spread", "action_network")
            
            assert result.status == ProcessingStatus.COMPLETED
            assert result.records_successful == 1
            mock_process.assert_called_once()
            
            # Verify record structure
            call_args = mock_process.call_args[0][0]
            assert call_args[0].bet_type == "spread"
            assert call_args[0].source == "action_network"


class TestPipelineOrchestrator:
    """Test pipeline orchestrator coordination."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create pipeline orchestrator for testing."""
        with patch('src.data.pipeline.pipeline_orchestrator.get_settings') as mock_settings:
            settings = Mock()
            settings.pipeline.zones.raw_enabled = True
            settings.pipeline.zones.staging_enabled = True
            settings.pipeline.zones.curated_enabled = False
            settings.pipeline.validation_enabled = True
            settings.pipeline.auto_promotion = True
            settings.schemas.raw = "raw_data"
            settings.schemas.staging = "staging"
            settings.schemas.curated = "curated"
            mock_settings.return_value = settings
            
            return DataPipelineOrchestrator()
    
    def test_orchestrator_initialization(self, orchestrator):
        """Test pipeline orchestrator initialization."""
        assert ZoneType.RAW in orchestrator.zones
        assert ZoneType.STAGING in orchestrator.zones
        # CURATED zone should not be initialized (disabled in fixture)
        assert ZoneType.CURATED not in orchestrator.zones
    
    @pytest.mark.asyncio
    async def test_zone_processing(self, orchestrator):
        """Test processing records through a specific zone."""
        # Mock the RAW zone processor
        with patch.object(orchestrator.zones[ZoneType.RAW], 'process_batch') as mock_process:
            mock_process.return_value = ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                records_processed=3,
                records_successful=3,
                records_failed=0,
                processing_time=1.5
            )
            
            records = [
                DataRecord(external_id="1", source="test"),
                DataRecord(external_id="2", source="test"),
                DataRecord(external_id="3", source="test")
            ]
            
            result = await orchestrator._process_zone(ZoneType.RAW, records)
            
            assert result.status == ProcessingStatus.COMPLETED
            assert result.records_processed == 3
            assert result.records_successful == 3
            assert result.processing_time == 1.5
    
    @pytest.mark.asyncio
    async def test_health_check(self, orchestrator):
        """Test zone health checking."""
        # Mock health checks for zones
        with patch.object(orchestrator.zones[ZoneType.RAW], 'health_check') as mock_raw_health:
            with patch.object(orchestrator.zones[ZoneType.STAGING], 'health_check') as mock_staging_health:
                mock_raw_health.return_value = {"status": "healthy", "zone_type": "raw"}
                mock_staging_health.return_value = {"status": "healthy", "zone_type": "staging"}
                
                health = await orchestrator.get_zone_health()
                
                assert ZoneType.RAW in health
                assert ZoneType.STAGING in health
                assert health[ZoneType.RAW]["status"] == "healthy"
                assert health[ZoneType.STAGING]["status"] == "healthy"


class TestCLIIntegration:
    """Test CLI pipeline commands."""
    
    def test_cli_command_import(self):
        """Test that CLI commands can be imported without errors."""
        try:
            from src.interfaces.cli.commands.pipeline import pipeline_group
            assert pipeline_group is not None
        except ImportError as e:
            pytest.fail(f"Failed to import pipeline CLI commands: {e}")
    
    def test_cli_command_structure(self):
        """Test CLI command structure and help text."""
        from src.interfaces.cli.commands.pipeline import pipeline_group
        
        # Check that main command group exists
        assert pipeline_group.name == "pipeline"
        
        # Check that subcommands exist
        subcommands = [cmd.name for cmd in pipeline_group.commands.values()]
        assert "run" in subcommands
        assert "status" in subcommands
        assert "migrate" in subcommands


# Integration test that combines multiple components
class TestPipelineIntegration:
    """Integration tests for pipeline components working together."""
    
    @pytest.mark.asyncio
    async def test_raw_to_staging_flow(self):
        """Test data flow from RAW to STAGING zone."""
        # Create configurations
        raw_config = create_zone_config(ZoneType.RAW, "raw_data")
        staging_config = create_zone_config(ZoneType.STAGING, "staging")
        
        # Create processors
        raw_processor = RawZoneProcessor(raw_config)
        staging_processor = StagingZoneProcessor(staging_config)
        
        # Mock database connections
        with patch.object(raw_processor, 'get_connection') as mock_raw_conn:
            with patch.object(staging_processor, 'get_connection') as mock_staging_conn:
                mock_raw_conn.return_value = Mock()
                mock_staging_conn.return_value = Mock()
                
                # Create test record
                raw_record = RawDataRecord(
                    external_id="integration_test",
                    source="action_network",
                    raw_data={
                        "game_id": "test_game",
                        "home_team": "Yankees", 
                        "away_team": "Red Sox",
                        "sportsbook": {"name": "draftkings"},
                        "moneyline": {"home": -150, "away": 130}
                    }
                )
                
                # Process through RAW zone
                raw_processed = await raw_processor.process_record(raw_record)
                assert raw_processed is not None
                assert raw_processed.validation_status == ProcessingStatus.COMPLETED
                
                # Convert to STAGING record and process
                staging_record = StagingDataRecord(**raw_processed.model_dump())
                staging_processed = await staging_processor.process_record(staging_record)
                
                assert staging_processed is not None
                assert staging_processed.quality_score is not None
                assert staging_processed.sportsbook_name == "DraftKings"  # Normalized


def run_pipeline_tests():
    """Run all pipeline tests and return results."""
    import subprocess
    import sys
    
    # Run tests with verbose output
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        "tests/test_pipeline_implementation.py", 
        "-v", "--tb=short"
    ], capture_output=True, text=True)
    
    return result.returncode, result.stdout, result.stderr


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])