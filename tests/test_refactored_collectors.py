#!/usr/bin/env python3
"""
Test Suite for Refactored Data Collectors

This test suite validates all the changes mentioned in DATA_COLLECTION_REFACTORING_SUMMARY.md
to ensure the refactored collectors work properly with BaseCollector inheritance,
Pydantic models, and standardized interfaces.
"""

import asyncio
import pytest
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import Mock, patch, AsyncMock

# Import the base classes and models
from src.data.collection.base import (
    BaseCollector,
    CollectorConfig,
    CollectionRequest,
    CollectionResult,
    DataSource,
    CollectorFactory
)

# Import refactored collectors
from src.data.collection.sbd_unified_collector_api import SBDUnifiedCollectorAPI
from src.data.collection.vsin_unified_collector import VSINUnifiedCollector
from src.data.collection.consolidated_action_network_collector import ActionNetworkCollector

# Import migration helpers
from src.data.collection.migration_helper import (
    create_collector_config,
    create_collection_request,
    DeprecatedCollectorWrapper
)


class TestBaseCollectorInheritance:
    """Test that all refactored collectors properly inherit from BaseCollector."""
    
    def test_sbd_collector_inherits_from_base(self):
        """Test SBD collector inherits from BaseCollector."""
        config = CollectorConfig(source=DataSource.SBD)
        collector = SBDUnifiedCollectorAPI(config)
        
        assert isinstance(collector, BaseCollector)
        assert collector.source == DataSource.SBD
        assert hasattr(collector, 'collect_data')
        assert hasattr(collector, 'validate_record')
        assert hasattr(collector, 'normalize_record')
    
    def test_vsin_collector_inherits_from_base(self):
        """Test VSIN collector inherits from BaseCollector."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = VSINUnifiedCollector(config)
        
        assert isinstance(collector, BaseCollector)
        assert collector.source == DataSource.VSIN
        assert hasattr(collector, 'collect_data')
        assert hasattr(collector, 'validate_record')
        assert hasattr(collector, 'normalize_record')
    
    def test_action_network_collector_inherits_from_base(self):
        """Test Action Network collector inherits from BaseCollector."""
        config = CollectorConfig(source=DataSource.ACTION_NETWORK)
        collector = ActionNetworkCollector(config)
        
        assert isinstance(collector, BaseCollector)
        assert collector.source == DataSource.ACTION_NETWORK
        assert hasattr(collector, 'collect_data')
        assert hasattr(collector, 'validate_record')
        assert hasattr(collector, 'normalize_record')


class TestCollectorConfigUsage:
    """Test that all collectors properly use CollectorConfig."""
    
    def test_sbd_collector_uses_config(self):
        """Test SBD collector properly uses CollectorConfig."""
        config = CollectorConfig(
            source=DataSource.SBD,
            base_url="https://test.sportsbettingdime.com",
            rate_limit_per_minute=30,
            timeout_seconds=15,
            params={"api_path": "/test-api"}
        )
        
        collector = SBDUnifiedCollectorAPI(config)
        
        assert collector.config == config
        assert collector.base_url == "https://test.sportsbettingdime.com"
        assert "test-api" in collector.api_url
    
    def test_vsin_collector_uses_config(self):
        """Test VSIN collector properly uses CollectorConfig."""
        config = CollectorConfig(
            source=DataSource.VSIN,
            base_url="https://test.vsin.com",
            rate_limit_per_minute=20,
            timeout_seconds=45
        )
        
        collector = VSINUnifiedCollector(config)
        
        assert collector.config == config
        assert collector.base_url == "https://test.vsin.com"
    
    def test_action_network_collector_uses_config(self):
        """Test Action Network collector properly uses CollectorConfig."""
        config = CollectorConfig(
            source=DataSource.ACTION_NETWORK,
            base_url="https://test.actionnetwork.com",
            rate_limit_per_minute=120
        )
        
        collector = ActionNetworkCollector(config)
        
        assert collector.config == config
        assert collector.source == DataSource.ACTION_NETWORK


class TestCollectionRequestInterface:
    """Test that all collectors properly handle CollectionRequest."""
    
    @pytest.mark.asyncio
    async def test_sbd_collector_handles_collection_request(self):
        """Test SBD collector processes CollectionRequest properly."""
        config = CollectorConfig(source=DataSource.SBD)
        collector = SBDUnifiedCollectorAPI(config)
        
        request = CollectionRequest(
            source=DataSource.SBD,
            sport="mlb",
            dry_run=True,
            additional_params={"sport": "mlb", "test": True}
        )
        
        # Mock the HTTP session and API response
        with patch.object(collector, 'session') as mock_session:
            mock_response = AsyncMock()
            mock_response.json.return_value = {"data": []}
            mock_response.raise_for_status.return_value = None
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Test that collect_data accepts CollectionRequest
            result = await collector.collect_data(request)
            assert isinstance(result, list)
    
    @pytest.mark.asyncio 
    async def test_vsin_collector_handles_collection_request(self):
        """Test VSIN collector processes CollectionRequest properly."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = VSINUnifiedCollector(config)
        
        request = CollectionRequest(
            source=DataSource.VSIN,
            sport="mlb",
            dry_run=True,
            additional_params={"sport": "mlb", "sportsbook": "dk"}
        )
        
        # Mock the internal collection method
        with patch.object(collector, '_collect_vsin_data_sync') as mock_collect:
            mock_collect.return_value = []
            
            result = await collector.collect_data(request)
            assert isinstance(result, list)
            mock_collect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_action_network_collector_handles_collection_request(self):
        """Test Action Network collector processes CollectionRequest properly.""" 
        config = CollectorConfig(source=DataSource.ACTION_NETWORK)
        collector = ActionNetworkCollector(config)
        
        request = CollectionRequest(
            source=DataSource.ACTION_NETWORK,
            start_date=datetime.now(),
            sport="mlb",
            dry_run=True
        )
        
        # Mock the client
        with patch.object(collector.client, 'fetch_games') as mock_fetch:
            mock_fetch.return_value = []
            
            result = await collector.collect_data(request)
            assert isinstance(result, list)


class TestValidationAndNormalization:
    """Test validation and normalization methods for all collectors."""
    
    def test_sbd_collector_validation(self):
        """Test SBD collector record validation."""
        config = CollectorConfig(source=DataSource.SBD)
        collector = SBDUnifiedCollectorAPI(config)
        
        # Valid record
        valid_record = {
            "external_game_id": "sbd_123",
            "game_name": "Yankees @ Red Sox",
            "away_team": "Yankees",
            "home_team": "Red Sox", 
            "game_datetime": "2025-01-10T19:00:00",
            "betting_records": [
                {"sportsbook": "DraftKings", "bet_type": "moneyline", "timestamp": "2025-01-10T18:00:00"}
            ]
        }
        
        assert collector.validate_record(valid_record) == True
        
        # Invalid record (missing required fields)
        invalid_record = {
            "game_name": "Yankees @ Red Sox"
        }
        
        assert collector.validate_record(invalid_record) == False
    
    def test_sbd_collector_normalization(self):
        """Test SBD collector record normalization."""
        config = CollectorConfig(source=DataSource.SBD)
        collector = SBDUnifiedCollectorAPI(config)
        
        record = {
            "external_game_id": "sbd_123",
            "game_name": "Yankees @ Red Sox",
            "away_team": "Yankees",
            "home_team": "Red Sox",
            "betting_records": [],
            "betting_splits": {"moneyline": {}}
        }
        
        normalized = collector.normalize_record(record)
        
        assert normalized["source"] == DataSource.SBD.value
        assert "collected_at_est" in normalized
        assert "collector_version" in normalized
        assert normalized["has_betting_splits"] == True
        assert "betting_record_count" in normalized
        assert "sportsbook_count" in normalized
    
    def test_vsin_collector_validation(self):
        """Test VSIN collector record validation."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = VSINUnifiedCollector(config)
        
        # Valid record
        valid_record = {
            "teams": "Yankees @ Red Sox",
            "data_source": "vsin",
            "timestamp": "2025-01-10T18:00:00",
            "moneyline_handle": "65%"
        }
        
        assert collector.validate_record(valid_record) == True
        
        # Invalid record (no betting metrics)
        invalid_record = {
            "teams": "Yankees @ Red Sox",
            "data_source": "vsin",
            "timestamp": "2025-01-10T18:00:00"
        }
        
        assert collector.validate_record(invalid_record) == False
    
    def test_vsin_collector_normalization(self):
        """Test VSIN collector record normalization."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = VSINUnifiedCollector(config)
        
        record = {
            "teams": "Yankees @ Red Sox",
            "data_source": "vsin",
            "timestamp": "2025-01-10T18:00:00",
            "moneyline_handle": "65%",
            "total_bets": "45%"
        }
        
        normalized = collector.normalize_record(record)
        
        assert normalized["source"] == DataSource.VSIN.value
        assert "collected_at_est" in normalized
        assert "collector_version" in normalized
        assert normalized["away_team"] == "Yankees"
        assert normalized["home_team"] == "Red Sox"
        assert normalized["has_moneyline_data"] == True
        assert normalized["has_total_data"] == True
        assert "data_completeness_score" in normalized
    
    def test_action_network_collector_validation(self):
        """Test Action Network collector record validation."""
        config = CollectorConfig(source=DataSource.ACTION_NETWORK)
        collector = ActionNetworkCollector(config)
        
        # Valid record
        valid_record = {
            "id": 12345,
            "teams": ["Yankees", "Red Sox"],
            "start_time": "2025-01-10T19:00:00Z",
            "markets": {"moneyline": {}}
        }
        
        assert collector.validate_record(valid_record) == True
        
        # Invalid record (missing markets)
        invalid_record = {
            "id": 12345,
            "teams": ["Yankees", "Red Sox"],
            "start_time": "2025-01-10T19:00:00Z"
        }
        
        assert collector.validate_record(invalid_record) == False
    
    def test_action_network_collector_normalization(self):
        """Test Action Network collector record normalization."""
        config = CollectorConfig(source=DataSource.ACTION_NETWORK)
        collector = ActionNetworkCollector(config)
        
        record = {
            "id": 12345,
            "teams": ["Yankees", "Red Sox"],
            "start_time": "2025-01-10T19:00:00Z",
            "markets": {"moneyline": {}},
            "public_betting": {"moneyline": {}}
        }
        
        normalized = collector.normalize_record(record)
        
        assert normalized["source"] == DataSource.ACTION_NETWORK.value
        assert "collected_at_est" in normalized
        assert "collector_version" in normalized
        assert normalized["has_teams"] == True
        assert normalized["has_markets"] == True
        assert normalized["has_public_betting"] == True


class TestFactoryRegistration:
    """Test that factory registration works with refactored collectors."""
    
    def test_sbd_factory_registration(self):
        """Test SBD collector can be created through factory."""
        config = CollectorConfig(source=DataSource.SBD)
        collector = CollectorFactory.create_collector(config)
        
        # Should return the refactored SBD collector or fallback
        assert isinstance(collector, BaseCollector)
        assert collector.source == DataSource.SBD
    
    def test_vsin_factory_registration(self):
        """Test VSIN collector can be created through factory."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = CollectorFactory.create_collector(config)
        
        # Should return the refactored VSIN collector or fallback
        assert isinstance(collector, BaseCollector)
        assert collector.source == DataSource.VSIN
    
    def test_action_network_factory_registration(self):
        """Test Action Network collector can be created through factory."""
        config = CollectorConfig(source=DataSource.ACTION_NETWORK)
        collector = CollectorFactory.create_collector(config)
        
        # Should return the refactored Action Network collector or fallback
        assert isinstance(collector, BaseCollector)
        assert collector.source == DataSource.ACTION_NETWORK


class TestMigrationHelper:
    """Test migration helper utilities."""
    
    def test_create_collector_config(self):
        """Test create_collector_config helper."""
        config = create_collector_config(
            DataSource.SBD,
            base_url="https://test.com",
            rate_limit_per_minute=30
        )
        
        assert isinstance(config, CollectorConfig)
        assert config.source == DataSource.SBD
        assert config.base_url == "https://test.com"
        assert config.rate_limit_per_minute == 30
        assert "api_path" in config.params  # SBD-specific default
    
    def test_create_collection_request(self):
        """Test create_collection_request helper."""
        request = create_collection_request(
            DataSource.VSIN,
            sport="nfl",
            dry_run=True,
            sportsbook="dk"
        )
        
        assert isinstance(request, CollectionRequest)
        assert request.source == DataSource.VSIN
        assert request.sport == "nfl"
        assert request.dry_run == True
        assert request.additional_params["sportsbook"] == "dk"
    
    def test_deprecated_collector_wrapper(self):
        """Test backward compatibility wrapper.""" 
        wrapper = DeprecatedCollectorWrapper(DataSource.SBD)
        
        assert hasattr(wrapper, 'collect_raw_data')
        assert hasattr(wrapper, 'collect_game_data')
        assert hasattr(wrapper, 'test_collection')
        
        # These should issue deprecation warnings
        with pytest.warns(DeprecationWarning):
            # Mock the async call to avoid actual HTTP requests
            with patch.object(wrapper.collector, 'collect_data') as mock_collect:
                mock_collect.return_value = []
                result = wrapper.collect_raw_data("mlb")
                assert isinstance(result, list)


class TestAsyncContextManager:
    """Test that all collectors support async context manager."""
    
    @pytest.mark.asyncio
    async def test_sbd_collector_async_context(self):
        """Test SBD collector async context manager."""
        config = CollectorConfig(source=DataSource.SBD)
        collector = SBDUnifiedCollectorAPI(config)
        
        async with collector:
            assert collector.session is not None
        
        # Session should be cleaned up
        assert collector.session is None
    
    @pytest.mark.asyncio
    async def test_vsin_collector_async_context(self):
        """Test VSIN collector async context manager."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = VSINUnifiedCollector(config)
        
        async with collector:
            # Should initialize properly
            assert collector.session is not None
    
    @pytest.mark.asyncio
    async def test_action_network_collector_async_context(self):
        """Test Action Network collector async context manager."""
        config = CollectorConfig(source=DataSource.ACTION_NETWORK)
        collector = ActionNetworkCollector(config)
        
        async with collector:
            # Should initialize properly
            assert collector.client is not None


class TestIntegrationScenarios:
    """Integration tests that validate end-to-end scenarios."""
    
    @pytest.mark.asyncio
    async def test_full_collection_workflow_sbd(self):
        """Test complete SBD collection workflow."""
        config = create_collector_config(DataSource.SBD)
        collector = CollectorFactory.create_collector(config)
        
        request = create_collection_request(
            DataSource.SBD,
            sport="mlb",
            dry_run=True
        )
        
        async with collector:
            # Test connection
            connection_ok = await collector.test_connection()
            assert isinstance(connection_ok, bool)
            
            # Collect data (with mocking to avoid real HTTP calls)
            with patch.object(collector, '_collect_with_api') as mock_collect:
                mock_collect.return_value = [
                    {
                        "external_game_id": "test_123",
                        "game_name": "Test Game",
                        "away_team": "Team A",
                        "home_team": "Team B", 
                        "game_datetime": "2025-01-10T19:00:00",
                        "betting_records": [{"sportsbook": "test", "bet_type": "moneyline", "timestamp": "2025-01-10"}]
                    }
                ]
                
                data = await collector.collect_data(request)
                assert len(data) == 1
                
                # Test validation and normalization
                record = data[0]
                assert collector.validate_record(record) == True
                
                normalized = collector.normalize_record(record)
                assert normalized["source"] == DataSource.SBD.value
                assert "collected_at_est" in normalized
    
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self):
        """Test error handling in refactored collectors."""
        config = CollectorConfig(source=DataSource.VSIN)
        collector = CollectorFactory.create_collector(config)
        
        request = CollectionRequest(
            source=DataSource.VSIN,
            sport="mlb",
            dry_run=True
        )
        
        async with collector:
            # Test that exceptions are properly handled
            with patch.object(collector, '_collect_vsin_data_sync') as mock_collect:
                mock_collect.side_effect = Exception("Test error")
                
                # Should not raise exception, but handle gracefully
                with pytest.raises(Exception):
                    await collector.collect_data(request)


# Test runner function
def run_refactoring_tests():
    """Run all refactoring validation tests."""
    import subprocess
    import sys
    
    test_file = __file__
    
    # Run pytest with verbose output
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        test_file,
        "-v",
        "--tb=short",
        "--durations=10"
    ], capture_output=True, text=True)
    
    print("=== REFACTORING VALIDATION TEST RESULTS ===")
    print(result.stdout)
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    return result.returncode == 0


if __name__ == "__main__":
    success = run_refactoring_tests()
    if success:
        print("✅ All refactoring tests passed!")
    else:
        print("❌ Some refactoring tests failed!")
    
    exit(0 if success else 1)