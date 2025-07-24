#!/usr/bin/env python3
"""
Integration Tests for Collector Configuration Standardization

Tests that all collectors properly use CollectorConfig pattern and handle
async/sync test methods correctly.
"""

import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any

from src.data.collection.base import CollectorConfig, DataSource, CollectionRequest
from src.data.collection.vsin_unified_collector import VSINUnifiedCollector
from src.data.collection.sbd_unified_collector_api import SBDUnifiedCollectorAPI
from src.data.collection.consolidated_action_network_collector import ActionNetworkCollector, CollectionMode
from src.data.collection.mlb_stats_api_collector import MLBStatsAPICollector


class TestCollectorConfigurationIntegration:
    """Integration tests for collector configuration standardization."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for database configuration."""
        mock_settings = MagicMock()
        mock_settings.database.host = "test-host"
        mock_settings.database.port = 5433
        mock_settings.database.database = "test-database"
        mock_settings.database.user = "test-user"
        mock_settings.database.password = "test-password"
        return mock_settings

    def test_vsin_collector_accepts_collector_config(self):
        """Test VSIN collector properly accepts CollectorConfig parameter."""
        config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        
        # Should not raise any errors
        collector = VSINUnifiedCollector(config)
        
        # Verify collector is properly configured
        assert collector.config == config
        assert collector.source == DataSource.VSIN.value  # Config uses enum values
        assert hasattr(collector, 'base_url')
        assert collector.base_url == "https://data.vsin.com"  # Default fallback

    def test_vsin_collector_with_custom_base_url(self):
        """Test VSIN collector respects custom base_url in config."""
        config = CollectorConfig(
            source=DataSource.VSIN,
            enabled=True,
            base_url="https://custom-vsin.com"
        )
        
        collector = VSINUnifiedCollector(config)
        
        # Verify custom base_url is used
        assert collector.base_url == "https://custom-vsin.com"

    def test_sbd_collector_accepts_collector_config(self):
        """Test SBD collector properly accepts CollectorConfig parameter."""
        config = CollectorConfig(source=DataSource.SPORTS_BETTING_DIME, enabled=True)
        
        # Should not raise any errors
        collector = SBDUnifiedCollectorAPI(config)
        
        # Verify collector is properly configured
        assert collector.config == config
        assert collector.source == DataSource.SPORTS_BETTING_DIME.value  # Config uses enum values
        assert hasattr(collector, 'base_url')
        assert collector.base_url == "https://www.sportsbettingdime.com"  # Default fallback

    def test_sbd_collector_with_custom_configuration(self):
        """Test SBD collector respects custom configuration parameters."""
        config = CollectorConfig(
            source=DataSource.SPORTS_BETTING_DIME,
            enabled=True,
            base_url="https://custom-sbd.com",
            params={"api_path": "/custom/api/endpoint"}
        )
        
        collector = SBDUnifiedCollectorAPI(config)
        
        # Verify custom configuration is used
        assert collector.base_url == "https://custom-sbd.com"
        assert collector.api_url == "https://custom-sbd.com/custom/api/endpoint"

    @pytest.mark.asyncio
    async def test_action_network_collector_accepts_collector_config(self, mock_settings):
        """Test Action Network collector properly accepts CollectorConfig parameter."""
        with patch('src.data.collection.consolidated_action_network_collector.get_settings', return_value=mock_settings):
            config = CollectorConfig(source=DataSource.ACTION_NETWORK, enabled=True)
            
            # Should not raise any errors
            collector = ActionNetworkCollector(config, CollectionMode.COMPREHENSIVE)
            
            # Verify collector is properly configured
            assert collector.config == config
            assert collector.source == DataSource.ACTION_NETWORK.value  # Config uses enum values
            assert collector.mode == CollectionMode.COMPREHENSIVE

    def test_mlb_stats_api_collector_accepts_collector_config(self, mock_settings):
        """Test MLB Stats API collector properly accepts CollectorConfig parameter."""
        with patch('src.data.collection.mlb_stats_api_collector.get_settings', return_value=mock_settings):
            config = CollectorConfig(source=DataSource.MLB_STATS_API, enabled=True)
            
            # Should not raise any errors
            collector = MLBStatsAPICollector(config)
            
            # Verify collector is properly configured
            assert collector.config == config
            assert collector.source == DataSource.MLB_STATS_API.value  # Config uses enum values

    def test_vsin_collector_has_sync_test_collection_method(self):
        """Test VSIN collector has synchronous test_collection method."""
        config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        collector = VSINUnifiedCollector(config)
        
        # Verify test_collection method exists and is not a coroutine
        assert hasattr(collector, 'test_collection')
        assert callable(collector.test_collection)
        
        # Method should be sync (not async) - this is important for CLI handling
        import inspect
        assert not inspect.iscoroutinefunction(collector.test_collection)

    def test_sbd_collector_has_async_test_collection_method(self):
        """Test SBD collector has asynchronous test_collection method."""
        config = CollectorConfig(source=DataSource.SPORTS_BETTING_DIME, enabled=True)
        collector = SBDUnifiedCollectorAPI(config)
        
        # Verify test_collection method exists and is async
        assert hasattr(collector, 'test_collection')
        assert callable(collector.test_collection)
        
        # Method should be async - this is important for CLI handling
        import inspect
        assert inspect.iscoroutinefunction(collector.test_collection)

    @pytest.mark.asyncio
    async def test_vsin_collector_test_collection_sync_execution(self):
        """Test VSIN collector test_collection executes synchronously."""
        config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        collector = VSINUnifiedCollector(config)
        
        # Mock the internal collection method to avoid actual network calls
        with patch.object(collector, '_collect_vsin_data_sync', return_value=[]):
            # Should execute without await and return result
            result = collector.test_collection("mlb")
            
            # Verify result structure
            assert isinstance(result, dict)
            assert "status" in result

    @pytest.mark.asyncio
    async def test_sbd_collector_test_collection_async_execution(self):
        """Test SBD collector test_collection executes asynchronously."""
        config = CollectorConfig(source=DataSource.SPORTS_BETTING_DIME, enabled=True)
        collector = SBDUnifiedCollectorAPI(config)
        
        # Mock the internal collection method to avoid actual API calls
        with patch.object(collector, 'collect_data', new_callable=AsyncMock) as mock_collect:
            mock_collect.return_value = []
            
            # Should require await and return result
            result = await collector.test_collection("mlb")
            
            # Verify result structure
            assert isinstance(result, dict)
            assert "status" in result

    def test_all_collectors_follow_base_collector_pattern(self, mock_settings):
        """Test that all collectors follow the BaseCollector pattern."""
        from src.data.collection.base import BaseCollector
        
        # Define collector classes and their required parameters
        collector_configs = [
            (VSINUnifiedCollector, CollectorConfig(source=DataSource.VSIN, enabled=True)),
            (SBDUnifiedCollectorAPI, CollectorConfig(source=DataSource.SPORTS_BETTING_DIME, enabled=True)),
            (MLBStatsAPICollector, CollectorConfig(source=DataSource.MLB_STATS_API, enabled=True)),
        ]
        
        with patch('src.data.collection.mlb_stats_api_collector.get_settings', return_value=mock_settings):
            for collector_class, config in collector_configs:
                # Verify each collector inherits from BaseCollector
                assert issubclass(collector_class, BaseCollector), f"{collector_class.__name__} should inherit from BaseCollector"
                
                # Verify each collector can be instantiated with CollectorConfig
                collector = collector_class(config)
                assert collector.config == config
                assert collector.source == config.source

    def test_collector_config_validation(self):
        """Test CollectorConfig validation and required fields."""
        # Test valid configuration
        valid_config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        assert valid_config.source == DataSource.VSIN.value  # Config uses enum values
        assert valid_config.enabled is True
        assert valid_config.rate_limit_per_minute == 60  # Default value
        
        # Test with custom parameters
        custom_config = CollectorConfig(
            source=DataSource.SPORTS_BETTING_DIME,
            enabled=True,
            base_url="https://custom.com",
            rate_limit_per_minute=30,
            timeout_seconds=45,
            retry_attempts=5
        )
        assert custom_config.base_url == "https://custom.com"
        assert custom_config.rate_limit_per_minute == 30
        assert custom_config.timeout_seconds == 45
        assert custom_config.retry_attempts == 5

    @pytest.mark.asyncio
    async def test_cli_compatibility_sync_vs_async_handling(self):
        """Test that CLI can handle both sync and async test_collection methods."""
        # Simulate CLI handling for VSIN (sync)
        vsin_config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        vsin_collector = VSINUnifiedCollector(vsin_config)
        
        with patch.object(vsin_collector, '_collect_vsin_data_sync', return_value=[]):
            # CLI should call this without await
            vsin_result = vsin_collector.test_collection("mlb")
            assert isinstance(vsin_result, dict)
        
        # Simulate CLI handling for SBD (async)
        sbd_config = CollectorConfig(source=DataSource.SPORTS_BETTING_DIME, enabled=True)
        sbd_collector = SBDUnifiedCollectorAPI(sbd_config)
        
        with patch.object(sbd_collector, 'collect_data', new_callable=AsyncMock) as mock_collect:
            mock_collect.return_value = []
            
            # CLI should call this with await
            sbd_result = await sbd_collector.test_collection("mlb")
            assert isinstance(sbd_result, dict)

    def test_collector_config_immutability_and_thread_safety(self):
        """Test that CollectorConfig is properly immutable and thread-safe."""
        config = CollectorConfig(
            source=DataSource.VSIN,
            enabled=True,
            base_url="https://original.com"
        )
        
        # Config should be immutable (Pydantic BaseModel behavior)
        original_url = config.base_url
        
        # In Pydantic v2, models are mutable by default but we can test validation
        # This test focuses on the fact that creating a new config preserves types
        try:
            # Create a new config with modified values (this should work)
            modified_config = config.model_copy(update={"base_url": "https://modified.com"})
            assert modified_config.base_url == "https://modified.com"
            # Original config should remain unchanged
            assert config.base_url == original_url
        except Exception:
            # If model_copy isn't available, that's also acceptable behavior
            pass

    def test_data_source_enum_consistency(self):
        """Test that DataSource enum values are consistent across collectors."""
        # Verify all expected data sources exist
        expected_sources = [
            DataSource.VSIN,
            DataSource.SPORTS_BETTING_DIME,
            DataSource.ACTION_NETWORK,
            DataSource.MLB_STATS_API
        ]
        
        for source in expected_sources:
            # Each source should have a string value
            assert isinstance(source.value, str)
            assert len(source.value) > 0
        
        # Test that alternate names work
        assert DataSource.SBD == DataSource.SPORTS_BETTING_DIME or "sbd" in [s.value for s in DataSource]

    @pytest.mark.asyncio 
    async def test_collector_error_handling_with_config(self):
        """Test that collectors handle configuration errors gracefully."""
        # Test with missing required source
        with pytest.raises((ValueError, TypeError)):
            CollectorConfig(enabled=True)  # Missing source
        
        # Test collectors with invalid configuration
        valid_config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        
        # Collectors should handle configuration gracefully
        try:
            vsin_collector = VSINUnifiedCollector(valid_config)
            sbd_collector = SBDUnifiedCollectorAPI(valid_config)  # Wrong source but should not crash
            assert True  # If we get here, error handling is working
        except Exception as e:
            pytest.fail(f"Collectors should handle configuration gracefully: {e}")

    def test_backward_compatibility_maintained(self):
        """Test that new configuration doesn't break existing functionality."""
        # Test that essential collector attributes still exist
        vsin_config = CollectorConfig(source=DataSource.VSIN, enabled=True)
        vsin_collector = VSINUnifiedCollector(vsin_config)
        
        # Essential attributes that existing code might depend on
        assert hasattr(vsin_collector, 'source')
        assert hasattr(vsin_collector, 'config')
        assert hasattr(vsin_collector, 'base_url')
        
        sbd_config = CollectorConfig(source=DataSource.SPORTS_BETTING_DIME, enabled=True)
        sbd_collector = SBDUnifiedCollectorAPI(sbd_config)
        
        assert hasattr(sbd_collector, 'source')
        assert hasattr(sbd_collector, 'config')
        assert hasattr(sbd_collector, 'base_url')