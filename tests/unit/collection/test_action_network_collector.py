"""
Unit tests for Action Network collector.

Tests collector logic without external API dependencies using mocks.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any

from src.data.collection.base import CollectorConfig, CollectionRequest
from tests.mocks.collectors import MockActionNetworkCollector, CollectorMockFactory
from tests.mocks.external_apis import ActionNetworkMock
from tests.fixtures.api_responses import ActionNetworkFixtures
from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging


class TestActionNetworkCollector:
    """Test Action Network collector functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging(log_level="INFO", include_sanitization=True)
        self.logger = create_test_logger("action_network_collector_test")
        
        # Create mock collector with test configuration
        self.config = CollectorConfig(
            source_name="action_network",
            rate_limit_requests=100,
            rate_limit_period=3600,
            timeout_seconds=30
        )
        
        self.collector = CollectorMockFactory.create_action_network_mock(self.config)
        self.logger.info("Action Network collector test setup complete")
    
    @pytest.mark.asyncio
    async def test_collector_initialization(self):
        """Test collector initializes correctly."""
        assert self.collector.source_name == "action_network"
        assert self.collector.call_count == 0
        assert not self.collector.should_fail
        
        self.logger.info("✅ Collector initialization test passed")
    
    @pytest.mark.asyncio
    async def test_collect_data_success(self):
        """Test successful data collection."""
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"},
            parameters={"include_odds": True}
        )
        
        results = await self.collector.collect_data(request)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert self.collector.call_count == 1
        
        # Verify data structure
        for result in results:
            assert "external_game_id" in result
            assert "game_data" in result
            assert "odds_data" in result
            assert "collected_at" in result
            assert result["source"] == "action_network"
            assert result["_mock"] is True
        
        self.logger.info(f"✅ Data collection successful: {len(results)} games")
    
    @pytest.mark.asyncio
    async def test_collect_data_failure(self):
        """Test data collection failure handling."""
        # Configure collector to fail
        self.collector.set_failure_mode(True, "Mock API failure")
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        with pytest.raises(Exception) as exc_info:
            await self.collector.collect_data(request)
        
        assert "Mock API failure" in str(exc_info.value)
        assert self.collector.call_count == 1
        
        self.logger.info("✅ Failure handling test passed")
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting behavior."""
        # Simulate rate limit
        self.collector.simulate_rate_limit()
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        with pytest.raises(Exception) as exc_info:
            await self.collector.collect_data(request)
        
        assert "Rate limit exceeded" in str(exc_info.value)
        
        # Reset rate limit and try again
        self.collector.reset_rate_limit()
        results = await self.collector.collect_data(request)
        assert len(results) > 0
        
        self.logger.info("✅ Rate limiting test passed")
    
    def test_validate_record_success(self):
        """Test record validation with valid data."""
        fixture_data = ActionNetworkFixtures.game_response()
        
        is_valid = self.collector.validate_record(fixture_data)
        
        assert is_valid is True
        self.logger.info("✅ Record validation success test passed")
    
    def test_validate_record_failure(self):
        """Test record validation with invalid data."""
        # Configure validator to fail
        self.collector.set_failure_mode(True)
        
        invalid_data = {"incomplete": "data"}
        
        is_valid = self.collector.validate_record(invalid_data)
        
        assert is_valid is False
        self.logger.info("✅ Record validation failure test passed")
    
    def test_normalize_record(self):
        """Test record normalization."""
        raw_data = ActionNetworkFixtures.game_response()
        
        normalized = self.collector.normalize_record(raw_data)
        
        # Should contain original data plus normalization metadata
        assert "_mock_normalized" in normalized
        assert "_normalized_at" in normalized
        assert normalized["id"] == raw_data["id"]  # Original data preserved
        
        self.logger.info("✅ Record normalization test passed")
    
    def test_collection_history_tracking(self):
        """Test collection history tracking."""
        # Initially empty
        history = self.collector.get_collection_history()
        assert len(history) == 0
        
        # Make a collection call
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        import asyncio
        asyncio.run(self.collector.collect_data(request))
        
        # Check history was recorded
        history = self.collector.get_collection_history()
        assert len(history) == 1
        
        history_entry = history[0]
        assert "timestamp" in history_entry
        assert "request" in history_entry
        assert "call_count" in history_entry
        assert history_entry["call_count"] == 1
        
        self.logger.info("✅ Collection history tracking test passed")
    
    def test_history_reset(self):
        """Test history reset functionality."""
        # Make some collection calls
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        import asyncio
        asyncio.run(self.collector.collect_data(request))
        asyncio.run(self.collector.collect_data(request))
        
        # Verify history exists
        history = self.collector.get_collection_history()
        assert len(history) == 2
        
        # Reset history
        self.collector.reset_history()
        
        # Verify history is cleared
        history = self.collector.get_collection_history()
        assert len(history) == 0
        assert self.collector.call_count == 0
        
        self.logger.info("✅ History reset test passed")
    
    @pytest.mark.asyncio
    async def test_concurrent_collections(self):
        """Test handling of concurrent collection requests."""
        import asyncio
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        # Run 3 concurrent collections
        tasks = [
            self.collector.collect_data(request) for _ in range(3)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All should succeed
        assert len(results) == 3
        for result_set in results:
            assert len(result_set) > 0
            assert isinstance(result_set, list)
        
        # Call count should be 3
        assert self.collector.call_count == 3
        
        # History should track all calls
        history = self.collector.get_collection_history()
        assert len(history) == 3
        
        self.logger.info("✅ Concurrent collections test passed")
    
    def test_mock_api_data_generation(self):
        """Test mock API data generation."""
        api_mock = ActionNetworkMock()
        
        # Test game data generation
        game_data = api_mock.generate_game_data()
        assert "id" in game_data
        assert "teams" in game_data
        assert "odds" in game_data
        assert len(game_data["teams"]) == 2
        assert len(game_data["odds"]) >= 3  # moneyline, spread, total
        
        # Test odds history generation
        game_id = game_data["id"]
        history = api_mock.generate_odds_history(game_id, hours_back=12)
        assert len(history) == 12
        assert all("timestamp" in entry for entry in history)
        assert all("game_id" in entry for entry in history)
        
        self.logger.info("✅ Mock API data generation test passed")
    
    @pytest.mark.asyncio
    async def test_configuration_override(self):
        """Test collector behavior with different configurations."""
        # Create collector with different config
        strict_config = CollectorConfig(
            source_name="action_network",
            rate_limit_requests=10,  # Much lower limit
            rate_limit_period=60,    # Shorter period
            timeout_seconds=5        # Shorter timeout
        )
        
        strict_collector = CollectorMockFactory.create_action_network_mock(strict_config)
        
        assert strict_collector.config.rate_limit_requests == 10
        assert strict_collector.config.rate_limit_period == 60
        assert strict_collector.config.timeout_seconds == 5
        
        # Should still collect data successfully
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        results = await strict_collector.collect_data(request)
        assert len(results) > 0
        
        self.logger.info("✅ Configuration override test passed")
    
    def test_error_recovery(self):
        """Test error recovery mechanisms."""
        # Configure to fail initially
        self.collector.set_failure_mode(True, "Temporary failure")
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        # First call should fail
        with pytest.raises(Exception):
            import asyncio
            asyncio.run(self.collector.collect_data(request))
        
        # Reset failure mode (simulating recovery)
        self.collector.set_failure_mode(False)
        
        # Second call should succeed
        import asyncio
        results = asyncio.run(self.collector.collect_data(request))
        assert len(results) > 0
        
        self.logger.info("✅ Error recovery test passed")


class TestCollectorFactory:
    """Test collector factory functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("collector_factory_test")
    
    def test_create_all_mocks(self):
        """Test creating all collector mocks."""
        mocks = CollectorMockFactory.create_all_mocks()
        
        expected_sources = ["action_network", "sbd", "vsin", "mlb_stats"]
        
        assert len(mocks) == len(expected_sources)
        
        for source in expected_sources:
            assert source in mocks
            assert hasattr(mocks[source], 'collect_data')
            assert hasattr(mocks[source], 'validate_record')
            assert hasattr(mocks[source], 'normalize_record')
        
        self.logger.info("✅ Collector factory test passed")
    
    def test_factory_methods(self):
        """Test individual factory methods."""
        # Test each factory method
        an_mock = CollectorMockFactory.create_action_network_mock()
        assert an_mock.source_name == "action_network"
        
        sbd_mock = CollectorMockFactory.create_sbd_mock()
        assert sbd_mock.source_name == "sbd"
        
        vsin_mock = CollectorMockFactory.create_vsin_mock()
        assert vsin_mock.source_name == "vsin"
        
        mlb_mock = CollectorMockFactory.create_mlb_stats_mock()
        assert mlb_mock.source_name == "mlb_stats"
        
        self.logger.info("✅ Factory methods test passed")