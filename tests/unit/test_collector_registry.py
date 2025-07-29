#!/usr/bin/env python3
"""
Comprehensive test suite for CollectorRegistry.

Tests cover:
- Thread safety of singleton pattern
- Registration and deregistration
- Cache functionality with LRU and TTL
- Error handling scenarios
- Performance monitoring
"""

import pytest
import threading
import time
from typing import Optional
from unittest.mock import MagicMock, patch

from src.data.collection.base import BaseCollector, DataSource, CollectorConfig, CollectionRequest
from src.data.collection.registry import (
    CollectorRegistry,
    RegistrationInfo,
    CacheEntry,
    register_collector,
    get_collector_class,
    get_collector_instance,
    get_cache_statistics,
    clear_collector_cache,
)


class MockCollector(BaseCollector):
    """Mock collector for testing purposes."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.initialized = True
    
    async def collect_data(self, request: CollectionRequest) -> list[dict]:
        return [{"test": "data"}]
    
    def validate_record(self, record: dict) -> bool:
        return True
    
    def normalize_record(self, record: dict) -> dict:
        return record


class AnotherMockCollector(BaseCollector):
    """Another mock collector for testing conflicts."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.initialized = True
    
    async def collect_data(self, request: CollectionRequest) -> list[dict]:
        return [{"other": "data"}]
    
    def validate_record(self, record: dict) -> bool:
        return True
    
    def normalize_record(self, record: dict) -> dict:
        return record


@pytest.fixture
def fresh_registry():
    """Create a fresh registry instance for each test."""
    # Reset the singleton for testing
    CollectorRegistry._instance = None
    CollectorRegistry._initialized = False
    registry = CollectorRegistry()
    yield registry
    # Clean up
    registry.reset_registry()


class TestSingletonPattern:
    """Test thread safety of singleton pattern."""
    
    def test_singleton_behavior(self, fresh_registry):
        """Test that multiple calls return the same instance."""
        registry1 = CollectorRegistry()
        registry2 = CollectorRegistry()
        assert registry1 is registry2
        assert registry1 is fresh_registry
    
    def test_thread_safety(self):
        """Test singleton pattern is thread-safe."""
        CollectorRegistry._instance = None
        CollectorRegistry._initialized = False
        
        instances = []
        
        def create_registry():
            instances.append(CollectorRegistry())
        
        # Create multiple threads trying to create instances
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=create_registry)
            threads.append(thread)
        
        # Start all threads simultaneously
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All instances should be the same
        assert len(instances) == 10
        for instance in instances:
            assert instance is instances[0]
    
    def test_initialization_only_once(self, fresh_registry):
        """Test that initialization happens only once."""
        # Check that creating multiple instances doesn't re-initialize
        initial_collectors = fresh_registry._registered_collectors.copy()
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        # Create another instance
        registry2 = CollectorRegistry()
        
        # Should have the registered collector
        assert DataSource.VSIN in registry2._registered_collectors
        assert len(registry2._registered_collectors) > len(initial_collectors)


class TestCollectorRegistration:
    """Test collector registration functionality."""
    
    def test_successful_registration(self, fresh_registry):
        """Test successful collector registration."""
        result = fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        assert result is True
        assert DataSource.VSIN in fresh_registry._registered_collectors
        
        info = fresh_registry._registered_collectors[DataSource.VSIN]
        assert info.collector_class == MockCollector
        assert info.source == DataSource.VSIN
        assert info.is_primary is True
    
    def test_duplicate_registration_prevention(self, fresh_registry):
        """Test that duplicate registrations are prevented."""
        # First registration should succeed
        result1 = fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        assert result1 is True
        
        # Second registration should fail
        result2 = fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        assert result2 is False
    
    def test_conflicting_registration_prevention(self, fresh_registry):
        """Test that conflicting registrations are prevented."""
        # Register first collector
        result1 = fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        assert result1 is True
        
        # Try to register different collector for same source
        result2 = fresh_registry.register_collector(DataSource.VSIN, AnotherMockCollector)
        assert result2 is False
    
    def test_registration_with_override(self, fresh_registry):
        """Test registration with allow_override=True."""
        # Register first collector
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        # Override with different collector
        result = fresh_registry.register_collector(
            DataSource.VSIN, 
            AnotherMockCollector,
            allow_override=True
        )
        assert result is True
        
        info = fresh_registry._registered_collectors[DataSource.VSIN]
        assert info.collector_class == AnotherMockCollector
    
    def test_source_alias_resolution(self, fresh_registry):
        """Test source alias resolution."""
        # Register with primary source
        fresh_registry.register_collector(DataSource.SBD, MockCollector)
        
        # Should be able to get via alias
        collector_class = fresh_registry.get_collector_class("sports_betting_dime")
        assert collector_class == MockCollector
    
    @patch('src.data.collection.registry.CollectorFactory')
    def test_legacy_factory_integration_success(self, mock_factory, fresh_registry):
        """Test successful legacy factory integration."""
        mock_factory._collectors = {}
        mock_factory.register_collector = MagicMock()
        
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        mock_factory.register_collector.assert_called_once_with(DataSource.VSIN, MockCollector)
    
    @patch('src.data.collection.registry.CollectorFactory')
    def test_legacy_factory_integration_failure(self, mock_factory, fresh_registry):
        """Test graceful handling of legacy factory failures."""
        mock_factory._collectors = {}
        mock_factory.register_collector = MagicMock(side_effect=Exception("Factory error"))
        
        # Should not raise exception
        result = fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        assert result is True
        
        # Should still be registered in main registry
        assert DataSource.VSIN in fresh_registry._registered_collectors


class TestCachefunctionality:
    """Test cache functionality with LRU and TTL."""
    
    def test_cache_basic_functionality(self, fresh_registry):
        """Test basic cache hit/miss functionality."""
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        # First call should be cache miss
        instance1 = fresh_registry.get_collector_instance(DataSource.VSIN)
        assert instance1 is not None
        assert fresh_registry._cache_misses == 1
        assert fresh_registry._cache_hits == 0
        
        # Second call should be cache hit
        instance2 = fresh_registry.get_collector_instance(DataSource.VSIN)
        assert instance2 is instance1
        assert fresh_registry._cache_hits == 1
    
    def test_cache_size_limit(self):
        """Test LRU cache size limiting."""
        CollectorRegistry._instance = None
        CollectorRegistry._initialized = False
        registry = CollectorRegistry()
        registry.configure_cache(max_cache_size=2)
        registry.register_collector(DataSource.VSIN, MockCollector)
        registry.register_collector(DataSource.SBD, MockCollector)
        registry.register_collector(DataSource.ACTION_NETWORK, MockCollector)
        
        # Create instances to fill cache beyond limit
        instance1 = registry.get_collector_instance(DataSource.VSIN)
        instance2 = registry.get_collector_instance(DataSource.SBD)
        instance3 = registry.get_collector_instance(DataSource.ACTION_NETWORK)
        
        # Cache should have evicted oldest entry
        assert len(registry._instance_cache) == 2
        
        # First instance should have been evicted (LRU)
        instance1_again = registry.get_collector_instance(DataSource.VSIN)
        assert instance1_again is not instance1  # New instance created
    
    def test_cache_ttl_expiration(self):
        """Test TTL-based cache expiration."""
        CollectorRegistry._instance = None
        CollectorRegistry._initialized = False
        registry = CollectorRegistry()
        registry.configure_cache(default_ttl=0.1)  # 100ms TTL
        registry.register_collector(DataSource.VSIN, MockCollector)
        
        # Get instance
        instance1 = registry.get_collector_instance(DataSource.VSIN)
        assert instance1 is not None
        
        # Should hit cache immediately
        instance2 = registry.get_collector_instance(DataSource.VSIN)
        assert instance2 is instance1
        
        # Wait for TTL to expire
        time.sleep(0.15)
        
        # Should create new instance
        instance3 = registry.get_collector_instance(DataSource.VSIN)
        assert instance3 is not instance1
    
    def test_cache_custom_ttl(self, fresh_registry):
        """Test custom TTL per request."""
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        # Create instance with custom TTL
        instance1 = fresh_registry.get_collector_instance(DataSource.VSIN, ttl=0.05)
        
        # Wait for expiration
        time.sleep(0.1)
        
        # Should create new instance
        instance2 = fresh_registry.get_collector_instance(DataSource.VSIN)
        assert instance2 is not instance1
    
    def test_force_new_instance(self, fresh_registry):
        """Test force_new parameter bypasses cache."""
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        instance1 = fresh_registry.get_collector_instance(DataSource.VSIN)
        instance2 = fresh_registry.get_collector_instance(DataSource.VSIN, force_new=True)
        
        assert instance1 is not instance2
    
    def test_cache_statistics(self, fresh_registry):
        """Test cache statistics collection."""
        fresh_registry.register_collector(DataSource.VSIN, MockCollector)
        
        # Generate some cache activity
        fresh_registry.get_collector_instance(DataSource.VSIN)  # miss
        fresh_registry.get_collector_instance(DataSource.VSIN)  # hit
        fresh_registry.get_collector_instance(DataSource.VSIN, force_new=True)  # miss, replaces cache entry
        
        stats = fresh_registry.get_cache_stats()
        
        assert stats["cache_hits"] == 1
        assert stats["cache_misses"] == 2
        assert stats["hit_rate_percent"] == 33.33
        assert stats["cache_size"] >= 1  # At least one entry
        assert len(stats["entries"]) >= 1
        
        # Check entry details
        for entry in stats["entries"]:
            assert "key" in entry
            assert "created_at" in entry
            assert "is_expired" in entry


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_unknown_source_handling(self, fresh_registry):
        """Test handling of unknown data sources."""
        collector_class = fresh_registry.get_collector_class("unknown_source")
        assert collector_class is None
        
        instance = fresh_registry.get_collector_instance("unknown_source")
        assert instance is None
    
    def test_collector_instantiation_failure(self, fresh_registry):
        """Test handling of collector instantiation failures."""
        
        class FailingCollector(BaseCollector):
            def __init__(self, config):
                raise ValueError("Instantiation failed")
            
            def collect_data(self):
                return {}
        
        fresh_registry.register_collector(DataSource.VSIN, FailingCollector)
        
        instance = fresh_registry.get_collector_instance(DataSource.VSIN)
        assert instance is None
        assert fresh_registry._cache_misses == 1


class TestConvenienceFunctions:
    """Test module-level convenience functions."""
    
    def test_register_collector_function(self):
        """Test global register_collector function."""
        # Reset global registry
        from src.data.collection.registry import _registry
        _registry.reset_registry()
        
        result = register_collector(DataSource.VSIN, MockCollector)
        assert result is True
        assert DataSource.VSIN in _registry._registered_collectors
    
    def test_get_collector_class_function(self):
        """Test global get_collector_class function."""
        from src.data.collection.registry import _registry
        _registry.reset_registry()
        
        register_collector(DataSource.VSIN, MockCollector)
        register_collector(DataSource.SBD, MockCollector)  # For alias test
        
        collector_class = get_collector_class(DataSource.VSIN)
        assert collector_class == MockCollector
        
        collector_class_alias = get_collector_class("sports_betting_dime")
        assert collector_class_alias == MockCollector  # SBD alias
    
    def test_get_collector_instance_function(self):
        """Test global get_collector_instance function."""
        from src.data.collection.registry import _registry
        _registry.reset_registry()
        
        register_collector(DataSource.VSIN, MockCollector)
        
        instance = get_collector_instance(DataSource.VSIN)
        assert instance is not None
        assert isinstance(instance, MockCollector)
    
    def test_cache_statistics_function(self):
        """Test global get_cache_statistics function."""
        from src.data.collection.registry import _registry
        _registry.reset_registry()
        
        register_collector(DataSource.VSIN, MockCollector)
        get_collector_instance(DataSource.VSIN)
        
        stats = get_cache_statistics()
        assert "cache_hits" in stats
        assert "cache_misses" in stats
        assert stats["cache_size"] >= 1
    
    def test_clear_cache_function(self):
        """Test global clear_collector_cache function."""
        from src.data.collection.registry import _registry
        _registry.reset_registry()
        
        register_collector(DataSource.VSIN, MockCollector)
        get_collector_instance(DataSource.VSIN)
        
        assert len(_registry._instance_cache) > 0
        clear_collector_cache()
        assert len(_registry._instance_cache) == 0


if __name__ == "__main__":
    pytest.main([__file__])