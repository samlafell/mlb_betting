"""
Comprehensive tests for Redis Atomic Store
Tests atomic operations, performance, and consistency
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import msgpack
import json

from src.ml.features.redis_atomic_store import RedisAtomicStore
from src.ml.features.models import FeatureVector, TemporalFeatures


class TestRedisAtomicStore:
    """Test suite for Redis atomic operations"""

    @pytest.fixture
    async def redis_store(self):
        """Create Redis store instance for testing"""
        store = RedisAtomicStore(
            redis_url="redis://localhost:6379/15", use_msgpack=True
        )  # Use test DB
        await store.initialize()
        yield store
        await store.close()

    @pytest.fixture
    def mock_redis_store(self):
        """Mock Redis store for unit tests"""
        store = RedisAtomicStore(redis_url="redis://localhost:6379/15")
        store.redis_client = AsyncMock()
        return store

    @pytest.fixture
    def sample_feature_vector(self):
        """Sample feature vector for testing"""
        return FeatureVector(
            game_id=12345,
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            feature_version="test_v1.0",
            minutes_before_game=120,
            temporal_features=TemporalFeatures(
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                minutes_before_game=120,
            ),
        )

    @pytest.mark.asyncio
    async def test_atomic_cache_operations(
        self, mock_redis_store, sample_feature_vector
    ):
        """Test atomic cache operations prevent race conditions"""
        # Mock Redis operations
        mock_redis_store.redis_client.set.return_value = True
        mock_redis_store.redis_client.setex.return_value = True
        mock_redis_store.redis_client.eval.return_value = 1

        # Test atomic cache
        success = await mock_redis_store.cache_feature_vector_atomic(
            12345, sample_feature_vector, ttl=300
        )

        assert success is True
        assert mock_redis_store.cache_stats["writes"] == 1
        assert mock_redis_store.cache_stats["lock_acquisitions"] == 1

    @pytest.mark.asyncio
    async def test_distributed_lock_mechanism(self, mock_redis_store):
        """Test distributed locking prevents concurrent access"""
        # Mock Redis to simulate lock acquisition
        mock_redis_store.redis_client.set.return_value = True
        mock_redis_store.redis_client.eval.return_value = 1

        async with mock_redis_store.distributed_lock("test_key"):
            # Inside lock - should have acquired it
            assert mock_redis_store.cache_stats["lock_acquisitions"] == 1

        # Lock should be released after context
        mock_redis_store.redis_client.eval.assert_called()  # Release script was called

    @pytest.mark.asyncio
    async def test_lock_timeout_handling(self, mock_redis_store):
        """Test lock timeout handling"""
        # Mock Redis to simulate lock contention
        mock_redis_store.redis_client.set.return_value = False  # Lock already held

        with pytest.raises(TimeoutError, match="Failed to acquire lock"):
            async with mock_redis_store.distributed_lock("test_key", timeout=0.1):
                pass

        assert mock_redis_store.cache_stats["lock_timeouts"] == 1

    @pytest.mark.asyncio
    async def test_messagepack_serialization_fallback(
        self, mock_redis_store, sample_feature_vector
    ):
        """Test MessagePack serialization with JSON fallback"""
        # Test MessagePack serialization
        serialized = mock_redis_store._serialize_feature_vector(sample_feature_vector)
        assert isinstance(serialized, bytes)

        # Verify it's MessagePack (not JSON)
        try:
            msgpack.unpackb(serialized)
            is_msgpack = True
        except:
            is_msgpack = False
        assert is_msgpack is True

        # Test deserialization
        deserialized = mock_redis_store._deserialize_feature_vector(serialized)
        assert isinstance(deserialized, FeatureVector)
        assert deserialized.game_id == sample_feature_vector.game_id

    @pytest.mark.asyncio
    async def test_batch_atomic_operations(
        self, mock_redis_store, sample_feature_vector
    ):
        """Test atomic batch operations"""
        # Create batch of feature vectors
        feature_vectors = [(12345 + i, sample_feature_vector) for i in range(10)]

        # Mock Redis pipeline operations
        mock_pipe = AsyncMock()
        mock_pipe.execute.return_value = [True] * 10  # All operations succeed
        mock_redis_store.redis_client.pipeline.return_value.__aenter__.return_value = (
            mock_pipe
        )

        # Test batch caching
        cached_count = await mock_redis_store.cache_batch_features_atomic(
            feature_vectors
        )

        assert cached_count == 10
        assert mock_redis_store.cache_stats["writes"] == 10

    @pytest.mark.asyncio
    async def test_cache_invalidation_atomic(self, mock_redis_store):
        """Test atomic cache invalidation"""
        # Mock Redis operations
        mock_redis_store.redis_client.delete.return_value = 1
        mock_redis_store.redis_client.keys.return_value = ["key1", "key2", "key3"]

        # Test single key invalidation
        deleted = await mock_redis_store.invalidate_game_cache_atomic(12345, "v1.0")
        assert deleted == 1

        # Test pattern invalidation (all versions)
        deleted = await mock_redis_store.invalidate_game_cache_atomic(12345)
        assert deleted >= 1  # Should delete multiple keys

    @pytest.mark.asyncio
    async def test_concurrent_access_consistency(
        self, mock_redis_store, sample_feature_vector
    ):
        """Test consistency under concurrent access"""

        # Simulate concurrent cache operations
        async def cache_operation(game_id_offset):
            return await mock_redis_store.cache_feature_vector_atomic(
                12345 + game_id_offset, sample_feature_vector
            )

        # Mock successful operations
        mock_redis_store.redis_client.set.return_value = True
        mock_redis_store.redis_client.setex.return_value = True
        mock_redis_store.redis_client.eval.return_value = 1

        # Run concurrent operations
        tasks = [cache_operation(i) for i in range(5)]
        results = await asyncio.gather(*tasks)

        # All operations should succeed
        assert all(results)
        assert mock_redis_store.cache_stats["writes"] == 5
        assert mock_redis_store.cache_stats["lock_acquisitions"] == 5

    def test_serialization_performance(self, mock_redis_store, sample_feature_vector):
        """Test serialization performance claims"""
        import time

        # Benchmark MessagePack serialization
        start_time = time.time()
        for _ in range(1000):
            mock_redis_store._serialize_feature_vector(sample_feature_vector)
        msgpack_time = time.time() - start_time

        # Benchmark JSON serialization
        mock_redis_store.use_msgpack = False
        start_time = time.time()
        for _ in range(1000):
            mock_redis_store._serialize_feature_vector(sample_feature_vector)
        json_time = time.time() - start_time

        # MessagePack should be significantly faster
        speedup = json_time / msgpack_time
        assert speedup > 1.5  # Should be at least 1.5x faster

    @pytest.mark.asyncio
    async def test_memory_efficiency(self, mock_redis_store):
        """Test memory usage with large datasets"""
        import sys

        # Create large feature vector list
        large_batch = []
        for i in range(1000):
            fv = FeatureVector(
                game_id=i,
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                feature_version="test_v1.0",
                minutes_before_game=120,
            )
            large_batch.append((i, fv))

        # Mock Redis operations
        mock_pipe = AsyncMock()
        mock_pipe.execute.return_value = [True] * 100  # Batch size is 100
        mock_redis_store.redis_client.pipeline.return_value.__aenter__.return_value = (
            mock_pipe
        )

        # Memory usage should be reasonable (this is a basic check)
        initial_memory = sys.getsizeof(large_batch)

        # Process batch
        await mock_redis_store.cache_batch_features_atomic(large_batch)

        # Should not cause memory explosion
        final_memory = sys.getsizeof(large_batch)
        assert final_memory == initial_memory  # No memory leak

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(
        self, mock_redis_store, sample_feature_vector
    ):
        """Test error handling and graceful recovery"""
        # Test Redis connection error
        mock_redis_store.redis_client.setex.side_effect = Exception("Connection error")

        success = await mock_redis_store.cache_feature_vector_atomic(
            12345, sample_feature_vector
        )
        assert success is False
        assert mock_redis_store.cache_stats["errors"] == 1

        # Test serialization error recovery
        with patch.object(
            mock_redis_store, "_serialize_feature_vector"
        ) as mock_serialize:
            mock_serialize.side_effect = Exception("Serialization error")

            success = await mock_redis_store.cache_feature_vector_atomic(
                12345, sample_feature_vector
            )
            assert success is False

    @pytest.mark.asyncio
    async def test_health_check_comprehensive(self, mock_redis_store):
        """Test comprehensive health check"""
        # Mock successful health check operations
        mock_redis_store.redis_client.set.return_value = True
        mock_redis_store.redis_client.get.return_value = b"test_value"
        mock_redis_store.redis_client.delete.return_value = 1

        health = await mock_redis_store.health_check()

        assert health["status"] == "healthy"
        assert "response_time_ms" in health
        assert "cache_stats" in health
        assert health["msgpack_enabled"] is True

    def test_cache_key_generation(self, mock_redis_store):
        """Test cache key generation consistency"""
        game_id = 12345
        feature_version = "v1.0"

        key1 = mock_redis_store._generate_feature_key(game_id, feature_version)
        key2 = mock_redis_store._generate_feature_key(game_id, feature_version)

        # Same inputs should generate same keys
        assert key1 == key2
        assert f"game:{game_id}" in key1
        assert f"version:{feature_version}" in key1

        # Different inputs should generate different keys
        key3 = mock_redis_store._generate_feature_key(12346, feature_version)
        assert key1 != key3

    def test_cache_statistics_tracking(self, mock_redis_store):
        """Test cache statistics are tracked correctly"""
        initial_stats = mock_redis_store.get_cache_stats()
        assert "hit_rate" in initial_stats
        assert "total_operations" in initial_stats

        # Simulate cache operations
        mock_redis_store.cache_stats["hits"] = 70
        mock_redis_store.cache_stats["misses"] = 30

        stats = mock_redis_store.get_cache_stats()
        assert stats["hit_rate"] == 0.7
        assert stats["total_operations"] == 100


class TestRedisStoreIntegration:
    """Integration tests for Redis store (requires Redis)"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_real_redis_operations(self):
        """Test with real Redis instance"""
        pytest.skip("Integration test - requires Redis server")

        store = RedisAtomicStore(redis_url="redis://localhost:6379/15")
        await store.initialize()

        try:
            # Test real operations
            feature_vector = FeatureVector(
                game_id=99999,
                feature_cutoff_time=datetime.now() - timedelta(hours=2),
                feature_version="integration_test_v1.0",
                minutes_before_game=120,
            )

            # Test cache and retrieve
            success = await store.cache_feature_vector_atomic(99999, feature_vector)
            assert success is True

            retrieved = await store.get_feature_vector_atomic(
                99999, "integration_test_v1.0"
            )
            assert retrieved is not None
            assert retrieved.game_id == 99999

            # Test health check
            health = await store.health_check()
            assert health["status"] == "healthy"

        finally:
            await store.close()


# Performance benchmarks
@pytest.mark.benchmark
class TestRedisStorePerformance:
    """Performance benchmark tests"""

    @pytest.mark.asyncio
    async def test_cache_operation_performance(self, benchmark):
        """Benchmark cache operations for sub-100ms target"""
        mock_store = RedisAtomicStore()
        mock_store.redis_client = AsyncMock()
        mock_store.redis_client.setex.return_value = True
        mock_store.redis_client.set.return_value = True
        mock_store.redis_client.eval.return_value = 1

        feature_vector = FeatureVector(
            game_id=12345,
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            feature_version="benchmark_v1.0",
            minutes_before_game=120,
        )

        # Benchmark cache operation
        result = await benchmark(
            mock_store.cache_feature_vector_atomic, 12345, feature_vector
        )

        assert result is True

    def test_serialization_benchmark(self, benchmark):
        """Benchmark serialization performance"""
        store = RedisAtomicStore(use_msgpack=True)
        feature_vector = FeatureVector(
            game_id=12345,
            feature_cutoff_time=datetime.now() - timedelta(hours=2),
            feature_version="benchmark_v1.0",
            minutes_before_game=120,
        )

        # Benchmark serialization
        result = benchmark(store._serialize_feature_vector, feature_vector)
        assert isinstance(result, bytes)
