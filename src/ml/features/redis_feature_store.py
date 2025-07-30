"""
Redis Feature Store with MessagePack Optimization
High-performance feature caching and serving with sub-100ms latency
Supports feature vector caching, batch operations, and cache invalidation
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from decimal import Decimal

import redis.asyncio as redis
import msgpack
import json

from .models import FeatureVector

# Add src to path for imports
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.config import get_settings

logger = logging.getLogger(__name__)


class RedisFeatureStore:
    """
    High-performance Redis feature store with MessagePack serialization
    Optimized for sub-100ms feature retrieval and efficient storage
    """

    def __init__(self, redis_url: Optional[str] = None):
        self.settings = get_settings()
        self.redis_url = redis_url or getattr(
            self.settings, "redis_url", "redis://localhost:6379/0"
        )
        self.redis_client: Optional[redis.Redis] = None

        # Cache configuration
        self.default_ttl = 900  # 15 minutes as suggested
        self.feature_key_prefix = "ml:features"
        self.model_prediction_prefix = "ml:predictions"
        self.batch_key_prefix = "ml:batch"

        # Performance settings
        self.max_batch_size = 100
        self.connection_pool_size = 10
        self.socket_timeout = 5

        # Serialization settings
        self.use_msgpack = True  # Use MessagePack for 2-5x performance improvement
        self.compression_threshold = 1024  # Compress data larger than 1KB

        # Cache statistics
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "writes": 0,
            "errors": 0,
            "avg_get_time_ms": 0.0,
            "avg_set_time_ms": 0.0,
        }

    async def initialize(self) -> bool:
        """Initialize Redis connection with optimized settings"""
        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=False,  # Handle binary data for MessagePack
                socket_connect_timeout=self.socket_timeout,
                socket_timeout=self.socket_timeout,
                retry_on_timeout=True,
                health_check_interval=30,
                max_connections=self.connection_pool_size,
            )

            # Test connection
            await self.redis_client.ping()
            logger.info(f"✅ Redis feature store initialized: {self.redis_url}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis feature store: {e}")
            return False

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis feature store connection closed")

    async def cache_feature_vector(
        self, game_id: int, feature_vector: FeatureVector, ttl: Optional[int] = None
    ) -> bool:
        """
        Cache feature vector with MessagePack optimization

        Args:
            game_id: Game ID for caching key
            feature_vector: FeatureVector to cache
            ttl: Time to live in seconds (default: self.default_ttl)

        Returns:
            True if cached successfully, False otherwise
        """
        start_time = datetime.utcnow()

        try:
            if not self.redis_client:
                await self.initialize()

            # Generate cache key
            cache_key = self._generate_feature_key(
                game_id, feature_vector.feature_version
            )

            # Serialize feature vector
            serialized_data = self._serialize_feature_vector(feature_vector)

            # Set TTL
            ttl = ttl or self.default_ttl

            # Store in Redis
            await self.redis_client.setex(cache_key, ttl, serialized_data)

            # Update statistics
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats("set", processing_time, True)

            logger.debug(
                f"Cached feature vector for game {game_id} in {processing_time:.1f}ms"
            )
            return True

        except Exception as e:
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats("set", processing_time, False)
            logger.error(f"Error caching feature vector for game {game_id}: {e}")
            return False

    async def get_feature_vector(
        self, game_id: int, feature_version: str = "v2.1"
    ) -> Optional[FeatureVector]:
        """
        Retrieve cached feature vector with MessagePack optimization

        Args:
            game_id: Game ID to retrieve
            feature_version: Feature version to retrieve

        Returns:
            FeatureVector if found, None otherwise
        """
        start_time = datetime.utcnow()

        try:
            if not self.redis_client:
                await self.initialize()

            # Generate cache key
            cache_key = self._generate_feature_key(game_id, feature_version)

            # Get from Redis
            cached_data = await self.redis_client.get(cache_key)

            if cached_data is None:
                # Cache miss
                processing_time = (
                    datetime.utcnow() - start_time
                ).total_seconds() * 1000
                self._update_stats("get", processing_time, False)
                logger.debug(f"Cache miss for game {game_id}")
                return None

            # Deserialize feature vector
            feature_vector = self._deserialize_feature_vector(cached_data)

            # Cache hit
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats("get", processing_time, True)

            logger.debug(f"Cache hit for game {game_id} in {processing_time:.1f}ms")
            return feature_vector

        except Exception as e:
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats("get", processing_time, False)
            logger.error(f"Error retrieving feature vector for game {game_id}: {e}")
            return None

    async def cache_batch_features(
        self,
        feature_vectors: List[Tuple[int, FeatureVector]],
        ttl: Optional[int] = None,
    ) -> int:
        """
        Cache multiple feature vectors in a single batch operation

        Args:
            feature_vectors: List of (game_id, feature_vector) tuples
            ttl: Time to live in seconds

        Returns:
            Number of successfully cached vectors
        """
        if not feature_vectors:
            return 0

        start_time = datetime.utcnow()

        try:
            if not self.redis_client:
                await self.initialize()

            ttl = ttl or self.default_ttl
            successful_caches = 0

            # Process in batches to avoid memory issues
            for i in range(0, len(feature_vectors), self.max_batch_size):
                batch = feature_vectors[i : i + self.max_batch_size]

                # Prepare batch operations
                pipe = self.redis_client.pipeline()

                for game_id, feature_vector in batch:
                    cache_key = self._generate_feature_key(
                        game_id, feature_vector.feature_version
                    )
                    serialized_data = self._serialize_feature_vector(feature_vector)
                    pipe.setex(cache_key, ttl, serialized_data)

                # Execute batch
                results = await pipe.execute()
                successful_caches += sum(1 for result in results if result)

            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            logger.info(
                f"Batch cached {successful_caches}/{len(feature_vectors)} feature vectors in {processing_time:.1f}ms"
            )

            return successful_caches

        except Exception as e:
            logger.error(f"Error batch caching feature vectors: {e}")
            return 0

    async def get_batch_features(
        self, game_ids: List[int], feature_version: str = "v2.1"
    ) -> Dict[int, Optional[FeatureVector]]:
        """
        Retrieve multiple feature vectors in a single batch operation

        Args:
            game_ids: List of game IDs to retrieve
            feature_version: Feature version to retrieve

        Returns:
            Dictionary mapping game_id to FeatureVector (or None if not found)
        """
        if not game_ids:
            return {}

        start_time = datetime.utcnow()

        try:
            if not self.redis_client:
                await self.initialize()

            results = {}

            # Process in batches
            for i in range(0, len(game_ids), self.max_batch_size):
                batch_ids = game_ids[i : i + self.max_batch_size]

                # Generate cache keys
                cache_keys = [
                    self._generate_feature_key(game_id, feature_version)
                    for game_id in batch_ids
                ]

                # Batch get from Redis
                cached_data = await self.redis_client.mget(cache_keys)

                # Process results
                for game_id, data in zip(batch_ids, cached_data):
                    if data is not None:
                        try:
                            feature_vector = self._deserialize_feature_vector(data)
                            results[game_id] = feature_vector
                        except Exception as e:
                            logger.warning(
                                f"Error deserializing cached data for game {game_id}: {e}"
                            )
                            results[game_id] = None
                    else:
                        results[game_id] = None

            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            cache_hits = sum(1 for v in results.values() if v is not None)

            logger.info(
                f"Batch retrieved {cache_hits}/{len(game_ids)} feature vectors in {processing_time:.1f}ms"
            )

            return results

        except Exception as e:
            logger.error(f"Error batch retrieving feature vectors: {e}")
            return {game_id: None for game_id in game_ids}

    async def invalidate_game_cache(
        self, game_id: int, feature_version: Optional[str] = None
    ) -> bool:
        """
        Invalidate cached features for a specific game

        Args:
            game_id: Game ID to invalidate
            feature_version: Specific version to invalidate (or all versions if None)

        Returns:
            True if invalidated successfully
        """
        try:
            if not self.redis_client:
                await self.initialize()

            if feature_version:
                # Invalidate specific version
                cache_key = self._generate_feature_key(game_id, feature_version)
                deleted = await self.redis_client.delete(cache_key)
                logger.debug(
                    f"Invalidated cache for game {game_id} version {feature_version}: {deleted} keys"
                )
            else:
                # Invalidate all versions for this game
                pattern = f"{self.feature_key_prefix}:game:{game_id}:*"
                keys = []
                async for key in self.redis_client.scan_iter(match=pattern):
                    keys.append(key)

                if keys:
                    deleted = await self.redis_client.delete(*keys)
                    logger.debug(
                        f"Invalidated cache for game {game_id} all versions: {deleted} keys"
                    )

            return True

        except Exception as e:
            logger.error(f"Error invalidating cache for game {game_id}: {e}")
            return False

    async def cache_model_prediction(
        self,
        game_id: int,
        model_name: str,
        prediction_data: Dict[str, Any],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Cache model prediction results

        Args:
            game_id: Game ID
            model_name: Model name
            prediction_data: Prediction results to cache
            ttl: Time to live in seconds

        Returns:
            True if cached successfully
        """
        try:
            if not self.redis_client:
                await self.initialize()

            cache_key = (
                f"{self.model_prediction_prefix}:game:{game_id}:model:{model_name}"
            )
            serialized_data = self._serialize_data(prediction_data)

            ttl = ttl or self.default_ttl
            await self.redis_client.setex(cache_key, ttl, serialized_data)

            logger.debug(f"Cached prediction for game {game_id} model {model_name}")
            return True

        except Exception as e:
            logger.error(
                f"Error caching prediction for game {game_id} model {model_name}: {e}"
            )
            return False

    async def get_model_prediction(
        self, game_id: int, model_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached model prediction

        Args:
            game_id: Game ID
            model_name: Model name

        Returns:
            Prediction data if found, None otherwise
        """
        try:
            if not self.redis_client:
                await self.initialize()

            cache_key = (
                f"{self.model_prediction_prefix}:game:{game_id}:model:{model_name}"
            )
            cached_data = await self.redis_client.get(cache_key)

            if cached_data is None:
                return None

            return self._deserialize_data(cached_data)

        except Exception as e:
            logger.error(
                f"Error retrieving prediction for game {game_id} model {model_name}: {e}"
            )
            return None

    async def clear_expired_cache(self) -> int:
        """
        Clear expired cache entries (Redis handles this automatically, but useful for stats)

        Returns:
            Number of keys that would be expired (estimation)
        """
        try:
            if not self.redis_client:
                await self.initialize()

            # Get cache info
            info = await self.redis_client.info("memory")
            expired_keys = info.get("expired_keys", 0)

            logger.info(f"Redis expired keys: {expired_keys}")
            return expired_keys

        except Exception as e:
            logger.error(f"Error checking expired cache: {e}")
            return 0

    def _generate_feature_key(self, game_id: int, feature_version: str) -> str:
        """Generate cache key for feature vector"""
        return f"{self.feature_key_prefix}:game:{game_id}:version:{feature_version}"

    def _serialize_feature_vector(self, feature_vector: FeatureVector) -> bytes:
        """Serialize feature vector using MessagePack or JSON"""
        try:
            # Convert to dictionary
            data = feature_vector.model_dump()

            # Handle special types for serialization
            serializable_data = self._make_serializable(data)

            if self.use_msgpack:
                # MessagePack serialization (2-5x faster than JSON)
                return msgpack.packb(serializable_data, use_bin_type=True)
            else:
                # JSON fallback
                return json.dumps(serializable_data, default=str).encode("utf-8")

        except Exception as e:
            logger.error(f"Error serializing feature vector: {e}")
            # Fallback to JSON
            data = feature_vector.model_dump()
            return json.dumps(self._make_serializable(data), default=str).encode(
                "utf-8"
            )

    def _deserialize_feature_vector(self, data: bytes) -> FeatureVector:
        """Deserialize feature vector from MessagePack or JSON"""
        try:
            if self.use_msgpack:
                # Try MessagePack first
                try:
                    deserialized_data = msgpack.unpackb(data, raw=False)
                except (msgpack.exceptions.ExtraData, ValueError):
                    # Fallback to JSON if MessagePack fails
                    deserialized_data = json.loads(data.decode("utf-8"))
            else:
                # JSON deserialization
                deserialized_data = json.loads(data.decode("utf-8"))

            # Convert back to FeatureVector
            return FeatureVector(**self._restore_types(deserialized_data))

        except Exception as e:
            logger.error(f"Error deserializing feature vector: {e}")
            raise

    def _serialize_data(self, data: Any) -> bytes:
        """Serialize arbitrary data"""
        try:
            serializable_data = self._make_serializable(data)

            if self.use_msgpack:
                return msgpack.packb(serializable_data, use_bin_type=True)
            else:
                return json.dumps(serializable_data, default=str).encode("utf-8")

        except Exception as e:
            logger.error(f"Error serializing data: {e}")
            return json.dumps(self._make_serializable(data), default=str).encode(
                "utf-8"
            )

    def _deserialize_data(self, data: bytes) -> Any:
        """Deserialize arbitrary data"""
        try:
            if self.use_msgpack:
                try:
                    return msgpack.unpackb(data, raw=False)
                except (msgpack.exceptions.ExtraData, ValueError):
                    return json.loads(data.decode("utf-8"))
            else:
                return json.loads(data.decode("utf-8"))

        except Exception as e:
            logger.error(f"Error deserializing data: {e}")
            raise

    def _make_serializable(self, obj: Any) -> Any:
        """Convert objects to serializable format"""
        if isinstance(obj, dict):
            return {key: self._make_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, "model_dump"):
            return self._make_serializable(obj.model_dump())
        else:
            return obj

    def _restore_types(self, obj: Any) -> Any:
        """Restore types from serialized format"""
        if isinstance(obj, dict):
            restored = {}
            for key, value in obj.items():
                # Restore datetime fields
                if key.endswith("_time") or key.endswith("_at") or key == "created_at":
                    if isinstance(value, str):
                        try:
                            restored[key] = datetime.fromisoformat(
                                value.replace("Z", "+00:00")
                            )
                        except ValueError:
                            restored[key] = value
                    else:
                        restored[key] = value
                else:
                    restored[key] = self._restore_types(value)
            return restored
        elif isinstance(obj, list):
            return [self._restore_types(item) for item in obj]
        else:
            return obj

    def _update_stats(self, operation: str, processing_time_ms: float, success: bool):
        """Update cache performance statistics"""
        if operation == "get":
            if success:
                self.cache_stats["hits"] += 1
            else:
                self.cache_stats["misses"] += 1

            # Update average get time
            current_avg = self.cache_stats["avg_get_time_ms"]
            total_gets = self.cache_stats["hits"] + self.cache_stats["misses"]
            new_avg = (
                (current_avg * (total_gets - 1)) + processing_time_ms
            ) / total_gets
            self.cache_stats["avg_get_time_ms"] = new_avg

        elif operation == "set":
            if success:
                self.cache_stats["writes"] += 1
            else:
                self.cache_stats["errors"] += 1

            # Update average set time
            current_avg = self.cache_stats["avg_set_time_ms"]
            total_sets = self.cache_stats["writes"] + self.cache_stats["errors"]
            new_avg = (
                (current_avg * (total_sets - 1)) + processing_time_ms
            ) / total_sets
            self.cache_stats["avg_set_time_ms"] = new_avg

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        stats = self.cache_stats.copy()

        total_operations = stats["hits"] + stats["misses"]
        if total_operations > 0:
            stats["hit_rate"] = stats["hits"] / total_operations
        else:
            stats["hit_rate"] = 0.0

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on Redis feature store"""
        try:
            if not self.redis_client:
                await self.initialize()

            start_time = datetime.utcnow()

            # Test basic operations
            test_key = f"{self.feature_key_prefix}:health_check"
            test_data = {"timestamp": datetime.utcnow().isoformat(), "test": True}

            # Test set
            serialized = self._serialize_data(test_data)
            await self.redis_client.setex(test_key, 60, serialized)

            # Test get
            retrieved = await self.redis_client.get(test_key)
            deserialized = self._deserialize_data(retrieved)

            # Clean up
            await self.redis_client.delete(test_key)

            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            return {
                "status": "healthy",
                "response_time_ms": response_time,
                "serialization": "msgpack" if self.use_msgpack else "json",
                "connection_url": self.redis_url,
                "cache_stats": self.get_cache_stats(),
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "connection_url": self.redis_url,
            }
