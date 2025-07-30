"""
Enhanced Redis Feature Store with Atomic Operations
Thread-safe Redis operations with distributed locking and transactions
"""

import logging
import asyncio
import hashlib
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from contextlib import asynccontextmanager

import redis.asyncio as redis
import msgpack
import json

from .models import FeatureVector
from ..database.connection_pool import get_connection_pool

logger = logging.getLogger(__name__)


class RedisAtomicStore:
    """
    Enhanced Redis feature store with atomic operations and distributed locking
    Prevents race conditions and ensures data consistency
    """

    def __init__(
        self, redis_url: str = "redis://localhost:6379/0", use_msgpack: bool = True
    ):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.use_msgpack = use_msgpack

        # Cache configuration
        self.default_ttl = 900  # 15 minutes
        self.feature_key_prefix = "ml:features"
        self.model_prediction_prefix = "ml:predictions"
        self.batch_key_prefix = "ml:batch"
        self.lock_prefix = "ml:lock"

        # Performance settings
        self.max_batch_size = 100
        self.connection_pool_size = 10
        self.socket_timeout = 5
        self.lock_timeout = 10  # seconds
        self.lock_sleep = 0.01  # 10ms

        # Cache statistics
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "writes": 0,
            "errors": 0,
            "lock_acquisitions": 0,
            "lock_timeouts": 0,
        }

    async def initialize(self) -> bool:
        """Initialize Redis connection with connection pooling"""
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
            logger.info(f"✅ Redis atomic store initialized: {self.redis_url}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis atomic store: {e}")
            return False

    async def close(self) -> None:
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis connection closed")

    @asynccontextmanager
    async def distributed_lock(self, key: str, timeout: Optional[int] = None):
        """Distributed lock context manager to prevent race conditions"""
        if not self.redis_client:
            raise RuntimeError("Redis client not initialized")

        lock_key = f"{self.lock_prefix}:{key}"
        lock_value = f"{datetime.utcnow().timestamp()}:{id(asyncio.current_task())}"
        timeout = timeout or self.lock_timeout

        acquired = False
        try:
            # Try to acquire lock with exponential backoff
            start_time = asyncio.get_event_loop().time()
            sleep_time = self.lock_sleep

            while (asyncio.get_event_loop().time() - start_time) < timeout:
                # Atomic set if not exists with TTL
                acquired = await self.redis_client.set(
                    lock_key,
                    lock_value,
                    nx=True,  # Only set if key doesn't exist
                    ex=timeout,  # Expire after timeout seconds
                )

                if acquired:
                    self.cache_stats["lock_acquisitions"] += 1
                    logger.debug(f"Acquired lock for key: {key}")
                    break

                # Wait with exponential backoff
                await asyncio.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, 0.1)  # Max 100ms sleep

            if not acquired:
                self.cache_stats["lock_timeouts"] += 1
                raise TimeoutError(
                    f"Failed to acquire lock for key: {key} within {timeout}s"
                )

            yield

        finally:
            if acquired:
                # Release lock only if we own it
                lua_script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
                """
                try:
                    await self.redis_client.eval(lua_script, 1, lock_key, lock_value)
                    logger.debug(f"Released lock for key: {key}")
                except Exception as e:
                    logger.warning(f"Failed to release lock for key {key}: {e}")

    def _generate_feature_key(self, game_id: int, feature_version: str) -> str:
        """Generate cache key for feature vector"""
        return f"{self.feature_key_prefix}:game:{game_id}:version:{feature_version}"

    def _generate_prediction_key(self, game_id: int, model_name: str) -> str:
        """Generate cache key for model prediction"""
        return f"{self.model_prediction_prefix}:game:{game_id}:model:{model_name}"

    def _serialize_feature_vector(self, feature_vector: FeatureVector) -> bytes:
        """Serialize feature vector using MessagePack or JSON with error handling"""
        try:
            # Convert to dictionary with type handling
            data = feature_vector.model_dump()
            serializable_data = self._make_serializable(data)

            if self.use_msgpack:
                # MessagePack serialization (2-5x faster than JSON)
                return msgpack.packb(serializable_data, use_bin_type=True)
            else:
                # JSON fallback
                return json.dumps(serializable_data, default=str).encode("utf-8")

        except Exception as e:
            logger.error(f"Error serializing feature vector: {e}")
            # Graceful fallback to JSON
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

            # Convert back to FeatureVector with type restoration
            return FeatureVector(**self._restore_types(deserialized_data))

        except Exception as e:
            logger.error(f"Error deserializing feature vector: {e}")
            raise

    def _make_serializable(self, data: Any) -> Any:
        """Convert data to serializable format"""
        if isinstance(data, dict):
            return {k: self._make_serializable(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._make_serializable(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        elif isinstance(data, datetime):
            return data.isoformat()
        else:
            return data

    def _restore_types(self, data: Any) -> Any:
        """Restore data types from serialized format"""
        if isinstance(data, dict):
            restored = {}
            for key, value in data.items():
                if isinstance(value, str) and (
                    "T" in value and "Z" in value or "+" in value[-6:]
                ):
                    # Restore datetime
                    try:
                        if value.endswith("Z"):
                            restored[key] = datetime.fromisoformat(
                                value.replace("Z", "+00:00")
                            )
                        else:
                            restored[key] = datetime.fromisoformat(value)
                    except ValueError:
                        restored[key] = value
                else:
                    restored[key] = self._restore_types(value)
            return restored
        elif isinstance(data, list):
            return [self._restore_types(item) for item in data]
        else:
            return data

    async def cache_feature_vector_atomic(
        self, game_id: int, feature_vector: FeatureVector, ttl: Optional[int] = None
    ) -> bool:
        """
        Atomically cache feature vector with distributed locking
        Prevents race conditions during cache updates
        """
        if not self.redis_client:
            logger.error("Redis client not initialized")
            return False

        cache_key = self._generate_feature_key(game_id, feature_vector.feature_version)
        ttl = ttl or self.default_ttl

        try:
            # Use distributed lock to prevent race conditions
            async with self.distributed_lock(cache_key):
                serialized_data = self._serialize_feature_vector(feature_vector)

                # Atomic cache operation with TTL
                success = await self.redis_client.setex(cache_key, ttl, serialized_data)

                if success:
                    self.cache_stats["writes"] += 1
                    logger.debug(f"Cached feature vector for game {game_id}")
                    return True
                else:
                    self.cache_stats["errors"] += 1
                    return False

        except TimeoutError:
            logger.warning(f"Lock timeout for caching game {game_id}")
            self.cache_stats["errors"] += 1
            return False
        except Exception as e:
            logger.error(f"Error caching feature vector for game {game_id}: {e}")
            self.cache_stats["errors"] += 1
            return False

    async def get_feature_vector_atomic(
        self, game_id: int, feature_version: str
    ) -> Optional[FeatureVector]:
        """
        Atomically retrieve feature vector with consistency guarantees
        """
        if not self.redis_client:
            logger.error("Redis client not initialized")
            return None

        cache_key = self._generate_feature_key(game_id, feature_version)

        try:
            # Use pipeline for atomic read
            pipe = self.redis_client.pipeline()
            pipe.get(cache_key)
            pipe.ttl(cache_key)
            results = await pipe.execute()

            data, ttl_remaining = results

            if data is None:
                self.cache_stats["misses"] += 1
                logger.debug(f"Cache miss for game {game_id}")
                return None

            # Check if TTL is reasonable (not expired)
            if ttl_remaining <= 0:
                self.cache_stats["misses"] += 1
                logger.debug(f"Cache expired for game {game_id}")
                return None

            feature_vector = self._deserialize_feature_vector(data)
            self.cache_stats["hits"] += 1
            logger.debug(f"Cache hit for game {game_id}")
            return feature_vector

        except Exception as e:
            logger.error(f"Error retrieving feature vector for game {game_id}: {e}")
            self.cache_stats["errors"] += 1
            return None

    async def cache_batch_features_atomic(
        self,
        feature_vectors: List[Tuple[int, FeatureVector]],
        ttl: Optional[int] = None,
    ) -> int:
        """
        Atomically cache multiple feature vectors using Redis transactions
        Returns number of successfully cached features
        """
        if not self.redis_client or not feature_vectors:
            return 0

        ttl = ttl or self.default_ttl
        successful_caches = 0

        # Process in batches to avoid memory issues and lock contention
        for i in range(0, len(feature_vectors), self.max_batch_size):
            batch = feature_vectors[i : i + self.max_batch_size]

            try:
                # Use Redis transaction for atomic batch operation
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    # Prepare batch operations
                    for game_id, feature_vector in batch:
                        cache_key = self._generate_feature_key(
                            game_id, feature_vector.feature_version
                        )
                        serialized_data = self._serialize_feature_vector(feature_vector)
                        pipe.setex(cache_key, ttl, serialized_data)

                    # Execute transaction atomically
                    results = await pipe.execute()
                    successful_caches += sum(1 for result in results if result)

            except Exception as e:
                logger.error(
                    f"Error in batch caching (batch {i // self.max_batch_size + 1}): {e}"
                )
                self.cache_stats["errors"] += 1

        self.cache_stats["writes"] += successful_caches
        logger.info(
            f"Batch cached {successful_caches}/{len(feature_vectors)} feature vectors"
        )
        return successful_caches

    async def invalidate_game_cache_atomic(
        self, game_id: int, feature_version: Optional[str] = None
    ) -> int:
        """
        Atomically invalidate cache entries for a game
        Returns number of keys invalidated
        """
        if not self.redis_client:
            return 0

        try:
            if feature_version:
                # Invalidate specific version
                cache_key = self._generate_feature_key(game_id, feature_version)
                deleted = await self.redis_client.delete(cache_key)
                logger.debug(
                    f"Invalidated cache for game {game_id}, version {feature_version}"
                )
                return deleted
            else:
                # Invalidate all versions for the game
                pattern = f"{self.feature_key_prefix}:game:{game_id}:*"
                keys = await self.redis_client.keys(pattern)

                if keys:
                    deleted = await self.redis_client.delete(*keys)
                    logger.debug(
                        f"Invalidated {deleted} cache entries for game {game_id}"
                    )
                    return deleted
                else:
                    return 0

        except Exception as e:
            logger.error(f"Error invalidating cache for game {game_id}: {e}")
            return 0

    async def health_check(self) -> Dict[str, Any]:
        """Health check with connection and performance testing"""
        try:
            if not self.redis_client:
                return {"status": "unhealthy", "error": "Client not initialized"}

            start_time = asyncio.get_event_loop().time()

            # Test basic operations
            test_key = "health_check_test"
            await self.redis_client.set(test_key, "test_value", ex=10)
            result = await self.redis_client.get(test_key)
            await self.redis_client.delete(test_key)

            response_time = (asyncio.get_event_loop().time() - start_time) * 1000

            if result == b"test_value":
                return {
                    "status": "healthy",
                    "response_time_ms": round(response_time, 2),
                    "cache_stats": self.cache_stats.copy(),
                    "msgpack_enabled": self.use_msgpack,
                }
            else:
                return {"status": "unhealthy", "error": "Test operation failed"}

        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_operations = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = self.cache_stats["hits"] / max(total_operations, 1)

        return {
            **self.cache_stats.copy(),
            "hit_rate": round(hit_rate, 3),
            "total_operations": total_operations,
        }
