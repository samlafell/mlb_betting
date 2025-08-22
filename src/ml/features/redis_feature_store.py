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

try:
    from ...core.config import get_settings
except ImportError:
    # Fallback for environments where unified config is not available
    get_settings = None

# Import resilience components
try:
    from ..resilience import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError
    from ..resilience.fallback_strategies import fallback_strategies
    from ..resilience.degradation_manager import degradation_manager
except ImportError:
    # Fallback for environments where resilience module is not available
    CircuitBreaker = None
    CircuitBreakerConfig = None
    CircuitBreakerError = None
    fallback_strategies = None
    degradation_manager = None

# Import resource monitoring
try:
    from ..monitoring.resource_monitor import get_resource_monitor, check_resource_pressure
except ImportError:
    # Fallback for environments where resource monitor is not available
    get_resource_monitor = None
    check_resource_pressure = None

logger = logging.getLogger(__name__)


class RedisFeatureStore:
    """
    High-performance Redis feature store with MessagePack serialization
    Optimized for sub-100ms feature retrieval and efficient storage
    """

    def __init__(self, redis_url: Optional[str] = None):
        self.settings = get_settings()
        # Use Redis configuration from ML settings with security support
        if redis_url:
            self.redis_url = redis_url
        else:
            # Build Redis URL from configuration with authentication
            redis_config = self.settings.ml.redis
            auth_part = ""
            if hasattr(redis_config, 'password') and redis_config.password:
                # Support environment variable substitution
                password = redis_config.password
                if password.startswith('${') and password.endswith('}'):
                    import os
                    env_var = password[2:-1]
                    password = os.getenv(env_var, "")
                if password:
                    auth_part = f":{password}@"
            
            self.redis_url = f"redis://{auth_part}{redis_config.host}:{redis_config.port}/{redis_config.database}"
        self.redis_client: Optional[redis.Redis] = None
        
        # Initialize circuit breaker for resilience
        self.circuit_breaker: Optional[CircuitBreaker] = None
        self.fallback_strategy = None
        self.resilience_enabled = CircuitBreaker is not None

        # Initialize resource monitoring
        self.resource_monitor = None
        self.resource_monitoring_enabled = get_resource_monitor is not None

        # Get ML pipeline configuration
        try:
            from ...core.config import get_settings
            config = get_settings()
            ml_config = config.ml_pipeline
            configured_ttl = ml_config.feature_cache_ttl_seconds
            configured_socket_timeout = ml_config.redis_socket_timeout
            configured_pool_size = ml_config.redis_connection_pool_size
            self.max_retries = ml_config.redis_max_retries
            self.retry_delay = ml_config.redis_retry_delay_seconds
        except ImportError:
            # Fallback configuration
            configured_ttl = 900
            configured_socket_timeout = 5.0
            configured_pool_size = 20
            self.max_retries = 3
            self.retry_delay = 1.0

        # Cache configuration
        self.default_ttl = configured_ttl
        self.feature_key_prefix = "ml:features"
        self.model_prediction_prefix = "ml:predictions"
        self.batch_key_prefix = "ml:batch"

        # Performance settings
        self.max_batch_size = 100
        self.connection_pool_size = configured_pool_size
        self.socket_timeout = configured_socket_timeout

        # Serialization settings
        self.use_msgpack = True  # Use MessagePack for 2-5x performance improvement
        self.compression_threshold = 1024  # Compress data larger than 1KB

        # Cache statistics with resource monitoring
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "writes": 0,
            "errors": 0,
            "avg_get_time_ms": 0.0,
            "avg_set_time_ms": 0.0,
            "resource_pressure_events": 0,
            "cache_size_limit_hits": 0,
        }

    async def initialize(self) -> bool:
        """Initialize Redis connection with optimized settings, retry logic, and resilience patterns"""
        # Initialize resource monitor if available
        if self.resource_monitoring_enabled and not self.resource_monitor:
            try:
                self.resource_monitor = await get_resource_monitor()
                if not self.resource_monitor._running:
                    await self.resource_monitor.start_monitoring()
                logger.info("✅ Redis Feature Store resource monitoring initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize resource monitoring: {e}")
                self.resource_monitoring_enabled = False

        # Initialize circuit breaker if resilience is enabled
        if self.resilience_enabled:
            try:
                config = CircuitBreakerConfig.from_unified_config("redis")
                self.circuit_breaker = CircuitBreaker("redis_feature_store", config)
                
                # Get fallback strategy
                if fallback_strategies:
                    self.fallback_strategy = fallback_strategies.get_strategy("redis")
                
                # Register with degradation manager
                if degradation_manager:
                    await degradation_manager.register_health_check("redis", self._health_check)
                    await degradation_manager.register_fallback("redis", self._activate_fallback)
                    await degradation_manager.register_recovery("redis", self._recovery_check)
                
                logger.info("✅ Redis Feature Store resilience patterns initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize resilience patterns: {e}")
                self.resilience_enabled = False
        
        max_retries = self.max_retries
        retry_delay = self.retry_delay
        
        for attempt in range(max_retries):
            try:
                # Build Redis connection with security configuration
                redis_config = self.settings.ml.redis
                connection_params = {
                    "encoding": "utf-8",
                    "decode_responses": False,  # Handle binary data for MessagePack
                    "socket_connect_timeout": redis_config.socket_timeout,
                    "socket_timeout": redis_config.socket_timeout,
                    "retry_on_timeout": True,
                    "health_check_interval": 30,
                    "max_connections": redis_config.connection_pool_size,
                }
                
                # Add SSL/TLS configuration if enabled
                if hasattr(redis_config, 'ssl_enabled') and redis_config.ssl_enabled:
                    import ssl
                    ssl_context = ssl.create_default_context()
                    
                    # Configure SSL certificates if provided
                    if hasattr(redis_config, 'ssl_cert_path') and redis_config.ssl_cert_path:
                        ssl_context.load_cert_chain(
                            redis_config.ssl_cert_path,
                            redis_config.ssl_key_path if hasattr(redis_config, 'ssl_key_path') else None
                        )
                    
                    # Configure CA certificate if provided
                    if hasattr(redis_config, 'ssl_ca_path') and redis_config.ssl_ca_path:
                        ssl_context.load_verify_locations(redis_config.ssl_ca_path)
                    
                    connection_params["ssl"] = ssl_context
                    connection_params["ssl_check_hostname"] = False  # Allow self-signed certificates
                    
                    # Use rediss:// scheme for SSL connections
                    redis_url = self.redis_url.replace("redis://", "rediss://")
                    logger.info("Redis SSL/TLS encryption enabled")
                else:
                    redis_url = self.redis_url
                
                self.redis_client = redis.from_url(redis_url, **connection_params)

                # Test connection with timeout
                await asyncio.wait_for(self.redis_client.ping(), timeout=5.0)
                
                # Log connection success without exposing credentials
                safe_url = self._mask_credentials_in_url(redis_url)
                logger.info(f"✅ Redis feature store initialized: {safe_url}")
                return True

            except asyncio.TimeoutError:
                logger.warning(f"Redis connection timeout on attempt {attempt + 1}/{max_retries}")
            except redis.ConnectionError as e:
                logger.warning(f"Redis connection failed on attempt {attempt + 1}/{max_retries}: {e}")
            except Exception as e:
                logger.warning(f"Redis initialization failed on attempt {attempt + 1}/{max_retries}: {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

        logger.error(f"❌ Failed to initialize Redis feature store after {max_retries} attempts")
        self.redis_client = None
        return False

    def _mask_credentials_in_url(self, redis_url: str) -> str:
        """Mask credentials in Redis URL for safe logging"""
        import re
        # Replace password in redis://username:password@host:port/db format
        masked_url = re.sub(r'://([^:]+:)[^@]+(@)', r'://\1***\2', redis_url)
        return masked_url

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis feature store connection closed")

    async def cache_feature_vector(
        self, game_id: int, feature_vector: FeatureVector, ttl: Optional[int] = None
    ) -> bool:
        """
        Cache feature vector with MessagePack optimization and circuit breaker protection

        Args:
            game_id: Game ID for caching key
            feature_vector: FeatureVector to cache
            ttl: Time to live in seconds (default: self.default_ttl)

        Returns:
            True if cached successfully, False otherwise
        """
        # Try circuit breaker protected operation first
        if self.resilience_enabled and self.circuit_breaker:
            try:
                return await self.circuit_breaker.call(
                    self._cache_feature_vector_impl, game_id, feature_vector, ttl
                )
            except CircuitBreakerError:
                logger.warning(f"Circuit breaker open for Redis - using fallback for game {game_id}")
                return await self._cache_with_fallback(game_id, feature_vector, ttl)
            except Exception as e:
                logger.error(f"Circuit breaker error for cache operation: {e}")
                return await self._cache_with_fallback(game_id, feature_vector, ttl)
        
        # Fallback to direct implementation if resilience not enabled
        return await self._cache_feature_vector_impl(game_id, feature_vector, ttl)
    
    async def _cache_feature_vector_impl(
        self, game_id: int, feature_vector: FeatureVector, ttl: Optional[int] = None
    ) -> bool:
        """Internal implementation of cache_feature_vector with resource monitoring"""
        start_time = datetime.utcnow()

        try:
            # Check resource pressure before caching
            if self.resource_monitoring_enabled and check_resource_pressure:
                resource_pressure = await check_resource_pressure()
                if resource_pressure:
                    logger.warning(f"Resource pressure detected before caching game {game_id}")
                    self.cache_stats["resource_pressure_events"] += 1
                    
                    # Attempt resource cleanup
                    if self.resource_monitor:
                        cleanup_results = await self.resource_monitor.force_cleanup()
                        logger.debug(f"Resource cleanup before caching: {cleanup_results}")

            if not self.redis_client:
                initialization_success = await self.initialize()
                if not initialization_success:
                    logger.error(f"Cannot cache feature vector for game {game_id}: Redis initialization failed")
                    return False

            # Generate cache key
            cache_key = self._generate_feature_key(
                game_id, feature_vector.feature_version
            )

            # Serialize feature vector
            serialized_data = self._serialize_feature_vector(feature_vector)

            # Set TTL
            ttl = ttl or self.default_ttl

            # Store in Redis with connection validation
            try:
                await self.redis_client.setex(cache_key, ttl, serialized_data)
            except redis.ConnectionError:
                logger.warning(f"Redis connection lost during cache operation for game {game_id}, attempting reconnection")
                initialization_success = await self.initialize()
                if initialization_success:
                    await self.redis_client.setex(cache_key, ttl, serialized_data)
                else:
                    raise redis.ConnectionError("Failed to reconnect to Redis")

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
    
    async def _cache_with_fallback(
        self, game_id: int, feature_vector: FeatureVector, ttl: Optional[int] = None
    ) -> bool:
        """Cache using fallback strategy when Redis is unavailable"""
        if self.fallback_strategy:
            return await self.fallback_strategy.cache_feature_vector(game_id, feature_vector, ttl)
        
        logger.warning(f"No fallback available for caching game {game_id}")
        return False

    async def get_feature_vector(
        self, game_id: int, feature_version: str = "v2.1"
    ) -> Optional[FeatureVector]:
        """
        Retrieve cached feature vector with MessagePack optimization and circuit breaker protection

        Args:
            game_id: Game ID to retrieve
            feature_version: Feature version to retrieve

        Returns:
            FeatureVector if found, None otherwise
        """
        # Try circuit breaker protected operation first
        if self.resilience_enabled and self.circuit_breaker:
            try:
                return await self.circuit_breaker.call(
                    self._get_feature_vector_impl, game_id, feature_version
                )
            except CircuitBreakerError:
                logger.warning(f"Circuit breaker open for Redis - using fallback for game {game_id}")
                return await self._get_with_fallback(game_id, feature_version)
            except Exception as e:
                logger.error(f"Circuit breaker error for get operation: {e}")
                return await self._get_with_fallback(game_id, feature_version)
        
        # Fallback to direct implementation if resilience not enabled
        return await self._get_feature_vector_impl(game_id, feature_version)
    
    async def _get_feature_vector_impl(
        self, game_id: int, feature_version: str = "v2.1"
    ) -> Optional[FeatureVector]:
        """Internal implementation of get_feature_vector"""
        start_time = datetime.utcnow()

        try:
            if not self.redis_client:
                initialization_success = await self.initialize()
                if not initialization_success:
                    logger.error(f"Cannot retrieve feature vector for game {game_id}: Redis initialization failed")
                    return None

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
    
    async def _get_with_fallback(
        self, game_id: int, feature_version: str = "v2.1"
    ) -> Optional[FeatureVector]:
        """Get feature vector using fallback strategy when Redis is unavailable"""
        if self.fallback_strategy:
            return await self.fallback_strategy.get_feature_vector(game_id, feature_version)
        
        logger.warning(f"No fallback available for retrieving game {game_id}")
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
        """Get cache performance statistics with resource monitoring data"""
        stats = self.cache_stats.copy()

        total_operations = stats["hits"] + stats["misses"]
        if total_operations > 0:
            stats["hit_rate"] = stats["hits"] / total_operations
        else:
            stats["hit_rate"] = 0.0

        # Add resource monitoring stats if available
        if self.resource_monitoring_enabled and self.resource_monitor:
            stats["resource_monitoring"] = {
                "enabled": True,
                "current_metrics": self.resource_monitor.get_current_metrics(),
                "active_alerts": len(self.resource_monitor.active_alerts),
            }
        else:
            stats["resource_monitoring"] = {"enabled": False}

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
    
    # Resilience pattern helper methods
    
    async def _health_check(self) -> bool:
        """Health check callback for degradation manager"""
        try:
            if not self.redis_client:
                return False
            
            await asyncio.wait_for(self.redis_client.ping(), timeout=2.0)
            return True
        except Exception:
            return False
    
    async def _activate_fallback(self) -> Any:
        """Activate fallback callback for degradation manager"""
        if self.fallback_strategy:
            await self.fallback_strategy.activate()
            logger.info("Redis fallback activated via degradation manager")
        return True
    
    async def _recovery_check(self) -> bool:
        """Recovery check callback for degradation manager"""
        try:
            # Test if Redis is back online
            is_healthy = await self._health_check()
            
            if is_healthy and self.fallback_strategy and self.fallback_strategy.fallback_active:
                # Deactivate fallback if Redis has recovered
                await self.fallback_strategy.deactivate()
                logger.info("Redis recovered - fallback deactivated")
            
            return is_healthy
        except Exception as e:
            logger.error(f"Redis recovery check failed: {e}")
            return False
    
    def get_resilience_status(self) -> Dict[str, Any]:
        """Get resilience-related status information"""
        status = {
            "resilience_enabled": self.resilience_enabled,
            "circuit_breaker": None,
            "fallback_strategy": None
        }
        
        if self.circuit_breaker:
            status["circuit_breaker"] = self.circuit_breaker.get_status()
        
        if self.fallback_strategy:
            status["fallback_strategy"] = self.fallback_strategy.get_status()
        
        return status
