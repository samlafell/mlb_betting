"""
Fallback Strategies for ML Pipeline Services
Provides alternative implementations when external services fail
"""

import asyncio
import logging
import json
import os
import pickle
from typing import Any, Dict, List, Optional, Union, Callable, Awaitable
from datetime import datetime, timedelta
from pathlib import Path

# Import ML pipeline components
try:
    from ..features.models import FeatureVector, TemporalFeatures, MarketFeatures
    from ...core.config import get_unified_config
except ImportError:
    FeatureVector = None
    TemporalFeatures = None
    MarketFeatures = None
    get_unified_config = None

logger = logging.getLogger(__name__)


class FallbackStrategy:
    """Base class for fallback strategies"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.fallback_active = False
        self.activation_time: Optional[datetime] = None
        self.fallback_count = 0
        self.success_count = 0
        self.error_count = 0
    
    async def activate(self) -> bool:
        """Activate the fallback strategy"""
        if self.fallback_active:
            return True
        
        try:
            success = await self._activate_implementation()
            if success:
                self.fallback_active = True
                self.activation_time = datetime.utcnow()
                self.fallback_count += 1
                logger.warning(f"Activated fallback strategy for {self.service_name}")
            return success
        except Exception as e:
            logger.error(f"Failed to activate fallback for {self.service_name}: {e}")
            return False
    
    async def deactivate(self) -> bool:
        """Deactivate the fallback strategy"""
        if not self.fallback_active:
            return True
        
        try:
            success = await self._deactivate_implementation()
            if success:
                self.fallback_active = False
                self.activation_time = None
                logger.info(f"Deactivated fallback strategy for {self.service_name}")
            return success
        except Exception as e:
            logger.error(f"Failed to deactivate fallback for {self.service_name}: {e}")
            return False
    
    async def _activate_implementation(self) -> bool:
        """Override in subclasses"""
        raise NotImplementedError
    
    async def _deactivate_implementation(self) -> bool:
        """Override in subclasses"""
        return True  # Default: simple deactivation
    
    def get_status(self) -> Dict[str, Any]:
        """Get fallback strategy status"""
        return {
            "service_name": self.service_name,
            "active": self.fallback_active,
            "activation_time": self.activation_time.isoformat() if self.activation_time else None,
            "fallback_count": self.fallback_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "uptime_seconds": (
                (datetime.utcnow() - self.activation_time).total_seconds()
                if self.activation_time else 0
            )
        }


class RedisFeatureStoreFallback(FallbackStrategy):
    """
    Fallback strategy for Redis Feature Store
    Computes features on-demand without caching when Redis is unavailable
    """
    
    def __init__(self):
        super().__init__("redis_feature_store")
        self.local_cache: Dict[str, Any] = {}
        self.max_cache_size = 1000
        self.cache_ttl_seconds = 300  # 5 minutes local cache
        
    async def _activate_implementation(self) -> bool:
        """Activate in-memory caching fallback"""
        self.local_cache.clear()
        logger.info("Redis fallback: Activated in-memory feature caching")
        return True
    
    async def get_feature_vector(self, game_id: int, feature_version: str = "v2.1") -> Optional[FeatureVector]:
        """Get feature vector using fallback strategy"""
        if not self.fallback_active:
            return None
        
        cache_key = f"game:{game_id}:version:{feature_version}"
        
        # Check local cache first
        if cache_key in self.local_cache:
            cached_item = self.local_cache[cache_key]
            if datetime.utcnow() - cached_item["timestamp"] < timedelta(seconds=self.cache_ttl_seconds):
                self.success_count += 1
                return cached_item["data"]
            else:
                # Remove expired item
                del self.local_cache[cache_key]
        
        # Feature vector not in cache - would need to compute
        # This would integrate with the feature pipeline to compute features
        logger.debug(f"Redis fallback: Feature vector not available for game {game_id}")
        return None
    
    async def cache_feature_vector(self, game_id: int, feature_vector: FeatureVector, ttl: Optional[int] = None) -> bool:
        """Cache feature vector in local memory"""
        if not self.fallback_active:
            return False
        
        try:
            cache_key = f"game:{game_id}:version:{feature_vector.feature_version}"
            
            # Manage cache size
            if len(self.local_cache) >= self.max_cache_size:
                # Remove oldest items
                oldest_key = min(self.local_cache.keys(), 
                               key=lambda k: self.local_cache[k]["timestamp"])
                del self.local_cache[oldest_key]
            
            # Cache the feature vector
            self.local_cache[cache_key] = {
                "data": feature_vector,
                "timestamp": datetime.utcnow()
            }
            
            self.success_count += 1
            logger.debug(f"Redis fallback: Cached feature vector for game {game_id}")
            return True
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Redis fallback: Error caching feature vector: {e}")
            return False
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get local cache statistics"""
        if not self.fallback_active:
            return {"active": False}
        
        return {
            "active": True,
            "cache_size": len(self.local_cache),
            "max_cache_size": self.max_cache_size,
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "success_count": self.success_count,
            "error_count": self.error_count
        }


class DatabaseFallback(FallbackStrategy):
    """
    Fallback strategy for Database connections
    Uses cached metadata and read-only operations when database is unavailable
    """
    
    def __init__(self):
        super().__init__("database")
        self.metadata_cache: Dict[str, Any] = {}
        self.cache_file_path = Path("./cache/database_metadata.json")
        
    async def _activate_implementation(self) -> bool:
        """Activate database fallback with cached metadata"""
        try:
            # Load cached metadata if available
            if self.cache_file_path.exists():
                with open(self.cache_file_path, 'r') as f:
                    self.metadata_cache = json.load(f)
                logger.info(f"Database fallback: Loaded {len(self.metadata_cache)} cached metadata items")
            else:
                logger.warning("Database fallback: No cached metadata available")
            
            return True
        except Exception as e:
            logger.error(f"Database fallback activation failed: {e}")
            return False
    
    async def get_cached_metadata(self, key: str) -> Optional[Any]:
        """Get cached metadata"""
        if not self.fallback_active:
            return None
        
        if key in self.metadata_cache:
            self.success_count += 1
            return self.metadata_cache[key]
        
        logger.debug(f"Database fallback: Metadata not available for key {key}")
        return None
    
    async def cache_metadata(self, key: str, data: Any):
        """Cache metadata for future fallback use"""
        self.metadata_cache[key] = {
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Persist to file
        try:
            self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file_path, 'w') as f:
                json.dump(self.metadata_cache, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error persisting database metadata cache: {e}")


class MLflowFallback(FallbackStrategy):
    """
    Fallback strategy for MLflow Model Registry
    Uses local model files when MLflow registry is unavailable
    """
    
    def __init__(self):
        super().__init__("mlflow")
        self.local_model_paths: Dict[str, str] = {}
        self.model_cache_dir = Path("./cache/models")
        
    async def _activate_implementation(self) -> bool:
        """Activate local model loading fallback"""
        try:
            # Scan for local model files
            if self.model_cache_dir.exists():
                for model_file in self.model_cache_dir.glob("*.pkl"):
                    model_name = model_file.stem
                    self.local_model_paths[model_name] = str(model_file)
                
                logger.info(f"MLflow fallback: Found {len(self.local_model_paths)} local models")
            else:
                logger.warning("MLflow fallback: No local model cache directory found")
            
            return True
        except Exception as e:
            logger.error(f"MLflow fallback activation failed: {e}")
            return False
    
    async def load_model(self, model_name: str, version: Optional[str] = None):
        """Load model from local cache"""
        if not self.fallback_active:
            return None
        
        # Try version-specific model first
        if version:
            versioned_name = f"{model_name}_v{version}"
            if versioned_name in self.local_model_paths:
                return await self._load_model_file(self.local_model_paths[versioned_name])
        
        # Try base model name
        if model_name in self.local_model_paths:
            return await self._load_model_file(self.local_model_paths[model_name])
        
        logger.warning(f"MLflow fallback: Model {model_name} not available locally")
        return None
    
    async def _load_model_file(self, file_path: str):
        """Load model from local file"""
        try:
            with open(file_path, 'rb') as f:
                model = pickle.load(f)
            
            self.success_count += 1
            logger.debug(f"MLflow fallback: Loaded model from {file_path}")
            return model
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"MLflow fallback: Error loading model from {file_path}: {e}")
            return None
    
    async def cache_model(self, model_name: str, model: Any, version: Optional[str] = None):
        """Cache model locally for future fallback use"""
        try:
            self.model_cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Create versioned filename
            if version:
                filename = f"{model_name}_v{version}.pkl"
            else:
                filename = f"{model_name}.pkl"
            
            file_path = self.model_cache_dir / filename
            
            with open(file_path, 'wb') as f:
                pickle.dump(model, f)
            
            self.local_model_paths[model_name] = str(file_path)
            logger.debug(f"MLflow fallback: Cached model {model_name} to {file_path}")
            
        except Exception as e:
            logger.error(f"Error caching model {model_name}: {e}")


class ExternalAPIFallback(FallbackStrategy):
    """
    Fallback strategy for External APIs
    Uses cached data and historical averages when APIs are unavailable
    """
    
    def __init__(self):
        super().__init__("external_apis")
        self.historical_data: Dict[str, Any] = {}
        self.cache_file_path = Path("./cache/api_data.json")
        
    async def _activate_implementation(self) -> bool:
        """Activate API fallback with cached/historical data"""
        try:
            if self.cache_file_path.exists():
                with open(self.cache_file_path, 'r') as f:
                    self.historical_data = json.load(f)
                logger.info(f"API fallback: Loaded {len(self.historical_data)} cached data points")
            else:
                # Generate some default historical averages
                self.historical_data = self._generate_default_data()
                logger.warning("API fallback: Using default historical averages")
            
            return True
        except Exception as e:
            logger.error(f"API fallback activation failed: {e}")
            return False
    
    def _generate_default_data(self) -> Dict[str, Any]:
        """Generate default fallback data"""
        return {
            "default_odds": {"home": -110, "away": -110, "over": -110, "under": -110},
            "average_line_movement": 0.02,
            "typical_volume": 1000,
            "default_consensus": 0.5,
            "historical_averages": {
                "mlb_home_advantage": 0.54,
                "average_total": 8.5,
                "typical_spread": 1.5
            }
        }
    
    async def get_fallback_data(self, data_type: str, key: str) -> Optional[Any]:
        """Get fallback data for API requests"""
        if not self.fallback_active:
            return None
        
        try:
            if data_type in self.historical_data:
                data = self.historical_data[data_type]
                if isinstance(data, dict) and key in data:
                    self.success_count += 1
                    return data[key]
                elif not isinstance(data, dict):
                    self.success_count += 1
                    return data
            
            logger.debug(f"API fallback: No data available for {data_type}.{key}")
            return None
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"API fallback error: {e}")
            return None
    
    async def cache_api_data(self, data_type: str, key: str, data: Any):
        """Cache API data for future fallback use"""
        if data_type not in self.historical_data:
            self.historical_data[data_type] = {}
        
        if isinstance(self.historical_data[data_type], dict):
            self.historical_data[data_type][key] = {
                "data": data,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # Persist to file
        try:
            self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file_path, 'w') as f:
                json.dump(self.historical_data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error persisting API data cache: {e}")


class FallbackStrategies:
    """
    Centralized management of all fallback strategies
    """
    
    def __init__(self):
        self.strategies: Dict[str, FallbackStrategy] = {
            "redis": RedisFeatureStoreFallback(),
            "database": DatabaseFallback(), 
            "mlflow": MLflowFallback(),
            "external_apis": ExternalAPIFallback(),
        }
        
    async def activate_fallback(self, service_name: str) -> bool:
        """Activate fallback for a specific service"""
        if service_name not in self.strategies:
            logger.warning(f"No fallback strategy available for {service_name}")
            return False
        
        return await self.strategies[service_name].activate()
    
    async def deactivate_fallback(self, service_name: str) -> bool:
        """Deactivate fallback for a specific service"""
        if service_name not in self.strategies:
            return True  # Nothing to deactivate
        
        return await self.strategies[service_name].deactivate()
    
    async def is_fallback_active(self, service_name: str) -> bool:
        """Check if fallback is active for a service"""
        if service_name not in self.strategies:
            return False
        
        return self.strategies[service_name].fallback_active
    
    def get_strategy(self, service_name: str) -> Optional[FallbackStrategy]:
        """Get fallback strategy for a service"""
        return self.strategies.get(service_name)
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all fallback strategies"""
        return {
            name: strategy.get_status()
            for name, strategy in self.strategies.items()
        }
    
    async def activate_all_fallbacks(self):
        """Activate all available fallback strategies (emergency mode)"""
        results = {}
        for service_name, strategy in self.strategies.items():
            results[service_name] = await strategy.activate()
        
        logger.warning(f"Emergency mode: Activated all fallbacks - {results}")
        return results
    
    async def deactivate_all_fallbacks(self):
        """Deactivate all fallback strategies"""
        results = {}
        for service_name, strategy in self.strategies.items():
            results[service_name] = await strategy.deactivate()
        
        logger.info(f"Deactivated all fallbacks - {results}")
        return results


# Global fallback strategies instance
fallback_strategies = FallbackStrategies()


# Convenience functions
async def get_fallback_strategies() -> FallbackStrategies:
    """Get the global fallback strategies instance"""
    return fallback_strategies


async def activate_fallback(service_name: str) -> bool:
    """Activate fallback for a service"""
    return await fallback_strategies.activate_fallback(service_name)


async def deactivate_fallback(service_name: str) -> bool:
    """Deactivate fallback for a service"""
    return await fallback_strategies.deactivate_fallback(service_name)


async def is_fallback_active(service_name: str) -> bool:
    """Check if fallback is active for a service"""
    return await fallback_strategies.is_fallback_active(service_name)