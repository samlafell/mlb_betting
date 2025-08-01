"""
Unified Feature Pipeline for Production and Backtesting

Provides consistent feature engineering across production predictions and backtesting
contexts. Integrates Feast, Redis, and database sources with intelligent fallback
strategies to ensure production-backtest parity.

Key capabilities:
- Unified feature serving for production and backtesting
- Intelligent fallback between Feast, Redis, and database sources
- Production-backtest parity validation
- Feature versioning and consistency checking
- Performance optimization with caching and batching
"""

import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

from .models import FeatureVector
from .feast_integration import get_feast_store, FeastFeatureStore
from .redis_feature_store import RedisFeatureStore
from .feature_pipeline import FeaturePipeline
from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...data.database import UnifiedRepository

logger = get_logger(__name__, LogComponent.ML)


class FeatureSource(str, Enum):
    """Feature source enumeration"""
    FEAST = "feast"
    REDIS = "redis"
    DATABASE = "database"
    HYBRID = "hybrid"


class UnifiedFeaturePipeline:
    """
    Unified feature pipeline ensuring production-backtest parity.
    
    Provides consistent feature engineering across different execution contexts
    by using identical feature pipelines and intelligent source selection.
    """
    
    def __init__(self, repository: UnifiedRepository):
        """
        Initialize unified feature pipeline.
        
        Args:
            repository: Unified repository for database access
        """
        self.repository = repository
        self.settings = get_settings()
        self.logger = get_logger(__name__, LogComponent.ML)
        
        # Initialize feature sources
        self.feast_store: Optional[FeastFeatureStore] = None
        self.redis_store: Optional[RedisFeatureStore] = None
        self.database_pipeline: Optional[FeaturePipeline] = None
        
        # Source priority order for fallback
        self.source_priority = [
            FeatureSource.REDIS,    # Fastest
            FeatureSource.FEAST,    # Production store
            FeatureSource.DATABASE  # Fallback
        ]
        
        # Performance tracking
        self.metrics = {
            "feature_requests": 0,
            "feast_hits": 0,
            "redis_hits": 0,
            "database_hits": 0,
            "source_failures": 0,
            "parity_validations": 0,
            "consistency_checks": 0
        }
        
        # Thread pool for parallel feature retrieval
        self._thread_pool = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="unified_features"
        )
        
        self.logger.info("Initialized unified feature pipeline")
    
    async def initialize(self) -> bool:
        """
        Initialize all feature sources.
        
        Returns:
            True if at least one source is available
        """
        sources_available = 0
        
        try:
            # Initialize Feast store
            try:
                self.feast_store = await get_feast_store()
                self.logger.info("✅ Feast feature store initialized")
                sources_available += 1
            except Exception as e:
                self.logger.warning(f"⚠️ Feast store initialization failed: {e}")
            
            # Initialize Redis store
            try:
                self.redis_store = RedisFeatureStore()
                redis_ready = await self.redis_store.initialize()
                if redis_ready:
                    self.logger.info("✅ Redis feature store initialized")
                    sources_available += 1
                else:
                    self.logger.warning("⚠️ Redis store not available")
                    self.redis_store = None
            except Exception as e:
                self.logger.warning(f"⚠️ Redis store initialization failed: {e}")
                self.redis_store = None
            
            # Initialize database pipeline
            try:
                self.database_pipeline = FeaturePipeline()
                self.logger.info("✅ Database feature pipeline initialized")
                sources_available += 1
            except Exception as e:
                self.logger.warning(f"⚠️ Database pipeline initialization failed: {e}")
            
            if sources_available == 0:
                self.logger.error("❌ No feature sources available")
                return False
            
            self.logger.info(f"✅ Unified pipeline initialized with {sources_available} sources")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize unified feature pipeline: {e}")
            return False
    
    async def get_features_for_production(
        self,
        game_ids: List[int],
        feature_version: str = "v2.1"
    ) -> Dict[int, Optional[FeatureVector]]:
        """
        Get features for production predictions with optimized source selection.
        
        Args:
            game_ids: List of game IDs
            feature_version: Feature version to retrieve
            
        Returns:
            Dictionary mapping game_id to FeatureVector
        """
        self.logger.info(f"Getting production features for {len(game_ids)} games")
        self.metrics["feature_requests"] += len(game_ids)
        
        results = {}
        remaining_games = set(game_ids)
        
        # Try Redis first (fastest for production)
        if self.redis_store and remaining_games:
            try:
                redis_results = await self.redis_store.get_batch_features(
                    list(remaining_games), feature_version
                )
                
                for game_id, feature_vector in redis_results.items():
                    if feature_vector is not None:
                        results[game_id] = feature_vector
                        remaining_games.remove(game_id)
                        self.metrics["redis_hits"] += 1
                
                self.logger.debug(f"Redis provided features for {len(redis_results) - len(remaining_games)} games")
                
            except Exception as e:
                self.logger.warning(f"Redis batch retrieval failed: {e}")
                self.metrics["source_failures"] += 1
        
        # Try Feast for remaining games
        if self.feast_store and remaining_games:
            try:
                feast_results = await self.feast_store.get_online_features(
                    list(remaining_games), feature_version
                )
                
                for game_id, feature_vector in feast_results.items():
                    if feature_vector is not None and game_id in remaining_games:
                        results[game_id] = feature_vector
                        remaining_games.remove(game_id)
                        self.metrics["feast_hits"] += 1
                        
                        # Cache in Redis for future requests
                        if self.redis_store:
                            await self.redis_store.cache_feature_vector(
                                game_id, feature_vector
                            )
                
                self.logger.debug(f"Feast provided features for {len(feast_results)} games")
                
            except Exception as e:
                self.logger.warning(f"Feast retrieval failed: {e}")
                self.metrics["source_failures"] += 1
        
        # Use database pipeline for remaining games
        if self.database_pipeline and remaining_games:
            try:
                cutoff_time = datetime.utcnow() - timedelta(minutes=60)
                
                for game_id in remaining_games:
                    feature_vector = await self.database_pipeline.extract_features_for_game(
                        game_id, cutoff_time
                    )
                    
                    if feature_vector is not None:
                        results[game_id] = feature_vector
                        self.metrics["database_hits"] += 1
                        
                        # Cache in both Redis and update Feast
                        if self.redis_store:
                            await self.redis_store.cache_feature_vector(
                                game_id, feature_vector
                            )
                
                self.logger.debug(f"Database provided features for {len(remaining_games)} games")
                
            except Exception as e:
                self.logger.warning(f"Database feature extraction failed: {e}")
                self.metrics["source_failures"] += 1
        
        # Fill missing games with None
        for game_id in game_ids:
            if game_id not in results:
                results[game_id] = None
        
        success_rate = len([r for r in results.values() if r is not None]) / len(game_ids)
        self.logger.info(f"Production features: {success_rate:.1%} success rate")
        
        return results
    
    async def get_features_for_backtesting(
        self,
        game_ids: List[int],
        start_time: datetime,
        end_time: datetime,
        feature_version: str = "v2.1"
    ) -> Dict[int, Optional[FeatureVector]]:
        """
        Get features for backtesting with historical point-in-time consistency.
        
        Args:
            game_ids: List of game IDs
            start_time: Start of time range
            end_time: End of time range (point-in-time cutoff)
            feature_version: Feature version to retrieve
            
        Returns:
            Dictionary mapping game_id to FeatureVector
        """
        self.logger.info(f"Getting backtesting features for {len(game_ids)} games")
        self.metrics["feature_requests"] += len(game_ids)
        
        results = {}
        
        # For backtesting, prefer Feast historical features for consistency
        if self.feast_store:
            try:
                feast_results = await self.feast_store.get_historical_features(
                    game_ids, start_time, end_time, feature_version
                )
                
                for game_id, feature_vector in feast_results.items():
                    if feature_vector is not None:
                        results[game_id] = feature_vector
                        self.metrics["feast_hits"] += 1
                
                self.logger.debug(f"Feast historical provided features for {len(feast_results)} games")
                
            except Exception as e:
                self.logger.warning(f"Feast historical retrieval failed: {e}")
                self.metrics["source_failures"] += 1
        
        # Fallback to database pipeline for missing games
        remaining_games = [gid for gid in game_ids if gid not in results or results[gid] is None]
        
        if self.database_pipeline and remaining_games:
            try:
                for game_id in remaining_games:
                    feature_vector = await self.database_pipeline.extract_features_for_game(
                        game_id, end_time  # Use end_time as cutoff for historical consistency
                    )
                    
                    if feature_vector is not None:
                        results[game_id] = feature_vector
                        self.metrics["database_hits"] += 1
                
                self.logger.debug(f"Database provided features for {len(remaining_games)} games")
                
            except Exception as e:
                self.logger.warning(f"Database backtesting extraction failed: {e}")
                self.metrics["source_failures"] += 1
        
        # Fill missing games with None
        for game_id in game_ids:
            if game_id not in results:
                results[game_id] = None
        
        success_rate = len([r for r in results.values() if r is not None]) / len(game_ids)
        self.logger.info(f"Backtesting features: {success_rate:.1%} success rate")
        
        return results
    
    async def validate_production_backtest_parity(
        self,
        game_ids: List[int],
        feature_version: str = "v2.1",
        tolerance: float = 0.001
    ) -> Dict[str, Any]:
        """
        Validate feature consistency between production and backtesting contexts.
        
        Args:
            game_ids: Game IDs to validate
            feature_version: Feature version to validate
            tolerance: Numerical tolerance for comparisons
            
        Returns:
            Validation results with consistency metrics
        """
        self.logger.info(f"Validating production-backtest parity for {len(game_ids)} games")
        self.metrics["parity_validations"] += 1
        
        validation_results = {
            "total_games": len(game_ids),
            "consistent_games": 0,
            "inconsistent_games": 0,
            "validation_details": [],
            "overall_consistency": True,
            "consistency_rate": 0.0
        }
        
        try:
            # Get features from production sources
            production_features = await self.get_features_for_production(
                game_ids, feature_version
            )
            
            # Get features from backtesting sources (using historical approach)
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=1)
            backtesting_features = await self.get_features_for_backtesting(
                game_ids, start_time, end_time, feature_version
            )
            
            # Compare features for each game
            for game_id in game_ids:
                prod_features = production_features.get(game_id)
                backtest_features = backtesting_features.get(game_id)
                
                # Validate consistency using Feast integration if available
                if self.feast_store:
                    game_validation = await self.feast_store.validate_feature_consistency(
                        game_id, prod_features, backtest_features, tolerance
                    )
                else:
                    # Fallback validation
                    game_validation = self._validate_feature_consistency_fallback(
                        game_id, prod_features, backtest_features, tolerance
                    )
                
                validation_results["validation_details"].append(game_validation)
                
                if game_validation["consistent"]:
                    validation_results["consistent_games"] += 1
                else:
                    validation_results["inconsistent_games"] += 1
                    validation_results["overall_consistency"] = False
            
            # Calculate consistency rate
            if validation_results["total_games"] > 0:
                consistency_rate = validation_results["consistent_games"] / validation_results["total_games"]
                validation_results["consistency_rate"] = consistency_rate
            
            self.logger.info(f"Parity validation: {validation_results['consistency_rate']:.2%} consistent")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Parity validation failed: {e}")
            validation_results["error"] = str(e)
            validation_results["overall_consistency"] = False
            return validation_results
    
    def _validate_feature_consistency_fallback(
        self,
        game_id: int,
        production_features: Optional[FeatureVector],
        backtesting_features: Optional[FeatureVector],
        tolerance: float = 0.001
    ) -> Dict[str, Any]:
        """
        Fallback feature consistency validation when Feast is not available.
        """
        validation_result = {
            "game_id": game_id,
            "consistent": True,
            "inconsistencies": [],
            "missing_features": [],
            "total_features": 0,
            "consistent_features": 0
        }
        
        try:
            # Handle case where one or both feature sets are None
            if production_features is None and backtesting_features is None:
                return validation_result
            
            if production_features is None or backtesting_features is None:
                validation_result["consistent"] = False
                validation_result["inconsistencies"].append(
                    "One feature set is None while the other is not"
                )
                return validation_result
            
            # Compare feature vectors
            prod_dict = production_features.model_dump()
            backtest_dict = backtesting_features.model_dump()
            
            # Get all unique feature names
            all_features = set(prod_dict.keys()) | set(backtest_dict.keys())
            validation_result["total_features"] = len(all_features)
            
            for feature_name in all_features:
                if feature_name not in prod_dict:
                    validation_result["missing_features"].append(f"production:{feature_name}")
                    validation_result["consistent"] = False
                    continue
                    
                if feature_name not in backtest_dict:
                    validation_result["missing_features"].append(f"backtesting:{feature_name}")
                    validation_result["consistent"] = False
                    continue
                
                # Compare values
                prod_val = prod_dict[feature_name]
                backtest_val = backtest_dict[feature_name]
                
                if isinstance(prod_val, (int, float)) and isinstance(backtest_val, (int, float)):
                    if abs(prod_val - backtest_val) > tolerance:
                        validation_result["inconsistencies"].append({
                            "feature": feature_name,
                            "production": prod_val,
                            "backtesting": backtest_val,
                            "difference": abs(prod_val - backtest_val)
                        })
                        validation_result["consistent"] = False
                    else:
                        validation_result["consistent_features"] += 1
                else:
                    if prod_val != backtest_val:
                        validation_result["inconsistencies"].append({
                            "feature": feature_name,
                            "production": prod_val,
                            "backtesting": backtest_val,
                            "type": "exact_mismatch"
                        })
                        validation_result["consistent"] = False
                    else:
                        validation_result["consistent_features"] += 1
            
            return validation_result
            
        except Exception as e:
            validation_result["consistent"] = False
            validation_result["error"] = str(e)
            return validation_result
    
    async def cache_features(
        self,
        feature_vectors: List[Tuple[int, FeatureVector]],
        ttl: Optional[int] = None
    ) -> int:
        """
        Cache feature vectors in available stores.
        
        Args:
            feature_vectors: List of (game_id, feature_vector) tuples
            ttl: Time to live in seconds
            
        Returns:
            Number of successfully cached vectors
        """
        if not feature_vectors:
            return 0
        
        cached_count = 0
        
        # Cache in Redis if available
        if self.redis_store:
            try:
                redis_cached = await self.redis_store.cache_batch_features(
                    feature_vectors, ttl
                )
                cached_count = max(cached_count, redis_cached)
                self.logger.debug(f"Cached {redis_cached} features in Redis")
            except Exception as e:
                self.logger.warning(f"Redis batch caching failed: {e}")
        
        # Note: Feast caching would be handled by Feast's internal mechanisms
        # and is typically done during feature computation/materialization
        
        return cached_count
    
    async def invalidate_cache(
        self,
        game_ids: List[int],
        feature_version: Optional[str] = None
    ) -> bool:
        """
        Invalidate cached features for specific games.
        
        Args:
            game_ids: Game IDs to invalidate
            feature_version: Specific version to invalidate (or all if None)
            
        Returns:
            True if invalidation successful
        """
        try:
            if self.redis_store:
                for game_id in game_ids:
                    await self.redis_store.invalidate_game_cache(game_id, feature_version)
            
            self.logger.info(f"Invalidated cache for {len(game_ids)} games")
            return True
            
        except Exception as e:
            self.logger.error(f"Cache invalidation failed: {e}")
            return False
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Comprehensive health check of all feature sources.
        
        Returns:
            Health check results
        """
        health = {
            "status": "healthy",
            "sources": {},
            "metrics": self.get_pipeline_metrics()
        }
        
        # Check Feast store
        if self.feast_store:
            try:
                feast_health = await self.feast_store.health_check()
                health["sources"]["feast"] = feast_health
            except Exception as e:
                health["sources"]["feast"] = {"status": "unhealthy", "error": str(e)}
                health["status"] = "degraded"
        else:
            health["sources"]["feast"] = {"status": "unavailable"}
        
        # Check Redis store
        if self.redis_store:
            try:
                redis_health = await self.redis_store.health_check()
                health["sources"]["redis"] = redis_health
            except Exception as e:
                health["sources"]["redis"] = {"status": "unhealthy", "error": str(e)}
                health["status"] = "degraded"
        else:
            health["sources"]["redis"] = {"status": "unavailable"}
        
        # Check database pipeline
        if self.database_pipeline:
            try:
                db_stats = self.database_pipeline.get_pipeline_stats()
                health["sources"]["database"] = {
                    "status": "healthy",
                    "stats": db_stats
                }
            except Exception as e:
                health["sources"]["database"] = {"status": "unhealthy", "error": str(e)}
                health["status"] = "degraded"
        else:
            health["sources"]["database"] = {"status": "unavailable"}
        
        # Determine overall status
        available_sources = len([s for s in health["sources"].values() if s.get("status") == "healthy"])
        if available_sources == 0:
            health["status"] = "unhealthy"
        elif available_sources < len(health["sources"]):
            health["status"] = "degraded"
        
        return health
    
    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get pipeline performance metrics"""
        total_requests = self.metrics["feature_requests"]
        
        metrics = self.metrics.copy()
        
        if total_requests > 0:
            metrics["redis_hit_rate"] = self.metrics["redis_hits"] / total_requests
            metrics["feast_hit_rate"] = self.metrics["feast_hits"] / total_requests
            metrics["database_hit_rate"] = self.metrics["database_hits"] / total_requests
            metrics["overall_success_rate"] = (
                self.metrics["redis_hits"] + 
                self.metrics["feast_hits"] + 
                self.metrics["database_hits"]
            ) / total_requests
        else:
            metrics["redis_hit_rate"] = 0.0
            metrics["feast_hit_rate"] = 0.0
            metrics["database_hit_rate"] = 0.0
            metrics["overall_success_rate"] = 0.0
        
        return metrics


# Global instance
_unified_pipeline: Optional[UnifiedFeaturePipeline] = None


async def get_feature_pipeline(repository: UnifiedRepository) -> UnifiedFeaturePipeline:
    """
    Get or create global unified feature pipeline instance.
    
    Args:
        repository: Unified repository for database access
        
    Returns:
        UnifiedFeaturePipeline instance
    """
    global _unified_pipeline
    
    if _unified_pipeline is None:
        _unified_pipeline = UnifiedFeaturePipeline(repository)
        await _unified_pipeline.initialize()
    
    return _unified_pipeline


def create_unified_feature_pipeline(repository: UnifiedRepository) -> UnifiedFeaturePipeline:
    """
    Create a new unified feature pipeline instance.
    
    Args:
        repository: Unified repository for database access
        
    Returns:
        UnifiedFeaturePipeline instance
    """
    return UnifiedFeaturePipeline(repository)