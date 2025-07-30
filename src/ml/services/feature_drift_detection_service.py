"""
Feature Drift Detection Service
Monitors feature importance changes and distribution drift over time
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import json
import numpy as np
from scipy import stats
from collections import defaultdict

from ...core.config import get_settings
from ..database.connection_pool import get_db_connection, get_db_transaction
from .mlflow_integration import mlflow_service

logger = logging.getLogger(__name__)


@dataclass
class FeatureDriftResult:
    """Feature drift detection result"""
    feature_name: str
    feature_type: str
    baseline_importance: float
    current_importance: float
    importance_drift: float
    drift_score: float
    drift_detected: bool
    detection_method: str
    sample_size: int
    baseline_distribution: Dict[str, Any]
    current_distribution: Dict[str, Any]
    metadata: Dict[str, Any]


@dataclass
class DriftBaseline:
    """Baseline feature importance and distribution"""
    feature_name: str
    feature_type: str
    importance: float
    distribution_stats: Dict[str, Any]
    sample_size: int
    created_at: datetime


class FeatureDriftDetectionService:
    """
    Service for detecting feature drift and importance changes
    """
    
    def __init__(self):
        self.settings = get_settings()
        
        # Drift detection thresholds
        self.thresholds = {
            'importance_drift': 0.05,      # Alert if feature importance changes by 5%
            'distribution_drift': 0.1,     # KS test p-value threshold
            'psi_threshold': 0.2,          # Population Stability Index threshold
            'min_sample_size': 100,        # Minimum sample size for reliable drift detection
            'importance_threshold': 0.01   # Minimum importance to monitor
        }
        
        # Baseline cache
        self._baseline_cache = {}
        self._last_cache_update = None
        self.cache_ttl = timedelta(hours=6)
    
    async def detect_feature_drift(
        self, 
        model_name: str,
        model_version: str = None,
        lookback_days: int = 7
    ) -> List[FeatureDriftResult]:
        """
        Detect feature drift for a specific model
        
        Args:
            model_name: Name of the model to analyze
            model_version: Specific version (None for latest)
            lookback_days: Days of recent data to analyze
            
        Returns:
            List of drift detection results
        """
        try:
            # Get baseline feature importance
            baseline_features = await self._get_baseline_feature_importance(
                model_name, model_version
            )
            
            if not baseline_features:
                logger.warning(f"No baseline features found for model {model_name}")
                return []
            
            # Get current feature importance
            current_features = await self._get_current_feature_importance(
                model_name, model_version, lookback_days
            )
            
            if not current_features:
                logger.warning(f"No current features found for model {model_name}")
                return []
            
            # Compare and detect drift
            drift_results = []
            for feature_name, baseline in baseline_features.items():
                if feature_name in current_features:
                    current = current_features[feature_name]
                    drift_result = await self._analyze_feature_drift(
                        feature_name, baseline, current, model_name
                    )
                    if drift_result:
                        drift_results.append(drift_result)
            
            # Check for new features (not in baseline)
            new_features = set(current_features.keys()) - set(baseline_features.keys())
            if new_features:
                logger.info(f"New features detected for {model_name}: {new_features}")
            
            # Check for missing features (in baseline but not current)
            missing_features = set(baseline_features.keys()) - set(current_features.keys())
            if missing_features:
                logger.warning(f"Missing features for {model_name}: {missing_features}")
            
            logger.info(f"Drift detection completed for {model_name}: {len(drift_results)} features analyzed")
            return drift_results
            
        except Exception as e:
            logger.error(f"Error detecting feature drift for {model_name}: {e}")
            return []
    
    async def _get_baseline_feature_importance(
        self, 
        model_name: str, 
        model_version: str = None
    ) -> Dict[str, DriftBaseline]:
        """Get baseline feature importance from historical data"""
        try:
            # Check cache first
            cache_key = f"{model_name}_{model_version or 'latest'}"
            if (cache_key in self._baseline_cache and 
                self._last_cache_update and 
                datetime.utcnow() - self._last_cache_update < self.cache_ttl):
                return self._baseline_cache[cache_key]
            
            async with get_db_connection() as conn:
                # Get baseline feature importance from successful predictions
                # Look at the last 30 days of stable performance
                version_clause = "AND mp.model_version = $2" if model_version else ""
                params = [model_name]
                if model_version:
                    params.append(model_version)
                
                query = f"""
                    SELECT 
                        fv.temporal_features,
                        fv.market_features,
                        fv.team_features,
                        fv.betting_splits_features,
                        mp.feature_importance,
                        fv.created_at,
                        mp.accuracy,
                        mp.confidence_score
                    FROM curated.ml_predictions mp
                    JOIN curated.ml_feature_vectors fv ON mp.game_id = fv.game_id
                    WHERE mp.model_name = $1
                    {version_clause}
                    AND mp.created_at >= NOW() - INTERVAL '30 days'
                    AND mp.accuracy >= 0.5  -- Only use successful predictions
                    AND fv.feature_completeness_score >= 0.7
                    ORDER BY mp.created_at DESC
                    LIMIT 1000
                """
                
                rows = await conn.fetch(query, *params)
                
                if not rows:
                    return {}
                
                # Aggregate feature importance across predictions
                baseline_features = {}
                feature_values = defaultdict(list)
                
                for row in rows:
                    # Extract feature importance if available
                    if row['feature_importance']:
                        importance_data = json.loads(row['feature_importance'])
                        for feature_name, importance in importance_data.items():
                            if importance >= self.thresholds['importance_threshold']:
                                feature_values[feature_name].append(importance)
                    
                    # Also extract feature values for distribution analysis
                    for feature_type, features_json in [
                        ('temporal', row['temporal_features']),
                        ('market', row['market_features']),
                        ('team', row['team_features']),
                        ('betting_splits', row['betting_splits_features'])
                    ]:
                        if features_json:
                            features = json.loads(features_json)
                            for feature_name, value in features.items():
                                if isinstance(value, (int, float)):
                                    feature_key = f"{feature_type}_{feature_name}"
                                    feature_values[feature_key].append(value)
                
                # Create baseline objects
                for feature_name, values in feature_values.items():
                    if len(values) >= self.thresholds['min_sample_size']:
                        values_array = np.array(values)
                        baseline_features[feature_name] = DriftBaseline(
                            feature_name=feature_name,
                            feature_type=self._determine_feature_type(feature_name),
                            importance=np.mean(values_array),
                            distribution_stats={
                                'mean': float(np.mean(values_array)),
                                'std': float(np.std(values_array)),
                                'min': float(np.min(values_array)),
                                'max': float(np.max(values_array)),
                                'percentiles': {
                                    '25': float(np.percentile(values_array, 25)),
                                    '50': float(np.percentile(values_array, 50)),
                                    '75': float(np.percentile(values_array, 75))
                                }
                            },
                            sample_size=len(values),
                            created_at=datetime.utcnow()
                        )
                
                # Update cache
                self._baseline_cache[cache_key] = baseline_features
                self._last_cache_update = datetime.utcnow()
                
                return baseline_features
                
        except Exception as e:
            logger.error(f"Error getting baseline feature importance: {e}")
            return {}
    
    async def _get_current_feature_importance(
        self, 
        model_name: str, 
        model_version: str = None,
        lookback_days: int = 7
    ) -> Dict[str, DriftBaseline]:
        """Get current feature importance from recent data"""
        try:
            async with get_db_connection() as conn:
                version_clause = "AND mp.model_version = $3" if model_version else ""
                params = [model_name, lookback_days]
                if model_version:
                    params.append(model_version)
                
                query = f"""
                    SELECT 
                        fv.temporal_features,
                        fv.market_features,
                        fv.team_features,
                        fv.betting_splits_features,
                        mp.feature_importance,
                        fv.created_at,
                        mp.accuracy,
                        mp.confidence_score
                    FROM curated.ml_predictions mp
                    JOIN curated.ml_feature_vectors fv ON mp.game_id = fv.game_id
                    WHERE mp.model_name = $1
                    AND mp.created_at >= NOW() - INTERVAL '%s days'
                    {version_clause}
                    AND fv.feature_completeness_score >= 0.7
                    ORDER BY mp.created_at DESC
                """
                
                rows = await conn.fetch(query, *params)
                
                if not rows:
                    return {}
                
                # Aggregate current feature importance
                current_features = {}
                feature_values = defaultdict(list)
                
                for row in rows:
                    # Extract feature importance if available
                    if row['feature_importance']:
                        importance_data = json.loads(row['feature_importance'])
                        for feature_name, importance in importance_data.items():
                            if importance >= self.thresholds['importance_threshold']:
                                feature_values[feature_name].append(importance)
                    
                    # Extract feature values for distribution analysis
                    for feature_type, features_json in [
                        ('temporal', row['temporal_features']),
                        ('market', row['market_features']),
                        ('team', row['team_features']),
                        ('betting_splits', row['betting_splits_features'])
                    ]:
                        if features_json:
                            features = json.loads(features_json)
                            for feature_name, value in features.items():
                                if isinstance(value, (int, float)):
                                    feature_key = f"{feature_type}_{feature_name}"
                                    feature_values[feature_key].append(value)
                
                # Create current feature objects
                for feature_name, values in feature_values.items():
                    if len(values) >= 10:  # Lower threshold for current data
                        values_array = np.array(values)
                        current_features[feature_name] = DriftBaseline(
                            feature_name=feature_name,
                            feature_type=self._determine_feature_type(feature_name),
                            importance=np.mean(values_array),
                            distribution_stats={
                                'mean': float(np.mean(values_array)),
                                'std': float(np.std(values_array)),
                                'min': float(np.min(values_array)),
                                'max': float(np.max(values_array)),
                                'percentiles': {
                                    '25': float(np.percentile(values_array, 25)),
                                    '50': float(np.percentile(values_array, 50)),
                                    '75': float(np.percentile(values_array, 75))
                                }
                            },
                            sample_size=len(values),
                            created_at=datetime.utcnow()
                        )
                
                return current_features
                
        except Exception as e:
            logger.error(f"Error getting current feature importance: {e}")
            return {}
    
    def _determine_feature_type(self, feature_name: str) -> str:
        """Determine feature type from feature name"""
        if feature_name.startswith('temporal_'):
            return 'temporal'
        elif feature_name.startswith('market_'):
            return 'market'
        elif feature_name.startswith('team_'):
            return 'team'
        elif feature_name.startswith('betting_splits_'):
            return 'betting_splits'
        else:
            return 'unknown'
    
    async def _analyze_feature_drift(
        self, 
        feature_name: str, 
        baseline: DriftBaseline, 
        current: DriftBaseline,
        model_name: str
    ) -> Optional[FeatureDriftResult]:
        """Analyze drift between baseline and current feature"""
        try:
            # Calculate importance drift
            importance_drift = abs(current.importance - baseline.importance)
            importance_drift_pct = importance_drift / max(baseline.importance, 0.001)
            
            # Calculate distribution drift using multiple methods
            drift_scores = []
            
            # Method 1: Simple statistical comparison
            baseline_stats = baseline.distribution_stats
            current_stats = current.distribution_stats
            
            # Mean shift
            mean_shift = abs(current_stats['mean'] - baseline_stats['mean'])
            mean_shift_normalized = mean_shift / max(baseline_stats['std'], 0.001)
            drift_scores.append(min(mean_shift_normalized / 3.0, 1.0))  # 3-sigma rule
            
            # Std change
            std_ratio = current_stats['std'] / max(baseline_stats['std'], 0.001)
            std_drift = abs(1.0 - std_ratio)
            drift_scores.append(min(std_drift, 1.0))
            
            # Method 2: Population Stability Index (PSI) approximation
            psi_score = self._calculate_psi_approximation(baseline_stats, current_stats)
            drift_scores.append(min(psi_score / self.thresholds['psi_threshold'], 1.0))
            
            # Overall drift score (weighted average)
            drift_score = np.mean(drift_scores)
            
            # Determine if drift is detected
            drift_detected = (
                importance_drift_pct > self.thresholds['importance_drift'] or
                drift_score > self.thresholds['distribution_drift']
            )
            
            # Create result
            result = FeatureDriftResult(
                feature_name=feature_name,
                feature_type=current.feature_type,
                baseline_importance=baseline.importance,
                current_importance=current.importance,
                importance_drift=importance_drift,
                drift_score=drift_score,
                drift_detected=drift_detected,
                detection_method="statistical_comparison",
                sample_size=current.sample_size,
                baseline_distribution=baseline.distribution_stats,
                current_distribution=current.distribution_stats,
                metadata={
                    "importance_drift_pct": importance_drift_pct,
                    "mean_shift_normalized": mean_shift_normalized,
                    "std_drift": std_drift,
                    "psi_score": psi_score,
                    "baseline_sample_size": baseline.sample_size,
                    "model_name": model_name
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing feature drift for {feature_name}: {e}")
            return None
    
    def _calculate_psi_approximation(
        self, 
        baseline_stats: Dict[str, Any], 
        current_stats: Dict[str, Any]
    ) -> float:
        """Calculate Population Stability Index approximation"""
        try:
            # Simplified PSI calculation using percentiles
            baseline_pct = baseline_stats['percentiles']
            current_pct = current_stats['percentiles']
            
            psi = 0.0
            for pct in ['25', '50', '75']:
                baseline_val = baseline_pct[pct]
                current_val = current_pct[pct]
                
                # Avoid division by zero
                if baseline_val != 0:
                    ratio = current_val / baseline_val
                    if ratio > 0:
                        psi += (current_val - baseline_val) * np.log(ratio)
            
            return abs(psi)
            
        except Exception as e:
            logger.error(f"Error calculating PSI approximation: {e}")
            return 0.0
    
    async def store_drift_results(
        self, 
        model_name: str,
        model_version: str,
        drift_results: List[FeatureDriftResult],
        evaluation_period_start: datetime,
        evaluation_period_end: datetime
    ) -> bool:
        """Store feature drift results in database with MLFlow integration"""
        if not drift_results:
            return True
            
        try:
            # Get MLFlow context for this model
            mlflow_context = await self._get_mlflow_context(model_name)
            
            async with get_db_transaction() as conn:
                query = """
                    INSERT INTO curated.ml_feature_drift_detection (
                        model_name, model_version, feature_name, feature_type,
                        baseline_importance, current_importance, importance_drift,
                        baseline_distribution, current_distribution, drift_score,
                        drift_threshold, drift_detected, detection_method,
                        evaluation_period_start, evaluation_period_end, sample_size,
                        metadata, mlflow_experiment_id, mlflow_run_id, baseline_mlflow_run_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                """
                
                for result in drift_results:
                    await conn.execute(
                        query,
                        model_name,
                        model_version,
                        result.feature_name,
                        result.feature_type,
                        result.baseline_importance,
                        result.current_importance,
                        result.importance_drift,
                        json.dumps(result.baseline_distribution),
                        json.dumps(result.current_distribution),
                        result.drift_score,
                        self.thresholds['distribution_drift'],
                        result.drift_detected,
                        result.detection_method,
                        evaluation_period_start,
                        evaluation_period_end,
                        result.sample_size,
                        json.dumps(result.metadata),
                        mlflow_context.get('experiment_id'),
                        mlflow_context.get('current_run_id'),
                        mlflow_context.get('baseline_run_id')
                    )
                
                logger.info(f"Stored {len(drift_results)} drift detection results for {model_name} with MLFlow context")
                return True
                
        except Exception as e:
            logger.error(f"Error storing drift results: {e}")
            return False
    
    async def _get_mlflow_context(self, model_name: str) -> Dict[str, Any]:
        """Get MLFlow context for drift detection"""
        try:
            # Try to find experiments for this model
            experiments = []
            for prediction_type in ['total_over', 'home_ml', 'home_spread']:
                experiment_name = f"{model_name}_{prediction_type}"
                experiment = mlflow_service.get_experiment_by_name(experiment_name)
                if experiment:
                    experiments.append(experiment)
            
            if experiments:
                # Use the first experiment found
                experiment = experiments[0]
                
                # Get latest model run
                latest_model = mlflow_service.get_latest_model(experiment.name)
                
                return {
                    'experiment_id': experiment.experiment_id,
                    'current_run_id': latest_model['run_id'] if latest_model else None,
                    'baseline_run_id': latest_model['run_id'] if latest_model else None  # Use same run as baseline for now
                }
            
            return {}
            
        except Exception as e:
            logger.warning(f"Could not get MLFlow context for drift detection of {model_name}: {e}")
            return {}
    
    async def run_drift_detection_cycle(self, model_name: str = None) -> Dict[str, Any]:
        """Run a complete drift detection cycle"""
        try:
            start_time = datetime.utcnow()
            
            # Get models to analyze
            if model_name:
                models_to_analyze = [model_name]
            else:
                models_to_analyze = await self._get_active_models()
            
            total_drift_results = []
            models_analyzed = 0
            
            for model in models_to_analyze:
                try:
                    drift_results = await self.detect_feature_drift(model)
                    
                    if drift_results:
                        # Store results
                        await self.store_drift_results(
                            model,
                            "latest",  # TODO: Get actual version
                            drift_results,
                            start_time - timedelta(days=7),
                            start_time
                        )
                        
                        total_drift_results.extend(drift_results)
                    
                    models_analyzed += 1
                    
                except Exception as e:
                    logger.error(f"Error analyzing drift for model {model}: {e}")
                    continue
            
            # Summary
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            drift_detected_count = len([r for r in total_drift_results if r.drift_detected])
            
            summary = {
                "drift_detection_completed": True,
                "models_analyzed": models_analyzed,
                "features_analyzed": len(total_drift_results),
                "drift_detected": drift_detected_count,
                "duration_seconds": duration,
                "timestamp": end_time
            }
            
            logger.info(f"Drift detection cycle completed: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error in drift detection cycle: {e}")
            return {
                "drift_detection_completed": False,
                "error": str(e),
                "timestamp": datetime.utcnow()
            }
    
    async def _get_active_models(self) -> List[str]:
        """Get list of active models to monitor"""
        try:
            async with get_db_connection() as conn:
                query = """
                    SELECT DISTINCT model_name
                    FROM curated.ml_predictions
                    WHERE created_at >= NOW() - INTERVAL '7 days'
                    ORDER BY model_name
                """
                
                rows = await conn.fetch(query)
                return [row['model_name'] for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting active models: {e}")
            return []


# Global drift detection service instance
_drift_service: Optional[FeatureDriftDetectionService] = None


async def get_drift_detection_service() -> FeatureDriftDetectionService:
    """Get the global drift detection service instance"""
    global _drift_service
    
    if _drift_service is None:
        _drift_service = FeatureDriftDetectionService()
    
    return _drift_service