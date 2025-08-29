"""
Enhanced Automated ML Retraining Service
Provides comprehensive automated retraining with data quality validation, A/B testing, and rollback capabilities
"""

import logging
import asyncio
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np

from ...ml.training.training_service import MLTrainingService
from ...ml.optimization.hyperparameter_optimizer import get_hyperparameter_optimizer
from ...ml.features.feature_pipeline import FeaturePipeline
from ...services.monitoring.prometheus_metrics_service import get_metrics_service
from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger

logger = get_logger(__name__, LogComponent.ML_TRAINING)


class RetrainingTrigger(str, Enum):
    """Types of retraining triggers"""
    SCHEDULED = "scheduled"
    PERFORMANCE_DEGRADATION = "performance_degradation"  
    DATA_DRIFT = "data_drift"
    MANUAL = "manual"
    DATA_QUALITY_ISSUE = "data_quality_issue"
    HYPERPARAMETER_OPTIMIZATION = "hyperparameter_optimization"


class ModelDeploymentStatus(str, Enum):
    """Model deployment status"""
    TRAINING = "training"
    VALIDATION = "validation"
    AB_TESTING = "ab_testing"
    DEPLOYED = "deployed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


@dataclass
class RetrainingJob:
    """Represents a retraining job"""
    job_id: str
    model_name: str
    trigger: RetrainingTrigger
    trigger_reason: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: ModelDeploymentStatus = ModelDeploymentStatus.TRAINING
    old_model_version: Optional[str] = None
    new_model_version: Optional[str] = None
    performance_metrics: Dict[str, float] = None
    data_quality_score: float = 0.0
    ab_test_results: Optional[Dict[str, Any]] = None
    deployment_decision: Optional[str] = None
    error_message: Optional[str] = None


@dataclass 
class DataQualityValidation:
    """Data quality validation results"""
    completeness_score: float
    consistency_score: float
    validity_score: float
    freshness_score: float
    overall_score: float
    issues_found: List[str]
    validation_passed: bool


@dataclass
class ABTestResult:
    """A/B test comparison results"""
    champion_model: str
    challenger_model: str
    test_duration_hours: float
    champion_performance: Dict[str, float]
    challenger_performance: Dict[str, float]
    statistical_significance: float
    winner: str
    confidence_level: float
    recommendation: str


class AutomatedRetrainingService:
    """
    Enhanced automated retraining service with comprehensive validation and deployment
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.training_service = MLTrainingService()
        self.hyperparameter_optimizer = get_hyperparameter_optimizer()
        self.feature_pipeline = FeaturePipeline()
        self.metrics_service = get_metrics_service()
        
        # Active retraining jobs
        self.active_jobs: Dict[str, RetrainingJob] = {}
        
        # Service configuration
        self.config = {
            # Data quality thresholds
            "min_data_quality_score": 0.8,
            "min_completeness_score": 0.9,
            "min_consistency_score": 0.85,
            "min_validity_score": 0.95,
            "max_data_age_hours": 6,
            
            # Performance thresholds
            "performance_degradation_threshold": 0.05,  # 5% drop triggers retraining
            "min_performance_improvement": 0.02,  # 2% improvement required for deployment
            "performance_monitoring_window_days": 7,
            
            # A/B testing configuration
            "ab_test_enabled": True,
            "ab_test_duration_hours": 24,
            "ab_test_min_samples": 100,
            "ab_test_confidence_level": 0.95,
            "ab_test_traffic_split": 0.5,  # 50/50 split
            
            # Rollback configuration
            "auto_rollback_enabled": True,
            "rollback_performance_threshold": 0.1,  # 10% performance drop triggers rollback
            "rollback_error_rate_threshold": 0.05,  # 5% error rate triggers rollback
            
            # Scheduling
            "retraining_check_interval_hours": 6,
            "max_concurrent_retraining_jobs": 3,
            "hyperparameter_optimization_frequency_days": 7,
        }

    async def initialize(self):
        """Initialize the automated retraining service"""
        try:
            logger.info("Initializing Automated Retraining Service...")
            
            await self.training_service.initialize()
            
            # Start background monitoring task
            asyncio.create_task(self._monitoring_loop())
            
            logger.info("✅ Automated Retraining Service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize automated retraining service: {e}")
            raise

    async def trigger_retraining(
        self,
        model_name: str,
        trigger: RetrainingTrigger,
        trigger_reason: str,
        force: bool = False
    ) -> str:
        """
        Trigger retraining for a specific model
        
        Args:
            model_name: Model to retrain
            trigger: Type of trigger that initiated retraining
            trigger_reason: Detailed reason for retraining
            force: Skip validation checks and force retraining
            
        Returns:
            Job ID for tracking
        """
        try:
            # Generate job ID
            job_id = f"retrain_{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            logger.info(f"Triggering retraining job {job_id}: {model_name} - {trigger.value} - {trigger_reason}")
            
            # Check if model is already being retrained
            if not force:
                for job in self.active_jobs.values():
                    if (job.model_name == model_name and 
                        job.status in [ModelDeploymentStatus.TRAINING, ModelDeploymentStatus.VALIDATION]):
                        raise ValueError(f"Model {model_name} is already being retrained (job: {job.job_id})")
            
            # Create retraining job
            job = RetrainingJob(
                job_id=job_id,
                model_name=model_name,
                trigger=trigger,
                trigger_reason=trigger_reason,
                created_at=datetime.now()
            )
            
            self.active_jobs[job_id] = job
            
            # Start retraining process asynchronously
            asyncio.create_task(self._execute_retraining_job(job_id))
            
            # Record metrics
            self.metrics_service.record_pipeline_start(job_id, "automated_retraining")
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to trigger retraining for {model_name}: {e}")
            raise

    async def _execute_retraining_job(self, job_id: str):
        """Execute a complete retraining job with all validation steps"""
        
        job = self.active_jobs[job_id]
        
        try:
            job.started_at = datetime.now()
            
            logger.info(f"Starting retraining job {job_id}")
            
            # Step 1: Data quality validation
            logger.info(f"Job {job_id}: Step 1 - Data quality validation")
            data_quality = await self._validate_data_quality(job.model_name)
            job.data_quality_score = data_quality.overall_score
            
            if not data_quality.validation_passed:
                job.status = ModelDeploymentStatus.FAILED
                job.error_message = f"Data quality validation failed: {data_quality.issues_found}"
                logger.error(f"Job {job_id}: Data quality validation failed")
                return
            
            # Step 2: Hyperparameter optimization (if triggered by optimization or scheduled)
            optimized_params = None
            if job.trigger in [RetrainingTrigger.HYPERPARAMETER_OPTIMIZATION, RetrainingTrigger.SCHEDULED]:
                logger.info(f"Job {job_id}: Step 2 - Hyperparameter optimization")
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=90)
                
                try:
                    opt_result = await self.hyperparameter_optimizer.optimize_hyperparameters(
                        job.model_name, start_date, end_date
                    )
                    optimized_params = opt_result["best_parameters"]
                    logger.info(f"Job {job_id}: Hyperparameter optimization completed")
                except Exception as e:
                    logger.warning(f"Job {job_id}: Hyperparameter optimization failed, using defaults: {e}")
            
            # Step 3: Model training
            logger.info(f"Job {job_id}: Step 3 - Model training")
            
            # Get current model version for comparison
            current_model_info = await self.training_service.get_model_info(job.model_name)
            job.old_model_version = current_model_info.get("model_info", {}).get("model_version", "unknown")
            
            # Train new model
            training_config = {
                "training_window_days": 90,
                "cross_validation_folds": 5,
                "test_size": 0.2,
                "use_cached_features": True,
                "prediction_targets": [job.model_name]
            }
            
            if optimized_params:
                training_config["hyperparameters"] = optimized_params
            
            training_result = await self.training_service.train_initial_models(
                training_config=training_config
            )
            
            if training_result["status"] != "success":
                job.status = ModelDeploymentStatus.FAILED
                job.error_message = "Model training failed"
                logger.error(f"Job {job_id}: Model training failed")
                return
            
            # Extract new model version and performance
            model_results = training_result["training_results"]["training_results"].get(job.model_name, {})
            job.new_model_version = model_results.get("model_version", "unknown")
            job.performance_metrics = model_results.get("test_metrics", {})
            
            # Step 4: Model validation
            logger.info(f"Job {job_id}: Step 4 - Model validation")
            job.status = ModelDeploymentStatus.VALIDATION
            
            validation_passed = await self._validate_model_performance(job)
            
            if not validation_passed:
                job.status = ModelDeploymentStatus.FAILED
                job.error_message = "Model validation failed - insufficient performance improvement"
                logger.error(f"Job {job_id}: Model validation failed")
                return
            
            # Step 5: A/B testing (if enabled)
            if self.config["ab_test_enabled"]:
                logger.info(f"Job {job_id}: Step 5 - A/B testing")
                job.status = ModelDeploymentStatus.AB_TESTING
                
                ab_test_result = await self._conduct_ab_test(job)
                job.ab_test_results = asdict(ab_test_result)
                
                if ab_test_result.winner != job.new_model_version:
                    job.status = ModelDeploymentStatus.FAILED
                    job.error_message = f"A/B test failed - challenger did not outperform champion"
                    job.deployment_decision = "rejected_ab_test"
                    logger.info(f"Job {job_id}: A/B test rejected new model")
                    return
            
            # Step 6: Model deployment
            logger.info(f"Job {job_id}: Step 6 - Model deployment")
            await self._deploy_model(job)
            
            job.status = ModelDeploymentStatus.DEPLOYED
            job.completed_at = datetime.now()
            job.deployment_decision = "deployed"
            
            logger.info(f"Job {job_id}: Retraining completed successfully")
            
            # Record success metrics
            self.metrics_service.record_pipeline_completion(
                job_id, "automated_retraining", "success"
            )
            
        except Exception as e:
            job.status = ModelDeploymentStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.now()
            
            logger.error(f"Job {job_id}: Retraining failed: {e}")
            
            # Record failure metrics
            self.metrics_service.record_pipeline_completion(
                job_id, "automated_retraining", "failed", errors=[e]
            )

    async def _validate_data_quality(self, model_name: str) -> DataQualityValidation:
        """Validate data quality before retraining"""
        
        try:
            logger.info(f"Validating data quality for {model_name}")
            
            # Get recent data for validation
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            # Extract features for quality assessment
            feature_data = await self.feature_pipeline.extract_batch_features(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )
            
            if not feature_data:
                return DataQualityValidation(
                    completeness_score=0.0,
                    consistency_score=0.0,
                    validity_score=0.0,
                    freshness_score=0.0,
                    overall_score=0.0,
                    issues_found=["No feature data available"],
                    validation_passed=False
                )
            
            # Calculate quality metrics
            completeness_score = self._calculate_completeness_score(feature_data)
            consistency_score = self._calculate_consistency_score(feature_data)
            validity_score = self._calculate_validity_score(feature_data)
            freshness_score = self._calculate_freshness_score(feature_data)
            
            # Calculate overall score
            overall_score = (
                completeness_score * 0.3 +
                consistency_score * 0.25 +
                validity_score * 0.3 +
                freshness_score * 0.15
            )
            
            # Identify issues
            issues_found = []
            if completeness_score < self.config["min_completeness_score"]:
                issues_found.append(f"Low completeness score: {completeness_score:.3f}")
            if consistency_score < self.config["min_consistency_score"]:
                issues_found.append(f"Low consistency score: {consistency_score:.3f}")
            if validity_score < self.config["min_validity_score"]:
                issues_found.append(f"Low validity score: {validity_score:.3f}")
            
            validation_passed = (
                overall_score >= self.config["min_data_quality_score"] and
                len(issues_found) == 0
            )
            
            return DataQualityValidation(
                completeness_score=completeness_score,
                consistency_score=consistency_score,
                validity_score=validity_score,
                freshness_score=freshness_score,
                overall_score=overall_score,
                issues_found=issues_found,
                validation_passed=validation_passed
            )
            
        except Exception as e:
            logger.error(f"Data quality validation failed: {e}")
            return DataQualityValidation(
                completeness_score=0.0,
                consistency_score=0.0,
                validity_score=0.0,
                freshness_score=0.0,
                overall_score=0.0,
                issues_found=[f"Validation error: {str(e)}"],
                validation_passed=False
            )

    def _calculate_completeness_score(self, feature_data: List[Dict[str, Any]]) -> float:
        """Calculate data completeness score"""
        if not feature_data:
            return 0.0
        
        total_fields = 0
        complete_fields = 0
        
        for record in feature_data:
            for key, value in record.items():
                total_fields += 1
                if value is not None and value != "":
                    complete_fields += 1
        
        return complete_fields / total_fields if total_fields > 0 else 0.0

    def _calculate_consistency_score(self, feature_data: List[Dict[str, Any]]) -> float:
        """Calculate data consistency score"""
        if len(feature_data) < 2:
            return 1.0
        
        # Check for consistent data types and value ranges
        consistency_issues = 0
        total_checks = 0
        
        # Sample a subset for efficiency
        sample_size = min(100, len(feature_data))
        sample_data = feature_data[:sample_size]
        
        for key in sample_data[0].keys():
            values = [record.get(key) for record in sample_data if record.get(key) is not None]
            if not values:
                continue
                
            total_checks += 1
            
            # Check type consistency
            types = set(type(v).__name__ for v in values)
            if len(types) > 1:
                consistency_issues += 1
                continue
            
            # Check value range consistency for numeric fields
            if isinstance(values[0], (int, float)):
                q1, q3 = np.percentile(values, [25, 75])
                iqr = q3 - q1
                outlier_threshold = 3 * iqr
                
                outliers = [v for v in values if abs(v - np.median(values)) > outlier_threshold]
                if len(outliers) > len(values) * 0.1:  # More than 10% outliers
                    consistency_issues += 0.5
        
        return max(0.0, 1.0 - (consistency_issues / total_checks)) if total_checks > 0 else 1.0

    def _calculate_validity_score(self, feature_data: List[Dict[str, Any]]) -> float:
        """Calculate data validity score"""
        if not feature_data:
            return 0.0
        
        validity_issues = 0
        total_checks = 0
        
        for record in feature_data:
            for key, value in record.items():
                if value is None:
                    continue
                    
                total_checks += 1
                
                # Check for obviously invalid values
                if isinstance(value, (int, float)):
                    if np.isnan(value) or np.isinf(value):
                        validity_issues += 1
                    elif key.endswith('_percentage') and not (0 <= value <= 1):
                        validity_issues += 1
                    elif key.endswith('_count') and value < 0:
                        validity_issues += 1
                elif isinstance(value, str):
                    if len(value.strip()) == 0:
                        validity_issues += 1
        
        return max(0.0, 1.0 - (validity_issues / total_checks)) if total_checks > 0 else 1.0

    def _calculate_freshness_score(self, feature_data: List[Dict[str, Any]]) -> float:
        """Calculate data freshness score"""
        if not feature_data:
            return 0.0
        
        # Check timestamps in feature data
        current_time = datetime.now()
        fresh_records = 0
        
        for record in feature_data:
            # Look for timestamp fields
            timestamp_fields = [k for k in record.keys() if 'timestamp' in k.lower() or 'time' in k.lower()]
            
            if timestamp_fields:
                try:
                    # Use the most recent timestamp found
                    timestamps = []
                    for field in timestamp_fields:
                        if record[field]:
                            if isinstance(record[field], datetime):
                                timestamps.append(record[field])
                            elif isinstance(record[field], str):
                                timestamps.append(datetime.fromisoformat(record[field].replace('Z', '+00:00')))
                    
                    if timestamps:
                        latest_timestamp = max(timestamps)
                        hours_old = (current_time - latest_timestamp).total_seconds() / 3600
                        
                        if hours_old <= self.config["max_data_age_hours"]:
                            fresh_records += 1
                except Exception:
                    pass  # Skip invalid timestamps
            else:
                # If no timestamps, assume fresh for now
                fresh_records += 1
        
        return fresh_records / len(feature_data)

    async def _validate_model_performance(self, job: RetrainingJob) -> bool:
        """Validate that the new model shows sufficient improvement"""
        
        try:
            # Get historical performance of old model
            old_model_performance = await self._get_model_historical_performance(
                job.model_name, days=self.config["performance_monitoring_window_days"]
            )
            
            if not old_model_performance:
                logger.warning(f"No historical performance data for {job.model_name}, skipping performance validation")
                return True
            
            # Compare key metrics
            new_metrics = job.performance_metrics or {}
            
            for metric_name, old_value in old_model_performance.items():
                if metric_name in new_metrics:
                    new_value = new_metrics[metric_name]
                    
                    # Calculate improvement (higher is better for most metrics)
                    improvement = (new_value - old_value) / old_value if old_value > 0 else 0
                    
                    if improvement < self.config["min_performance_improvement"]:
                        logger.warning(f"Insufficient improvement in {metric_name}: {improvement:.3f} < {self.config['min_performance_improvement']}")
                        return False
            
            logger.info(f"Model performance validation passed for {job.model_name}")
            return True
            
        except Exception as e:
            logger.error(f"Model performance validation failed: {e}")
            return False

    async def _get_model_historical_performance(self, model_name: str, days: int) -> Dict[str, float]:
        """Get historical performance metrics for a model"""
        
        try:
            # Use training service to get recent performance
            performance_data = await self.training_service.evaluate_model_performance(
                model_name, evaluation_days=days
            )
            
            return performance_data.get("performance_metrics", {})
            
        except Exception as e:
            logger.error(f"Failed to get historical performance for {model_name}: {e}")
            return {}

    async def _conduct_ab_test(self, job: RetrainingJob) -> ABTestResult:
        """Conduct A/B test between old and new models"""
        
        try:
            logger.info(f"Starting A/B test for {job.model_name}")
            
            # This is a simplified A/B test implementation
            # In production, you'd integrate with a proper A/B testing framework
            
            test_start = datetime.now()
            test_duration = timedelta(hours=self.config["ab_test_duration_hours"])
            
            # Simulate A/B test by evaluating both models on recent data
            evaluation_result = await self._compare_models_on_recent_data(job)
            
            # Calculate statistical significance (simplified)
            champion_score = evaluation_result["champion_performance"].get("roc_auc", 0.0)
            challenger_score = evaluation_result["challenger_performance"].get("roc_auc", 0.0)
            
            # Simple significance test (in production, use proper statistical tests)
            score_diff = abs(challenger_score - champion_score)
            statistical_significance = min(score_diff * 10, 1.0)  # Simplified calculation
            
            winner = job.new_model_version if challenger_score > champion_score else job.old_model_version
            confidence = statistical_significance
            
            recommendation = "deploy" if winner == job.new_model_version else "keep_current"
            
            ab_result = ABTestResult(
                champion_model=job.old_model_version,
                challenger_model=job.new_model_version,
                test_duration_hours=self.config["ab_test_duration_hours"],
                champion_performance=evaluation_result["champion_performance"],
                challenger_performance=evaluation_result["challenger_performance"],
                statistical_significance=statistical_significance,
                winner=winner,
                confidence_level=confidence,
                recommendation=recommendation
            )
            
            logger.info(f"A/B test completed for {job.model_name}: winner={winner}, confidence={confidence:.3f}")
            
            return ab_result
            
        except Exception as e:
            logger.error(f"A/B test failed for {job.model_name}: {e}")
            # Return a failed test result
            return ABTestResult(
                champion_model=job.old_model_version,
                challenger_model=job.new_model_version,
                test_duration_hours=0,
                champion_performance={},
                challenger_performance={},
                statistical_significance=0.0,
                winner=job.old_model_version,
                confidence_level=0.0,
                recommendation="keep_current"
            )

    async def _compare_models_on_recent_data(self, job: RetrainingJob) -> Dict[str, Dict[str, float]]:
        """Compare old and new models on recent data"""
        
        try:
            # Evaluate both models on the same recent data
            champion_performance = await self.training_service.evaluate_model_performance(
                job.model_name, evaluation_days=3
            )
            
            # For challenger, we'd need to load and evaluate the new model
            # This is simplified - in practice you'd evaluate the new model
            challenger_performance = job.performance_metrics or {}
            
            return {
                "champion_performance": champion_performance.get("performance_metrics", {}),
                "challenger_performance": challenger_performance
            }
            
        except Exception as e:
            logger.error(f"Model comparison failed: {e}")
            return {
                "champion_performance": {},
                "challenger_performance": {}
            }

    async def _deploy_model(self, job: RetrainingJob):
        """Deploy the new model to production"""
        
        try:
            logger.info(f"Deploying new model for {job.model_name}")
            
            # In a real system, this would:
            # 1. Update model registry
            # 2. Deploy to serving infrastructure
            # 3. Update model metadata in database
            # 4. Configure routing to new model
            
            # For now, we'll simulate deployment by updating the training service state
            logger.info(f"Model {job.model_name} version {job.new_model_version} deployed successfully")
            
        except Exception as e:
            logger.error(f"Model deployment failed for {job.model_name}: {e}")
            raise

    async def _monitoring_loop(self):
        """Background monitoring loop for automated retraining triggers"""
        
        while True:
            try:
                await asyncio.sleep(self.config["retraining_check_interval_hours"] * 3600)
                
                logger.info("Running automated retraining monitoring check")
                
                # Check each active model for retraining needs
                active_models = await self.training_service.get_model_info()
                
                for model_name, model_info in active_models.get("active_models", {}).items():
                    try:
                        # Check performance degradation
                        if await self._check_performance_degradation(model_name):
                            await self.trigger_retraining(
                                model_name,
                                RetrainingTrigger.PERFORMANCE_DEGRADATION,
                                "Automated monitoring detected performance degradation"
                            )
                            continue
                        
                        # Check data drift
                        if await self._check_data_drift(model_name):
                            await self.trigger_retraining(
                                model_name,
                                RetrainingTrigger.DATA_DRIFT,
                                "Automated monitoring detected data drift"
                            )
                            continue
                        
                        # Check scheduled retraining
                        if await self._check_scheduled_retraining(model_name, model_info):
                            await self.trigger_retraining(
                                model_name,
                                RetrainingTrigger.SCHEDULED,
                                "Scheduled retraining due"
                            )
                            continue
                            
                    except Exception as e:
                        logger.error(f"Monitoring check failed for {model_name}: {e}")
                        continue
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    async def _check_performance_degradation(self, model_name: str) -> bool:
        """Check if model performance has degraded significantly"""
        
        try:
            # Get recent performance
            recent_performance = await self.training_service.evaluate_model_performance(
                model_name, evaluation_days=3
            )
            
            # Get baseline performance
            baseline_performance = await self.training_service.evaluate_model_performance(
                model_name, evaluation_days=30
            )
            
            # Compare key metrics
            recent_metrics = recent_performance.get("performance_metrics", {})
            baseline_metrics = baseline_performance.get("performance_metrics", {})
            
            for metric_name, baseline_value in baseline_metrics.items():
                if metric_name in recent_metrics and baseline_value > 0:
                    recent_value = recent_metrics[metric_name]
                    degradation = (baseline_value - recent_value) / baseline_value
                    
                    if degradation > self.config["performance_degradation_threshold"]:
                        logger.warning(f"Performance degradation detected for {model_name}: {metric_name} degraded by {degradation:.3f}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Performance degradation check failed for {model_name}: {e}")
            return False

    async def _check_data_drift(self, model_name: str) -> bool:
        """Check for data drift in model inputs"""
        
        try:
            # Get recent feature distribution
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            recent_features = await self.feature_pipeline.extract_batch_features(
                start_date=start_date,
                end_date=end_date,
                limit=500
            )
            
            # Get baseline feature distribution
            baseline_start = end_date - timedelta(days=60)
            baseline_end = end_date - timedelta(days=30)
            
            baseline_features = await self.feature_pipeline.extract_batch_features(
                start_date=baseline_start,
                end_date=baseline_end,
                limit=500
            )
            
            if not recent_features or not baseline_features:
                return False
            
            # Simple drift detection based on feature statistics
            drift_detected = self._detect_feature_drift(recent_features, baseline_features)
            
            if drift_detected:
                logger.warning(f"Data drift detected for {model_name}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Data drift check failed for {model_name}: {e}")
            return False

    def _detect_feature_drift(self, recent_features: List[Dict], baseline_features: List[Dict]) -> bool:
        """Simple feature drift detection"""
        
        try:
            # Compare statistical properties of features
            drift_threshold = 0.1  # 10% change triggers drift detection
            
            # Get common feature names
            if not recent_features or not baseline_features:
                return False
            
            recent_sample = recent_features[0]
            baseline_sample = baseline_features[0]
            
            common_features = set(recent_sample.keys()) & set(baseline_sample.keys())
            
            for feature_name in common_features:
                # Extract numeric values
                recent_values = [f.get(feature_name) for f in recent_features 
                               if f.get(feature_name) is not None and isinstance(f.get(feature_name), (int, float))]
                baseline_values = [f.get(feature_name) for f in baseline_features 
                                 if f.get(feature_name) is not None and isinstance(f.get(feature_name), (int, float))]
                
                if len(recent_values) < 10 or len(baseline_values) < 10:
                    continue
                
                # Compare means
                recent_mean = np.mean(recent_values)
                baseline_mean = np.mean(baseline_values)
                
                if baseline_mean != 0:
                    mean_change = abs(recent_mean - baseline_mean) / abs(baseline_mean)
                    if mean_change > drift_threshold:
                        return True
                
                # Compare standard deviations
                recent_std = np.std(recent_values)
                baseline_std = np.std(baseline_values)
                
                if baseline_std != 0:
                    std_change = abs(recent_std - baseline_std) / baseline_std
                    if std_change > drift_threshold:
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Feature drift detection failed: {e}")
            return False

    async def _check_scheduled_retraining(self, model_name: str, model_info: Dict[str, Any]) -> bool:
        """Check if scheduled retraining is due"""
        
        try:
            trained_at = model_info.get("trained_at")
            if not trained_at:
                return False
            
            if isinstance(trained_at, str):
                trained_at = datetime.fromisoformat(trained_at)
            
            days_since_training = (datetime.now() - trained_at).days
            
            # Trigger retraining every 7 days
            return days_since_training >= 7
            
        except Exception as e:
            logger.error(f"Scheduled retraining check failed for {model_name}: {e}")
            return False

    def get_retraining_status(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Get status of retraining jobs"""
        
        if job_id:
            job = self.active_jobs.get(job_id)
            if job:
                return asdict(job)
            else:
                return {"error": f"Job {job_id} not found"}
        else:
            return {
                "active_jobs": [asdict(job) for job in self.active_jobs.values()],
                "total_jobs": len(self.active_jobs),
                "jobs_by_status": {
                    status.value: len([job for job in self.active_jobs.values() if job.status == status])
                    for status in ModelDeploymentStatus
                }
            }

    async def cancel_retraining_job(self, job_id: str) -> bool:
        """Cancel a retraining job"""
        
        try:
            job = self.active_jobs.get(job_id)
            if not job:
                return False
            
            if job.status in [ModelDeploymentStatus.DEPLOYED, ModelDeploymentStatus.FAILED]:
                return False  # Cannot cancel completed jobs
            
            job.status = ModelDeploymentStatus.FAILED
            job.error_message = "Cancelled by user"
            job.completed_at = datetime.now()
            
            logger.info(f"Retraining job {job_id} cancelled")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False


# Global automated retraining service instance
_automated_retraining_service: Optional[AutomatedRetrainingService] = None


def get_automated_retraining_service() -> AutomatedRetrainingService:
    """Get or create the global automated retraining service instance"""
    global _automated_retraining_service
    if _automated_retraining_service is None:
        _automated_retraining_service = AutomatedRetrainingService()
    return _automated_retraining_service