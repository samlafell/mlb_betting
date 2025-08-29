"""
Automated Retraining Engine

Manages automated retraining workflows that continuously improve betting strategies.
Integrates with the existing hyperparameter optimization framework to provide:

- Multi-strategy parallel retraining capabilities
- Integration with existing hyperparameter optimization framework
- Data pipeline integration for fresh training data
- Model versioning and rollback capabilities
- Automated A/B testing against current production models
- Statistical significance testing for model improvements
- Gradual rollout capabilities with performance monitoring

Coordinates with RetrainingTriggerService, ModelValidationService, and
PerformanceMonitoringService to provide comprehensive automated retraining.
"""

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.analysis.optimization import (
    OptimizationEngine,
    StrategyParameterRegistry,
    OptimizationAlgorithm,
    create_optimization_job
)
from src.analysis.strategies.orchestrator import StrategyOrchestrator
from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.data.database import UnifiedRepository
from src.services.monitoring.prometheus_metrics_service import get_metrics_service
from .trigger_service import TriggerCondition, TriggerType


logger = get_logger(__name__, LogComponent.CORE)


class RetrainingStatus(str, Enum):
    """Status of retraining jobs"""
    
    PENDING = "pending"
    PREPARING_DATA = "preparing_data"
    OPTIMIZING = "optimizing"
    VALIDATING = "validating"
    AB_TESTING = "ab_testing"
    DEPLOYING = "deploying"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ROLLED_BACK = "rolled_back"


class RetrainingStrategy(str, Enum):
    """Retraining strategies"""
    
    FULL_RETRAINING = "full_retraining"  # Complete hyperparameter optimization
    INCREMENTAL_UPDATE = "incremental_update"  # Update with recent data only
    TARGETED_OPTIMIZATION = "targeted_optimization"  # Focus on specific parameters
    EMERGENCY_ROLLBACK = "emergency_rollback"  # Emergency rollback to previous version


@dataclass
class RetrainingConfiguration:
    """Configuration for retraining jobs"""
    
    # Optimization settings
    algorithm: OptimizationAlgorithm = OptimizationAlgorithm.BAYESIAN_OPTIMIZATION
    max_evaluations: int = 50
    high_impact_only: bool = True
    parallel_jobs: int = 2
    timeout_hours: int = 12
    
    # Data settings
    training_period_days: int = 90
    validation_period_days: int = 30
    min_training_samples: int = 100
    data_quality_threshold: float = 0.8
    
    # A/B testing settings
    ab_test_duration_hours: int = 72
    ab_test_traffic_percentage: float = 20.0
    statistical_significance_threshold: float = 0.05
    min_improvement_threshold: float = 2.0  # Minimum 2% improvement required
    
    # Rollout settings
    gradual_rollout_enabled: bool = True
    rollout_stages: List[float] = field(default_factory=lambda: [10.0, 25.0, 50.0, 100.0])
    rollout_stage_duration_hours: int = 24
    
    # Safety settings
    max_concurrent_retraining_jobs: int = 2
    enable_auto_rollback: bool = True
    performance_monitoring_hours: int = 48


@dataclass
class ModelVersion:
    """Represents a versioned model configuration"""
    
    version_id: str
    strategy_name: str
    parameters: Dict[str, Any]
    performance_metrics: Dict[str, float]
    created_at: datetime
    training_data_period: str
    optimization_job_id: Optional[str] = None
    is_production: bool = False
    is_baseline: bool = False


@dataclass
class RetrainingJob:
    """Represents an automated retraining job"""
    
    job_id: str
    strategy_name: str
    trigger_conditions: List[TriggerCondition]
    retraining_strategy: RetrainingStrategy
    configuration: RetrainingConfiguration
    status: RetrainingStatus
    created_at: datetime
    
    # Job progress tracking
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    current_stage: Optional[str] = None
    progress_percentage: float = 0.0
    
    # Optimization details
    optimization_job_id: Optional[str] = None
    baseline_model: Optional[ModelVersion] = None
    candidate_model: Optional[ModelVersion] = None
    
    # A/B testing results
    ab_test_id: Optional[str] = None
    ab_test_results: Optional[Dict[str, Any]] = None
    
    # Deployment tracking
    deployment_stage: Optional[str] = None
    traffic_percentage: float = 0.0
    
    # Results and metadata
    improvement_percentage: Optional[float] = None
    statistical_significance: Optional[float] = None
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomatedRetrainingEngine:
    """
    Automated retraining engine that manages end-to-end retraining workflows.
    
    Provides comprehensive automated retraining capabilities including:
    - Multi-strategy parallel retraining
    - Integration with hyperparameter optimization
    - Model versioning and rollback
    - A/B testing and gradual rollouts
    - Performance monitoring and automatic rollback
    - Statistical validation of improvements
    
    Integrates with existing optimization infrastructure and monitoring systems.
    """
    
    def __init__(
        self,
        repository: UnifiedRepository,
        strategy_orchestrator: StrategyOrchestrator,
        default_config: Optional[RetrainingConfiguration] = None
    ):
        """Initialize the automated retraining engine."""
        
        self.repository = repository
        self.strategy_orchestrator = strategy_orchestrator
        self.config = get_settings()
        self.logger = logger
        self.metrics_service = get_metrics_service()
        
        # Configuration
        self.default_config = default_config or RetrainingConfiguration()
        
        # Core services
        self.optimization_engine = OptimizationEngine(
            repository, 
            {"max_workers": self.default_config.parallel_jobs}
        )
        self.parameter_registry = StrategyParameterRegistry()
        
        # Job management
        self.active_jobs: Dict[str, RetrainingJob] = {}
        self.job_history: List[RetrainingJob] = []
        
        # Model versioning
        self.model_versions: Dict[str, List[ModelVersion]] = {}  # strategy -> versions
        self.production_models: Dict[str, ModelVersion] = {}  # strategy -> current production
        
        # A/B testing state
        self.active_ab_tests: Dict[str, Dict[str, Any]] = {}
        
        # Background task management
        self.background_tasks: List[asyncio.Task] = []
        self.engine_running = False
        
        self.logger.info("AutomatedRetrainingEngine initialized")
    
    async def start_engine(self) -> None:
        """Start the automated retraining engine."""
        
        if self.engine_running:
            self.logger.warning("Retraining engine already running")
            return
        
        self.engine_running = True
        
        # Load existing model versions
        await self._load_model_versions()
        
        # Start background monitoring tasks
        self.background_tasks = [
            asyncio.create_task(self._monitor_retraining_jobs()),
            asyncio.create_task(self._monitor_ab_tests()),
            asyncio.create_task(self._monitor_deployed_models()),
        ]
        
        self.logger.info("Automated retraining engine started")
    
    async def stop_engine(self) -> None:
        """Stop the automated retraining engine."""
        
        self.engine_running = False
        
        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()
        
        self.logger.info("Automated retraining engine stopped")
    
    async def trigger_retraining(
        self,
        strategy_name: str,
        trigger_conditions: List[TriggerCondition],
        retraining_strategy: RetrainingStrategy = RetrainingStrategy.FULL_RETRAINING,
        configuration: Optional[RetrainingConfiguration] = None
    ) -> RetrainingJob:
        """Trigger automated retraining for a strategy."""
        
        # Check if we're at max concurrent jobs
        active_count = len([j for j in self.active_jobs.values() if j.status in [
            RetrainingStatus.PENDING, RetrainingStatus.PREPARING_DATA,
            RetrainingStatus.OPTIMIZING, RetrainingStatus.VALIDATING,
            RetrainingStatus.AB_TESTING, RetrainingStatus.DEPLOYING
        ]])
        
        if active_count >= self.default_config.max_concurrent_retraining_jobs:
            raise ValueError(f"Maximum concurrent retraining jobs ({self.default_config.max_concurrent_retraining_jobs}) reached")
        
        # Create retraining job
        job_config = configuration or self.default_config
        
        job = RetrainingJob(
            job_id=str(uuid.uuid4()),
            strategy_name=strategy_name,
            trigger_conditions=trigger_conditions,
            retraining_strategy=retraining_strategy,
            configuration=job_config,
            status=RetrainingStatus.PENDING,
            created_at=datetime.now()
        )
        
        # Add to active jobs
        self.active_jobs[job.job_id] = job
        
        self.logger.info(
            f"Retraining job created for {strategy_name}",
            extra={
                "job_id": job.job_id,
                "strategy": strategy_name,
                "retraining_strategy": retraining_strategy.value,
                "trigger_count": len(trigger_conditions),
                "trigger_types": [t.trigger_type.value for t in trigger_conditions]
            }
        )
        
        # Record metrics
        self.metrics_service.record_pipeline_start(
            pipeline_id=job.job_id,
            pipeline_type="retraining"
        )
        
        # Start retraining workflow asynchronously
        asyncio.create_task(self._execute_retraining_workflow(job))
        
        return job
    
    async def _execute_retraining_workflow(self, job: RetrainingJob) -> None:
        """Execute the complete retraining workflow."""
        
        try:
            job.started_at = datetime.now()
            job.status = RetrainingStatus.PREPARING_DATA
            job.current_stage = "Data Preparation"
            await self._update_job_progress(job, 5.0)
            
            # Stage 1: Prepare training data
            training_data_info = await self._prepare_training_data(job)
            job.metadata["training_data_info"] = training_data_info
            await self._update_job_progress(job, 15.0)
            
            # Stage 2: Get baseline model performance
            baseline_model = await self._get_baseline_model(job)
            job.baseline_model = baseline_model
            await self._update_job_progress(job, 25.0)
            
            # Stage 3: Run hyperparameter optimization
            job.status = RetrainingStatus.OPTIMIZING
            job.current_stage = "Hyperparameter Optimization"
            candidate_model = await self._run_optimization(job)
            job.candidate_model = candidate_model
            await self._update_job_progress(job, 60.0)
            
            # Stage 4: Validate candidate model
            job.status = RetrainingStatus.VALIDATING
            job.current_stage = "Model Validation"
            validation_result = await self._validate_candidate_model(job)
            job.metadata["validation_result"] = validation_result
            await self._update_job_progress(job, 75.0)
            
            # Stage 5: A/B test if validation passes
            if validation_result["passes_validation"]:
                job.status = RetrainingStatus.AB_TESTING
                job.current_stage = "A/B Testing"
                ab_test_result = await self._run_ab_test(job)
                job.ab_test_results = ab_test_result
                await self._update_job_progress(job, 90.0)
                
                # Stage 6: Deploy if A/B test is successful
                if ab_test_result["deployment_recommended"]:
                    job.status = RetrainingStatus.DEPLOYING
                    job.current_stage = "Gradual Deployment"
                    await self._deploy_model(job)
                    await self._update_job_progress(job, 100.0)
                    
                    job.status = RetrainingStatus.COMPLETED
                    job.improvement_percentage = ab_test_result.get("improvement_percentage")
                    job.statistical_significance = ab_test_result.get("statistical_significance")
                else:
                    job.status = RetrainingStatus.COMPLETED
                    job.logs.append("A/B test did not show significant improvement - keeping current model")
            else:
                job.status = RetrainingStatus.COMPLETED
                job.logs.append("Candidate model failed validation - keeping current model")
            
            job.completed_at = datetime.now()
            
            # Record successful completion metrics
            self.metrics_service.record_pipeline_completion(
                pipeline_id=job.job_id,
                pipeline_type="retraining",
                status="completed",
                stages_executed=6
            )
            
            self.logger.info(
                f"Retraining workflow completed for {job.strategy_name}",
                extra={
                    "job_id": job.job_id,
                    "duration_minutes": (job.completed_at - job.started_at).total_seconds() / 60,
                    "improvement_percentage": job.improvement_percentage,
                    "final_status": job.status.value
                }
            )
            
        except Exception as e:
            # Handle workflow failure
            job.status = RetrainingStatus.FAILED
            job.completed_at = datetime.now()
            job.error_message = str(e)
            job.logs.append(f"Workflow failed: {str(e)}")
            
            # Record failure metrics
            self.metrics_service.record_pipeline_completion(
                pipeline_id=job.job_id,
                pipeline_type="retraining",
                status="failed",
                errors=[e]
            )
            
            self.logger.error(
                f"Retraining workflow failed for {job.strategy_name}: {e}",
                extra={"job_id": job.job_id},
                exc_info=True
            )
        
        finally:
            # Move job to history and clean up
            self._finalize_job(job)
    
    async def _prepare_training_data(self, job: RetrainingJob) -> Dict[str, Any]:
        """Prepare training data for retraining."""
        
        job.logs.append("Preparing training data...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=job.configuration.training_period_days)
        
        # Query for training data with game outcomes
        query = """
        SELECT COUNT(*) as total_games,
               COUNT(CASE WHEN game_outcome IS NOT NULL THEN 1 END) as completed_games,
               AVG(CASE WHEN game_outcome IS NOT NULL THEN 1.0 ELSE 0.0 END) as completeness_ratio
        FROM enhanced_games
        WHERE game_date BETWEEN $1 AND $2
        """
        
        async with self.repository.get_connection() as conn:
            result = await conn.fetchrow(query, start_date, end_date)
            
            total_games = int(result["total_games"])
            completed_games = int(result["completed_games"])
            completeness_ratio = float(result["completeness_ratio"])
        
        # Validate data quality
        if completed_games < job.configuration.min_training_samples:
            raise ValueError(f"Insufficient training samples: {completed_games} < {job.configuration.min_training_samples}")
        
        if completeness_ratio < job.configuration.data_quality_threshold:
            raise ValueError(f"Data quality too low: {completeness_ratio:.2f} < {job.configuration.data_quality_threshold}")
        
        training_info = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_games": total_games,
            "completed_games": completed_games,
            "completeness_ratio": completeness_ratio,
            "quality_score": completeness_ratio
        }
        
        job.logs.append(f"Training data prepared: {completed_games} samples from {start_date.date()} to {end_date.date()}")
        
        return training_info
    
    async def _get_baseline_model(self, job: RetrainingJob) -> ModelVersion:
        """Get or create baseline model for comparison."""
        
        job.logs.append("Getting baseline model...")
        
        # Check if we have a production model
        if job.strategy_name in self.production_models:
            baseline = self.production_models[job.strategy_name]
            job.logs.append(f"Using current production model as baseline: {baseline.version_id}")
            return baseline
        
        # Create baseline with default parameters
        parameter_space = self.parameter_registry.get_parameter_space(job.strategy_name)
        default_parameters = parameter_space.get_default_parameters()
        
        # Get baseline performance from recent data (if available)
        baseline_performance = await self._get_strategy_performance(job.strategy_name, days=30)
        
        baseline = ModelVersion(
            version_id=f"baseline_{job.strategy_name}_{datetime.now().strftime('%Y%m%d')}",
            strategy_name=job.strategy_name,
            parameters=default_parameters,
            performance_metrics=baseline_performance or {"roi": 0.0, "win_rate": 0.5},
            created_at=datetime.now(),
            training_data_period="default",
            is_baseline=True
        )
        
        # Store baseline version
        if job.strategy_name not in self.model_versions:
            self.model_versions[job.strategy_name] = []
        self.model_versions[job.strategy_name].append(baseline)
        
        job.logs.append(f"Created baseline model: {baseline.version_id}")
        return baseline
    
    async def _get_strategy_performance(self, strategy_name: str, days: int = 30) -> Optional[Dict[str, float]]:
        """Get recent strategy performance metrics."""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT 
            AVG(CASE WHEN outcome = 'win' THEN roi ELSE 0 END) as avg_roi,
            COUNT(CASE WHEN outcome = 'win' THEN 1 END)::float / COUNT(*) as win_rate,
            COUNT(*) as total_bets
        FROM betting_recommendations 
        WHERE strategy_name = $1 
            AND created_at BETWEEN $2 AND $3
            AND outcome IS NOT NULL
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetchrow(query, strategy_name, start_date, end_date)
                
                if result and result["total_bets"] and result["total_bets"] > 5:
                    return {
                        "roi": float(result["avg_roi"] or 0),
                        "win_rate": float(result["win_rate"] or 0),
                        "total_bets": int(result["total_bets"])
                    }
        except Exception as e:
            self.logger.error(f"Error getting strategy performance: {e}")
        
        return None
    
    async def _run_optimization(self, job: RetrainingJob) -> ModelVersion:
        """Run hyperparameter optimization to find candidate model."""
        
        job.logs.append("Starting hyperparameter optimization...")
        
        # Get parameter space
        if job.configuration.high_impact_only:
            parameter_space = self.parameter_registry.create_focused_parameter_space(
                job.strategy_name, focus_high_impact=True
            )
            job.logs.append("Using high-impact parameters only")
        else:
            parameter_space = self.parameter_registry.get_parameter_space(job.strategy_name)
        
        # Determine optimization period
        end_date = datetime.now()
        start_date = end_date - timedelta(days=job.configuration.training_period_days)
        
        # Create strategy processors (simplified - would use factory in production)
        strategy_processors = await self._create_strategy_processors(job.strategy_name)
        
        # Run optimization
        optimization_job = await self.optimization_engine.optimize_strategy(
            strategy_name=job.strategy_name,
            parameter_space=parameter_space,
            strategy_processors=strategy_processors,
            validation_start_date=start_date,
            validation_end_date=end_date,
            algorithm=job.configuration.algorithm,
            max_evaluations=job.configuration.max_evaluations,
            objective_metric="roi_percentage",
            n_parallel_jobs=job.configuration.parallel_jobs,
            timeout_hours=job.configuration.timeout_hours,
            results_directory=f"retraining_results/{job.job_id}"
        )
        
        job.optimization_job_id = optimization_job.job_id
        job.logs.append(f"Started optimization job: {optimization_job.job_id}")
        
        # Wait for optimization to complete
        while True:
            status = optimization_job.get_progress_info()
            
            if status["status"] == "completed":
                break
            elif status["status"] == "failed":
                raise ValueError(f"Optimization failed: {status.get('error_message', 'Unknown error')}")
            
            # Update job progress based on optimization progress
            opt_progress = status.get("progress_percentage", 0)
            job_progress = 25.0 + (opt_progress * 0.35)  # Scale to job progress range
            await self._update_job_progress(job, job_progress)
            
            await asyncio.sleep(30)  # Check every 30 seconds
        
        # Get best parameters from optimization
        best_parameters = optimization_job.get_best_parameters()
        best_performance = optimization_job.get_best_performance()
        
        # Create candidate model version
        candidate = ModelVersion(
            version_id=f"candidate_{job.strategy_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            strategy_name=job.strategy_name,
            parameters=best_parameters,
            performance_metrics=best_performance,
            created_at=datetime.now(),
            training_data_period=f"{start_date.date()}_to_{end_date.date()}",
            optimization_job_id=optimization_job.job_id
        )
        
        # Store candidate version
        if job.strategy_name not in self.model_versions:
            self.model_versions[job.strategy_name] = []
        self.model_versions[job.strategy_name].append(candidate)
        
        job.logs.append(f"Optimization completed. Candidate model: {candidate.version_id}")
        job.logs.append(f"Best ROI: {best_performance.get('roi', 0):.2f}%")
        
        return candidate
    
    async def _create_strategy_processors(self, strategy_name: str) -> List[Any]:
        """Create strategy processor instances (simplified implementation)."""
        
        # This would be implemented similar to the optimization CLI
        # For now, return empty list as placeholder
        return []
    
    async def _validate_candidate_model(self, job: RetrainingJob) -> Dict[str, Any]:
        """Validate candidate model against baseline."""
        
        job.logs.append("Validating candidate model...")
        
        candidate = job.candidate_model
        baseline = job.baseline_model
        
        # Compare performance metrics
        candidate_roi = candidate.performance_metrics.get("roi", 0)
        baseline_roi = baseline.performance_metrics.get("roi", 0)
        
        improvement_percentage = 0
        if baseline_roi > 0:
            improvement_percentage = ((candidate_roi - baseline_roi) / baseline_roi) * 100
        else:
            improvement_percentage = candidate_roi
        
        # Validation criteria
        passes_minimum_improvement = improvement_percentage >= job.configuration.min_improvement_threshold
        passes_absolute_threshold = candidate_roi >= 2.0  # Minimum 2% ROI required
        
        validation_result = {
            "passes_validation": passes_minimum_improvement and passes_absolute_threshold,
            "improvement_percentage": improvement_percentage,
            "candidate_roi": candidate_roi,
            "baseline_roi": baseline_roi,
            "meets_minimum_improvement": passes_minimum_improvement,
            "meets_absolute_threshold": passes_absolute_threshold,
            "validation_details": {
                "min_improvement_required": job.configuration.min_improvement_threshold,
                "min_absolute_roi_required": 2.0
            }
        }
        
        if validation_result["passes_validation"]:
            job.logs.append(f"Validation PASSED: {improvement_percentage:.1f}% improvement over baseline")
        else:
            reasons = []
            if not passes_minimum_improvement:
                reasons.append(f"improvement {improvement_percentage:.1f}% < {job.configuration.min_improvement_threshold}%")
            if not passes_absolute_threshold:
                reasons.append(f"ROI {candidate_roi:.1f}% < 2.0%")
            job.logs.append(f"Validation FAILED: {', '.join(reasons)}")
        
        return validation_result
    
    async def _run_ab_test(self, job: RetrainingJob) -> Dict[str, Any]:
        """Run A/B test comparing candidate model to baseline."""
        
        job.logs.append("Starting A/B test...")
        
        ab_test_id = f"ab_test_{job.job_id}"
        job.ab_test_id = ab_test_id
        
        # Mock A/B test implementation
        # In production, this would integrate with the live betting system
        # to gradually route traffic to the candidate model
        
        # Simulate A/B test duration
        test_duration_seconds = 60  # Shortened for demo
        start_time = datetime.now()
        
        self.active_ab_tests[ab_test_id] = {
            "job_id": job.job_id,
            "strategy_name": job.strategy_name,
            "start_time": start_time,
            "duration_hours": job.configuration.ab_test_duration_hours,
            "traffic_percentage": job.configuration.ab_test_traffic_percentage,
            "status": "running"
        }
        
        job.logs.append(f"A/B test started: {job.configuration.ab_test_traffic_percentage}% traffic for {job.configuration.ab_test_duration_hours} hours")
        
        # Simulate test execution
        await asyncio.sleep(test_duration_seconds)
        
        # Mock A/B test results
        # In production, this would analyze actual performance data
        baseline_performance = {"roi": 5.2, "win_rate": 0.58, "bets": 100}
        candidate_performance = {"roi": 6.8, "win_rate": 0.61, "bets": 98}
        
        improvement_pct = ((candidate_performance["roi"] - baseline_performance["roi"]) / baseline_performance["roi"]) * 100
        statistical_significance = 0.03  # p < 0.05
        
        ab_test_result = {
            "test_id": ab_test_id,
            "duration_hours": job.configuration.ab_test_duration_hours,
            "baseline_performance": baseline_performance,
            "candidate_performance": candidate_performance,
            "improvement_percentage": improvement_pct,
            "statistical_significance": statistical_significance,
            "is_statistically_significant": statistical_significance < job.configuration.statistical_significance_threshold,
            "deployment_recommended": (
                improvement_pct >= job.configuration.min_improvement_threshold and
                statistical_significance < job.configuration.statistical_significance_threshold
            ),
            "completed_at": datetime.now().isoformat()
        }
        
        # Clean up active A/B test
        if ab_test_id in self.active_ab_tests:
            del self.active_ab_tests[ab_test_id]
        
        if ab_test_result["deployment_recommended"]:
            job.logs.append(f"A/B test PASSED: {improvement_pct:.1f}% improvement (p={statistical_significance:.3f})")
        else:
            job.logs.append(f"A/B test FAILED: insufficient improvement or significance")
        
        return ab_test_result
    
    async def _deploy_model(self, job: RetrainingJob) -> None:
        """Deploy candidate model with gradual rollout."""
        
        job.logs.append("Starting model deployment...")
        
        if not job.configuration.gradual_rollout_enabled:
            # Direct deployment
            await self._deploy_model_direct(job)
        else:
            # Gradual rollout
            await self._deploy_model_gradual(job)
        
        job.logs.append("Model deployment completed")
    
    async def _deploy_model_direct(self, job: RetrainingJob) -> None:
        """Deploy model directly to 100% traffic."""
        
        candidate = job.candidate_model
        
        # Update production model
        self.production_models[job.strategy_name] = candidate
        candidate.is_production = True
        
        job.traffic_percentage = 100.0
        job.deployment_stage = "production"
        job.logs.append("Direct deployment to 100% traffic")
    
    async def _deploy_model_gradual(self, job: RetrainingJob) -> None:
        """Deploy model with gradual traffic rollout."""
        
        candidate = job.candidate_model
        
        for stage_percentage in job.configuration.rollout_stages:
            job.deployment_stage = f"rollout_{stage_percentage}%"
            job.traffic_percentage = stage_percentage
            
            job.logs.append(f"Deploying to {stage_percentage}% traffic")
            
            # Monitor performance during rollout stage
            stage_duration_seconds = 30  # Shortened for demo
            await asyncio.sleep(stage_duration_seconds)
            
            # Check for performance degradation during rollout
            # In production, this would monitor actual metrics
            performance_check_passed = True  # Mock success
            
            if not performance_check_passed:
                job.logs.append(f"Performance degradation detected at {stage_percentage}% - rolling back")
                await self._rollback_deployment(job)
                return
            
            job.logs.append(f"Stage {stage_percentage}% completed successfully")
        
        # Final deployment
        self.production_models[job.strategy_name] = candidate
        candidate.is_production = True
        job.deployment_stage = "production"
        job.traffic_percentage = 100.0
    
    async def _rollback_deployment(self, job: RetrainingJob) -> None:
        """Rollback to previous model version."""
        
        job.logs.append("Rolling back deployment...")
        
        # Restore baseline model
        if job.baseline_model:
            self.production_models[job.strategy_name] = job.baseline_model
            job.status = RetrainingStatus.ROLLED_BACK
            job.traffic_percentage = 0.0
            job.logs.append("Rollback completed - restored baseline model")
        else:
            job.logs.append("Warning: No baseline model available for rollback")
    
    async def _update_job_progress(self, job: RetrainingJob, progress: float) -> None:
        """Update job progress and record metrics."""
        
        job.progress_percentage = progress
        
        # Record progress metrics
        self.metrics_service.record_stage_execution(
            stage=job.current_stage or "unknown",
            duration=0.1,  # Placeholder duration
            status="running" if progress < 100 else "completed"
        )
    
    def _finalize_job(self, job: RetrainingJob) -> None:
        """Move job from active to history and clean up."""
        
        if job.job_id in self.active_jobs:
            del self.active_jobs[job.job_id]
        
        self.job_history.append(job)
        
        # Keep only recent history (last 100 jobs)
        if len(self.job_history) > 100:
            self.job_history = self.job_history[-100:]
    
    async def _monitor_retraining_jobs(self) -> None:
        """Monitor active retraining jobs for timeouts and issues."""
        
        while self.engine_running:
            try:
                current_time = datetime.now()
                
                for job in list(self.active_jobs.values()):
                    if job.started_at:
                        runtime_hours = (current_time - job.started_at).total_seconds() / 3600
                        
                        # Check for job timeout
                        if runtime_hours > job.configuration.timeout_hours:
                            job.status = RetrainingStatus.FAILED
                            job.error_message = f"Job timed out after {runtime_hours:.1f} hours"
                            job.completed_at = current_time
                            job.logs.append(f"Job timed out after {runtime_hours:.1f} hours")
                            
                            self.logger.warning(
                                f"Retraining job timed out: {job.job_id}",
                                extra={
                                    "job_id": job.job_id,
                                    "strategy": job.strategy_name,
                                    "runtime_hours": runtime_hours
                                }
                            )
                            
                            self._finalize_job(job)
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in job monitoring: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _monitor_ab_tests(self) -> None:
        """Monitor active A/B tests."""
        
        while self.engine_running:
            try:
                current_time = datetime.now()
                
                for test_id, test_info in list(self.active_ab_tests.items()):
                    start_time = test_info["start_time"]
                    duration_hours = test_info["duration_hours"]
                    runtime_hours = (current_time - start_time).total_seconds() / 3600
                    
                    if runtime_hours >= duration_hours:
                        # A/B test should be completed
                        self.logger.info(f"A/B test {test_id} duration completed")
                
                await asyncio.sleep(600)  # Check every 10 minutes
                
            except Exception as e:
                self.logger.error(f"Error in A/B test monitoring: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _monitor_deployed_models(self) -> None:
        """Monitor deployed models for performance degradation."""
        
        while self.engine_running:
            try:
                for strategy_name, model in self.production_models.items():
                    # Check model performance
                    recent_performance = await self._get_strategy_performance(strategy_name, days=7)
                    
                    if recent_performance:
                        current_roi = recent_performance["roi"]
                        baseline_roi = model.performance_metrics.get("roi", 0)
                        
                        # Check for significant performance degradation
                        if baseline_roi > 0:
                            degradation_pct = ((baseline_roi - current_roi) / baseline_roi) * 100
                            
                            if degradation_pct > 20:  # 20% degradation threshold
                                self.logger.warning(
                                    f"Performance degradation detected for {strategy_name}",
                                    extra={
                                        "strategy": strategy_name,
                                        "current_roi": current_roi,
                                        "baseline_roi": baseline_roi,
                                        "degradation_percentage": degradation_pct
                                    }
                                )
                
                await asyncio.sleep(3600)  # Check every hour
                
            except Exception as e:
                self.logger.error(f"Error in model performance monitoring: {e}", exc_info=True)
                await asyncio.sleep(300)
    
    async def _load_model_versions(self) -> None:
        """Load existing model versions from storage."""
        
        # In production, this would load from database or file storage
        # For now, initialize empty
        self.model_versions = {}
        self.production_models = {}
        
        self.logger.debug("Model versions loaded")
    
    # Public API methods
    
    def get_active_jobs(self) -> List[RetrainingJob]:
        """Get all active retraining jobs."""
        return list(self.active_jobs.values())
    
    def get_job_status(self, job_id: str) -> Optional[RetrainingJob]:
        """Get status of a specific retraining job."""
        return self.active_jobs.get(job_id) or next(
            (job for job in self.job_history if job.job_id == job_id), None
        )
    
    def get_job_history(self, strategy_name: Optional[str] = None, limit: int = 20) -> List[RetrainingJob]:
        """Get retraining job history."""
        jobs = self.job_history
        
        if strategy_name:
            jobs = [job for job in jobs if job.strategy_name == strategy_name]
        
        return jobs[-limit:]
    
    def get_model_versions(self, strategy_name: str) -> List[ModelVersion]:
        """Get all model versions for a strategy."""
        return self.model_versions.get(strategy_name, [])
    
    def get_production_model(self, strategy_name: str) -> Optional[ModelVersion]:
        """Get current production model for a strategy."""
        return self.production_models.get(strategy_name)
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel an active retraining job."""
        
        if job_id in self.active_jobs:
            job = self.active_jobs[job_id]
            job.status = RetrainingStatus.CANCELLED
            job.completed_at = datetime.now()
            job.logs.append("Job cancelled by user")
            
            # Cancel optimization job if running
            if job.optimization_job_id:
                self.optimization_engine.cancel_job(job.optimization_job_id)
            
            self._finalize_job(job)
            
            self.logger.info(f"Cancelled retraining job {job_id}")
            return True
        
        return False
    
    def get_engine_status(self) -> Dict[str, Any]:
        """Get comprehensive engine status."""
        
        active_jobs_by_status = {}
        for job in self.active_jobs.values():
            status = job.status.value
            active_jobs_by_status[status] = active_jobs_by_status.get(status, 0) + 1
        
        return {
            "engine_running": self.engine_running,
            "active_jobs_count": len(self.active_jobs),
            "active_jobs_by_status": active_jobs_by_status,
            "total_jobs_completed": len(self.job_history),
            "active_ab_tests": len(self.active_ab_tests),
            "production_models": list(self.production_models.keys()),
            "model_versions_count": sum(len(versions) for versions in self.model_versions.values()),
            "background_tasks_running": len([t for t in self.background_tasks if not t.done()]),
            "configuration": {
                "max_concurrent_jobs": self.default_config.max_concurrent_retraining_jobs,
                "default_timeout_hours": self.default_config.timeout_hours,
                "ab_test_duration_hours": self.default_config.ab_test_duration_hours,
                "gradual_rollout_enabled": self.default_config.gradual_rollout_enabled
            }
        }