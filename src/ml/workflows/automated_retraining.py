"""
Automated Model Retraining Workflows
Scheduled retraining, performance monitoring, and automatic model updates
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from enum import Enum
import json

from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ...core.config import get_settings
from ..training.lightgbm_trainer import LightGBMTrainer
from ..registry.model_registry import model_registry, ModelStage
from ..features.feature_pipeline import FeaturePipeline

logger = logging.getLogger(__name__)


class RetrainingTrigger(str, Enum):
    """Retraining trigger types"""
    SCHEDULED = "scheduled"
    PERFORMANCE_DEGRADATION = "performance_degradation"  
    DATA_DRIFT = "data_drift"
    NEW_DATA_AVAILABLE = "new_data_available"
    MANUAL = "manual"


class RetrainingStatus(str, Enum):
    """Retraining job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RetrainingConfig(BaseModel):
    """Configuration for automated retraining"""
    model_name: str
    schedule_cron: Optional[str] = "0 2 * * *"  # Daily at 2 AM
    sliding_window_days: int = 90
    min_samples: int = 100
    performance_threshold: float = 0.05  # 5% degradation triggers retraining
    data_drift_threshold: float = 0.1
    auto_promote_to_staging: bool = True
    auto_promote_to_production: bool = False
    notification_email: Optional[str] = None
    enabled: bool = True


class RetrainingJob(BaseModel):
    """Retraining job information"""
    job_id: str
    model_name: str
    trigger: RetrainingTrigger
    status: RetrainingStatus
    scheduled_time: datetime
    started_time: Optional[datetime] = None
    completed_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Training details
    training_start_date: Optional[datetime] = None
    training_end_date: Optional[datetime] = None
    samples_used: Optional[int] = None
    
    # Results
    new_model_version: Optional[str] = None
    performance_metrics: Optional[Dict[str, float]] = None
    improvement_metrics: Optional[Dict[str, float]] = None
    promoted_to_staging: bool = False
    promoted_to_production: bool = False
    
    # Error information
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None


class AutomatedRetrainingService:
    """
    Service for automated model retraining and lifecycle management
    Handles scheduled retraining, performance monitoring, and automatic promotions
    """

    def __init__(self):
        self.settings = get_settings()
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.running_jobs: Dict[str, RetrainingJob] = {}
        self.job_history: List[RetrainingJob] = []
        self.retraining_configs: Dict[str, RetrainingConfig] = {}
        
        # Initialize components
        self.trainer = LightGBMTrainer()
        self.feature_pipeline = FeaturePipeline()
        
        # Performance monitoring
        self.monitoring_enabled = True
        self.monitoring_interval_minutes = 60
        self.last_performance_check = {}

    async def initialize(self) -> bool:
        """Initialize automated retraining service"""
        try:
            # Initialize scheduler
            self.scheduler = AsyncIOScheduler()
            
            # Initialize model registry
            await model_registry.initialize()
            
            # Load existing retraining configurations
            await self._load_retraining_configs()
            
            # Start performance monitoring
            if self.monitoring_enabled:
                await self._start_performance_monitoring()
            
            # Start scheduler
            self.scheduler.start()
            
            logger.info("Automated retraining service initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize automated retraining service: {e}")
            return False

    async def schedule_model_retraining(
        self, 
        config: RetrainingConfig
    ) -> bool:
        """
        Schedule automated retraining for a model
        
        Args:
            config: Retraining configuration
            
        Returns:
            True if scheduled successfully
        """
        try:
            if not self.scheduler:
                await self.initialize()
            
            # Store configuration
            self.retraining_configs[config.model_name] = config
            
            if not config.enabled:
                logger.info(f"Retraining disabled for {config.model_name}")
                return True
            
            # Create cron trigger
            trigger = CronTrigger.from_crontab(config.schedule_cron)
            
            # Schedule job
            job_id = f"retrain_{config.model_name}"
            self.scheduler.add_job(
                func=self._execute_scheduled_retraining,
                trigger=trigger,
                args=[config.model_name],
                id=job_id,
                replace_existing=True,
                misfire_grace_time=3600  # 1 hour grace period
            )
            
            logger.info(f"Scheduled retraining for {config.model_name}: {config.schedule_cron}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling retraining for {config.model_name}: {e}")
            return False

    async def trigger_manual_retraining(
        self, 
        model_name: str,
        sliding_window_days: Optional[int] = None
    ) -> str:
        """
        Trigger manual retraining for a model
        
        Args:
            model_name: Name of the model to retrain
            sliding_window_days: Override sliding window
            
        Returns:
            Job ID of the retraining job
        """
        try:
            job_id = f"manual_retrain_{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Create retraining job
            job = RetrainingJob(
                job_id=job_id,
                model_name=model_name,
                trigger=RetrainingTrigger.MANUAL,
                status=RetrainingStatus.PENDING,
                scheduled_time=datetime.utcnow()
            )
            
            # Add to running jobs
            self.running_jobs[job_id] = job
            
            # Execute retraining asynchronously
            asyncio.create_task(self._execute_retraining_job(job, sliding_window_days))
            
            logger.info(f"Triggered manual retraining for {model_name}: {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Error triggering manual retraining for {model_name}: {e}")
            raise

    async def check_performance_degradation(self, model_name: str) -> bool:
        """
        Check if model performance has degraded below threshold
        
        Args:
            model_name: Name of the model to check
            
        Returns:
            True if performance degradation detected
        """
        try:
            # Get current production model
            versions = await model_registry.get_model_versions(
                model_name, stages=[ModelStage.PRODUCTION]
            )
            
            if not versions:
                logger.warning(f"No production model found for {model_name}")
                return False
            
            current_model = versions[0]
            
            # Get recent performance (last 7 days)
            recent_performance = await self._get_recent_model_performance(model_name)
            
            if not recent_performance or not current_model.metrics:
                logger.warning(f"Insufficient performance data for {model_name}")
                return False
            
            # Compare performance metrics
            config = self.retraining_configs.get(model_name)
            threshold = config.performance_threshold if config else 0.05
            
            degradation_detected = False
            for metric, current_value in current_model.metrics.items():
                recent_value = recent_performance.get(metric)
                if recent_value is not None:
                    degradation = (current_value - recent_value) / current_value
                    if degradation > threshold:
                        logger.warning(
                            f"Performance degradation detected for {model_name}: "
                            f"{metric} dropped from {current_value:.4f} to {recent_value:.4f} "
                            f"({degradation:.2%})"
                        )
                        degradation_detected = True
            
            return degradation_detected
            
        except Exception as e:
            logger.error(f"Error checking performance degradation for {model_name}: {e}")
            return False

    async def get_retraining_job_status(self, job_id: str) -> Optional[RetrainingJob]:
        """Get status of a retraining job"""
        try:
            # Check running jobs first
            if job_id in self.running_jobs:
                return self.running_jobs[job_id]
            
            # Check job history
            for job in self.job_history:
                if job.job_id == job_id:
                    return job
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting job status for {job_id}: {e}")
            return None

    async def get_model_retraining_history(
        self, 
        model_name: str, 
        limit: int = 10
    ) -> List[RetrainingJob]:
        """Get retraining history for a model"""
        try:
            # Get jobs for this model from history
            model_jobs = [
                job for job in self.job_history 
                if job.model_name == model_name
            ]
            
            # Sort by scheduled time (most recent first)
            model_jobs.sort(key=lambda x: x.scheduled_time, reverse=True)
            
            return model_jobs[:limit]
            
        except Exception as e:
            logger.error(f"Error getting retraining history for {model_name}: {e}")
            return []

    async def cancel_retraining_job(self, job_id: str) -> bool:
        """Cancel a running retraining job"""
        try:
            if job_id not in self.running_jobs:
                return False
            
            job = self.running_jobs[job_id]
            
            if job.status == RetrainingStatus.RUNNING:
                # Note: This is a simplified cancellation
                # In production, you'd need more sophisticated job cancellation
                job.status = RetrainingStatus.CANCELLED
                job.completed_time = datetime.utcnow()
                
                # Move to history
                self.job_history.append(job)
                del self.running_jobs[job_id]
                
                logger.info(f"Cancelled retraining job {job_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            return False

    async def _execute_scheduled_retraining(self, model_name: str):
        """Execute scheduled retraining"""
        try:
            job_id = f"scheduled_retrain_{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Create retraining job
            job = RetrainingJob(
                job_id=job_id,
                model_name=model_name,
                trigger=RetrainingTrigger.SCHEDULED,
                status=RetrainingStatus.PENDING,
                scheduled_time=datetime.utcnow()
            )
            
            # Add to running jobs
            self.running_jobs[job_id] = job
            
            # Execute retraining
            await self._execute_retraining_job(job)
            
        except Exception as e:
            logger.error(f"Error in scheduled retraining for {model_name}: {e}")

    async def _execute_retraining_job(
        self, 
        job: RetrainingJob,
        sliding_window_days: Optional[int] = None
    ):
        """Execute a retraining job"""
        try:
            job.status = RetrainingStatus.RUNNING
            job.started_time = datetime.utcnow()
            
            logger.info(f"Starting retraining job {job.job_id}")
            
            # Get configuration
            config = self.retraining_configs.get(job.model_name)
            window_days = sliding_window_days or (config.sliding_window_days if config else 90)
            min_samples = config.min_samples if config else 100
            
            # Calculate training window
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=window_days)
            
            job.training_start_date = start_date
            job.training_end_date = end_date
            
            # Execute retraining
            training_results = await self.trainer.retrain_model(
                model_name=job.model_name,
                sliding_window_days=window_days,
                min_samples=min_samples
            )
            
            if training_results.get("status") == "skipped":
                job.status = RetrainingStatus.CANCELLED
                job.error_message = training_results.get("reason", "Unknown reason")
                job.completed_time = datetime.utcnow()
                self._finalize_job(job)
                return
            
            # Extract results
            job.samples_used = training_results.get("training_samples", 0)
            job.performance_metrics = training_results.get("test_metrics", {})
            
            # Register new model version in MLflow
            new_version = await self._register_retrained_model(job, training_results)
            if new_version:
                job.new_model_version = new_version.version
                
                # Auto-promote to staging if configured
                if config and config.auto_promote_to_staging:
                    promoted = await model_registry.promote_to_staging(
                        job.model_name, 
                        new_version.version
                    )
                    job.promoted_to_staging = promoted
                    
                    if promoted:
                        logger.info(f"Automatically promoted {job.model_name} v{new_version.version} to staging")
                
                # Auto-promote to production if configured and meets criteria
                if config and config.auto_promote_to_production:
                    # Add delay for staging evaluation
                    await asyncio.sleep(60)  # Wait 1 minute (in production, would be longer)
                    
                    promoted = await model_registry.promote_to_production(
                        job.model_name,
                        new_version.version,
                        force=True  # Skip timing requirements for automated promotion
                    )
                    job.promoted_to_production = promoted
                    
                    if promoted:
                        logger.info(f"Automatically promoted {job.model_name} v{new_version.version} to production")
            
            job.status = RetrainingStatus.COMPLETED
            job.completed_time = datetime.utcnow()
            
            logger.info(f"Completed retraining job {job.job_id}")
            
        except Exception as e:
            job.status = RetrainingStatus.FAILED
            job.error_message = str(e)
            job.completed_time = datetime.utcnow()
            logger.error(f"Retraining job {job.job_id} failed: {e}")
            
        finally:
            # Calculate duration
            if job.started_time and job.completed_time:
                job.duration_seconds = (job.completed_time - job.started_time).total_seconds()
            
            # Finalize job
            self._finalize_job(job)

    async def _register_retrained_model(
        self, 
        job: RetrainingJob, 
        training_results: Dict[str, Any]
    ) -> Optional[Any]:
        """Register retrained model in MLflow registry"""
        try:
            # Get MLflow run info
            run_id = training_results.get("run_id")
            if not run_id:
                logger.error("No MLflow run ID in training results")
                return None
            
            # Create model URI
            model_uri = f"runs:/{run_id}/model"
            
            # Register model version
            model_version = await model_registry.register_model(
                model_uri=model_uri,
                model_name=job.model_name,
                description=f"Automated retraining on {job.started_time.strftime('%Y-%m-%d %H:%M:%S')}",
                tags={
                    "retraining_job_id": job.job_id,
                    "trigger": job.trigger.value,
                    "automated": "true",
                    "training_samples": str(job.samples_used or 0)
                }
            )
            
            return model_version
            
        except Exception as e:
            logger.error(f"Error registering retrained model for job {job.job_id}: {e}")
            return None

    def _finalize_job(self, job: RetrainingJob):
        """Finalize a retraining job"""
        try:
            # Move from running jobs to history
            if job.job_id in self.running_jobs:
                del self.running_jobs[job.job_id]
            
            self.job_history.append(job)
            
            # Keep only last 100 jobs in history
            if len(self.job_history) > 100:
                self.job_history = self.job_history[-100:]
            
            # Send notification if configured
            config = self.retraining_configs.get(job.model_name)
            if config and config.notification_email:
                asyncio.create_task(self._send_notification(job, config))
            
        except Exception as e:
            logger.error(f"Error finalizing job {job.job_id}: {e}")

    async def _start_performance_monitoring(self):
        """Start performance monitoring scheduler"""
        try:
            if not self.scheduler:
                return
            
            # Schedule performance monitoring
            self.scheduler.add_job(
                func=self._monitor_model_performance,
                trigger=IntervalTrigger(minutes=self.monitoring_interval_minutes),
                id="performance_monitoring",
                replace_existing=True
            )
            
            logger.info(f"Started performance monitoring (interval: {self.monitoring_interval_minutes} minutes)")
            
        except Exception as e:
            logger.error(f"Error starting performance monitoring: {e}")

    async def _monitor_model_performance(self):
        """Monitor model performance and trigger retraining if needed"""
        try:
            for model_name, config in self.retraining_configs.items():
                if not config.enabled:
                    continue
                
                try:
                    # Check for performance degradation
                    degradation_detected = await self.check_performance_degradation(model_name)
                    
                    if degradation_detected:
                        # Check if we've already triggered retraining recently
                        last_check = self.last_performance_check.get(model_name)
                        if last_check:
                            time_since_check = datetime.utcnow() - last_check
                            if time_since_check < timedelta(hours=6):  # Don't retrain more than every 6 hours
                                continue
                        
                        # Trigger automatic retraining
                        job_id = await self._trigger_performance_retraining(model_name)
                        logger.info(f"Triggered performance-based retraining for {model_name}: {job_id}")
                        
                        self.last_performance_check[model_name] = datetime.utcnow()
                        
                except Exception as e:
                    logger.error(f"Error monitoring performance for {model_name}: {e}")
            
        except Exception as e:
            logger.error(f"Error in performance monitoring: {e}")

    async def _trigger_performance_retraining(self, model_name: str) -> str:
        """Trigger retraining due to performance degradation"""
        job_id = f"perf_retrain_{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Create retraining job
        job = RetrainingJob(
            job_id=job_id,
            model_name=model_name,
            trigger=RetrainingTrigger.PERFORMANCE_DEGRADATION,
            status=RetrainingStatus.PENDING,
            scheduled_time=datetime.utcnow()
        )
        
        # Add to running jobs
        self.running_jobs[job_id] = job
        
        # Execute retraining asynchronously
        asyncio.create_task(self._execute_retraining_job(job))
        
        return job_id

    async def _get_recent_model_performance(self, model_name: str) -> Optional[Dict[str, float]]:
        """Get recent model performance metrics"""
        # This would typically query your production monitoring system
        # For now, return mock data
        return {
            "accuracy": 0.62,
            "precision": 0.60,
            "recall": 0.65,
            "f1_score": 0.62,
            "roc_auc": 0.68
        }

    async def _load_retraining_configs(self):
        """Load retraining configurations from storage"""
        # In production, this would load from database or config files
        # For now, set up default configurations for existing models
        default_configs = [
            RetrainingConfig(
                model_name="lightgbm_total_over_v1",
                schedule_cron="0 2 * * *",  # Daily at 2 AM
                sliding_window_days=90,
                auto_promote_to_staging=True,
                auto_promote_to_production=False
            ),
            RetrainingConfig(
                model_name="lightgbm_moneyline_v1", 
                schedule_cron="0 3 * * *",  # Daily at 3 AM
                sliding_window_days=90,
                auto_promote_to_staging=True,
                auto_promote_to_production=False
            )
        ]
        
        for config in default_configs:
            self.retraining_configs[config.model_name] = config
            
        logger.info(f"Loaded {len(default_configs)} default retraining configurations")

    async def _send_notification(self, job: RetrainingJob, config: RetrainingConfig):
        """Send notification about retraining job completion"""
        # In production, this would send email, Slack message, etc.
        logger.info(
            f"NOTIFICATION: Retraining job {job.job_id} for {job.model_name} "
            f"completed with status {job.status.value}"
        )

    def get_service_stats(self) -> Dict[str, Any]:
        """Get automated retraining service statistics"""
        try:
            return {
                "scheduler_running": self.scheduler.running if self.scheduler else False,
                "monitoring_enabled": self.monitoring_enabled,
                "configured_models": len(self.retraining_configs),
                "running_jobs": len(self.running_jobs),
                "completed_jobs": len([j for j in self.job_history if j.status == RetrainingStatus.COMPLETED]),
                "failed_jobs": len([j for j in self.job_history if j.status == RetrainingStatus.FAILED]),
                "total_jobs_history": len(self.job_history),
                "scheduled_jobs": len(self.scheduler.get_jobs()) if self.scheduler else 0
            }
        except Exception as e:
            return {"error": str(e)}

    async def shutdown(self):
        """Shutdown the automated retraining service"""
        try:
            if self.scheduler:
                self.scheduler.shutdown()
                logger.info("Automated retraining service shutdown")
        except Exception as e:
            logger.error(f"Error shutting down automated retraining service: {e}")


# Global automated retraining service instance
automated_retraining_service = AutomatedRetrainingService()