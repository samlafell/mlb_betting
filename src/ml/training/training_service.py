"""
ML Training Service
Orchestrates LightGBM training pipeline with scheduling and monitoring
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path

from .lightgbm_trainer import LightGBMTrainer

# Add src to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from core.config import get_settings

logger = logging.getLogger(__name__)


class MLTrainingService:
    """
    ML training service with scheduling and model lifecycle management
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.trainer = LightGBMTrainer()
        
        # Training configuration
        self.default_training_config = {
            'prediction_targets': ['moneyline_home_win', 'total_over_under', 'run_total_regression'],
            'training_window_days': 90,  # 3 months of training data
            'cross_validation_folds': 5,
            'test_size': 0.2,
            'use_cached_features': True
        }
        
        # Retraining configuration
        self.retraining_config = {
            'sliding_window_days': 7,
            'min_samples_for_retrain': 100,
            'retrain_schedule_hours': 24,  # Retrain every 24 hours
            'performance_degradation_threshold': 0.05  # 5% performance drop triggers retrain
        }
        
        # Service state
        self.service_state = {
            'last_training_run': None,
            'last_retrain_check': None,
            'active_models': {},
            'training_in_progress': False,
            'service_health': 'healthy'
        }
    
    async def train_initial_models(
        self,
        end_date: Optional[datetime] = None,
        training_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Train initial set of models for production deployment
        
        Args:
            end_date: End date for training data (default: now)
            training_config: Custom training configuration
            
        Returns:
            Training results and model deployment status
        """
        try:
            if self.service_state['training_in_progress']:
                raise ValueError("Training already in progress")
            
            self.service_state['training_in_progress'] = True
            
            logger.info("Starting initial model training...")
            
            # Use provided config or defaults
            config = training_config or self.default_training_config
            
            # Calculate training period
            if end_date is None:
                end_date = datetime.utcnow()
            
            start_date = end_date - timedelta(days=config['training_window_days'])
            
            # Train models
            training_results = await self.trainer.train_models(
                start_date=start_date,
                end_date=end_date,
                prediction_targets=config['prediction_targets'],
                use_cached_features=config['use_cached_features'],
                cross_validation_folds=config['cross_validation_folds'],
                test_size=config['test_size']
            )
            
            # Update service state
            self.service_state['last_training_run'] = datetime.utcnow()
            self.service_state['active_models'] = {
                target: {
                    'trained_at': datetime.utcnow(),
                    'training_samples': training_results.get('data_samples', 0),
                    'performance_metrics': training_results['training_results'].get(target, {}).get('test_metrics', {}),
                    'status': 'active'
                }
                for target in config['prediction_targets']
                if target in training_results['training_results']
            }
            
            logger.info(f"Initial model training completed: {len(self.service_state['active_models'])} models trained")
            
            return {
                'status': 'success',
                'models_trained': list(self.service_state['active_models'].keys()),
                'training_results': training_results,
                'deployment_ready': True
            }
            
        except Exception as e:
            logger.error(f"Error in initial model training: {e}")
            self.service_state['service_health'] = 'error'
            raise
        finally:
            self.service_state['training_in_progress'] = False
    
    async def check_and_retrain_models(
        self,
        force_retrain: bool = False
    ) -> Dict[str, Any]:
        """
        Check model performance and retrain if necessary
        
        Args:
            force_retrain: Force retraining regardless of performance
            
        Returns:
            Retraining results and updated model status
        """
        try:
            logger.info("Checking models for retraining needs...")
            
            current_time = datetime.utcnow()
            retrain_results = {}
            
            for model_name, model_info in self.service_state['active_models'].items():
                should_retrain = force_retrain
                retrain_reason = "forced" if force_retrain else None
                
                # Check time-based retraining
                if not should_retrain:
                    hours_since_training = (current_time - model_info['trained_at']).total_seconds() / 3600
                    if hours_since_training >= self.retraining_config['retrain_schedule_hours']:
                        should_retrain = True
                        retrain_reason = "scheduled"
                
                # Check performance-based retraining
                if not should_retrain:
                    performance_degraded = await self._check_model_performance_degradation(model_name)
                    if performance_degraded:
                        should_retrain = True
                        retrain_reason = "performance_degradation"
                
                if should_retrain:
                    logger.info(f"Retraining model {model_name} - reason: {retrain_reason}")
                    
                    retrain_result = await self.trainer.retrain_model(
                        model_name=model_name,
                        sliding_window_days=self.retraining_config['sliding_window_days'],
                        min_samples=self.retraining_config['min_samples_for_retrain']
                    )
                    
                    if retrain_result.get('status') != 'skipped':
                        # Update model info
                        self.service_state['active_models'][model_name].update({
                            'trained_at': current_time,
                            'performance_metrics': retrain_result.get('test_metrics', {}),
                            'drift_detected': retrain_result.get('drift_detected', False),
                            'retrain_reason': retrain_reason
                        })
                    
                    retrain_results[model_name] = retrain_result
                else:
                    logger.debug(f"Model {model_name} does not need retraining")
            
            self.service_state['last_retrain_check'] = current_time
            
            logger.info(f"Retraining check completed: {len(retrain_results)} models processed")
            
            return {
                'retrain_results': retrain_results,
                'models_retrained': len([r for r in retrain_results.values() if r.get('status') != 'skipped']),
                'active_models': self.service_state['active_models']
            }
            
        except Exception as e:
            logger.error(f"Error in model retraining check: {e}")
            raise
    
    async def evaluate_model_performance(
        self,
        model_name: str,
        evaluation_days: int = 7
    ) -> Dict[str, Any]:
        """
        Evaluate model performance on recent data
        
        Args:
            model_name: Model to evaluate
            evaluation_days: Days of recent data for evaluation
            
        Returns:
            Performance evaluation results
        """
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=evaluation_days)
            
            evaluation_results = await self.trainer.evaluate_model_performance(
                model_name=model_name,
                evaluation_start=start_date,
                evaluation_end=end_date
            )
            
            logger.info(f"Model evaluation completed for {model_name}")
            return evaluation_results
            
        except Exception as e:
            logger.error(f"Error evaluating model {model_name}: {e}")
            raise
    
    async def get_model_info(self, model_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about trained models
        
        Args:
            model_name: Specific model name (optional)
            
        Returns:
            Model information and status
        """
        try:
            if model_name:
                if model_name not in self.service_state['active_models']:
                    raise ValueError(f"Model {model_name} not found")
                
                model_info = self.service_state['active_models'][model_name].copy()
                
                # Add trainer statistics
                trainer_stats = self.trainer.get_training_stats()
                model_specific_stats = trainer_stats.get('best_model_scores', {}).get(model_name, {})
                
                model_info['best_scores'] = model_specific_stats
                
                return {
                    'model_name': model_name,
                    'model_info': model_info
                }
            else:
                # Return all models info
                all_models_info = {}
                for name, info in self.service_state['active_models'].items():
                    model_info = info.copy()
                    trainer_stats = self.trainer.get_training_stats()
                    model_specific_stats = trainer_stats.get('best_model_scores', {}).get(name, {})
                    model_info['best_scores'] = model_specific_stats
                    all_models_info[name] = model_info
                
                return {
                    'active_models': all_models_info,
                    'service_stats': self.trainer.get_training_stats()
                }
                
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            raise
    
    async def schedule_training_job(
        self,
        schedule_type: str = "daily",
        schedule_hour: int = 2,
        training_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Schedule automated training jobs
        
        Args:
            schedule_type: Type of schedule ('daily', 'weekly')
            schedule_hour: Hour of day to run (0-23)
            training_config: Custom training configuration
            
        Returns:
            Scheduling status and configuration
        """
        try:
            # This is a simplified version - in production, you'd use a task scheduler like Celery
            logger.info(f"Training job scheduled: {schedule_type} at {schedule_hour}:00")
            
            # Store schedule configuration
            schedule_config = {
                'schedule_type': schedule_type,
                'schedule_hour': schedule_hour,
                'training_config': training_config or self.default_training_config,
                'created_at': datetime.utcnow(),
                'status': 'active'
            }
            
            return {
                'status': 'scheduled',
                'schedule_config': schedule_config,
                'next_run': self._calculate_next_run_time(schedule_type, schedule_hour)
            }
            
        except Exception as e:
            logger.error(f"Error scheduling training job: {e}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check of training service
        
        Returns:
            Health status and service metrics
        """
        try:
            health_status = {
                'service_status': self.service_state['service_health'],
                'training_in_progress': self.service_state['training_in_progress'],
                'active_models_count': len(self.service_state['active_models']),
                'last_training_run': self.service_state['last_training_run'],
                'last_retrain_check': self.service_state['last_retrain_check']
            }
            
            # Check trainer health
            trainer_stats = self.trainer.get_training_stats()
            health_status['trainer_stats'] = trainer_stats
            
            # Check if models are too old
            current_time = datetime.utcnow()
            stale_models = []
            
            for model_name, model_info in self.service_state['active_models'].items():
                hours_since_training = (current_time - model_info['trained_at']).total_seconds() / 3600
                if hours_since_training > 72:  # 3 days
                    stale_models.append(model_name)
            
            health_status['stale_models'] = stale_models
            health_status['models_need_attention'] = len(stale_models) > 0
            
            # Overall health assessment
            if self.service_state['service_health'] == 'error':
                health_status['overall_health'] = 'unhealthy'
            elif len(stale_models) > 0:
                health_status['overall_health'] = 'degraded'
            elif len(self.service_state['active_models']) == 0:
                health_status['overall_health'] = 'no_models'
            else:
                health_status['overall_health'] = 'healthy'
            
            return health_status
            
        except Exception as e:
            logger.error(f"Error in health check: {e}")
            return {
                'overall_health': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow()
            }
    
    async def _check_model_performance_degradation(self, model_name: str) -> bool:
        """Check if model performance has degraded significantly"""
        try:
            # Evaluate model on recent 3 days of data
            evaluation_results = await self.evaluate_model_performance(model_name, evaluation_days=3)
            
            # Get baseline performance
            baseline_metrics = self.service_state['active_models'][model_name].get('performance_metrics', {})
            current_metrics = evaluation_results.get('performance_metrics', {})
            
            # Check for performance degradation
            for metric_name, baseline_value in baseline_metrics.items():
                if metric_name in current_metrics:
                    current_value = current_metrics[metric_name]
                    
                    # Calculate relative degradation
                    if baseline_value > 0:
                        degradation = (baseline_value - current_value) / baseline_value
                        
                        if degradation > self.retraining_config['performance_degradation_threshold']:
                            logger.warning(f"Performance degradation detected for {model_name} - {metric_name}: {degradation:.3f}")
                            return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking performance degradation for {model_name}: {e}")
            return False  # Default to no degradation on error
    
    def _calculate_next_run_time(self, schedule_type: str, schedule_hour: int) -> datetime:
        """Calculate next scheduled run time"""
        now = datetime.utcnow()
        
        if schedule_type == "daily":
            next_run = now.replace(hour=schedule_hour, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
        elif schedule_type == "weekly":
            # Run on Sundays
            days_until_sunday = (6 - now.weekday()) % 7
            if days_until_sunday == 0 and now.hour >= schedule_hour:
                days_until_sunday = 7
            next_run = now + timedelta(days=days_until_sunday)
            next_run = next_run.replace(hour=schedule_hour, minute=0, second=0, microsecond=0)
        else:
            raise ValueError(f"Unknown schedule type: {schedule_type}")
        
        return next_run
    
    def get_service_state(self) -> Dict[str, Any]:
        """Get current service state"""
        return self.service_state.copy()