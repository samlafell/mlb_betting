"""
Advanced Hyperparameter Optimization Framework
Uses Optuna for intelligent hyperparameter tuning with production monitoring integration
"""

import logging
import asyncio
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.pruners import MedianPruner
    from optuna.storages import RDBStorage
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False

import mlflow
from ..training.lightgbm_trainer import LightGBMTrainer
from ..features.feature_pipeline import FeaturePipeline
from ...services.monitoring.prometheus_metrics_service import get_metrics_service
from ...core.config import get_settings

logger = logging.getLogger(__name__)


class HyperparameterOptimizer:
    """
    Advanced hyperparameter optimization with production monitoring integration
    """
    
    def __init__(self):
        if not OPTUNA_AVAILABLE:
            raise ImportError("Optuna is required for hyperparameter optimization. Install with: pip install optuna")
        
        self.settings = get_settings()
        self.trainer = LightGBMTrainer()
        self.feature_pipeline = FeaturePipeline()
        self.metrics_service = get_metrics_service()
        
        # Initialize Optuna storage
        self._initialize_optuna_storage()
        
        # Optimization configuration
        self.optimization_config = {
            "n_trials": 100,
            "timeout_hours": 6,
            "n_jobs": 4,  # Parallel optimization
            "cv_folds": 5,
            "optimization_metric": "roc_auc",  # Primary optimization target
            "early_stopping_rounds": 50,
            "pruning_enabled": True,
            "sampler_warmup_steps": 10,
        }
        
        # Hyperparameter search spaces
        self.search_spaces = {
            "lightgbm_binary": {
                "num_leaves": ("int", 10, 300),
                "learning_rate": ("float", 0.01, 0.3),
                "feature_fraction": ("float", 0.4, 1.0),
                "bagging_fraction": ("float", 0.4, 1.0),
                "bagging_freq": ("int", 1, 7),
                "min_child_samples": ("int", 5, 100),
                "max_depth": ("int", 3, 12),
                "reg_alpha": ("float", 0.0, 10.0),
                "reg_lambda": ("float", 0.0, 10.0),
                "subsample_for_bin": ("int", 50000, 200000),
            },
            "lightgbm_regression": {
                "num_leaves": ("int", 10, 300),
                "learning_rate": ("float", 0.01, 0.3),
                "feature_fraction": ("float", 0.4, 1.0),
                "bagging_fraction": ("float", 0.4, 1.0),
                "bagging_freq": ("int", 1, 7),
                "min_child_samples": ("int", 5, 100),
                "max_depth": ("int", 3, 12),
                "reg_alpha": ("float", 0.0, 10.0),
                "reg_lambda": ("float", 0.0, 10.0),
                "objective": ("categorical", ["regression", "regression_l1", "huber"]),
            }
        }
        
        # Best parameters cache
        self.best_parameters = {}
        self.optimization_history = {}

    def _initialize_optuna_storage(self):
        """Initialize Optuna storage backend"""
        try:
            # Use SQLite for storage (can be upgraded to PostgreSQL for production)
            storage_url = f"sqlite:///{self.settings.ml_pipeline.optuna_db_path}"
            self.storage = RDBStorage(url=storage_url)
            logger.info(f"Optuna storage initialized: {storage_url}")
        except Exception as e:
            logger.error(f"Failed to initialize Optuna storage: {e}")
            self.storage = None

    async def optimize_hyperparameters(
        self,
        model_name: str,
        start_date: datetime,
        end_date: datetime,
        optimization_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Optimize hyperparameters for a specific model
        
        Args:
            model_name: Target model to optimize
            start_date: Training data start date
            end_date: Training data end date  
            optimization_config: Custom optimization configuration
            
        Returns:
            Optimization results with best parameters and performance metrics
        """
        try:
            logger.info(f"Starting hyperparameter optimization for {model_name}")
            
            # Record optimization start in metrics
            self.metrics_service.record_pipeline_start(
                f"hyperopt_{model_name}", "hyperparameter_optimization"
            )
            
            # Merge configuration
            config = {**self.optimization_config, **(optimization_config or {})}
            
            # Validate model configuration
            if model_name not in self.trainer.model_configs:
                raise ValueError(f"Model {model_name} not found in trainer configurations")
            
            model_config = self.trainer.model_configs[model_name]
            model_type = "lightgbm_binary" if model_config["objective"] == "binary" else "lightgbm_regression"
            
            if model_type not in self.search_spaces:
                raise ValueError(f"No search space defined for model type: {model_type}")
            
            # Create study
            study_name = f"{model_name}_optimization_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            sampler = TPESampler(
                n_startup_trials=config["sampler_warmup_steps"],
                n_ei_candidates=24
            )
            
            pruner = MedianPruner(
                n_startup_trials=10,
                n_warmup_steps=20,
                interval_steps=10
            ) if config["pruning_enabled"] else None
            
            study = optuna.create_study(
                study_name=study_name,
                storage=self.storage,
                direction="maximize",
                sampler=sampler,
                pruner=pruner,
                load_if_exists=True
            )
            
            # Define objective function
            objective_func = self._create_objective_function(
                model_name, model_type, start_date, end_date, config
            )
            
            # Run optimization
            start_time = datetime.now()
            
            await asyncio.to_thread(
                study.optimize,
                objective_func,
                n_trials=config["n_trials"],
                timeout=config["timeout_hours"] * 3600,
                n_jobs=config["n_jobs"]
            )
            
            optimization_time = (datetime.now() - start_time).total_seconds()
            
            # Get best results
            best_params = study.best_params
            best_value = study.best_value
            
            # Store best parameters
            self.best_parameters[model_name] = {
                "parameters": best_params,
                "score": best_value,
                "optimization_date": datetime.now(),
                "trials_completed": len(study.trials),
                "optimization_time_seconds": optimization_time
            }
            
            # Record metrics
            self.metrics_service.record_pipeline_completion(
                f"hyperopt_{model_name}",
                "hyperparameter_optimization", 
                "success",
                stages_executed=len(study.trials)
            )
            
            # Store optimization history
            self.optimization_history[model_name] = {
                "study_name": study_name,
                "best_params": best_params,
                "best_score": best_value,
                "trials": [
                    {
                        "number": trial.number,
                        "value": trial.value,
                        "params": trial.params,
                        "state": trial.state.name
                    }
                    for trial in study.trials
                ]
            }
            
            logger.info(f"Hyperparameter optimization completed for {model_name}: "
                       f"best_score={best_value:.4f}, trials={len(study.trials)}")
            
            return {
                "model_name": model_name,
                "best_parameters": best_params,
                "best_score": best_value,
                "optimization_metric": config["optimization_metric"],
                "trials_completed": len(study.trials),
                "optimization_time_seconds": optimization_time,
                "study_name": study_name,
                "improvement_over_default": await self._calculate_improvement(
                    model_name, best_params, start_date, end_date
                )
            }
            
        except Exception as e:
            logger.error(f"Hyperparameter optimization failed for {model_name}: {e}")
            
            # Record failure metrics
            self.metrics_service.record_pipeline_completion(
                f"hyperopt_{model_name}",
                "hyperparameter_optimization",
                "failed",
                errors=[e]
            )
            raise

    def _create_objective_function(
        self, 
        model_name: str, 
        model_type: str,
        start_date: datetime,
        end_date: datetime, 
        config: Dict[str, Any]
    ) -> Callable:
        """Create Optuna objective function for hyperparameter optimization"""
        
        def objective(trial):
            try:
                # Sample hyperparameters
                params = {}
                search_space = self.search_spaces[model_type]
                
                for param_name, param_config in search_space.items():
                    param_type, *param_args = param_config
                    
                    if param_type == "int":
                        params[param_name] = trial.suggest_int(param_name, *param_args)
                    elif param_type == "float":
                        params[param_name] = trial.suggest_float(param_name, *param_args)
                    elif param_type == "categorical":
                        params[param_name] = trial.suggest_categorical(param_name, *param_args)
                
                # Add fixed parameters
                params.update({
                    "objective": self.trainer.model_configs[model_name]["objective"],
                    "metric": config["optimization_metric"],
                    "verbosity": -1,
                    "early_stopping_rounds": config["early_stopping_rounds"],
                    "n_estimators": 1000,  # Will be limited by early stopping
                })
                
                # Perform cross-validation with these parameters
                cv_scores = asyncio.run(self._cross_validate_parameters(
                    model_name, params, start_date, end_date, config["cv_folds"]
                ))
                
                # Return mean CV score
                mean_score = np.mean(cv_scores)
                
                # Report intermediate result for pruning
                trial.report(mean_score, step=0)
                
                # Check if trial should be pruned
                if trial.should_prune():
                    raise optuna.exceptions.TrialPruned()
                
                return mean_score
                
            except Exception as e:
                logger.error(f"Trial {trial.number} failed: {e}")
                return 0.0  # Return poor score for failed trials
        
        return objective

    async def _cross_validate_parameters(
        self,
        model_name: str,
        params: Dict[str, Any],
        start_date: datetime,
        end_date: datetime,
        cv_folds: int
    ) -> List[float]:
        """Perform cross-validation with given parameters"""
        
        try:
            # Use trainer's cross-validation method
            cv_results = await self.trainer.cross_validate_model(
                model_name=model_name,
                start_date=start_date,
                end_date=end_date,
                hyperparameters=params,
                cv_folds=cv_folds
            )
            
            # Extract scores from CV results
            scores = [fold_result["test_score"] for fold_result in cv_results["fold_results"]]
            return scores
            
        except Exception as e:
            logger.error(f"Cross-validation failed: {e}")
            return [0.0] * cv_folds

    async def _calculate_improvement(
        self,
        model_name: str,
        optimized_params: Dict[str, Any],
        start_date: datetime,
        end_date: datetime
    ) -> float:
        """Calculate improvement over default parameters"""
        
        try:
            # Get default parameters
            default_config = self.trainer.model_configs[model_name]
            default_params = default_config.get("hyperparameters", {})
            
            # Train with default parameters
            default_scores = await self._cross_validate_parameters(
                model_name, default_params, start_date, end_date, 3
            )
            default_score = np.mean(default_scores)
            
            # Train with optimized parameters
            optimized_scores = await self._cross_validate_parameters(
                model_name, optimized_params, start_date, end_date, 3
            )
            optimized_score = np.mean(optimized_scores)
            
            # Calculate relative improvement
            improvement = (optimized_score - default_score) / default_score
            return improvement
            
        except Exception as e:
            logger.error(f"Failed to calculate improvement: {e}")
            return 0.0

    async def optimize_all_models(
        self,
        start_date: datetime,
        end_date: datetime,
        parallel_execution: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """Optimize hyperparameters for all active models"""
        
        try:
            logger.info("Starting hyperparameter optimization for all models")
            
            model_names = list(self.trainer.model_configs.keys())
            optimization_results = {}
            
            if parallel_execution and len(model_names) > 1:
                # Run optimizations in parallel
                tasks = [
                    self.optimize_hyperparameters(model_name, start_date, end_date)
                    for model_name in model_names
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for model_name, result in zip(model_names, results):
                    if isinstance(result, Exception):
                        logger.error(f"Optimization failed for {model_name}: {result}")
                        optimization_results[model_name] = {"status": "failed", "error": str(result)}
                    else:
                        optimization_results[model_name] = result
            else:
                # Run optimizations sequentially
                for model_name in model_names:
                    try:
                        result = await self.optimize_hyperparameters(model_name, start_date, end_date)
                        optimization_results[model_name] = result
                    except Exception as e:
                        logger.error(f"Optimization failed for {model_name}: {e}")
                        optimization_results[model_name] = {"status": "failed", "error": str(e)}
            
            logger.info(f"Hyperparameter optimization completed for {len(optimization_results)} models")
            return optimization_results
            
        except Exception as e:
            logger.error(f"Batch hyperparameter optimization failed: {e}")
            raise

    def get_best_parameters(self, model_name: Optional[str] = None) -> Dict[str, Any]:
        """Get best parameters for a model or all models"""
        if model_name:
            return self.best_parameters.get(model_name, {})
        return self.best_parameters

    def get_optimization_history(self, model_name: Optional[str] = None) -> Dict[str, Any]:
        """Get optimization history for a model or all models"""
        if model_name:
            return self.optimization_history.get(model_name, {})
        return self.optimization_history

    async def schedule_optimization(
        self,
        schedule_type: str = "weekly",
        schedule_day: int = 0,  # 0 = Sunday
        schedule_hour: int = 2,
        training_window_days: int = 90
    ) -> Dict[str, Any]:
        """Schedule automated hyperparameter optimization"""
        
        try:
            logger.info(f"Scheduling hyperparameter optimization: {schedule_type} on day {schedule_day} at {schedule_hour}:00")
            
            schedule_config = {
                "schedule_type": schedule_type,
                "schedule_day": schedule_day,
                "schedule_hour": schedule_hour,
                "training_window_days": training_window_days,
                "created_at": datetime.now(),
                "status": "scheduled"
            }
            
            # Calculate next run time
            next_run = self._calculate_next_optimization_time(schedule_type, schedule_day, schedule_hour)
            
            return {
                "status": "scheduled",
                "schedule_config": schedule_config,
                "next_run": next_run,
                "optimization_enabled": True
            }
            
        except Exception as e:
            logger.error(f"Failed to schedule hyperparameter optimization: {e}")
            raise

    def _calculate_next_optimization_time(
        self, schedule_type: str, schedule_day: int, schedule_hour: int
    ) -> datetime:
        """Calculate next scheduled optimization time"""
        
        now = datetime.now()
        
        if schedule_type == "weekly":
            days_until_target = (schedule_day - now.weekday()) % 7
            if days_until_target == 0 and now.hour >= schedule_hour:
                days_until_target = 7
            
            next_run = now + timedelta(days=days_until_target)
            next_run = next_run.replace(hour=schedule_hour, minute=0, second=0, microsecond=0)
        else:
            raise ValueError(f"Unsupported schedule type: {schedule_type}")
        
        return next_run


# Global hyperparameter optimizer instance
_hyperparameter_optimizer: Optional[HyperparameterOptimizer] = None


def get_hyperparameter_optimizer() -> HyperparameterOptimizer:
    """Get or create the global hyperparameter optimizer instance"""
    global _hyperparameter_optimizer
    if _hyperparameter_optimizer is None:
        _hyperparameter_optimizer = HyperparameterOptimizer()
    return _hyperparameter_optimizer