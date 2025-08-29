"""
Optimization Job Management

Manages hyperparameter optimization jobs with progress tracking,
result persistence, and performance validation.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
import asyncio
import json
import uuid
from pathlib import Path

from src.core.logging import LogComponent, get_logger
from src.core.datetime_utils import EST
from .parameter_space import ParameterSpace


class OptimizationStatus(str, Enum):
    """Status of an optimization job"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OptimizationAlgorithm(str, Enum):
    """Supported optimization algorithms"""
    GRID_SEARCH = "grid_search"
    RANDOM_SEARCH = "random_search"
    BAYESIAN_OPTIMIZATION = "bayesian_optimization"
    GENETIC_ALGORITHM = "genetic_algorithm"


@dataclass
class OptimizationConfig:
    """Configuration for an optimization job"""
    job_id: str
    strategy_name: str
    algorithm: OptimizationAlgorithm
    parameter_space: ParameterSpace
    
    # Algorithm-specific settings
    max_evaluations: int = 100
    timeout_hours: int = 24
    n_parallel_jobs: int = 2
    
    # Objective function settings
    objective_metric: str = "roi_percentage"  # "roi_percentage", "win_rate", "profit_factor", "sharpe_ratio"
    maximize: bool = True
    min_samples_required: int = 20  # Minimum bets required for evaluation
    
    # Cross-validation settings
    cv_folds: int = 5
    test_split_ratio: float = 0.2
    validation_start_date: Optional[datetime] = None
    validation_end_date: Optional[datetime] = None
    
    # Early stopping
    enable_early_stopping: bool = True
    patience: int = 20  # Stop if no improvement for N evaluations
    min_improvement_threshold: float = 0.01  # Minimum improvement to reset patience
    
    # Resource limits
    max_memory_mb: int = 4096
    max_cpu_percent: int = 80
    
    # Output settings
    save_intermediate_results: bool = True
    results_directory: str = "optimization_results"
    
    created_at: datetime = field(default_factory=lambda: datetime.now(EST))


@dataclass 
class OptimizationResult:
    """Result of a parameter optimization evaluation"""
    job_id: str
    evaluation_id: str
    parameters: Dict[str, Any]
    
    # Performance metrics
    objective_value: float
    roi_percentage: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    total_bets: int = 0
    winning_bets: int = 0
    losing_bets: int = 0
    total_profit: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    
    # Cross-validation results
    cv_scores: List[float] = field(default_factory=list)
    cv_mean: float = 0.0
    cv_std: float = 0.0
    
    # Execution details
    execution_time_seconds: float = 0.0
    evaluation_timestamp: datetime = field(default_factory=lambda: datetime.now(EST))
    
    # Validation details
    validation_period_start: Optional[datetime] = None
    validation_period_end: Optional[datetime] = None
    
    # Error information
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary format"""
        return {
            "job_id": self.job_id,
            "evaluation_id": self.evaluation_id,
            "parameters": self.parameters,
            "objective_value": self.objective_value,
            "roi_percentage": self.roi_percentage,
            "win_rate": self.win_rate,
            "profit_factor": self.profit_factor,
            "total_bets": self.total_bets,
            "winning_bets": self.winning_bets,
            "losing_bets": self.losing_bets,
            "total_profit": self.total_profit,
            "max_drawdown": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "cv_scores": self.cv_scores,
            "cv_mean": self.cv_mean,
            "cv_std": self.cv_std,
            "execution_time_seconds": self.execution_time_seconds,
            "evaluation_timestamp": self.evaluation_timestamp.isoformat(),
            "validation_period_start": self.validation_period_start.isoformat() if self.validation_period_start else None,
            "validation_period_end": self.validation_period_end.isoformat() if self.validation_period_end else None,
            "error_message": self.error_message
        }


class OptimizationJob:
    """
    Manages a hyperparameter optimization job with progress tracking
    and result persistence.
    """
    
    def __init__(self, config: OptimizationConfig):
        """
        Initialize optimization job.
        
        Args:
            config: Job configuration
        """
        self.config = config
        self.job_id = config.job_id
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
        
        # Job state
        self.status = OptimizationStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.current_evaluation = 0
        self.total_evaluations = 0
        
        # Results tracking
        self.all_results: List[OptimizationResult] = []
        self.best_result: Optional[OptimizationResult] = None
        self.best_objective_value: Optional[float] = None
        
        # Early stopping state
        self.evaluations_without_improvement = 0
        self.last_improvement_evaluation = 0
        
        # Progress tracking
        self.progress_callbacks: List[Callable] = []
        
        # Results persistence
        self.results_dir = Path(config.results_directory) / config.job_id
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Initialized optimization job {self.job_id} for {config.strategy_name}")
    
    def add_progress_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Add a progress callback function"""
        self.progress_callbacks.append(callback)
    
    def start(self) -> None:
        """Mark job as started"""
        self.status = OptimizationStatus.RUNNING
        self.start_time = datetime.now(EST)
        self._notify_progress("started")
        self.logger.info(f"Started optimization job {self.job_id}")
    
    def pause(self) -> None:
        """Pause the optimization job"""
        if self.status == OptimizationStatus.RUNNING:
            self.status = OptimizationStatus.PAUSED
            self._notify_progress("paused")
            self.logger.info(f"Paused optimization job {self.job_id}")
    
    def resume(self) -> None:
        """Resume a paused optimization job"""
        if self.status == OptimizationStatus.PAUSED:
            self.status = OptimizationStatus.RUNNING
            self._notify_progress("resumed")
            self.logger.info(f"Resumed optimization job {self.job_id}")
    
    def cancel(self) -> None:
        """Cancel the optimization job"""
        if self.status in [OptimizationStatus.RUNNING, OptimizationStatus.PAUSED]:
            self.status = OptimizationStatus.CANCELLED
            self.end_time = datetime.now(EST)
            self._notify_progress("cancelled")
            self.logger.info(f"Cancelled optimization job {self.job_id}")
    
    def complete(self) -> None:
        """Mark job as completed"""
        self.status = OptimizationStatus.COMPLETED
        self.end_time = datetime.now(EST)
        self._save_final_results()
        self._notify_progress("completed")
        self.logger.info(f"Completed optimization job {self.job_id}")
    
    def fail(self, error_message: str) -> None:
        """Mark job as failed"""
        self.status = OptimizationStatus.FAILED
        self.end_time = datetime.now(EST)
        self._notify_progress("failed", {"error": error_message})
        self.logger.error(f"Optimization job {self.job_id} failed: {error_message}")
    
    def add_result(self, result: OptimizationResult) -> bool:
        """
        Add an optimization result and update job state.
        
        Args:
            result: Optimization result to add
            
        Returns:
            True if this is a new best result, False otherwise
        """
        self.all_results.append(result)
        self.current_evaluation += 1
        
        is_new_best = False
        
        # Check if this is the best result so far
        if result.error_message is None:
            if (self.best_result is None or 
                self._is_better_result(result.objective_value, self.best_objective_value)):
                self.best_result = result
                self.best_objective_value = result.objective_value
                self.evaluations_without_improvement = 0
                self.last_improvement_evaluation = self.current_evaluation
                is_new_best = True
                self.logger.info(f"New best result for job {self.job_id}: {result.objective_value}")
            else:
                self.evaluations_without_improvement += 1
        
        # Save intermediate results if configured
        if self.config.save_intermediate_results:
            self._save_result(result)
        
        # Notify progress
        self._notify_progress("evaluation_completed", {
            "current_evaluation": self.current_evaluation,
            "total_evaluations": self.config.max_evaluations,
            "best_objective_value": self.best_objective_value,
            "is_new_best": is_new_best
        })
        
        return is_new_best
    
    def should_stop_early(self) -> bool:
        """Check if job should stop early due to lack of improvement"""
        if not self.config.enable_early_stopping:
            return False
        
        return self.evaluations_without_improvement >= self.config.patience
    
    def is_timeout(self) -> bool:
        """Check if job has timed out"""
        if self.start_time is None:
            return False
        
        elapsed = datetime.now(EST) - self.start_time
        return elapsed > timedelta(hours=self.config.timeout_hours)
    
    def get_progress_info(self) -> Dict[str, Any]:
        """Get current progress information"""
        elapsed_time = None
        if self.start_time:
            elapsed_time = (datetime.now(EST) - self.start_time).total_seconds()
        
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "strategy_name": self.config.strategy_name,
            "algorithm": self.config.algorithm.value,
            "current_evaluation": self.current_evaluation,
            "max_evaluations": self.config.max_evaluations,
            "progress_percentage": (self.current_evaluation / self.config.max_evaluations) * 100 if self.config.max_evaluations > 0 else 0,
            "best_objective_value": self.best_objective_value,
            "evaluations_without_improvement": self.evaluations_without_improvement,
            "patience": self.config.patience,
            "elapsed_time_seconds": elapsed_time,
            "estimated_time_remaining": self._estimate_time_remaining(),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None
        }
    
    def get_best_parameters(self) -> Optional[Dict[str, Any]]:
        """Get the best parameters found so far"""
        return self.best_result.parameters if self.best_result else None
    
    def get_results_summary(self) -> Dict[str, Any]:
        """Get summary of all results"""
        if not self.all_results:
            return {"total_evaluations": 0, "valid_evaluations": 0}
        
        valid_results = [r for r in self.all_results if r.error_message is None]
        objective_values = [r.objective_value for r in valid_results]
        
        summary = {
            "total_evaluations": len(self.all_results),
            "valid_evaluations": len(valid_results),
            "failed_evaluations": len(self.all_results) - len(valid_results),
            "best_objective_value": self.best_objective_value,
            "mean_objective_value": sum(objective_values) / len(objective_values) if objective_values else 0,
            "std_objective_value": 0,  # Would calculate standard deviation
        }
        
        if self.best_result:
            summary.update({
                "best_roi_percentage": self.best_result.roi_percentage,
                "best_win_rate": self.best_result.win_rate,
                "best_profit_factor": self.best_result.profit_factor,
                "best_total_bets": self.best_result.total_bets,
                "best_parameters": self.best_result.parameters
            })
        
        return summary
    
    def export_results(self, filepath: Optional[str] = None) -> str:
        """
        Export all results to JSON file.
        
        Args:
            filepath: Optional custom file path
            
        Returns:
            Path to exported file
        """
        if filepath is None:
            filepath = str(self.results_dir / f"results_{self.job_id}.json")
        
        export_data = {
            "job_config": {
                "job_id": self.config.job_id,
                "strategy_name": self.config.strategy_name,
                "algorithm": self.config.algorithm.value,
                "max_evaluations": self.config.max_evaluations,
                "objective_metric": self.config.objective_metric,
                "created_at": self.config.created_at.isoformat()
            },
            "job_progress": self.get_progress_info(),
            "results_summary": self.get_results_summary(),
            "all_results": [result.to_dict() for result in self.all_results]
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        self.logger.info(f"Exported results to {filepath}")
        return filepath
    
    def _is_better_result(self, new_value: float, current_best: Optional[float]) -> bool:
        """Check if new value is better than current best"""
        if current_best is None:
            return True
        
        if self.config.maximize:
            return new_value > current_best + self.config.min_improvement_threshold
        else:
            return new_value < current_best - self.config.min_improvement_threshold
    
    def _estimate_time_remaining(self) -> Optional[float]:
        """Estimate remaining time based on current progress"""
        if (self.start_time is None or self.current_evaluation == 0 or 
            self.current_evaluation >= self.config.max_evaluations):
            return None
        
        elapsed = (datetime.now(EST) - self.start_time).total_seconds()
        time_per_evaluation = elapsed / self.current_evaluation
        remaining_evaluations = self.config.max_evaluations - self.current_evaluation
        
        return time_per_evaluation * remaining_evaluations
    
    def _notify_progress(self, event: str, data: Optional[Dict[str, Any]] = None) -> None:
        """Notify progress callbacks of job events"""
        progress_data = {
            "job_id": self.job_id,
            "event": event,
            "timestamp": datetime.now(EST).isoformat(),
            "progress_info": self.get_progress_info()
        }
        
        if data:
            progress_data.update(data)
        
        for callback in self.progress_callbacks:
            try:
                callback(progress_data)
            except Exception as e:
                self.logger.warning(f"Progress callback failed: {e}")
    
    def _save_result(self, result: OptimizationResult) -> None:
        """Save individual result to file"""
        result_file = self.results_dir / f"result_{result.evaluation_id}.json"
        with open(result_file, 'w') as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
    
    def _save_final_results(self) -> None:
        """Save final job results"""
        self.export_results()
        
        # Also save best parameters separately for easy access
        if self.best_result:
            best_params_file = self.results_dir / "best_parameters.json"
            with open(best_params_file, 'w') as f:
                json.dump({
                    "best_parameters": self.best_result.parameters,
                    "best_objective_value": self.best_objective_value,
                    "evaluation_id": self.best_result.evaluation_id,
                    "performance_metrics": {
                        "roi_percentage": self.best_result.roi_percentage,
                        "win_rate": self.best_result.win_rate,
                        "profit_factor": self.best_result.profit_factor,
                        "total_bets": self.best_result.total_bets
                    }
                }, f, indent=2, default=str)


def create_optimization_job(
    strategy_name: str,
    parameter_space: ParameterSpace,
    algorithm: OptimizationAlgorithm = OptimizationAlgorithm.BAYESIAN_OPTIMIZATION,
    max_evaluations: int = 100,
    **kwargs
) -> OptimizationJob:
    """
    Factory function to create an optimization job.
    
    Args:
        strategy_name: Name of the strategy to optimize
        parameter_space: Parameter space for optimization
        algorithm: Optimization algorithm to use
        max_evaluations: Maximum number of parameter evaluations
        **kwargs: Additional configuration options
        
    Returns:
        Configured OptimizationJob
    """
    job_id = f"{strategy_name}_opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    
    config = OptimizationConfig(
        job_id=job_id,
        strategy_name=strategy_name,
        algorithm=algorithm,
        parameter_space=parameter_space,
        max_evaluations=max_evaluations,
        **kwargs
    )
    
    return OptimizationJob(config)