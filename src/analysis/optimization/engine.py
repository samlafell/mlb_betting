"""
Hyperparameter Optimization Engine

Core optimization algorithms for maximizing betting strategy ROI through
systematic parameter tuning. Supports multiple algorithms with intelligent
resource management and early stopping.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import json
import time

from src.core.logging import LogComponent, get_logger
from src.core.datetime_utils import EST
from src.core.exceptions import OptimizationError
from src.analysis.backtesting.engine import (
    RecommendationBasedBacktestingEngine,
    RecommendationBacktestConfig
)
from src.data.database import UnifiedRepository

from .parameter_space import ParameterSpace
from .job import (
    OptimizationJob,
    OptimizationResult,
    OptimizationAlgorithm,
    OptimizationConfig,
    create_optimization_job
)


class OptimizationAlgorithmBase(ABC):
    """Base class for optimization algorithms"""
    
    def __init__(self, parameter_space: ParameterSpace, config: OptimizationConfig):
        self.parameter_space = parameter_space
        self.config = config
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
        
        # Track evaluated parameters to avoid duplicates
        self.evaluated_parameters: List[Dict[str, Any]] = []
        self.evaluation_count = 0
    
    @abstractmethod
    async def suggest_parameters(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        """
        Suggest next parameter combinations to evaluate.
        
        Args:
            job: Current optimization job with history
            
        Returns:
            List of parameter dictionaries to evaluate
        """
        pass
    
    def _is_duplicate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Check if parameters have already been evaluated"""
        for evaluated in self.evaluated_parameters:
            if self._parameters_equal(parameters, evaluated):
                return True
        return False
    
    def _parameters_equal(self, params1: Dict[str, Any], params2: Dict[str, Any], tolerance: float = 1e-6) -> bool:
        """Check if two parameter sets are equal (with tolerance for floats)"""
        if set(params1.keys()) != set(params2.keys()):
            return False
        
        for key in params1.keys():
            val1, val2 = params1[key], params2[key]
            if isinstance(val1, float) and isinstance(val2, float):
                if abs(val1 - val2) > tolerance:
                    return False
            elif val1 != val2:
                return False
        
        return True
    
    def _add_evaluated_parameters(self, parameters: Dict[str, Any]) -> None:
        """Add parameters to evaluated set"""
        self.evaluated_parameters.append(parameters.copy())
        self.evaluation_count += 1


class GridSearchOptimizer(OptimizationAlgorithmBase):
    """Grid search optimization algorithm"""
    
    def __init__(self, parameter_space: ParameterSpace, config: OptimizationConfig):
        super().__init__(parameter_space, config)
        self.grid_points = config.__dict__.get("grid_points", 5)
        self.grid_combinations: Optional[List[Dict[str, Any]]] = None
        self.current_index = 0
    
    async def suggest_parameters(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        """Suggest next parameters using grid search"""
        if self.grid_combinations is None:
            self.grid_combinations = self.parameter_space.create_grid_space(self.grid_points)
            self.logger.info(f"Created grid with {len(self.grid_combinations)} combinations")
        
        # Return next batch of parameters
        batch_size = min(self.config.n_parallel_jobs, len(self.grid_combinations) - self.current_index)
        batch = []
        
        for _ in range(batch_size):
            if self.current_index < len(self.grid_combinations):
                params = self.grid_combinations[self.current_index]
                batch.append(params)
                self.current_index += 1
        
        return batch


class RandomSearchOptimizer(OptimizationAlgorithmBase):
    """Random search optimization algorithm"""
    
    async def suggest_parameters(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        """Suggest next parameters using random search"""
        batch_size = min(
            self.config.n_parallel_jobs, 
            self.config.max_evaluations - self.evaluation_count
        )
        
        batch = []
        max_attempts = batch_size * 10  # Avoid infinite loop
        attempts = 0
        
        while len(batch) < batch_size and attempts < max_attempts:
            parameters = self.parameter_space.sample_parameters(1)[0]
            
            # Avoid duplicates
            if not self._is_duplicate_parameters(parameters):
                batch.append(parameters)
            
            attempts += 1
        
        return batch


class BayesianOptimizer(OptimizationAlgorithmBase):
    """Bayesian optimization using Gaussian Process"""
    
    def __init__(self, parameter_space: ParameterSpace, config: OptimizationConfig):
        super().__init__(parameter_space, config)
        
        # Import optional dependency
        try:
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import Matern
            from sklearn.preprocessing import StandardScaler
            self.gp_available = True
            self.gp_regressor = None
            self.scaler = StandardScaler()
        except ImportError:
            self.logger.warning("scikit-learn not available, falling back to random search")
            self.gp_available = False
        
        # Acquisition function parameters
        self.exploration_weight = 2.0  # Higher = more exploration
        self.initial_random_samples = max(5, self.config.n_parallel_jobs * 2)
    
    async def suggest_parameters(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        """Suggest next parameters using Bayesian optimization"""
        
        if not self.gp_available or len(job.all_results) < self.initial_random_samples:
            # Use random search for initial samples or if GP not available
            return await self._random_suggest(job)
        
        # Use Gaussian Process for informed suggestions
        return await self._bayesian_suggest(job)
    
    async def _random_suggest(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        """Use random search for initial exploration"""
        batch_size = min(
            self.config.n_parallel_jobs,
            self.config.max_evaluations - len(job.all_results)
        )
        
        batch = []
        for _ in range(batch_size):
            params = self.parameter_space.sample_parameters(1)[0]
            if not self._is_duplicate_parameters(params):
                batch.append(params)
        
        return batch
    
    async def _bayesian_suggest(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        """Use Gaussian Process to suggest parameters"""
        try:
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import Matern
            
            # Prepare training data
            X_train, y_train = self._prepare_gp_data(job.all_results)
            
            if len(X_train) == 0:
                return await self._random_suggest(job)
            
            # Train Gaussian Process
            kernel = Matern(length_scale=1.0, nu=2.5)
            self.gp_regressor = GaussianProcessRegressor(
                kernel=kernel,
                alpha=1e-6,
                normalize_y=True,
                n_restarts_optimizer=5,
                random_state=42
            )
            
            X_scaled = self.scaler.fit_transform(X_train)
            self.gp_regressor.fit(X_scaled, y_train)
            
            # Generate candidate parameters and select best using acquisition function
            candidates = self._generate_candidates(1000)
            best_candidates = self._select_by_acquisition(candidates, job)
            
            batch_size = min(self.config.n_parallel_jobs, len(best_candidates))
            return best_candidates[:batch_size]
            
        except Exception as e:
            self.logger.warning(f"Bayesian optimization failed, using random search: {e}")
            return await self._random_suggest(job)
    
    def _prepare_gp_data(self, results: List[OptimizationResult]) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare data for Gaussian Process training"""
        valid_results = [r for r in results if r.error_message is None]
        
        if not valid_results:
            return np.array([]), np.array([])
        
        # Extract parameter vectors
        param_names = self.parameter_space.get_parameter_names()
        X = []
        y = []
        
        for result in valid_results:
            param_vector = []
            for param_name in param_names:
                value = result.parameters.get(param_name, 0)
                
                # Normalize categorical parameters
                param_config = self.parameter_space.get_parameter_config(param_name)
                if param_config.parameter_type.value == "categorical":
                    if param_config.choices and value in param_config.choices:
                        value = param_config.choices.index(value)
                    else:
                        value = 0
                
                param_vector.append(float(value))
            
            X.append(param_vector)
            y.append(result.objective_value)
        
        return np.array(X), np.array(y)
    
    def _generate_candidates(self, n_candidates: int) -> List[Dict[str, Any]]:
        """Generate candidate parameter combinations"""
        candidates = []
        for _ in range(n_candidates):
            candidate = self.parameter_space.sample_parameters(1)[0]
            candidates.append(candidate)
        return candidates
    
    def _select_by_acquisition(self, candidates: List[Dict[str, Any]], job: OptimizationJob) -> List[Dict[str, Any]]:
        """Select candidates using Upper Confidence Bound acquisition function"""
        if self.gp_regressor is None:
            return candidates[:self.config.n_parallel_jobs]
        
        candidate_scores = []
        param_names = self.parameter_space.get_parameter_names()
        
        for candidate in candidates:
            # Convert to parameter vector
            param_vector = []
            for param_name in param_names:
                value = candidate.get(param_name, 0)
                
                # Handle categorical parameters
                param_config = self.parameter_space.get_parameter_config(param_name)
                if param_config.parameter_type.value == "categorical":
                    if param_config.choices and value in param_config.choices:
                        value = param_config.choices.index(value)
                    else:
                        value = 0
                
                param_vector.append(float(value))
            
            # Predict with uncertainty
            X_candidate = self.scaler.transform([param_vector])
            mean, std = self.gp_regressor.predict(X_candidate, return_std=True)
            
            # Upper Confidence Bound acquisition function
            acquisition_score = mean[0] + self.exploration_weight * std[0]
            
            # Penalize duplicates
            if self._is_duplicate_parameters(candidate):
                acquisition_score -= 10.0
            
            candidate_scores.append((candidate, acquisition_score))
        
        # Sort by acquisition score (higher is better)
        candidate_scores.sort(key=lambda x: x[1], reverse=True)
        
        return [candidate for candidate, _ in candidate_scores[:self.config.n_parallel_jobs * 2]]


class OptimizationEngine:
    """
    Main hyperparameter optimization engine.
    
    Manages optimization jobs, coordinates algorithm execution,
    and integrates with the backtesting engine for performance validation.
    """
    
    def __init__(self, repository: UnifiedRepository, config: Dict[str, Any]):
        """
        Initialize optimization engine.
        
        Args:
            repository: Database repository for strategy data
            config: Engine configuration
        """
        self.repository = repository
        self.config = config
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
        
        # Initialize backtesting engine
        backtest_config = config.get("backtesting", {})
        self.backtesting_engine = RecommendationBasedBacktestingEngine(
            repository, backtest_config
        )
        
        # Active jobs tracking
        self.active_jobs: Dict[str, OptimizationJob] = {}
        
        # Thread pool for parallel evaluations
        self.executor = ThreadPoolExecutor(
            max_workers=config.get("max_workers", 4),
            thread_name_prefix="optimization"
        )
        
        self.logger.info("Initialized OptimizationEngine")
    
    async def optimize_strategy(
        self,
        strategy_name: str,
        parameter_space: ParameterSpace,
        strategy_processors: List[Any],
        validation_start_date: datetime,
        validation_end_date: datetime,
        algorithm: OptimizationAlgorithm = OptimizationAlgorithm.BAYESIAN_OPTIMIZATION,
        max_evaluations: int = 100,
        **kwargs
    ) -> OptimizationJob:
        """
        Start hyperparameter optimization for a strategy.
        
        Args:
            strategy_name: Name of the strategy to optimize
            parameter_space: Parameter space for optimization
            strategy_processors: List of strategy processors to optimize
            validation_start_date: Start date for backtesting validation
            validation_end_date: End date for backtesting validation
            algorithm: Optimization algorithm to use
            max_evaluations: Maximum parameter evaluations
            **kwargs: Additional optimization configuration
            
        Returns:
            OptimizationJob for tracking progress and results
        """
        
        # Create optimization job
        job = create_optimization_job(
            strategy_name=strategy_name,
            parameter_space=parameter_space,
            algorithm=algorithm,
            max_evaluations=max_evaluations,
            validation_start_date=validation_start_date,
            validation_end_date=validation_end_date,
            **kwargs
        )
        
        # Store processors for evaluation
        job.strategy_processors = strategy_processors
        
        self.active_jobs[job.job_id] = job
        
        # Start optimization asynchronously
        asyncio.create_task(self._run_optimization(job))
        
        return job
    
    async def _run_optimization(self, job: OptimizationJob) -> None:
        """Run the optimization job"""
        job.start()
        
        try:
            # Create algorithm instance
            algorithm = self._create_algorithm(job.config)
            
            while (job.current_evaluation < job.config.max_evaluations and 
                   not job.should_stop_early() and 
                   not job.is_timeout() and
                   job.status.value == "running"):
                
                # Get next parameters to evaluate
                parameter_batches = await algorithm.suggest_parameters(job)
                
                if not parameter_batches:
                    self.logger.info(f"No more parameters to evaluate for job {job.job_id}")
                    break
                
                # Evaluate parameters in parallel
                evaluation_tasks = []
                for parameters in parameter_batches:
                    if job.current_evaluation >= job.config.max_evaluations:
                        break
                    
                    task = self._evaluate_parameters(job, parameters)
                    evaluation_tasks.append(task)
                
                # Wait for evaluations to complete
                if evaluation_tasks:
                    results = await asyncio.gather(*evaluation_tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            self.logger.error(f"Parameter evaluation failed: {result}")
                        elif result is not None:
                            is_new_best = job.add_result(result)
                            
                            if is_new_best:
                                self.logger.info(
                                    f"New best parameters for {job.config.strategy_name}: "
                                    f"{result.objective_value:.4f} ROI"
                                )
            
            # Determine completion reason
            if job.should_stop_early():
                self.logger.info(f"Job {job.job_id} stopped early due to lack of improvement")
            elif job.is_timeout():
                self.logger.info(f"Job {job.job_id} timed out")
            elif job.current_evaluation >= job.config.max_evaluations:
                self.logger.info(f"Job {job.job_id} completed maximum evaluations")
            
            job.complete()
            
        except Exception as e:
            job.fail(str(e))
            self.logger.error(f"Optimization job {job.job_id} failed: {e}", exc_info=True)
        
        finally:
            # Clean up
            if job.job_id in self.active_jobs:
                del self.active_jobs[job.job_id]
    
    async def _evaluate_parameters(
        self, 
        job: OptimizationJob, 
        parameters: Dict[str, Any]
    ) -> Optional[OptimizationResult]:
        """
        Evaluate a parameter configuration using backtesting.
        
        Args:
            job: Optimization job
            parameters: Parameter configuration to evaluate
            
        Returns:
            OptimizationResult with performance metrics
        """
        evaluation_id = f"eval_{job.job_id}_{job.current_evaluation + 1}_{int(time.time())}"
        start_time = time.time()
        
        try:
            # Validate parameters
            is_valid, errors = job.config.parameter_space.validate_parameters(parameters)
            if not is_valid:
                error_msg = f"Invalid parameters: {errors}"
                return OptimizationResult(
                    job_id=job.job_id,
                    evaluation_id=evaluation_id,
                    parameters=parameters,
                    objective_value=0.0,
                    error_message=error_msg
                )
            
            # Update strategy processors with new parameters
            updated_processors = self._update_processor_parameters(
                job.strategy_processors, parameters
            )
            
            # Create backtest configuration
            backtest_config = RecommendationBacktestConfig(
                backtest_id=f"opt_{evaluation_id}",
                strategy_processors=updated_processors,
                start_date=job.config.validation_start_date,
                end_date=job.config.validation_end_date,
                initial_bankroll=10000,
                bet_sizing_method="fixed",
                fixed_bet_size=100,
                min_confidence_threshold=0.6
            )
            
            # Run backtest
            backtest_result = await self.backtesting_engine.run_recommendation_backtest(
                backtest_config
            )
            
            # Check if we have enough bets for evaluation
            if backtest_result.recommendations_with_outcomes < job.config.min_samples_required:
                error_msg = f"Insufficient samples: {backtest_result.recommendations_with_outcomes} < {job.config.min_samples_required}"
                return OptimizationResult(
                    job_id=job.job_id,
                    evaluation_id=evaluation_id,
                    parameters=parameters,
                    objective_value=0.0,
                    error_message=error_msg
                )
            
            # Calculate objective value
            objective_value = self._calculate_objective_value(backtest_result, job.config)
            
            # Create result
            result = OptimizationResult(
                job_id=job.job_id,
                evaluation_id=evaluation_id,
                parameters=parameters,
                objective_value=objective_value,
                roi_percentage=backtest_result.roi_percentage,
                win_rate=backtest_result.win_rate,
                profit_factor=backtest_result.profit_factor,
                total_bets=backtest_result.recommendations_with_outcomes,
                winning_bets=backtest_result.winning_bets,
                losing_bets=backtest_result.losing_bets,
                total_profit=float(backtest_result.total_profit),
                max_drawdown=backtest_result.max_drawdown_percentage,
                execution_time_seconds=time.time() - start_time,
                validation_period_start=job.config.validation_start_date,
                validation_period_end=job.config.validation_end_date
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Parameter evaluation failed: {e}", exc_info=True)
            return OptimizationResult(
                job_id=job.job_id,
                evaluation_id=evaluation_id,
                parameters=parameters,
                objective_value=0.0,
                error_message=str(e),
                execution_time_seconds=time.time() - start_time
            )
    
    def _create_algorithm(self, config: OptimizationConfig) -> OptimizationAlgorithmBase:
        """Create optimization algorithm instance"""
        if config.algorithm == OptimizationAlgorithm.GRID_SEARCH:
            return GridSearchOptimizer(config.parameter_space, config)
        elif config.algorithm == OptimizationAlgorithm.RANDOM_SEARCH:
            return RandomSearchOptimizer(config.parameter_space, config)
        elif config.algorithm == OptimizationAlgorithm.BAYESIAN_OPTIMIZATION:
            return BayesianOptimizer(config.parameter_space, config)
        else:
            raise OptimizationError(f"Unsupported algorithm: {config.algorithm}")
    
    def _update_processor_parameters(
        self, 
        processors: List[Any], 
        parameters: Dict[str, Any]
    ) -> List[Any]:
        """Update strategy processor configurations with new parameters"""
        updated_processors = []
        
        for processor in processors:
            # Create new processor instance with updated config
            processor_config = processor.config.copy()
            
            # Update parameters that match processor's parameter space
            for param_name, param_value in parameters.items():
                if param_name in processor_config:
                    processor_config[param_name] = param_value
            
            # Create new processor instance
            ProcessorClass = processor.__class__
            updated_processor = ProcessorClass(self.repository, processor_config)
            updated_processors.append(updated_processor)
        
        return updated_processors
    
    def _calculate_objective_value(
        self, 
        backtest_result: Any, 
        config: OptimizationConfig
    ) -> float:
        """Calculate objective value based on configuration"""
        if config.objective_metric == "roi_percentage":
            return backtest_result.roi_percentage
        elif config.objective_metric == "win_rate":
            return backtest_result.win_rate * 100
        elif config.objective_metric == "profit_factor":
            return backtest_result.profit_factor
        elif config.objective_metric == "sharpe_ratio":
            # Simplified Sharpe ratio calculation
            if backtest_result.max_drawdown_percentage > 0:
                return backtest_result.roi_percentage / backtest_result.max_drawdown_percentage
            else:
                return backtest_result.roi_percentage
        else:
            return backtest_result.roi_percentage
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an optimization job"""
        if job_id in self.active_jobs:
            return self.active_jobs[job_id].get_progress_info()
        return None
    
    def list_active_jobs(self) -> List[Dict[str, Any]]:
        """List all active optimization jobs"""
        return [job.get_progress_info() for job in self.active_jobs.values()]
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel an active optimization job"""
        if job_id in self.active_jobs:
            self.active_jobs[job_id].cancel()
            return True
        return False
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        self.executor.shutdown(wait=True)
        return False