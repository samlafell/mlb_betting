"""
Cross-Validation and Performance Validation for Hyperparameter Optimization

Implements robust validation methods to prevent overfitting and ensure
reliable performance estimation for betting strategies.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
from sklearn.model_selection import KFold, TimeSeriesSplit
import asyncio

from src.core.logging import LogComponent, get_logger
from src.core.datetime_utils import EST
from src.analysis.backtesting.engine import (
    RecommendationBasedBacktestingEngine,
    RecommendationBacktestConfig
)

from .job import OptimizationResult


@dataclass
class ValidationConfig:
    """Configuration for validation methods"""
    
    # Cross-validation settings
    cv_method: str = "time_series"  # "time_series", "k_fold", "walk_forward"
    n_folds: int = 5
    test_split_ratio: float = 0.2
    
    # Time-based validation
    validation_start_date: Optional[datetime] = None
    validation_end_date: Optional[datetime] = None
    min_samples_per_fold: int = 20
    
    # Statistical validation
    confidence_level: float = 0.95
    min_improvement_threshold: float = 0.01  # 1% minimum improvement
    
    # Performance thresholds
    min_roi_threshold: float = 0.0  # Minimum ROI to consider valid
    min_win_rate_threshold: float = 0.45  # Minimum win rate
    max_drawdown_threshold: float = 50.0  # Maximum drawdown percentage


@dataclass
class ValidationResult:
    """Result of parameter validation"""
    
    parameters: Dict[str, Any]
    cv_scores: List[float]
    cv_mean: float
    cv_std: float
    cv_confidence_interval: Tuple[float, float]
    
    # Out-of-sample test result
    test_score: Optional[float] = None
    
    # Validation metrics
    is_statistically_significant: bool = False
    passes_performance_thresholds: bool = False
    overfitting_risk: float = 0.0  # 0-1 score, higher = more risk
    
    # Additional metrics
    consistency_score: float = 0.0  # How consistent are the CV scores
    robustness_score: float = 0.0  # How robust across different periods
    
    validation_timestamp: datetime = None


class CrossValidator:
    """
    Cross-validation for hyperparameter optimization.
    
    Implements time-series aware cross-validation to prevent data leakage
    and provide realistic performance estimates for betting strategies.
    """
    
    def __init__(self, backtesting_engine: RecommendationBasedBacktestingEngine, config: ValidationConfig):
        """
        Initialize cross-validator.
        
        Args:
            backtesting_engine: Backtesting engine for evaluation
            config: Validation configuration
        """
        self.backtesting_engine = backtesting_engine
        self.config = config
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
    
    async def validate_parameters(
        self,
        parameters: Dict[str, Any],
        strategy_processors: List[Any],
        job_id: str
    ) -> ValidationResult:
        """
        Validate parameters using cross-validation.
        
        Args:
            parameters: Parameter configuration to validate
            strategy_processors: Strategy processors to test
            job_id: Job identifier for tracking
            
        Returns:
            ValidationResult with cross-validation metrics
        """
        self.logger.info(f"Starting cross-validation for job {job_id}")
        
        try:
            # Create time folds
            folds = self._create_time_folds()
            
            if len(folds) < 2:
                raise ValueError("Insufficient data for cross-validation")
            
            # Run cross-validation
            cv_scores = await self._run_cross_validation(
                parameters, strategy_processors, folds, job_id
            )
            
            # Calculate statistics
            cv_mean = np.mean(cv_scores)
            cv_std = np.std(cv_scores)
            cv_confidence_interval = self._calculate_confidence_interval(cv_scores)
            
            # Assess validation quality
            is_significant = self._test_statistical_significance(cv_scores)
            passes_thresholds = self._check_performance_thresholds(cv_scores)
            overfitting_risk = self._assess_overfitting_risk(cv_scores)
            consistency_score = self._calculate_consistency_score(cv_scores)
            robustness_score = self._calculate_robustness_score(cv_scores)
            
            # Run out-of-sample test if configured
            test_score = None
            if len(folds) > self.config.n_folds:
                test_fold = folds[-1]  # Use last fold as test set
                test_scores = await self._run_cross_validation(
                    parameters, strategy_processors, [test_fold], f"{job_id}_test"
                )
                test_score = test_scores[0] if test_scores else None
            
            result = ValidationResult(
                parameters=parameters,
                cv_scores=cv_scores,
                cv_mean=cv_mean,
                cv_std=cv_std,
                cv_confidence_interval=cv_confidence_interval,
                test_score=test_score,
                is_statistically_significant=is_significant,
                passes_performance_thresholds=passes_thresholds,
                overfitting_risk=overfitting_risk,
                consistency_score=consistency_score,
                robustness_score=robustness_score,
                validation_timestamp=datetime.now(EST)
            )
            
            self.logger.info(
                f"Cross-validation completed for job {job_id}: "
                f"CV={cv_mean:.3f}±{cv_std:.3f}, significant={is_significant}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Cross-validation failed for job {job_id}: {e}")
            raise
    
    def _create_time_folds(self) -> List[Tuple[datetime, datetime]]:
        """Create time-based folds for cross-validation"""
        if not self.config.validation_start_date or not self.config.validation_end_date:
            raise ValueError("Validation date range not configured")
        
        start_date = self.config.validation_start_date
        end_date = self.config.validation_end_date
        total_days = (end_date - start_date).days
        
        if self.config.cv_method == "time_series":
            # Time series split - each fold uses progressively more historical data
            folds = []
            fold_size_days = total_days // (self.config.n_folds + 1)  # Reserve last fold for testing
            
            for i in range(self.config.n_folds):
                fold_start = start_date
                fold_end = start_date + timedelta(days=fold_size_days * (i + 1))
                folds.append((fold_start, fold_end))
            
            # Add test fold
            test_start = start_date + timedelta(days=fold_size_days * self.config.n_folds)
            test_end = end_date
            if (test_end - test_start).days > fold_size_days // 2:  # Only if significant test period
                folds.append((test_start, test_end))
        
        elif self.config.cv_method == "walk_forward":
            # Walk-forward validation - each fold is a time window that slides forward
            folds = []
            window_days = total_days // self.config.n_folds
            
            for i in range(self.config.n_folds):
                fold_start = start_date + timedelta(days=i * window_days // 2)  # 50% overlap
                fold_end = fold_start + timedelta(days=window_days)
                if fold_end <= end_date:
                    folds.append((fold_start, min(fold_end, end_date)))
        
        else:  # k_fold - divide time period into equal segments
            fold_size_days = total_days // self.config.n_folds
            folds = []
            
            for i in range(self.config.n_folds):
                fold_start = start_date + timedelta(days=i * fold_size_days)
                fold_end = start_date + timedelta(days=(i + 1) * fold_size_days)
                folds.append((fold_start, min(fold_end, end_date)))
        
        self.logger.info(f"Created {len(folds)} time folds for validation")
        return folds
    
    async def _run_cross_validation(
        self,
        parameters: Dict[str, Any],
        strategy_processors: List[Any],
        folds: List[Tuple[datetime, datetime]],
        job_id: str
    ) -> List[float]:
        """Run cross-validation across time folds"""
        
        cv_scores = []
        
        for i, (fold_start, fold_end) in enumerate(folds):
            try:
                # Update processors with parameters
                updated_processors = self._update_processor_parameters(
                    strategy_processors, parameters
                )
                
                # Create backtest config for this fold
                backtest_config = RecommendationBacktestConfig(
                    backtest_id=f"cv_{job_id}_fold_{i}",
                    strategy_processors=updated_processors,
                    start_date=fold_start,
                    end_date=fold_end,
                    initial_bankroll=10000,
                    bet_sizing_method="fixed",
                    fixed_bet_size=100,
                    min_confidence_threshold=0.6
                )
                
                # Run backtest for this fold
                backtest_result = await self.backtesting_engine.run_recommendation_backtest(
                    backtest_config
                )
                
                # Check minimum samples requirement
                if backtest_result.recommendations_with_outcomes >= self.config.min_samples_per_fold:
                    cv_scores.append(backtest_result.roi_percentage)
                else:
                    self.logger.warning(
                        f"Fold {i} has insufficient samples: "
                        f"{backtest_result.recommendations_with_outcomes} < {self.config.min_samples_per_fold}"
                    )
                
            except Exception as e:
                self.logger.error(f"Fold {i} evaluation failed: {e}")
                continue
        
        return cv_scores
    
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
                # Handle prefixed parameters (strategy_name_param_name)
                if param_name in processor_config:
                    processor_config[param_name] = param_value
                elif param_name.startswith(processor.__class__.__name__.lower().replace('processor', '')):
                    # Remove prefix and update
                    clean_param_name = param_name.split('_', 1)[1] if '_' in param_name else param_name
                    if clean_param_name in processor_config:
                        processor_config[clean_param_name] = param_value
            
            # Create new processor instance
            ProcessorClass = processor.__class__
            updated_processor = ProcessorClass(processor.repository, processor_config)
            updated_processors.append(updated_processor)
        
        return updated_processors
    
    def _calculate_confidence_interval(self, scores: List[float]) -> Tuple[float, float]:
        """Calculate confidence interval for CV scores"""
        if len(scores) < 2:
            return (0.0, 0.0)
        
        mean = np.mean(scores)
        std = np.std(scores, ddof=1)  # Sample standard deviation
        n = len(scores)
        
        # t-distribution critical value (approximation for 95% confidence)
        t_critical = 2.0 if n < 30 else 1.96
        margin_of_error = t_critical * (std / np.sqrt(n))
        
        return (mean - margin_of_error, mean + margin_of_error)
    
    def _test_statistical_significance(self, scores: List[float]) -> bool:
        """Test if results are statistically significant"""
        if len(scores) < 3:
            return False
        
        mean = np.mean(scores)
        
        # Simple t-test against minimum threshold
        if mean <= self.config.min_improvement_threshold:
            return False
        
        # Check if confidence interval is above threshold
        ci_lower, ci_upper = self._calculate_confidence_interval(scores)
        return ci_lower > self.config.min_improvement_threshold
    
    def _check_performance_thresholds(self, scores: List[float]) -> bool:
        """Check if scores meet performance thresholds"""
        if not scores:
            return False
        
        mean_score = np.mean(scores)
        
        # Check ROI threshold (assuming scores are ROI percentages)
        return mean_score >= self.config.min_roi_threshold
    
    def _assess_overfitting_risk(self, cv_scores: List[float]) -> float:
        """Assess overfitting risk based on score variance"""
        if len(cv_scores) < 2:
            return 1.0
        
        # High variance indicates potential overfitting
        coefficient_of_variation = np.std(cv_scores) / abs(np.mean(cv_scores)) if np.mean(cv_scores) != 0 else 1.0
        
        # Normalize to 0-1 scale (higher = more risk)
        overfitting_risk = min(1.0, coefficient_of_variation)
        return overfitting_risk
    
    def _calculate_consistency_score(self, cv_scores: List[float]) -> float:
        """Calculate consistency score (1 - coefficient of variation)"""
        if len(cv_scores) < 2:
            return 0.0
        
        coefficient_of_variation = np.std(cv_scores) / abs(np.mean(cv_scores)) if np.mean(cv_scores) != 0 else 1.0
        consistency_score = max(0.0, 1.0 - coefficient_of_variation)
        return consistency_score
    
    def _calculate_robustness_score(self, cv_scores: List[float]) -> float:
        """Calculate robustness score based on minimum performance across folds"""
        if not cv_scores:
            return 0.0
        
        min_score = min(cv_scores)
        mean_score = np.mean(cv_scores)
        
        # Robustness is ratio of worst case to average case
        if mean_score > 0:
            robustness_score = min_score / mean_score
        else:
            robustness_score = 0.0
        
        return max(0.0, robustness_score)


class PerformanceValidator:
    """
    Advanced performance validation with statistical tests and benchmark comparisons.
    """
    
    def __init__(self, config: ValidationConfig):
        """
        Initialize performance validator.
        
        Args:
            config: Validation configuration
        """
        self.config = config
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
    
    def validate_optimization_results(
        self, 
        results: List[OptimizationResult], 
        baseline_performance: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Validate optimization results with statistical tests.
        
        Args:
            results: List of optimization results
            baseline_performance: Optional baseline performance to compare against
            
        Returns:
            Validation summary with statistical tests
        """
        if not results:
            return {"valid": False, "reason": "No results to validate"}
        
        valid_results = [r for r in results if r.error_message is None]
        
        if not valid_results:
            return {"valid": False, "reason": "No valid results"}
        
        objective_values = [r.objective_value for r in valid_results]
        
        validation_summary = {
            "total_results": len(results),
            "valid_results": len(valid_results),
            "success_rate": len(valid_results) / len(results),
            "mean_performance": np.mean(objective_values),
            "std_performance": np.std(objective_values),
            "best_performance": max(objective_values),
            "worst_performance": min(objective_values),
            "performance_range": max(objective_values) - min(objective_values)
        }
        
        # Statistical significance test
        if baseline_performance is not None:
            validation_summary["baseline_comparison"] = self._compare_to_baseline(
                objective_values, baseline_performance
            )
        
        # Improvement trend analysis
        validation_summary["improvement_trend"] = self._analyze_improvement_trend(
            valid_results
        )
        
        # Overall validation decision
        validation_summary["valid"] = (
            validation_summary["success_rate"] >= 0.8 and
            validation_summary["mean_performance"] > self.config.min_roi_threshold and
            len(valid_results) >= 10
        )
        
        return validation_summary
    
    def _compare_to_baseline(
        self, 
        objective_values: List[float], 
        baseline: float
    ) -> Dict[str, Any]:
        """Compare optimization results to baseline performance"""
        
        improvements = [val - baseline for val in objective_values]
        positive_improvements = [imp for imp in improvements if imp > 0]
        
        comparison = {
            "baseline_value": baseline,
            "mean_improvement": np.mean(improvements),
            "improvement_rate": len(positive_improvements) / len(improvements),
            "significant_improvement": False,
            "effect_size": 0.0
        }
        
        # Simple statistical test
        if len(improvements) >= 10:
            mean_improvement = np.mean(improvements)
            std_improvement = np.std(improvements)
            
            # Effect size (Cohen's d)
            if std_improvement > 0:
                effect_size = mean_improvement / std_improvement
                comparison["effect_size"] = effect_size
                
                # Consider significant if large effect size and positive improvement
                comparison["significant_improvement"] = (
                    effect_size > 0.5 and 
                    mean_improvement > self.config.min_improvement_threshold
                )
        
        return comparison
    
    def _analyze_improvement_trend(self, results: List[OptimizationResult]) -> Dict[str, Any]:
        """Analyze improvement trend over optimization iterations"""
        
        if len(results) < 5:
            return {"trend": "insufficient_data"}
        
        # Sort by evaluation timestamp
        sorted_results = sorted(results, key=lambda r: r.evaluation_timestamp)
        objective_values = [r.objective_value for r in sorted_results]
        
        # Calculate rolling best values
        rolling_best = []
        current_best = objective_values[0]
        
        for value in objective_values:
            if value > current_best:
                current_best = value
            rolling_best.append(current_best)
        
        # Analyze trend
        improvements = []
        for i in range(1, len(rolling_best)):
            if rolling_best[i] > rolling_best[i-1]:
                improvements.append(i)
        
        trend_analysis = {
            "total_improvements": len(improvements),
            "improvement_rate": len(improvements) / len(objective_values),
            "final_improvement": rolling_best[-1] - rolling_best[0],
            "stagnation_period": len(rolling_best) - (improvements[-1] if improvements else 0)
        }
        
        # Classify trend
        if trend_analysis["improvement_rate"] > 0.2:
            trend_analysis["trend"] = "improving"
        elif trend_analysis["stagnation_period"] > len(rolling_best) * 0.5:
            trend_analysis["trend"] = "stagnating"
        else:
            trend_analysis["trend"] = "stable"
        
        return trend_analysis