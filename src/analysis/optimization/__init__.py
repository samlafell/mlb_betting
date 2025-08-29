"""
Hyperparameter Optimization Framework

This module provides automated hyperparameter optimization for MLB betting strategies
to maximize profitability through systematic parameter tuning.

Key components:
- OptimizationEngine: Core optimization algorithms (Grid Search, Random Search, Bayesian)
- ParameterSpace: Strategy-specific parameter definitions and constraints
- OptimizationJob: Job management and progress tracking
- ValidationEngine: Cross-validation and performance validation
- ResultsAnalyzer: Performance comparison and analysis

Integration with existing systems:
- Uses strategy processors from src/analysis/processors/
- Leverages backtesting engine for validation
- Integrates with CLI for user interface
- Stores results in database for persistence

The framework follows the principle of maximizing ROI while preventing overfitting
through proper cross-validation and statistical validation.
"""

from .engine import OptimizationEngine
from .parameter_space import ParameterSpace, ParameterConfig, ParameterType
from .job import OptimizationJob, OptimizationResult, OptimizationAlgorithm, create_optimization_job
from .strategies import StrategyParameterRegistry
from .validation import CrossValidator, PerformanceValidator, ValidationConfig
from .analysis import ResultsAnalyzer

__all__ = [
    "OptimizationEngine",
    "ParameterSpace", 
    "ParameterConfig",
    "ParameterType",
    "OptimizationJob",
    "OptimizationResult",
    "OptimizationAlgorithm",
    "create_optimization_job",
    "StrategyParameterRegistry",
    "CrossValidator",
    "PerformanceValidator",
    "ValidationConfig",
    "ResultsAnalyzer"
]