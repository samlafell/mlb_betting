"""
Comprehensive tests for the hyperparameter optimization framework.

Tests all core components including parameter spaces, optimization algorithms,
validation, and CLI integration.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import tempfile
import json
from pathlib import Path

from src.analysis.optimization import (
    ParameterSpace,
    ParameterConfig,
    ParameterType,
    StrategyParameterRegistry,
    OptimizationEngine,
    OptimizationJob,
    OptimizationResult,
    OptimizationAlgorithm,
    CrossValidator,
    ValidationConfig,
    ResultsAnalyzer,
    create_optimization_job
)
from src.core.datetime_utils import EST


class TestParameterSpace:
    """Test parameter space functionality"""
    
    def test_parameter_config_validation(self):
        """Test parameter configuration validation"""
        
        # Valid continuous parameter
        param = ParameterConfig(
            name="test_param",
            parameter_type=ParameterType.CONTINUOUS,
            bounds=(0.0, 1.0),
            default_value=0.5
        )
        assert param.validate_value(0.7) is True
        assert param.validate_value(-0.1) is False
        assert param.validate_value(1.5) is False
        
        # Valid discrete parameter
        param = ParameterConfig(
            name="test_discrete",
            parameter_type=ParameterType.DISCRETE,
            choices=[1, 2, 3, 4, 5],
            default_value=3
        )
        assert param.validate_value(3) is True
        assert param.validate_value(6) is False
        
        # Invalid configuration
        with pytest.raises(ValueError):
            ParameterConfig(
                name="invalid",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=None  # Missing bounds for continuous
            )
    
    def test_parameter_space_creation(self):
        """Test parameter space creation and validation"""
        
        params = [
            ParameterConfig("param1", ParameterType.CONTINUOUS, bounds=(0.0, 10.0), default_value=5.0),
            ParameterConfig("param2", ParameterType.DISCRETE, choices=[1, 2, 3], default_value=2),
            ParameterConfig("param3", ParameterType.BOOLEAN, default_value=True)
        ]
        
        space = ParameterSpace("TestStrategy", params)
        
        assert len(space.get_parameter_names()) == 3
        assert "param1" in space.get_parameter_names()
        
        # Test sampling
        samples = space.sample_parameters(5)
        assert len(samples) == 5
        
        for sample in samples:
            is_valid, errors = space.validate_parameters(sample)
            assert is_valid, f"Invalid sample: {sample}, errors: {errors}"
    
    def test_grid_space_creation(self):
        """Test grid space creation"""
        
        params = [
            ParameterConfig("param1", ParameterType.CONTINUOUS, bounds=(0.0, 1.0)),
            ParameterConfig("param2", ParameterType.DISCRETE, choices=[1, 2, 3])
        ]
        
        space = ParameterSpace("TestStrategy", params)
        grid = space.create_grid_space(grid_points=3)
        
        # Should have 3 * 3 = 9 combinations
        assert len(grid) == 9
        
        # All combinations should be valid
        for combination in grid:
            is_valid, _ = space.validate_parameters(combination)
            assert is_valid


class TestStrategyParameterRegistry:
    """Test strategy parameter registry"""
    
    def test_registry_initialization(self):
        """Test registry initializes with all strategies"""
        
        registry = StrategyParameterRegistry()
        
        strategies = registry.list_strategies()
        assert "sharp_action" in strategies
        assert "line_movement" in strategies
        assert "consensus" in strategies
        assert "late_flip" in strategies
    
    def test_parameter_space_retrieval(self):
        """Test parameter space retrieval"""
        
        registry = StrategyParameterRegistry()
        
        # Test valid strategy
        space = registry.get_parameter_space("sharp_action")
        assert space.strategy_name == "SharpActionProcessor"
        assert len(space.get_parameter_names()) > 0
        
        # Test invalid strategy
        with pytest.raises(KeyError):
            registry.get_parameter_space("nonexistent_strategy")
    
    def test_high_impact_parameters(self):
        """Test high-impact parameter identification"""
        
        registry = StrategyParameterRegistry()
        
        high_impact = registry.get_high_impact_parameters("sharp_action")
        assert len(high_impact) > 0
        assert "min_differential_threshold" in high_impact
        
        # Test focused parameter space
        focused_space = registry.create_focused_parameter_space("sharp_action")
        assert len(focused_space.get_parameter_names()) <= len(high_impact)
    
    def test_combined_parameter_space(self):
        """Test combined parameter space creation"""
        
        registry = StrategyParameterRegistry()
        
        combined = registry.create_combined_parameter_space(["sharp_action", "line_movement"])
        param_names = combined.get_parameter_names()
        
        # Should have prefixed parameters from both strategies
        assert any(name.startswith("sharp_action_") for name in param_names)
        assert any(name.startswith("line_movement_") for name in param_names)


class TestOptimizationJob:
    """Test optimization job management"""
    
    def test_job_creation(self):
        """Test optimization job creation"""
        
        registry = StrategyParameterRegistry()
        parameter_space = registry.get_parameter_space("sharp_action")
        
        job = create_optimization_job(
            strategy_name="sharp_action",
            parameter_space=parameter_space,
            max_evaluations=10
        )
        
        assert job.config.strategy_name == "sharp_action"
        assert job.config.max_evaluations == 10
        assert job.status.value == "pending"
    
    def test_job_lifecycle(self):
        """Test job lifecycle management"""
        
        registry = StrategyParameterRegistry()
        parameter_space = registry.get_parameter_space("sharp_action")
        
        job = create_optimization_job(
            strategy_name="sharp_action",
            parameter_space=parameter_space,
            max_evaluations=5
        )
        
        # Start job
        job.start()
        assert job.status.value == "running"
        assert job.start_time is not None
        
        # Add results
        for i in range(3):
            result = OptimizationResult(
                job_id=job.job_id,
                evaluation_id=f"eval_{i}",
                parameters={"param1": i * 0.1},
                objective_value=i * 0.05,  # Increasing values
                roi_percentage=i * 5.0,
                total_bets=20
            )
            is_new_best = job.add_result(result)
            assert is_new_best  # Each should be better than the last
        
        # Complete job
        job.complete()
        assert job.status.value == "completed"
        assert job.end_time is not None
        
        # Check results
        summary = job.get_results_summary()
        assert summary["total_evaluations"] == 3
        assert summary["best_performance"] == 0.1  # Last result was best
    
    def test_early_stopping(self):
        """Test early stopping functionality"""
        
        registry = StrategyParameterRegistry()
        parameter_space = registry.get_parameter_space("sharp_action")
        
        job = create_optimization_job(
            strategy_name="sharp_action",
            parameter_space=parameter_space,
            max_evaluations=100,
            patience=3,
            min_improvement_threshold=0.01
        )
        
        job.start()
        
        # Add result that sets baseline
        result1 = OptimizationResult(
            job_id=job.job_id,
            evaluation_id="eval_1",
            parameters={"param1": 0.5},
            objective_value=0.1,
            roi_percentage=10.0,
            total_bets=20
        )
        job.add_result(result1)
        
        # Add results with no significant improvement
        for i in range(4):
            result = OptimizationResult(
                job_id=job.job_id,
                evaluation_id=f"eval_{i+2}",
                parameters={"param1": 0.5 + i * 0.001},
                objective_value=0.1 + i * 0.001,  # Tiny improvements
                roi_percentage=10.0 + i * 0.1,
                total_bets=20
            )
            job.add_result(result)
        
        # Should trigger early stopping
        assert job.should_stop_early()


class MockBacktestingEngine:
    """Mock backtesting engine for testing"""
    
    async def run_recommendation_backtest(self, config):
        """Mock backtest that returns realistic results"""
        
        # Simulate realistic performance based on config
        mock_result = Mock()
        mock_result.recommendations_with_outcomes = 25
        mock_result.roi_percentage = 8.5
        mock_result.win_rate = 0.65
        mock_result.profit_factor = 1.4
        mock_result.winning_bets = 16
        mock_result.losing_bets = 9
        mock_result.total_profit = 850.0
        mock_result.max_drawdown_percentage = 15.2
        
        return mock_result


class TestOptimizationEngine:
    """Test optimization engine"""
    
    @pytest.mark.asyncio
    async def test_engine_initialization(self):
        """Test optimization engine initialization"""
        
        mock_repository = Mock()
        config = {"max_workers": 2, "backtesting": {}}
        
        with patch('src.analysis.optimization.engine.RecommendationBasedBacktestingEngine'):
            engine = OptimizationEngine(mock_repository, config)
            assert engine.repository == mock_repository
            assert engine.config == config
    
    @pytest.mark.asyncio
    async def test_parameter_evaluation(self):
        """Test parameter evaluation with mocked backtesting"""
        
        mock_repository = Mock()
        config = {"max_workers": 2, "backtesting": {}}
        
        with patch('src.analysis.optimization.engine.RecommendationBasedBacktestingEngine'):
            engine = OptimizationEngine(mock_repository, config)
            engine.backtesting_engine = MockBacktestingEngine()
            
            # Create mock job
            registry = StrategyParameterRegistry()
            parameter_space = registry.get_parameter_space("sharp_action")
            
            job = create_optimization_job(
                strategy_name="sharp_action",
                parameter_space=parameter_space,
                max_evaluations=5
            )
            
            # Mock strategy processors
            mock_processor = Mock()
            mock_processor.config = {"param1": 0.5}
            mock_processor.__class__.__name__ = "SharpActionProcessor"
            job.strategy_processors = [mock_processor]
            
            # Test parameter evaluation
            parameters = {"min_differential_threshold": 15.0}
            
            result = await engine._evaluate_parameters(job, parameters)
            
            assert result is not None
            assert result.objective_value > 0
            assert result.roi_percentage > 0
            assert result.total_bets >= 20  # Mock returns 25


class TestCrossValidator:
    """Test cross-validation functionality"""
    
    @pytest.mark.asyncio
    async def test_validator_initialization(self):
        """Test validator initialization"""
        
        mock_engine = MockBacktestingEngine()
        config = ValidationConfig(
            n_folds=3,
            validation_start_date=datetime(2024, 1, 1, tzinfo=EST),
            validation_end_date=datetime(2024, 3, 31, tzinfo=EST)
        )
        
        validator = CrossValidator(mock_engine, config)
        assert validator.config.n_folds == 3
    
    def test_time_fold_creation(self):
        """Test time-based fold creation"""
        
        mock_engine = MockBacktestingEngine()
        config = ValidationConfig(
            n_folds=4,
            validation_start_date=datetime(2024, 1, 1, tzinfo=EST),
            validation_end_date=datetime(2024, 5, 1, tzinfo=EST),  # 4 months
            cv_method="time_series"
        )
        
        validator = CrossValidator(mock_engine, config)
        folds = validator._create_time_folds()
        
        assert len(folds) >= 4  # May include test fold
        
        # Check folds are chronological and non-overlapping for time series
        for i in range(len(folds) - 1):
            assert folds[i][1] <= folds[i+1][1]  # End of fold i <= end of fold i+1


class TestResultsAnalyzer:
    """Test results analysis functionality"""
    
    def test_analyzer_initialization(self):
        """Test analyzer initialization"""
        
        analyzer = ResultsAnalyzer()
        assert analyzer is not None
    
    def test_parameter_importance_analysis(self):
        """Test parameter importance analysis"""
        
        analyzer = ResultsAnalyzer()
        
        # Create mock results with varying parameters and performance
        results = []
        for i in range(20):
            result = OptimizationResult(
                job_id="test_job",
                evaluation_id=f"eval_{i}",
                parameters={
                    "param1": i * 0.1,  # Strong correlation with performance
                    "param2": i * 0.05,  # Moderate correlation
                    "param3": 5.0,  # No correlation (constant)
                },
                objective_value=i * 0.1 + (i * 0.05) * 0.5,  # param1 has higher impact
                roi_percentage=i * 1.0,
                total_bets=20
            )
            results.append(result)
        
        importance = analyzer._analyze_parameter_importance(results)
        
        assert len(importance) > 0
        # param1 should have higher importance than param2
        param1_importance = next((p for p in importance if p.parameter_name == "param1"), None)
        param2_importance = next((p for p in importance if p.parameter_name == "param2"), None)
        
        if param1_importance and param2_importance:
            assert param1_importance.importance_score >= param2_importance.importance_score
    
    def test_performance_distribution_analysis(self):
        """Test performance distribution analysis"""
        
        analyzer = ResultsAnalyzer()
        
        results = []
        values = [0.05, 0.08, 0.12, 0.15, 0.18, 0.22, 0.25, 0.28, 0.32, 0.35]
        
        for i, value in enumerate(values):
            result = OptimizationResult(
                job_id="test_job",
                evaluation_id=f"eval_{i}",
                parameters={"param1": i},
                objective_value=value,
                roi_percentage=value * 100,
                total_bets=20
            )
            results.append(result)
        
        distribution = analyzer._analyze_performance_distribution(results)
        
        assert distribution["count"] == 10
        assert distribution["min"] == 0.05
        assert distribution["max"] == 0.35
        assert 0.15 < distribution["mean"] < 0.25  # Rough check
        assert distribution["q25"] < distribution["median"] < distribution["q75"]


class TestCLIIntegration:
    """Test CLI command integration"""
    
    def test_optimization_commands_available(self):
        """Test that optimization commands are properly registered"""
        
        from src.interfaces.cli.commands.optimization import optimization_cli
        
        # Check that the command group exists
        assert optimization_cli is not None
        assert optimization_cli.name == "optimize"
        
        # Check that subcommands exist
        command_names = [cmd.name for cmd in optimization_cli.commands.values()]
        expected_commands = ["run", "status", "analyze", "cancel", "list-parameters", "validate"]
        
        for expected in expected_commands:
            assert expected in command_names, f"Command '{expected}' not found"


class TestEndToEndOptimization:
    """End-to-end optimization workflow tests"""
    
    @pytest.mark.asyncio
    async def test_complete_optimization_workflow(self):
        """Test complete optimization workflow"""
        
        # This test would require more setup and mocking
        # For now, test the main components work together
        
        registry = StrategyParameterRegistry()
        parameter_space = registry.get_parameter_space("sharp_action")
        
        # Create job
        job = create_optimization_job(
            strategy_name="sharp_action",
            parameter_space=parameter_space,
            max_evaluations=5
        )
        
        # Test job can be created and managed
        assert job.config.strategy_name == "sharp_action"
        
        job.start()
        assert job.status.value == "running"
        
        # Add some mock results
        for i in range(3):
            result = OptimizationResult(
                job_id=job.job_id,
                evaluation_id=f"eval_{i}",
                parameters=parameter_space.sample_parameters(1)[0],
                objective_value=0.1 + i * 0.02,
                roi_percentage=(0.1 + i * 0.02) * 100,
                total_bets=25
            )
            job.add_result(result)
        
        job.complete()
        
        # Test analysis
        analyzer = ResultsAnalyzer()
        analysis = analyzer.analyze_optimization_job(job)
        
        assert "job_summary" in analysis
        assert "best_configuration" in analysis
        assert "recommendations" in analysis
        
        # Test export
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = analyzer.generate_optimization_report(job, f"{temp_dir}/test_report.json")
            assert Path(report_path).exists()
            
            # Verify report content
            with open(report_path) as f:
                report_data = json.load(f)
            
            assert "optimization_report" in report_data
            assert report_data["optimization_report"]["job_id"] == job.job_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])